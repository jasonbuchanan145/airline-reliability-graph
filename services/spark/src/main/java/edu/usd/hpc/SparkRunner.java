package edu.usd.hpc;

import org.apache.spark.sql.Row;
import org.graphframes.GraphFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Component
public class SparkRunner {


    private final CacheGraph cacheGraph;

    //overly paranoid of a service autowiring to another service, make a custom constructor to make a circular dependency fail fast
    @Autowired
    public SparkRunner(@Autowired CacheGraph cacheGraph){
        this.cacheGraph = cacheGraph;
    }

    public Report report(String origin, String dest) {
        long start = System.currentTimeMillis();
        Report report = new Report();
        origin = origin.toUpperCase();
        dest = dest.toUpperCase();
        GraphFrame graphFrame = cacheGraph.getGraphFrame();
        //start with directs
        long startDirect = System.currentTimeMillis();
        List<Flight> directs = makeDirect(graphFrame,origin,dest);
        report.setLeastDelayedDirect(directs);
        report.setNumberOfFlightsDirect(directs.parallelStream().map(Flight::getNumFlights).reduce(0, Integer::sum));
        report.setTimeToCalculateDirectRoutes(System.currentTimeMillis()-startDirect);


        //start with one layover routes
        long startLayover=System.currentTimeMillis();
        List<List<Flight>> layovers = makeOneHop(graphFrame,origin,dest);
        report.setTimeToCalculateOneStopRoutes(System.currentTimeMillis()-startLayover);

        //sum how many flights are in all the one hop routes in parallel
        int totalFlightsOnRoute = layovers.parallelStream()
                //this gets each flight out of it's list to sum the flight from the result set in parallel
                .flatMap(Collection::stream)
                .map(Flight::getNumFlights)
                //reduce all numFlights to a sum
                .reduce(0, Integer::sum);
        report.setLeastDelayedOneHop(layovers);
        report.setNumberOfFlightsEvaluatedOneHop(totalFlightsOnRoute);
        report.setTotalTime(System.currentTimeMillis()-start);
        return report;
    }



    private List<Flight> makeDirect(GraphFrame graphFrame, String origin, String dest) {
        //please see comments for this in makeOneHop in this file, it's the same concept.
        String[] selectExp = makeSelect(graphFrame,0);
        return graphFrame.find("(a)-[e1]->(b)")
                .filter(String.format("e1.src = '%s' and e1.dst = '%s'",origin,dest))
                .selectExpr(selectExp).toJavaRDD().collect()
                .parallelStream()
                .map(row ->converter(1,row))

                .sorted(
                        Comparator.comparingDouble(Flight::getAvgDelayLongerThan15)
                )
                .collect(Collectors.toList());
    }
    private List<List<Flight>> makeOneHop(GraphFrame graphFrame, String origin, String dest) {
        String[] selectExp = makeSelect(graphFrame,1);
        List<List<Flight>> layovers = graphFrame
                //reference https://graphframes.github.io/graphframes/docs/_site/user-guide.html#motif-finding if we wanted to do more layovers
                // like (a)-[e1]->(b); (b)-[e2]->(c); (c)->[e3]->(d) and just modify the filter conditions. it's pretty neat
                .find("(a)-[e1]->(b); (b)-[e2]->(c)")
                //this uses spark's api, injection is not possible so parameterization is not needed.
                //The spark task scheduler ensures parallelism.
                .filter(String.format("e1.carrier_name = e2.carrier_name " +
                        "AND e1.src = '%s' AND e2.dst = '%s'", origin, dest))
                .selectExpr(selectExp).toJavaRDD().collect()
                //All result sets have been returned. For anything after the above collect() the spark api is not
                // available to ensure parallelism so we can use java's parallelStream() to map the result set to objects in parallel
                .parallelStream().map(row -> {
                    Flight flight1 = converter(1, row);
                    Flight flight2 = converter(2, row);
                    return Arrays.asList(flight1, flight2);
                }).
                //sorts the flight list based on the maximum number of the highest delay percentage in a route
                        sorted(Comparator.comparing(route ->
                        route.stream()
                                .max(Comparator.comparingDouble(Flight::getPercentageDelayedLongerThan15)).map(Flight::getPercentageDelayedLongerThan15)
                                .orElse(0.0)))

                .collect(Collectors.toList());
        return layovers;
    }

    private String[] makeSelect(GraphFrame graphFrame, int numHop) {
        List<String> select = new ArrayList<>();
        //automates the building of the select fields since there's multiple edges you have to tell it which field individually from which edge
        //maps to what value. wild cards like e1.* do not preserve which e a given value came from so for the first edge the properties are
        //being mapped from e1.(whatever) to e1_(whatever)
        String[] properties = graphFrame.edges().schema().fieldNames();
        for (String fieldName : properties) {
            select.add("e1." + fieldName + " as e1_" + fieldName);
            if(numHop>0)
                select.add("e2." + fieldName + " as e2_" + fieldName);
        }
        return select.toArray(new String[0]);
    }

    private String buildProperty(String property, int num){
        return String.format("e%d_%s",num,property);
    }
    //maps an edge in a row to a flight object
    private Flight converter(int num,Row row){
        Flight flight = new Flight();
        flight.setOrigin(row.getAs(buildProperty("src",num)));
        flight.setOriginCityName(row.getAs(buildProperty("origin_city_name",num)));
        flight.setDest(row.getAs(buildProperty("dst",num)));
        flight.setCarrierName(row.getAs(buildProperty("carrier_name",num)));
        flight.setDestCityName(row.getAs(buildProperty("dest_city_name",num)));
        flight.setPercentageDelayedLongerThan15(row.getAs(buildProperty("percentage_delayed_longer_than_15",num)));
        flight.setPercentageCancelled(row.getAs(buildProperty("percentage_cancelled",num)));
        flight.setNumFlights(row.getAs(buildProperty("num_flights",num)));
        flight.setAvgDelayLongerThan15(row.getAs(buildProperty("avg_delay_longer_than_15",num)));
        return flight;
    }


}