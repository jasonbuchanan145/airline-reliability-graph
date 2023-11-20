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

    @Autowired
    public SparkRunner(@Autowired CacheGraph cacheGraph){
        //throw away the results because we just want to load the cache managed by spring
        //this is done in this service because with the cachegraph being autowired it goes through spring
        //beans as opposed to just a method reference so the @Cachable is respected.
        this.cacheGraph = cacheGraph;
    }

    public Report report(String origin, String dest) {
        Report report = new Report();
        origin = origin.toUpperCase();
        dest = dest.toUpperCase();
        GraphFrame graphFrame = cacheGraph.getGf();
        List<Flight> directs = makeDirect(graphFrame,origin,dest);

        System.out.println("or"+origin+"dest"+dest);


        String[] selectExp = makeSelect(graphFrame,1);

        List<List<Flight>> layovers = graphFrame
                //reference https://graphframes.github.io/graphframes/docs/_site/user-guide.html#motif-finding
                .find("(a)-[e1]->(b); (b)-[e2]->(c)")
                //this uses spark's api, injection is not possible so parameterization is not needed.
                //The spark task scheduler ensures parallelism.
                .filter(String.format("e1.carrier_name = e2.carrier_name " +
                        "AND e1.src = '%s' AND e2.dst = '%s'", origin, dest))
                .selectExpr(selectExp).toJavaRDD().collect().parallelStream().map(row -> {
                    Flight flight1 = converter(1, row);
                    Flight flight2 = converter(2, row);
                    return Arrays.asList(flight1, flight2);
                }).sorted(Comparator.comparing(route ->
                        route.stream()
                                .max(Comparator.comparingDouble(Flight::getPercentageDelayedLongerThan15)).map(Flight::getPercentageDelayedLongerThan15)
                                .orElse(0.0))).collect(Collectors.toList());
        Collections.reverse(layovers);
        int totalFlightsOnRoute = layovers.parallelStream().flatMap(Collection::stream).map(Flight::getNumFlights).reduce(0, Integer::sum);
        report.setLeastDelayedOneHop(layovers);
        report.setNumberOfFlightsEvaluatedOneHop(totalFlightsOnRoute);
        return report;
    }

    private List<Flight> makeDirect(GraphFrame graphFrame, String origin, String dest) {
        String[] selectExp = makeSelect(graphFrame,0);
        return graphFrame.find("(a)-[e1]->(b)")
                .filter(String.format("e1.src = '%s' and e1.dst = '%s'",origin,dest))
                .selectExpr(selectExp).toJavaRDD().collect()
                .parallelStream()
                .map(row ->converter(1,row))
                .collect(Collectors.toList());
    }

    private String[] makeSelect(GraphFrame graphFrame, int numHop) {
        List<String> select = new ArrayList<>();
        for (String fieldName : graphFrame.edges().schema().fieldNames()) {
            select.add("e1." + fieldName + " as e1_" + fieldName);
            if(numHop>0)
                select.add("e2." + fieldName + " as e2_" + fieldName);
        }
        return select.toArray(new String[0]);
    }

    private String buildProperty(String property, int num){
        return String.format("e%d_%s",num,property);
    }
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