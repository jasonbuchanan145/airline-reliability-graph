package edu.usd.hpc;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;
import java.util.stream.Collectors;

@Component
public class SparkRunner {


    private final CacheGraph cacheGraph;

    @Autowired
    public SparkRunner(@Autowired CacheGraph cacheGraph){
        cacheGraph.getEdges();
        this.cacheGraph = cacheGraph;
    }

    public Report report(String origin, String dest) {
        Report report = new Report();
        long initializationStart = System.currentTimeMillis();
        origin = origin.toUpperCase();
        dest = dest.toUpperCase();
        JavaRDD<Edge<Row>> edges = cacheGraph.getEdges();
        String finalOrigin = origin;
        List<Edge<Row>> edgesList = edges.collect();
        JavaRDD<Edge<Row>> starting = edges
                .filter(edge -> edge.attr().getAs("src").equals(finalOrigin));

        report.setTimeForInitializingTheGraph(System.currentTimeMillis()-initializationStart);

        String finalDest = dest;
        long timeForDirectStart = System.currentTimeMillis();


        List<Flight> directs = starting
                .filter(rowEdge -> rowEdge.attr().getAs("dst").equals(finalDest))
                .collect().stream().map(this::convertEdgeToFlight).collect(Collectors.toList());

        report.setTimeToCalculateDirectRoutes(System.currentTimeMillis()-timeForDirectStart);
        long timeForOneHopStart = System.currentTimeMillis();
        List<List<Edge<Row>>> oneHop = starting.map(rowEdge -> {
                    String carrier = rowEdge.attr().getAs("carrier_name");
                    long id = rowEdge.dstId();
                    List<Edge<Row>> filteredEdges = edgesList.stream()
                            .filter(vert -> vert.srcId() == id &&
                                    vert.attr().getAs("carrier_name").equals(carrier) &&
                                    vert.attr().getAs("dst").equals(finalDest))
                            .collect(Collectors.toList());

                    if (filteredEdges.isEmpty()) {
                        return null;
                    }

                    List<Edge<Row>> flightRows = new ArrayList<>();
                    flightRows.add(rowEdge);
                    flightRows.add(filteredEdges.get(0));
                    return flightRows;
                })
                .filter(Objects::nonNull)
                .collect();

        List<List<Flight>> oneHopFlights = oneHop.parallelStream().map(route -> {
            List<Flight> flights = new ArrayList<>();
            for (Edge<Row> edge : route) {
                Flight flight = convertEdgeToFlight(edge);
                flights.add(flight);
            }
            return flights;
        }).collect(Collectors.toList());
        report.setTimeToCalculateOneStopRoutes(System.currentTimeMillis()-timeForOneHopStart);
        long timeToPrepareReport = System.currentTimeMillis();
        List<Flight> leastDelayedDirect = directs.stream().sorted(Comparator.comparing(Flight::getPercentageDelayedLongerThan15).reversed()).collect(Collectors.toList());
        List<List<Flight>> leastDelayedOneHop = oneHopFlights.stream()
                .sorted(Comparator.comparing(route -> route.stream().max(Comparator.comparingDouble(Flight::getPercentageDelayedLongerThan15))
                        .map(Flight::getPercentageDelayedLongerThan15)
                        .get())).collect(Collectors.toList());
        int totalNumberOfFlightsInRoute = oneHopFlights.parallelStream().flatMap(flights -> flights.stream().map(Flight::getNumFlights)).reduce(0,(a,b)->a+b);
        report.setLeastDelayedOneHop(leastDelayedOneHop);
        report.setLeastDelayedDirect(leastDelayedDirect);
        report.setTimeToPrepareTheReport(System.currentTimeMillis()-timeToPrepareReport);
        return report;
    }

    private Flight convertEdgeToFlight(Edge<Row> edge) {
        Flight flight = new Flight();
        flight.setOrigin(edge.attr().getAs("src"));
        flight.setOriginCityName(edge.attr().getAs("origin_city_name"));
        flight.setDest(edge.attr().getAs("dst"));
        flight.setCarrierName(edge.attr().getAs("carrier_name"));
        flight.setDestCityName(edge.attr().getAs("dest_city_name"));
        flight.setPercentageDelayedLongerThan15(edge.attr().getAs("percentage_delayed_longer_than_15"));
        flight.setPercentageCancelled(edge.attr().getAs("percentage_cancelled"));
        flight.setNumFlights(edge.attr().getAs("num_flights"));
        flight.setAvgDelayLongerThan15(edge.attr().getAs("avg_delay_longer_than_15"));
        return flight;
    }


    private GraphFrame createGraphFrame(Dataset<Row> dataset) {
        Dataset<Row> airports = dataset.selectExpr("origin as id").distinct().union(dataset.selectExpr("dest as id").distinct());
        Dataset<Row> edges = dataset.selectExpr("origin as src", "dest as dst", "carrier_name",
                "origin_city_name",
                "dest_city_name",
                "percentage_delayed_longer_than_15", "avg_delay_longer_than_15","num_flights",
                "percentage_cancelled");
        return new GraphFrame(airports, edges);
    }
}
