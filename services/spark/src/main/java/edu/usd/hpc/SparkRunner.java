package edu.usd.hpc;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Component
public class SparkRunner {

    public Report report(String origin, String dest) {
        origin = origin.toUpperCase();
        dest = dest.toUpperCase();
        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("hpc").getOrCreate();
        Dataset<Row> dataset = sparkSession.read().format("jdbc")
                .format("jdbc")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3307/HPC")
                .option("dbtable", "airlineAirportData")
                .option("user", "root")
                .option("password", "root")
                .load();

        GraphFrame graphFrame = createGraphFrame(dataset);
        graphFrame.edges().dropDuplicates();
        Graph<Row, Row> graphx = graphFrame.toGraphX();
        JavaRDD<Edge<Row>> edges = graphx.edges().toJavaRDD();

        String finalOrigin = origin;
        JavaRDD<Edge<Row>> starting = edges
                .filter(vert -> vert.attr().getAs("src").equals(finalOrigin));
        List<Edge<Row>> edgesList = edges.collect();

        String finalDest = dest;


        List<Flight> directs = starting
                .filter(rowEdge -> rowEdge.attr().getAs("dst").equals(finalDest))
                .collect().stream().map(this::convertEdgeToFlight).collect(Collectors.toList());

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

        System.out.println(oneHop.size());
        Report report = new Report();
        return report;
    }

    private Flight convertEdgeToFlight(Edge<Row> edge) {
        Flight flight = new Flight();
        flight.setOrigin(edge.attr().getAs("src"));
        flight.setOriginCityName(edge.attr().getAs("origin_city_name"));
        flight.setOriginState(edge.attr().getAs("origin_state_abr"));
        flight.setDest(edge.attr().getAs("dst"));
        flight.setDestCityName(edge.attr().getAs("dest_city_name"));
        flight.setPercentageDelayedLongerThan15(edge.attr().getAs("percentage_delayed_longer_than_15"));
        flight.setPercentage_cancelled(edge.attr().getAs("percentage_cancelled"));
        flight.setAvgDelayLongerThan15(edge.attr().getAs("avg_delay_longer_than_15"));
        return flight;
    }


    private GraphFrame createGraphFrame(Dataset<Row> dataset) {
        Dataset<Row> airports = dataset.selectExpr("origin as id").distinct().union(dataset.selectExpr("dest as id").distinct());
        Dataset<Row> edges = dataset.selectExpr("origin as src", "dest as dst", "carrier_name",
                "origin_city_name", "origin_state_abr",
                "dest_city_name", "dest_state_abr",
                "percentage_delayed_longer_than_15", "avg_delay_longer_than_15",
                "percentage_cancelled");
        return new GraphFrame(airports, edges);
    }
}
