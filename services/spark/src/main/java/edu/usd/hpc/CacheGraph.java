package edu.usd.hpc;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;
import scala.Tuple2;
import scala.Tuple3;

@Component
public class CacheGraph {
    @Cacheable
    public JavaRDD<Edge<Row>> getEdges(){
        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("hpc").getOrCreate();
        Dataset<Row> dataset = sparkSession.read().format("jdbc")
                .format("jdbc")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("url", "jdbc:mysql://services-mysqldb-1:3306/HPC")
                .option("dbtable", "airlineAirportData")
                .option("user", "root")
                .option("password", "root")
                .load();

        GraphFrame graphFrame = createGraphFrame(dataset);
        Graph<Row, Row> graphx = graphFrame.toGraphX();
        JavaRDD<Edge<Row>> edgesPossibleDuplicates = graphx.edges().toJavaRDD();
//define a key of origin, dest, and carrier_name
        JavaPairRDD<Tuple3<Object, Object,String>, Edge<Row>> pairEdges = edgesPossibleDuplicates.mapToPair(edge ->
                new Tuple2<>(new Tuple3<>(edge.srcId(), edge.dstId(),edge.attr().getAs("carrier_name")), edge));
//remove duplicate edges based on the key above
        JavaPairRDD<Tuple3<Object, Object, String>, Edge<Row>> uniqueEdges = pairEdges.reduceByKey((edge1, edge2) -> edge1);

        return uniqueEdges.values();
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
