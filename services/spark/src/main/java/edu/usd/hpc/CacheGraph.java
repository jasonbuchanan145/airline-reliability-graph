package edu.usd.hpc;

import lombok.Getter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.graphx.Edge;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.graphframes.GraphFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class CacheGraph {
    Dataset<Row> dataset;
    @Getter
    private List<Edge<Row>> edgeListMemBroadcast;
    @Getter
    private GraphFrame gf;

    CacheGraph(@Autowired Dataset<Row> dataset){
        this.dataset=dataset;
        initGraphFrame();
       // collect(edges);
    }
    private void initGraphFrame(){

        GraphFrame graphFrame = createGraphFrame(dataset);
        Dataset<Row> uniqueEdge = graphFrame.edges().dropDuplicates("src","carrier_name", "dst");
        Dataset<Row> uniqueVerts = graphFrame.vertices().dropDuplicates("id");
        GraphFrame fixup = GraphFrame.apply(uniqueVerts,uniqueEdge);
        this.gf = fixup.cache();
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
