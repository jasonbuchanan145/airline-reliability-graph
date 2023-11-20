package edu.usd.hpc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableCaching
public class SparkSession {
    @Bean(name = "dataset")
    public Dataset<Row> dataset() {
        org.apache.spark.sql.SparkSession sparkSession = org.apache.spark.sql.SparkSession.builder().master("local[*]").appName("hpc").getOrCreate();
        return sparkSession.read().format("jdbc")
                .format("jdbc")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("url", "jdbc:mysql://services-mysqldb-1:3306/HPC")
                .option("dbtable", "airlineAirportData")
                .option("user", "root")
                .option("password", "root").load().cache();

    }
}
