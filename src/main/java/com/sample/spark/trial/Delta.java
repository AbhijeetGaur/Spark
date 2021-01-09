package com.sample.spark.trial;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Delta {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("DeltaTrial")
                .config("spark.master", "local")
                .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate();

//        Dataset<Long> data = sparkSession.range(0, 5);
//        data.write().format("delta").save("src/main/resources/try/delta-table");

        Dataset<Row> df = sparkSession.read().format("delta").load("src/main/resources/try/delta-table");
        df.show();
    }

}
