package com.sample.spark.trial;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataFrameBasics {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .appName("Dataframe Basics")
                .config("spark.master","local")
                .getOrCreate();

        Dataset<Row> dataset = sparkSession.read()
                .format("json")
                .option("inferSchema", "true")
                .load("src/main/resources/unece.json");

        dataset.show();
        dataset.printSchema();

       // dataset.takeAsList(4).forEach(System.out::println);

        System.out.println(dataset.select("Country").count());
        System.out.println(dataset.groupBy("Country").count().count());
    }

}
