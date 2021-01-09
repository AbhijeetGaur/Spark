package com.sample.spark.trial;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CsvRead {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("DeltaTrial")
                .config("spark.master", "local")
                .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate();

        StructType tradeSchema = new StructType()
                .add(new StructField("symbol", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("event_ts", DataTypes.TimestampType, true, Metadata.empty()))
                .add(new StructField("mod_dt", DataTypes.DateType, true, Metadata.empty()))
                .add(new StructField("trade_pr", DataTypes.DoubleType, true, Metadata.empty()));

        Dataset<Row> dataset = sparkSession.read().option("header", "true").schema(tradeSchema)
                .csv("src/main/resources/ASOF_Trades.csv");
        dataset.show(10);

        dataset.printSchema();
    }

}
