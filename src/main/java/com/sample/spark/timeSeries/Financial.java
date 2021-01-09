package com.sample.spark.timeSeries;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Financial {

    public static void main(String[] args) {
        String inputFormat = "csv";
        String outputFormat = "delta";

        String tradeCsvFile = "src/main/resources/ASOF_Trades.csv";
        String tradeDeltaFile = "src/main/resources/trades";
        String quoteCsvFile = "src/main/resources/ASOF_Quotes.csv";
        String quoteDeltaFile = "src/main/resources/quotes";

        StructType tradeSchema = getTradeSchema();
        StructType quoteSchema = getQuoteSchema();

        SparkSession sparkSession = createSparkSession();
        convertFormats(sparkSession, tradeSchema, inputFormat, tradeCsvFile, outputFormat, tradeDeltaFile);
        convertFormats(sparkSession, quoteSchema, inputFormat, quoteCsvFile, outputFormat, quoteDeltaFile);

        Dataset<Row> tradeDataset = readFile(sparkSession, tradeDeltaFile, outputFormat);
        tradeDataset.show(10);
    }

    private static Dataset<Row> readFile(SparkSession sparkSession, String fileName, String format) {
        return sparkSession.read().format(format).load(fileName);
    }

    private static void convertFormats(SparkSession sparkSession, StructType schema, String iFormat, String ifileName,
                                       String oFormat, String ofileName) {
        sparkSession.read().format(iFormat).schema(schema).option("header", "true").option("delimiter", ",")
                .load(ifileName).write().mode("overwrite").format(oFormat).save(ofileName);
    }

    private static SparkSession createSparkSession() {
        return SparkSession.builder()
                .appName("Financial Time Series")
                .config("spark.master", "local")
                .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate();
    }

    private static StructType getTradeSchema() {
        return new StructType()
                .add(new StructField("symbol", DataTypes.StringType, false, Metadata.empty()))
                .add(new StructField("event_ts", DataTypes.TimestampType, false, Metadata.empty()))
                .add(new StructField("mod_dt", DataTypes.DateType, false, Metadata.empty()))
                .add(new StructField("trade_pr", DataTypes.DoubleType, false, Metadata.empty()));
    }

    private static StructType getQuoteSchema() {
        return new StructType()
                .add(new StructField("symbol", DataTypes.StringType, false, Metadata.empty()))
                .add(new StructField("event_ts", DataTypes.TimestampType, false, Metadata.empty()))
                .add(new StructField("trade_dt", DataTypes.DateType, false, Metadata.empty()))
                .add(new StructField("bid_pr", DataTypes.DoubleType, false, Metadata.empty()))
                .add(new StructField("ask_pr", DataTypes.DoubleType, false, Metadata.empty()));
    }

}
