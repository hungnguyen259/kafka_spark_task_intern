import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class Kafka {
    public static void main(String[] args) {
        String kafkaServer = "10.3.68.20:9092, 10.3.68.21:9092, 10.3.68.23:9092, 10.3.68.26:9092, 10.3.68.28:9092, 10.3.68.32:9092, 10.3.68.47:9092, 10.3.68.48:9092, 10.3.68.50:9092, 10.3.68.52:9092";
        String kafkaTopic = "rt-queue_1";
        String savedDataLocation = "test/data";
        String checkpoint = "/tmp/test";
        SparkSession spark = SparkSession
                .builder()
                .appName("kafka")
                .getOrCreate();

        Dataset<Row> data = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaServer)
                .option("subscribe", kafkaTopic)
                .load()
                .selectExpr("CAST(value AS STRING) AS dataValue");
        data.printSchema();

        Dataset<Row> df = data
                .withColumn("timeLog", split(col("dataValue"), "\t").getItem(0).cast("long"))
                .withColumn("ip", split(col("dataValue"), "\t").getItem(1).cast("String"))
                .withColumn("userAgent", split(col("dataValue"), "\t").getItem(2).cast("String"))
                .withColumn("guidIime", split(col("dataValue"), "\t").getItem(3).cast("String"))
                .withColumn("iditem", split(col("dataValue"), "\t").getItem(4).cast("String"))
                .withColumn("viewCount", split(col("dataValue"), "\t").getItem(5).cast("int"))
                .withColumn("guid", split(col("dataValue"), "\t").getItem(6).cast("String"))
                .withColumn("domain", split(col("dataValue"), "\t").getItem(7).cast("String"))
                .withColumn("tp", split(col("dataValue"), "\t").getItem(8).cast("long"))
                .withColumn("cov", split(col("dataValue"), "\t").getItem(9).cast("long"))
                .withColumn("zoneId", split(col("dataValue"), "\t").getItem(10).cast("long"))
                .withColumn("campaign", split(col("dataValue"), "\t").getItem(11).cast("String"))
                .withColumn("channelId", split(col("dataValue"), "\t").getItem(12).cast("long"))
                .withColumn("guidIsNew", split(col("dataValue"), "\t").getItem(13).cast("long"))
                .withColumn("referer", split(col("dataValue"), "\t").getItem(14).cast("String"))
                .withColumn("location", split(col("dataValue"), "\t").getItem(15).cast("int"))
                .withColumn("transactionId", split(col("dataValue"), "\t").getItem(16).cast("String"))
                .withColumn("lsn", split(col("dataValue"), "\t").getItem(17).cast("int"))
                .withColumn("iteminfo", split(col("dataValue"), "\t").getItem(18).cast("int"))
                .withColumn("tpn", split(col("dataValue"), "\t").getItem(19).cast("int"))
                .withColumn("rid", split(col("dataValue"), "\t").getItem(20).cast("String"))
                .withColumn("sspz", split(col("dataValue"), "\t").getItem(21).cast("long"))
                .withColumn("adc_cpa", split(col("dataValue"), "\t").getItem(22).cast("String"))
                .withColumn("pgl", split(col("dataValue"), "\t").getItem(23).cast("int"))
                .withColumn("dl", split(col("dataValue"), "\t").getItem(24).cast("int"))
                .withColumn("date", from_unixtime(col("timeLog"), "MM-dd-yyyy"))
                .withColumn("year", split(col("date"), "-").getItem(2).cast("int"))
                .withColumn("month", split(col("date"), "-").getItem(0).cast("int"))
                .withColumn("day", split(col("date"), "-").getItem(1).cast("int"))
                .drop("timeLog")
                .drop("date")
                .drop("dataValue");


        try {
            df
            .coalesce(1)
                    .writeStream()
                    .format("parquet")
                    .outputMode("append")
                    .option("path", savedDataLocation)
                    .option("header", true)
                    .option("checkpointLocation", checkpoint +"/data")
                    .trigger(Trigger.ProcessingTime("5 minute"))
                    .partitionBy("year", "month", "day")
                    .start()
                    .awaitTermination();
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }
}
