import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class Task {
    public static void main(String[] args) {

        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("campaign", DataTypes.IntegerType, true),
                DataTypes.createStructField("cov", DataTypes.IntegerType, true),
                DataTypes.createStructField("location", DataTypes.IntegerType, true),
                DataTypes.createStructField("guid", DataTypes.IntegerType, true) }
        );

        SparkSession spark = SparkSession
                .builder()
                .appName("spark tasks")
                .getOrCreate();

        String sourceFile = "task_intern/data";
        String resultFolder = "task_intern/result";
        String checkpoint ="tmp/task_intern";
        Dataset<Row> df = spark
                .readStream()
                .schema(schema)
                .option("mergeSchema", "true")
                .parquet(sourceFile)
                .select("campaign", "cov", "location", "guid", "year", "month", "day")
                .withColumn("date", expr("make_date(year, month, day)"));


//        Số lượng click, view ứng với mỗi campaign
        Dataset<Row> df1 = df.groupBy("date", "campaign", "cov").count();
        Dataset<Row> df2 = df1.groupBy("date", "campaign").agg(sum("count").as("sum"));
        df1.createOrReplaceTempView("df1");
        df2.createOrReplaceTempView("df2");
        Dataset<Row> ex1 = spark.sql("select df2.date, df2.campaign, ifnull(df1.count, 0) as view, ifnull(df2.sum-df1.count, sum) as click " +
                "from df1 right join df2 " +
                "on df1.date=df2.date and df1.campaign=df2.campaign and df1.cov=0 " +
                "order by df2.date desc, df2.campaign desc");


//        Tìm số lượng click, tỉ lệ view ứng với mỗi campaign theo location
        Dataset<Row> df3 = df.groupBy("date", "location", "campaign", "cov").count();
        Dataset<Row> df4 = df3.groupBy("date", "location", "campaign").agg(sum("count").as("sum"));
        df3.createOrReplaceTempView("df3");
        df4.createOrReplaceTempView("df4");
        Dataset<Row> ex2 = spark.sql("select df4.date, df4.location, df4.campaign, df4.sum, ifnull(df3.count, 0) as view, ifnull(df4.sum-df3.count, sum) as click " +
                "from df3 right join df4 " +
                "on df4.date=df3.date and df3.campaign=df4.campaign and df3.location=df4.location and df3.cov=0 " +
                "order by df4.date desc, df4.location desc, df4.campaign desc");

//        Tìm tỉ lệ click, tỉ lệ view ứng vỡi mỗi campaign
//        Dataset<Row> df1 = df.groupBy("date", "campaign", "cov").count();
//        Dataset<Row> df2 = df1.groupBy("date", "campaign").agg(sum("count").as("sum"));
//        df1.createOrReplaceTempView("df1");
//        df2.createOrReplaceTempView("df2");
//        Dataset<Row> ex1 = spark.sql("select df2.date, df2.campaign, ifnull(df1.count, 0) as view, ifnull(df2.sum-df1.count, sum) as click " +
//                "from df1 right join df2 " +
//                "on df1.date=df2.date and df1.campaign=df2.campaign and df1.cov=0 " +
//                "order by df1.date desc, df2.campaign desc");
//        ex1.show();

//        Tìm tỉ lệ click, tỉ lệ view ứng với mỗi campaign theo location
//        Dataset<Row> df3 = df.groupBy("date", "location", "campaign", "cov").count();
//        Dataset<Row> df4 = df3.groupBy("date", "location", "campaign").agg(sum("count").as("sum"));
//        df3.createOrReplaceTempView("df3");
//        df4.createOrReplaceTempView("df4");
//        Dataset<Row> ex5 = spark.sql("select df4.date, df4.campaign, df4.location, df4.sum, ifnull(df3.count, 0) as view, ifnull(df4.sum-df3.count, sum) as click " +
//                        "from df3 right join df4 " +
//                        "on df4.date=df3.date and df3.campaign=df4.campaign and df3.location=df4.location and df3.cov=0 " +
//                        "order by df3.date desc, df3.location desc, df3.campaign desc")
//                .withColumn("rateView", col("view").divide(col("sum")))
//                .withColumn("rateClick", col("click").divide(col("sum")));
//        ex5.show();

//      Số lượng user truy cập ứng với mỗi campaign

        Dataset<Row> ex3 = df.groupBy("date", "campaign")
                .agg(countDistinct("guid").alias("count")).
                orderBy(col("date").desc(), col("campaign").desc());

//      Số lượng user truy cập nhiều hơn một campaign
        Dataset<Row> ex4 = df.groupBy("date", "guid")
                .agg(count("campaign").as("count"))
                .filter("count>1")
                .groupBy("date").agg(count("guid"))
                .orderBy(col("date").desc());

        try{
            ex1.writeStream()
                    .format("csv")
                    .outputMode("append")
                    .option("path", resultFolder + "/ex1")
                    .option("header", true)
                    .option("checkpointLocation", checkpoint+ "/ex1")
                    .trigger(Trigger.ProcessingTime("60 seconds"))
                    .partitionBy("year", "month", "day")
                    .start()
                    .awaitTermination();
        }
        catch(TimeoutException | StreamingQueryException e){
            e.printStackTrace();
        }
        try{
            ex2.writeStream()
                    .format("csv")
                    .outputMode("append")
                    .option("path", resultFolder + "/ex2")
                    .option("header", true)
                    .option("checkpointLocation", checkpoint+ "/ex2")
                    .trigger(Trigger.ProcessingTime("60 seconds"))
                    .partitionBy("year", "month", "day")
                    .start()
                    .awaitTermination();
        }
        catch(TimeoutException | StreamingQueryException e){
            e.printStackTrace();
        }
        try{
            ex3.writeStream()
                    .format("csv")
                    .outputMode("append")
                    .option("path", resultFolder + "/ex3")
                    .option("header", true)
                    .option("checkpointLocation", checkpoint + "/ex3")
                    .trigger(Trigger.ProcessingTime("60 seconds"))
                    .partitionBy("year", "month", "day")
                    .start()
                    .awaitTermination();
        }
        catch(TimeoutException | StreamingQueryException e){
            e.printStackTrace();
        }
        try{
            ex4.writeStream()
                    .format("csv")
                    .outputMode("append")
                    .option("path", resultFolder + "/ex4")
                    .option("header", true)
                    .option("checkpointLocation", checkpoint+ "/ex4")
                    .trigger(Trigger.ProcessingTime("60 seconds"))
                    .partitionBy("year", "month", "day")
                    .start()
                    .awaitTermination();
        }
        catch(TimeoutException | StreamingQueryException e){
            e.printStackTrace();
        }
    }
}

