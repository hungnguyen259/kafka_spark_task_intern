import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class Task {
    String resultFolder = "spark_task_intern/result";
    String checkpoint ="checkpoint/spark_task_intern/Task";
    String sourceFile = "spark_task_intern/data";

    public Dataset<Row> read(){
        SparkSession spark = SparkSession
                .builder()
                .appName("spark tasks")
                .getOrCreate();

        spark.sql("set spark.sql.streaming.schemaInference=true");
        Dataset<Row> df = spark
                .readStream()
                .option("mergeSchema", "true")
                .parquet(sourceFile)
                .select("campaign", "cov", "location", "guid", "year", "month", "day")
                .withColumn("date", expr("make_date(year, month, day)"));
        return df;
    }
    public void write(Dataset<Row> df, String path) {
        df
                .coalesce(1)
                .write()
                .format("csv")
                .mode("append")
                .option("path", resultFolder + path)
                .option("header", true)
                .option("checkpointLocation", checkpoint+ path)
                .partitionBy("date")
                .save();

    }


    public static void main(String[] args) {

        Task task = new Task();
        Dataset<Row> df = task.read();

        try{
            df.writeStream()
                    .foreachBatch((dataframe, batchId) ->{
                        dataframe.persist();
                        //        Số lượng click, view ứng với mỗi campaign
                        Dataset<Row> df1 = dataframe.groupBy("date", "campaign", "cov").count();
                        Dataset<Row> df2 = dataframe.groupBy("date", "campaign").count();
                        df1.createOrReplaceTempView("df1");
                        df2.createOrReplaceTempView("df2");
                        Dataset<Row> ex1 = dataframe.sparkSession().sql("select df2.date, df2.campaign, ifnull(df1.count, 0) as view, ifnull(df2.count-df1.count, df1.count) as click " +
                                "from df1 right join df2 " +
                                "on df1.date=df2.date and df1.campaign=df2.campaign and df1.cov=0 " +
                                "order by df2.date desc, df2.campaign desc");
                        ex1.filter("cast(campaign as decimal) is not null");

                        // Số lương click, view môi campaign theo location
                        Dataset<Row> df3 = dataframe.groupBy("date", "location", "campaign", "cov").count();
                        Dataset<Row> df4 = dataframe.groupBy("date", "location", "campaign").count();
                        df3.createOrReplaceTempView("df3");
                        df4.createOrReplaceTempView("df4");
                        Dataset<Row> ex2 = dataframe.sparkSession().sql("select df4.date, df4.location, df4.campaign, df4.count as sum, ifnull(df3.count, 0) as view, ifnull(df4.count-df3.count, df4.count) as click " +
                                "from df3 right join df4 " +
                                "on df4.date=df3.date and df3.campaign=df4.campaign and df3.location=df4.location and df3.cov=0 " +
                                "order by df4.date desc, df4.location desc, df4.campaign desc");
                        ex2.filter("cast(campaign as decimal) is not null");

                        //      Số lượng user truy cập ứng với mỗi campaign
                        Dataset<Row> ex3 = dataframe.groupBy("date", "campaign")
                                .agg(countDistinct("guid").alias("count"))
                                .filter("cast(campaign as decimal) is not null")
                                .orderBy(col("date").desc(), col("campaign").desc());

                        //      Số lượng user truy cập nhiều hơn một campaign
                        Dataset<Row> ex4 = dataframe.groupBy("date", "guid")
                                .agg(count("campaign").as("count"))
                                .filter("count>1")
                                .groupBy("date").agg(count("guid"))
                                .orderBy(col("date").desc());

                        //Ghi
                        task.write(ex1, "/ex1");
                        task.write(ex2, "/ex2");
                        task.write(ex3, "/ex3");
                        task.write(ex4, "/ex4");

                        dataframe.unpersist();

                    })
                    .trigger(Trigger.ProcessingTime("60 minute"))
                    .start()
                    .awaitTermination();
        }
        catch (TimeoutException|StreamingQueryException e){
            e.printStackTrace();
        }

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

    }
}

