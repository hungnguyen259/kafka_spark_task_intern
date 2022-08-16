import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;
public class Tasks {
    public static void main(String[] args){
        SparkSession spark = SparkSession
                .builder()
                .appName("spark tasks")
                .getOrCreate();

        String sourceFile = "test";
        String destinationFolder = "spark_tasks_data";

        Dataset<Row> df = spark
                .read()
                .option("mergeSchema", "true")
                .parquet(sourceFile)
                .select("campaign", "cov", "location", "guid", "year", "month","day")
                .withColumn("date", expr("make_date(year, month, day)"));

        Dataset<Row> df1 = df.groupBy("date", "campaign", "cov").count();
        Dataset<Row> df2 = df1.groupBy("date", "campaign").agg(sum("count").as("sum"));
        df1.createOrReplaceTempView("df1");
        df2.createOrReplaceTempView("df2");
        Dataset<Row> ex1 = spark.sql("select df1.date, df1.campaign,round( df1.count/df2.sum, 4) as rateView, round(1-df1.count/df2.sum, 4) as rateCLick  " +
                "from df1, df2 " +
                "where df1.date=df2.date and df1.campaign=df2.campaign " +
		"order by df1.date desc");
	        Dataset<Row> df4 = df.groupBy("date", "location", "campaign", "cov").count();
        Dataset<Row> df5 = df4.groupBy("date", "location", "campaign").agg(sum("count").as("sum"));
        df4.createOrReplaceTempView("df4");
        df5.createOrReplaceTempView("df5");
        Dataset<Row> ex2 = spark.sql("select df4.date, df4.location, df4.campaign, round(df4.count*1.0/df5.sum, 4) as rateView, round(1 - df4.count*1.0/df5.sum, 4) as rateClick " +
                "from df4, df5 " +
                "where df4.date=df5.date and df4.location=df5.location and df4.campaign=df5.campaign and df4.cov=0 " +
                "order by df4.date desc, df4.location desc, df4.campaign desc");
        ex1.show();
	ex2.show();
	Dataset<Row> ex3 = df.groupBy("date", "campaign").agg(countDistinct("guid").alias("count"));
        Dataset<Row> ex4 = df.groupBy("date", "guid").agg(count("campaign").as("count")).filter("count>1");
	Dataset<Row> ex5 = ex4.groupBy("date").agg(count("guid"));
        ex3.show();
	ex4.show();
	ex5.show();
    }
}
