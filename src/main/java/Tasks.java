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

        String sourceFile = "kafka_tasks_data";
        String destinationFolder = "spark_tasks_data";

        Dataset<Row> df = spark
                .read()
                .option("mergeSchema", "true")
                .parquet(sourceFile)
                .select("campaign", "cov", "location", "guid", "year", "month","day")
                .withColumn("date", expr("make_date(year, month, day)"));

        Dataset<Row> df1 = df.groupBy("date", "campaign", "cov").count();
        Dataset<Row> df2 = df1.groupBy("date", "campaign").agg(sum("count") .as ("sum"));
        df1.createOrReplaceTempView("df1");
        df2.createOrReplaceTempView("df2");
        Dataset<Row> ex1 = spark.sql("select df1.date, df1.campaign, df1.count*1.0/df2.count as rateView, 1 - df1.count*1.0/df2.count as rateClick" +
                "from df1, df2 " +
                "where df1.date=df2.date and df1.campaign=df2.campaign and df1.cov=0");
        ex1.show();

        Dataset<Row> df3 = df.groupBy("date", "location", "campaign", "cov").count();
        Dataset<Row> df4 = df3.groupBy("date", "location", "campaign").agg(sum("count").as("sum"));
        df3.createOrReplaceTempView("df3");
        df4.createOrReplaceTempView("df4");
        Dataset<Row> ex2 = spark.sql("select df3.date, df3.location, df3.campaign, df3.count*1.0/df4.sum as rateView, 1 - df3.count*1.0/df4.sum as rateClick " +
                "from df4, df5 " +
                "where df4.date=df5.date and df4.location=df5.location and df4.campaign=df5.campaign and df4.cov=0 " +
                "order by df4.date desc, df4.location desc, df4.campaign desc");
        ex2.show();

        Dataset<Row> ex3 = df.groupBy("date", "campaign").agg(countDistinct("guid").alias("count"));
        Dataset<Row> ex4 = df.groupBy("date", "guid").agg(count("campaign").as("count")).filter("count>1");
        ex4 = ex4.groupBy("date").agg(count("guid").as("count"));
        ex4.show();

        StructType structType = new StructType();
        structType = structType.add("guid", DataTypes.IntegerType, true);
        structType = structType.add("cov", DataTypes.IntegerType, true);
        structType = structType.add("campaign", DataTypes.IntegerType, true);
        structType = structType.add("location", DataTypes.IntegerType, true);
        List<Row> nums = new ArrayList<Row>();
        nums.add(RowFactory.create(1,1,1,7));
        nums.add(RowFactory.create(1,1,1,9));
        nums.add(RowFactory.create(1,0,1,7));
        nums.add(RowFactory.create(2,1,2,7));
        nums.add(RowFactory.create(2,0,2,7));
        nums.add(RowFactory.create(3,1,2,8));
        nums.add(RowFactory.create(4,0,2,8));
        nums.add(RowFactory.create(4,0,2,8));
        nums.add(RowFactory.create(5,0,2,9));
        nums.add(RowFactory.create(5,1,3,9));
        nums.add(RowFactory.create(6,0,3,8));
        nums.add(RowFactory.create(7,1,3,8));
        nums.add(RowFactory.create(6,1,3,8));
        nums.add(RowFactory.create(7,0,3,8));
        Dataset<Row> res3 = spark.createDataFrame(nums, structType);
        res3.write().option("delimiter", ";").option("header", "true").mode(SaveMode.Overwrite).parquet( "test");

    }
}
