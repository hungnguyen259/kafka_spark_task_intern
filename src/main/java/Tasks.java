import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Tasks {
    public static void main(String[] args){
        SparkSession spark = SparkSession
                .builder()
                .appName("spark tasks")
                .getOrCreate();

        String sourceFile = "kafka_tasks_data/*/*/*";
        String destinationFolder = "spark_tasks_data";

        Dataset<Row> df = spark
                .read()
                .parquet(sourceFile);
        df.select("year").select("month").select("day").show();

    }
}
