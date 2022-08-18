spark-submit --class Kafka --deploy-mode client --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.apache.kafka:kafka-clients:2.8.1 spark_tasks-1.0-SNAPSHOT.jar

spark-submit --class Task --deploy-mode client --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.apache.kafka:kafka-clients:2.8.1 spark_tasks-1.0-SNAPSHOT.jar
