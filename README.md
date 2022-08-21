Đề bài:
Xử lý lưu trữ dữ liệu theo campaignID vào database 1 giờ 1 lần và đảm bảo các truy vấn sau:
1/ Xác định số lượng click, view từng campaign. Biết cov = 0 là click, cov = 1 là view
2/ Xác định tỉ lệ click, view từng campaign theo location
3/ Xác định số lượng người dùng từng campaign. Mỗi guid đại diện cho 1 người dùng duy nhất.
4/ Xác định số user vào nhiều campaign.


Cách chạy chương trình:
- Clone repository từ github:
git clone https://github.com/hungnguyensv259/spark_tasks_intern
- Trỏ vào thư mục repository và build chương trình nếu trong thư mục repository chưa có file jar:
cd spark_tasks_intern
mvn clean package
mv target/spark_tasks_intern-1.0-SNAPSHOT.jar DIRECTORY_TO_REPOSITORY/spark_tasks_intern
- Trong thư mục repository, chạy class Kafka:
spark-submit --class Kafka --master yarn --deploy-mode cluster --executor-memory 1g --num-executors 3 --executor-cores 3 --packages org.apache.spark:spark-sql kafka-0-10_2.12:3.1.2,org.apache.kafka:kafka-clients:2.8.1 spark_tasks_intern-1.0-SNAPSHOT.jar

- Chạy job spark với class Tasks:
spark-submit --class Tasks --master yarn --deploy-mode cluster --executor-memory 1g --num-executors 3 --executor-cores 3 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.apache.kafka:kafka-clients:2.8.1 spark_tasks_intern-1.0-SNAPSHOT.jar
Đảm bảo rằng kafka job chạy trước spark job vài phút để có dữ liệu.

- Dữ liệu được lưu vào ~/spark_task_intern/data trong hdfs, phân vùng theo năm - tháng -ngày.
- Kết quả spark job được lưu vào ~/spark_task_intern/result trong hdfs, phân vùng theo từng câu truy vấn, theo ngày.

