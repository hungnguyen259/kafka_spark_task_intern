Đề bài: Lấy dữ liệu từ các kafka broker sau:

* Tên topic: rt-queue_1

* Host:  10.3.68.20:9092,10.3.68.21:9092,10.3.68.23:9092,10.3.68.26:9092, 10.3.68.28:9092, 10.3.68.32:9092, 10.3.68.47:9092, 10.3.68.48:9092, 10.3.68.50:9092, 10.3.68.52:9092 

* Message: Mỗi trường cách nhau bởi dấu '\t'  

{  

timeLog: thời gian xuất hiện log( thời gian lên quảng cáo)  

ip:  

userAgent:  

GUIDTime:   

bannerId:  id của banner quảng cáo.  

viewCount:  

GUID: Định danh người dùng duy nhất.  

admDomain:  

tp:  

cov: click or view. banner có được click hay chỉ lướt qua.

zoneID:vị trí quảng cáo lên website.  

campaign: một id thể hiện chiến dịch quảng cáo gồm 1 list các banner.  

channelID:  

isNew:  

referer:  

regionInfo.Value  

tid  

price: giá lên quảng cáo tại vị trí zoneId.  

}

Xử lý lưu trữ dữ liệu theo campaignID vào database 1 giờ 1 lần và đảm bảo các truy vấn sau:

1/ Xác định số lượng click, view từng campaign. Biết cov = 0 là click, cov = 1 là view

2/ Xác định tỉ lệ click, view từng campaign theo location

3/ Xác định số lượng người dùng từng campaign. Mỗi guid đại diện cho 1 người dùng duy nhất.

4/ Xác định số user vào nhiều campaign.

Cách chạy chương trình:

- Clone repository từ github:

git clone https://github.com/hungnguyensv259/spark_task_intern

- Trỏ vào thư mục repository và build chương trình nếu trong thư mục repository chưa có file jar:

cd spark_task_intern

mvn clean package

mv target/spark_task_intern-1.0-SNAPSHOT.jar DIRECTORY_TO_REPOSITORY/spark_task_intern

- Trong thư mục repository, chạy class Kafka:

spark-submit --class Kafka --master yarn --deploy-mode cluster --executor-memory 1g --num-executors 3 --executor-cores 3 --packages org.apache.spark:spark-sql kafka-0-10_2.12:3.1.2,org.apache.kafka:kafka-clients:2.8.1 spark_task_intern-1.0-SNAPSHOT.jar

- Chạy job spark với class Tasks:

spark-submit --class Tasks --master yarn --deploy-mode cluster --executor-memory 1g --num-executors 3 --executor-cores 3 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.apache.kafka:kafka-clients:2.8.1 spark_task_intern-1.0-SNAPSHOT.jar

Đảm bảo rằng kafka job chạy trước spark job vài phút để có dữ liệu.

- Dữ liệu được lưu vào ~/spark_task_intern/data trong hdfs, phân vùng theo năm - tháng -ngày.

- Kết quả spark job được lưu vào ~/spark_task_intern/result trong hdfs, phân vùng theo từng câu truy vấn, theo ngày.
