from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, split, when

spark = SparkSession.builder \
    .appName("LogParser") \
    .config("spark.jars", "D:\\TDK_Assignment\\DE_CaseStudy\\JDBC\\ojdbc8.jar") \
    .getOrCreate()


jdbcUrl = "jdbc:oracle:thin:@localhost:1521/XE"
username = "sys as sysdba"
password = "root"
driver = "oracle.jdbc.driver.OracleDriver"


fields = ["ip_address", "user_id", "request_timestamp", "request_method", "requested_resource", "status_code", "object_size", "url", "browser"]

# Read log data
raw_logs_df = spark.read.text("logs/logs/*.log")

# Apply regex to parse the logs
logs_df = raw_logs_df.withColumn("ip_address", regexp_extract("value", r"^(\S+)", 0)) \
    .withColumn("rfc_id", split('value', ' ')[1]) \
    .withColumn("user_id", split('value', ' ')[2])  \
    .withColumn("timestamp", regexp_extract("value", r"\[(\d{2}/\w+/\d{4}:\d{2}:\d{2}:\d{2} \+\d{4})\]", 1)) \
    .withColumn("request_method", regexp_extract("value", r"\"(GET|POST|PUT|DELETE|HEAD)\s", 1)) \
    .withColumn("request_resource", regexp_extract("value", r" (/[^\s]*)\s", 1)) \
    .withColumn("request_protocol", regexp_extract("value", r" (HTTP/\d\.\d)\"", 1)) \
    .withColumn("status_code", regexp_extract("value", r" (\d{3}) ", 1)) \
    .withColumn("object_size", regexp_extract("value", r"\s(\d+|-)\s", 1)) \
    .withColumn("url", regexp_extract("value", r'"(http[s]?://\S+)"', 1)) \
    .withColumn("browser", regexp_extract("value", r'"([^"]+)"$', 1))

logs_df = logs_df.withColumn("object_size", when(col("object_size") == "-", "0").otherwise(col("object_size")))

# print schema
logs_df.printSchema()

# Drop the original 'value' column with raw log string data
final_df = logs_df.drop('value')

table_name = "user_requests"

final_df.write \
  .format("jdbc") \
  .option("url", jdbcUrl) \
  .option("user", username) \
  .option("password", password) \
  .option("driver", driver) \
  .mode("append") \
  .option("dbtable", table_name) \
  .save()

# Cleanup
spark.stop()