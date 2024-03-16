from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, split, when, sha2, concat_ws
import os
from dotenv import load_dotenv
import yaml

def load_config(path):
    """Load YAML configuration file.

    Args:
    path (str): Path to the YAML configuration file.

    Returns:
    dict: Dictionary containing the loaded configuration data or None if failed.
    """
    try:
        with open(path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"Error loading config: {e}")
        return None

def get_spark_session(app_name, jars):
    """Initialize and get a Spark session.

    Args:
    app_name (str): Application name for the Spark session.
    jars (str): Comma-separated string of jar packages to include with Spark.

    Returns:
    SparkSession: Initialized Spark session object or None if failed.
    """
    try:
        return SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars", jars) \
            .getOrCreate()
    except Exception as e:
        print(f"Error initializing Spark session: {e}")
        return None

def parse_logs(raw_logs_df):
    """Parse the raw log data from DataFrame.

    Args:
    raw_logs_df (DataFrame): DataFrame containing raw log strings.

    Returns:
    DataFrame: Parsed DataFrame with log details as columns or None if failed.
    """
    try:
        return raw_logs_df.withColumn("ip_address", regexp_extract("value", r"^(\S+)", 0)) \
            .withColumn("rfc_id", split('value', ' ')[1]) \
            .withColumn("user_id", split('value', ' ')[2])  \
            .withColumn("timestamp", regexp_extract("value", r"\[(\d{2}/\w+/\d{4}:\d{2}:\d{2}:\d{2} \+\d{4})\]", 1)) \
            .withColumn("request_method", regexp_extract("value", r"\"(GET|POST|PUT|DELETE|HEAD)\s", 1)) \
            .withColumn("request_resource", regexp_extract("value", r" (/[^\s]*)\s", 1)) \
            .withColumn("request_protocol", regexp_extract("value", r" (HTTP/\d\.\d)\"", 1)) \
            .withColumn("status_code", regexp_extract("value", r" (\d{3}) ", 1)) \
            .withColumn("object_size", regexp_extract("value", r"\s(\d+|-)\s", 1)) \
            .withColumn("url", regexp_extract("value", r'"(http[s]?://\S+)"', 1)) \
            .withColumn("browser", regexp_extract("value", r'"([^"]+)"', 1)) \
            .withColumn("user_time_key", sha2(concat_ws("_", "user_id", "timestamp","request_resource"), 256)) \
            .withColumn("object_size", when(col("object_size") == "-", "0").otherwise(col("object_size")))
    except Exception as e:
        print(f"Error parsing logs: {e}")
        return None

def write_to_db(df, jdbc_url, table_name, user, password, driver):
    """Write the DataFrame to the specified database.

    Args:
    df (DataFrame): DataFrame containing the data to be written.
    jdbc_url (str): JDBC URL to the database.
    table_name (str): Name of the database table.
    user (str): Username for the database.
    password (str): Password for the database.
    driver (str): Database driver.

    Raises:
    Exception: If there's any issue writing to the database.
    """
    try:
        df.write \
          .format("jdbc") \
          .option("url", jdbc_url) \
          .option("user", user) \
          .option("password", password) \
          .option("driver", driver) \
          .mode("append") \
          .option("dbtable", table_name) \
          .save()
    except Exception as e:
        print(f"Error writing to DB: {e}")

load_dotenv()
config = load_config('config.yml')
if config:
    spark = get_spark_session(config['spark']['app_name'], config['spark']['jars'])
    if spark:
        raw_logs_df = spark.read.text("logs/logs/*.log")
        logs_df = parse_logs(raw_logs_df)
        if logs_df:
            logs_df = logs_df.dropDuplicates(['user_time_key'])
            logs_df.printSchema()
            logs_df = logs_df.drop('value')
            write_to_db(logs_df, config['database']['jdbc_url'],
                        config['table_name'],
                        config['database']['user'],
                        os.getenv("DB_PASSWORD"),
                        config['database']['driver'])
        spark.stop()