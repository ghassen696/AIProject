from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DoubleType

# Initialize Spark
spark = SparkSession.builder \
    .appName("EmployeeActivityToES") \
    .config("spark.sql.streaming.checkpointLocation", "C:/sc/dashboard/checkpoint") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()

# Kafka Config
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "logs_event"

# Define schema
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("event", StringType(), True),
    StructField("window", StringType(), True),
    StructField("application", StringType(), True),
    StructField("control", StringType(), True),
    StructField("text", StringType(), True),
    StructField("employee_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("seq_num", LongType(), True),
    StructField("reason", StringType(), True),
    StructField("idle_id", StringType(), True),
    StructField("duration_minutes", DoubleType(), True),
    StructField("idle_duration_sec", DoubleType(), True)

])

# Read from Kafka
raw_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON messages
df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Optional: filter out noise (example)
clean_df = df.filter(col("employee_id").isNotNull())

# Write to Elasticsearch
query = clean_df.writeStream \
    .format("es") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .option("es.resource", "employee_activity/_doc") \
    .option("checkpointLocation", "C:/sc/dashboard/checkpoint") \
    .outputMode("append") \
    .start()

query.awaitTermination()
