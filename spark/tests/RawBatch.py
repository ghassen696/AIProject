from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum, count, avg, collect_set, max as max_
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DoubleType

# Initialize Spark
spark = SparkSession.builder \
    .appName("EmployeeActivityToES") \
    .config("spark.sql.streaming.checkpointLocation", "/root/AI-PFE/checkpoints/idle_summary") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()

# Kafka Config
kafka_bootstrap_servers = "193.95.30.190:9092"
kafka_topic = "logs_event2"

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
    StructField("idle_duration_sec", DoubleType(), True),
    StructField("shortcut_name", StringType(), True),       # for shortcuts
    StructField("status", StringType(), True)  
])

# Read from Kafka
raw_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON messages
df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Clean logs (remove noise)
clean_df = df.filter(col("employee_id").isNotNull())

# ----------------------------
# 1. Write RAW logs → ES
# ----------------------------
raw_query = clean_df.writeStream \
    .format("es") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .option("es.resource", "employee_activity") \
    .option("checkpointLocation", "/root/AI-PFE/checkpoints/raw_activity") \
    .outputMode("append") \
    .start()


# Wait for both queries
spark.streams.awaitAnyTermination()
