from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

# Initialize Spark
spark = SparkSession.builder \
    .appName("IdleTimeCalculator") \
    .config("spark.sql.streaming.checkpointLocation", "C:/sc/idle_summary/checkpoint") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()

# Kafka input
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "employee-activity-logs"

# Define schema
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("event", StringType(), True),
    StructField("application", StringType(), True),
    StructField("control", StringType(), True),
    StructField("text", StringType(), True),
    StructField("employee_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("seq_num", LongType(), True)
])

# Read from Kafka
raw_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON
df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Split streams
idle_start = df.filter(col("event") == "idle_start") \
    .withWatermark("timestamp", "10 minute") \
    .withColumnRenamed("timestamp", "start_time")\
    .select("employee_id", "session_id", "seq_num", "start_time")

idle_end = df.filter(col("event") == "idle_end") \
    .withWatermark("timestamp", "10 minute") \
    .withColumnRenamed("timestamp", "end_time")\
    .select("employee_id", "session_id", "seq_num", "end_time")

# Join streams
idle_duration = idle_start.join(
    idle_end,
    on=["employee_id", "session_id","seq_num"],
    how="inner"
).filter(col("end_time") > col("start_time")) \
 .select(
    col("employee_id"),
    col("session_id"),
    col("start_time"),
    col("end_time"),
    (col("end_time").cast("long") - col("start_time").cast("long")).alias("idle_seconds")
)

# Output to console (for testing)
query = idle_duration.writeStream \
.format("es") \
.option("es.nodes", "localhost") \
.option("es.port", "9200") \
.option("es.resource", "employee_idle/_doc")\
.option("checkpointLocation", "C:/sc/idle_summary/checkpoint") \
.outputMode("append") \
.start()

query.awaitTermination(timeout=60000)
