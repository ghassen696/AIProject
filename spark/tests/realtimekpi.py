#roolin not cumulatice
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, sum as sum_, count, avg, collect_set, 
    max as max_, min as min_, when, coalesce,
    unix_timestamp, date_format, current_timestamp,
    regexp_replace, length, lit, concat, greatest, size
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, 
    TimestampType, DoubleType
)
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark
spark = SparkSession.builder \
    .appName("EmployeeKPIRealTime") \
    .config("spark.sql.streaming.checkpointLocation", "/root/AI-PFE/checkpoints/kpi_realtime") \
    .config("spark.es.nodes.wan.only", "true") \
    .config("spark.sql.streaming.trigger.processingTime", "30 seconds") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Kafka
KAFKA_BOOTSTRAP_SERVERS = "193.95.30.190:9092"
KAFKA_TOPIC = "logs_event2"

# Elasticsearch
ES_HOST = "localhost"
ES_PORT = "9200"
KPI_INDEX = "employee_kpi_realtime_v2"

# Schema (renamed window -> window_name)
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("event", StringType(), True),
    StructField("window_name", StringType(), True),
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
    StructField("shortcut_name", StringType(), True),
    StructField("status", StringType(), True)
])

# Read from Kafka
logger.info("Starting Kafka stream reader...")
raw_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON
parsed_df = raw_df.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json(col("json_str"), schema)) \
    .filter(col("data").isNotNull()) \
    .select("data.*")

# Filter valid activity events only
activity_events = ["keystrokes", "window_switch", "clipboard_paste", "shortcut", "pause", "idle_end","heartbeat"]
clean_df = parsed_df.filter(col("employee_id").isNotNull()) \
    .filter(col("event").isin(activity_events)) \
    .withColumn("hour", date_format(col("timestamp"), "yyyy-MM-dd HH")) \
    .withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd")) \
    .withColumn("clean_text", regexp_replace(col("text"), "<Key\\.[^>]+>", "")) \
    .withColumn("text_length", when(col("clean_text").isNotNull(), length(col("clean_text"))).otherwise(0))

# Sliding window grouping
windowed_df = clean_df \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes", "1 minute"),
        col("employee_id")
    )

# KPI Aggregates
kpi_df = windowed_df.agg(
    count("*").alias("total_events"),
    count(when(col("event") == "keystrokes", 1)).alias("keystroke_events"),
    count(when(col("event") == "window_switch", 1)).alias("window_switches"),
    count(when(col("event") == "clipboard_paste", 1)).alias("clipboard_operations"),
    count(when(col("event") == "shortcut", 1)).alias("shortcut_usage"),
    count(when(col("event") == "pause", 1)).alias("pause_count"),
    count(when(col("event") == "heartbeat", 1)).alias("heartbeat_count"),
    sum_(when(col("event") == "pause", col("duration_minutes")).otherwise(0)).alias("total_pause_minutes"),
    count(when(col("event") == "idle_end", 1)).alias("idle_periods"),
    sum_(when(col("event") == "idle_end", coalesce(col("idle_duration_sec"), lit(0))).otherwise(0)).alias("total_idle_seconds"),
    collect_set(when(col("application").isNotNull(), col("application"))).alias("applications_used"),
    collect_set(when(col("window_name").isNotNull(), col("window_name"))).alias("windows_accessed"),
    sum_(col("text_length")).alias("total_characters_typed"),
    avg(when(col("text_length") > 0, col("text_length"))).alias("avg_text_length"),
    min_(col("timestamp")).alias("window_start"),
    max_(col("timestamp")).alias("window_end"),
    collect_set(col("event")).alias("event_types")
).select(
    col("window.start").alias("kpi_window_start"),
    col("window.end").alias("kpi_window_end"),
    col("employee_id"),
    col("total_events"),
    col("keystroke_events"),
    col("window_switches"),
    col("clipboard_operations"),
    col("shortcut_usage"),
    col("pause_count"),
    col("total_pause_minutes"),
    col("idle_periods"),
    col("total_idle_seconds"),
    col("heartbeat_count"),
    size(col("applications_used")).alias("unique_applications"),
    size(col("windows_accessed")).alias("unique_windows"),
    col("total_characters_typed"),
    col("avg_text_length"),
    col("window_start"),
    col("window_end"),
    ((unix_timestamp(col("window_end")) - unix_timestamp(col("window_start"))) / 60.0).alias("window_duration_minutes"),
    (col("keystroke_events").cast("double") / 
     greatest(((unix_timestamp(col("window_end")) - unix_timestamp(col("window_start"))) / 60.0), lit(0.001))).alias("keystrokes_per_minute"),
    (col("window_switches").cast("double") / 
     greatest(((unix_timestamp(col("window_end")) - unix_timestamp(col("window_start"))) / 60.0), lit(0.001))).alias("window_switches_per_minute"),
    (((unix_timestamp(col("window_end")) - unix_timestamp(col("window_start"))) / 60.0) - 
     col("total_pause_minutes") - (col("total_idle_seconds") / 60.0)).alias("active_minutes"),
    current_timestamp().alias("calculated_at")
)

# Prepare output
output_df = kpi_df.select(
    col("employee_id"),
    col("kpi_window_start"),
    col("kpi_window_end"),
    col("window_duration_minutes"),
    col("active_minutes"),
    col("total_events"),
    col("keystroke_events"),
    col("keystrokes_per_minute"),
    col("window_switches"),
    col("clipboard_operations"),
    col("shortcut_usage"),
    col("pause_count"),
    col("total_pause_minutes"),
    col("idle_periods"),
    col("total_idle_seconds"),
    col("unique_applications"),
    col("total_characters_typed"),
    col("calculated_at"),
    concat(col("employee_id"), lit("_"), date_format(col("kpi_window_start"), "yyyyMMddHHmmss")).alias("doc_id"),
        when(col("heartbeat_count") == 0, lit("offline"))
    .when(col("keystroke_events") > 0, lit("active"))
    .otherwise(lit("idle")).alias("employee_status")

)

# Write to Elasticsearch
logger.info("Starting KPI stream to Elasticsearch...")
kpi_query = output_df.writeStream \
    .format("es") \
    .option("es.nodes", ES_HOST) \
    .option("es.port", ES_PORT) \
    .option("es.resource", KPI_INDEX) \
    .option("es.mapping.id", "doc_id") \
    .option("es.batch.size.entries", "1000") \
    .option("checkpointLocation", "/root/AI-PFE/checkpoints/kpi_stream") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    logger.info("Stopping streams...")
    kpi_query.stop()
    spark.stop()
    logger.info("Streams stopped successfully.")
