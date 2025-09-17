from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, when, count, sum as sum_, max as max_, collect_list,
    to_date, concat, lit, current_timestamp, array_contains
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType, DoubleType
)
from pyspark.sql.window import Window
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------- Spark Initialization -------------------
spark = SparkSession.builder \
    .appName("EmployeeKPIRealTime_Daily") \
    .config("spark.sql.streaming.checkpointLocation", "/root/AI-PFE/checkpoints/kpi_daily") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ------------------- Kafka Configuration -------------------
KAFKA_BOOTSTRAP_SERVERS = "193.95.30.190:9092"
KAFKA_TOPIC = "logs_event2"

# ------------------- Elasticsearch Configuration -------------------
ES_HOST = "localhost"
ES_PORT = "9200"
KPI_INDEX = "employee_kpi_daily_v1"

# ------------------- Schema Definition -------------------
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

# ------------------- Read Kafka Stream -------------------
logger.info("Starting Kafka stream reader...")
raw_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON messages
parsed_df = raw_df.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json(col("json_str"), schema)) \
    .filter(col("data").isNotNull()) \
    .select("data.*") \
    .filter(col("employee_id").isNotNull())

# ------------------- Filter Valid Activity Events -------------------
activity_events = ["keystrokes", "window_switch", "clipboard_paste", "shortcut", "pause", "idle_end", "heartbeat"]
clean_df = parsed_df.filter(col("event").isin(activity_events))

# ------------------- Daily Aggregation -------------------
# Add 'date' column for daily tumbling
daily_df = clean_df.withColumn("date", to_date("timestamp"))

# Window to get latest event per employee (for current state and last idle)
latest_event_window = Window.partitionBy("employee_id").orderBy(col("timestamp").desc())

with_latest = daily_df.withColumn("row_num", when(col("employee_id").isNotNull(), 1).otherwise(0)) \
    .withColumn("last_idle_duration", when(
        (col("event") == "idle_end"), col("idle_duration_sec")
    ))

# Group by employee and date to compute daily cumulative KPIs
daily_agg = daily_df.groupBy("employee_id", "date").agg(
    sum_(when(col("event") == "idle_end", col("idle_duration_sec")).otherwise(0)).alias("total_idle_today"),
    count(when(col("event") == "pause", 1)).alias("pauses_today"),
    sum_(when(col("event") == "pause", col("duration_minutes")).otherwise(0)).alias("total_pause_minutes_today"),
    count(when(col("event") == "keystrokes", 1)).alias("keystrokes_today"),
    max_(when(col("event") == "idle_end", col("idle_duration_sec"))).alias("last_idle_duration"),
    max_("timestamp").alias("last_event_time"),
    collect_list("event").alias("events_today")
)

# Determine employee current state
daily_agg = daily_agg.withColumn("employee_status",
    when(array_contains(col("events_today"), "keystrokes"), lit("active"))
    .when(array_contains(col("events_today"), "idle_end"), lit("idle"))
    .otherwise(lit("offline"))
)

# Create unique doc_id per employee per day
daily_agg = daily_agg.withColumn("doc_id",
    concat(col("employee_id"), lit("_"), col("date").cast("string"))
)

# ------------------- Write Stream to Elasticsearch -------------------
logger.info("Starting daily KPI stream to Elasticsearch...")
daily_query = daily_agg.writeStream \
    .format("es") \
    .option("es.nodes", ES_HOST) \
    .option("es.port", ES_PORT) \
    .option("es.resource", KPI_INDEX) \
    .option("es.mapping.id", "doc_id") \
    .outputMode("update") \
    .trigger(processingTime="30 seconds") \
    .start()

# ------------------- Await Termination -------------------
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    logger.info("Stopping streams...")
    daily_query.stop()
    spark.stop()
    logger.info("Streams stopped successfully.")
