from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# --------------------- Spark / deps ---------------------
spark = SparkSession.builder \
    .appName("Keylogger-RT-and-Daily") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1,"
            "org.elasticsearch:elasticsearch-spark-30_2.13:9.0.0") \
    .config("spark.es.nodes.wan.only", "true") \
    .config("spark.local.dir", "C:/spark_temp") \
    .getOrCreate()

BROKER = "localhost:9092"         
ES_HOST = "localhost"              
CHKPT_BASE = "C:/spark_temp/" 

# --------------------- Schema ---------------------
schema = StructType([
    StructField("timestamp", TimestampType()),
    StructField("event", StringType()),
    StructField("window", StringType()),
    StructField("application", StringType()),
    StructField("control", StringType()),
    StructField("text", StringType()),
    StructField("employee_id", StringType()),
    StructField("session_id", StringType()),
    StructField("seq_num", IntegerType())
])

# --------------------- Kafka source ---------------------
raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", BROKER) \
    .option("subscribe", "employee-activity-logs") \
    .option("startingOffsets", "earliest") \
    .load()

parsed = raw.select(from_json(col("value").cast("string"), schema).alias("d")).select("d.*")

# add helpers
events = parsed \
    .withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))\
    .withColumn("text_len", length(coalesce(col("text"), lit("")))) \
    .withWatermark("timestamp", "10 minutes")

# --------------------- Sink A: Daily history (append) ---------------------
# keep everything for historical analytics
daily_out = events.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", ES_HOST) \
    .option("es.port", "9200") \
    .option("es.resource", "keylogger_events/_doc") \
    .option("checkpointLocation", f"{CHKPT_BASE}/daily_events") \
    .outputMode("append") \
    .start()

# --------------------- Real-time KPIs (upsert) ---------------------
# Typing speed: chars per minute per employee (1-min windows)
keystrokes = events.filter(col("event") == "keystrokes")

typing_speed = keystrokes.groupBy(
    window(col("timestamp"), "1 minute"),
    col("employee_id")
).agg(
    sum("text_len").alias("chars_per_minute")
).select(
    col("employee_id"),
    col("chars_per_minute"),
    col("window.start").cast("timestamp").alias("window_start"),
    col("window.end").cast("timestamp").alias("window_end"),
    # deterministic id per employee+window -> upsert
    concat(col("employee_id"), lit("_"), date_format(col("window.start"), "yyyy-MM-dd-HH-mm")).alias("doc_id")
)

# Approx app usage: count events per app per minute (proxy for presence)
app_events = events.withColumn(
    "app_key",
    when(
        col("application").isNotNull() & (length(col("application")) > 0),
        col("application")
    ).when(
        col("window").isNotNull() & (instr(col("window"), "-") > 0),
        element_at(split(col("window"), "-"), -1)
    ).otherwise(lit("Unknown"))
)


app_usage = app_events.groupBy(
    window(col("timestamp"), "1 minute"),
    col("employee_id"),
    col("app_key")
).agg(
    count(lit(1)).alias("events_in_window")
).select(
    col("employee_id"),
    col("app_key").alias("application"),
    col("events_in_window"),
    col("window.start").cast("timestamp").alias("window_start"),
    col("window.end").cast("timestamp").alias("window_end"),
    concat(col("employee_id"), lit("_"), col("app_key"), lit("_"),
           date_format(col("window.start"), "yyyy-MM-dd-HH-mm")).alias("doc_id")
)

ES_RT = {
    "es.nodes": ES_HOST,
    "es.port": "9200",
    "es.resource": "keylogger_rt/_doc",        
    "es.write.operation": "upsert",
    "es.mapping.id": "doc_id"
}

rt_typing = typing_speed.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .options(**ES_RT) \
    .option("checkpointLocation", f"{CHKPT_BASE}/rt_typing") \
    .outputMode("update") \
    .trigger(processingTime="1 minute") \
    .start()

rt_apps = app_usage.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .options(**ES_RT) \
    .option("checkpointLocation", f"{CHKPT_BASE}/rt_apps") \
    .outputMode("update") \
    .trigger(processingTime="1 minute") \
    .start()
try:
    daily_out.awaitTermination()
except Exception as e:
    print("Daily history3 stream stopped:", e)

try:
    rt_typing.awaitTermination()
except Exception as e:
    print("Typing speed3 stream stopped:", e)

try:
    rt_apps.awaitTermination()
except Exception as e:
    print("App usage3 stream stopped:", e)
#spark.streams.awaitAnyTermination()
