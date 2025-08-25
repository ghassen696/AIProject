from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum, length, expr, concat, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KeyloggerAnalytics") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.local.dir", "C:/spark_temp") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.12.0") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()

# Define schema for JSON data
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

# Read from Kafka mbaed tethat latest not earliest
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "employee-activity-logs") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON from Kafka value
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")
"""
# Write raw events to Elasticsearch
raw_query = parsed_df \
    .writeStream \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .option("es.resource", "keylogger_events/_doc") \
    .option("es.write.operation", "index") \
    .option("checkpointLocation", "C:/spark_temp/checkpoints/raw") \
    .outputMode("append") \
    .start()


# Compute typing speed (characters per minute)
keystrokes_df = parsed_df.filter(col("event") == "keystrokes") \
    .withWatermark("timestamp", "10 minutes")
typing_speed_df = keystrokes_df \
    .withColumn("text_length", length(col("text"))) \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("employee_id")
    ) \
    .agg(sum("text_length").alias("chars_per_minute")) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("employee_id"),
        col("chars_per_minute"),
        concat(col("employee_id"), lit("_"), col("window.start").cast("string")).alias("doc_id")
    )

# Compute application usage (count switches per minute as a proxy for usage)
window_switches_df = parsed_df.filter(col("event") == "window_switch") \
    .withWatermark("timestamp", "10 minutes")
app_usage_df = window_switches_df \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("employee_id"),
        col("application")
    ) \
    .agg(sum(lit(60)).alias("total_seconds")) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("employee_id"),
        col("application"),
        col("total_seconds"),
        concat(col("employee_id"), lit("_"), col("application"), lit("_"), col("window.start").cast("string")).alias("doc_id")
    )
"""


# Compute idle time
idle_starts_df = parsed_df.filter(col("event") == "idle_start") \
    .withWatermark("timestamp", "10 minutes") \
    .select(col("timestamp").alias("idle_start"), col("employee_id"), col("session_id"))

idle_ends_df = parsed_df.filter(col("event") == "idle_end") \
    .withWatermark("timestamp", "10 minutes") \
    .select(col("timestamp").alias("idle_end"), col("employee_id"), col("session_id"))
idle_starts_df.printSchema()  # To see the schema and data types
idle_ends_df.printSchema()  # To see the schema and data types

query = idle_starts_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination(timeout=20)  # run for 30 seconds (or any seconds you want)
query.stop()
query = idle_ends_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination(timeout=20)  # run for 30 seconds (or any seconds you want)
query.stop()
"""
idle_df = idle_starts_df.join(idle_ends_df, ["employee_id"]) \
    .filter(col("idle_start") < col("idle_end")) \
    .withColumn("idle_duration_seconds", 
                expr("unix_timestamp(idle_end) - unix_timestamp(idle_start)")) \
    .groupBy(
        window(col("idle_start"), "1 minute"),
        col("employee_id")
    ) \
    .agg(sum("idle_duration_seconds").alias("idle_seconds")) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("employee_id"),
        col("idle_seconds"),
        concat(col("employee_id"), lit("_"), col("window.start").cast("string")).alias("doc_id")
    )
"""
from pyspark.sql.functions import explode, sequence, to_timestamp, unix_timestamp, expr, floor


# Step 1: Join idle_start and idle_end (already done)
idle_intervals_df = idle_starts_df.join(
    idle_ends_df,
    on=["employee_id", "session_id"]
).filter(col("idle_start") < col("idle_end"))


               
# Step 2: Generate hourly window boundaries between idle_start and idle_end
# Round down start and end timestamps to the nearest hour
idle_intervals_df = idle_intervals_df \
    .withColumn("start_hour", expr("timestamp_seconds(floor(unix_timestamp(idle_start)/3600)*3600)")) \
    .withColumn("end_hour", expr("timestamp_seconds(floor(unix_timestamp(idle_end)/3600)*3600)")) \
    .filter(col("start_hour") <= col("end_hour")) \
    .withColumn("hours_seq", sequence(col("start_hour"), col("end_hour"), expr("interval 1 hour")))


# Step 3: Explode the hours sequence to get one row per hour window intersecting the idle interval
exploded_df = idle_intervals_df.withColumn("hour_window_start", explode(col("hours_seq")))

# Step 4: Calculate overlap per hour window
# Calculate window end as hour_window_start + 1 hour
exploded_df = exploded_df.withColumn("hour_window_end", expr("hour_window_start + interval 1 hour"))

# Calculate overlap start: max(idle_start, hour_window_start)
exploded_df = exploded_df.withColumn("overlap_start", expr("greatest(idle_start, hour_window_start)"))
# Calculate overlap end: min(idle_end, hour_window_end)
exploded_df = exploded_df.withColumn("overlap_end", expr("least(idle_end, hour_window_end)"))

# Calculate overlap seconds (duration in seconds)
exploded_df = exploded_df.withColumn(
    "overlap_seconds",
    (unix_timestamp(col("overlap_end")) - unix_timestamp(col("overlap_start"))).cast("long")
)
exploded_df = exploded_df.withColumn(
    "window", 
    window(col("hour_window_start"), "1 hour")
)
exploded_df_with_watermark = exploded_df.withWatermark("hour_window_start", "10 minutes")

# Step 5: Aggregate overlap seconds per employee and hour window
idle_per_hour_df = exploded_df_with_watermark.groupBy(
    col("employee_id"),
    col("session_id"),
    col("hour_window_start"),
    col("hour_window_end")
).agg(
    sum("overlap_seconds").alias("idle_seconds")
)

# Step 6: Prepare for writing to Elasticsearch
idle_per_hour_df = idle_per_hour_df.select(
    col("hour_window_start").alias("window_start"),
    col("hour_window_end").alias("window_end"),
    col("employee_id"),
    col("session_id"),
    col("idle_seconds"),
    ((col("idle_seconds") / 3600) * 100).alias("idle_percentage"),
    concat(col("employee_id"), lit("_"), col("session_id"), lit("_"), col("hour_window_start").cast("string")).alias("doc_id")
)

"""
# Write metrics to Elasticsearch with upsert
typing_speed_query = typing_speed_df \
    .writeStream \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .option("es.resource", "keylogger_metrics/_doc") \
    .option("es.write.operation", "upsert") \
    .option("es.mapping.id", "doc_id") \
    .option("checkpointLocation", "C:/spark_temp/checkpoints/typing_speed") \
    .outputMode("update") \
    .trigger(processingTime="1 minute") \
    .start()

app_usage_query = app_usage_df \
    .writeStream \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .option("es.resource", "keylogger_metrics/_doc") \
    .option("es.write.operation", "upsert") \
    .option("es.mapping.id", "doc_id") \
    .option("checkpointLocation", "C:/spark_temp/checkpoints/app_usage") \
    .outputMode("update") \
    .trigger(processingTime="1 minute") \
    .start()
"""
"""idle_query = idle_per_hour_df \
    .writeStream \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .option("es.resource", "employee-idle-metrics/_doc") \
    .option("es.write.operation", "upsert") \
    .option("es.mapping.id", "doc_id") \
    .option("checkpointLocation", "C:/spark_temp/checkpoints/idle_time") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start()"""

# Await termination

spark.streams.awaitAnyTermination()