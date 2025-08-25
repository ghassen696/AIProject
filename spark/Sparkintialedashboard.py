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


# Await termination

spark.streams.awaitAnyTermination()