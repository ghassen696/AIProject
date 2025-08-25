from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define schema (same as yours)
schema = StructType() \
    .add("timestamp", TimestampType()) \
    .add("event", StringType()) \
    .add("window", StringType()) \
    .add("control", StringType()) \
    .add("text", StringType()) \
    .add("employee_id", StringType())

spark = SparkSession.builder \
    .appName("SparkStreamingAggregations") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1,"
            "org.elasticsearch:elasticsearch-spark-30_2.13:9.0.0") \
    .config("spark.es.nodes.wan.only", "true") \
    .config("spark.local.dir", "C:/spark_temp") \
    .getOrCreate()

# Read raw data from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "employee-activity-logs") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON and extract fields
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

df_parsed_with_watermark = df_parsed.withWatermark("timestamp", "10 seconds")

# Add extra columns for metrics
df_metrics = df_parsed_with_watermark \
    .withColumn(
    "application",
    when(col("window").contains(" - "),
          split(col("window"), " - ").getItem(size(split(col("window"), " - ")) - 1))  # last element
    .when(col("control").isNotNull() & (length(col("control")) > 0),
          col("control"))  # fallback to control (e.g., "Command Prompt")
    .when(col("window").contains("@"),
          lit("Terminal"))  # identify Linux/macOS terminal window
    .otherwise(lit("Unknown"))
    ) \
    .withColumn("text_length", length(coalesce(col("text"), lit("")))) \
    .withColumn("is_idle", when(col("text") == "", 1).otherwise(0))

# 1-min summary per employee
summary_df = df_metrics.groupBy(
    col("employee_id"),
    window("timestamp", "1 minute").alias("time_window")
).agg(
    count("*").alias("total_events"),
    count(when(col("event") == "keystrokes", True)).alias("keystroke_events"),
    sum("text_length").alias("total_characters"),
    sum("is_idle").alias("idle_events"),
    first("application").alias("application")
).select(
    col("employee_id"),
    col("application"),
    col("time_window.start").alias("window_start"),
    col("time_window.end").alias("window_end"),
    "total_events",
    "keystroke_events",
    "total_characters",
    "idle_events"
)

# Keystrokes per 5-min window
keystroke_agg = df_metrics.filter(col("event") == "keystrokes") \
    .groupBy(
        col("employee_id"),
        window("timestamp", "5 minutes").alias("time_window")
    ).agg(
        count("event").alias("keystroke_count")
    ).select(
        col("employee_id"),
        col("time_window.start").alias("window_start"),
        col("time_window.end").alias("window_end"),
        "keystroke_count"
    )

# Frequent apps usage per 10-min window
frequent_apps_df = df_metrics.groupBy(
    window("timestamp", "10 minutes").alias("time_window"),
    "employee_id",
    "application"
).agg(
    count("*").alias("usage_count")
).select(
    col("employee_id"),
    col("application"),
    col("time_window.start").alias("window_start"),
    col("time_window.end").alias("window_end"),
    "usage_count"
)

# Elasticsearch options
ES_OPTIONS = {
    "es.nodes": "localhost",
    "es.port": "9200",
    "es.nodes.wan.only": "true",
    "es.index.auto.create": "false"
}

# Write raw parsed logs to ES
raw_query = df_metrics.writeStream \
    .format("es") \
    .option("checkpointLocation", "C:/tmp/checkpoints/raw") \
    .option("es.resource", "employee-activity-logs/_doc") \
    .options(**ES_OPTIONS) \
    .outputMode("append") \
    .start()

# Write summary to ES
summary_query = summary_df.writeStream \
    .format("es") \
    .option("checkpointLocation", "C:/tmp/checkpoints/summary") \
    .option("es.resource", "employee-activity-summary/_doc") \
    .options(**ES_OPTIONS) \
    .outputMode("append") \
    .start()

# Write keystroke agg to ES
keystroke_query = keystroke_agg.writeStream \
    .format("es") \
    .option("checkpointLocation", "C:/tmp/checkpoints/keystrokes") \
    .option("es.resource", "employee-activity-keystrokes/_doc") \
    .options(**ES_OPTIONS) \
    .outputMode("append") \
    .start()

# Write frequent app usage to ES
frequent_apps_query = frequent_apps_df.writeStream \
    .format("es") \
    .option("checkpointLocation", "C:/tmp/checkpoints/frequent-apps") \
    .option("es.resource", "employee-frequent-apps/_doc") \
    .options(**ES_OPTIONS) \
    .outputMode("append") \
    .start()

# Await termination for all queries
spark.streams.awaitAnyTermination()
