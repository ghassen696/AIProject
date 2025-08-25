from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, min, max, unix_timestamp, to_timestamp
from pyspark.sql.types import *

# Step 1: Create Spark session with ES configs
spark = SparkSession.builder \
    .appName("IdleSessionAggregator") \
    .config("spark.es.nodes", "localhost") \
    .config("spark.es.port", "9200") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()

# Step 2: Define schema (if needed — you can skip this if ES infers types well)
schema = StructType([
    StructField("timestamp", StringType()),  # we'll cast it later
    StructField("event", StringType()),
    StructField("employee_id", StringType()),
    StructField("session_id", StringType()),
])

# Step 3: Read raw events from Elasticsearch
df = spark.read \
    .format("es") \
    .load("employee-activity-logs") \
    .select("timestamp", "event", "employee_id", "session_id") \
    .filter(col("event").isin("idle_start", "idle_end")) \
    .filter(col("employee_id").isNotNull() & col("session_id").isNotNull())

# Step 4: Convert timestamp string to actual timestamp type
df = df.withColumn("timestamp", to_timestamp("timestamp"))

# Step 5: Group by employee & session to extract idle_start and idle_end
paired_df = df.groupBy("employee_id", "session_id") \
    .agg(
        min(when(col("event") == "idle_start", col("timestamp"))).alias("idle_start"),
        max(when(col("event") == "idle_end", col("timestamp"))).alias("idle_end")
    )

# Step 6: Compute duration in seconds
paired_df = paired_df.withColumn("duration", unix_timestamp("idle_end") - unix_timestamp("idle_start"))

# Optional: Filter out invalid/negative durations
paired_df = paired_df.filter(col("duration") > 0)

# Step 7: Write results to Elasticsearch index
paired_df.write \
    .format("es") \
    .option("es.resource", "employee-idle-sessions") \
    .mode("append") \
    .save()

print("✅ Idle session durations successfully written to Elasticsearch.")

spark.stop()
