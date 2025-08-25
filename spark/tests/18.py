from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

log_schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("event", StringType(), True),
    StructField("window", StringType(), True),
    StructField("application", StringType(), True),
    StructField("control", StringType(), True),
    StructField("text", StringType(), True),
    StructField("employee_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("seq_num", StringType(), True)
])

spark = SparkSession.builder \
    .appName("EmployeeActivityStreaming") \
    .config("spark.sql.streaming.checkpointLocation", "C:/sc/checkpoints") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "logs") \
    .option("startingOffsets", "earliest") \
    .load()

logs_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), log_schema).alias("data")) \
    .select("data.*")

metrics_df = logs_df.withWatermark("timestamp", "2 minutes") \
    .groupBy(
        col("employee_id"),
        window(col("timestamp"), "1 minute"),
        col("application"),
        col("event")
    ).count() \
    .withColumnRenamed("count", "event_count")

es_host = "http://localhost:9200"
es_index = "employee_activity_metrics"

def write_to_es(batch_df, batch_id):
    batch_df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", es_index) \
        .option("es.nodes", "localhost") \
        .mode("append") \
        .save()

query = metrics_df.writeStream \
    .foreachBatch(write_to_es) \
    .outputMode("update") \
    .start()

query.awaitTermination()
