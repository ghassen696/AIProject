from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_unixtime, sum, count, length, hour, collect_list, struct, to_date, lit, min as min_, max as max_, row_number, countDistinct, concat_ws
)
from pyspark.sql.window import Window
from functools import reduce

# ----------------------------
# Spark Init
# ----------------------------
spark = SparkSession.builder \
    .appName("EmployeeActivityBatchEnhanced") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()

# ----------------------------
# Read raw logs from Elasticsearch
# ----------------------------
df = spark.read.format("es") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .option("es.resource", "employee_activity") \
    .load()
df = df.withColumn("timestamp", (col("timestamp") / 1000).cast("double"))
df = df.withColumn("timestamp", from_unixtime(col("timestamp")).cast("timestamp"))
# ----------------------------
# Filter logs by day
# ----------------------------
target_date = "2025-09-15"
df = df.withColumn("date", to_date(col("timestamp"))).filter(col("date") == lit(target_date))

# ----------------------------
# KPI Functions
# ----------------------------
def typing_metrics(df):
    typing_total = df.filter(col("event") == "keystrokes") \
        .withColumn("char_count", length(col("text"))) \
        .groupBy("employee_id") \
        .agg(sum("char_count").alias("total_keystrokes"))

    typing_hourly = df.filter(col("event") == "keystrokes") \
        .withColumn("char_count", length(col("text"))) \
        .groupBy("employee_id", hour(col("timestamp")).alias("hour")) \
        .agg(sum("char_count").alias("chars_per_hour")) \
        .groupBy("employee_id") \
        .agg(collect_list(struct(col("hour"), col("chars_per_hour"))).alias("typing_per_hour"))
    
    return typing_total, typing_hourly

def idle_metrics(df):
    idle_total = df.filter(col("event") == "idle_end") \
        .groupBy("employee_id") \
        .agg(sum("idle_duration_sec").alias("total_idle_sec"))
    
    idle_hourly = df.filter(col("event") == "idle_end") \
        .groupBy("employee_id", hour(col("timestamp")).alias("hour")) \
        .agg(sum("idle_duration_sec").alias("idle_sec_per_hour")) \
        .groupBy("employee_id") \
        .agg(collect_list(struct(col("hour"), col("idle_sec_per_hour"))).alias("idle_per_hour"))
    
    idle_per_app = df.filter(col("event") == "idle_end") \
        .groupBy("employee_id", "application") \
        .agg(sum("idle_duration_sec").alias("idle_sec")) \
        .groupBy("employee_id") \
        .agg(collect_list(struct(col("application"), col("idle_sec"))).alias("idle_per_app"))

    return idle_total, idle_hourly, idle_per_app

def pause_metrics(df):
    pause_total = df.filter(col("event") == "pause") \
        .groupBy("employee_id") \
        .agg(count("*").alias("pause_count"), sum("duration_minutes").alias("pause_total_min"))

    pause_hourly = df.filter(col("event") == "pause") \
        .groupBy("employee_id", hour(col("timestamp")).alias("hour"), col("reason")) \
        .agg(count("*").alias("pause_count"), sum("duration_minutes").alias("pause_total_min")) \
        .groupBy("employee_id") \
        .agg(collect_list(struct(col("hour"), col("reason"), col("pause_count"), col("pause_total_min"))).alias("pause_per_hour"))

    avg_pause_duration = df.filter(col("event") == "pause") \
        .groupBy("employee_id") \
        .agg((sum("duration_minutes")/count("*")).alias("avg_pause_duration_min"))
    
    return pause_total, pause_hourly, avg_pause_duration

def shortcut_metrics(df):
    shortcut_total = df.filter(col("event") == "shortcut") \
        .groupBy("employee_id") \
        .agg(count("*").alias("shortcut_count"),
             collect_list("shortcut_name").alias("shortcuts_used"))
    
    shortcut_hourly = df.filter(col("event") == "shortcut") \
        .groupBy("employee_id", hour(col("timestamp")).alias("hour")) \
        .agg(count("*").alias("shortcuts_per_hour"))
    
    return shortcut_total, shortcut_hourly

def heartbeat_metrics(df):
    heartbeat_count = df.filter(col("event") == "heartbeat") \
        .groupBy("employee_id") \
        .agg(count("*").alias("heartbeat_count"))
    return heartbeat_count

def app_metrics(df):
    active_per_app = df.filter(col("event") == "keystrokes") \
        .groupBy("employee_id", "application") \
        .agg(count("*").alias("keystroke_count")) \
        .groupBy("employee_id") \
        .agg(collect_list(struct(col("application"), col("keystroke_count"))).alias("active_per_app"))

    app_hourly = df.filter(col("application").isNotNull()) \
        .groupBy("employee_id", "application", hour("timestamp").alias("hour")) \
        .agg(count("*").alias("events_per_hour")) \
        .groupBy("employee_id") \
        .agg(collect_list(struct("application", "hour", "events_per_hour")).alias("app_usage_per_hour"))

    return active_per_app, app_hourly

def session_metrics(df):
    session_df = df.groupBy("employee_id", "session_id") \
        .agg(min_("timestamp").alias("session_start"), max_("timestamp").alias("session_end")) \
        .withColumn("session_duration_sec", (col("session_end").cast("long") - col("session_start").cast("long")))
    
    session_idle = df.filter(col("event") == "idle_end") \
        .groupBy("employee_id", "session_id") \
        .agg(sum("idle_duration_sec").alias("idle_sec"))

    session_pause = df.filter(col("event") == "pause") \
        .groupBy("employee_id", "session_id") \
        .agg(sum("duration_minutes").alias("pause_min"))

    session_summary = session_df.join(session_idle, ["employee_id","session_id"], "left") \
        .join(session_pause, ["employee_id","session_id"], "left") \
        .fillna(0, subset=["idle_sec","pause_min"]) \
        .withColumn("active_sec", col("session_duration_sec") - col("idle_sec") - col("pause_min")*60) \
        .groupBy("employee_id") \
        .agg(collect_list(struct("session_id", "session_duration_sec", "active_sec", "idle_sec", "pause_min")).alias("sessions"))

    return session_summary

def window_metrics(df):
    window_counts = df.filter(col("event") == "window_switch") \
        .groupBy("employee_id", "window") \
        .agg(count("*").alias("switch_count"))

    window_rank = window_counts.withColumn("rank", row_number().over(Window.partitionBy("employee_id").orderBy(col("switch_count").desc()))) \
        .filter(col("rank") <= 5) \
        .groupBy("employee_id") \
        .agg(collect_list(struct("window", "switch_count")).alias("top_windows"))

    window_switch_count = df.filter(col("event") == "window_switch") \
        .groupBy("employee_id") \
        .agg(count("*").alias("window_switch_count"))

    return window_rank, window_switch_count

def general_metrics(df):
    # Active vs idle vs pause %
    df_time = df.groupBy("employee_id") \
        .agg(
            sum((col("event") == "idle_end").cast("int")*col("idle_duration_sec")).alias("idle_sec"),
            sum((col("event") == "pause").cast("int")*col("duration_minutes")*60).alias("pause_sec"),
            ((max_("timestamp").cast("long") - min_("timestamp").cast("long"))).alias("total_sec")
        ) \
        .withColumn("active_sec", col("total_sec") - col("idle_sec") - col("pause_sec")) \
        .withColumn("active_pct", (col("active_sec")/col("total_sec")*100)) \
        .withColumn("idle_pct", (col("idle_sec")/col("total_sec")*100)) \
        .withColumn("pause_pct", (col("pause_sec")/col("total_sec")*100))

    # Keystrokes per active hour
    keystrokes_per_active_hour = df.groupBy("employee_id", hour("timestamp").alias("hour")) \
        .agg(
            sum((col("event") == "keystrokes").cast("int")).alias("keystrokes"),
            sum((col("event") != "idle_end").cast("int")).alias("active_events")
        ) \
        .withColumn("keystrokes_per_active_event", col("keystrokes")/col("active_events")) \
        .groupBy("employee_id") \
        .agg(collect_list(struct("hour","keystrokes_per_active_event")).alias("keystrokes_per_active_hour"))

    # Unique applications used per day
    unique_apps = df.filter(col("application").isNotNull()) \
        .groupBy("employee_id") \
        .agg(countDistinct("application").alias("unique_apps_count"))

    # Number of different event types triggered
    event_diversity = df.groupBy("employee_id") \
        .agg(countDistinct("event").alias("distinct_event_types"))

    return df_time, keystrokes_per_active_hour, unique_apps, event_diversity

# ----------------------------
# Call all KPI functions
# ----------------------------
dfs = []
dfs.extend(typing_metrics(df))
dfs.extend(idle_metrics(df))
dfs.extend(pause_metrics(df))
dfs.extend(shortcut_metrics(df))
dfs.append(heartbeat_metrics(df))
dfs.extend(app_metrics(df))
dfs.append(session_metrics(df))
dfs.extend(window_metrics(df))
dfs.extend(general_metrics(df))

# ----------------------------
# Combine all metrics
# ----------------------------
summary_df = reduce(lambda left, right: left.join(right, on="employee_id", how="outer"), dfs)
summary_df = summary_df.withColumn("date", lit(target_date))
summary_df = summary_df.withColumn("doc_id", concat_ws("-", col("employee_id"), col("date")))

# ----------------------------
# Write to Elasticsearch
# ----------------------------
summary_df.write.format("es") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .option("es.resource", "employee_kpi_summary3") \
    .option("es.mapping.id", "doc_id") \
    .mode("append") \
    .save()

print(f"✅ Employee activity KPI summary for {target_date} is DONE and saved to Elasticsearch!")
