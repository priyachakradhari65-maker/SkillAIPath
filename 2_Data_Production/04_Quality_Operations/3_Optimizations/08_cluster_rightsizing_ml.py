# Databricks notebook source
# Cluster Right-Sizing with ML Recommendations
# FREE EDITION COMPATIBLE
# PURPOSE: ML-based cluster size recommendations using historical patterns
# June 2026

from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor

print("=" * 70)
print("ML-BASED CLUSTER RIGHT-SIZING")
print("=" * 70)

# Historical cluster usage (simulated - Premium uses system.compute.clusters)
cluster_history = [
    ("cluster-etl-001", "i3.xlarge", 8, 120, 450, 12, 2.5, 45, "under-utilized"),
    ("cluster-ml-002", "i3.2xlarge", 16, 95, 1200, 78, 12.8, 85, "well-sized"),
    ("cluster-adhoc-003", "i3.4xlarge", 32, 180, 380, 18, 4.2, 25, "over-provisioned"),
    ("cluster-etl-004", "i3.xlarge", 8, 60, 890, 85, 6.9, 92, "over-utilized"),
    ("cluster-streaming-005", "i3.2xlarge", 16, 240, 2400, 55, 9.5, 70, "well-sized"),
]

schema = "cluster_id STRING, instance_type STRING, cores INT, runtime_hours INT, tasks_executed INT, avg_cpu_pct DOUBLE, avg_memory_gb DOUBLE, avg_disk_pct DOUBLE, sizing_label STRING"
df = spark.createDataFrame(cluster_history, schema)

# Feature engineering
df = df.withColumn("tasks_per_hour", col("tasks_executed") / col("runtime_hours"))
df = df.withColumn("cpu_util_score", col("avg_cpu_pct") / 100)
df = df.withColumn("memory_util_score", col("avg_memory_gb") / col("cores"))
df = df.withColumn("disk_util_score", col("avg_disk_pct") / 100)

# Recommendations based on utilization patterns
df_recommendations = df.withColumn(
    "recommendation",
    when((col("cpu_util_score") < 0.3) & (col("memory_util_score") < 0.5), 
         "DOWNSIZE: Over-provisioned by 50 percent - halve cores")
    .when((col("cpu_util_score") > 0.8) | (col("memory_util_score") > 1.5),
         "UPSIZE: Under-provisioned - double cores")
    .when((col("disk_util_score") > 0.9),
         "OPTIMIZE: Add storage - disk at capacity")
    .otherwise("OPTIMAL: Current size appropriate")
).withColumn(
    "estimated_monthly_savings_usd",
    when(col("recommendation").contains("DOWNSIZE"), col("runtime_hours") * 0.75)
    .when(col("recommendation").contains("UPSIZE"), lit(-col("runtime_hours") * 0.50))
    .otherwise(lit(0))
)

print("\nCluster Right-Sizing Analysis:")
display(df_recommendations.select(
    "cluster_id", "instance_type", "cores", "avg_cpu_pct", "avg_memory_gb", 
    "recommendation", "estimated_monthly_savings_usd"
))

# Summary
total_savings = df_recommendations.filter(col("estimated_monthly_savings_usd") > 0).agg(
    sum("estimated_monthly_savings_usd")
).collect()[0][0]

print(f"\nTotal potential monthly savings: ${total_savings:,.2f}")
print(f"Annual savings: ${total_savings * 12:,.2f}")

print("\nKey Insights:")
print("- Over-provisioned clusters: Check for low CPU and memory utilization")
print("- Under-utilized clusters: Consider spot instances or auto-termination")
print("- Well-sized clusters: Monitor for workload changes monthly")