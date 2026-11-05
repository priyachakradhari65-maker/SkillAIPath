# Databricks notebook source
# SQL Warehouse Cost Optimization
# FREE EDITION COMPATIBLE - Uses simulated data
# PURPOSE: Analyze SQL Warehouse costs and right-sizing recommendations
# June 2026

from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring
from datetime import datetime, timedelta

print("=" * 70)
print("SQL WAREHOUSE COST OPTIMIZATION")
print("=" * 70)

# Warehouse usage simulation (Premium Edition uses system.query.history)
warehouse_data = [
    ("wh-analytics-xl", "X-Large", 2400, 180, 3.2, 12, "Downsize to Large"),
    ("wh-dashboards-m", "Medium", 800, 95, 1.8, 5, "Optimized"),
    ("wh-adhoc-l", "Large", 1200, 45, 5.5, 8, "Enable auto-stop 10min"),
    ("wh-etl-xxl", "2X-Large", 4800, 240, 2.8, 18, "Downsize to X-Large"),
]

schema = "warehouse_id STRING, size STRING, monthly_dbu INT, active_hours INT, avg_concurrency DOUBLE, peak_concurrency INT, recommendation STRING"
df = spark.createDataFrame(warehouse_data, schema)

# Calculate costs (example pricing)
df = df.withColumn(
    "monthly_cost_usd",
    when(col("size").contains("2X-Large"), col("monthly_dbu") * 1.20)
    .when(col("size").contains("X-Large"), col("monthly_dbu") * 0.75)
    .when(col("size") == "Large", col("monthly_dbu") * 0.50)
    .when(col("size") == "Medium", col("monthly_dbu") * 0.35)
    .otherwise(col("monthly_dbu") * 0.22)
)

# Right-sizing recommendations
df = df.withColumn(
    "target_size",
    when(col("peak_concurrency") <= 5, lit("Small"))
    .when(col("peak_concurrency") <= 15, lit("Medium"))
    .when(col("peak_concurrency") <= 30, lit("Large"))
    .otherwise(lit("X-Large"))
).withColumn(
    "potential_savings_pct",
    when(col("size").contains("2X") & (col("peak_concurrency") < 20), lit(40))
    .when(col("size").contains("X") & (col("peak_concurrency") < 10), lit(30))
    .otherwise(lit(0))
)

print("\nWarehouse Cost Analysis:")
display(df.select("warehouse_id", "size", "peak_concurrency", "monthly_cost_usd", "target_size", "potential_savings_pct"))

# Summary
total_cost = df.agg(sum("monthly_cost_usd")).collect()[0][0]
potential_savings = df.filter(col("potential_savings_pct") > 0).agg(
    sum(col("monthly_cost_usd") * col("potential_savings_pct") / 100)
).collect()[0][0]

print(f"\nTotal monthly cost: ${total_cost:,.2f}")
print(f"Potential monthly savings: ${potential_savings:,.2f}")
print(f"Annual savings: ${potential_savings * 12:,.2f}")

print("\nRecommendations:")
print("1. Enable scale-to-zero for all warehouses")
print("2. Set auto-stop to 10-15 minutes for ad-hoc warehouses")
print("3. Consolidate small warehouses serving same team")
print("4. Use Serverless SQL for unpredictable workloads")