# Databricks notebook source
# DBTITLE 1,Streaming Product Performance - Real-Time Analytics
# MAGIC %md
# MAGIC # Streaming Product Performance Analytics
# MAGIC
# MAGIC Real-time product metrics with tumbling window aggregations.
# MAGIC
# MAGIC Features:
# MAGIC - 1-hour tumbling windows for product sales aggregation
# MAGIC - Top-N products by revenue per window
# MAGIC - Running totals and moving averages
# MAGIC - Watermarking for late arrivals (10-minute tolerance)

# COMMAND ----------

# DBTITLE 1,Configuration
# MAGIC   %run ../../../1_Canvas/1_Setup/1_config

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring, window, countDistinct, round, current_timestamp, approx_count_distinct
from pyspark.sql.types import *
from datetime import datetime

print("="*70)
print("STREAMING PRODUCT PERFORMANCE")
print("="*70)
print(f"Target: {Config.TBL_STREAMING_PRODUCT_PERFORMANCE}")
print(f"Source: {Config.TBL_FACT_SALES}")
print(f"Window: 1-hour tumbling windows")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Read Streaming Source
# ========================================================================
# NOTE: This cell demonstrates the INTENDED streaming approach.
# However, streaming adds complexity with minimal benefit for hourly windows.
# This code is kept for documentation/educational purposes.
# The ACTUAL implementation is in Cell 6 using batch mode.
# ========================================================================

# Read from Delta table as stream
streaming_df = spark.readStream \
    .format("delta") \
    .table(Config.TBL_FACT_SALES)

print(f"Schema: {len(streaming_df.columns)} columns")
print(f"Source: {Config.TBL_FACT_SALES}")

# COMMAND ----------

# DBTITLE 1,Windowed Aggregation - 1-Hour Tumbling Windows
# ========================================================================
# NOTE: This cell demonstrates the INTENDED streaming approach with windows.
# However, streaming has limitations: approx_count_distinct only, complex state.
# This code is kept for documentation/educational purposes.
# The ACTUAL implementation is in Cell 6 using batch mode with exact counts.
# ========================================================================

# Tumbling window: 1-hour windows, 10-minute watermark for late arrivals
# Note: Using LoadedAt (timestamp) for watermark since fact_sales uses OrderDateID (date key)
product_performance = streaming_df \
    .withWatermark("LoadedAt", "10 minutes") \
    .groupBy(
        window(col("LoadedAt"), "1 hour"),
        col("ProductSK")  # Using surrogate key (fact table doesn't have ProductName)
    ) \
    .agg(
        count("*").alias("order_count"),
        sum("ProductQuantity").alias("total_quantity"),
        sum("finalprice").alias("total_revenue"),
        avg("finalprice").alias("avg_order_value"),
        approx_count_distinct("CustomerSK").alias("unique_customers")  # approx for streaming (exact count not supported)
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("ProductSK"),
        col("order_count"),
        col("total_quantity"),
        round(col("total_revenue"), 2).alias("total_revenue"),
        round(col("avg_order_value"), 2).alias("avg_order_value"),
        col("unique_customers"),
        current_timestamp().alias("processing_time")
    )

print("Windowed aggregation configured")
print("Window: 1 hour tumbling (based on LoadedAt timestamp)")
print("Watermark: 10 minutes for late arrivals")

# COMMAND ----------

# DBTITLE 1,Write to Streaming Table
# ========================================================================
# WHY WE SHIFTED FROM STREAMING TO BATCH MODE
# ========================================================================
#
# ORIGINAL INTENT: Use Spark Structured Streaming with tumbling windows
# 
# ISSUES ENCOUNTERED:
#   1. countDistinct() NOT supported → Had to use approx_count_distinct()
#   2. Windowed aggregations with watermarking are SLOW in streaming
#   3. Complex checkpoint management for simple hourly aggregations
#   4. Same pattern as notebook 06 (streaming_rfm_live) which we converted to batch
#
# SPARK STREAMING LIMITATIONS:
#   - Exact distinct counts not supported (only approximate)
#   - Complete mode requires maintaining ALL state in memory
#   - Window aggregations + watermarking = complex state management
#
# FINAL SOLUTION: Pure Batch Mode
#   - Read entire fact_sales as batch (not stream)
#   - Apply window aggregations in one pass
#   - Write to gold layer
#   - Execution time: 2-3 minutes (vs potential streaming complexity)
#   - Result: Exact counts, faster execution, simpler code
#
# CONCLUSION:
#   For hourly window aggregations with distinct counts, BATCH is simpler and faster.
#
# ========================================================================

print("="*70)
print("PRODUCT PERFORMANCE BATCH PROCESSING STARTED")
print("="*70)
print(f"Source: {Config.TBL_FACT_SALES}")
print(f"Target: {Config.TBL_STREAMING_PRODUCT_PERFORMANCE}")
print("Pattern: Fast batch mode (not streaming)")
print("="*70)

# Step 1: Read sales as BATCH (not stream)
df_sales_batch = spark.table(Config.TBL_FACT_SALES)
print(f"\n[1/3] Loaded {df_sales_batch.count():,} sales records")

# Step 2: Apply windowed aggregations (works perfectly in batch)
product_performance_batch = df_sales_batch \
    .groupBy(
        window(col("LoadedAt"), "1 hour"),
        col("ProductSK")
    ) \
    .agg(
        count("*").alias("order_count"),
        sum("ProductQuantity").alias("total_quantity"),
        sum("finalprice").alias("total_revenue"),
        avg("finalprice").alias("avg_order_value"),
        countDistinct("CustomerSK").alias("unique_customers")  # EXACT count (not approximate)
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("ProductSK"),
        col("order_count"),
        col("total_quantity"),
        round(col("total_revenue"), 2).alias("total_revenue"),
        round(col("avg_order_value"), 2).alias("avg_order_value"),
        col("unique_customers"),
        current_timestamp().alias("processing_time")
    )

print("[2/3] Applied 1-hour window aggregations")

# Step 3: Write to gold layer
product_performance_batch.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(Config.TBL_STREAMING_PRODUCT_PERFORMANCE)

final_count = spark.table(Config.TBL_STREAMING_PRODUCT_PERFORMANCE).count()
print(f"[3/3] Written {final_count:,} windowed records to gold layer")

print("\n" + "="*70)
print("PRODUCT PERFORMANCE BATCH PROCESSING COMPLETED")
print("="*70)
print(f"\nTable: {Config.TBL_STREAMING_PRODUCT_PERFORMANCE}")
print("\nNote: Despite the name 'streaming_product_performance', this is now a batch job.")
print("Reason: Streaming adds complexity with minimal benefit for hourly aggregations.")
print("Result: Exact distinct counts, faster execution, simpler code.")

# COMMAND ----------

# DBTITLE 1,Monitor Stream Status
# Verify results
print("\nVerifying Results:")
print("="*70)

df_results = spark.table(Config.TBL_STREAMING_PRODUCT_PERFORMANCE)
result_count = df_results.count()

print(f" Total windowed records: {result_count:,}")
print(f" Table: {Config.TBL_STREAMING_PRODUCT_PERFORMANCE}")

# Show sample windows
print("\nSample windows (latest):")
df_results.orderBy(col("window_start").desc()).limit(5).display()

print("\n" + "="*70)

# COMMAND ----------

# DBTITLE 1,Sample Results - Top Products by Window
# Top products by window (using Config variable - no hardcoding)
query = f"""
SELECT
  window_start,
  window_end,
  ProductSK,
  order_count,
  total_quantity,
  total_revenue,
  unique_customers,
  RANK() OVER (PARTITION BY window_start ORDER BY total_revenue DESC) AS revenue_rank
FROM {Config.TBL_STREAMING_PRODUCT_PERFORMANCE}
WHERE window_start >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
QUALIFY revenue_rank <= 10
ORDER BY window_start DESC, revenue_rank
"""

print("Top 10 Products by Revenue (per 1-hour window):")
print("="*70)
spark.sql(query).display()
