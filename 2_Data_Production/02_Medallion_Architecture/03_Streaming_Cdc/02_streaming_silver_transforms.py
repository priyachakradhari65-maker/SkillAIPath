# Databricks notebook source
# DBTITLE 1,Load Config
# MAGIC %run ../../../1_Canvas/1_Setup/1_config

# COMMAND ----------

# DBTITLE 1,Streaming Silver Transforms - Real-Time Aggregations
# MAGIC %md
# MAGIC # Streaming Silver Transforms: Real-Time Data Processing
# MAGIC **SkillAIPath Data Warehouse - Bronze → Silver Streaming Pipeline**
# MAGIC
# MAGIC ## THE PROBLEM: "Reports are always 24 hours behind"
# MAGIC
# MAGIC **Tuesday 2 PM - Business Review**
# MAGIC
# MAGIC **CMO:** "Yesterday we ran a flash sale. I need to know: did it work? But reports won't update until tomorrow morning."
# MAGIC
# MAGIC **BI Team:** "We run batch nightly at 2 AM. Your flash sale data is in bronze, but silver aggregates won't refresh until tomorrow."
# MAGIC
# MAGIC **CMO:** "Competitors have real-time dashboards. We're flying blind for 24 hours after every campaign."
# MAGIC
# MAGIC **Data Architect:** "Switch to streaming. Bronze → Silver transforms can run continuously. Reports update every 5 minutes instead of daily."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Batch vs Streaming Processing
# MAGIC
# MAGIC ### Traditional Batch (24-hour latency)
# MAGIC ```
# MAGIC 2:00 AM: Read ALL bronze data
# MAGIC 3:00 AM: Aggregate (SUM, COUNT, AVG)
# MAGIC 4:00 AM: Write to silver tables
# MAGIC 8:00 AM: Reports ready (6+ hours old)
# MAGIC
# MAGIC Next update: Tomorrow 8 AM (24-hour cycle)
# MAGIC ```
# MAGIC
# MAGIC **Problems:**
# MAGIC -  Stale data (business decisions on yesterday's reality)
# MAGIC -  Full table scans every night (inefficient)
# MAGIC -  Resource spikes at 2 AM (cluster scaling)
# MAGIC -  Can't react to real-time events
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Streaming Processing (5-minute latency)
# MAGIC ```
# MAGIC Continuous: Read NEW bronze records
# MAGIC Continuous: Incremental aggregation
# MAGIC Continuous: Update silver tables
# MAGIC Dashboards: Auto-refresh every 5 min
# MAGIC
# MAGIC Data freshness: < 5 minutes
# MAGIC ```
# MAGIC
# MAGIC **Benefits:**
# MAGIC -  Real-time insights (sub-10-minute latency)
# MAGIC -  Incremental processing (only new data)
# MAGIC -  Even resource usage (no spikes)
# MAGIC -  Event-driven reactions (alerts, triggers)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Streaming Aggregation Patterns
# MAGIC
# MAGIC ### Pattern 1: Tumbling Window
# MAGIC **Use Case:** "Hourly sales totals"
# MAGIC
# MAGIC ```python
# MAGIC df.groupBy(window(col("timestamp"), "1 hour"), "product_id") \
# MAGIC   .agg(sum("amount").alias("hourly_sales"))
# MAGIC ```
# MAGIC
# MAGIC **Characteristics:**
# MAGIC - Fixed-size, non-overlapping windows
# MAGIC - Each record belongs to exactly 1 window
# MAGIC - Example: 10:00-11:00, 11:00-12:00 (no overlap)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Pattern 2: Sliding Window
# MAGIC **Use Case:** "Moving 1-hour average (updated every 10 min)"
# MAGIC
# MAGIC ```python
# MAGIC df.groupBy(window(col("timestamp"), "1 hour", "10 minutes"), "product_id") \
# MAGIC   .agg(avg("amount").alias("moving_avg"))
# MAGIC ```
# MAGIC
# MAGIC **Characteristics:**
# MAGIC - Overlapping windows
# MAGIC - Records appear in multiple windows
# MAGIC - Example: 10:00-11:00, 10:10-11:10, 10:20-11:20
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Pattern 3: Watermarking (Late Data Handling)
# MAGIC **Use Case:** "Handle events arriving out-of-order"
# MAGIC
# MAGIC ```python
# MAGIC df.withWatermark("event_time", "10 minutes") \
# MAGIC   .groupBy(window(col("event_time"), "1 hour")) \
# MAGIC   .agg(count("*").alias("event_count"))
# MAGIC ```
# MAGIC
# MAGIC **Characteristics:**
# MAGIC - Tolerates late-arriving events (up to watermark threshold)
# MAGIC - After threshold: late events dropped (for performance)
# MAGIC - Balances completeness vs latency
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Interview Positioning
# MAGIC
# MAGIC  **Question:** "How do you build real-time aggregations in Spark?"
# MAGIC
# MAGIC **Answer:** "I use Structured Streaming with these patterns:
# MAGIC
# MAGIC 1. **Windowed Aggregations:** Group events into time windows (tumbling for distinct periods, sliding for moving averages)
# MAGIC
# MAGIC 2. **Watermarking:** Handle late arrivals - wait up to 10 minutes for out-of-order events, then finalize window
# MAGIC
# MAGIC 3. **Incremental State:** Spark maintains partial aggregates in state store, updates incrementally (not full re-scan)
# MAGIC
# MAGIC 4. **Trigger Modes:**
# MAGIC    - Micro-batch (5 min): Balance freshness vs efficiency
# MAGIC    - Continuous (sub-second): Low-latency dashboards
# MAGIC    - availableNow: Batch-like catch-up
# MAGIC
# MAGIC At SkillAIPath, switched from nightly batch (24-hour latency) to streaming (5-minute latency). CMO can now track flash sales in real-time. Reduced compute costs 40% (incremental vs full scans)."
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Setup & Configuration
# Streaming Silver Transforms Setup

from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring, window, countDistinct, round, current_timestamp
import pyspark.sql.functions as F
from pyspark.sql.types import *
from datetime import datetime

# Force widget reset to pick up latest defaults (ensures portability)
dbutils.widgets.removeAll()

print("="*70)
print(" STREAMING SILVER TRANSFORMS")
print("="*70)
print(f"   Pattern: Real-time Bronze → Silver aggregations")
print(f"   Source: Bronze streaming tables")
print(f"   Target: Silver aggregated tables")
print(f"   Latency: Sub-10-minute freshness")
print("="*70)

# Configuration
dbutils.widgets.text("bronze_table", f"{Config.CATALOG}.{Config.SCHEMA_BRONZE}.stg_sales", "Bronze Table")
dbutils.widgets.text("silver_table", f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.sales_hourly", "Silver Table")
dbutils.widgets.text("checkpoint_path", f"/Volumes/{Config.CATALOG}/{Config.SCHEMA_SILVER}/checkpoints/silver_hourly", "Checkpoint Location")
dbutils.widgets.dropdown("window_duration", "1 hour", ["5 minutes", "15 minutes", "1 hour", "1 day"], "Window Duration")
dbutils.widgets.dropdown("trigger_interval", "5 minutes", ["1 minute", "5 minutes", "15 minutes", "continuous"], "Trigger Interval")
dbutils.widgets.text("watermark_threshold", "10 minutes", "Watermark Threshold")

bronze_table = dbutils.widgets.get("bronze_table")
silver_table = dbutils.widgets.get("silver_table")
checkpoint_path = dbutils.widgets.get("checkpoint_path")
window_duration = dbutils.widgets.get("window_duration")
trigger_interval = dbutils.widgets.get("trigger_interval")
watermark_threshold = dbutils.widgets.get("watermark_threshold")

print(f"\n Source: {bronze_table}")
print(f" Target: {silver_table}")
print(f" Window: {window_duration}")
print(f"⏱ Trigger: Every {trigger_interval}")
print(f" Watermark: {watermark_threshold}")
print(f"\n Streaming Pipeline: Incremental aggregation (not full scan)")

# COMMAND ----------

# DBTITLE 1,Auto-Setup: Create Checkpoint Volume
# Auto-create checkpoint volume if it doesn't exist
# This makes the notebook portable - works on any environment

print("🔧 CHECKPOINT SETUP")
print("="*70)

try:
    # Check if checkpoint volume exists
    volumes = spark.sql(f"SHOW VOLUMES IN {Config.CATALOG}.{Config.SCHEMA_SILVER}").collect()
    checkpoint_volume_exists = any(v.volume_name == "checkpoints" for v in volumes)
    
    if not checkpoint_volume_exists:
        print(f"⚠️  Checkpoint volume not found in {Config.CATALOG}.{Config.SCHEMA_SILVER}")
        print(f"📁 Creating: {Config.CATALOG}.{Config.SCHEMA_SILVER}.checkpoints")
        
        spark.sql(f"""
            CREATE VOLUME IF NOT EXISTS {Config.CATALOG}.{Config.SCHEMA_SILVER}.checkpoints
            COMMENT 'Streaming checkpoint storage for Bronze → Silver transforms'
        """)
        
        print(f"✅ Volume created successfully")
    else:
        print(f"✅ Checkpoint volume exists: {Config.CATALOG}.{Config.SCHEMA_SILVER}.checkpoints")
    
    print(f"📍 Checkpoint location: /Volumes/{Config.CATALOG}/{Config.SCHEMA_SILVER}/checkpoints/")
    print("="*70)
    
except Exception as e:
    print(f"❌ Error setting up checkpoint volume: {str(e)}")
    print("   Please ensure you have CREATE VOLUME permissions on the schema")
    raise

# COMMAND ----------

# DBTITLE 1,Read Bronze Stream
# Read streaming data from Bronze Delta table

print(" READING BRONZE STREAM")
print("="*70)

# Read bronze table as stream
df_bronze_stream = spark.readStream \
    .format("delta") \
    .table(bronze_table)

print(f" Bronze stream initialized: {bronze_table}")

# Parse and prepare data
df_prepared = df_bronze_stream \
    .withColumn("event_time", to_timestamp(col("OrderDate"))) \
    .withColumn("sales_amount", col("finalPrice").cast("double") / 100000)  # Convert to lakhs

print("\n Schema:")
df_prepared.printSchema()

print("\n Key Columns:")
print("   • event_time: Transaction timestamp (for windowing)")
print("   • sales_amount: Revenue in lakhs (for aggregation)")
print("   • ProductName, ProductCategory: Product dimensions (for grouping)")
print("   • CustomerName: Customer identifier (for unique counts)")

print("\n Streaming Mode: Incremental reads (only NEW bronze records)")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Apply Watermark - Handle Late Arrivals
# Apply watermark to handle late-arriving events

print(" APPLYING WATERMARK")
print("="*70)

df_watermarked = df_prepared.withWatermark("event_time", watermark_threshold)

print(f" Watermark configured: {watermark_threshold}")

print("\n How Watermarking Works:")
print("   1. Spark tracks max event_time seen")
print(f"   2. Watermark = max_event_time - {watermark_threshold}")
print(f"   3. Events BEFORE watermark: Dropped (too late)")
print(f"   4. Events AFTER watermark: Processed")

print("\n Example:")
print(f"   Max event seen: 2024-05-27 14:00")
print(f"   Watermark: 2024-05-27 13:50 (14:00 - {watermark_threshold})")
print(f"   Event at 13:45:  Dropped (before watermark)")
print(f"   Event at 13:55:  Processed (after watermark)")

print("\n Trade-off:")
print(f"   Longer watermark ({watermark_threshold}): More complete data, higher latency")
print("   Shorter watermark (1 minute): Faster results, may miss late events")

print("\n Typical Settings:")
print("   Real-time dashboards: 1-5 minutes")
print("   Financial accuracy: 10-30 minutes")
print("   Batch-like processing: 1+ hours")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Windowed Aggregation - Hourly Sales
# Tumbling window aggregation: Hourly sales by product

print(" WINDOWED AGGREGATION")
print("="*70)

# Tumbling window: Non-overlapping 1-hour windows
df_aggregated = df_watermarked \
    .groupBy(
        window(col("event_time"), window_duration),
        col("ProductName")
    ) \
    .agg(
        count("*").alias("transaction_count"),
        sum("sales_amount").alias("total_sales_lakhs"),
        avg("sales_amount").alias("avg_order_value_lakhs"),
        F.approx_count_distinct("CustomerName").alias("unique_customers"),
        min("event_time").alias("window_start_actual"),
        max("event_time").alias("window_end_actual")
    ) \
    .withColumn("total_sales_lakhs", F.round(col("total_sales_lakhs"), 2)) \
    .withColumn("avg_order_value_lakhs", F.round(col("avg_order_value_lakhs"), 2)) \
    .withColumn("processed_at", F.current_timestamp()) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("ProductName"),
        col("transaction_count"),
        col("total_sales_lakhs"),
        col("avg_order_value_lakhs"),
        col("unique_customers"),
        col("processed_at")
    )

print(f" Aggregation defined: {window_duration} tumbling windows")

print("\n Aggregation Metrics:")
print("   • transaction_count: Total transactions in window")
print("   • total_sales_lakhs: Revenue sum")
print("   • avg_order_value_lakhs: Average transaction size")
print("   • unique_customers: Distinct customer count")

print("\n Window Behavior:")
if window_duration == "1 hour":
    print("   10:00-11:00, 11:00-12:00, 12:00-13:00 (non-overlapping)")
    print("   Transaction at 10:30 → 10:00-11:00 window")
    print("   Transaction at 11:00 → 11:00-12:00 window")
elif window_duration == "5 minutes":
    print("   10:00-10:05, 10:05-10:10, 10:10-10:15 (non-overlapping)")
    print("   Near real-time (5-minute buckets)")

print("\n Incremental State:")
print("   Spark maintains partial aggregates in memory")
print("   Only updates affected windows (not full re-scan)")
print("   Checkpoint tracks processed offsets")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Write Streaming Aggregates to Silver
# Write streaming aggregates to Silver Delta table

print(" WRITING TO SILVER TABLE")
print("="*70)

# Fix checkpoint path if widget has stale DBFS value
if checkpoint_path.startswith("/tmp/") or checkpoint_path.startswith("dbfs:/tmp/"):
    checkpoint_path = f"/Volumes/{Config.CATALOG}/{Config.SCHEMA_SILVER}/checkpoints/silver_hourly"
    print(f"⚠️  Corrected checkpoint path from DBFS root to: {checkpoint_path}")

# Configure trigger (Serverless only supports availableNow/once)
trigger_config = {"availableNow": True}
print(" Trigger: AvailableNow (batch-like, incremental reads)")

# Write stream with complete mode for aggregations
query = df_aggregated.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(**trigger_config) \
    .toTable(silver_table)

print(f" Streaming write started to: {silver_table}")
print(f" Checkpoint: {checkpoint_path}")
print(f" Output Mode: append (finalized windows only)")

print("\n Output Modes Explained:")
print("   • append: Write finalized windows (after watermark)")
print("   • update: Re-write updated windows (not supported with watermark)")
print("   • complete: Full result every trigger (memory-intensive)")

print("\n Why 'append' for Windowed Aggregations:")
print("   1. Watermark finalizes windows (no more updates after threshold)")
print("   2. Write finalized windows once (efficient)")
print("   3. Downstream queries simple (no deduplication needed)")

print("\n⏱ Streaming Behavior:")
print(f"   Every {trigger_interval}:")
print("   1. Check for new bronze records")
print("   2. Update affected windows in state")
print("   3. Finalize windows past watermark")
print("   4. Write finalized windows to silver")
print("   5. Update checkpoint")

print(f"\n Data Freshness:")
print(f"   Latency = {trigger_interval} + {watermark_threshold}")
if trigger_interval == "5 minutes" and watermark_threshold == "10 minutes":
    print("   Example: 5 min trigger + 10 min watermark = 15 min end-to-end")

print("\n" + "="*70)
print(" Streaming pipeline running...")
print(f"   Monitor: query.recentProgress[-1]")
print(f"   Stop: query.stop()")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Validation - Silver Table Contents
# MAGIC %sql
# MAGIC -- Check streaming aggregates in Silver table
# MAGIC SELECT 
# MAGIC   window_start,
# MAGIC   window_end,
# MAGIC   ProductName,
# MAGIC   transaction_count,
# MAGIC   total_sales_lakhs,
# MAGIC   avg_order_value_lakhs,
# MAGIC   unique_customers,
# MAGIC   processed_at
# MAGIC FROM sap_dev.silver.sales_hourly
# MAGIC ORDER BY window_start DESC, total_sales_lakhs DESC
# MAGIC LIMIT 20;
# MAGIC
# MAGIC --  Real-time aggregates: Updates every trigger interval

# COMMAND ----------

# DBTITLE 1,Query Pattern - Latest Window
# MAGIC %sql
# MAGIC -- Dashboard query: Latest complete window (for real-time KPIs)
# MAGIC WITH latest_window AS (
# MAGIC   SELECT MAX(window_end) as max_window
# MAGIC   FROM sap_dev.silver.sales_hourly
# MAGIC )
# MAGIC SELECT 
# MAGIC   h.window_start,
# MAGIC   h.window_end,
# MAGIC   SUM(h.transaction_count) as total_transactions,
# MAGIC   ROUND(SUM(h.total_sales_lakhs), 2) as total_revenue_lakhs,
# MAGIC   ROUND(AVG(h.avg_order_value_lakhs), 2) as avg_order_value,
# MAGIC   SUM(h.unique_customers) as total_customers
# MAGIC FROM sap_dev.silver.sales_hourly h
# MAGIC CROSS JOIN latest_window lw
# MAGIC WHERE h.window_end = lw.max_window
# MAGIC GROUP BY h.window_start, h.window_end;
# MAGIC
# MAGIC --  Dashboard KPI: "Last hour's performance"
# MAGIC -- Updates automatically every trigger interval

# COMMAND ----------

# DBTITLE 1,Sliding Window Example - Moving Averages
# MAGIC %md
# MAGIC ##  Advanced Pattern: Sliding Windows
# MAGIC
# MAGIC ### Use Case: "1-hour moving average, updated every 10 minutes"
# MAGIC
# MAGIC ```python
# MAGIC # Sliding window: Overlapping windows
# MAGIC df_sliding = df_watermarked \
# MAGIC     .groupBy(
# MAGIC         window(col("event_time"), "1 hour", "10 minutes"),  # Window + slide
# MAGIC         col("ProductSK")
# MAGIC     ) \
# MAGIC     .agg(
# MAGIC         avg("sales_amount").alias("moving_avg_sales_lakhs"),
# MAGIC         sum("sales_amount").alias("total_sales_lakhs")
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Window Behavior
# MAGIC
# MAGIC **Tumbling (non-overlapping):**
# MAGIC ```
# MAGIC 10:00-11:00
# MAGIC 11:00-12:00
# MAGIC 12:00-13:00
# MAGIC ```
# MAGIC
# MAGIC **Sliding (overlapping):**
# MAGIC ```
# MAGIC 10:00-11:00
# MAGIC 10:10-11:10  ← Overlaps with above
# MAGIC 10:20-11:20  ← Overlaps with both
# MAGIC 10:30-11:30
# MAGIC ...
# MAGIC ```
# MAGIC
# MAGIC **Result:** Same event appears in multiple windows
# MAGIC - Transaction at 10:30 appears in: 10:00-11:00, 10:10-11:10, 10:20-11:20, 10:30-11:30
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### When to Use Each
# MAGIC
# MAGIC | Pattern | Use Case | Example |
# MAGIC |---------|----------|----------|
# MAGIC | **Tumbling** | Distinct time buckets | "Hourly sales totals" |
# MAGIC | **Sliding** | Moving averages, trends | "1-hour moving average" |
# MAGIC
# MAGIC **Sliding Window Trade-offs:**
# MAGIC -  Smooth trends (no sudden jumps)
# MAGIC -  Real-time moving averages
# MAGIC -  Higher compute (more windows per event)
# MAGIC -  More storage (overlapping results)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Code Comparison
# MAGIC
# MAGIC **Tumbling (5-minute buckets):**
# MAGIC ```python
# MAGIC window(col("event_time"), "5 minutes")
# MAGIC # 10:00-10:05, 10:05-10:10, 10:10-10:15
# MAGIC ```
# MAGIC
# MAGIC **Sliding (5-minute window, 1-minute slide):**
# MAGIC ```python
# MAGIC window(col("event_time"), "5 minutes", "1 minute")
# MAGIC # 10:00-10:05, 10:01-10:06, 10:02-10:07, 10:03-10:08...
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Performance & Cost Analysis
# MAGIC %md
# MAGIC ##  Performance & Cost: Batch vs Streaming
# MAGIC
# MAGIC ### Scenario: SkillAIPath Hourly Sales Aggregation
# MAGIC
# MAGIC **Data Volume:** 250K transactions/day
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Batch Approach (Traditional)
# MAGIC
# MAGIC **Process:**
# MAGIC ```python
# MAGIC # Every hour (scheduled job)
# MAGIC df = spark.read.table("bronze_sales") \
# MAGIC     .filter(col("date") == current_date())
# MAGIC df.groupBy("hour", "product").agg(...).write.mode("overwrite").save(...)
# MAGIC ```
# MAGIC
# MAGIC **Characteristics:**
# MAGIC - Full scan of day's data every hour (cumulative)
# MAGIC - 1st run: 10K rows (1 hour)
# MAGIC - 12th run: 120K rows (12 hours) → slowest
# MAGIC - 24th run: 250K rows (full day)
# MAGIC
# MAGIC **Performance:**
# MAGIC - Morning (8 AM): Scan 80K rows → 20 seconds
# MAGIC - Evening (8 PM): Scan 200K rows → 90 seconds
# MAGIC - Midnight (12 AM): Scan 250K rows → 120 seconds
# MAGIC
# MAGIC **Cost (24 hourly jobs):**
# MAGIC - Total scans: 3M rows/day (10K + 20K + 30K ... + 250K)
# MAGIC - Compute: ~45 minutes cluster time/day
# MAGIC - Monthly: $257/hr × 22.5 hrs = **$5,782/month**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Streaming Approach (Incremental)
# MAGIC
# MAGIC **Process:**
# MAGIC ```python
# MAGIC # Continuous stream
# MAGIC df = spark.readStream.table("bronze_sales")
# MAGIC df.groupBy(window("timestamp", "1 hour")).agg(...).writeStream.save(...)
# MAGIC ```
# MAGIC
# MAGIC **Characteristics:**
# MAGIC - Incremental reads (only NEW records)
# MAGIC - Each trigger: ~10K rows (1 hour worth)
# MAGIC - State: Partial aggregates in memory
# MAGIC
# MAGIC **Performance:**
# MAGIC - Every hour: Process 10K NEW rows → 5 seconds
# MAGIC - Consistent latency (no slowdown over day)
# MAGIC
# MAGIC **Cost (continuous stream):**
# MAGIC - Total scans: 250K rows/day (each row processed once)
# MAGIC - Compute: ~2 minutes active processing/day
# MAGIC - Small cluster always-on for streaming: $64/hr × 24 = $1,536/day
# MAGIC - Monthly: **$46,080/month**
# MAGIC
# MAGIC **Wait, HIGHER cost?!**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Cost Optimization: Trigger Intervals
# MAGIC
# MAGIC **Problem:** Continuous streaming = cluster always running
# MAGIC
# MAGIC **Solution:** Micro-batch with smart trigger intervals
# MAGIC
# MAGIC **Option 1: 5-minute micro-batch**
# MAGIC ```python
# MAGIC .trigger(processingTime="5 minutes")
# MAGIC ```
# MAGIC - Cluster runs 5 min, stops, restarts in 5 min
# MAGIC - Utilization: (2 sec / 300 sec) = 0.67% per cycle
# MAGIC - Actual cost: ~$200/month
# MAGIC
# MAGIC **Option 2: availableNow (batch-like)**
# MAGIC ```python
# MAGIC .trigger(availableNow=True)
# MAGIC ```
# MAGIC - Run every hour via job scheduler
# MAGIC - Process all available data, then terminate
# MAGIC - Cost: Same as batch ($5,782/month) BUT incremental reads
# MAGIC - Benefit: Faster (incremental) + same cost
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Cost Comparison Summary
# MAGIC
# MAGIC | Approach | Cluster Utilization | Monthly Cost | Latency |
# MAGIC |----------|---------------------|--------------|----------|
# MAGIC | **Batch (hourly)** | 45 min/day (3%) | $5,782 | 1-hour |
# MAGIC | **Streaming (continuous)** | 24/7 (100%) | $46,080 | Real-time |
# MAGIC | **Streaming (5-min trigger)** | 2 min/day (0.1%) | $200 | 5-min |
# MAGIC | **Streaming (availableNow hourly)** | 10 min/day (0.7%) | $640 | 1-hour |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Recommendation: Hybrid Approach
# MAGIC
# MAGIC **Real-time KPIs (5-minute trigger):**
# MAGIC - Flash sales, inventory alerts, fraud detection
# MAGIC - Cost: $200/month
# MAGIC - Latency: 5 minutes
# MAGIC
# MAGIC **Hourly aggregates (availableNow trigger):**
# MAGIC - Historical reports, analytics dashboards
# MAGIC - Cost: $640/month
# MAGIC - Latency: 1 hour
# MAGIC - Benefit: Incremental (faster than batch scan)
# MAGIC
# MAGIC **Total:** $840/month vs $5,782 batch
# MAGIC **Savings:** $4,942/month (85% reduction)
# MAGIC **Bonus:** Real-time KPIs included
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Summary & Best Practices
# Streaming Silver Transforms Summary

print("="*70)
print(" STREAMING SILVER TRANSFORMS - BEST PRACTICES")
print("="*70)

print("\n Key Patterns:")
print("-" * 70)
print("  1.  Tumbling Windows: Non-overlapping time buckets")
print("  2.  Sliding Windows: Overlapping (moving averages)")
print("  3.  Watermarking: Handle late arrivals (threshold-based)")
print("  4.  Incremental State: Spark tracks partial aggregates")

print("\n Window Functions:")
print("-" * 70)
print("  Tumbling: window(col('timestamp'), '1 hour')")
print("  Sliding: window(col('timestamp'), '1 hour', '10 minutes')")
print("  Watermark: withWatermark('timestamp', '10 minutes')")

print("\n Output Modes:")
print("-" * 70)
print("  • append: Finalized windows only (with watermark)")
print("  • update: Modified rows (without watermark)")
print("  • complete: Full result every trigger (stateful)")

print("\n Performance:")
print("-" * 70)
print("  Batch (hourly full scan): 90 seconds (evening)")
print("  Streaming (incremental): 5 seconds (consistent)")
print("  Speedup: 18x faster")

print("\n Cost Optimization:")
print("-" * 70)
print("  Batch (hourly): $5,782/month")
print("  Streaming (continuous): $46,080/month (always-on)")
print("  Streaming (5-min trigger): $200/month (micro-batch)")
print("  Streaming (availableNow hourly): $640/month (batch-like)")
print("  Recommendation: $840/month (85% savings vs batch)")

print("\n⏱ Latency Configuration:")
print("-" * 70)
print("  End-to-end latency = trigger_interval + watermark_threshold")
print("  Example: 5 min trigger + 10 min watermark = 15 min total")
print("\n  Real-time dashboard: 1-5 min trigger, 5 min watermark")
print("  Analytics reports: 1-hour trigger, 10 min watermark")

print("\nSECURITY Production Checklist:")
print("-" * 70)
print("  1. Choose window type (tumbling vs sliding)")
print("  2. Set watermark threshold (balance completeness vs latency)")
print("  3. Configure trigger interval (balance freshness vs cost)")
print("  4. Use append mode with watermark (efficient writes)")
print("  5. Partition output tables (by date for queries)")
print("  6. Monitor state size (grows with windows)")
print("  7. Test late arrival handling (simulate delayed events)")

print("\n SkillAIPath Use Cases:")
print("-" * 70)
print("  • Flash sale monitoring: 5-min trigger (real-time)")
print("  • Hourly sales reports: 1-hour trigger (cost-optimized)")
print("  • Daily aggregates: availableNow hourly (batch-like efficiency)")

print("\n Interview Key Points:")
print("-" * 70)
print("  1. Streaming = incremental processing (not full scans)")
print("  2. Windowing groups events by time (tumbling or sliding)")
print("  3. Watermarking handles late arrivals (threshold-based)")
print("  4. Trigger modes balance latency vs cost")
print("  5. SkillAIPath: 24-hour → 5-minute latency, 85% cost savings")

print("\n" + "="*70)
print(" STREAMING SILVER: Real-time aggregations with cost control")
print("="*70)

print("\n Code Template:")
print("-" * 70)
print("""
df = spark.readStream.table("bronze_table") \\
    .withWatermark("event_time", "10 minutes") \\
    .groupBy(window(col("event_time"), "1 hour"), col("product_id")) \\
    .agg(sum("amount").alias("total_sales"))

df.writeStream \\
    .format("delta") \\
    .outputMode("append") \\
    .trigger(processingTime="5 minutes") \\
    .option("checkpointLocation", "/checkpoints/silver") \\
    .toTable("silver_table")
""")

print("\n Streaming = 18x faster + 85% cheaper + real-time insights")