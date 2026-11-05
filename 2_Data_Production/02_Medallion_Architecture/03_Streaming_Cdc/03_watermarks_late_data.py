# Databricks notebook source
# DBTITLE 1,Watermarks and Late Data Handling
# MAGIC %md
# MAGIC # Watermarks: Handling Late-Arriving Data
# MAGIC **SkillAIPath Streaming - Advanced Time-Based Processing**
# MAGIC
# MAGIC ## THE PROBLEM: "Events arrive out of order"
# MAGIC
# MAGIC **Tuesday Morning - Stream Processing Review**
# MAGIC
# MAGIC **Data Engineer:** "Our hourly sales aggregates are off. The 9 AM batch shows lower revenue than the 10 AM reprocessing of the same hour."
# MAGIC
# MAGIC **Architect:** "Late data. Events from 9:05 AM can arrive at 9:15 AM due to network delays. How are you handling this?"
# MAGIC
# MAGIC **Engineer:** "We aggregate by processing time (when the event arrived), not event time (when it actually happened)."
# MAGIC
# MAGIC **Architect:** "That's why numbers drift. You need watermarks - tell Spark to wait for late data up to a threshold, then close the window."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## THE SOLUTION: Watermarks
# MAGIC
# MAGIC **What It Is:**
# MAGIC * Time threshold for late data tolerance
# MAGIC * Spark tracks max event time seen, subtracts watermark delay
# MAGIC * Windows older than watermark are finalized (no more updates)
# MAGIC
# MAGIC **Example:**
# MAGIC * Watermark: 10 minutes
# MAGIC * Max event time seen: 9:15 AM
# MAGIC * Windows older than 9:05 AM (9:15 - 10 min) are closed
# MAGIC
# MAGIC **When to Use:**
# MAGIC * Time-based aggregations (hourly sales, daily summaries)
# MAGIC * Late data is common but bounded (network delays, batch uploads)
# MAGIC * Need finalized results (no infinite state growth)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC * Module 04_STREAMING_CDC complete
# MAGIC * Understanding of event time vs processing time
# MAGIC * Structured Streaming basics

# COMMAND ----------

# DBTITLE 1,Setup Watermark Example
# ============================================================================
# ENVIRONMENT-AWARE CONFIGURATION (PORTABLE)
# Auto-detects environment from workspace path: /Shared/{env}/SkillAIPath/
# ============================================================================

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import re

print('='*70)
print('WATERMARKS - LATE DATA HANDLING WITH REAL STREAMING DATA')
print('='*70)

# Auto-detect environment from notebook path
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
env_match = re.search(r'/Shared/(\w+)/SkillAIPath/', notebook_path)
ENV = env_match.group(1) if env_match else 'dev'

print(f"📌 Environment auto-detected: {ENV}")
print('='*70)

# Environment-aware configuration
CATALOG = f"sap_{ENV}"
BRONZE_SCHEMA = "bronze"
BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.sales_streaming_bronze"
VOLUME_BASE = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/streaming_test_data"
CHECKPOINT_BASE = f"{VOLUME_BASE}/_checkpoints"

print(f"\n📊 Configuration:")
print(f"   Environment: {ENV}")
print(f"   Bronze Table: {BRONZE_TABLE}")
print(f"   Checkpoint Base: {CHECKPOINT_BASE}")
print(f"\n   Pattern: Time-based aggregations with late tolerance")
print(f"   Watermark: Max event time - delay threshold")
print(f"   Effect: Finalize old windows, prevent infinite state")
print('='*70)

# Read streaming data from bronze table
print(f"\n🔄 Reading from bronze table: {BRONZE_TABLE}")
print("   This table contains 1,000 realistic events with 20% late arrivals")

df_bronze_stream = spark.readStream \
    .table(BRONZE_TABLE) \
    .select(
        col("CustomerID").alias("customer_id"),
        col("OrderID").alias("order_id"),
        col("ProductName").alias("product_name"),
        col("TotalAmount").alias("amount"),
        col("PaymentMode").alias("payment_mode"),
        col("event_timestamp"),
        col("_processing_time").alias("processing_time")
    )

print("\n✅ Streaming source connected")
print("   - Source: Bronze table with realistic late arrivals")
print("   - Late data: 20% of events (200 out of 1,000)")
print("   - Event column: event_timestamp (when event actually happened)")
print("   - Processing column: _processing_time (when event was ingested)")

# COMMAND ----------

# DBTITLE 1,Watermark Aggregation
# Streaming aggregation with watermark on realistic data
from pyspark.sql.functions import window

print('\n🔍 Testing Watermarks with Real Late Arrivals')
print('='*70)

# Scenario 1: 10-minute watermark (strict)
print("\n📊 Scenario 1: 10-minute watermark")
print("   - Tolerates events up to 10 min late")
print("   - Late events beyond 10 min are DROPPED")

watermarked_10min = df_bronze_stream \
    .withWatermark('event_timestamp', '10 minutes') \
    .groupBy(
        window(col('event_timestamp'), '1 hour'),
        col('customer_id')
    ) \
    .agg(
        count('*').alias('event_count'),
        sum(when(col('amount').isNotNull(), col('amount')).otherwise(0)).alias('total_amount'),
        count(when(col('order_id').isNotNull(), 1)).alias('purchase_count'),
        count(when(col('order_id').isNull(), 1)).alias('browse_count')
    ) \
    .select(
        col('window.start').alias('window_start'),
        col('window.end').alias('window_end'),
        'customer_id',
        'event_count',
        'total_amount',
        'purchase_count',
        'browse_count'
    )

# Write to output table with checkpoint
checkpoint_10min = f"{CHECKPOINT_BASE}/watermark_10min"
output_table_10min = f"{CATALOG}.{BRONZE_SCHEMA}.watermark_test_10min"

print(f"\n💾 Writing results to: {output_table_10min}")
print(f"   Checkpoint: {checkpoint_10min}")

query_10min = watermarked_10min.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_10min) \
    .trigger(availableNow=True) \
    .toTable(output_table_10min)

query_10min.awaitTermination()

print("\n✅ 10-minute watermark aggregation complete")
print("\n💡 Key Points:")
print("   - Events within 10 min of max event_timestamp are included")
print("   - Windows older than (max_event_time - 10 min) are finalized")
print("   - Late events beyond 10 min watermark are DROPPED")
print("   - This prevents infinite state growth")

# COMMAND ----------

# DBTITLE 1,Analyze Late Arrival Delays
# ANALYZE ACTUAL LATE ARRIVALS - Calculate lateness for each event
print('='*70)
print('🔍 DETECTING LATE ARRIVALS IN YOUR DATA')
print('='*70)

# Read bronze table to analyze lateness
df_lateness = spark.read.table(BRONZE_TABLE) \
    .select(
        col('CustomerID'),
        col('OrderID'),
        col('ProductName'),
        col('event_timestamp'),
        col('_processing_time')
    ) \
    .withColumn(
        'lateness_seconds',
        (unix_timestamp('_processing_time') - unix_timestamp('event_timestamp'))
    ) \
    .withColumn(
        'lateness_minutes',
        round(col('lateness_seconds') / 60, 2)
    ) \
    .withColumn(
        'is_late',
        when(col('lateness_seconds') > 0, 'Late Arrival').otherwise('On-Time')
    ) \
    .withColumn(
        'exceeds_10min_watermark',
        when(col('lateness_minutes') > 10, 'DROPPED by 10-min watermark').otherwise('Included')
    )

print("\n📊 Late Arrival Statistics:")
print("-" * 70)

# Overall stats
total_events = df_lateness.count()
late_events = df_lateness.filter(col('lateness_seconds') > 0).count()
on_time_events = total_events - late_events
exceeds_watermark = df_lateness.filter(col('lateness_minutes') > 10).count()

print(f"\n   📦 Total events: {total_events}")
print(f"   ⏰ On-time events: {on_time_events} ({on_time_events/total_events*100:.1f}%)")
print(f"   🕐 Late arrivals: {late_events} ({late_events/total_events*100:.1f}%)")
print(f"   ❌ Exceeds 10-min watermark: {exceeds_watermark} ({exceeds_watermark/total_events*100:.1f}% would be DROPPED)")

print("\n📈 Lateness Distribution:")
print("-" * 70)

lateness_buckets = df_lateness \
    .groupBy('is_late') \
    .agg(
        count('*').alias('event_count'),
        round(avg('lateness_minutes'), 2).alias('avg_lateness_min'),
        round(min('lateness_minutes'), 2).alias('min_lateness_min'),
        round(max('lateness_minutes'), 2).alias('max_lateness_min')
    ) \
    .orderBy('is_late')

display(lateness_buckets)

print("\n🚨 Events That Would Be DROPPED by 10-min Watermark:")
print("-" * 70)

dropped_events = df_lateness \
    .filter(col('lateness_minutes') > 10) \
    .select(
        'CustomerID',
        'OrderID',
        'ProductName',
        'event_timestamp',
        '_processing_time',
        'lateness_minutes',
        'exceeds_10min_watermark'
    ) \
    .orderBy(col('lateness_minutes').desc()) \
    .limit(10)

display(dropped_events)

print("\n💡 What This Shows:")
print("   - Late arrivals = event_timestamp < _processing_time")
print("   - Your generator injected 20% late arrivals (200 events)")
print(f"   - Actual late events detected: {late_events}")
print(f"   - Events exceeding 10-min watermark: {exceeds_watermark} (would be dropped)")
print("   - This is WHY watermarks matter - balance data completeness vs state size!")

print('\n' + '='*70)

# COMMAND ----------

# DBTITLE 1,Late Data Scenarios
# Analyze actual late arrivals in the bronze data
print('='*70)
print('🔍 ANALYZING REAL LATE ARRIVALS')
print('='*70)

print("\n📊 Querying watermark results...")

# Read the aggregated results
results_df = spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.watermark_test_10min")

print(f"\n📊 Summary Statistics:")
print("-" * 70)

# Overall stats
total_windows = results_df.select('window_start').distinct().count()
total_customers = results_df.select('customer_id').distinct().count()
total_events = results_df.agg(sum('event_count')).collect()[0][0]
total_purchases = results_df.agg(sum('purchase_count')).collect()[0][0]
total_browsing = results_df.agg(sum('browse_count')).collect()[0][0]
total_revenue = results_df.agg(sum('total_amount')).collect()[0][0]

print(f"\n   📅 Time windows: {total_windows}")
print(f"   👥 Customers: {total_customers}")
print(f"   📦 Total events: {int(total_events) if total_events else 0}")
print(f"   🛍️  Purchases: {int(total_purchases) if total_purchases else 0}")
print(f"   👀 Browsing events: {int(total_browsing) if total_browsing else 0}")
print(f"   💰 Total revenue: ${total_revenue:,.2f}" if total_revenue else "   💰 Total revenue: $0.00")

print("\n🕐 Top 5 Customer Sessions (by hour window):")
print("-" * 70)

top_sessions = results_df \
    .orderBy(col('event_count').desc()) \
    .limit(5) \
    .select(
        'window_start',
        'window_end', 
        'customer_id',
        'event_count',
        'purchase_count',
        'browse_count',
        'total_amount'
    )

display(top_sessions)

print("\n💡 What This Shows:")
print("   - Events are grouped by 1-hour time windows")
print("   - 10-minute watermark tolerates late arrivals up to 10 min")
print("   - Your data had 20% late arrivals - see if any were dropped!")
print("   - Each window shows: total events, purchases, browsing")

print('\n' + '='*70)
print('KEY INSIGHT: Watermark prevents infinite state growth')
print('Without watermark, Spark must keep all windows open forever')
print('='*70)

# COMMAND ----------

# DBTITLE 1,Summary
print('='*70)
print('WATERMARKS - BEST PRACTICES')
print('='*70)
print('\nWhen to Use:')
print('  - Time-based window aggregations')
print('  - Late data is bounded (not infinite delay)')
print('  - Need finalized results')

print('\nKey Syntax:')
print('  .withWatermark("event_time", "10 minutes")')

print('\nCommon Mistakes:')
print('  - Using processing time instead of event time')
print('  - Watermark too short (drops valid data)')
print('  - No watermark (infinite state growth)')

print('\nInterview Talking Point:')
print('  "Watermarks solve two problems: late data handling and')
print('   state management. By setting a tolerance threshold,')
print('   we include delayed events while finalizing old windows')
print('   to prevent memory growth."')
print('='*70)

# COMMAND ----------

# DBTITLE 1,Simulate Watermark Logic (Order-Based)
# SIMULATE WATERMARK BEHAVIOR - Process events by event_timestamp order
print('='*70)
print('🔍 SIMULATING WATERMARK LOGIC (WITHOUT ACTUAL STREAMING)')
print('='*70)

# Read all events from bronze
df_all_events = spark.read.table(BRONZE_TABLE) \
    .select(
        col('CustomerID'),
        col('OrderID'),
        col('ProductName'),
        col('event_timestamp'),
        col('TotalAmount')
    ) \
    .orderBy('event_timestamp')

print("\n📊 Event Timestamp Range:")
print("-" * 70)

min_ts = df_all_events.select(min('event_timestamp')).collect()[0][0]
max_ts = df_all_events.select(max('event_timestamp')).collect()[0][0]
total = df_all_events.count()

print(f"   Earliest event: {min_ts}")
print(f"   Latest event: {max_ts}")
print(f"   Total events: {total}")
print(f"   Time span: {(max_ts.timestamp() - min_ts.timestamp()) / 60:.1f} minutes")

# Simulate watermark: For each event, calculate if it would be dropped
# based on the max timestamp seen UP TO THAT POINT
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, unix_timestamp

window_spec = Window.orderBy('event_timestamp').rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_with_watermark = df_all_events \
    .withColumn(
        'max_event_ts_so_far',
        max('event_timestamp').over(window_spec)
    ) \
    .withColumn(
        'watermark_threshold',
        expr("max_event_ts_so_far - INTERVAL '10' MINUTES")
    ) \
    .withColumn(
        'would_be_dropped',
        when(col('event_timestamp') < col('watermark_threshold'), 'DROPPED').otherwise('INCLUDED')
    ) \
    .withColumn(
        'minutes_behind_watermark',
        round((unix_timestamp('watermark_threshold') - unix_timestamp('event_timestamp')) / 60, 2)
    )

print("\n🎯 Watermark Simulation Results:")
print("-" * 70)

# Overall stats
included_count = df_with_watermark.filter(col('would_be_dropped') == 'INCLUDED').count()
dropped_count = df_with_watermark.filter(col('would_be_dropped') == 'DROPPED').count()

print(f"\n   ✅ Events INCLUDED: {included_count} ({included_count/total*100:.1f}%)")
print(f"   ❌ Events DROPPED: {dropped_count} ({dropped_count/total*100:.1f}%)")

print("\n💡 How Watermark Works:")
print("   1. Spark tracks max event_timestamp seen so far")
print("   2. Watermark = max_event_timestamp - 10 minutes")
print("   3. Events older than watermark are DROPPED")
print("   4. This prevents infinite state growth in windows")

print("\n📋 Sample Events (showing watermark calculation):")
print("-" * 70)

# Show a sample with watermark progression
sample_events = df_with_watermark \
    .select(
        'CustomerID',
        'OrderID',
        'event_timestamp',
        'max_event_ts_so_far',
        'watermark_threshold',
        'minutes_behind_watermark',
        'would_be_dropped'
    ) \
    .limit(20)

display(sample_events)

print("\n🚨 Events That Would Be DROPPED (10-min watermark):")
print("-" * 70)

dropped_sample = df_with_watermark \
    .filter(col('would_be_dropped') == 'DROPPED') \
    .select(
        'CustomerID',
        'OrderID',
        'ProductName',
        'event_timestamp',
        'watermark_threshold',
        'minutes_behind_watermark'
    ) \
    .orderBy(col('minutes_behind_watermark').desc()) \
    .limit(10)

if dropped_count > 0:
    display(dropped_sample)
    print(f"\n   These {dropped_count} events arrived too late (>10 min behind max timestamp)")
else:
    print("\n   🎉 NO EVENTS DROPPED! All events within 10-min watermark window")
    print("   This means your event timestamps are tightly clustered (< 10 min spread)")

print('\n' + '='*70)
print('KEY TAKEAWAY: Watermark = max_event_timestamp - delay')
print('NOT: processing_time - event_timestamp')
print('='*70)

# COMMAND ----------

# DBTITLE 1,Create Out-of-Order Test Data
# CREATE TEST DATA WITH TRUE OUT-OF-ORDER ARRIVALS
print('='*70)
print('🧪 CREATING OUT-OF-ORDER TEST DATA')
print('='*70)

from datetime import datetime, timedelta
import random

# Create sample events that arrive OUT OF ORDER
base_time = datetime(2026, 7, 17, 15, 0, 0)  # 3:00 PM today

# Simulate events arriving in this ORDER (not chronological!):
events_data = [
    # Batch 1: Early events (15:00 - 15:05)
    (1, base_time, 100.0, 'Batch 1'),
    (2, base_time + timedelta(minutes=2), 150.0, 'Batch 1'),
    (3, base_time + timedelta(minutes=5), 200.0, 'Batch 1'),
    
    # Batch 2: Jump ahead 25 minutes (15:30) - WATERMARK MOVES TO 15:20
    (4, base_time + timedelta(minutes=30), 300.0, 'Batch 2 - JUMPS AHEAD!'),
    (5, base_time + timedelta(minutes=32), 350.0, 'Batch 2'),
    (6, base_time + timedelta(minutes=35), 400.0, 'Batch 2'),
    
    # Batch 3: LATE ARRIVALS - Events from 15:10-15:15 arrive AFTER Batch 2
    # Watermark is now 15:25 (15:35 - 10 min)
    # These events (15:10-15:15) are OLDER than watermark → DROPPED!
    (7, base_time + timedelta(minutes=10), 250.0, 'Batch 3 - TOO LATE! 15 min behind'),
    (8, base_time + timedelta(minutes=12), 275.0, 'Batch 3 - TOO LATE! 13 min behind'),
    (9, base_time + timedelta(minutes=15), 280.0, 'Batch 3 - TOO LATE! 10 min behind'),
    
    # Batch 4: More recent events (within watermark)
    (10, base_time + timedelta(minutes=28), 450.0, 'Batch 4 - OK, within watermark'),
    (11, base_time + timedelta(minutes=33), 500.0, 'Batch 4 - OK'),
    
    # Batch 5: Another late arrival
    (12, base_time + timedelta(minutes=20), 320.0, 'Batch 5 - TOO LATE! 13 min behind'),
]

# Create DataFrame in ARRIVAL ORDER (not event_timestamp order!)
from pyspark.sql import Row

test_events = spark.createDataFrame(
    [Row(
        event_id=e[0],
        event_timestamp=e[1],
        amount=e[2],
        batch_label=e[3],
        arrival_order=i+1  # Track arrival sequence
    ) for i, e in enumerate(events_data)]
)

print("\n📊 Test Events (in ARRIVAL order):")
print("-" * 70)
test_events.select(
    'arrival_order',
    'event_id',
    'event_timestamp',
    'amount',
    'batch_label'
).show(20, truncate=False)

print("\n🔍 Now simulating watermark processing...")
print("-" * 70)

# Process events in ARRIVAL order and track watermark
from pyspark.sql.window import Window

# Sort by ARRIVAL order (not timestamp!)
window_spec = Window.orderBy('arrival_order').rowsBetween(Window.unboundedPreceding, Window.currentRow)

test_with_watermark = test_events \
    .withColumn(
        'max_event_ts_so_far',
        max('event_timestamp').over(window_spec)
    ) \
    .withColumn(
        'watermark_threshold',
        expr("max_event_ts_so_far - INTERVAL '10' MINUTES")
    ) \
    .withColumn(
        'minutes_behind_watermark',
        round((unix_timestamp('watermark_threshold') - unix_timestamp('event_timestamp')) / 60, 2)
    ) \
    .withColumn(
        'would_be_dropped',
        when(col('event_timestamp') < col('watermark_threshold'), 'DROPPED ❌').otherwise('INCLUDED ✅')
    ) \
    .orderBy('arrival_order')

print("\n🎯 Watermark Processing Results:")
print("-" * 70)

test_with_watermark.select(
    'arrival_order',
    'event_id',
    'event_timestamp',
    'max_event_ts_so_far',
    'watermark_threshold',
    'minutes_behind_watermark',
    'would_be_dropped'
).show(20, truncate=False)

# Count dropped vs included
included = test_with_watermark.filter(col('would_be_dropped').contains('INCLUDED')).count()
dropped = test_with_watermark.filter(col('would_be_dropped').contains('DROPPED')).count()

print("\n📊 Final Results:")
print("-" * 70)
print(f"   ✅ Events INCLUDED: {included}")
print(f"   ❌ Events DROPPED: {dropped}")
print(f"   📈 Drop rate: {dropped/len(events_data)*100:.1f}%")

print("\n💡 Key Insight:")
print("   When Event 4 arrives at 15:30, watermark moves to 15:20")
print("   Events 7, 8, 9, 12 (from 15:10-15:20) arrive AFTER this")
print("   They're >10 min behind the watermark → DROPPED!")

print("\n🚨 Dropped Events Detail:")
print("-" * 70)
dropped_detail = test_with_watermark \
    .filter(col('would_be_dropped').contains('DROPPED')) \
    .select(
        'event_id',
        'event_timestamp',
        'batch_label',
        'minutes_behind_watermark'
    )

dropped_detail.show(truncate=False)

print('\n' + '='*70)
print('THIS IS THE REAL WATERMARK BEHAVIOR!')
print('Late events (>10 min behind max) are DROPPED to prevent infinite state')
print('='*70)

# COMMAND ----------

# DBTITLE 1,Connection to SkillAIPath DLT Pipeline
# MAGIC %md
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔗 How This Applies to YOUR SkillAIPath Pipeline
# MAGIC
# MAGIC ### Your Current Setup:
# MAGIC
# MAGIC ```
# MAGIC Faker (Synthetic Data) → Bronze Tables → DLT Pipeline → Silver (SCD Type 2)
# MAGIC ```
# MAGIC
# MAGIC **Bronze Tables:**
# MAGIC - `sap_dev.bronze.stg_sales` (with TransactionDate)
# MAGIC - `sap_dev.bronze.stg_purchase` (with PurchaseDate)
# MAGIC - `sap_dev.bronze.stg_stock_status` (with StatusDate)
# MAGIC
# MAGIC **DLT Pipeline:** `13_enterprise_dlt_silver_cdc_pipeline`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Where Watermarks Would Help:
# MAGIC
# MAGIC If your bronze tables had **late-arriving data** (e.g., delayed file uploads, batch processing), you'd add watermarks to your DLT notebook:
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table(name="silver_sales")
# MAGIC def process_sales():
# MAGIC     return (
# MAGIC         dlt.read_stream("stg_sales")
# MAGIC         .withWatermark("TransactionDate", "1 day")  # ← Watermark here!
# MAGIC         .groupBy(
# MAGIC             window(col("TransactionDate"), "1 day"),
# MAGIC             "CustomerID"
# MAGIC         )
# MAGIC         .agg(sum("NetAmount").alias("daily_revenue"))
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC **Use Case:** If sales data from Monday arrives on Tuesday, the 1-day watermark ensures it's still included in Monday's aggregation.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📂 Files Referenced:
# MAGIC - **This Demo:** In-memory test data (portable, works anywhere)
# MAGIC - **Your Pipeline:** `/Shared/dev/SkillAIPath/.../13_enterprise_dlt_silver_cdc_pipeline`
# MAGIC - **Setup Notebook:** `/Shared/dev/SkillAIPath/.../00_setup_silver_dlt_pipeline`
# MAGIC
# MAGIC **Next Step:** Review your DLT pipeline notebook to see if time-based aggregations need watermarks!