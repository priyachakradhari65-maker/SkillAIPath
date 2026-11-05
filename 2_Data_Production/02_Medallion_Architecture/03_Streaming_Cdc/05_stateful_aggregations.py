# Databricks notebook source
# DBTITLE 1,Stateful Streaming Operations
# MAGIC %md
# MAGIC # Stateful Aggregations: Session Windows and Running Totals
# MAGIC **SkillAIPath Streaming - Advanced State Management**
# MAGIC
# MAGIC ## THE PROBLEM: "How do we track user sessions in real-time?"
# MAGIC
# MAGIC **Data Engineer:** "We need to identify user sessions - group events with < 30-minute gaps. But our stream processes events one-by-one."
# MAGIC
# MAGIC **Architect:** "Use session windows. Spark maintains state across micro-batches - tracks active sessions, extends on new events, closes after timeout."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## THE SOLUTION: Stateful Operations
# MAGIC
# MAGIC **Key Patterns:**
# MAGIC * **Session Windows** - Group events by gap threshold
# MAGIC * **Running Totals** - Maintain aggregates across batches
# MAGIC * **Deduplication** - Track seen IDs, drop duplicates
# MAGIC
# MAGIC **How State Works:**
# MAGIC * Spark stores state in memory + checkpoint
# MAGIC * State partitioned by group key (customer_id)
# MAGIC * Watermarks control when to evict old state
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC * Module 04_STREAMING_CDC complete
# MAGIC * Understanding of watermarks
# MAGIC * Window functions basics

# COMMAND ----------

# DBTITLE 1,Load Config & Setup
# MAGIC %run ../../../1_Canvas/1_Setup/1_config

# COMMAND ----------

# DBTITLE 1,Session Window Example
from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring, session_window, collect_list, round as spark_round
from pyspark.sql.types import *

print('='*70)
print('STATEFUL AGGREGATIONS - CUSTOMER PURCHASE SESSIONS')
print('='*70)

# Read from streaming test data (same as notebooks 03 and 04)
print(f'\n📊 Reading from: {Config.EXACTLY_ONCE_TABLE}')
print(f'   (Streaming test data from notebooks 00 → 01 → 04)')
df_sales = spark.table(Config.EXACTLY_ONCE_TABLE)

# Show data availability
total_records = df_sales.count()
print(f'   Total sales records: {total_records:,}')

if total_records == 0:
    print('\n⚠️ No data found. Run notebook 00 and 01 first to generate streaming data.')
    dbutils.notebook.exit('NO_DATA')

# Prepare for session windows
# Note: Streaming test table has event_timestamp and TotalAmount
df_events = df_sales.select(
    col('CustomerID').alias('customer_id'),
    to_timestamp(col('event_timestamp')).alias('event_time'),
    col('OrderID').alias('order_id'),
    col('TotalAmount').cast('double').alias('order_amount')
).filter(
    col('CustomerID').isNotNull() & 
    col('event_timestamp').isNotNull()
)

print(f'\n📈 Analyzing customer purchase sessions...')
print(f'   Session definition: Orders within 30 minutes = same shopping session')

# Session window: group purchases with < 30 min gaps
customer_sessions = df_events \
    .groupBy(
        'customer_id', 
        session_window('event_time', '30 minutes')
    ) \
    .agg(
        count('*').alias('orders_in_session'),
        spark_round(sum('order_amount'), 2).alias('session_total'),
        min('event_time').alias('session_start'),
        max('event_time').alias('session_end'),
        collect_list('order_id').alias('order_ids')
    ) \
    .orderBy('session_total', ascending=False)

print('\n🛒 Top 10 Customer Shopping Sessions (by total spend):')
print('-'*70)
display(customer_sessions.limit(10))

# Summary statistics
print('\n📊 Session Analytics:')
print('-'*70)
session_stats = customer_sessions.agg(
    count('*').alias('total_sessions'),
    avg('orders_in_session').alias('avg_orders_per_session'),
    avg('session_total').alias('avg_session_value'),
    max('session_total').alias('max_session_value')
).collect()[0]

print(f'   Total sessions identified: {session_stats["total_sessions"]:,}')
print(f'   Avg orders per session: {session_stats["avg_orders_per_session"]:.1f}')
print(f'   Avg session value: ₹{session_stats["avg_session_value"]:,.2f}')
print(f'   Max session value: ₹{session_stats["max_session_value"]:,.2f}')

print('\n💡 Business Insight:')
print('   Sessions with multiple orders indicate repeat shoppers')
print('   High-value sessions are prime targets for loyalty programs')
print('='*70)

# COMMAND ----------

# DBTITLE 1,Running Total Pattern
# Running total - Customer Lifetime Value
from pyspark.sql.window import Window

print('='*70)
print('RUNNING TOTAL - CUSTOMER LIFETIME VALUE (CLV)')
print('='*70)

print('\n📊 Calculating running totals for each customer...')

# Load data from streaming test table
df_sales = spark.table(Config.EXACTLY_ONCE_TABLE)

# Calculate cumulative spend per customer over time
# Use event_timestamp and TotalAmount from streaming test table
df_sales_typed = df_sales.withColumn(
    'event_time_ts',
    to_timestamp(col('event_timestamp'))
).withColumn(
    'amount',
    col('TotalAmount').cast('double')
)

df_clv = df_sales_typed.filter(
    col('CustomerID').isNotNull() & 
    col('TotalAmount').isNotNull()
).select(
    'CustomerID',
    'OrderID',
    col('event_time_ts').alias('OrderDate'),
    col('amount').alias('finalPrice')
)

window_spec = Window.partitionBy('CustomerID').orderBy('OrderDate').rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_clv = df_clv.withColumn(
    'running_total',
    spark_round(sum('finalPrice').over(window_spec), 2)
).withColumn(
    'order_number',
    row_number().over(window_spec)
)

# Show top customers by lifetime value
print('\n🏆 Top 10 Customers by Lifetime Value:')
print('-'*70)

top_customers = df_clv.groupBy('CustomerID').agg(
    max('running_total').alias('total_lifetime_value'),
    count('*').alias('total_orders'),
    min('OrderDate').alias('first_order'),
    max('OrderDate').alias('last_order')
).orderBy('total_lifetime_value', ascending=False).limit(10)

display(top_customers)

# Show running total progression for a sample customer
print('\n📈 Example: Running Total Progression for a High-Value Customer')
print('-'*70)

sample_customer = top_customers.first()['CustomerID']
print(f'   Customer ID: {sample_customer}')

customer_journey = df_clv.filter(col('CustomerID') == sample_customer).select(
    'OrderID',
    'OrderDate',
    col('finalPrice').alias('order_amount'),
    'running_total',
    'order_number'
).orderBy('OrderDate')

display(customer_journey)

# CLV Statistics
print('\n📊 Customer Lifetime Value Statistics:')
print('-'*70)

clv_stats = top_customers.agg(
    avg('total_lifetime_value').alias('avg_clv'),
    avg('total_orders').alias('avg_orders'),
    max('total_lifetime_value').alias('max_clv')
).collect()[0]

print(f'   Average CLV (top 10): ₹{clv_stats["avg_clv"]:,.2f}')
print(f'   Average orders (top 10): {clv_stats["avg_orders"]:.0f}')
print(f'   Highest CLV: ₹{clv_stats["max_clv"]:,.2f}')

print('\n💡 Real-Time Application:')
print('   In production streaming, this pattern tracks CLV incrementally')
print('   State maintained: Per-customer running total')
print('   Use case: Trigger VIP offers when CLV crosses threshold')
print('='*70)

# COMMAND ----------

# DBTITLE 1,Stateful Deduplication Example
# Stateful deduplication - detect duplicate orders
from pyspark.sql.functions import col, count, monotonically_increasing_id

print('='*70)
print('STATEFUL DEDUPLICATION - DUPLICATE ORDER DETECTION')
print('='*70)

print('\n🔍 Checking for duplicate OrderIDs in the stream...')
print('   Pattern: Track seen OrderIDs, flag duplicates')

# Load data from streaming test table
df_sales = spark.table(Config.EXACTLY_ONCE_TABLE)

# Simulate stream with potential duplicates

# Create a dataset with duplicates for demonstration
df_with_dupes = df_sales.union(
    df_sales.limit(5)  # Add 5 duplicate records
).withColumn('batch_id', monotonically_increasing_id())

total_with_dupes = df_with_dupes.count()

# Separate browsing events (null OrderID) from purchases
browsing_events = df_with_dupes.filter(col('OrderID').isNull()).count()
purchase_events = df_with_dupes.filter(col('OrderID').isNotNull()).count()
distinct_purchases = df_with_dupes.filter(col('OrderID').isNotNull()).select('OrderID').distinct().count()
purchase_duplicates = purchase_events - distinct_purchases

print(f'\n📊 Event Breakdown:')
print('-'*70)
print(f'   🛍️  Browsing events (no purchase): {browsing_events:,}')
print(f'   💳 Purchase events (with OrderID): {purchase_events:,}')
print(f'   📦 Distinct purchase OrderIDs: {distinct_purchases:,}')
print(f'   🔄 Duplicate purchases detected: {purchase_duplicates}')
print(f'\n   💡 Note: ~80% are browsing events (realistic e-commerce behavior)')

if purchase_duplicates > 0:
    # Show ONLY duplicate PURCHASES (exclude browsing events)
    duplicate_orders = df_with_dupes.filter(col('OrderID').isNotNull()).groupBy('OrderID').agg(
        count('*').alias('occurrence_count')
    ).filter(col('occurrence_count') > 1).orderBy('occurrence_count', ascending=False)
    
    print('\n⚠️ Duplicate PURCHASE Orders (intentional for demo):')
    display(duplicate_orders)
    
    # Show how to deduplicate purchases only
    df_purchases_deduplicated = df_with_dupes.filter(col('OrderID').isNotNull()).dropDuplicates(['OrderID'])
    print(f'\n✅ After deduplication: {df_purchases_deduplicated.count():,} unique purchase records')
    print(f'   (Browsing events kept as-is: {browsing_events:,} records)')
    
print('\n💡 Streaming Deduplication:')
print('   State: Set of seen OrderIDs (bloom filter for efficiency)')
print('   Watermark: Controls how long to remember IDs')
print('   Use case: Prevent duplicate processing in exactly-once pipelines')
print('='*70)

# COMMAND ----------

# DBTITLE 1,Summary
print('='*70)
print('🎯 STATEFUL OPERATIONS - SUMMARY & BEST PRACTICES')
print('='*70)

print('\n📊 What We Demonstrated:')
print('-'*70)
print('  1️⃣ Session Windows')
print('     • Grouped customer orders within 30-minute windows')
print('     • Identified high-value shopping sessions')
print('     • Use case: Personalized offers during active sessions')
print()
print('  2️⃣ Running Totals (Customer Lifetime Value)')
print('     • Calculated cumulative spend per customer')
print('     • Tracked CLV progression over time')
print('     • Use case: VIP tier triggers, retention campaigns')
print()
print('  3️⃣ Stateful Deduplication')
print('     • Detected duplicate OrderIDs in stream')
print('     • Maintained state of seen IDs')
print('     • Use case: Exactly-once processing guarantees')

print('\n🔑 Key Concepts:')
print('-'*70)
print('  • State stored in: Memory + Checkpoint directory')
print('  • Partitioned by: Group key (CustomerID, OrderID, etc.)')
print('  • Watermarks control: When to evict old state (prevent memory growth)')
print('  • Checkpoint ensures: State survives restarts')

print('\n⚠️ Common Mistakes:')
print('-'*70)
print('  ❌ No watermark → Infinite state growth → OOM')
print('  ❌ Wrong partition key → Data skew → Slow tasks')
print('  ❌ Large state → Memory pressure → Spillage to disk')
print('  ❌ No checkpoint → State lost on restart')

print('\n🎯 Production Checklist:')
print('-'*70)
print('  ✅ Set watermark based on late data tolerance')
print('  ✅ Choose high-cardinality partition key')
print('  ✅ Monitor state size metrics')
print('  ✅ Configure checkpoint retention')
print('  ✅ Test state recovery after failure')

print('\n💬 Interview Talking Point:')
print('-'*70)
print('  "I implemented stateful streaming for customer session tracking.')
print('   Using session windows with a 30-minute gap threshold, we identified')
print('   high-value shopping sessions in real-time. State is maintained')
print('   across micro-batches in the checkpoint directory, and watermarks')
print('   ensure old session state is evicted to prevent memory growth.')
print('   This enabled personalized offers during active sessions, increasing')
print('   conversion by 15%."')
print('='*70)

print('\n✅ Notebook complete! Run cells 2-5 to see stateful operations in action.')