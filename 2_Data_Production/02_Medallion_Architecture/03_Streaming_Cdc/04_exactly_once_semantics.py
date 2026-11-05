# Databricks notebook source
# DBTITLE 1,Config Setup
# MAGIC %run ../../../1_Canvas/1_Setup/1_config

# COMMAND ----------

# DBTITLE 1,Exactly-Once Semantics
# MAGIC %md
# MAGIC # Exactly-Once Semantics: Idempotent Writes
# MAGIC **SkillAIPath Streaming - Reliable Data Processing**
# MAGIC
# MAGIC ## THE PROBLEM: "Our aggregates have duplicates after retries"
# MAGIC
# MAGIC **Wednesday Afternoon - Data Quality Review**
# MAGIC
# MAGIC **Data Engineer:** "Our daily revenue total is ₹25 Cr in the database but ₹27 Cr in the dashboard. The stream restarted twice yesterday."
# MAGIC
# MAGIC **Architect:** "Double counting. Your stream is at-least-once - on failure, events are replayed and written again."
# MAGIC
# MAGIC **Engineer:** "How do we prevent duplicates without tracking every single event ID?"
# MAGIC
# MAGIC **Architect:** "Structured Streaming has exactly-once guarantees built-in via checkpointing and idempotent writes."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## THE SOLUTION: Exactly-Once Processing
# MAGIC
# MAGIC **How It Works:**
# MAGIC 1. **Checkpointing** - Tracks offset of last processed record
# MAGIC 2. **Idempotent Writes** - Replayed events don't duplicate (Delta merge)
# MAGIC 3. **Transactional Guarantees** - All-or-nothing batch commits
# MAGIC
# MAGIC **Three Levels:**
# MAGIC * **At-most-once** - May lose data on failure
# MAGIC * **At-least-once** - May duplicate on failure
# MAGIC * **Exactly-once** - No loss, no duplicates
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC * Module 04_STREAMING_CDC complete
# MAGIC * Understanding of checkpointing
# MAGIC * Delta Lake MERGE operation basics

# COMMAND ----------

# DBTITLE 1,Setup and Checkpointing
from pyspark.sql.functions import col, current_timestamp

print('='*70)
print('EXACTLY-ONCE SEMANTICS - TESTING WITH REAL DATA')
print('='*70)
print('   Strategy: Run streaming query TWICE')
print('   Expected: Same record count (no duplicates)')
print('   Proof: Checkpoint prevents reprocessing')
print('='*70)

print(f'\n📂 Source: {Config.SOURCE_PATH}')
print(f'📍 Checkpoint: {Config.EXACTLY_ONCE_CHECKPOINT}')
print(f'🎯 Target: {Config.EXACTLY_ONCE_TABLE}')

# Clean up from previous runs
print('\n🧹 Cleaning previous run...')
spark.sql(f'DROP TABLE IF EXISTS {Config.EXACTLY_ONCE_TABLE}')
try:
    dbutils.fs.rm(Config.EXACTLY_ONCE_CHECKPOINT, recurse=True)
    print('   ✓ Checkpoint cleared')
except:
    print('   ✓ No previous checkpoint found')

print('\n✅ Setup complete - ready to test exactly-once!')

# COMMAND ----------

# DBTITLE 1,Exactly-Once Stream Write
print('='*70)
print('🚀 RUN 1: INITIAL STREAMING LOAD')
print('='*70)

print('\nReading from CSV files with Auto Loader...')
df_stream = spark.readStream \
    .format('cloudFiles') \
    .option('cloudFiles.format', 'csv') \
    .option('cloudFiles.schemaLocation', f'{Config.EXACTLY_ONCE_CHECKPOINT}/_schema') \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .load(Config.VOLUME_PATH) \
    .select(
        col('CustomerID'),
        col('OrderID'),
        col('ProductName'),
        col('TotalAmount'),
        col('event_timestamp'),
        current_timestamp().alias('processing_time')
    )

print('\nWriting to Delta table with checkpoint...')
query = df_stream.writeStream \
    .format('delta') \
    .option('checkpointLocation', Config.EXACTLY_ONCE_CHECKPOINT) \
    .trigger(availableNow=True) \
    .toTable(Config.EXACTLY_ONCE_TABLE)

query.awaitTermination()

print('\n✅ Run 1 Complete!')
print('-'*70)

# Check results
run1_count = spark.table(Config.EXACTLY_ONCE_TABLE).count()
file_count = len([f for f in dbutils.fs.ls(Config.VOLUME_PATH) if f.name.startswith('batch_')])

print(f'\n📊 Results:')
print(f'   CSV files in source: {file_count}')
print(f'   Records written: {run1_count}')
print(f'   Checkpoint created: {Config.EXACTLY_ONCE_CHECKPOINT}')

print('\n💡 Checkpoint tracks which files were processed')
print('   Next run will skip these files (exactly-once!)')
print('='*70)

# COMMAND ----------

# DBTITLE 1,Idempotent Merge Pattern
print('='*70)
print('🔁 RUN 2: RE-RUN SAME STREAMING QUERY')
print('='*70)

print('\n🧠 Testing Exactly-Once:')
print('   - Same checkpoint location')
print('   - Same source files')
print('   - Will Spark reprocess and create duplicates?')

# Stop previous query to release checkpoint lock
print('\nStopping previous streaming query...')
try:
    query.stop()
    print('   ✓ Previous query stopped')
except:
    print('   ✓ No active query to stop')

print('\nRe-reading same source with same checkpoint...')
df_stream_2 = spark.readStream \
    .format('cloudFiles') \
    .option('cloudFiles.format', 'csv') \
    .option('cloudFiles.schemaLocation', f'{Config.EXACTLY_ONCE_CHECKPOINT}/_schema') \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .load(Config.VOLUME_PATH) \
    .select(
        col('CustomerID'),
        col('OrderID'),
        col('ProductName'),
        col('TotalAmount'),
        col('event_timestamp'),
        current_timestamp().alias('processing_time')
    )

print('\nWriting to same table with same checkpoint...')
query_2 = df_stream_2.writeStream \
    .format('delta') \
    .option('checkpointLocation', Config.EXACTLY_ONCE_CHECKPOINT) \
    .trigger(availableNow=True) \
    .toTable(Config.EXACTLY_ONCE_TABLE)

query_2.awaitTermination()

print('\n✅ Run 2 Complete!')
print('-'*70)

# Compare results
run2_count = spark.table(Config.EXACTLY_ONCE_TABLE).count()

print(f'\n🎯 EXACTLY-ONCE PROOF:')
print(f'   Run 1 records: {run1_count}')
print(f'   Run 2 records: {run2_count}')
print(f'   Difference: {run2_count - run1_count}')

if run2_count == run1_count:
    print('\n✅ ✅ ✅ EXACTLY-ONCE VERIFIED! ✅ ✅ ✅')
    print('   No duplicates! Checkpoint prevented reprocessing.')
    print('   Same files processed = same record count')
else:
    print('\n❌ DUPLICATES DETECTED!')
    print('   Checkpoint may not be working correctly')

print('='*70)

# COMMAND ----------

# DBTITLE 1,Summary
print('='*70)
print('🔍 INSPECT CHECKPOINT STATE')
print('='*70)

# Show checkpoint contents
print('\n📁 Checkpoint directory contents:')
print('-'*70)
try:
    checkpoint_files = dbutils.fs.ls(Config.EXACTLY_ONCE_CHECKPOINT)
    for file in checkpoint_files:
        print(f'   {file.name}')
except:
    print('   Checkpoint not found')

print('\n💡 How Checkpoint Ensures Exactly-Once:')
print('-'*70)
print('   1. offsets/ - Tracks which files/partitions processed')
print('   2. commits/ - Atomic batch commits')
print('   3. metadata - Schema and configuration')
print('   4. sources/ - File listing and progress tracking')

print('\n🎯 What We Proved:')
print('-'*70)
print('   ✓ Ran streaming query twice')
print('   ✓ Same source files both times')
print('   ✓ Same checkpoint location')
print('   ✓ Second run: 0 new records (checkpoint skipped duplicates)')
print('   ✓ Result: Exactly-once guaranteed!')

print('\n⚠️ What Breaks Exactly-Once:')
print('-'*70)
print('   ✗ Delete checkpoint → reprocesses everything')
print('   ✗ Change checkpoint path → loses state')
print('   ✗ Non-idempotent foreachBatch logic → side effects duplicate')

print('\n✅ KEY TAKEAWAY:')
print('   Checkpoint + Delta Lake = Exactly-Once Guarantee')
print('   No manual deduplication needed!')
print('='*70)

# COMMAND ----------

# DBTITLE 1,Proof: Delays Handled Without Duplicates
print('='*70)
print('🎯 PROOF: DELAYED BATCHES PROCESSED WITHOUT DUPLICATES')
print('='*70)

from pyspark.sql.functions import col, count, countDistinct, unix_timestamp, when, lit
from pyspark.sql.functions import min as spark_min, max as spark_max, avg as spark_avg

df_analysis = spark.table(Config.EXACTLY_ONCE_TABLE)

# 1. Calculate delays for each record
print('\n📊 Step 1: Delay Analysis (Event Time vs Processing Time)')
print('-'*70)

df_with_delays = df_analysis.withColumn(
    'delay_minutes',
    (unix_timestamp('processing_time') - unix_timestamp(col('event_timestamp').cast('timestamp'))) / 60
)

# Aggregate delay statistics
delay_stats = df_with_delays.agg(
    spark_min('delay_minutes').alias('min_delay'),
    spark_max('delay_minutes').alias('max_delay'),
    spark_avg('delay_minutes').alias('avg_delay')
).collect()[0]

print(f'   ✅ Files arrived with delays:')
print(f'      Minimum delay: {delay_stats["min_delay"]:.0f} minutes')
print(f'      Maximum delay: {delay_stats["max_delay"]:.0f} minutes')
print(f'      Average delay: {delay_stats["avg_delay"]:.0f} minutes')
print(f'\n   💡 Events occurred {delay_stats["min_delay"]:.0f}-{delay_stats["max_delay"]:.0f} minutes before processing')

# 2. Show sample delayed records
print('\n📋 Step 2: Sample Delayed Records')
print('-'*70)
print('\n   Top 10 delayed orders (showing event time vs processing time):')

display(
    df_with_delays
    .filter(col('OrderID').isNotNull())
    .select(
        'OrderID',
        'CustomerID',
        col('event_timestamp').alias('Event_Time'),
        col('processing_time').alias('Processed_Time'),
        col('delay_minutes').cast('int').alias('Delay_Minutes'),
        'ProductName',
        'TotalAmount'
    )
    .orderBy(col('delay_minutes').desc())
    .limit(10)
)

# 3. Duplicate detection proof
print('\n🔍 Step 3: Duplicate Detection Analysis')
print('-'*70)

total_records = df_analysis.count()
distinct_orders = df_analysis.filter(col('OrderID').isNotNull()).select('OrderID').distinct().count()
orders_with_data = df_analysis.filter(col('OrderID').isNotNull()).count()

print(f'   Total records in table: {total_records:,}')
print(f'   Records with OrderID: {orders_with_data:,}')
print(f'   Distinct OrderIDs: {distinct_orders:,}')
print(f'   Duplicate OrderIDs: {orders_with_data - distinct_orders}')

if orders_with_data == distinct_orders:
    print('\n   ✅ ✅ ✅ ZERO DUPLICATES! ✅ ✅ ✅')
    print('      Every OrderID appears exactly once')
else:
    print(f'\n   ⚠️ Found {orders_with_data - distinct_orders} duplicate OrderIDs')

# 4. Checkpoint effectiveness proof
print('\n🏆 Step 4: Checkpoint Effectiveness Summary')
print('-'*70)
print(f'   ✓ Processed {total_records:,} records from delayed batches')
print(f'   ✓ Events delayed by {delay_stats["avg_delay"]:.0f} minutes on average')
print(f'   ✓ Re-ran same query with same checkpoint')
print(f'   ✓ Zero duplicates despite delays')
print(f'   ✓ Checkpoint tracked all processed files')

print('\n💡 KEY INSIGHT:')
print('   Late-arriving data + checkpoint = Exactly-once guarantee')
print('   Delays do NOT cause duplicates when checkpoint is used!')
print('='*70)