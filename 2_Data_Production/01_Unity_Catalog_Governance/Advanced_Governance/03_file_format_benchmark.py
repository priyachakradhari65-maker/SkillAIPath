# Databricks notebook source
# DBTITLE 1,File Format Benchmark
# MAGIC %md
# MAGIC # File Format Benchmark: Parquet vs Avro vs ORC
# MAGIC **SkillAIPath Data Warehouse - Storage Optimization**
# MAGIC
# MAGIC ## THE PROBLEM: "Which file format should we use?"
# MAGIC
# MAGIC **Thursday Afternoon - Performance Review**
# MAGIC
# MAGIC **Data Engineer:** "Our fact_orders table is 25 GB. Query performance is slow on date filters."
# MAGIC
# MAGIC **Architect:** "What format are you using?"
# MAGIC
# MAGIC **Engineer:** "Delta Lake, which uses Parquet internally. Should we consider other formats?"
# MAGIC
# MAGIC **Architect:** "Delta with Parquet is the right choice for analytics. But let's benchmark to understand why."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## File Format Overview
# MAGIC
# MAGIC **Parquet:**
# MAGIC * Columnar storage (optimized for analytical queries)
# MAGIC * Excellent compression ratios
# MAGIC * Schema evolution support
# MAGIC * Default for Delta Lake
# MAGIC
# MAGIC **Avro:**
# MAGIC * Row-based storage (optimized for full-row reads)
# MAGIC * Schema evolution with backward/forward compatibility
# MAGIC * Used in streaming pipelines (Kafka)
# MAGIC * Compact binary format
# MAGIC
# MAGIC **ORC:**
# MAGIC * Columnar storage (Hive ecosystem)
# MAGIC * Built-in indexes and statistics
# MAGIC * Type-aware compression
# MAGIC * Strong in Hive/Presto environments
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC **Required:**
# MAGIC * Module 02_FACTS complete (fact_orders table exists)
# MAGIC * Understanding of storage formats
# MAGIC * Performance testing basics

# COMMAND ----------

# DBTITLE 1,Create Sample Dataset
from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring, date_add, current_date
from pyspark.sql.types import *
import time

print('='*70)
print('FILE FORMAT BENCHMARK')
print('='*70)

# Create sample fact table (1M rows)
df = spark.range(0, 1000000) \
  .withColumn('order_id', concat(lit('ORD'), col('id'))) \
  .withColumn('order_date', date_add(current_date(), (-col('id') % 365).cast('int'))) \
  .withColumn('customer_id', concat(lit('CUST'), (col('id') % 10000).cast('string'))) \
  .withColumn('product_id', concat(lit('PROD'), (col('id') % 500).cast('string'))) \
  .withColumn('quantity', (col('id') % 10 + 1)) \
  .withColumn('unit_price', (col('id') % 1000 + 100)) \
  .withColumn('total_amount', col('quantity') * col('unit_price')) \
  .drop('id')

print(f'Created sample dataset: {df.count():,} rows')
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Write in Multiple Formats
# Unity Catalog Volume path (works on FREE edition)
volume_path = '/Volumes/sap_dev/bronze/benchmark_data'

# Write as Parquet
start = time.time()
df.write.mode('overwrite').format('parquet').save(f'{volume_path}/parquet')
parquet_write_time = time.time() - start

# Write as Avro
start = time.time()
df.write.mode('overwrite').format('avro').save(f'{volume_path}/avro')
avro_write_time = time.time() - start

# Write as ORC
start = time.time()
df.write.mode('overwrite').format('orc').save(f'{volume_path}/orc')
orc_write_time = time.time() - start

print('='*70)
print('WRITE PERFORMANCE')
print('-'*70)
print(f'Parquet: {parquet_write_time:.2f}s')
print(f'Avro:    {avro_write_time:.2f}s')
print(f'ORC:     {orc_write_time:.2f}s')
print('='*70)

# COMMAND ----------

# DBTITLE 1,Read Performance Benchmark
# Full scan read
start = time.time()
spark.read.parquet(f'{volume_path}/parquet').count()
parquet_read_full = time.time() - start

start = time.time()
spark.read.format('avro').load(f'{volume_path}/avro').count()
avro_read_full = time.time() - start

start = time.time()
spark.read.orc(f'{volume_path}/orc').count()
orc_read_full = time.time() - start

print('='*70)
print('READ PERFORMANCE - FULL SCAN')
print('-'*70)
print(f'Parquet: {parquet_read_full:.2f}s')
print(f'Avro:    {avro_read_full:.2f}s')
print(f'ORC:     {orc_read_full:.2f}s')
print('='*70)

# COMMAND ----------

# DBTITLE 1,Column Projection Benchmark
# Column projection (read only 2 columns)
start = time.time()
spark.read.parquet(f'{volume_path}/parquet').select('order_id', 'total_amount').count()
parquet_projection = time.time() - start

start = time.time()
spark.read.format('avro').load(f'{volume_path}/avro').select('order_id', 'total_amount').count()
avro_projection = time.time() - start

start = time.time()
spark.read.orc(f'{volume_path}/orc').select('order_id', 'total_amount').count()
orc_projection = time.time() - start

print('='*70)
print('COLUMN PROJECTION (2 of 7 columns)')
print('-'*70)
print(f'Parquet: {parquet_projection:.2f}s (columnar benefit)')
print(f'Avro:    {avro_projection:.2f}s (row-based, reads all)')
print(f'ORC:     {orc_projection:.2f}s (columnar benefit)')
print('='*70)

# COMMAND ----------

# DBTITLE 1,Storage Size Comparison
def get_dir_size(path):
    total = 0
    for entry in dbutils.fs.ls(path):
        if not entry.isDir():
            total += entry.size
    return total / (1024*1024)  # MB

parquet_size = get_dir_size(f'{volume_path}/parquet')
avro_size = get_dir_size(f'{volume_path}/avro')
orc_size = get_dir_size(f'{volume_path}/orc')

print('='*70)
print('STORAGE SIZE')
print('-'*70)
print(f'Parquet: {parquet_size:.2f} MB')
print(f'Avro:    {avro_size:.2f} MB')
print(f'ORC:     {orc_size:.2f} MB')
print('='*70)

print('\nSUMMARY:')
print('-'*70)
print('WINNER: Parquet (Delta Lake default)')
print('  - Best compression')
print('  - Columnar = fast analytics queries')
print('  - Schema evolution support')
print('\nAvro: Use for streaming (Kafka), row-based access')
print('ORC: Use if Hive/Presto ecosystem required')
print('='*70)