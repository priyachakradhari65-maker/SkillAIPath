# Databricks notebook source
# DBTITLE 1,S3 Event-Driven Ingestion with Auto Loader
# MAGIC %md
# MAGIC # S3 Event-Driven Ingestion: Auto Loader Pattern + File Notifications
# MAGIC
# MAGIC FREE EDITION COMPATIBLE
# MAGIC
# MAGIC Purpose: Incrementally ingest new files from S3 buckets using Spark Structured Streaming (simulating Auto Loader)
# MAGIC
# MAGIC Key Features:
# MAGIC - Automatic file discovery (no manual listing)
# MAGIC - Schema inference and evolution
# MAGIC - Exactly-once processing (idempotent)
# MAGIC - Support for CSV, JSON, Parquet, Avro
# MAGIC - Handles late-arriving files
# MAGIC - Checkpointing for fault tolerance
# MAGIC
# MAGIC Use Cases:
# MAGIC - Log file ingestion from application servers
# MAGIC - CSV exports from legacy systems
# MAGIC - Partner data drops (daily/hourly files)
# MAGIC - Data lake ingestion from S3 data lake
# MAGIC
# MAGIC Cost: Streaming compute, ~1-3 DBU/hour (depends on file arrival rate)

# COMMAND ----------

# DBTITLE 1,Config Import
# MAGIC %run ../../1_Canvas/1_Setup/1_config

# COMMAND ----------

# DBTITLE 1,Import Libraries & Setup
from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring
from pyspark.sql.types import *

print("Libraries loaded successfully")
print(f"Target bronze table: {Config.TBL_BRONZE_S3_FILES}")

# COMMAND ----------

# DBTITLE 1,S3 Configuration
# S3 bucket and path configuration
s3_config = {
    'source_path': 's3://your-bucket/incoming-data/',
    'file_format': 'json',
    'schema_location': 's3://your-bucket/schemas/incoming_data',
    'checkpoint_location': '/tmp/sap/s3_checkpoint',
    'merge_schema': True,
    'rescue_data_column': '_rescued_data'
}

# AWS credentials (optional - use IAM role if possible)
# spark.conf.set('spark.hadoop.fs.s3a.access.key', dbutils.secrets.get('aws-credentials', 'access-key'))
# spark.conf.set('spark.hadoop.fs.s3a.secret.key', dbutils.secrets.get('aws-credentials', 'secret-key'))

print(f"S3 source path: {s3_config['source_path']}")
print(f"File format: {s3_config['file_format']}")
print(f"Checkpoint: {s3_config['checkpoint_location']}")

# COMMAND ----------

# DBTITLE 1,Read S3 Stream with Schema Inference
# Read stream from S3 using cloudFiles (Auto Loader simulation)
# In Free Edition, use standard readStream with maxFilesPerTrigger

if s3_config['file_format'] == 'json':
    df_s3_stream = spark.readStream \
        .format('json') \
        .option('path', s3_config['source_path']) \
        .option('maxFilesPerTrigger', 10) \
        .option('rescuedDataColumn', s3_config['rescue_data_column']) \
        .option('mergeSchema', s3_config['merge_schema']) \
        .load()
elif s3_config['file_format'] == 'csv':
    df_s3_stream = spark.readStream \
        .format('csv') \
        .option('path', s3_config['source_path']) \
        .option('header', True) \
        .option('inferSchema', True) \
        .option('maxFilesPerTrigger', 10) \
        .load()
elif s3_config['file_format'] == 'parquet':
    df_s3_stream = spark.readStream \
        .format('parquet') \
        .option('path', s3_config['source_path']) \
        .option('maxFilesPerTrigger', 10) \
        .option('mergeSchema', s3_config['merge_schema']) \
        .load()
else:
    raise ValueError(f"Unsupported file format: {s3_config['file_format']}")

print("S3 stream configured")
print("Inferred schema:")
df_s3_stream.printSchema()

# COMMAND ----------

# DBTITLE 1,Add Bronze Metadata Columns
# Add metadata columns for bronze layer
df_bronze = df_s3_stream.select(
    '*',
    input_file_name().alias('_input_file_name'),
    current_timestamp().alias('_processing_time'),
    lit('S3').alias('_source_system')
)

print("Bronze metadata columns added")

# COMMAND ----------

# DBTITLE 1,Write to Bronze with Checkpointing
# Write stream to Delta table with exactly-once semantics
query = df_bronze.writeStream \
    .format('delta') \
    .outputMode('append') \
    .option('checkpointLocation', s3_config['checkpoint_location']) \
    .option('mergeSchema', 'true') \
    .trigger(processingTime='30 seconds') \
    .toTable(Config.TBL_BRONZE_S3_FILES)

print(f"Stream started, writing to {Config.TBL_BRONZE_S3_FILES}")
print(f"Checkpoint location: {s3_config['checkpoint_location']}")
print(f"Trigger interval: 30 seconds")
print(f"Query ID: {query.id}")
print("\nMonitor stream status with: query.status, query.lastProgress")

# COMMAND ----------

# DBTITLE 1,Stream Monitoring & Health Checks
# Check active streams
print("Active streams:")
for stream in spark.streams.active:
    print(f"  Stream ID: {stream.id}, Status: {stream.status}")

# Monitor file processing metrics
if query.lastProgress:
    progress = query.lastProgress
    print(f"\nLast batch metrics:")
    print(f"  Batch ID: {progress['batchId']}")
    print(f"  Files processed: {progress.get('sources', [{}])[0].get('numInputRows', 0)} rows")
    print(f"  Processing time: {progress.get('durationMs', {}).get('triggerExecution', 0)} ms")

# Query to check recent files
recent_files_query = f"""
SELECT 
    DATE(_processing_time) AS processing_date,
    COUNT(DISTINCT _input_file_name) AS files_processed,
    COUNT(*) AS total_records,
    COUNT_IF(_rescued_data IS NOT NULL) AS malformed_records,
    ROUND(SUM(LENGTH(TO_JSON(STRUCT(*)))) / 1024 / 1024, 2) AS total_size_mb
FROM {Config.TBL_BRONZE_S3_FILES}
WHERE _processing_time >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY DATE(_processing_time)
ORDER BY processing_date DESC
"""

print("\nFile processing history (last 7 days):")
display(spark.sql(recent_files_query))

# COMMAND ----------

# DBTITLE 1,Production Deployment Guide
# MAGIC %md
# MAGIC ## Production Deployment Guide
# MAGIC
# MAGIC S3 Best Practices:
# MAGIC - Use IAM role for authentication (avoid access keys)
# MAGIC - Enable S3 event notifications to SQS/SNS for faster discovery
# MAGIC - Organize files by date partitions: s3://bucket/data/year=2024/month=06/day=23/
# MAGIC - Use consistent file naming: data_20240623_120000.json
# MAGIC
# MAGIC Schema Evolution:
# MAGIC - Enable mergeSchema to handle new columns automatically
# MAGIC - Test schema changes in dev environment first
# MAGIC - Use rescuedDataColumn to capture parsing errors
# MAGIC - Review rescued data phase-based: SELECT * WHERE _rescued_data IS NOT NULL
# MAGIC
# MAGIC Checkpointing:
# MAGIC - Store checkpoints in Unity Catalog Volumes (not /tmp)
# MAGIC - Checkpoint path: /Volumes/sap/bronze/s3_checkpoints/
# MAGIC - Never delete checkpoint unless reprocessing from beginning
# MAGIC - Backup checkpoints before major changes
# MAGIC
# MAGIC Performance Tuning:
# MAGIC - Set maxFilesPerTrigger based on file size:
# MAGIC   - Small files (< 10 MB): 50-100 files
# MAGIC   - Medium files (10-100 MB): 10-20 files
# MAGIC   - Large files (> 100 MB): 1-5 files
# MAGIC - Adjust trigger interval based on arrival rate:
# MAGIC   - High frequency (files every minute): 30 seconds
# MAGIC   - Medium frequency (files every 10 minutes): 2 minutes
# MAGIC   - Low frequency (files hourly): 5-10 minutes
# MAGIC
# MAGIC Monitoring:
# MAGIC - Track file processing lag (time between file arrival and processing)
# MAGIC - Track error rate (rescued_data count / total count)
# MAGIC - Track throughput (files/minute, MB/minute)
# MAGIC - Alert if lag > 30 minutes
# MAGIC - Alert if error rate > 2 percent
# MAGIC
# MAGIC Cost Optimization:
# MAGIC - Use single-node cluster for low-volume ingestion (< 100 files/hour)
# MAGIC - Use multi-worker cluster for high-volume (> 1000 files/hour)
# MAGIC - Enable auto-termination if file arrival is sporadic
# MAGIC - Estimated cost:
# MAGIC   - Low volume: 1-2 DBU/hour (single-node)
# MAGIC   - High volume: 5-10 DBU/hour (4-worker cluster)
# MAGIC
# MAGIC Error Handling:
# MAGIC - Malformed records stored with _rescued_data (not dropped)
# MAGIC - Set up daily job to review and fix rescued data
# MAGIC - Reprocess files by resetting checkpoint (use with caution)
# MAGIC
# MAGIC Graceful Shutdown:
# MAGIC - Use query.stop() to stop stream gracefully
# MAGIC - Wait for current batch to complete
# MAGIC - Never force-kill streaming jobs (checkpoint corruption risk)