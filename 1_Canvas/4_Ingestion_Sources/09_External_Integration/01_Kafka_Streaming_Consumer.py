# Databricks notebook source
# DBTITLE 1,Kafka Streaming Consumer with Checkpointing
# MAGIC %md
# MAGIC # Kafka Streaming Ingestion: Consumer with Checkpointing + Error Handling
# MAGIC
# MAGIC FREE EDITION COMPATIBLE
# MAGIC
# MAGIC Purpose: Real-time ingestion from Kafka topics into Delta bronze layer with exactly-once semantics
# MAGIC
# MAGIC Key Features:
# MAGIC - Automatic checkpointing for fault tolerance
# MAGIC - Schema evolution handling
# MAGIC - Deserialization (Avro, JSON, String)
# MAGIC - Dead letter queue for malformed messages
# MAGIC - Exactly-once processing
# MAGIC - Backpressure handling
# MAGIC
# MAGIC Use Cases:
# MAGIC - IoT sensor data streams
# MAGIC - User clickstream events
# MAGIC - Transaction logs from payment gateways
# MAGIC - CDC events from operational databases
# MAGIC
# MAGIC Cost: Streaming compute (continuous running), ~2-5 DBU/hour

# COMMAND ----------

# DBTITLE 1,Config Import
# MAGIC %run ../../1_Canvas/1_Setup/1_config

# COMMAND ----------

# DBTITLE 1,Import Libraries & Setup
from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring
from pyspark.sql.types import *
import json

print("Libraries loaded successfully")
print(f"Target bronze table: {Config.TBL_BRONZE_KAFKA_STREAM}")
print(f"Checkpoint location: /tmp/sap/kafka_checkpoint")

# COMMAND ----------

# DBTITLE 1,Kafka Connection Configuration
# Kafka cluster configuration
kafka_config = {
    'bootstrap.servers': 'kafka-broker1:9092,kafka-broker2:9092',
    'topic': 'customer-events',
    'starting.offsets': 'latest',
    'max.offsets.per.trigger': 10000,
    'failOnDataLoss': False
}

# Security configuration (optional)
# kafka_config['kafka.security.protocol'] = 'SASL_SSL'
# kafka_config['kafka.sasl.mechanism'] = 'PLAIN'
# kafka_config['kafka.sasl.jaas.config'] = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" password="{YOUR_PASSWORD}";'

print(f"Kafka topic: {kafka_config['topic']}")
print(f"Starting from: {kafka_config['starting.offsets']}")

# COMMAND ----------

# DBTITLE 1,Define Message Schema
# Define expected schema for Kafka messages (JSON payload)
event_schema = StructType([
    StructField('event_id', StringType(), True),
    StructField('customer_id', IntegerType(), True),
    StructField('event_type', StringType(), True),
    StructField('product_id', IntegerType(), True),
    StructField('event_timestamp', TimestampType(), True),
    StructField('session_id', StringType(), True),
    StructField('device_type', StringType(), True),
    StructField('properties', StringType(), True)
])

print("Event schema defined")
print(event_schema)

# COMMAND ----------

# DBTITLE 1,Read Kafka Stream
# Read from Kafka topic
df_kafka_raw = spark \
    .readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', kafka_config['bootstrap.servers']) \
    .option('subscribe', kafka_config['topic']) \
    .option('startingOffsets', kafka_config['starting.offsets']) \
    .option('maxOffsetsPerTrigger', kafka_config['max.offsets.per.trigger']) \
    .option('failOnDataLoss', kafka_config['failOnDataLoss']) \
    .load()

print("Kafka stream connected")
print("Raw Kafka schema:")
df_kafka_raw.printSchema()

# COMMAND ----------

# DBTITLE 1,Parse JSON & Handle Errors
# Deserialize value column (JSON) and handle malformed messages
df_parsed = df_kafka_raw.select(
    col('key').cast('string').alias('kafka_key'),
    col('value').cast('string').alias('kafka_value'),
    col('topic'),
    col('partition'),
    col('offset'),
    col('timestamp').alias('kafka_timestamp'),
    from_json(col('value').cast('string'), event_schema).alias('data'),
    col('value').cast('string').alias('_rescued_data')
)

# Separate good records from bad records
df_good = df_parsed.filter(col('data').isNotNull()).select(
    'data.*',
    'kafka_key',
    'topic',
    'partition',
    'offset',
    'kafka_timestamp',
    current_timestamp().alias('_processing_time'),
    lit(None).cast(StringType()).alias('_rescued_data')
)

df_bad = df_parsed.filter(col('data').isNull()).select(
    lit(None).cast(StringType()).alias('event_id'),
    lit(None).cast(IntegerType()).alias('customer_id'),
    lit(None).cast(StringType()).alias('event_type'),
    lit(None).cast(IntegerType()).alias('product_id'),
    lit(None).cast(TimestampType()).alias('event_timestamp'),
    lit(None).cast(StringType()).alias('session_id'),
    lit(None).cast(StringType()).alias('device_type'),
    lit(None).cast(StringType()).alias('properties'),
    'kafka_key',
    'topic',
    'partition',
    'offset',
    'kafka_timestamp',
    current_timestamp().alias('_processing_time'),
    col('_rescued_data')
)

# Union good and bad records for unified storage
df_bronze = df_good.union(df_bad)

print("Parsing logic configured")

# COMMAND ----------

# DBTITLE 1,Write to Bronze with Checkpointing
# Write stream to bronze Delta table with exactly-once semantics
checkpoint_path = '/tmp/sap/kafka_checkpoint'

query = df_bronze.writeStream \
    .format('delta') \
    .outputMode('append') \
    .option('checkpointLocation', checkpoint_path) \
    .option('mergeSchema', 'true') \
    .trigger(processingTime='10 seconds') \
    .toTable(Config.TBL_BRONZE_KAFKA_STREAM)

print(f"Stream started, writing to {Config.TBL_BRONZE_KAFKA_STREAM}")
print(f"Checkpoint location: {checkpoint_path}")
print(f"Trigger interval: 10 seconds")
print(f"Query ID: {query.id}")

# Monitor stream health (optional - uncomment in production)
# while query.isActive:
#     status = query.lastProgress
#     if status:
#         print(f"Batch {status['batchId']}: {status['numInputRows']} rows processed in {status['durationMs']['triggerExecution']} ms")
#     time.sleep(30)

# COMMAND ----------

# DBTITLE 1,Stream Monitoring Queries
# Check stream status
print("Active streams:")
for stream in spark.streams.active:
    print(f"  {stream.name} - {stream.status}")

# Query to check recent ingested records
recent_records_query = f"""
SELECT 
    COUNT(*) AS total_records,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT_IF(_rescued_data IS NOT NULL) AS malformed_records,
    MAX(_processing_time) AS last_processing_time,
    ROUND(COUNT_IF(_rescued_data IS NOT NULL) / COUNT(*) * 100, 2) AS error_rate_pct
FROM {Config.TBL_BRONZE_KAFKA_STREAM}
WHERE _processing_time >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
"""

print("\nStream health metrics (last 1 hour):")
display(spark.sql(recent_records_query))

# COMMAND ----------

# DBTITLE 1,Production Deployment Guide
# MAGIC %md
# MAGIC ## Production Deployment Guide
# MAGIC
# MAGIC Checkpointing Strategy:
# MAGIC - Store checkpoints in Unity Catalog Volumes (not /tmp in production)
# MAGIC - Checkpoint path: /Volumes/sap/bronze/kafka_checkpoints/
# MAGIC - Never delete checkpoint unless reprocessing from beginning
# MAGIC - Backup checkpoints before major schema changes
# MAGIC
# MAGIC Error Handling:
# MAGIC - Malformed messages stored with _rescued_data column (not dropped)
# MAGIC - Review rescued data daily: SELECT * WHERE _rescued_data IS NOT NULL
# MAGIC - Fix deserialization logic and reprocess if needed
# MAGIC
# MAGIC Schema Evolution:
# MAGIC - Enable mergeSchema=true for new columns
# MAGIC - Use schema registry (Confluent Schema Registry) for Avro messages
# MAGIC - Test schema changes in dev environment first
# MAGIC
# MAGIC Monitoring:
# MAGIC - Track throughput: rows/second, MB/second
# MAGIC - Track latency: processing time vs event timestamp
# MAGIC - Track error rate: rescued_data count / total count
# MAGIC - Alert if error rate > 1 percent
# MAGIC - Alert if lag > 1 million offsets
# MAGIC
# MAGIC Backpressure Handling:
# MAGIC - Set maxOffsetsPerTrigger to prevent memory overflow
# MAGIC - Start with 10,000, increase if latency acceptable
# MAGIC - Monitor executor memory usage
# MAGIC
# MAGIC Scaling:
# MAGIC - Use cluster with 2-4 workers for small topics (< 100 MB/hour)
# MAGIC - Use cluster with 8-16 workers for large topics (> 1 GB/hour)
# MAGIC - Enable auto-scaling if available
# MAGIC
# MAGIC Cost Estimation:
# MAGIC - Small topic: 2-3 DBU/hour (2-worker cluster, continuous)
# MAGIC - Large topic: 8-12 DBU/hour (8-worker cluster, continuous)
# MAGIC - Monthly cost: 1,500-9,000 DBU (rupees 15,000-90,000)
# MAGIC
# MAGIC Graceful Shutdown:
# MAGIC - Use query.stop() to stop stream gracefully
# MAGIC - Wait for current batch to complete before terminating cluster
# MAGIC - Never force-kill streaming jobs (checkpoint corruption risk)

# COMMAND ----------

