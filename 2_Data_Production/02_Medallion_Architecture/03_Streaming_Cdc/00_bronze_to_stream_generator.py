# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Bronze to Stream Generator - Overview
# MAGIC %md
# MAGIC # Bronze to Stream Generator
# MAGIC
# MAGIC ## Purpose
# MAGIC Transforms existing Bronze table data into streaming batches for testing Spark Streaming concepts:
# MAGIC - ✅ Watermarks (late arrivals)
# MAGIC - ✅ Session windows
# MAGIC - ✅ Checkpoints
# MAGIC - ✅ Stateful aggregations
# MAGIC - ✅ Auto Loader incremental processing
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC 1. Reads `sap_dev.bronze.stg_sales` (existing data)
# MAGIC 2. Adds streaming timestamps with 20% late arrivals (10-30 min late)
# MAGIC 3. Adds session patterns (session_id)
# MAGIC 4. Splits into 10 CSV batches
# MAGIC 5. Writes to volume with 20-second delays between batches
# MAGIC
# MAGIC ## Run Instructions
# MAGIC **Run this notebook ONCE manually** (3-5 minutes total)
# MAGIC - No scheduled job needed
# MAGIC - Uses your free credits/trial
# MAGIC - Creates streaming data for testing
# MAGIC
# MAGIC ## Cost
# MAGIC ~5 minutes of serverless compute (your free access)

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Configuration & Setup
# MAGIC %run ../../../1_Canvas/1_Setup/1_Config

# COMMAND ----------

# DBTITLE 1,Notebook Parameters (non-config values only)
# Define only values NOT in config
volume_name = "streaming_test_data"
source_table = "stg_sales"
num_batches = 10
late_arrival_pct = 0.20
delay_seconds = 20

# COMMAND ----------

# DBTITLE 1,Auto-Create Volume (Portability)
# Auto-create volume if it doesn't exist (makes this portable)
try:
    spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS {Config.CATALOG}.{Config.SCHEMA_BRONZE}.{volume_name}
    """)
    print(f"✅ Volume ready: {Config.VOLUME_STREAMING_TEST}")
except Exception as e:
    print(f"⚠️ Volume creation failed: {e}")
    print(f"Manual creation: CREATE VOLUME {Config.CATALOG}.{Config.SCHEMA_BRONZE}.{volume_name};")
    raise

# Clean previous test data (optional - comment out to keep old data)
try:
    dbutils.fs.rm(Config.VOLUME_STREAMING_TEST, recurse=True)
    print("✅ Volume cleaned (ready for fresh test data)")
except:
    print("✅ Volume is empty (first run, nothing to clean)")

# COMMAND ----------

# DBTITLE 0,Load Bronze Data
from pyspark.sql import functions as F
from pyspark.sql.functions import explode, array, lit, col, rand, when
import random

# Read Bronze table
df_bronze = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_BRONZE}.{source_table}")

# Generate REALISTIC session-friendly test data:
# - 50 customers, each with 20 events
# - Most events = browsing different products
# - Few events = actual purchases
num_unique_customers = 50
events_per_customer = 20
purchase_probability = 0.15  # 15% of events are purchases (realistic conversion)

print(f"🎯 Building realistic customer sessions...")

# Step 1: Get 50 distinct customers with their purchase history
df_customers = df_bronze.select("CustomerID").distinct().limit(num_unique_customers)

# Step 2: Get one actual purchase per customer (will use for purchase events)
from pyspark.sql.window import Window

# Use window function to get one row per customer
window_customer = Window.partitionBy("CustomerID").orderBy(F.rand())
df_customer_purchases = df_bronze.join(df_customers, "CustomerID", "inner") \
    .withColumn("rn", F.row_number().over(window_customer)) \
    .filter(col("rn") == 1) \
    .drop("rn")

# Step 3: Create product catalog for browsing (100 random products)
df_product_catalog = df_bronze.select(
    "ProductName", "ProductCategory", "CostPrice", "SellingPrice"
).distinct().limit(100)

product_list = [row.ProductName for row in df_product_catalog.collect()]
print(f"   📦 Product catalog: {len(product_list)} products for browsing")

# Step 4: Generate 20 events per customer
df_events = df_customer_purchases.withColumn(
    "event_num",
    explode(array([lit(i) for i in range(events_per_customer)]))
)

# Step 5: Mark which events are purchases (15% = ~3 purchases per customer)
df_events = df_events.withColumn(
    "is_purchase_event",
    when(rand() < purchase_probability, True).otherwise(False)
)

print(f"✅ Generated {df_events.count()} realistic events")
print(f"   {num_unique_customers} customers × {events_per_customer} events each")
print(f"   ~{int(events_per_customer * purchase_probability)} purchases per customer")
print(f"   ~{int(events_per_customer * (1-purchase_probability))} browsing events per customer")

# COMMAND ----------

# DBTITLE 1,Transform to Streaming Format
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import random

print(f"\n🛠️ Transforming to realistic streaming format...")

# Strategy: Distribute each customer's events across different batches
# Event 0,1,2,3... for Customer X go to different batches (round-robin)
# This ensures same CustomerID appears in multiple batches

# Step 1: Assign batch_id (round-robin distribution)
df_numbered = df_events.withColumn(
    "batch_id",
    F.col("event_num") % num_batches  # Event 0→Batch 0, Event 1→Batch 1, ...
)

# Step 2: For browsing events, assign RANDOM products from catalog
# Create UDF to pick random product
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def get_random_product():
    return random.choice(product_list)

df_numbered = df_numbered.withColumn(
    "browsing_product",
    get_random_product()
)

# Step 3: Use actual ProductName for purchases, random product for browsing
df_numbered = df_numbered.withColumn(
    "final_product",
    when(col("is_purchase_event"), col("ProductName")).otherwise(col("browsing_product"))
)

df_numbered = df_numbered.withColumn("ProductName", col("final_product"))
df_numbered = df_numbered.drop("browsing_product", "final_product")

# Add row number within each batch (for ordering)
window_spec = Window.partitionBy("batch_id").orderBy(F.monotonically_increasing_id())
df_numbered = df_numbered.withColumn("row_num", F.row_number().over(window_spec))

# Generate REALISTIC timestamps for watermark testing
# Events should be from the LAST 2 HOURS, not weeks ago!
# This ensures late arrivals (10-30 min) are meaningful for 10-minute watermarks

print("\n⏰ Generating realistic timestamps (last 2 hours):")

# Base: Current time minus 2 hours
df_numbered = df_numbered.withColumn(
    "base_timestamp",
    F.expr("CURRENT_TIMESTAMP() - INTERVAL '2' HOURS")
)

# Spread events over 2 hours (event 0=2hrs ago, event 19=now)
# Each event adds ~6 minutes (120 minutes / 20 events)
df_numbered = df_numbered.withColumn(
    "event_timestamp",
    F.expr("base_timestamp + INTERVAL '1' MINUTE * event_num * 6")
)

df_numbered = df_numbered.drop("base_timestamp")

print("   ✅ Events spread across last 2 hours (realistic for streaming)")

# Step 4: Add session_id (CustomerID becomes session identifier)
df_numbered = df_numbered.withColumn("session_id", F.col("CustomerID"))

# Step 5: For browsing events, generate NEW unique OrderIDs when they DO purchase
# For browsing, nullify purchase-specific columns
from pyspark.sql.functions import monotonically_increasing_id

# Generate unique OrderIDs for purchase events (different from original)
df_numbered = df_numbered.withColumn(
    "new_order_id",
    (10000000 + monotonically_increasing_id()).cast("int")  # New OrderIDs starting at 10M
)

df_numbered = df_numbered.withColumn(
    "OrderID",
    when(col("is_purchase_event"), col("new_order_id")).otherwise(F.lit(None).cast("int"))
)

# Keep TotalAmount only for purchases
df_numbered = df_numbered.withColumn(
    "TotalAmount",
    when(col("is_purchase_event"), col("TotalAmount")).otherwise(F.lit(None).cast("double"))
)

# Keep PaymentMode only for purchases
df_numbered = df_numbered.withColumn(
    "PaymentMode",
    when(col("is_purchase_event"), col("PaymentMode")).otherwise(F.lit(None).cast("string"))
)

# ProductName is now different for browsing vs purchase (already set above)
# Keep CustomerID, event_timestamp, session_id for all events

# Inject late arrivals (20% of events will be 10-30 minutes late)
df_numbered = df_numbered.withColumn(
    "is_late_arrival",
    F.when(F.rand() < late_arrival_pct, True).otherwise(False)
)

df_numbered = df_numbered.withColumn(
    "event_timestamp",
    F.when(
        F.col("is_late_arrival"),
        F.col("event_timestamp") - F.expr(f"INTERVAL {random.randint(10, 30)} MINUTES")
    ).otherwise(F.col("event_timestamp"))
)

print(f"✅ Transformed data with streaming columns")

# Show realistic statistics
purchase_count = df_numbered.filter(col("is_purchase_event") == True).count()
browse_count = df_numbered.filter(col("is_purchase_event") == False).count()
late_count = df_numbered.filter(col("is_late_arrival") == True).count()

print(f"\n📊 Realistic Event Mix:")
print(f"   Total events: {df_numbered.count()}")
print(f"   🛋️ Purchase events: {purchase_count} ({purchase_count*100//1000}%) - NEW unique OrderIDs")
print(f"   👁️ Browsing events: {browse_count} ({browse_count*100//1000}%) - varied products, no OrderID")
print(f"   ⏰ Late arrivals: {late_count} ({late_count*100//1000}%)")

print(f"\n🔍 Sample (realistic mix of purchases and browsing):")
df_numbered.select(
    "CustomerID", "OrderID", "ProductName", "TotalAmount",
    "event_timestamp", "session_id", "is_purchase_event", "batch_id"
).orderBy("CustomerID", "event_num").show(15, truncate=30)

print(f"\n📦 Product variety check (browsing vs purchase):")
df_numbered.groupBy("is_purchase_event").agg(
    F.countDistinct("ProductName").alias("unique_products"),
    F.count("*").alias("event_count")
).show()

# COMMAND ----------

# DBTITLE 1,Write Batches with Delays
import time

# Write each batch as a separate CSV file with delays
for batch_num in range(num_batches):
    # Filter to current batch
    df_batch = df_numbered.filter(F.col("batch_id") == batch_num)
    
    # CRITICAL FIX: Remove newlines from CustomerAddress (causes CSV parsing issues)
    df_batch = df_batch.withColumn(
        "CustomerAddress",
        F.regexp_replace(F.col("CustomerAddress"), "\n", ", ")
    )
    
    # Drop internal columns
    df_batch = df_batch.drop("row_num", "batch_id", "is_late_arrival", "event_num", "is_purchase_event", "new_order_id")
    
    # Write to volume
    batch_filename = f"batch_{str(batch_num + 1).zfill(3)}.csv"
    batch_path = f"{Config.VOLUME_STREAMING_TEST}/{batch_filename}"
    
    df_batch.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .option("quote", '"') \
        .option("escape", '"') \
        .option("quoteAll", "true") \
        .csv(batch_path)
    
    purchase_batch_count = df_batch.filter(col("OrderID").isNotNull()).count()
    browse_batch_count = df_batch.filter(col("OrderID").isNull()).count()
    late_count = df_batch.filter(col("event_timestamp") < F.current_timestamp() - F.expr("INTERVAL 10 MINUTES")).count()
    
    print(f"✅ Batch {batch_num + 1}/{num_batches}: {batch_filename} ({df_batch.count()} events: {purchase_batch_count} purchases, {browse_batch_count} browsing, {late_count} late)")
    
    # Wait before next batch (simulate streaming arrival)
    if batch_num < num_batches - 1:  # Don't wait after last batch
        print(f"   ⏳ Waiting {delay_seconds} seconds before next batch...")
        time.sleep(delay_seconds)

print(f"\n🎉 SUCCESS! Generated {num_batches} REALISTIC streaming batches in {Config.VOLUME_STREAMING_TEST}")
print(f"\n💡 What you got:")
print(f"   • 50 customers with realistic shopping behavior")
print(f"   • Each customer: ~3 purchases + ~17 browsing visits (15% conversion)")
print(f"   • Varied products: customers browse different products before buying")
print(f"   • New unique OrderIDs for each purchase (not reused from source)")
print(f"   • 20% late arrivals for watermark testing")
print(f"   • Events distributed across all batches for session testing")

# COMMAND ----------

# DBTITLE 1,Verify Generated Data
# List generated files
files = dbutils.fs.ls(Config.VOLUME_STREAMING_TEST)
print(f"\n📂 Generated Files ({len(files)} batches):")
for f in files:
    print(f"   {f.name}")

# Sample data from first batch
print(f"\n🔍 Sample from first batch:")
spark.read.option("header", "true").csv(f"{Config.VOLUME_STREAMING_TEST}/batch_001.csv").show(5)

print(f"\n✅ Ready for streaming tests!")
print(f"\nNext Steps:")
print(f"1. Run 00_stream_consumer_template to test concepts")
print(f"2. Run 01-05 streaming notebooks with this data")
print(f"3. Run 13-14 DLT pipelines with this data")

# COMMAND ----------

