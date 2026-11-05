# Databricks notebook source
# DBTITLE 1,Dimension: Payment Mode (SCD Type 1)
# MAGIC %md
# MAGIC # DimPaymentMode: Payment Method Master Data
# MAGIC **SkillAIPath Data Warehouse - Silver Layer**
# MAGIC
# MAGIC **Pattern:** SCD Type 1 | **Source:** stg_sales | **Target:** dim_payment_mode

# COMMAND ----------

# DBTITLE 1,Load Config
# MAGIC %run ../../../1_Canvas/1_Setup/1_Config

# COMMAND ----------

# DBTITLE 1,Parameters & Config (Unity Catalog)
from datetime import datetime
from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring
from pyspark.sql.window import Window

# Widget: Load type
dbutils.widgets.dropdown("load_type", "full", ["full", "incremental"], "Load Type")
load_type = dbutils.widgets.get("load_type")

# Widget: Load since (for full load)
dbutils.widgets.text("load_since", "2023-06-13", "Load Since (Full Load Only)")
load_since = dbutils.widgets.get("load_since")

# Get batch date: try history table first, fallback to incremental table
try:
    latest_batch = spark.sql(f"SELECT MAX(BatchDate) FROM {Config.TBL_STG_SALES}").collect()[0][0]
    batch_date = str(latest_batch) if latest_batch else "2023-06-13"
except:
    try:
        latest_batch = spark.sql(f"SELECT MAX(BatchDate) FROM {Config.TBL_STG_SALES_INCREMENTAL}").collect()[0][0]
        batch_date = str(latest_batch) if latest_batch else "2023-06-13"
    except:
        batch_date = "2023-06-13"

# Determine source table for logging based on load type
source_table = Config.TBL_STG_SALES_INCREMENTAL if load_type == "incremental" else Config.TBL_STG_SALES

print("="*70)
print("DIM_PAYMENT_MODE - SCD TYPE 1")
print("="*70)
print(f"Environment: {Config.ENVIRONMENT.upper()}")
print(f"Catalog: {Config.CATALOG}")
print(f"Batch Date: {batch_date}")
print(f"Load Type: {load_type}")
print(f"Source: {source_table}")
print(f"Target: {Config.TBL_DIM_PAYMENT_MODE}")
print("="*70)

# COMMAND ----------

# DBTITLE 0,Extract Source Data
# Extract distinct payment modes

if load_type == "full":
    print(f"FULL LOAD: Processing all payment modes since {load_since}")
    print(f"  Source: {Config.TBL_STG_SALES} (history table)")
    df_source = spark.read.table(Config.TBL_STG_SALES) \
        .filter(col(Config.COL_BATCH_DATE) >= load_since) \
        .select("PaymentMode") \
        .distinct()
else:
    print(f"INCREMENTAL LOAD: Processing payment modes from batch {batch_date}")
    print(f"  Source: {Config.TBL_STG_SALES_INCREMENTAL} (incremental staging)")
    df_source = spark.read.table(Config.TBL_STG_SALES_INCREMENTAL) \
        .select("PaymentMode") \
        .distinct()

mode_count = df_source.count()
print(f"Found {mode_count:,} unique payment modes")

# COMMAND ----------

# DBTITLE 1,Create Table & Unknown Record
# Create table with Unknown record
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {Config.TBL_DIM_PAYMENT_MODE} (
        PaymentModeSK BIGINT COMMENT 'Surrogate Key',
        PaymentModeID INT COMMENT 'Business Key (Integer ID)',
        PaymentMode STRING COMMENT 'Payment Mode Name'
    )
    USING DELTA
    COMMENT 'SCD Type 1: Payment Mode Dimension'
""")

# Insert Unknown record (-1) for orphan facts
try:
    spark.sql(f"""
        INSERT INTO {Config.TBL_DIM_PAYMENT_MODE}
        SELECT -1 AS PaymentModeSK, -1 AS PaymentModeID, 'Unknown' AS PaymentMode
        WHERE NOT EXISTS (SELECT 1 FROM {Config.TBL_DIM_PAYMENT_MODE} WHERE PaymentModeSK = -1)
    """)
    print("Unknown record inserted")
except:
    print("Unknown record already exists")

print(f"Table ready: {Config.TBL_DIM_PAYMENT_MODE}")

# COMMAND ----------

# DBTITLE 1,Generate Surrogate Keys & MERGE
if load_type == "full":
    print("FULL LOAD MODE: Truncating existing dimension")
    
    # Generate surrogate keys
    df_final = df_source \
        .withColumn("PaymentModeSK", row_number().over(Window.orderBy("PaymentMode"))) \
        .withColumn("PaymentModeID", col("PaymentModeSK").cast("int")) \
        .select("PaymentModeSK", "PaymentModeID", "PaymentMode")
    
    final_count = df_final.count()
    
    # Add Unknown record
    df_unknown = spark.sql("""
        SELECT -1 AS PaymentModeSK, -1 AS PaymentModeID, 'Unknown' AS PaymentMode
    """)
    
    df_with_unknown = df_unknown.unionByName(df_final)
    
    # Overwrite table
    df_with_unknown.write \
        .mode("overwrite") \
        .format("delta") \
        .option("overwriteSchema", "true") \
        .saveAsTable(Config.TBL_DIM_PAYMENT_MODE)
    
    print(f"\nFULL LOAD completed")
    print(f"  Records loaded: {final_count:,}")
    print(f"  Total (with Unknown): {final_count + 1:,}")
    
    new_count = final_count
    existing_count = 0
    
else:
    print("INCREMENTAL MODE: Using MERGE for upsert")
    
    max_sk = spark.sql(f"SELECT COALESCE(MAX(PaymentModeSK), 0) as max_sk FROM {Config.TBL_DIM_PAYMENT_MODE}").collect()[0]['max_sk']
    df_existing = spark.read.table(Config.TBL_DIM_PAYMENT_MODE).filter(col("PaymentModeSK") != -1).select("PaymentMode", "PaymentModeSK")
    
    df_new = df_source.join(df_existing, ["PaymentMode"], "left_anti")
    new_count = df_new.count()
    
    if new_count > 0:
        df_new_with_sk = df_new \
            .withColumn("PaymentModeSK", row_number().over(Window.orderBy("PaymentMode")) + max_sk) \
            .withColumn("PaymentModeID", col("PaymentModeSK").cast("int")) \
            .select("PaymentModeSK", "PaymentModeID", "PaymentMode")
        print(f"Assigned PaymentModeSK range: {max_sk + 1} to {max_sk + new_count}")
    else:
        df_new_with_sk = spark.createDataFrame(
            [],
            schema="PaymentModeSK BIGINT, PaymentModeID INT, PaymentMode STRING"
        )
    
    df_existing_pm = df_source.join(df_existing, ["PaymentMode"], "inner") \
        .withColumn("PaymentModeID", col("PaymentModeSK").cast("int")) \
        .select(col("PaymentModeSK"), col("PaymentModeID"), col("PaymentMode")).distinct()
    
    existing_count = df_existing_pm.count()
    
    df_merge_source = df_new_with_sk.unionByName(df_existing_pm)
    
    window_dedup = Window.partitionBy("PaymentMode").orderBy("PaymentModeSK")
    df_merge_source = df_merge_source \
        .withColumn("row_num", row_number().over(window_dedup)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    print(f"MERGE source prepared:")
    print(f"  New payment modes (INSERT): {new_count:,}")
    print(f"  Existing payment modes (UPDATE): {existing_count:,}")
    
    df_merge_source.createOrReplaceTempView("payment_mode_source")
    
    spark.sql(f"""
        MERGE INTO {Config.TBL_DIM_PAYMENT_MODE} AS tgt
        USING payment_mode_source AS src
        ON tgt.PaymentMode = src.PaymentMode
        WHEN MATCHED THEN UPDATE SET
            tgt.PaymentModeID = src.PaymentModeID
        WHEN NOT MATCHED THEN INSERT (
            PaymentModeSK, PaymentModeID, PaymentMode
        ) VALUES (
            src.PaymentModeSK, src.PaymentModeID, src.PaymentMode
        )
    """)
    
    print("\nMERGE completed (SCD Type 1)")
    print(f"  Records inserted: {new_count:,}")
    print(f"  Records updated: {existing_count:,}")

print("\nPattern Summary:")
if load_type == "full":
    print("  FULL LOAD = Complete refresh (TRUNCATE + RELOAD)")
else:
    print("  INCREMENTAL = Delta updates (MERGE: UPDATE + INSERT)")

# COMMAND ----------

# DBTITLE 1,Validation
# Verify dimension populated correctly
query = f"""
SELECT 
    PaymentModeSK,
    PaymentModeID,
    PaymentMode
FROM {Config.TBL_DIM_PAYMENT_MODE}
WHERE PaymentModeSK != -1
ORDER BY PaymentModeSK
"""

display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Return Summary
# Return processing summary
final_count = spark.read.table(Config.TBL_DIM_PAYMENT_MODE) \
    .filter(col("PaymentModeSK") != -1) \
    .count()

summary = {
    "status": "success",
    "environment": Config.ENVIRONMENT,
    "catalog": Config.CATALOG,
    "batch_date": batch_date,
    "load_since": load_since,
    "load_type": load_type,
    "dimension": "DimPaymentMode",
    "scd_type": "Type 1",
    "new_payment_modes": new_count,
    "updated_payment_modes": existing_count,
    "total_payment_modes": final_count,
    "table": Config.TBL_DIM_PAYMENT_MODE
}

print("\n" + "="*70)
print("DIM_PAYMENT_MODE PROCESSING COMPLETE")
print("="*70)
for key, value in summary.items():
    print(f"  {key}: {value}")
print("="*70)

dbutils.notebook.exit(str(summary))

# COMMAND ----------

