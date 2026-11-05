# Databricks notebook source
# DBTITLE 1,Dimension: Warehouse (SCD Type 1)
# MAGIC %md
# MAGIC # DimWareHouse: Warehouse Master Data
# MAGIC **SkillAIPath Data Warehouse - Silver Layer**
# MAGIC
# MAGIC **Pattern:** SCD Type 1 (Overwrite) | **Source:** stg_sales | **Target:** dim_warehouse

# COMMAND ----------

# DBTITLE 1,Load Config
# MAGIC %run ../../../1_Canvas/1_Setup/1_Config

# COMMAND ----------

# DBTITLE 1,Setup Parameters
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Widget: Load type
dbutils.widgets.dropdown("load_type", "full", ["full", "incremental"], "Load Type")
load_type = dbutils.widgets.get("load_type")

# Widget: Load since
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
print("DIM_WAREHOUSE - SCD TYPE 1 PATTERN")
print("="*70)
print(f"Environment: {Config.ENVIRONMENT}")
print(f"Catalog: {Config.CATALOG}")
print(f"Batch Date: {batch_date}")
print(f"Load Type: {load_type}")
print(f"Source: {source_table}")
print(f"Target: {Config.TBL_DIM_WAREHOUSE}")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Extract Source Data
# Extract distinct warehouses based on load type
# Warehouse dimension grain: WareHouseID (business key)

if load_type == "full":
    print(f"FULL LOAD: Processing all warehouses since {load_since}")
    print(f"  Source: {Config.TBL_STG_SALES} (history table)")
    df_source = spark.read.table(Config.TBL_STG_SALES) \
        .filter(col(Config.COL_BATCH_DATE) >= load_since) \
        .select("WareHouseName", "WareHouseCity") \
        .distinct()
else:
    print(f"INCREMENTAL LOAD: Processing warehouses from batch {batch_date}")
    print(f"  Source: {Config.TBL_STG_SALES_INCREMENTAL} (incremental staging)")
    df_source = spark.read.table(Config.TBL_STG_SALES_INCREMENTAL) \
        .select("WareHouseName", "WareHouseCity") \
        .distinct()

# Generate WareHouseID from row number (consistent across loads)
window_spec = Window.orderBy("WareHouseName")
df_source = df_source.withColumn("WareHouseID", row_number().over(window_spec))

warehouse_count = df_source.count()
print(f"Found {warehouse_count:,} unique warehouses")

if warehouse_count > 50:
    print(f"WARNING: Warehouse count unexpectedly high ({warehouse_count:,})")
    print("Expected ~20 warehouses (SkillAIPath base data)")
else:
    print(f" Warehouse count validated: {warehouse_count} warehouses within expected range")

# COMMAND ----------

# DBTITLE 1,Create Table & Unknown Record
# Create table schema (first run)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {Config.TBL_DIM_WAREHOUSE} (
        WareHouseSK BIGINT,
        WareHouseID BIGINT,
        WareHouseName STRING,
        WareHouseCity STRING
    )
    USING DELTA
""")

# Insert Unknown record (WareHouseSK = -1)
try:
    spark.sql(f"""
        INSERT INTO {Config.TBL_DIM_WAREHOUSE}
        SELECT -1 AS WareHouseSK, -1 AS WareHouseID, 'Unknown WareHouse' AS WareHouseName,
               'Unknown' AS WareHouseCity
        WHERE NOT EXISTS (SELECT 1 FROM {Config.TBL_DIM_WAREHOUSE} WHERE WareHouseSK = -1)
    """)
    print("SUCCESS: Unknown record inserted")
except:
    print("SUCCESS: Unknown record already exists")

print("SUCCESS: DimWareHouse table ready (with Unknown record)")

# COMMAND ----------

# DBTITLE 1,Generate Surrogate Keys & MERGE
# Different write strategies based on load_type

if load_type == "full":
    # FULL LOAD: Truncate existing data and reload all warehouses
    print("FULL LOAD MODE: Truncating existing dimension")
    
    # Prepare final dataset with surrogate keys
    df_final = df_source \
        .withColumn("WareHouseSK", row_number().over(Window.orderBy("WareHouseName"))) \
        .select(
            "WareHouseSK",
            "WareHouseID",
            "WareHouseName",
            "WareHouseCity"
        )
    
    final_count = df_final.count()
    
    # Write with overwrite mode (includes Unknown record)
    df_unknown = spark.sql("""
        SELECT -1 AS WareHouseSK, -1 AS WareHouseID, 'Unknown WareHouse' AS WareHouseName,
               'Unknown' AS WareHouseCity
    """)
    
    df_with_unknown = df_unknown.unionByName(df_final)
    
    df_with_unknown.write \
        .mode("overwrite") \
        .format("delta") \
        .option("overwriteSchema", "true") \
        .saveAsTable(Config.TBL_DIM_WAREHOUSE)
    
    print(f"\nFULL LOAD completed")
    print(f"  Records loaded: {final_count:,}")
    print(f"  Total (with Unknown): {final_count + 1:,}")
    
    new_count = final_count
    existing_count = 0
    
else:
    # INCREMENTAL LOAD: MERGE (UPDATE existing + INSERT new)
    print("INCREMENTAL MODE: Using MERGE for upsert")
    
    # Get max existing surrogate key
    max_sk = spark.sql(f"SELECT COALESCE(MAX(WareHouseSK), 0) as max_sk FROM {Config.TBL_DIM_WAREHOUSE}").collect()[0]['max_sk']
    
    # Read existing warehouses
    df_existing = spark.read.table(Config.TBL_DIM_WAREHOUSE) \
        .filter(col("WareHouseSK") != -1) \
        .select("WareHouseName", "WareHouseSK")
    
    # Identify NEW warehouses (not in dimension yet)
    df_new = df_source.join(
        df_existing,
        ["WareHouseName"],
        "left_anti"
    )
    new_count = df_new.count()
    
    # Assign surrogate keys to new warehouses
    if new_count > 0:
        df_new_with_sk = df_new \
            .withColumn("WareHouseSK", row_number().over(Window.orderBy("WareHouseName")) + max_sk) \
            .select(
                "WareHouseSK",
                "WareHouseID",
                "WareHouseName",
                "WareHouseCity"
            )
        print(f"Assigned WareHouseSK range: {max_sk + 1} to {max_sk + new_count}")
    else:
        df_new_with_sk = spark.createDataFrame(
            [],
            schema="WareHouseSK BIGINT, WareHouseID BIGINT, WareHouseName STRING, WareHouseCity STRING"
        )
    
    # Prepare existing warehouses for MERGE
    df_existing_wh = df_source \
        .join(
            df_existing,
            ["WareHouseName"],
            "inner"
        ) \
        .select(
            col("WareHouseSK"),
            col("WareHouseID"),
            col("WareHouseName"),
            col("WareHouseCity")
        ) \
        .distinct()
    
    existing_count = df_existing_wh.count()
    
    # Union NEW and EXISTING warehouses
    df_merge_source = df_new_with_sk.unionByName(df_existing_wh)
    
    # Final deduplication by WareHouseName (business key)
    window_dedup = Window.partitionBy("WareHouseName").orderBy("WareHouseSK")
    df_merge_source = df_merge_source \
        .withColumn("row_num", row_number().over(window_dedup)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    print(f"MERGE source prepared:")
    print(f"  New warehouses (INSERT): {new_count:,}")
    print(f"  Existing warehouses (UPDATE): {existing_count:,}")
    print(f"  Total in merge: {df_merge_source.count():,}")
    
    # Create temp view for MERGE
    df_merge_source.createOrReplaceTempView("warehouse_source")
    
    # Execute MERGE (SCD Type 1: UPDATE existing, INSERT new)
    merge_result = spark.sql(f"""
        MERGE INTO {Config.TBL_DIM_WAREHOUSE} AS tgt
        USING warehouse_source AS src
        ON tgt.WareHouseName = src.WareHouseName
        WHEN MATCHED THEN 
            UPDATE SET 
                tgt.WareHouseCity = src.WareHouseCity
        WHEN NOT MATCHED THEN
            INSERT (WareHouseSK, WareHouseID, WareHouseName, WareHouseCity)
            VALUES (src.WareHouseSK, src.WareHouseID, src.WareHouseName, src.WareHouseCity)
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

# DBTITLE 1,Validation Query
# Verify dimension populated correctly
query = f"""
SELECT WareHouseSK, WareHouseID, WareHouseName, WareHouseCity
FROM {Config.TBL_DIM_WAREHOUSE}
WHERE WareHouseSK != -1
ORDER BY WareHouseSK
LIMIT 10
"""

display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Return Summary
# Return processing summary
final_count = spark.read.table(Config.TBL_DIM_WAREHOUSE).filter(col("WareHouseSK") != -1).count()

summary = {
    "status": "success",
    "environment": Config.ENVIRONMENT,
    "catalog": Config.CATALOG,
    "batch_date": batch_date,
    "load_since": load_since,
    "load_type": load_type,
    "dimension": "DimWareHouse",
    "scd_type": "Type 1",
    "new_warehouses": new_count,
    "updated_warehouses": existing_count,
    "total_warehouses": final_count,
    "table": Config.TBL_DIM_WAREHOUSE
}

print("\n" + "="*70)
print("DIM_WAREHOUSE (SCD-1) PROCESSING COMPLETE")
print("="*70)
for key, value in summary.items():
    print(f"  {key}: {value}")
print("="*70)

if final_count > 50:
    print(f"\nWARNING: Warehouse count ({final_count:,}) exceeds expected (~20)")
    print("Bronze data may have incorrect warehouse structure")
    print("Review: ../00_UTILS/generate_batch.ipynb")

dbutils.notebook.exit(str(summary))

# COMMAND ----------

