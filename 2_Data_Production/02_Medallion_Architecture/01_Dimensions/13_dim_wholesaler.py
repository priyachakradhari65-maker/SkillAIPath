# Databricks notebook source
# DBTITLE 1,Dimension: Wholesaler (SCD Type 1)
# MAGIC %md
# MAGIC # DimWholesaler: Wholesaler Master Data
# MAGIC **SkillAIPath Data Warehouse - Silver Layer**
# MAGIC
# MAGIC **Pattern:** SCD Type 1 | **Source:** stg_purchase | **Target:** dim_wholesaler

# COMMAND ----------

# DBTITLE 1,Load Config
# MAGIC %run ../../../1_Canvas/1_Setup/1_Config

# COMMAND ----------

# DBTITLE 1,Import Config
# Config loaded via %run cell above

# COMMAND ----------

# DBTITLE 1,Setup Parameters
from datetime import datetime
from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring
from pyspark.sql.window import Window

# Widget: Load type
dbutils.widgets.dropdown("load_type", "full", ["full", "incremental"], "Load Type")
load_type = dbutils.widgets.get("load_type")

# Widget: Load since
dbutils.widgets.text("load_since", "2023-06-13", "Load Since (Full Load Only)")
load_since = dbutils.widgets.get("load_since")

# Get batch date: try history table first, fallback to incremental table
try:
    latest_batch = spark.sql(f"SELECT MAX(BatchDate) FROM {Config.TBL_STG_PURCHASE}").collect()[0][0]
    batch_date = str(latest_batch) if latest_batch else "2023-06-13"
except:
    try:
        latest_batch = spark.sql(f"SELECT MAX(BatchDate) FROM {Config.TBL_STG_PURCHASE_INCREMENTAL}").collect()[0][0]
        batch_date = str(latest_batch) if latest_batch else "2023-06-13"
    except:
        batch_date = "2023-06-13"

# Determine source table for logging based on load type
source_table = Config.TBL_STG_PURCHASE_INCREMENTAL if load_type == "incremental" else Config.TBL_STG_PURCHASE

print("="*70)
print("DIM_WHOLESALER - SCD TYPE 1")
print("="*70)
print(f"Environment: {Config.ENVIRONMENT.upper()}")
print(f"Catalog: {Config.CATALOG}")
print(f"Batch Date: {batch_date}")
print(f"Load Type: {load_type}")
print(f"Source: {source_table}")
print(f"Target: {Config.TBL_DIM_WHOLESALER}")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Extract Source Data
# Extract distinct wholesalers

if load_type == "full":
    print(f"FULL LOAD: Processing all wholesalers since {load_since}")
    print(f"  Source: {Config.TBL_STG_PURCHASE} (history table)")
    df_source = spark.read.table(Config.TBL_STG_PURCHASE) \
        .filter(col(Config.COL_BATCH_DATE) >= load_since) \
        .select("WholeSalerName") \
        .distinct()
else:
    print(f"INCREMENTAL LOAD: Processing wholesalers from batch {batch_date}")
    print(f"  Source: {Config.TBL_STG_PURCHASE_INCREMENTAL} (incremental staging)")
    df_source = spark.read.table(Config.TBL_STG_PURCHASE_INCREMENTAL) \
        .select("WholeSalerName") \
        .distinct()

window_spec = Window.orderBy("WholeSalerName")
df_source = df_source.withColumn("WholeSalerID", row_number().over(window_spec))

wholesaler_count = df_source.count()
print(f"Found {wholesaler_count:,} unique wholesalers")

if wholesaler_count > 20:
    print(f"WARNING: Wholesaler count unexpectedly high ({wholesaler_count:,})")
    print("Expected ~7-10 wholesalers based on SkillAIPath data")
else:
    print(f"Wholesaler count matches expected range (~7-10)")

# COMMAND ----------

# DBTITLE 1,Create Table & Unknown Record
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {Config.TBL_DIM_WHOLESALER} (
        WholeSalerSK BIGINT,
        WholeSalerID BIGINT,
        WholeSalerName STRING
    )
    USING DELTA
""")

try:
    spark.sql(f"""
        INSERT INTO {Config.TBL_DIM_WHOLESALER}
        SELECT -1 AS WholeSalerSK, -1 AS WholeSalerID, 'Unknown Wholesaler' AS WholeSalerName
        WHERE NOT EXISTS (SELECT 1 FROM {Config.TBL_DIM_WHOLESALER} WHERE WholeSalerSK = -1)
    """)
    print("SUCCESS: Unknown record inserted")
except:
    print("SUCCESS: Unknown record already exists")

print("SUCCESS: DimWholesaler table ready (with Unknown record)")
print("  Schema: WholeSalerSK, WholeSalerID, WholeSalerName")

# COMMAND ----------

# DBTITLE 1,Generate Surrogate Keys & MERGE
if load_type == "full":
    print("FULL LOAD MODE: Truncating existing dimension")

    df_final = df_source \
        .withColumn("WholeSalerSK", row_number().over(Window.orderBy("WholeSalerName"))) \
        .select(
            "WholeSalerSK",
            "WholeSalerID",
            "WholeSalerName"
        )

    final_count = df_final.count()

    df_unknown = spark.sql("""
        SELECT -1 AS WholeSalerSK, -1 AS WholeSalerID, 'Unknown Wholesaler' AS WholeSalerName
    """)

    df_with_unknown = df_unknown.unionByName(df_final)

    df_with_unknown.write \
        .mode("overwrite") \
        .format("delta") \
        .option("overwriteSchema", "true") \
        .saveAsTable(Config.TBL_DIM_WHOLESALER)

    print(f"\nFULL LOAD completed")
    print(f"  Records loaded: {final_count:,}")
    print(f"  Total (with Unknown): {final_count + 1:,}")

    new_count = final_count
    existing_count = 0

else:
    print("INCREMENTAL MODE: Using MERGE for upsert")

    max_sk = spark.sql(f"SELECT COALESCE(MAX(WholeSalerSK), 0) as max_sk FROM {Config.TBL_DIM_WHOLESALER}").collect()[0]['max_sk']
    df_existing = spark.read.table(Config.TBL_DIM_WHOLESALER).filter(col("WholeSalerSK") != -1).select("WholeSalerName", "WholeSalerSK")

    df_new = df_source.join(df_existing, ["WholeSalerName"], "left_anti")
    new_count = df_new.count()

    if new_count > 0:
        df_new_with_sk = df_new \
            .withColumn("WholeSalerSK", row_number().over(Window.orderBy("WholeSalerName")) + max_sk) \
            .select(
                "WholeSalerSK",
                "WholeSalerID",
                "WholeSalerName"
            )
        print(f"Assigned WholeSalerSK range: {max_sk + 1} to {max_sk + new_count}")
    else:
        df_new_with_sk = spark.createDataFrame(
            [],
            schema="WholeSalerSK BIGINT, WholeSalerID BIGINT, WholeSalerName STRING"
        )

    df_existing_wh = df_source.join(df_existing, ["WholeSalerName"], "inner") \
        .select(
            col("WholeSalerSK"),
            col("WholeSalerID"),
            col("WholeSalerName")
        ).distinct()

    existing_count = df_existing_wh.count()

    df_merge_source = df_new_with_sk.unionByName(df_existing_wh)

    window_dedup = Window.partitionBy("WholeSalerName").orderBy("WholeSalerSK")
    df_merge_source = df_merge_source \
        .withColumn("row_num", row_number().over(window_dedup)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")

    print(f"MERGE source prepared:")
    print(f"  New wholesalers: {new_count}")
    print(f"  Existing wholesalers: {existing_count}")

    if df_merge_source.count() > 0:
        df_merge_source.createOrReplaceTempView("merge_source")

        spark.sql(f"""
            MERGE INTO {Config.TBL_DIM_WHOLESALER} AS target
            USING merge_source AS source
            ON target.WholeSalerName = source.WholeSalerName
            WHEN MATCHED THEN
                UPDATE SET
                    target.WholeSalerID = source.WholeSalerID
            WHEN NOT MATCHED THEN
                INSERT (WholeSalerSK, WholeSalerID, WholeSalerName)
                VALUES (source.WholeSalerSK, source.WholeSalerID, source.WholeSalerName)
        """)

        print(f"\nINCREMENTAL LOAD completed")
        print(f"  New records inserted: {new_count}")
        print(f"  Existing records updated: {existing_count}")
    else:
        print("\nNo changes detected - skipping MERGE")

# COMMAND ----------

# DBTITLE 1,Validation
# Validate wholesaler dimension
query = f"""
SELECT
    WholeSalerSK,
    WholeSalerID,
    WholeSalerName
FROM {Config.TBL_DIM_WHOLESALER}
WHERE WholeSalerSK != -1
ORDER BY WholeSalerSK
LIMIT 10
"""

display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Return Summary
final_count = spark.read.table(Config.TBL_DIM_WHOLESALER).filter(col("WholeSalerSK") != -1).count()

summary = {
    "status": "success",
    "environment": Config.ENVIRONMENT,
    "catalog": Config.CATALOG,
    "batch_date": batch_date,
    "load_since": load_since,
    "load_type": load_type,
    "dimension": "DimWholesaler",
    "scd_type": "Type 1",
    "new_wholesalers": new_count,
    "updated_wholesalers": existing_count,
    "total_wholesalers": final_count,
    "table": Config.TBL_DIM_WHOLESALER
}

print("\n" + "="*70)
print("DIM_WHOLESALER (SCD-1) PROCESSING COMPLETE")
print("="*70)
for key, value in summary.items():
    print(f"  {key}: {value}")
print("="*70)

if final_count > 20:
    print(f"\nWARNING: Wholesaler count ({final_count:,}) exceeds expected (~7-10)")

dbutils.notebook.exit(str(summary))

# COMMAND ----------

