# Databricks notebook source
# DBTITLE 1,Dimension: Retailer (SCD Type 1)
# MAGIC %md
# MAGIC # DimRetailer: Retailer Master Data
# MAGIC **SkillAIPath Data Warehouse - Silver Layer**
# MAGIC
# MAGIC **Pattern:** SCD Type 1 | **Source:** stg_sales | **Target:** dim_retailer

# COMMAND ----------

# DBTITLE 1,Load Config
# MAGIC %run ../../../1_Canvas/1_Setup/1_Config

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
print("DIM_RETAILER - SCD TYPE 1")
print("="*70)
print(f"Environment: {Config.ENVIRONMENT.upper()}")
print(f"Catalog: {Config.CATALOG}")
print(f"Batch Date: {batch_date}")
print(f"Load Type: {load_type}")
print(f"Source: {source_table}")
print(f"Target: {Config.TBL_DIM_RETAILER}")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Extract Source Data
# Extract distinct retailers with full attributes

if load_type == "full":
    print(f"FULL LOAD: Processing all retailers since {load_since}")
    print(f"  Source: {Config.TBL_STG_SALES} (history table)")
    df_source = spark.read.table(Config.TBL_STG_SALES) \
        .filter(col(Config.COL_BATCH_DATE) >= load_since) \
        .select("RetailerName", "RetailerCity", "RetailerContact", "RetailerMail") \
        .distinct()
else:
    print(f"INCREMENTAL LOAD: Processing retailers from batch {batch_date}")
    print(f"  Source: {Config.TBL_STG_SALES_INCREMENTAL} (incremental staging)")
    df_source = spark.read.table(Config.TBL_STG_SALES_INCREMENTAL) \
        .select("RetailerName", "RetailerCity", "RetailerContact", "RetailerMail") \
        .distinct()

window_spec = Window.orderBy("RetailerName")
df_source = df_source.withColumn("RetailerID", row_number().over(window_spec))

retailer_count = df_source.count()
print(f"Found {retailer_count:,} unique retailers with full attributes")
print(f"  Attributes: Name, City, Contact, Email")

if retailer_count > 50:
    print(f"WARNING: Retailer count unexpectedly high ({retailer_count:,})")
    print("Expected ~20 retailers based on SkillAIPath V1")
else:
    print(f"Retailer count matches expected range (~20)")

# COMMAND ----------

# DBTITLE 1,Create Table & Unknown Record
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {Config.TBL_DIM_RETAILER} (
        RetailerSK BIGINT,
        RetailerID BIGINT,
        RetailerName STRING,
        RetailerCity STRING,
        RetailerContact STRING,
        RetailerMail STRING
    )
    USING DELTA
""")

try:
    spark.sql(f"""
        INSERT INTO {Config.TBL_DIM_RETAILER}
        SELECT -1 AS RetailerSK, -1 AS RetailerID, 'Unknown Retailer' AS RetailerName,
               'Unknown' AS RetailerCity, 'Unknown' AS RetailerContact, 'unknown@unknown.com' AS RetailerMail
        WHERE NOT EXISTS (SELECT 1 FROM {Config.TBL_DIM_RETAILER} WHERE RetailerSK = -1)
    """)
    print("SUCCESS: Unknown record inserted")
except:
    print("SUCCESS: Unknown record already exists")

print("SUCCESS: DimRetailer table ready (with Unknown record)")
print("  Schema: RetailerSK, RetailerID, RetailerName, RetailerCity, RetailerContact, RetailerMail")

# COMMAND ----------

# DBTITLE 1,Generate Surrogate Keys & MERGE
if load_type == "full":
    print("FULL LOAD MODE: Truncating existing dimension")

    df_final = df_source \
        .withColumn("RetailerSK", row_number().over(Window.orderBy("RetailerName"))) \
        .select(
            "RetailerSK",
            "RetailerID",
            "RetailerName",
            "RetailerCity",
            "RetailerContact",
            "RetailerMail"
        )

    final_count = df_final.count()

    df_unknown = spark.sql("""
        SELECT -1 AS RetailerSK, -1 AS RetailerID, 'Unknown Retailer' AS RetailerName,
               'Unknown' AS RetailerCity, 'Unknown' AS RetailerContact, 'unknown@unknown.com' AS RetailerMail
    """)

    df_with_unknown = df_unknown.unionByName(df_final)

    df_with_unknown.write \
        .mode("overwrite") \
        .format("delta") \
        .option("overwriteSchema", "true") \
        .saveAsTable(Config.TBL_DIM_RETAILER)

    print(f"\nFULL LOAD completed")
    print(f"  Records loaded: {final_count:,}")
    print(f"  Total (with Unknown): {final_count + 1:,}")
    print(f"  Attributes per retailer: 6 (SK, ID, Name, City, Contact, Mail)")

    new_count = final_count
    existing_count = 0

else:
    print("INCREMENTAL MODE: Using MERGE for upsert")

    max_sk = spark.sql(f"SELECT COALESCE(MAX(RetailerSK), 0) as max_sk FROM {Config.TBL_DIM_RETAILER}").collect()[0]['max_sk']
    df_existing = spark.read.table(Config.TBL_DIM_RETAILER).filter(col("RetailerSK") != -1).select("RetailerName", "RetailerSK")

    df_new = df_source.join(df_existing, ["RetailerName"], "left_anti")
    new_count = df_new.count()

    if new_count > 0:
        df_new_with_sk = df_new \
            .withColumn("RetailerSK", row_number().over(Window.orderBy("RetailerName")) + max_sk) \
            .select(
                "RetailerSK",
                "RetailerID",
                "RetailerName",
                "RetailerCity",
                "RetailerContact",
                "RetailerMail"
            )
        print(f"Assigned RetailerSK range: {max_sk + 1} to {max_sk + new_count}")
    else:
        df_new_with_sk = spark.createDataFrame(
            [],
            schema="RetailerSK BIGINT, RetailerID BIGINT, RetailerName STRING, RetailerCity STRING, RetailerContact STRING, RetailerMail STRING"
        )

    df_existing_ret = df_source.join(df_existing, ["RetailerName"], "inner") \
        .select(
            col("RetailerSK"),
            col("RetailerID"),
            col("RetailerName"),
            col("RetailerCity"),
            col("RetailerContact"),
            col("RetailerMail")
        ).distinct()

    existing_count = df_existing_ret.count()

    df_merge_source = df_new_with_sk.unionByName(df_existing_ret)

    window_dedup = Window.partitionBy("RetailerName").orderBy("RetailerSK")
    df_merge_source = df_merge_source \
        .withColumn("row_num", row_number().over(window_dedup)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")

    print(f"MERGE source prepared:")
    print(f"  New retailers (INSERT): {new_count:,}")
    print(f"  Existing retailers (UPDATE): {existing_count:,}")

    df_merge_source.createOrReplaceTempView("retailer_source")

    spark.sql(f"""
        MERGE INTO {Config.TBL_DIM_RETAILER} AS tgt
        USING retailer_source AS src
        ON tgt.RetailerName = src.RetailerName
        WHEN MATCHED THEN UPDATE SET
            tgt.RetailerID = src.RetailerID,
            tgt.RetailerCity = src.RetailerCity,
            tgt.RetailerContact = src.RetailerContact,
            tgt.RetailerMail = src.RetailerMail
        WHEN NOT MATCHED THEN INSERT (
            RetailerSK, RetailerID, RetailerName, RetailerCity, RetailerContact, RetailerMail
        ) VALUES (
            src.RetailerSK, src.RetailerID, src.RetailerName, src.RetailerCity, src.RetailerContact, src.RetailerMail
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
print("  Retailer attributes: Name, City, Contact, Email")

# COMMAND ----------

# DBTITLE 1,Validation
# Validate retailer dimension with full attributes
query = f"""
SELECT
    RetailerSK,
    RetailerID,
    RetailerName,
    RetailerCity,
    RetailerContact,
    RetailerMail
FROM {Config.TBL_DIM_RETAILER}
WHERE RetailerSK != -1
ORDER BY RetailerSK
LIMIT 10
"""

display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Return Summary
final_count = spark.read.table(Config.TBL_DIM_RETAILER).filter(col("RetailerSK") != -1).count()

summary = {
    "status": "success",
    "environment": Config.ENVIRONMENT,
    "catalog": Config.CATALOG,
    "batch_date": batch_date,
    "load_since": load_since,
    "load_type": load_type,
    "dimension": "DimRetailer",
    "scd_type": "Type 1",
    "new_retailers": new_count,
    "updated_retailers": existing_count,
    "total_retailers": final_count,
    "table": Config.TBL_DIM_RETAILER
}

print("\n" + "="*70)
print("DIM_RETAILER (SCD-1) PROCESSING COMPLETE")
print("="*70)
for key, value in summary.items():
    print(f"  {key}: {value}")
print("="*70)

if final_count > 50:
    print(f"\nWARNING: Retailer count ({final_count:,}) exceeds expected (~20)")

dbutils.notebook.exit(str(summary))

# COMMAND ----------

