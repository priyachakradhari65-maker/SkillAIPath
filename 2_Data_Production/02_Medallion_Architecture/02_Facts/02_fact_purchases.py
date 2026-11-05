# Databricks notebook source
# DBTITLE 1,FactPurchases - Wholesale Purchase Transactions
# MAGIC %md
# MAGIC # FactPurchases: Wholesale Purchase Transactions
# MAGIC **SkillAIPath Data Warehouse - Silver Layer**
# MAGIC
# MAGIC **Architecture:** Transaction-level purchase facts from wholesale procurement
# MAGIC
# MAGIC **Grain:** One row per purchase transaction
# MAGIC
# MAGIC **Measures:** Quantity, Price
# MAGIC
# MAGIC **Business Questions:**
# MAGIC * Which suppliers provide best prices?
# MAGIC * What is margin analysis (sales price - purchase price)?
# MAGIC * Which products have highest procurement costs?
# MAGIC * What is supplier performance by category?

# COMMAND ----------

# DBTITLE 1,Load Config
# MAGIC %run ../../../1_Canvas/1_Setup/1_Config

# COMMAND ----------

# DBTITLE 1,Configuration - DevOps Ready (Environment-Based)
# ================================================================
# SKILLAIPPATH CONFIGURATION - DevOps Ready
# ================================================================

from datetime import datetime
from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring
from pyspark.sql.window import Window

# Detect batch_date from history table first, then try incremental table
try:
    max_batch = spark.sql(f"SELECT MAX({Config.COL_BATCH_DATE}) FROM {Config.TBL_STG_PURCHASE}").collect()[0][0]
    batch_date = str(max_batch) if max_batch else str(datetime.now().date())
except:
    try:
        max_batch = spark.sql(f"SELECT MAX({Config.COL_BATCH_DATE}) FROM {Config.TBL_STG_PURCHASE_INCREMENTAL}").collect()[0][0]
        batch_date = str(max_batch) if max_batch else str(datetime.now().date())
    except:
        batch_date = str(datetime.now().date())

print("="*70)
print("FACT_PURCHASES - WHOLESALE PROCUREMENT")
print("="*70)
print(f"Environment: {Config.ENVIRONMENT.upper()}")
print(f"Catalog: {Config.CATALOG}")
print(f"Batch Date: {batch_date}")
print(f"Source: {Config.TBL_STG_PURCHASE}")
print(f"Target: {Config.TBL_FACT_PURCHASE}")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Setup Parameters
# Setup load parameters
dbutils.widgets.dropdown("load_type", "full", ["full", "incremental"], "Load Type")
dbutils.widgets.text("load_since", "2023-06-13", "Load Since (YYYY-MM-DD)")

load_type = dbutils.widgets.get("load_type")
load_since = dbutils.widgets.get("load_since")

print(f"Load Type: {load_type}")
if load_type == "full":
    print(f"Load Since: {load_since}")
else:
    print(f"Batch Date: {batch_date}")

# COMMAND ----------

# DBTITLE 1,Extract Source Data
# Read staging purchase data based on load type
# Switch source table: FULL uses history table, INCREMENTAL uses incremental staging table

if load_type == "full":
    source_table = Config.TBL_STG_PURCHASE
    print("="*70)
    print("📊 FULL LOAD MODE")
    print("="*70)
    print(f"Source: {source_table} (full history)")
    print(f"Filter: PurchaseDate >= {load_since}")
    print("="*70)
    
    df_stg = spark.table(source_table) \
        .filter(col("PurchaseDate") >= load_since)
else:
    source_table = Config.TBL_STG_PURCHASE_INCREMENTAL
    print("="*70)
    print("🔄 INCREMENTAL LOAD MODE")
    print("="*70)
    print(f"Source: {source_table} (incremental staging)")
    print(f"Batch Date: {batch_date}")
    print("="*70)
    
    df_stg = spark.table(source_table)

record_count = df_stg.count()
print(f"SUCCESS: Read {record_count:,} purchase records")

# COMMAND ----------

# DBTITLE 1,Lookup Surrogate Keys
# Read dimensions
df_product = spark.table(Config.TBL_DIM_PRODUCT)
df_warehouse = spark.table(Config.TBL_DIM_WAREHOUSE)
df_wholesaler = spark.table(Config.TBL_DIM_WHOLESALER)
df_date = spark.table(Config.TBL_DIM_DATE)

print("SUCCESS: Loaded 4 dimensions (Product, Warehouse, Wholesaler, Date)")

# Build fact with surrogate keys (joining on names from staging)
df_fact = df_stg \
    .join(df_product.select("ProductSK", "ProductName"), on="ProductName", how="left") \
    .join(df_warehouse.select("WarehouseSK", "WarehouseName").withColumnRenamed("WarehouseName", "WareHouseName"), on="WareHouseName", how="left") \
    .join(df_wholesaler.select("WholeSalerSK", "WholeSalerName"), on="WholeSalerName", how="left") \
    .join(df_date.select(col("DateID").alias("PurchaseDateID"), "Date"), to_date(col("PurchaseDate")) == col("Date"), how="left") \
    .select(
        col("PurchaseID").cast("bigint"),
        coalesce(col("ProductSK"), lit(-1)).alias("ProductSK"),
        coalesce(col("WarehouseSK"), lit(-1)).alias("WarehouseSK"),
        coalesce(col("WholeSalerSK"), lit(-1)).alias("WholesalerSK"),
        coalesce(col("PurchaseDateID"), lit(-1)).alias("PurchaseDateID"),
        col("Quantity").cast("int"),
        col("UnitCost").cast("decimal(10,2)"),
        col("NetAmount").cast("decimal(10,2)"),
        col("PurchaseDate").cast("date"),
        col("LoadedAt").cast("timestamp")
    )

fact_count = df_fact.count()
print(f"SUCCESS: Built FactPurchases with {fact_count:,} rows")
print("NOTE: coalesce(col, -1) handles orphans: Missing dimension keys default to -1 (Unknown)")

# COMMAND ----------

# DBTITLE 1,Write FactPurchases
# Write fact table based on load type
load_type = dbutils.widgets.get("load_type")
fact_count = df_fact.count()

if load_type == "full":
    # FULL LOAD: Truncate and reload all facts
    print("FULL LOAD MODE: Truncating existing facts")
    df_fact.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(Config.TBL_FACT_PURCHASE)
    print(f"\nFULL LOAD completed")
    print(f"  Records loaded: {fact_count:,}")
    print(f"  Mode: OVERWRITE (complete refresh)")
else:
    # INCREMENTAL LOAD: Append new batch
    print("INCREMENTAL MODE: Appending new batch")
    df_fact.write.mode("append").format("delta").saveAsTable(Config.TBL_FACT_PURCHASE)
    print(f"\nINCREMENTAL LOAD completed")
    print(f"  Records loaded: {fact_count:,}")
    print(f"  Mode: APPEND (additive)")

print(f"\nTable: {Config.TBL_FACT_PURCHASE}")
print("NOTE: Facts are additive - each batch adds to history (unless full reload)")

# COMMAND ----------

# DBTITLE 1,Validation - Purchase Summary
# Purchase Summary - Scope based on load_type
if load_type == "incremental":
    where_clause = f"WHERE PurchaseDate = '{batch_date}'"
    scope_msg = f"INCREMENTAL VALIDATION: Purchase summary for batch {batch_date}"
else:
    where_clause = f"WHERE PurchaseDate >= '{load_since}'"
    scope_msg = f"FULL VALIDATION: Purchase summary for all records since {load_since}"

query = f"""
SELECT 
    COUNT(*) AS purchase_count,
    ROUND(SUM(UnitCost * Quantity) / 10000000, 2) AS total_procurement_crores,
    ROUND(AVG(UnitCost), 2) AS avg_unit_price,
    SUM(CASE WHEN ProductSK = -1 THEN 1 ELSE 0 END) AS orphan_products,
    SUM(CASE WHEN WarehouseSK = -1 THEN 1 ELSE 0 END) AS orphan_warehouses
FROM {Config.TBL_FACT_PURCHASE}
{where_clause}
"""

print("="*70)
print("VALIDATION 1: PURCHASE SUMMARY")
print("="*70)
print(scope_msg)
print("="*70)

display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Validation - Row Count Match (Source vs Fact)
# Validate that fact_purchase has same row count as stg_purchase (no data loss)
# Scope based on load_type - SWITCH SOURCE TABLE for incremental
if load_type == "incremental":
    source_table = Config.TBL_STG_PURCHASE_INCREMENTAL
    source_where = f"WHERE PurchaseDate = '{batch_date}'"
    fact_where = f"WHERE PurchaseDate = '{batch_date}'"
    scope_msg = f"INCREMENTAL VALIDATION: Comparing batch {batch_date}"
else:
    source_table = Config.TBL_STG_PURCHASE
    source_where = f"WHERE PurchaseDate >= '{load_since}'"
    fact_where = f"WHERE PurchaseDate >= '{load_since}'"
    scope_msg = f"FULL VALIDATION: Comparing all records since {load_since}"

query = f"""
WITH source_count AS (
    SELECT COUNT(*) AS source_rows
    FROM {source_table}
    {source_where}
),
fact_count AS (
    SELECT COUNT(*) AS fact_rows
    FROM {Config.TBL_FACT_PURCHASE}
    {fact_where}
)
SELECT 
    s.source_rows,
    f.fact_rows,
    (f.fact_rows - s.source_rows) AS difference,
    CASE 
        WHEN f.fact_rows = s.source_rows THEN '✅ PASS - Row counts match'
        WHEN f.fact_rows < s.source_rows THEN '❌ FAIL - Missing rows in fact table'
        ELSE '⚠️ WARNING - Fact table has more rows than source'
    END AS validation_status
FROM source_count s, fact_count f
"""

print("="*70)
print("VALIDATION 2: ROW COUNT MATCH")
print("="*70)
print(scope_msg)
print(f"Source: {source_table}")
print(f"Target: {Config.TBL_FACT_PURCHASE}")
print("="*70)

display(spark.sql(query))

# Clean exit for orchestration
dbutils.notebook.exit("SUCCESS")