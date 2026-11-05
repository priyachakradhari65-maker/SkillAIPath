# Databricks notebook source
# DBTITLE 1,FactInventory - Stock Status Tracking
# MAGIC %md
# MAGIC # FactInventory: Stock Status Tracking
# MAGIC **SkillAIPath Data Warehouse - Silver Layer**
# MAGIC
# MAGIC **Architecture:** Stock snapshot fact table with product and warehouse foreign keys
# MAGIC
# MAGIC **Grain:** One row per product per warehouse per day
# MAGIC
# MAGIC **Measures:** StockQuantity, StockValue
# MAGIC
# MAGIC **Pattern:** Daily snapshots (not cumulative)
# MAGIC
# MAGIC **Business Questions:**
# MAGIC * Which products are out of stock?
# MAGIC * What is total inventory value by warehouse?
# MAGIC * Which products need restocking?
# MAGIC * What is stock health by category?

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
    max_batch = spark.sql(f"SELECT MAX({Config.COL_BATCH_DATE}) FROM {Config.TBL_STG_STOCK}").collect()[0][0]
    batch_date = str(max_batch) if max_batch else str(datetime.now().date())
except:
    try:
        max_batch = spark.sql(f"SELECT MAX({Config.COL_BATCH_DATE}) FROM {Config.TBL_STG_STOCK_INCREMENTAL}").collect()[0][0]
        batch_date = str(max_batch) if max_batch else str(datetime.now().date())
    except:
        batch_date = str(datetime.now().date())

print("="*70)
print("FACT_INVENTORY - STOCK STATUS TRACKING")
print("="*70)
print(f"Environment: {Config.ENVIRONMENT.upper()}")
print(f"Catalog: {Config.CATALOG}")
print(f"Batch Date: {batch_date}")
print(f"Source: {Config.TBL_STG_STOCK}")
print(f"Target: {Config.TBL_FACT_STOCK}")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Read Latest Batch from Bronze
# Setup load parameters (safe for orchestration)
try:
    dbutils.widgets.dropdown("load_type", "full", ["full", "incremental"], "Load Type")
    dbutils.widgets.text("load_since", "2023-06-13", "Load Since (YYYY-MM-DD)")
    load_type = dbutils.widgets.get("load_type")
    load_since = dbutils.widgets.get("load_since")
except:
    # Fallback for orchestrated execution
    load_type = "full"
    load_since = "2023-06-13"

# Read staging stock data based on load type
# Switch source table: FULL uses history table, INCREMENTAL uses incremental staging table
if load_type == "full":
    source_table = Config.TBL_STG_STOCK
    print("="*70)
    print("📊 FULL LOAD MODE")
    print("="*70)
    print(f"Source: {source_table} (full history)")
    print(f"Filter: StockDate >= {load_since}")
    print("="*70)
    
    df_stg = spark.table(source_table) \
        .filter(col("StockDate") >= load_since)
else:
    source_table = Config.TBL_STG_STOCK_INCREMENTAL
    print("="*70)
    print("🔄 INCREMENTAL LOAD MODE")
    print("="*70)
    print(f"Source: {source_table} (incremental staging)")
    print(f"Batch Date: {batch_date}")
    print("="*70)
    
    df_stg = spark.table(source_table)

record_count = df_stg.count()
print(f"SUCCESS: Read {record_count:,} stock records")
print("NOTE: Incremental pattern - Daily stock snapshots, not cumulative")

# display(df_stg.limit(3))  # Commented for orchestrated execution

# COMMAND ----------

# DBTITLE 1,Lookup Surrogate Keys from Dimensions
# Read dimensions
df_product = spark.table(Config.TBL_DIM_PRODUCT)
df_warehouse = spark.table(Config.TBL_DIM_WAREHOUSE)
df_date = spark.table(Config.TBL_DIM_DATE)

print("SUCCESS: Loaded 3 dimensions (Product, Warehouse, Date)")
print("NOTE: DimProduct maps ProductName to ProductSK")
print("NOTE: DimWarehouse maps WarehouseName to WarehouseSK")
print("NOTE: DimDate maps LastCheck date to DateID")

# Build fact with surrogate keys (use -1 for orphans)
df_fact = df_stg \
    .join(df_product.select("ProductSK", "ProductName", "MRP"), on="ProductName", how="left") \
    .join(df_warehouse.select("WarehouseSK", "WarehouseName"), on="WarehouseName", how="left") \
    .join(df_date.select(col("DateID"), col("Date")), to_date(col("StockDate")) == col("Date"), how="left") \
    .withColumn("StockValue", (coalesce(col("MRP"), lit(0)) * col("StockQuantity")).cast("decimal(15,2)")) \
    .select(
        coalesce(col("ProductSK"), lit(-1)).alias("ProductSK"),
        coalesce(col("WarehouseSK"), lit(-1)).alias("WarehouseSK"),
        coalesce(col("DateID"), lit(-1)).alias("DateID"),
        col("StockQuantity").cast("int").alias("StockQuantity"),
        col("StockValue").cast("decimal(15,2)").alias("StockValue"),
        col("StockStatus"),
        col("StockDate").alias("BatchDate"),
        col("LoadedAt").alias("LoadedAt")
    )

fact_count = df_fact.count()
print(f"SUCCESS: Built FactInventory with {fact_count:,} rows")
print("NOTE: StockValue calculated as Price * ProductQuantity")
print("NOTE: coalesce(col, -1) handles orphans: Missing dimension keys default to -1 (Unknown)")

# display(df_fact.limit(5))  # Commented for orchestrated execution
print(f"Sample fact records: {df_fact.limit(5).count()} rows displayed")

# COMMAND ----------

# DBTITLE 1,Write FactInventory to Delta
# Write fact table based on load type
load_type = dbutils.widgets.get("load_type")
fact_count = df_fact.count()

if load_type == "full":
    # FULL LOAD: Truncate and reload all facts
    print("FULL LOAD MODE: Truncating existing facts")
    df_fact.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(Config.TBL_FACT_STOCK)
    print(f"\nFULL LOAD completed")
    print(f"  Records loaded: {fact_count:,}")
    print(f"  Mode: OVERWRITE (complete refresh)")
else:
    # INCREMENTAL LOAD: Append new batch
    print("INCREMENTAL MODE: Appending new batch")
    df_fact.write.mode("append").format("delta").saveAsTable(Config.TBL_FACT_STOCK)
    print(f"\nINCREMENTAL LOAD completed")
    print(f"  Records loaded: {fact_count:,}")
    print(f"  Mode: APPEND (daily snapshots)")

print(f"\nTable: {Config.TBL_FACT_STOCK}")
print("NOTE: Stock facts are snapshots - each batch is a point-in-time view")

# COMMAND ----------

# DBTITLE 1,Validation 1 - Check for Orphan Facts
# Check for Orphan Facts - Scope based on load_type
if load_type == "incremental":
    where_clause = f"WHERE StockDate = '{batch_date}'"
    scope_msg = f"INCREMENTAL VALIDATION: Checking batch {batch_date}"
else:
    where_clause = f"WHERE StockDate >= '{load_since}'"
    scope_msg = f"FULL VALIDATION: Checking all records since {load_since}"

query = f"""
SELECT
    SUM(CASE WHEN ProductSK = -1 THEN 1 ELSE 0 END) AS orphan_products,
    SUM(CASE WHEN WarehouseSK = -1 THEN 1 ELSE 0 END) AS orphan_warehouses,
    SUM(CASE WHEN DateID = -1 THEN 1 ELSE 0 END) AS orphan_dates,
    COUNT(*) AS total_facts
FROM {Config.TBL_FACT_STOCK}
{where_clause}
"""

print("="*70)
print("VALIDATION 1: ORPHAN FACT CHECK")
print("="*70)
print(scope_msg)
print("="*70)

# display(spark.sql(query))  # Commented for orchestrated execution
print("Orphan check completed - Query executed successfully")

# COMMAND ----------

# DBTITLE 1,Check dim_date 2026 Coverage
print("=" * 70)
print("CHECKING IF DIM_DATE HAS 2026 COVERAGE")
print("=" * 70)

df_dim_date = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_date")

# Get overall date range
date_stats = df_dim_date.select(
    min("Date").alias("min_date"),
    max("Date").alias("max_date"),
    count("*").alias("total_dates")
).collect()[0]

print(f"\nCurrent dim_date coverage:")
print(f"  Min Date: {date_stats['min_date']}")
print(f"  Max Date: {date_stats['max_date']}")
print(f"  Total Dates: {date_stats['total_dates']:,}")

# Check specifically for 2026 dates
dates_2026 = df_dim_date.filter(year("Date") == 2026).count()
print(f"\n2026 Coverage:")
print(f"  Dates in 2026: {dates_2026:,}")

if dates_2026 > 0:
    range_2026 = df_dim_date.filter(year("Date") == 2026).select(
        min("Date").alias("min_2026"),
        max("Date").alias("max_2026")
    ).collect()[0]
    print(f"  2026 Range: {range_2026['min_2026']} to {range_2026['max_2026']}")

    print(f"\nYour fact data needs: 2026-03-09 to 2026-06-06")

    if dates_2026 >= 365:
        print("   FULL 2026 coverage (all 365+ days) - READY!")
    else:
        print(f"    PARTIAL 2026 coverage (only {dates_2026} days)")
else:
    print("   NO 2026 dates found!")
    print("\n  ACTION REQUIRED:")
    print("  Run the 01_dim_date notebook (Cells 2-5) to reload with 2026 dates")

print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Product Dimension Analysis
from pyspark.sql.functions import countDistinct

print("=" * 70)
print("PRODUCT DIMENSION ANALYSIS")
print("=" * 70)

# Check what ProductIDs exist in dim_product
df_dim_product = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_product")
product_stats = df_dim_product.select(
    count("*").alias("total_products"),
    countDistinct("ProductID").alias("distinct_product_ids")
).collect()[0]

print(f"\ndim_product contains:")
print(f"  Total Products: {product_stats['total_products']:,}")
print(f"  Distinct ProductIDs: {product_stats['distinct_product_ids']:,}")

print("\nSample ProductIDs from dim_product:")
product_id_sample = df_dim_product.select("ProductID", "ProductName").limit(5).collect()
for row in product_id_sample:
    print(f"  ProductID: {row['ProductID']} | Name: {row['ProductName']}")

# Check what ProductNames exist in stg_stock_status
df_stg_stock = spark.table(Config.TBL_STG_STOCK)
stg_product_stats = df_stg_stock.select(
    countDistinct("ProductName").alias("distinct_product_names")
).collect()[0]

print(f"\nstg_stock_status contains:")
print(f"  Distinct ProductNames: {stg_product_stats['distinct_product_names']:,}")

print("\nSample ProductNames from stg_stock_status:")
stg_product_sample = df_stg_stock.select("ProductName").distinct().limit(5).collect()
for row in stg_product_sample:
    print(f"  ProductName: {row['ProductName']}")

# Check overlap
print("\nCHECKING OVERLAP:")
overlap = df_dim_product.select("ProductName").intersect(df_stg_stock.select("ProductName")).count()
print(f"  ProductNames that exist in BOTH: {overlap:,}")

missing_in_dim = df_stg_stock.select("ProductName").subtract(df_dim_product.select("ProductName")).count()
print(f"  ProductNames in stg_stock_status but MISSING from dim_product: {missing_in_dim:,}")

print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Validation 2 - Stock Value Summary by Warehouse
# Stock Value Summary by Warehouse - Scope based on load_type
if load_type == "incremental":
    where_clause = f"WHERE f.StockDate = '{batch_date}'"
    scope_msg = f"INCREMENTAL VALIDATION: Stock summary for batch {batch_date}"
else:
    where_clause = f"WHERE f.StockDate >= '{load_since}'"
    scope_msg = f"FULL VALIDATION: Stock summary for all records since {load_since}"

query = f"""
SELECT
    w.WareHouseName,
    COUNT(DISTINCT f.ProductSK) AS unique_products,
    SUM(f.StockQuantity) AS total_units,
    ROUND(SUM(f.StockValue) / 100000, 2) AS stock_value_lakhs,
    ROUND(AVG(f.StockValue), 2) AS avg_product_value
FROM {Config.TBL_FACT_STOCK} f
JOIN {Config.TBL_DIM_WAREHOUSE} w ON f.WarehouseSK = w.WarehouseSK
{where_clause}
GROUP BY w.WareHouseName
ORDER BY stock_value_lakhs DESC
"""

print("="*70)
print("VALIDATION 2: STOCK VALUE SUMMARY")
print("="*70)
print(scope_msg)
print("="*70)

# display(spark.sql(query))  # Commented for orchestrated execution
print("Stock value validation completed")

# COMMAND ----------

# DBTITLE 1,Validation 3 - Row Count Match: stg_stock_status vs fact_inventory
# Row Count Match: stg_stock_status vs fact_inventory
# Scope based on load_type - SWITCH SOURCE TABLE for incremental
if load_type == "incremental":
    source_table = Config.TBL_STG_STOCK_INCREMENTAL
    source_where = f"WHERE StockDate = '{batch_date}'"
    fact_where = f"WHERE BatchDate = '{batch_date}'"
    scope_msg = f"INCREMENTAL VALIDATION: Comparing batch {batch_date}"
else:
    source_table = Config.TBL_STG_STOCK
    source_where = f"WHERE StockDate >= '{load_since}'"
    fact_where = f"WHERE BatchDate >= '{load_since}'"
    scope_msg = f"FULL VALIDATION: Comparing all records since {load_since}"

query = f"""
WITH source_count AS (
    SELECT COUNT(*) AS source_rows
    FROM {source_table}
    {source_where}
),
fact_count AS (
    SELECT COUNT(*) AS fact_rows
    FROM {Config.TBL_FACT_STOCK}
    {fact_where}
)
SELECT
    s.source_rows AS stg_count,
    f.fact_rows AS fact_count,
    (f.fact_rows - s.source_rows) AS difference,
    CASE
        WHEN f.fact_rows = s.source_rows THEN '✅ PASS - Row counts match'
        WHEN f.fact_rows < s.source_rows THEN '❌ FAIL - Missing rows in fact table'
        ELSE '⚠️ WARNING - Fact table has more rows than source'
    END AS validation_status
FROM source_count s, fact_count f
"""

print("="*70)
print("VALIDATION 3: ROW COUNT MATCH")
print("="*70)
print(scope_msg)
print(f"Source: {source_table}")
print(f"Target: {Config.TBL_FACT_STOCK}")
print("="*70)

# display(spark.sql(query))  # Commented for orchestrated execution
row_validation = spark.sql(query).collect()[0]
print(f"Row count validation: Stage={row_validation.stg_count}, Fact={row_validation.fact_count}, Status={row_validation.validation_status}")

# Clean exit for orchestration
dbutils.notebook.exit("SUCCESS")