# Databricks notebook source
# DBTITLE 1,Dimension: Product (SCD Type 1)
# MAGIC %md
# MAGIC # DimProduct: Slowly Changing Dimension Type 1
# MAGIC **SkillAIPath Data Warehouse - Silver Layer**
# MAGIC
# MAGIC ## Module Overview
# MAGIC **Purpose:** Product catalog (ProductID, ProductName, Category, MRP)
# MAGIC
# MAGIC **Pattern:** SCD Type 1 (Overwrite on change - NO history)
# MAGIC
# MAGIC **Source:** `sap_bronze.stg_sales` (distinct products)
# MAGIC
# MAGIC **Target:** `sap_silver.dim_product`
# MAGIC
# MAGIC **Key Concepts:**
# MAGIC - Type 1 vs Type 2 (when to use which)
# MAGIC - MERGE/UPSERT pattern (UPDATE existing, INSERT new)
# MAGIC - Product master data (typically no history needed)

# COMMAND ----------

# DBTITLE 1,Load Config
# MAGIC %run ../../../1_Canvas/1_Setup/1_Config

# COMMAND ----------

# DBTITLE 1,Parameters & Config Import
# Config loaded via %run cell above

# COMMAND ----------

# DBTITLE 1,Read Source Data - Distinct Products
from datetime import datetime
from pyspark.sql.functions import *
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
print("DIM_PRODUCT - SCD TYPE 1")
print("="*70)
print(f"Environment: {Config.ENVIRONMENT.upper()}")
print(f"Catalog: {Config.CATALOG}")
print(f"Batch Date: {batch_date}")
print(f"Load Type: {load_type}")
print(f"Source: {source_table}")
print(f"Target: {Config.TBL_DIM_PRODUCT}")
print("="*70)

if load_type == "full":
    print(f"\nFULL LOAD: Processing all products since {load_since}")
    print(f"  Source: {Config.TBL_STG_SALES} (history table)")
    df_products = spark.read.table(Config.TBL_STG_SALES) \
        .filter(col(Config.COL_BATCH_DATE) >= load_since) \
        .select("ProductName") \
        .distinct()
else:
    print(f"\nINCREMENTAL LOAD: Processing new products from batch {batch_date}")
    print(f"  Source: {Config.TBL_STG_SALES_INCREMENTAL} (incremental staging)")
    df_products = spark.read.table(Config.TBL_STG_SALES_INCREMENTAL) \
        .select("ProductName") \
        .distinct()

# NEW DATA GENERATOR: All 3 tables (sales, stock, purchase) use same 78 products
# No need for hybrid logic - just use products from stg_sales directly

# >>> FIX (Phase P1, 2026-11-03): deterministic business key <<<
# row_number() alone restarts at 1, so in incremental mode new products were
# assigned ProductIDs 1..N that COLLIDE with existing rows (dim_product.ProductID
# duplicates). Incremental runs must continue from MAX(ProductID); full loads
# renumber 1..N deterministically (same content & order as the dbt dim_product
# rebuild, keeping both writers SK-stable).
if load_type == "full":
    df_products = df_products.withColumn(
        "ProductID", row_number().over(Window.orderBy("ProductName")))
else:
    _max_pid = spark.sql(
        f"SELECT COALESCE(MAX(ProductID), 0) AS m FROM {Config.TBL_DIM_PRODUCT}"
    ).collect()[0]['m']
    df_products = df_products.withColumn(
        "ProductID", row_number().over(Window.orderBy("ProductName")) + _max_pid)

# >>> REMOVED (Phase P1, 2026-11-03): the "OnePlus Nord 3" -> "OnePlus Nord 3 CE" rename <<<
# This notebook-side rename was the direct cause of ALL 200 ProductSK = -1 orphans:
# the notebook-written dim (4AM, Job 05) contained "OnePlus Nord 3 CE" while bronze
# facts reference "OnePlus Nord 3", and the dbt-built dim (5AM) contains the
# un-renamed name. Facts joined at 4AM missed the product (-1): 20 rows in
# fact_sales + 180 rows in fact_inventory (its full daily rebuild re-orphaned the
# entire history). The name must pass through unchanged so both dim writers derive
# identical content from bronze.

product_count = df_products.count()
print(f"\nFound {product_count:,} products from stg_sales")
print(f"Note: New data generator ensures consistent products across all tables")

# COMMAND ----------

# DBTITLE 1,Create Dimension Table if Not Exists
# Create table schema (first run)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {Config.TBL_DIM_PRODUCT} (
        ProductSK BIGINT,
        ProductID BIGINT,
        ProductName STRING,
        MRP DOUBLE
    )
    USING DELTA
""")

# Insert Unknown record (ProductSK = -1) using INSERT
try:
    spark.sql(f"""
        INSERT INTO {Config.TBL_DIM_PRODUCT}
        SELECT -1 AS ProductSK, -1 AS ProductID, 'Unknown Product' AS ProductName, 0.0 AS MRP
        WHERE NOT EXISTS (SELECT 1 FROM {Config.TBL_DIM_PRODUCT} WHERE ProductSK = -1)
    """)
    print(" Unknown record inserted")
except:
    print(" Unknown record already exists")

print(" DimProduct table ready (with Unknown record)")

#  Interview: Always include Unknown record (-1) for graceful handling of orphan facts

# COMMAND ----------

# DBTITLE 1,Generate Surrogate Keys for New Products
# Add product attributes from bronze
# Bronze table schema: ProductName, sellingPrice only

from pyspark.sql.functions import first, max as spark_max

df_product_attrs = spark.read.table(Config.TBL_STG_SALES) \
    .select(
        "ProductName",
        "sellingPrice"
    )

# Get representative attributes per product
# Use max() for MRP
df_products_with_attrs = df_product_attrs.groupBy("ProductName").agg(
    spark_max("sellingPrice").alias("MRP")
)

# Join back to get ProductID (LEFT join to keep placeholder products)
df_source = df_products.join(
    df_products_with_attrs,
    ["ProductName"],
    "left"  # Keep ALL products, even placeholders
).withColumn("MRP", coalesce(col("MRP"), lit(0.0)))  # Default values for placeholders

print(f"Products with attributes assigned: {df_source.count():,}")
print("\nNote: MRP represents catalog/list price (MAX selling price)")
print("Actual transaction prices are stored in fact_sales table")

# Get max existing ProductSK for surrogate key generation
max_sk_result = spark.sql(f"SELECT COALESCE(MAX(ProductSK), 0) as max_sk FROM {Config.TBL_DIM_PRODUCT}").collect()
max_sk = max_sk_result[0]['max_sk']

# Read existing products to identify NEW vs EXISTING
df_existing = spark.read.table(Config.TBL_DIM_PRODUCT) \
    .filter(col("ProductSK") != -1) \
    .select("ProductName", "ProductSK")

# Identify NEW products (not in dimension yet)
df_new_products = df_source.join(
    df_existing,
    ["ProductName"],
    "left_anti"
)
new_count = df_new_products.count()

print(f"\nNEW products to insert: {new_count:,}")

# Assign surrogate keys to new products
if new_count > 0:
    window_sk = Window.orderBy("ProductName")
    df_new_with_sk = df_new_products \
        .withColumn("ProductSK", row_number().over(window_sk) + max_sk) \
        .select(
            "ProductSK",
            "ProductID",
            "ProductName",
            "MRP"
        )
    
    print(f"Assigned ProductSK range: {max_sk + 1} to {max_sk + new_count}")
else:
    # Create empty DataFrame with correct schema
    df_new_with_sk = spark.createDataFrame(
        [],
        schema="ProductSK BIGINT, ProductID BIGINT, ProductName STRING, MRP DOUBLE"
    )

# COMMAND ----------

# DBTITLE 1,MERGE into Dimension Table (Upsert)
# Different write strategies based on load_type

if load_type == "full":
    # FULL LOAD: Truncate existing data and reload all products
    print("FULL LOAD MODE: Truncating existing dimension")
    
    # Prepare final dataset (new products only, since we're replacing all)
    df_final = df_source \
        .withColumn("ProductSK", row_number().over(Window.orderBy("ProductName"))) \
        .select(
            "ProductSK",
            "ProductID",
            "ProductName",
            "MRP"
        )
    
    final_count = df_final.count()
    
    # Write with overwrite mode (includes Unknown record)
    df_unknown = spark.sql("""
        SELECT -1 AS ProductSK, -1 AS ProductID, 'Unknown Product' AS ProductName, 0.0 AS MRP
    """)
    
    df_with_unknown = df_unknown.unionByName(df_final)
    
    df_with_unknown.write \
        .mode("overwrite") \
        .format("delta") \
        .option("overwriteSchema", "true") \
        .saveAsTable(Config.TBL_DIM_PRODUCT)
    
    print(f"\nFULL LOAD completed")
    print(f"  Records loaded: {final_count:,}")
    print(f"  Total (with Unknown): {final_count + 1:,}")
    
    new_count = final_count
    existing_count = 0
    
else:
    # INCREMENTAL LOAD: MERGE (UPDATE existing + INSERT new)
    print("INCREMENTAL MODE: Using MERGE for upsert")
    
    # Prepare source for MERGE (new + existing products)
    df_existing_products = df_source \
        .join(
            df_existing,
            ["ProductName"],
            "inner"
        ) \
        .select(
            "ProductSK",
            "ProductID",
            "ProductName",
            "MRP"
        ) \
        .distinct()
    
    existing_count = df_existing_products.count()
    
    # Union NEW and EXISTING products
    df_merge_source = df_new_with_sk.unionByName(df_existing_products)
    
    # Final deduplication by ProductName (business key)
    window_dedup = Window.partitionBy("ProductName").orderBy("ProductSK")
    df_merge_source = df_merge_source \
        .withColumn("row_num", row_number().over(window_dedup)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    print(f"MERGE source prepared:")
    print(f"  New products (INSERT): {new_count:,}")
    print(f"  Existing products (UPDATE): {existing_count:,}")
    print(f"  Total in merge: {df_merge_source.count():,}")
    
    # Create temp view for MERGE
    df_merge_source.createOrReplaceTempView("product_source")
    
    # Execute MERGE (SCD Type 1: UPDATE existing, INSERT new)
    merge_result = spark.sql(f"""
        MERGE INTO {Config.TBL_DIM_PRODUCT} AS tgt
        USING product_source AS src
        ON tgt.ProductName = src.ProductName
        WHEN MATCHED THEN 
            UPDATE SET 
                tgt.MRP = src.MRP
        WHEN NOT MATCHED THEN
            INSERT (ProductSK, ProductID, ProductName, MRP)
            VALUES (src.ProductSK, src.ProductID, src.ProductName, src.MRP)
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

# DBTITLE 1,Validation Query 1 - Check Dimension Row Count
# Verify dimension populated correctly
query = f"""
SELECT 
    COUNT(*) AS total_products,
    COUNT(DISTINCT ProductID) AS unique_product_ids,
    COUNT(DISTINCT ProductSK) AS unique_product_sks
FROM {Config.TBL_DIM_PRODUCT}
WHERE ProductSK != -1
"""

display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Validation Query 2 - Sample Products
# Sample product records
query = f"""
SELECT 
    ProductSK,
    ProductID,
    ProductName,
    MRP
FROM {Config.TBL_DIM_PRODUCT}
WHERE ProductSK != -1
ORDER BY ProductSK
LIMIT 20
"""

display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Return Summary for Orchestration
# Return processing summary
final_count = spark.read.table(Config.TBL_DIM_PRODUCT).filter(col("ProductSK") != -1).count()

summary = {
    "status": "success",
    "environment": Config.ENVIRONMENT,
    "catalog": Config.CATALOG,
    "batch_date": batch_date,
    "load_since": load_since,
    "load_type": load_type,
    "dimension": "DimProduct",
    "scd_type": "Type 1",
    "new_products": new_count,
    "updated_products": existing_count,
    "total_products": final_count,
    "table": Config.TBL_DIM_PRODUCT
}

print("\n" + "="*70)
print("DIM_PRODUCT (SCD-1) PROCESSING COMPLETE")
print("="*70)
for key, value in summary.items():
    print(f"  {key}: {value}")
print("="*70)

if final_count > 100:
    print(f"\nWARNING: Product count ({final_count:,}) exceeds expected (~44)")
    print("Bronze data may have incorrect product catalog structure")
    print("Review: ../00_UTILS/generate_batch.ipynb")

dbutils.notebook.exit(str(summary))

# COMMAND ----------

