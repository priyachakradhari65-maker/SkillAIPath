# Databricks notebook source
# DBTITLE 1,Dimension: Product Category (SCD Type 1)
# MAGIC %md
# MAGIC # DimProductCategory: Product Category Master Data
# MAGIC **SkillAIPath Data Warehouse - Silver Layer**
# MAGIC
# MAGIC **Pattern:** SCD Type 1 | **Source:** stg_sales | **Target:** dim_product_category

# COMMAND ----------

# DBTITLE 0,Load Config
# MAGIC %run ../../../1_Canvas/1_Setup/1_Config

# COMMAND ----------

# DBTITLE 1,Setup Parameters
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
    latest_batch = spark.sql(f"SELECT MAX({Config.COL_BATCH_DATE}) FROM {Config.TBL_STG_SALES}").collect()[0][0]
    batch_date = str(latest_batch) if latest_batch else "2023-06-13"
except:
    try:
        latest_batch = spark.sql(f"SELECT MAX({Config.COL_BATCH_DATE}) FROM {Config.TBL_STG_SALES_INCREMENTAL}").collect()[0][0]
        batch_date = str(latest_batch) if latest_batch else "2023-06-13"
    except:
        batch_date = "2023-06-13"

# Determine source table for logging based on load type
source_table = Config.TBL_STG_SALES_INCREMENTAL if load_type == "incremental" else Config.TBL_STG_SALES

print("="*70)
print("DIM_PRODUCT_CATEGORY - SCD TYPE 1")
print("="*70)
print(f"Environment: {Config.ENVIRONMENT.upper()}")
print(f"Catalog: {Config.CATALOG}")
print(f"Batch Date: {batch_date}")
print(f"Load Type: {load_type}")
print(f"Source: {source_table}")
print(f"Target: {Config.TBL_DIM_CATEGORY}")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Extract Source Data
# Extract distinct product categories

if load_type == "full":
    print(f"FULL LOAD: Processing all categories since {load_since}")
    print(f"  Source: {Config.TBL_STG_SALES} (history table)")
    df_source = spark.read.table(Config.TBL_STG_SALES) \
        .filter(col(Config.COL_BATCH_DATE) >= load_since) \
        .select("ProductCategory") \
        .distinct()
else:
    print(f"INCREMENTAL LOAD: Processing categories from batch {batch_date}")
    print(f"  Source: {Config.TBL_STG_SALES_INCREMENTAL} (incremental staging)")
    df_source = spark.read.table(Config.TBL_STG_SALES_INCREMENTAL) \
        .select("ProductCategory") \
        .distinct()

category_count = df_source.count()
print(f"Found {category_count:,} unique product categories")

# COMMAND ----------

# DBTITLE 1,Create Table & Unknown Record
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {Config.TBL_DIM_CATEGORY} (
        ProductCategorySK BIGINT,
        ProductCategoryID BIGINT,
        ProductCategory STRING
    )
    USING DELTA
""")

spark.sql(f"""
    MERGE INTO {Config.TBL_DIM_CATEGORY} AS tgt
    USING (SELECT -1 AS ProductCategorySK) AS src
    ON tgt.ProductCategorySK = src.ProductCategorySK
    WHEN NOT MATCHED THEN INSERT (ProductCategorySK, ProductCategoryID, ProductCategory) VALUES (-1, -1, 'Unknown')
""")

print("Table ready")

# COMMAND ----------

# DBTITLE 1,Generate Surrogate Keys & MERGE
max_sk = spark.sql(f"SELECT COALESCE(MAX(ProductCategorySK), 0) as max_sk FROM {Config.TBL_DIM_CATEGORY} WHERE ProductCategorySK != -1").collect()[0]['max_sk']
df_existing = spark.read.table(Config.TBL_DIM_CATEGORY).filter(col("ProductCategorySK") != -1).select("ProductCategory", "ProductCategorySK", "ProductCategoryID")

df_new = df_source.join(df_existing, "ProductCategory", "left_anti")
new_count = df_new.count()

if new_count > 0:
    df_new_with_sk = df_new.withColumn("ProductCategorySK", row_number().over(Window.orderBy("ProductCategory")) + max_sk) \
                           .withColumn("ProductCategoryID", col("ProductCategorySK"))
else:
    df_new_with_sk = spark.createDataFrame([], schema="ProductCategorySK BIGINT, ProductCategoryID BIGINT, ProductCategory STRING")

df_existing_pc = df_source.join(df_existing, "ProductCategory", "inner").select(col("ProductCategorySK"), col("ProductCategoryID"), col("ProductCategory"))
df_merge_source = df_new_with_sk.unionByName(df_existing_pc)
df_merge_source.createOrReplaceTempView("product_category_source")

spark.sql(f"""
    MERGE INTO {Config.TBL_DIM_CATEGORY} AS tgt
    USING product_category_source AS src
    ON tgt.ProductCategory = src.ProductCategory
    WHEN MATCHED THEN UPDATE SET tgt.ProductCategory = src.ProductCategory
    WHEN NOT MATCHED THEN INSERT (ProductCategorySK, ProductCategoryID, ProductCategory) VALUES (src.ProductCategorySK, src.ProductCategoryID, src.ProductCategory)
""")

print(f"MERGE complete: {new_count} new")

# COMMAND ----------

# DBTITLE 1,Validation
display(spark.sql(f"SELECT * FROM {Config.TBL_DIM_CATEGORY} WHERE ProductCategorySK != -1 ORDER BY ProductCategorySK"))

# COMMAND ----------

# DBTITLE 1,Return Summary
summary = {
    "status": "success",
    "environment": Config.ENVIRONMENT,
    "catalog": Config.CATALOG,
    "batch_date": batch_date,
    "load_since": load_since,
    "load_type": load_type,
    "dimension": "DimProductCategory",
    "new_categories": new_count,
    "table": Config.TBL_DIM_CATEGORY
}

print("\n" + "="*70)
print("DIM_PRODUCT_CATEGORY (SCD-1) PROCESSING COMPLETE")
print("="*70)
for key, value in summary.items():
    print(f"  {key}: {value}")
print("="*70)

dbutils.notebook.exit(str(summary))