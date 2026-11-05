# Databricks notebook source
# DBTITLE 1,Fact Returns - Refund Analysis
# MAGIC %md
# MAGIC # FactReturns: Product Return & Refund Analysis
# MAGIC **SkillAIPath Data Warehouse - Silver Layer**
# MAGIC
# MAGIC ## Business Case
# MAGIC
# MAGIC **Problem:** Returns cost 15-20% of revenue in e-commerce
# MAGIC
# MAGIC **Analysis Questions:**
# MAGIC 1. Which products have highest return rates?
# MAGIC 2. Which customers return frequently?
# MAGIC 3. What's the return rate by payment mode?
# MAGIC 4. How long between purchase and return?
# MAGIC
# MAGIC **Fact Table Design:** ReturnID, OrderID, ProductSK, CustomerSK, DateID, ReturnQuantity, RefundAmount, ReturnReason, DaysSincePurchase

# COMMAND ----------

# DBTITLE 1,Configuration - DevOps Ready (Environment-Based)
# ================================================================
# SKILLAIPPATH CONFIGURATION - DevOps Ready
# ================================================================

from datetime import datetime
from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring
from pyspark.sql.window import Window

class Config:
    ENVIRONMENT = "dev"
    CATALOG = "sap"
    SCHEMA_BRONZE = "bronze"
    SCHEMA_SILVER = "silver"
    
    TBL_STG_RETURNS = f"{CATALOG}.{SCHEMA_BRONZE}.stg_returns"
    TBL_DIM_PRODUCT = f"{CATALOG}.{SCHEMA_SILVER}.dim_product"
    TBL_DIM_CUSTOMER = f"{CATALOG}.{SCHEMA_SILVER}.dim_customer"
    TBL_DIM_WAREHOUSE = f"{CATALOG}.{SCHEMA_SILVER}.dim_warehouse"
    TBL_DIM_DATE = f"{CATALOG}.{SCHEMA_SILVER}.dim_date"
    TBL_FACT_RETURNS = f"{CATALOG}.{SCHEMA_SILVER}.fact_returns"
    COL_BATCH_DATE = "BatchDate"
    COL_LOADED_AT = "LoadedAt"
    SCD_IS_CURRENT_COL = "IsCurrent"

try:
    max_batch = spark.sql(f"SELECT MAX({Config.COL_BATCH_DATE}) FROM {Config.TBL_STG_RETURNS}").collect()[0][0]
    batch_date = str(max_batch) if max_batch else str(datetime.now().date())
except:
    batch_date = str(datetime.now().date())

print("="*70)
print("FACT_RETURNS - RETURN TRANSACTIONS")
print("="*70)
print(f"Environment: {Config.ENVIRONMENT.upper()}")
print(f"Catalog: {Config.CATALOG}")
print(f"Batch Date: {batch_date}")
print(f"Source: {Config.TBL_STG_RETURNS}")
print(f"Target: {Config.TBL_FACT_RETURNS}")
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

# DBTITLE 1,Surrogate Key Lookup and Fact Build
# # Extract Source Data
# if load_type == "full":
#     print(f"FULL LOAD: Processing all return records since {load_since}")
#     df_stg = spark.table(Config.TBL_STG_RETURNS) \
#         .filter(col(Config.COL_BATCH_DATE) >= load_since)
# else:
#     print(f"INCREMENTAL LOAD: Processing return records from batch {batch_date}")
#     df_stg = spark.table(Config.TBL_STG_RETURNS) \
#         .filter(col(Config.COL_BATCH_DATE) == batch_date)

# record_count = df_stg.count()
# print(f"SUCCESS: Read {record_count:,} return records")

# # Read dimensions
# df_product = spark.table(Config.TBL_DIM_PRODUCT)
# df_customer = spark.table(Config.TBL_DIM_CUSTOMER).filter(col(Config.SCD_IS_CURRENT_COL) == 1)
# df_warehouse = spark.table(Config.TBL_DIM_WAREHOUSE)
# df_date = spark.table(Config.TBL_DIM_DATE)

# print("SUCCESS: Loaded 4 dimensions (Product, Customer, Warehouse, Date)")

# # Build fact with surrogate keys
# df_fact = df_stg \
#     .join(df_product.select("ProductSK", "ProductName"), on="ProductName", how="left") \
#     .join(df_customer.select("CustomerSK", "CustomerName"), on="CustomerName", how="left") \
#     .join(df_warehouse.select("WarehouseSK", "WareHouseName").withColumnRenamed("WareHouseName", "WarehouseName"), on="WarehouseName", how="left") \
#     .join(df_date.select(col("DateID").alias("ReturnDateID"), "Date"), col("ReturnDate") == col("Date"), how="left") \
#     .select(
#         col("ReturnID").cast("bigint"),
#         col("OrderID").cast("bigint"),
#         coalesce(col("ProductSK"), lit(-1)).alias("ProductSK"),
#         coalesce(col("CustomerSK"), lit(-1)).alias("CustomerSK"),
#         coalesce(col("WarehouseSK"), lit(-1)).alias("WarehouseSK"),
#         coalesce(col("ReturnDateID"), lit(-1)).alias("ReturnDateID"),
#         col("ReturnQuantity").cast("int"),
#         col("RefundAmount").cast("decimal(10,2)"),
#         col("ReturnReason"),
#         col("DaysSincePurchase").cast("int"),
#         col(Config.COL_BATCH_DATE),
#         col(Config.COL_LOADED_AT)
#     )

# fact_count = df_fact.count()
# print(f"SUCCESS: Built FactReturns with {fact_count:,} rows")
# print("NOTE: coalesce(col, -1) handles orphans: Missing dimension keys default to -1 (Unknown)")

# COMMAND ----------

# DBTITLE 1,Write FactReturns
# # Write fact table based on load type
# load_type = dbutils.widgets.get("load_type")
# fact_count = df_fact.count()

# if load_type == "full":
#     # FULL LOAD: Truncate and reload all facts
#     print("FULL LOAD MODE: Truncating existing facts")
#     df_fact.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(Config.TBL_FACT_RETURNS)
#     print(f"\nFULL LOAD completed")
#     print(f"  Records loaded: {fact_count:,}")
#     print(f"  Mode: OVERWRITE (complete refresh)")
# else:
#     # INCREMENTAL LOAD: Append new batch
#     print("INCREMENTAL MODE: Appending new batch")
#     df_fact.write.mode("append").format("delta").saveAsTable(Config.TBL_FACT_RETURNS)
#     print(f"\nINCREMENTAL LOAD completed")
#     print(f"  Records loaded: {fact_count:,}")
#     print(f"  Mode: APPEND (additive)")

# print(f"\nTable: {Config.TBL_FACT_RETURNS}")
# print("NOTE: Facts are additive - each batch adds to history (unless full reload)")

# COMMAND ----------

# DBTITLE 1,Validation - Returns Summary
# %sql
# SELECT 
#     COUNT(*) AS return_count,
#     ROUND(SUM(RefundAmount) / 10000000, 2) AS total_refunds_crores,
#     ROUND(AVG(ReturnQuantity), 2) AS avg_return_qty,
#     SUM(CASE WHEN ProductSK = -1 THEN 1 ELSE 0 END) AS orphan_products,
#     SUM(CASE WHEN CustomerSK = -1 THEN 1 ELSE 0 END) AS orphan_customers,
#     SUM(CASE WHEN WarehouseSK = -1 THEN 1 ELSE 0 END) AS orphan_warehouses,
#     SUM(CASE WHEN ReturnDateID = -1 THEN 1 ELSE 0 END) AS orphan_dates
# FROM {Config.SCHEMA_SILVER}.fact_returns;

# COMMAND ----------

# DBTITLE 1,Extract Source Data
# # Extract Source Data
# if load_type == "full":
#     print(f"FULL LOAD: Processing all return records since {load_since}")
#     df_stg = spark.table(Config.TBL_STG_RETURNS) \
#         .filter(col(Config.COL_BATCH_DATE) >= load_since)
# else:
#     print(f"INCREMENTAL LOAD: Processing return records from batch {batch_date}")
#     df_stg = spark.table(Config.TBL_STG_RETURNS) \
#         .filter(col(Config.COL_BATCH_DATE) == batch_date)

# record_count = df_stg.count()
# print(f"SUCCESS: Read {record_count:,} return records")

# COMMAND ----------

# DBTITLE 1,Return Rate Analysis
# %sql
# -- Calculate return rate by product
# WITH sales AS (
#     SELECT ProductSK, SUM(Quantity) AS total_sold
#     FROM ${environment}.bm.fact_sales
#     GROUP BY ProductSK
# ),
# returns AS (
#     SELECT ProductSK, SUM(ReturnQuantity) AS total_returned
#     FROM ${environment}.bm.fact_returns
#     GROUP BY ProductSK
# )
# SELECT 
#     p.ProductName,
#     s.total_sold,
#     COALESCE(r.total_returned, 0) AS total_returned,
#     ROUND(COALESCE(r.total_returned, 0) * 100.0 / s.total_sold, 2) AS return_rate_pct
# FROM sales s
# LEFT JOIN returns r ON s.ProductSK = r.ProductSK
# JOIN ${environment}.bm.dim_product p ON s.ProductSK = p.ProductSK
# ORDER BY return_rate_pct DESC
# LIMIT 20;