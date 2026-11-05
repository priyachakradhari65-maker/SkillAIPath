# Databricks notebook source
# DBTITLE 1,Inventory Health Mart - Business Intelligence Layer
# MAGIC %md
# MAGIC # Inventory Health Mart - Gold Layer
# MAGIC **SkillAIPath Data Warehouse**
# MAGIC
# MAGIC ## Purpose
# MAGIC Pre-aggregated inventory metrics for warehouse management dashboards and executive reporting.
# MAGIC
# MAGIC ## Business Questions Answered
# MAGIC * Which products have overstock/understock risk?
# MAGIC * What's the inventory value by warehouse?
# MAGIC * Which products have poor turnover ratios?
# MAGIC * What's the stock aging distribution?
# MAGIC
# MAGIC ## Data Sources
# MAGIC * **Silver Layer:** fact_inventory, dim_product, dim_warehouse, dim_date
# MAGIC * **Target:** {Config.SCHEMA_GOLD}.mart_inventory_health
# MAGIC
# MAGIC ## Refresh Strategy
# MAGIC Full refresh daily (gold marts use complete overwrite)

# COMMAND ----------

# DBTITLE 1,Load Config
# MAGIC %run ../../../1_Canvas/1_Setup/1_config

# COMMAND ----------

# DBTITLE 1,Configuration - Unity Catalog Paths
# ================================================================
# SKILLAIPPATH CONFIGURATION - DevOps Ready
# ================================================================

from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring, datediff, current_date, current_timestamp, desc, round
from pyspark.sql.window import Window
from datetime import datetime

print("="*70)
print("MART_INVENTORY_HEALTH - WAREHOUSE ANALYTICS")
print("="*70)
print(f"Environment: {Config.ENVIRONMENT}")
print(f"Source: {Config.TBL_FACT_INVENTORY}")
print(f"Target: {Config.TBL_MART_INVENTORY}")
print(f"Refresh: FULL (complete overwrite)")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Extract & Transform - Build Inventory Metrics
# ================================================================
# INVENTORY HEALTH METRICS CALCULATION
# ================================================================

print("Loading dimension and fact tables...")

# Load source tables
df_fact_inventory = spark.table(Config.TBL_FACT_INVENTORY)
df_product = spark.table(Config.TBL_DIM_PRODUCT)
df_warehouse = spark.table(Config.TBL_DIM_WAREHOUSE)
df_date = spark.table(Config.TBL_DIM_DATE)

print(f"  Fact Inventory: {df_fact_inventory.count():,} records")
print(f"  Products: {df_product.count():,}")
print(f"  Warehouses: {df_warehouse.count():,}")
print(f"  Dates: {df_date.count():,}")

# Join fact with dimensions to get business context
df_inventory_enriched = df_fact_inventory \
    .join(df_product, df_fact_inventory.ProductSK == df_product.ProductSK, "inner") \
    .join(df_warehouse, df_fact_inventory.WarehouseSK == df_warehouse.WareHouseSK, "inner") \
    .join(df_date, df_fact_inventory.DateID == df_date.DateID, "inner") \
    .select(
        df_product["ProductSK"],
        df_product["ProductName"],
        df_warehouse["WareHouseSK"].alias("WarehouseSK"),
        df_warehouse["WareHouseName"].alias("WarehouseName"),
        df_warehouse["WareHouseCity"].alias("WarehouseLocation"),
        df_fact_inventory["StockQuantity"].alias("ProductQuantity"),
        df_fact_inventory["StockValue"],
        df_date["Date"].alias("LastCheckDate")
    )

print("\nBuilding inventory health metrics...")

# Calculate business metrics
df_inventory_health = df_inventory_enriched.withColumn(
    "DaysInStock",
    datediff(current_date(), col("LastCheckDate"))
).withColumn(
    "StockStatus",
    when(col("DaysInStock") > 60, "Overstock")
    .when(col("DaysInStock") < 7, "Understock")
    .otherwise("Healthy")
).withColumn(
    "TurnoverRatio",
    # Simulated turnover: higher stock value = slower turnover
    round(lit(100) / (col("StockValue") / lit(10000) + lit(1)), 2)
).withColumn(
    "UtilizationPercent",
    # Simulated utilization based on quantity ranges
    when(col("ProductQuantity") > 1000, lit(85.0))
    .when(col("ProductQuantity") > 500, lit(65.0))
    .when(col("ProductQuantity") > 100, lit(45.0))
    .otherwise(lit(25.0))
).withColumn(
    "RefreshedAt",
    current_timestamp()
)

record_count = df_inventory_health.count()
print(f"SUCCESS: Built inventory health metrics for {record_count:,} product-warehouse combinations")

# Show sample
print("\nSample inventory health records:")
display(df_inventory_health.orderBy(desc("StockValue")).limit(10))

# COMMAND ----------

# DBTITLE 1,Write Gold Mart - Full Refresh
# ================================================================
# WRITE TO GOLD LAYER
# ================================================================

print("Writing to gold mart...")

df_inventory_health.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable(Config.TBL_MART_INVENTORY)

final_count = spark.table(Config.TBL_MART_INVENTORY).count()

print("="*70)
print("INVENTORY HEALTH MART - LOAD COMPLETE")
print("="*70)
print(f"Target Table: {Config.TBL_MART_INVENTORY}")
print(f"Records Written: {final_count:,}")
print(f"Load Type: FULL REFRESH (overwrite)")
print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Validation - Inventory Health Summary
# Inventory Health Validation Query
df_validation = spark.sql(f"""
SELECT
  COUNT(*) AS total_records,
  COUNT(DISTINCT ProductSK) AS unique_products,
  COUNT(DISTINCT WarehouseSK) AS unique_warehouses,
  ROUND(SUM(StockValue) / 10000000, 2) AS total_stock_value_crores,

  -- Stock Status Distribution
  SUM(CASE WHEN StockStatus = 'Healthy' THEN 1 ELSE 0 END) AS healthy_count,
  SUM(CASE WHEN StockStatus = 'Overstock' THEN 1 ELSE 0 END) AS overstock_count,
  SUM(CASE WHEN StockStatus = 'Understock' THEN 1 ELSE 0 END) AS understock_count,

  -- Metrics
  ROUND(AVG(TurnoverRatio), 2) AS avg_turnover_ratio,
  ROUND(AVG(UtilizationPercent), 2) AS avg_utilization_percent,
  ROUND(AVG(DaysInStock), 1) AS avg_days_in_stock

FROM {Config.TBL_MART_INVENTORY}
""")
display(df_validation)

# COMMAND ----------

# DBTITLE 1,Business Insights - Warehouse Performance
# Warehouse-Level Inventory Performance
df_warehouse_perf = spark.sql(f"""
SELECT
  WarehouseName,
  WarehouseLocation,
  COUNT(*) AS product_count,
  ROUND(SUM(StockValue) / 10000000, 2) AS stock_value_cr,
  ROUND(AVG(TurnoverRatio), 2) AS avg_turnover,
  ROUND(AVG(UtilizationPercent), 2) AS utilization_pct,

  -- Risk flags
  SUM(CASE WHEN StockStatus = 'Overstock' THEN 1 ELSE 0 END) AS overstock_items,
  SUM(CASE WHEN StockStatus = 'Understock' THEN 1 ELSE 0 END) AS understock_items,

  CASE
    WHEN AVG(UtilizationPercent) >= 80 THEN 'Critical - Near Capacity'
    WHEN AVG(UtilizationPercent) >= 60 THEN 'Warning - High Utilization'
    ELSE 'Healthy'
  END AS warehouse_health

FROM {Config.TBL_MART_INVENTORY}
GROUP BY WarehouseName, WarehouseLocation
ORDER BY stock_value_cr DESC
""")
display(df_warehouse_perf)

# COMMAND ----------

# DBTITLE 1,Business Insights - Slow Moving Products
# Identify Slow-Moving & Dead Stock for Markdown/Discount
df_slow_moving = spark.sql(f"""
SELECT
  ProductName,
  WarehouseName,
  DaysInStock,
  ProductQuantity,
  ROUND(StockValue / 100000, 2) AS stock_value_lakhs,
  TurnoverRatio,
  StockStatus,

  CASE
    WHEN DaysInStock > 90 THEN 'URGENT: Discount 40-50% to clear'
    WHEN DaysInStock > 60 THEN 'Action: Markdown 20-30%'
    WHEN DaysInStock > 30 THEN 'Monitor: Consider promotion'
    ELSE 'Normal'
  END AS recommended_action

FROM {Config.TBL_MART_INVENTORY}
WHERE StockStatus IN ('Overstock')
ORDER BY DaysInStock DESC, stock_value_lakhs DESC
LIMIT 50
""")
display(df_slow_moving)
