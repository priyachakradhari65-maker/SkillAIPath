# Databricks notebook source
# DBTITLE 1,Product Performance Mart - Business Intelligence Layer
# MAGIC %md
# MAGIC # Product Performance Mart - Gold Layer
# MAGIC **SkillAIPath Data Warehouse**
# MAGIC
# MAGIC ## Purpose
# MAGIC Pre-aggregated product sales metrics for merchandising dashboards and executive reporting.
# MAGIC
# MAGIC ## Business Questions Answered
# MAGIC * Which products drive most revenue?
# MAGIC * What's the sales trend by product category?
# MAGIC * Which products have highest profit margins?
# MAGIC * What's the product mix by warehouse?
# MAGIC
# MAGIC ## Data Sources
# MAGIC * **Silver Layer:** fact_sales, dim_product, dim_warehouse, dim_date
# MAGIC * **Target:** {Config.SCHEMA_GOLD}.mart_product_performance
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

from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring, round, countDistinct, datediff, desc, current_date, current_timestamp
from pyspark.sql.window import Window
from datetime import datetime

print("="*70)
print("MART_PRODUCT_PERFORMANCE - MERCHANDISING ANALYTICS")
print("="*70)
print(f"Environment: {Config.ENVIRONMENT}")
print(f"Source: {Config.TBL_FACT_SALES}")
print(f"Target: {Config.TBL_MART_PRODUCT}")
print(f"Refresh: FULL (complete overwrite)")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Extract & Transform - Build Product Metrics
# ================================================================
# PRODUCT PERFORMANCE METRICS CALCULATION
# ================================================================

print("Loading dimension and fact tables...")

# Load source tables
df_fact_sales = spark.table(Config.TBL_FACT_SALES)
df_product = spark.table(Config.TBL_DIM_PRODUCT)
df_category = spark.table(Config.TBL_DIM_CATEGORY)
df_warehouse = spark.table(Config.TBL_DIM_WAREHOUSE)
df_date = spark.table(Config.TBL_DIM_DATE)

print(f"  Fact Sales: {df_fact_sales.count():,} records")
print(f"  Products: {df_product.count():,}")
print(f"  Categories: {df_category.count():,}")
print(f"  Warehouses: {df_warehouse.count():,}")

# Join fact with dimensions
# Note: Category comes from dim_product_category via ProductCategorySK in fact_sales
df_sales_enriched = df_fact_sales \
    .join(df_product, df_fact_sales.ProductSK == df_product.ProductSK, "inner") \
    .join(df_category, df_fact_sales.ProductCategorySK == df_category.ProductCategorySK, "inner") \
    .join(df_warehouse, df_fact_sales.WarehouseSK == df_warehouse.WareHouseSK, "inner") \
    .join(df_date, df_fact_sales.OrderDateID == df_date.DateID, "inner")

print("\nAggregating product performance metrics...")

# Aggregate sales by product
df_product_performance = df_sales_enriched.groupBy(
    df_product["ProductSK"],
    df_product["ProductName"],
    df_category["ProductCategory"]
).agg(
    count("*").alias("TotalOrders"),
    sum("ProductQuantity").alias("TotalQuantitySold"),
    sum("finalprice").alias("TotalRevenue"),
    round(avg("finalprice"), 2).alias("AvgOrderValue"),
    round(avg("discount"), 2).alias("AvgDiscountPercent"),
    countDistinct(df_fact_sales["CustomerSK"]).alias("UniqueCustomers"),
    countDistinct(df_fact_sales["WarehouseSK"]).alias("WarehousesStocked"),
    min("Date").alias("FirstSaleDate"),
    max("Date").alias("LastSaleDate")
)

# Calculate derived metrics
df_product_performance = df_product_performance.withColumn(
    "RevenueCrores",
    round(col("TotalRevenue") / 10000000, 2)
).withColumn(
    "RevenueLakhs",
    round(col("TotalRevenue") / 100000, 2)
).withColumn(
    "DaysSinceLastSale",
    datediff(current_date(), col("LastSaleDate"))
).withColumn(
    "ProductStatus",
    when(col("DaysSinceLastSale") <= 7, "Active")
    .when(col("DaysSinceLastSale") <= 30, "Slow Moving")
    .otherwise("Inactive")
).withColumn(
    "PerformanceTier",
    when(col("TotalRevenue") > 10000000, "A - Top Performer")
    .when(col("TotalRevenue") > 5000000, "B - High Performer")
    .when(col("TotalRevenue") > 1000000, "C - Medium Performer")
    .otherwise("D - Low Performer")
).withColumn(
    "RefreshedAt",
    current_timestamp()
)

record_count = df_product_performance.count()
print(f"SUCCESS: Built performance metrics for {record_count:,} products")

# Show sample
print("\nTop 10 products by revenue:")
display(df_product_performance.orderBy(desc("TotalRevenue")).limit(10))

# COMMAND ----------

# DBTITLE 1,Write Gold Mart - Full Refresh
# ================================================================
# WRITE TO GOLD LAYER
# ================================================================

print("Writing to gold mart...")

df_product_performance.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable(Config.TBL_MART_PRODUCT)

final_count = spark.table(Config.TBL_MART_PRODUCT).count()
total_revenue_cr = spark.table(Config.TBL_MART_PRODUCT).agg(sum("RevenueCrores")).collect()[0][0]

print("="*70)
print("PRODUCT PERFORMANCE MART - LOAD COMPLETE")
print("="*70)
print(f"Target Table: {Config.TBL_MART_PRODUCT}")
print(f"Products: {final_count:,}")
print(f"Total Revenue: ₹{total_revenue_cr:.2f} Cr")
print(f"Load Type: FULL REFRESH (overwrite)")
print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Validation - Product Performance Summary
# Product Performance Validation Query
df_validation = spark.sql(f"""
SELECT
  COUNT(*) AS total_products,
  COUNT(DISTINCT ProductCategory) AS unique_categories,
  ROUND(SUM(RevenueCrores), 2) AS total_revenue_crores,
  SUM(TotalOrders) AS total_orders,
  SUM(TotalQuantitySold) AS total_quantity_sold,

  -- Performance Tier Distribution
  SUM(CASE WHEN PerformanceTier = 'A - Top Performer' THEN 1 ELSE 0 END) AS tier_a_count,
  SUM(CASE WHEN PerformanceTier = 'B - High Performer' THEN 1 ELSE 0 END) AS tier_b_count,
  SUM(CASE WHEN PerformanceTier = 'C - Medium Performer' THEN 1 ELSE 0 END) AS tier_c_count,
  SUM(CASE WHEN PerformanceTier = 'D - Low Performer' THEN 1 ELSE 0 END) AS tier_d_count,

  -- Status Distribution
  SUM(CASE WHEN ProductStatus = 'Active' THEN 1 ELSE 0 END) AS active_products,
  SUM(CASE WHEN ProductStatus = 'Slow Moving' THEN 1 ELSE 0 END) AS slow_moving_products,
  SUM(CASE WHEN ProductStatus = 'Inactive' THEN 1 ELSE 0 END) AS inactive_products,

  -- Metrics
  ROUND(AVG(AvgOrderValue), 2) AS avg_order_value,
  ROUND(AVG(AvgDiscountPercent), 2) AS avg_discount_percent

FROM {Config.TBL_MART_PRODUCT}
""")
display(df_validation)

# COMMAND ----------

# DBTITLE 1,Business Insights - Top Revenue Drivers
# Top 20 Products by Revenue (80/20 Rule)
df_top_revenue = spark.sql(f"""
WITH product_revenue AS (
  SELECT
    ProductName,
    ProductCategory,
    RevenueCrores,
    TotalOrders,
    UniqueCustomers,
    PerformanceTier,
    ROUND(AvgOrderValue / 1000, 2) AS avg_order_value_thousands,
    AvgDiscountPercent,
    SUM(RevenueCrores) OVER () AS total_revenue
  FROM {Config.TBL_MART_PRODUCT}
),
ranked_products AS (
  SELECT
    *,
    ROUND((RevenueCrores / total_revenue) * 100, 2) AS revenue_contribution_pct,
    ROUND(SUM(RevenueCrores) OVER (ORDER BY RevenueCrores DESC) / total_revenue * 100, 2) AS cumulative_pct
  FROM product_revenue
)
SELECT
  ProductName,
  ProductCategory,
  RevenueCrores,
  revenue_contribution_pct,
  cumulative_pct,
  TotalOrders,
  UniqueCustomers,
  avg_order_value_thousands,
  AvgDiscountPercent
FROM ranked_products
ORDER BY RevenueCrores DESC
LIMIT 20
""")
display(df_top_revenue)

# COMMAND ----------

# DBTITLE 1,Business Insights - Category Performance
# Category-Level Performance Analysis
df_category_perf = spark.sql(f"""
SELECT
  ProductCategory,
  COUNT(DISTINCT ProductName) AS product_count,
  SUM(TotalOrders) AS total_orders,
  SUM(TotalQuantitySold) AS total_quantity,
  ROUND(SUM(RevenueCrores), 2) AS category_revenue_cr,
  ROUND(AVG(AvgOrderValue), 2) AS avg_order_value,
  ROUND(AVG(AvgDiscountPercent), 2) AS avg_discount_pct,
  SUM(UniqueCustomers) AS total_customers,

  -- Performance indicators
  ROUND(SUM(RevenueCrores) * 100.0 / SUM(SUM(RevenueCrores)) OVER (), 2) AS revenue_share_pct,

  CASE
    WHEN SUM(RevenueCrores) > 50 THEN 'Strategic - High Revenue'
    WHEN SUM(RevenueCrores) > 20 THEN 'Core - Medium Revenue'
    WHEN SUM(RevenueCrores) > 5 THEN 'Supporting - Low Revenue'
    ELSE 'Niche - Very Low Revenue'
  END AS category_importance

FROM {Config.TBL_MART_PRODUCT}
GROUP BY ProductCategory
ORDER BY category_revenue_cr DESC
""")
print("\nUse Case: 'Which categories should get prime shelf space?'")
display(df_category_perf)

# COMMAND ----------

# DBTITLE 1,Business Insights - Products Needing Action
# Identify Products Needing Merchandising Action
df_action_items = spark.sql(f"""
SELECT
  ProductName,
  ProductCategory,
  ProductStatus,
  DaysSinceLastSale,
  ROUND(RevenueLakhs, 2) AS revenue_lakhs,
  TotalOrders,
  AvgDiscountPercent,

  CASE
    WHEN ProductStatus = 'Inactive' AND RevenueLakhs > 100 THEN 'URGENT: Restock high-value inactive product'
    WHEN ProductStatus = 'Slow Moving' AND AvgDiscountPercent < 10 THEN 'Action: Increase discount to 15-20%'
    WHEN ProductStatus = 'Active' AND AvgDiscountPercent > 25 THEN 'Warning: High discount eating margin'
    WHEN ProductStatus = 'Inactive' THEN 'Consider: Discontinue or heavy promotion'
    ELSE 'Monitor: Normal performance'
  END AS recommended_action

FROM {Config.TBL_MART_PRODUCT}
WHERE
  ProductStatus IN ('Inactive', 'Slow Moving')
  OR AvgDiscountPercent > 25
ORDER BY
  CASE
    WHEN ProductStatus = 'Inactive' AND RevenueLakhs > 100 THEN 1
    WHEN ProductStatus = 'Slow Moving' THEN 2
    WHEN AvgDiscountPercent > 25 THEN 3
    ELSE 4
  END,
  revenue_lakhs DESC
LIMIT 50
""")
print("\nUse Case: 'Weekly merchandising action list for category managers'")
display(df_action_items)

# Clean exit for orchestration
dbutils.notebook.exit("SUCCESS")
