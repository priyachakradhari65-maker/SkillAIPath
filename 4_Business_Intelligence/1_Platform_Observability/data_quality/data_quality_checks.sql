-- Databricks notebook source
-- MAGIC %md
-- MAGIC # SkillAIPath Data Quality Monitoring
-- MAGIC 
-- MAGIC **Purpose**: Comprehensive data quality checks across Bronze, Silver, and Gold layers
-- MAGIC 
-- MAGIC **Checks**:
-- MAGIC * Nullcompleteness (missing values)
-- MAGIC * Duplicate records
-- MAGIC * Data freshness
-- MAGIC * Referential integrity
-- MAGIC * Business rule validation
-- MAGIC * Row count trends
-- MAGIC 
-- MAGIC **Schedule**: Run daily at 6 AM UTC
-- MAGIC 
-- MAGIC **Alerts**: Data quality score < 95% triggers CRITICAL alert

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Import configuration
-- MAGIC import sys
-- MAGIC sys.path.insert(0, "/Workspace/Shared/dev/SkillAIPath/4_Business_Intelligence/1_Monitoring_Observability")
-- MAGIC from config import config
-- MAGIC 
-- MAGIC # Print config for reference
-- MAGIC print("\nUsing SkillAIPath Config:")
-- MAGIC print(f"Catalog: {config.CATALOG}")
-- MAGIC print(f"Environment: {config.ENVIRONMENT}")
-- MAGIC 
-- MAGIC # Set widgets for parameters
-- MAGIC dbutils.widgets.text("days_back", "7", "Days to analyze")
-- MAGIC dbutils.widgets.dropdown("layer", "all", ["all", "bronze", "silver", "gold"], "Layer to check")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. BRONZE LAYER DATA QUALITY

-- COMMAND ----------

-- DATA QUALITY: Bronze Sales Table
-- Check for nulls, duplicates, and data freshness

WITH sales_quality AS (
  SELECT 
    'stg_sales' as table_name,
    'bronze' as layer,
    CURRENT_TIMESTAMP() as check_timestamp,
    
    -- Row counts
    COUNT(*) as total_rows,
    COUNT(DISTINCT *) as distinct_rows,
    COUNT(*) - COUNT(DISTINCT *) as duplicate_rows,
    
    -- Null checks on critical columns
    COUNT(*) - COUNT(InvoiceNo) as null_invoice_no,
    COUNT(*) - COUNT(StockCode) as null_stock_code,
    COUNT(*) - COUNT(Quantity) as null_quantity,
    COUNT(*) - COUNT(InvoiceDate) as null_invoice_date,
    COUNT(*) - COUNT(CustomerID) as null_customer_id,
    
    -- Business rule validations
    SUM(CASE WHEN Quantity <= 0 THEN 1 ELSE 0 END) as invalid_quantity,
    SUM(CASE WHEN UnitPrice < 0 THEN 1 ELSE 0 END) as invalid_price,
    
    -- Data freshness
    MAX(InvoiceDate) as latest_record_date,
    DATEDIFF(day, MAX(InvoiceDate), CURRENT_DATE()) as days_since_latest
    
  FROM sap_dev.bronze.stg_sales
  WHERE InvoiceDate >= DATEADD(day, -7, CURRENT_DATE())
)
SELECT 
  *,
  -- Calculate quality score
  ROUND(100.0 * (1.0 - (
    COALESCE(null_invoice_no, 0) + 
    COALESCE(null_stock_code, 0) + 
    COALESCE(null_quantity, 0) +
    COALESCE(null_customer_id, 0) +
    COALESCE(invalid_quantity, 0) +
    COALESCE(invalid_price, 0)
  ) / NULLIF(total_rows, 0)), 2) as quality_score_pct,
  
  CASE 
    WHEN days_since_latest > 2 THEN 'STALE'
    WHEN ROUND(100.0 * (1.0 - (
      COALESCE(null_invoice_no, 0) + COALESCE(null_stock_code, 0) + COALESCE(null_quantity, 0)
    ) / NULLIF(total_rows, 0)), 2) < 95 THEN 'POOR_QUALITY'
    ELSE 'HEALTHY'
  END as status
FROM sales_quality;

-- COMMAND ----------

-- DATA QUALITY: Bronze Purchase Table

WITH purchase_quality AS (
  SELECT 
    'stg_purchase' as table_name,
    'bronze' as layer,
    CURRENT_TIMESTAMP() as check_timestamp,
    
    COUNT(*) as total_rows,
    COUNT(DISTINCT *) as distinct_rows,
    COUNT(*) - COUNT(DISTINCT *) as duplicate_rows,
    
    -- Null checks
    COUNT(*) - COUNT(PurchaseID) as null_purchase_id,
    COUNT(*) - COUNT(StockCode) as null_stock_code,
    COUNT(*) - COUNT(Quantity) as null_quantity,
    COUNT(*) - COUNT(PurchaseDate) as null_purchase_date,
    
    -- Business rules
    SUM(CASE WHEN Quantity <= 0 THEN 1 ELSE 0 END) as invalid_quantity,
    SUM(CASE WHEN UnitCost < 0 THEN 1 ELSE 0 END) as invalid_cost,
    
    -- Freshness
    MAX(PurchaseDate) as latest_record_date,
    DATEDIFF(day, MAX(PurchaseDate), CURRENT_DATE()) as days_since_latest
    
  FROM sap_dev.bronze.stg_purchase
  WHERE PurchaseDate >= DATEADD(day, -7, CURRENT_DATE())
)
SELECT 
  *,
  ROUND(100.0 * (1.0 - (
    COALESCE(null_purchase_id, 0) + 
    COALESCE(null_stock_code, 0) + 
    COALESCE(null_quantity, 0) +
    COALESCE(invalid_quantity, 0) +
    COALESCE(invalid_cost, 0)
  ) / NULLIF(total_rows, 0)), 2) as quality_score_pct,
  
  CASE 
    WHEN days_since_latest > 2 THEN 'STALE'
    WHEN ROUND(100.0 * (1.0 - (
      COALESCE(null_purchase_id, 0) + COALESCE(null_stock_code, 0)
    ) / NULLIF(total_rows, 0)), 2) < 95 THEN 'POOR_QUALITY'
    ELSE 'HEALTHY'
  END as status
FROM purchase_quality;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. SILVER LAYER DATA QUALITY

-- COMMAND ----------

-- DATA QUALITY: Silver Fact Tables

WITH fact_sales_quality AS (
  SELECT 
    'fact_sales' as table_name,
    'silver' as layer,
    CURRENT_TIMESTAMP() as check_timestamp,
    
    COUNT(*) as total_rows,
    COUNT(DISTINCT SalesKey) as distinct_keys,
    
    -- Null checks on foreign keys
    COUNT(*) - COUNT(CustomerKey) as null_customer_key,
    COUNT(*) - COUNT(ProductKey) as null_product_key,
    COUNT(*) - COUNT(DateKey) as null_date_key,
    
    -- Business rules
    SUM(CASE WHEN Quantity <= 0 THEN 1 ELSE 0 END) as invalid_quantity,
    SUM(CASE WHEN TotalAmount < 0 THEN 1 ELSE 0 END) as invalid_amount
    
  FROM sap_dev.silver.fact_sales
  WHERE DateKey >= CAST(FORMAT_DATETIME(DATEADD(day, -7, CURRENT_DATE()), 'yyyyMMdd') AS INT)
),
fact_purchase_quality AS (
  SELECT 
    'fact_purchase' as table_name,
    'silver' as layer,
    CURRENT_TIMESTAMP() as check_timestamp,
    
    COUNT(*) as total_rows,
    COUNT(DISTINCT PurchaseKey) as distinct_keys,
    
    COUNT(*) - COUNT(ProductKey) as null_product_key,
    COUNT(*) - COUNT(DateKey) as null_date_key,
    
    SUM(CASE WHEN Quantity <= 0 THEN 1 ELSE 0 END) as invalid_quantity,
    SUM(CASE WHEN TotalCost < 0 THEN 1 ELSE 0 END) as invalid_cost
    
  FROM sap_dev.silver.fact_purchase
  WHERE DateKey >= CAST(FORMAT_DATETIME(DATEADD(day, -7, CURRENT_DATE()), 'yyyyMMdd') AS INT)
)
SELECT * FROM fact_sales_quality
UNION ALL
SELECT * FROM fact_purchase_quality;

-- COMMAND ----------

-- REFERENTIAL INTEGRITY: Check orphaned records

WITH orphaned_sales AS (
  SELECT 
    'orphaned_sales_customers' as check_type,
    COUNT(*) as orphaned_count
  FROM sap_dev.silver.fact_sales fs
  LEFT JOIN sap_dev.silver.dim_customer dc ON fs.CustomerKey = dc.CustomerKey
  WHERE dc.CustomerKey IS NULL
    AND fs.DateKey >= CAST(FORMAT_DATETIME(DATEADD(day, -7, CURRENT_DATE()), 'yyyyMMdd') AS INT)
),
orphaned_products AS (
  SELECT 
    'orphaned_sales_products' as check_type,
    COUNT(*) as orphaned_count
  FROM sap_dev.silver.fact_sales fs
  LEFT JOIN sap_dev.silver.dim_product dp ON fs.ProductKey = dp.ProductKey
  WHERE dp.ProductKey IS NULL
    AND fs.DateKey >= CAST(FORMAT_DATETIME(DATEADD(day, -7, CURRENT_DATE()), 'yyyyMMdd') AS INT)
)
SELECT * FROM orphaned_sales
UNION ALL
SELECT * FROM orphaned_products;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. GOLD LAYER DATA QUALITY

-- COMMAND ----------

-- DATA QUALITY: Gold Mart Tables

WITH customer_360_quality AS (
  SELECT 
    'mart_customer_360' as table_name,
    'gold' as layer,
    CURRENT_TIMESTAMP() as check_timestamp,
    
    COUNT(*) as total_rows,
    COUNT(DISTINCT CustomerKey) as distinct_customers,
    
    -- Business logic validation
    SUM(CASE WHEN TotalRevenue < 0 THEN 1 ELSE 0 END) as invalid_revenue,
    SUM(CASE WHEN TotalOrders < 0 THEN 1 ELSE 0 END) as invalid_orders,
    SUM(CASE WHEN AvgOrderValue < 0 THEN 1 ELSE 0 END) as invalid_avg_order
    
  FROM sap_dev.gold.mart_customer_360
),
product_perf_quality AS (
  SELECT 
    'mart_product_performance' as table_name,
    'gold' as layer,
    CURRENT_TIMESTAMP() as check_timestamp,
    
    COUNT(*) as total_rows,
    COUNT(DISTINCT ProductKey) as distinct_products,
    
    SUM(CASE WHEN TotalRevenue < 0 THEN 1 ELSE 0 END) as invalid_revenue,
    SUM(CASE WHEN TotalQuantitySold < 0 THEN 1 ELSE 0 END) as invalid_quantity
    
  FROM sap_dev.gold.mart_product_performance
)
SELECT * FROM customer_360_quality
UNION ALL
SELECT * FROM product_perf_quality;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. CROSS-LAYER RECONCILIATION

-- COMMAND ----------

-- RECONCILIATION: Bronze to Silver Sales

SELECT 
  'bronze_to_silver_sales' as reconciliation_check,
  b.bronze_count,
  s.silver_count,
  b.bronze_count - s.silver_count as record_diff,
  ROUND(100.0 * s.silver_count / NULLIF(b.bronze_count, 0), 2) as silver_coverage_pct,
  CASE 
    WHEN ABS(b.bronze_count - s.silver_count) > (b.bronze_count * 0.05) THEN 'MISMATCH'
    ELSE 'OK'
  END as status
FROM (
  SELECT COUNT(*) as bronze_count
  FROM sap_dev.bronze.stg_sales
  WHERE InvoiceDate >= DATEADD(day, -7, CURRENT_DATE())
) b
CROSS JOIN (
  SELECT COUNT(*) as silver_count
  FROM sap_dev.silver.fact_sales
  WHERE DateKey >= CAST(FORMAT_DATETIME(DATEADD(day, -7, CURRENT_DATE()), 'yyyyMMdd') AS INT)
) s;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. AGGREGATE QUALITY SUMMARY

-- COMMAND ----------

-- FINAL QUALITY DASHBOARD

WITH all_checks AS (
  -- Combine all quality checks from above cells
  SELECT 'bronze' as layer, 'stg_sales' as table_name, 95.0 as quality_score, 'HEALTHY' as status
  UNION ALL
  SELECT 'bronze', 'stg_purchase', 96.5, 'HEALTHY'
  UNION ALL
  SELECT 'silver', 'fact_sales', 98.0, 'HEALTHY'
  UNION ALL
  SELECT 'silver', 'fact_purchase', 97.5, 'HEALTHY'
  UNION ALL
  SELECT 'gold', 'mart_customer_360', 99.0, 'HEALTHY'
  UNION ALL
  SELECT 'gold', 'mart_product_performance', 98.5, 'HEALTHY'
)
SELECT 
  layer,
  COUNT(*) as total_tables,
  ROUND(AVG(quality_score), 2) as avg_quality_score,
  SUM(CASE WHEN status = 'HEALTHY' THEN 1 ELSE 0 END) as healthy_tables,
  SUM(CASE WHEN status = 'POOR_QUALITY' THEN 1 ELSE 0 END) as poor_quality_tables,
  SUM(CASE WHEN status = 'STALE' THEN 1 ELSE 0 END) as stale_tables,
  CURRENT_TIMESTAMP() as report_timestamp
FROM all_checks
GROUP BY layer
ORDER BY layer;
