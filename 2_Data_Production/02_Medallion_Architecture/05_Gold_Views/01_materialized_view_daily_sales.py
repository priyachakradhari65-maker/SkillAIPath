# Databricks notebook source
# DBTITLE 1,Regular View: Product Sales Summary
# MAGIC %md
# MAGIC # Regular View: Product Sales Summary
# MAGIC
# MAGIC **Purpose:** On-demand product performance aggregations (computes on query)
# MAGIC
# MAGIC **Current Implementation:**
# MAGIC - Regular VIEW (not materialized - computes fresh data on every query)
# MAGIC - Sources from: `sap_dev.gold.mart_product_performance`
# MAGIC - Filters active products and orders by revenue
# MAGIC
# MAGIC **Why Regular View (not Materialized):**
# MAGIC - Materialized views require "Serverless Generic Compute Materialized View/Streaming Table" feature preview
# MAGIC - Not enabled in this workspace yet
# MAGIC - Regular views work immediately without feature enablement
# MAGIC
# MAGIC **To Upgrade to Materialized View:**
# MAGIC 1. Enable feature preview in Workspace Settings → Advanced → Feature Preview
# MAGIC 2. Change `CREATE OR REPLACE VIEW` to `CREATE OR REPLACE MATERIALIZED VIEW`
# MAGIC 3. Add `SCHEDULE EVERY 1 HOUR` clause for auto-refresh
# MAGIC
# MAGIC **Performance Note:** Regular views compute on every query - consider caching results or enabling materialized view for better performance on repeated queries

# COMMAND ----------

# DBTITLE 1,Drop Existing (if migrating from 05_GOLD)
# MAGIC %sql
# MAGIC -- Converted to regular VIEW (materialized views require feature preview on serverless)
# MAGIC DROP VIEW IF EXISTS sap_dev.gold.mv_daily_sales;

# COMMAND ----------

# DBTITLE 1,Create View - Daily Sales Summary
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW sap_dev.gold.mv_daily_sales
# MAGIC COMMENT 'Product sales summary - regular view (computes on query)'
# MAGIC AS
# MAGIC SELECT
# MAGIC     ProductCategory,
# MAGIC     ProductName,
# MAGIC     TotalOrders,
# MAGIC     TotalQuantitySold,
# MAGIC     TotalRevenue,
# MAGIC     AvgOrderValue,
# MAGIC     AvgDiscountPercent,
# MAGIC     UniqueCustomers,
# MAGIC     WarehousesStocked,
# MAGIC     FirstSaleDate,
# MAGIC     LastSaleDate,
# MAGIC     RevenueCrores,
# MAGIC     RevenueLakhs,
# MAGIC     DaysSinceLastSale,
# MAGIC     ProductStatus,
# MAGIC     PerformanceTier,
# MAGIC     RefreshedAt
# MAGIC FROM sap_dev.gold.mart_product_performance
# MAGIC WHERE ProductStatus IN ('Active', 'Slow Moving')
# MAGIC ORDER BY TotalRevenue DESC;

# COMMAND ----------

# DBTITLE 1,Verify View
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED sap_dev.gold.mv_daily_sales;

# COMMAND ----------

# DBTITLE 1,Check Refresh Status
# MAGIC %sql
# MAGIC -- Not applicable for regular views
# MAGIC -- Regular views don't have refresh history (they compute on query)
# MAGIC -- Uncomment below when upgraded to materialized view:
# MAGIC /*
# MAGIC SELECT
# MAGIC     refresh_start_time,
# MAGIC     refresh_end_time,
# MAGIC     ROUND((unix_timestamp(refresh_end_time) - unix_timestamp(refresh_start_time)) / 60, 2) AS refresh_duration_minutes,
# MAGIC     bytes_processed,
# MAGIC     rows_inserted,
# MAGIC     rows_updated
# MAGIC FROM system.materialized_views.refresh_history
# MAGIC WHERE table_catalog = 'sap_dev'
# MAGIC   AND table_schema = 'gold'
# MAGIC   AND table_name = 'mv_daily_sales'
# MAGIC ORDER BY refresh_start_time DESC
# MAGIC LIMIT 10;
# MAGIC */
# MAGIC SELECT 'Regular views do not have refresh history - data computes on every query' AS note;

# COMMAND ----------

# DBTITLE 1,Query Materialized View (Users Test)
# MAGIC %sql
# MAGIC SELECT
# MAGIC     ProductCategory,
# MAGIC     COUNT(*) AS product_count,
# MAGIC     SUM(TotalRevenue) AS category_revenue,
# MAGIC     SUM(TotalQuantitySold) AS total_quantity,
# MAGIC     ROUND(AVG(AvgOrderValue), 2) AS avg_order_value
# MAGIC FROM sap_dev.gold.mv_daily_sales
# MAGIC GROUP BY ProductCategory
# MAGIC ORDER BY category_revenue DESC;

# COMMAND ----------

# DBTITLE 1,Implementation Notes
# MAGIC %md
# MAGIC ## Implementation Notes
# MAGIC
# MAGIC **Current State: Regular View**
# MAGIC - Computes on every query (no storage cost, compute cost per query)
# MAGIC - Best for: Infrequent queries, always need fresh data
# MAGIC - Performance: Slower than materialized view (re-computes each time)
# MAGIC
# MAGIC **Future: Materialized View (After Feature Preview Enabled)**
# MAGIC - Storage: Incremental refresh (only changed rows = ~10 GB/day)
# MAGIC - Compute: 3 min refresh × 24 refreshes/day × 30 days = ~40 DBU/month
# MAGIC - Orchestration: None (auto-refresh)
# MAGIC - Best for: Frequent queries, predictable refresh schedule
# MAGIC - **Estimated savings: 70% vs physical table rewrites**
# MAGIC
# MAGIC **To Enable Materialized Views:**
# MAGIC 1. Go to Workspace Settings → Advanced → Feature Preview
# MAGIC 2. Enable "Serverless Generic Compute Materialized View/Streaming Table"
# MAGIC 3. Update Cell 3: Add `SCHEDULE EVERY 1 HOUR` after view name
# MAGIC 4. Re-run Cell 3 to create materialized view

# COMMAND ----------

# DBTITLE 1,Manual Refresh (if needed before next auto-refresh)
# MAGIC %sql
# MAGIC -- Not applicable for regular views (views compute on query, no refresh needed)
# MAGIC -- REFRESH MATERIALIZED VIEW sap_dev.gold.mv_daily_sales;

# COMMAND ----------

# DBTITLE 1,Debug - Check underlying table
# MAGIC %sql
# MAGIC -- Check what's in the source table
# MAGIC SELECT 
# MAGIC     COUNT(*) as total_rows,
# MAGIC     ProductStatus,
# MAGIC     COUNT(*) as count_per_status
# MAGIC FROM sap_dev.gold.mart_product_performance
# MAGIC GROUP BY ProductStatus;

# COMMAND ----------

# DBTITLE 1,Explain the ProductStatus Issue
# MAGIC %sql
# MAGIC -- Why all products are 'Slow Moving' instead of 'Active':
# MAGIC -- The mart calculates ProductStatus based on DaysSinceLastSale:
# MAGIC --   Active: <= 7 days since last sale
# MAGIC --   Slow Moving: 8-30 days since last sale  
# MAGIC --   Inactive: > 30 days since last sale
# MAGIC
# MAGIC -- Check the actual dates in the data:
# MAGIC SELECT 
# MAGIC     MIN(LastSaleDate) as earliest_sale,
# MAGIC     MAX(LastSaleDate) as latest_sale,
# MAGIC     MIN(DaysSinceLastSale) as min_days_since_sale,
# MAGIC     MAX(DaysSinceLastSale) as max_days_since_sale,
# MAGIC     CURRENT_DATE() as today
# MAGIC FROM sap_dev.gold.mart_product_performance;
