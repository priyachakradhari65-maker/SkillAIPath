# Databricks notebook source
# DBTITLE 1,Regular View: Product Performance
# MAGIC %md
# MAGIC # Regular View: Product Performance
# MAGIC
# MAGIC **Purpose:** On-demand product performance analytics (computes on query)
# MAGIC
# MAGIC **Current Implementation:**
# MAGIC - Regular VIEW (not materialized - computes fresh data on every query)
# MAGIC - Sources from: `sap_dev.gold.mart_product_performance`
# MAGIC - Filters top performers by revenue
# MAGIC
# MAGIC **Why Regular View:** Materialized views require feature preview not enabled in this workspace
# MAGIC
# MAGIC **To Upgrade to Materialized View:** Enable feature preview in Workspace Settings → Advanced → Feature Preview, then change to `CREATE MATERIALIZED VIEW` with `SCHEDULE EVERY 2 HOURS`

# COMMAND ----------

# DBTITLE 1,Create Regular View - Product Performance
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW sap_dev.gold.mv_product_performance
# MAGIC COMMENT 'Product performance analytics - regular view (computes on query)'
# MAGIC AS
# MAGIC SELECT
# MAGIC     ProductSK,
# MAGIC     ProductName,
# MAGIC     ProductCategory,
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
# MAGIC WHERE ProductStatus = 'Active'
# MAGIC ORDER BY TotalRevenue DESC;

# COMMAND ----------

# DBTITLE 1,Query Top Products by Revenue
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     ProductName,
# MAGIC     ProductCategory,
# MAGIC     TotalOrders,
# MAGIC     TotalQuantitySold,
# MAGIC     TotalRevenue,
# MAGIC     RevenueCrores,
# MAGIC     AvgOrderValue,
# MAGIC     PerformanceTier
# MAGIC FROM sap_dev.gold.mv_product_performance
# MAGIC ORDER BY TotalRevenue DESC 
# MAGIC LIMIT 20;
