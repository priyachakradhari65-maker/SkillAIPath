# Databricks notebook source
# DBTITLE 1,Regular View: Customer 360
# MAGIC %md
# MAGIC # Regular View: Customer 360
# MAGIC
# MAGIC **Purpose:** On-demand customer analytics with engagement status (computes on query)
# MAGIC
# MAGIC **Current Implementation:** Regular VIEW sourcing from `sap.dev_gold.mart_customer_360`
# MAGIC
# MAGIC **Why Regular View:** Materialized views require feature preview not enabled
# MAGIC
# MAGIC **To Upgrade:** Enable feature preview, change to `CREATE MATERIALIZED VIEW` with `SCHEDULE EVERY 4 HOURS`

# COMMAND ----------

# DBTITLE 1,Create Regular View - Customer 360
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW sap.dev_gold.mv_customer_360
# MAGIC COMMENT 'Customer 360 analytics with engagement status - regular view (computes on query)'
# MAGIC AS
# MAGIC SELECT
# MAGIC     CustomerSK,
# MAGIC     CustomerID,
# MAGIC     CustomerName,
# MAGIC     CustomerEmail,
# MAGIC     CustomerAge,
# MAGIC     CustomerGender,
# MAGIC     CustomerCity,
# MAGIC     total_orders,
# MAGIC     lifetime_value,
# MAGIC     avg_order_value,
# MAGIC     first_purchase_date,
# MAGIC     last_purchase_date,
# MAGIC     days_since_last_purchase,
# MAGIC     unique_products_purchased,
# MAGIC     preferred_payment_mode,
# MAGIC     customer_segment,
# MAGIC     
# MAGIC     -- Add customer status based on recency
# MAGIC     CASE
# MAGIC         WHEN days_since_last_purchase <= 30 THEN 'Active'
# MAGIC         WHEN days_since_last_purchase <= 90 THEN 'At Risk'
# MAGIC         ELSE 'Churned'
# MAGIC     END AS customer_status,
# MAGIC     
# MAGIC     refreshed_at
# MAGIC FROM sap.dev_gold.mart_customer_360
# MAGIC WHERE total_orders > 0;

# COMMAND ----------

# DBTITLE 1,Query Customer Status Distribution
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     customer_status, 
# MAGIC     COUNT(*) AS customer_count,
# MAGIC     ROUND(AVG(lifetime_value), 2) AS avg_ltv,
# MAGIC     ROUND(AVG(total_orders), 1) AS avg_orders,
# MAGIC     ROUND(AVG(days_since_last_purchase), 0) AS avg_days_since_purchase
# MAGIC FROM sap.dev_gold.mv_customer_360
# MAGIC GROUP BY customer_status
# MAGIC ORDER BY 
# MAGIC     CASE customer_status
# MAGIC         WHEN 'Active' THEN 1
# MAGIC         WHEN 'At Risk' THEN 2
# MAGIC         WHEN 'Churned' THEN 3
# MAGIC     END;
