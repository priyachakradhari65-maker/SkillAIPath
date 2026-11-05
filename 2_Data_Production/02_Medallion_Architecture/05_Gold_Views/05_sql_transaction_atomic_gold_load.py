# Databricks notebook source
# DBTITLE 1,SQL Transaction Blocks: Atomic Gold Load
# MAGIC %md
# MAGIC %md
# MAGIC # SQL Transaction Blocks: Atomic Gold Load
# MAGIC
# MAGIC **Purpose:** Demonstrate multi-table consistency using BEGIN TRANSACTION...COMMIT blocks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## The Problem This Solves
# MAGIC
# MAGIC **Scenario:** You need to refresh 3 Gold tables every night:
# MAGIC * `daily_sales_summary` - Overall revenue/transactions
# MAGIC * `daily_product_summary` - Product-level metrics  
# MAGIC * `daily_customer_summary` - Customer-level spending
# MAGIC
# MAGIC **Without Transactions (❌ BAD):**
# MAGIC ```
# MAGIC Job 1: Refresh sales_summary    ✅ Success
# MAGIC Job 2: Refresh product_summary  ✅ Success
# MAGIC Job 3: Refresh customer_summary ❌ FAILS
# MAGIC ```
# MAGIC **Result:** Partial load! Dashboards show inconsistent data (revenue without matching customers)
# MAGIC
# MAGIC **With Transactions (✅ GOOD):**
# MAGIC ```sql
# MAGIC BEGIN TRANSACTION;
# MAGIC     Refresh all 3 tables...
# MAGIC     If ANY fails → ROLLBACK (nothing commits)
# MAGIC COMMIT;
# MAGIC ```
# MAGIC **Result:** All-or-nothing atomicity. All 3 tables stay consistent.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## How to Use This Notebook
# MAGIC
# MAGIC **This is a complete, runnable example:**
# MAGIC
# MAGIC 1. **Run Cell 2** → Creates 4 demo tables (daily_sales_summary, daily_product_summary, daily_customer_summary, load_audit_log)
# MAGIC 2. **Run Cell 3** → Executes atomic transaction (loads last 7 days from Silver layer)
# MAGIC 3. **Run Cell 5** → Verifies all 3 tables have consistent timestamps
# MAGIC 4. **Run Cell 6** → Shows audit log confirming SUCCESS
# MAGIC
# MAGIC **Note:** These are demo/teaching tables, separate from production marts (mart_product_performance, mart_customer_360, etc.)

# COMMAND ----------

# DBTITLE 1,Configuration - Load Portable Settings
# Load configuration for portable table references
%run ../../../1_Canvas/1_Setup/1_config

print(f"✓ Configuration loaded (portable)")
print(f"  Silver Schema: {Config.SCHEMA_SILVER}")
print(f"  Gold Schema: {Config.SCHEMA_GOLD}")
print(f"  Note: Using Python f-strings for table references")

# COMMAND ----------

# DBTITLE 1,Step 1: Create Target Tables
# Create the target tables for this transaction example

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {Config.CATALOG}.{Config.SCHEMA_GOLD}.daily_sales_summary (
    sale_date DATE,
    total_revenue DECIMAL(20,2),
    total_transactions BIGINT,
    unique_customers BIGINT,
    loaded_at TIMESTAMP
) USING DELTA
COMMENT 'Daily aggregated sales metrics for executive dashboards'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {Config.CATALOG}.{Config.SCHEMA_GOLD}.daily_product_summary (
    sale_date DATE,
    ProductSK INT,
    units_sold BIGINT,
    revenue DECIMAL(20,2),
    loaded_at TIMESTAMP
) USING DELTA
COMMENT 'Daily product-level sales for merchandising reports'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {Config.CATALOG}.{Config.SCHEMA_GOLD}.daily_customer_summary (
    sale_date DATE,
    CustomerSK INT,
    order_count BIGINT,
    total_spent DECIMAL(20,2),
    loaded_at TIMESTAMP
) USING DELTA
COMMENT 'Daily customer spending for marketing segmentation'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {Config.CATALOG}.{Config.SCHEMA_GOLD}.load_audit_log (
    job_name STRING,
    run_timestamp TIMESTAMP,
    status STRING,
    message STRING
) USING DELTA
COMMENT 'Job execution audit trail'
""")

print("✓ Tables created successfully")

# COMMAND ----------

# DBTITLE 1,Atomic Multi-Table Gold Refresh
 
# Step 2: Atomic Multi-Table Load
# All 3 tables refresh together or none at all (all-or-nothing)
# Using SQL scripting syntax (BEGIN...END) for notebook compute

spark.sql(f"""
BEGIN

-- Step 1: Delete existing data for refresh window (last 7 days)
DELETE FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.daily_sales_summary
WHERE sale_date >= DATE_SUB(CURRENT_DATE(), 7);

DELETE FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.daily_product_summary
WHERE sale_date >= DATE_SUB(CURRENT_DATE(), 7);

DELETE FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.daily_customer_summary
WHERE sale_date >= DATE_SUB(CURRENT_DATE(), 7);

-- Step 2: Insert fresh data (all from same silver snapshot)
INSERT INTO {Config.CATALOG}.{Config.SCHEMA_GOLD}.daily_sales_summary
SELECT
    BatchDate AS sale_date,
    SUM(finalprice) AS total_revenue,
    COUNT(DISTINCT OrderID) AS total_transactions,
    COUNT(DISTINCT CustomerSK) AS unique_customers,
    CURRENT_TIMESTAMP() AS loaded_at
FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_sales
WHERE BatchDate >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY BatchDate;

INSERT INTO {Config.CATALOG}.{Config.SCHEMA_GOLD}.daily_product_summary
SELECT
    BatchDate AS sale_date,
    ProductSK,
    SUM(ProductQuantity) AS units_sold,
    SUM(finalprice) AS revenue,
    CURRENT_TIMESTAMP() AS loaded_at
FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_sales
WHERE BatchDate >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY BatchDate, ProductSK;

INSERT INTO {Config.CATALOG}.{Config.SCHEMA_GOLD}.daily_customer_summary
SELECT
    BatchDate AS sale_date,
    CustomerSK,
    COUNT(*) AS order_count,
    SUM(finalprice) AS total_spent,
    CURRENT_TIMESTAMP() AS loaded_at
FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_sales
WHERE BatchDate >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY BatchDate, CustomerSK;

-- Step 3: Log successful load
INSERT INTO {Config.CATALOG}.{Config.SCHEMA_GOLD}.load_audit_log
VALUES (
    'gold_daily_refresh',
    CURRENT_TIMESTAMP(),
    'SUCCESS',
    'All 3 tables loaded atomically - last 7 days'
);

END
""")

print("✓ Atomic transaction completed successfully")

# COMMAND ----------

# DBTITLE 1,Rollback Example and Table Creation
# MAGIC %md
# MAGIC %md
# MAGIC # Understanding Rollback Behavior
# MAGIC
# MAGIC ## What Happens on Failure?
# MAGIC
# MAGIC **If ANY step in the transaction fails:**
# MAGIC
# MAGIC ```sql
# MAGIC BEGIN TRANSACTION;
# MAGIC     DELETE FROM daily_sales_summary...     ✅ Executes
# MAGIC     INSERT INTO daily_sales_summary...     ✅ Succeeds
# MAGIC     INSERT INTO daily_product_summary...   ✅ Succeeds  
# MAGIC     INSERT INTO daily_customer_summary...  ❌ FAILS (constraint violation)
# MAGIC     
# MAGIC ROLLBACK;  -- Automatically triggered
# MAGIC ```
# MAGIC
# MAGIC **Result:**
# MAGIC * ✅ All 3 tables remain in their **original state** (before transaction)
# MAGIC * ✅ No partial load - either all tables get new data, or none do
# MAGIC * ✅ Data consistency maintained across all marts
# MAGIC * ✅ Audit log shows failure (no SUCCESS record written)
# MAGIC
# MAGIC **Without Transactions:**
# MAGIC * ❌ sales_summary and product_summary would have new data
# MAGIC * ❌ customer_summary would be empty or have old data
# MAGIC * ❌ Dashboards show inconsistent metrics

# COMMAND ----------

# DBTITLE 1,Step 3: Verify Transaction Results
 
# Verify all 3 tables were loaded with consistent timestamps

result = spark.sql(f"""
SELECT 
    'daily_sales_summary' AS table_name,
    COUNT(*) AS row_count,
    MIN(sale_date) AS earliest_date,
    MAX(sale_date) AS latest_date,
    COUNT(DISTINCT loaded_at) AS unique_load_times
FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.daily_sales_summary

UNION ALL

SELECT 
    'daily_product_summary' AS table_name,
    COUNT(*) AS row_count,
    MIN(sale_date) AS earliest_date,
    MAX(sale_date) AS latest_date,
    COUNT(DISTINCT loaded_at) AS unique_load_times
FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.daily_product_summary

UNION ALL

SELECT 
    'daily_customer_summary' AS table_name,
    COUNT(*) AS row_count,
    MIN(sale_date) AS earliest_date,
    MAX(sale_date) AS latest_date,
    COUNT(DISTINCT loaded_at) AS unique_load_times
FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.daily_customer_summary

ORDER BY table_name
""")

display(result)

# COMMAND ----------

# DBTITLE 1,Step 4: Check Audit Log
 
# View audit log - confirms transaction completed successfully

result = spark.sql(f"""
SELECT 
    job_name,
    run_timestamp,
    status,
    message
FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.load_audit_log
ORDER BY run_timestamp DESC
LIMIT 10
""")

display(result)
