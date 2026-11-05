# Databricks notebook source
# DBTITLE 1,Atomic Customer LTV Update (Conceptual)
# MAGIC %md
# MAGIC # Multi-Table Consistency with Transactions
# MAGIC
# MAGIC **Purpose:** Demonstrate atomic updates across multiple related tables
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## The Problem This Solves
# MAGIC
# MAGIC **Scenario:** Update customer lifetime value and tier across 3 tables:
# MAGIC * `customer_summary` - Main customer LTV
# MAGIC * `customer_tier` - Customer tier (Platinum/Gold/Silver/Bronze)
# MAGIC * `customer_update_log` - Audit trail
# MAGIC
# MAGIC **Without Transactions (❌ BAD):**
# MAGIC ```
# MAGIC Update customer_summary LTV     ✅ Success
# MAGIC Update customer_tier            ❌ FAILS (constraint violation)
# MAGIC Insert into audit log           (Never runs)
# MAGIC ```
# MAGIC **Result:** customer_summary has new LTV but tier is stale! Dashboards show incorrect tier for updated customers.
# MAGIC
# MAGIC **With Transactions (✅ GOOD on SQL Warehouse):**
# MAGIC ```sql
# MAGIC BEGIN TRANSACTION;
# MAGIC     Update all 3 tables...
# MAGIC     If ANY fails → ROLLBACK (nothing commits)
# MAGIC COMMIT;
# MAGIC ```
# MAGIC **Result:** All 3 tables stay consistent - LTV, tier, and audit log all update together or not at all.
# MAGIC
# MAGIC **⚠️ Note:** This notebook demonstrates the **concept** using separate statements. True atomic transactions with `BEGIN TRANSACTION...COMMIT` require **SQL Warehouse compute**. On notebook compute, each statement commits individually.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## How to Use This Notebook
# MAGIC
# MAGIC **This is a complete, runnable example:**
# MAGIC
# MAGIC 1. **Run Cell 2** → Creates 3 demo tables + seeds initial customer data
# MAGIC 2. **Run Cell 3** → Executes atomic transaction (updates LTV, tier, audit log)
# MAGIC 3. **Run Cell 4** → Verifies all 3 tables have consistent updates
# MAGIC
# MAGIC **Note:** These are demo/teaching tables, separate from production marts.
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Configuration - Load Portable Settings
# Load configuration for portable table references
%run ../../../1_Canvas/1_Setup/1_config

print(f"✓ Configuration loaded (portable)")
print(f"  Silver Schema: {Config.SCHEMA_SILVER}")
print(f"  Gold Schema: {Config.SCHEMA_GOLD}")
print(f"  Note: Using Python f-strings for table references")

# COMMAND ----------

# DBTITLE 1,Step 1: Create Tables and Seed Data
# Create the 3 target tables for this transaction example

# Customer summary table (main LTV storage)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {Config.CATALOG}.{Config.SCHEMA_GOLD}.customer_summary (
    CustomerSK INT,
    lifetime_value DECIMAL(20,2),
    last_updated_at TIMESTAMP
) USING DELTA
COMMENT 'Customer lifetime value summary'
""")

# Customer tier table (segmentation)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {Config.CATALOG}.{Config.SCHEMA_GOLD}.customer_tier (
    CustomerSK INT,
    tier STRING,
    tier_updated_at TIMESTAMP
) USING DELTA
COMMENT 'Customer tier classification (Platinum/Gold/Silver/Bronze)'
""")

# Customer update audit log
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {Config.CATALOG}.{Config.SCHEMA_GOLD}.customer_update_log (
    CustomerSK INT,
    lifetime_value DECIMAL(20,2),
    tier STRING,
    log_timestamp TIMESTAMP
) USING DELTA
COMMENT 'Audit trail for customer LTV/tier updates'
""")

# Seed initial data from Silver layer
spark.sql(f"""
INSERT OVERWRITE {Config.CATALOG}.{Config.SCHEMA_GOLD}.customer_summary
SELECT DISTINCT
    CustomerSK,
    0.00 AS lifetime_value,
    CURRENT_TIMESTAMP() AS last_updated_at
FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_sales
LIMIT 100
""")

spark.sql(f"""
INSERT OVERWRITE {Config.CATALOG}.{Config.SCHEMA_GOLD}.customer_tier
SELECT 
    CustomerSK,
    'Bronze' AS tier,
    CURRENT_TIMESTAMP() AS tier_updated_at
FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.customer_summary
""")

result = spark.sql(f"""
SELECT 'Tables created and seeded with ' || COUNT(*) || ' customers' AS status
FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.customer_summary
""")

display(result)

# COMMAND ----------

# DBTITLE 1,Step 2: Atomic Multi-Table Update (Transaction)
 
# Step 2: Update customer LTV and tier (3 separate statements)
# Note: True atomic transactions require SQL Warehouse compute
# On notebook compute, each statement commits individually

# Step 2a: Update main customer table with actual LTV from Silver
spark.sql(f"""
UPDATE {Config.CATALOG}.{Config.SCHEMA_GOLD}.customer_summary
SET
    lifetime_value = (
        SELECT COALESCE(SUM(finalprice), 0)
        FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_sales
        WHERE CustomerSK = customer_summary.CustomerSK
    ),
    last_updated_at = CURRENT_TIMESTAMP()
""")

# Step 2b: Update customer tier based on new LTV
spark.sql(f"""
MERGE INTO {Config.CATALOG}.{Config.SCHEMA_GOLD}.customer_tier ct
USING (
    SELECT 
        CustomerSK,
        CASE
            WHEN lifetime_value >= 100000 THEN 'Platinum'
            WHEN lifetime_value >= 50000 THEN 'Gold'
            WHEN lifetime_value >= 20000 THEN 'Silver'
            ELSE 'Bronze'
        END AS new_tier
    FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.customer_summary
) cs
ON ct.CustomerSK = cs.CustomerSK
WHEN MATCHED THEN UPDATE SET
    tier = cs.new_tier,
    tier_updated_at = CURRENT_TIMESTAMP()
""")

# Step 2c: Log the update to audit trail
spark.sql(f"""
INSERT INTO {Config.CATALOG}.{Config.SCHEMA_GOLD}.customer_update_log
SELECT
    cs.CustomerSK,
    cs.lifetime_value,
    ct.tier,
    CURRENT_TIMESTAMP() AS log_timestamp
FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.customer_summary cs
JOIN {Config.CATALOG}.{Config.SCHEMA_GOLD}.customer_tier ct ON cs.CustomerSK = ct.CustomerSK
""")

print("✓ Customer LTV and tier update completed")

# COMMAND ----------

# DBTITLE 1,Step 3: Verify Transaction Results
 
# Verify all 3 tables updated consistently

result = spark.sql(f"""
SELECT 
    cs.CustomerSK,
    cs.lifetime_value,
    cs.last_updated_at,
    ct.tier,
    ct.tier_updated_at,
    CASE 
        WHEN cl.log_timestamp IS NOT NULL THEN 'Logged'
        ELSE 'Missing'
    END AS audit_status
FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.customer_summary cs
JOIN {Config.CATALOG}.{Config.SCHEMA_GOLD}.customer_tier ct ON cs.CustomerSK = ct.CustomerSK
LEFT JOIN {Config.CATALOG}.{Config.SCHEMA_GOLD}.customer_update_log cl 
    ON cs.CustomerSK = cl.CustomerSK 
    AND cl.log_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 5 MINUTES
ORDER BY cs.lifetime_value DESC
LIMIT 20
""")

display(result)

# COMMAND ----------

# DBTITLE 1,Step 4: Customer Tier Distribution
 
# Show customer distribution by tier after update
# Note: audit_logged_count is a sanity check - in this demo it will always
# equal customer_count because Cell 3 inserts ALL customers in one batch.
# In production, this would catch partial failures (e.g., some customers logged, others not).

result = spark.sql(f"""
SELECT 
    ct.tier,
    COUNT(*) AS customer_count,
    ROUND(AVG(cs.lifetime_value), 2) AS avg_ltv,
    MIN(cs.lifetime_value) AS min_ltv,
    MAX(cs.lifetime_value) AS max_ltv,
    COUNT(DISTINCT cl.CustomerSK) AS audit_logged_count
FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.customer_tier ct
JOIN {Config.CATALOG}.{Config.SCHEMA_GOLD}.customer_summary cs ON ct.CustomerSK = cs.CustomerSK
LEFT JOIN {Config.CATALOG}.{Config.SCHEMA_GOLD}.customer_update_log cl 
    ON ct.CustomerSK = cl.CustomerSK
    AND cl.log_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
GROUP BY ct.tier
ORDER BY 
    CASE ct.tier
        WHEN 'Platinum' THEN 1
        WHEN 'Gold' THEN 2
        WHEN 'Silver' THEN 3
        WHEN 'Bronze' THEN 4
    END
""")

display(result)
