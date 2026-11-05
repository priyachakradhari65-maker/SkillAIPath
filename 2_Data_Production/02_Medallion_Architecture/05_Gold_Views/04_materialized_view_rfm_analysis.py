# Databricks notebook source
# DBTITLE 1,Regular View: RFM Analysis
# MAGIC %md
# MAGIC # Regular View: RFM Analysis
# MAGIC
# MAGIC **Purpose:** On-demand RFM customer segmentation (computes on query)
# MAGIC
# MAGIC **Current Implementation:** Regular VIEW sourcing from `sap_dev.gold.mart_rfm_analysis`
# MAGIC
# MAGIC **Why Regular View:** Materialized views require feature preview not enabled
# MAGIC
# MAGIC **To Upgrade:** Enable feature preview, change to `CREATE MATERIALIZED VIEW` with `SCHEDULE EVERY 6 HOURS`

# COMMAND ----------

# DBTITLE 1,Configuration - Load Portable Settings
# Load configuration for portable table references
%run ../../../1_Canvas/1_Setup/1_config

print(f"✓ Configuration loaded")
print(f"  Gold Schema: {Config.SCHEMA_GOLD}")
print(f"  Note: SQL cells below will use Config.SCHEMA_GOLD directly")

# COMMAND ----------

# DBTITLE 1,Create Regular View - RFM Analysis
# Using catalog/schema from Config for portability
spark.sql(f"""
CREATE OR REPLACE VIEW {Config.CATALOG}.{Config.SCHEMA_GOLD}.mv_rfm_analysis
COMMENT 'RFM customer segmentation - regular view (computes on query)'
AS
SELECT
    CustomerSK,
    CustomerName,
    Recency,
    Frequency,
    Monetary_Lakhs,
    R_Score,
    F_Score,
    M_Score,
    RFM_Score,
    Segment AS segment_label
FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_rfm_analysis
WHERE Segment IS NOT NULL
""")

print(f"✓ Created view: {Config.CATALOG}.{Config.SCHEMA_GOLD}.mv_rfm_analysis")

# COMMAND ----------

# DBTITLE 1,Query RFM Segment Distribution
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     segment_label, 
# MAGIC     COUNT(*) AS customer_count,
# MAGIC     ROUND(AVG(Monetary_Lakhs), 2) AS avg_spend_lakhs,
# MAGIC     ROUND(AVG(Frequency), 1) AS avg_frequency,
# MAGIC     ROUND(AVG(Recency), 1) AS avg_recency_days,
# MAGIC     ROUND(AVG(R_Score), 1) AS avg_r_score,
# MAGIC     ROUND(AVG(F_Score), 1) AS avg_f_score,
# MAGIC     ROUND(AVG(M_Score), 1) AS avg_m_score
# MAGIC FROM ${spark.conf.schema_gold}.mv_rfm_analysis
# MAGIC GROUP BY segment_label
# MAGIC ORDER BY customer_count DESC;
