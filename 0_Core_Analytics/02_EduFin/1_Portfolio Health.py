# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Portfolio Health Analysis
# MAGIC %md
# MAGIC # Portfolio Health Analysis
# MAGIC
# MAGIC **Dataset:** `sap_dev.edufin_regional.loans`

# COMMAND ----------

# DBTITLE 1,Data Check
# MAGIC %sql
# MAGIC -- Data validation check
# MAGIC SELECT 
# MAGIC   COUNT(*) AS total_loans,
# MAGIC   COUNT(DISTINCT loan_status) AS status_types,
# MAGIC   COUNT(DISTINCT customer_id) AS unique_customers
# MAGIC FROM sap_dev.edufin_regional.loans;

# COMMAND ----------

# DBTITLE 1,Portfolio Status Distribution
# MAGIC %sql
# MAGIC -- Portfolio Status Distribution
# MAGIC
# MAGIC SELECT 
# MAGIC   loan_status,
# MAGIC   COUNT(*) AS loan_count,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
# MAGIC FROM sap_dev.edufin_regional.loans
# MAGIC GROUP BY loan_status
# MAGIC ORDER BY loan_count DESC;
# MAGIC

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Portfolio Scale Metrics
# MAGIC %sql
# MAGIC -- Portfolio Scale Metrics
# MAGIC
# MAGIC SELECT 
# MAGIC   COUNT(DISTINCT customer_id) AS total_customers,
# MAGIC   COUNT(*) AS total_loans,
# MAGIC   ROUND(SUM(loan_amount) / 10000000, 2) AS portfolio_value_cr,
# MAGIC   ROUND(AVG(loan_amount) / 100000, 2) AS avg_loan_size_lakh
# MAGIC FROM sap_dev.edufin_regional.loans;
# MAGIC

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Risk Concentration
# MAGIC %sql
# MAGIC -- Risk Concentration by Loan Size
# MAGIC
# MAGIC SELECT 
# MAGIC   CASE 
# MAGIC     WHEN loan_amount < 300000 THEN 'Small (<₹3L)'
# MAGIC     WHEN loan_amount >= 300000 AND loan_amount <= 600000 THEN 'Medium (₹3L-₹6L)'
# MAGIC     WHEN loan_amount > 600000 THEN 'Large (>₹6L)'
# MAGIC   END AS risk_band,
# MAGIC   COUNT(*) AS total_loans,
# MAGIC   SUM(CASE WHEN loan_status = 'Defaulted' THEN 1 ELSE 0 END) AS defaulted_count,
# MAGIC   SUM(CASE WHEN loan_status = 'Overdue' THEN 1 ELSE 0 END) AS overdue_count,
# MAGIC   SUM(CASE WHEN loan_status = 'Active' THEN 1 ELSE 0 END) AS active_count
# MAGIC FROM sap_dev.edufin_regional.loans
# MAGIC GROUP BY risk_band
# MAGIC ORDER BY 
# MAGIC   CASE 
# MAGIC     WHEN risk_band = 'Small (<₹3L)' THEN 1
# MAGIC     WHEN risk_band = 'Medium (₹3L-₹6L)' THEN 2
# MAGIC     WHEN risk_band = 'Large (>₹6L)' THEN 3
# MAGIC   END;

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Financial Impact
# MAGIC %sql
# MAGIC -- Financial Impact by Status
# MAGIC
# MAGIC SELECT 
# MAGIC   loan_status,
# MAGIC   ROUND(SUM(loan_amount) / 10000000, 2) AS total_value_cr
# MAGIC FROM sap_dev.edufin_regional.loans
# MAGIC GROUP BY loan_status
# MAGIC ORDER BY total_value_cr DESC;
# MAGIC

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Executive Dashboard
# MAGIC %sql
# MAGIC -- Executive Dashboard
# MAGIC
# MAGIC WITH portfolio_metrics AS (
# MAGIC   SELECT 
# MAGIC     COUNT(*) AS total_loans,
# MAGIC     SUM(CASE WHEN loan_status = 'Active' THEN 1 ELSE 0 END) AS active_count,
# MAGIC     SUM(CASE WHEN loan_status = 'Defaulted' THEN 1 ELSE 0 END) AS defaulted_count,
# MAGIC     SUM(CASE WHEN loan_status = 'Overdue' THEN 1 ELSE 0 END) AS overdue_count,
# MAGIC     ROUND(SUM(loan_amount) / 10000000, 2) AS portfolio_value_cr,
# MAGIC     ROUND(SUM(CASE WHEN loan_status IN ('Defaulted', 'Overdue') THEN loan_amount ELSE 0 END) / 10000000, 2) AS at_risk_value_cr,
# MAGIC     ROUND((SUM(CASE WHEN loan_status = 'Defaulted' THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 2) AS default_rate_pct
# MAGIC   FROM sap_dev.edufin_regional.loans
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   CASE 
# MAGIC     WHEN default_rate_pct < 5.00 THEN 'HEALTHY'
# MAGIC     WHEN default_rate_pct >= 5.00 AND default_rate_pct < 10.00 THEN 'MODERATE RISK'
# MAGIC     WHEN default_rate_pct >= 10.00 AND default_rate_pct < 15.00 THEN 'HIGH RISK'
# MAGIC     WHEN default_rate_pct >= 15.00 THEN 'CRITICAL'
# MAGIC   END AS health_classification
# MAGIC FROM portfolio_metrics;
# MAGIC

# COMMAND ----------

