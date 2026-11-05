# Databricks notebook source
# DBTITLE 1,Portfolio Health Analysis
# MAGIC %md
# MAGIC # Part 1: Portfolio Health Analysis
# MAGIC
# MAGIC **Business Question:** Is EduFin's portfolio healthy enough to justify ₹50 crore expansion?
# MAGIC
# MAGIC **Dataset:** `workspace.sap_dev.loans` (5,000 rows - development subset)
# MAGIC
# MAGIC **Stakeholder:** CFO needs evidence before board meeting.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Analysis Flow:
# MAGIC 1. Data validation check
# MAGIC 2. Portfolio composition by status
# MAGIC 3. Portfolio scale metrics (customers, loans, value)
# MAGIC 4. Risk concentration by loan size
# MAGIC 5. Financial impact by status
# MAGIC 6. Executive dashboard (health classification)

# COMMAND ----------

# DBTITLE 1,Data Check
# MAGIC %sql
# MAGIC -- Data Validation: Verify dataset is ready
# MAGIC -- Expected: 5,000 loans, multiple statuses, 3,000+ customers
# MAGIC -- If you see 0 rows, run StartHere notebook first
# MAGIC SELECT 
# MAGIC   COUNT(*) AS total_loans,
# MAGIC   COUNT(DISTINCT loan_status) AS status_types,
# MAGIC   COUNT(DISTINCT customer_id) AS unique_customers
# MAGIC FROM workspace.sap_dev.loans;

# COMMAND ----------

# DBTITLE 1,Portfolio Status Distribution
# MAGIC %sql
# MAGIC -- Query 1A: Portfolio Status Distribution
# MAGIC -- Business Question: What % of loans are Active vs Defaulted vs Overdue?
# MAGIC -- Expected: Active should be majority, Defaulted <15% is healthy
# MAGIC
# MAGIC SELECT 
# MAGIC   loan_status,
# MAGIC   COUNT(*) AS loan_count,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
# MAGIC FROM workspace.sap_dev.loans
# MAGIC GROUP BY loan_status
# MAGIC ORDER BY loan_count DESC;

# COMMAND ----------

# DBTITLE 1,Portfolio Scale Metrics
# MAGIC %sql
# MAGIC -- Query 1B: Portfolio Scale Metrics
# MAGIC -- Shows: Total exposure, average loan size
# MAGIC -- Metrics in crores (cr) and lakhs for readability
# MAGIC
# MAGIC SELECT 
# MAGIC   COUNT(DISTINCT customer_id) AS total_customers,
# MAGIC   COUNT(*) AS total_loans,
# MAGIC   ROUND(SUM(loan_amount) / 10000000, 2) AS portfolio_value_cr,
# MAGIC   ROUND(AVG(loan_amount) / 100000, 2) AS avg_loan_size_lakh
# MAGIC FROM workspace.sap_dev.loans;

# COMMAND ----------

# DBTITLE 1,Risk Concentration
# MAGIC %sql
# MAGIC -- Query 1C: Risk Concentration by Loan Size
# MAGIC -- Business Question: Are large loans (>₹6L) defaulting more?
# MAGIC -- Risk bands: Small (<₹3L), Medium (₹3L-₹6L), Large (>₹6L)
# MAGIC -- Key insight: Which loan size has highest default count?
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
# MAGIC FROM workspace.sap_dev.loans
# MAGIC GROUP BY risk_band
# MAGIC ORDER BY 
# MAGIC   CASE 
# MAGIC     WHEN risk_band = 'Small (<₹3L)' THEN 1
# MAGIC     WHEN risk_band = 'Medium (₹3L-₹6L)' THEN 2
# MAGIC     WHEN risk_band = 'Large (>₹6L)' THEN 3
# MAGIC   END;

# COMMAND ----------

# DBTITLE 1,Financial Impact
# MAGIC %sql
# MAGIC -- Query 1D: Financial Impact by Status
# MAGIC -- Shows: Total value (crores) locked in each status
# MAGIC -- Critical: How much capital is at risk (Active + Overdue) vs lost (Defaulted)?
# MAGIC
# MAGIC SELECT 
# MAGIC   loan_status,
# MAGIC   ROUND(SUM(loan_amount) / 10000000, 2) AS total_value_cr
# MAGIC FROM workspace.sap_dev.loans
# MAGIC GROUP BY loan_status
# MAGIC ORDER BY total_value_cr DESC;

# COMMAND ----------

# DBTITLE 1,Executive Dashboard
# MAGIC %sql
# MAGIC -- Query 1E: Executive Dashboard (Final Recommendation)
# MAGIC -- Calculates: Default rate, at-risk value, health classification
# MAGIC -- Classification:
# MAGIC --   <5% default = HEALTHY
# MAGIC --   5-10% = MODERATE RISK
# MAGIC --   10-15% = HIGH RISK
# MAGIC --   >15% = CRITICAL
# MAGIC -- Answer: Should CFO approve ₹50 crore expansion?
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
# MAGIC   FROM workspace.sap_dev.loans
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

# COMMAND ----------

