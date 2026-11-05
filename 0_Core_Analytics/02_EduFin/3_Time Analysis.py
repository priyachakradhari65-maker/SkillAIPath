# Databricks notebook source
# DBTITLE 1,Time Analysis
# MAGIC %md
# MAGIC # Time Analysis
# MAGIC
# MAGIC **Dataset:** `sap_dev.edufin_regional.loans`

# COMMAND ----------

# DBTITLE 1,Query 3A - Monthly Default Rate Trend
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   DATE_FORMAT(default_date, 'yyyy-MM') AS default_month,
# MAGIC   COUNT(*) AS total_defaults,
# MAGIC   ROUND((COUNT(*) * 100.0) / (SELECT COUNT(*) FROM sap_dev.edufin_regional.loans WHERE default_date IS NOT NULL), 2) AS default_pct,
# MAGIC   LAG(COUNT(*)) OVER (ORDER BY DATE_FORMAT(default_date, 'yyyy-MM')) AS prev_month_defaults,
# MAGIC   COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY DATE_FORMAT(default_date, 'yyyy-MM')) AS month_over_month_change
# MAGIC FROM sap_dev.edufin_regional.loans
# MAGIC WHERE default_date IS NOT NULL
# MAGIC GROUP BY default_month
# MAGIC ORDER BY default_month;

# COMMAND ----------

# DBTITLE 1,Query 3B - Institutional Default Timing
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   i.institution_name,
# MAGIC   COUNT(CASE WHEN l.loan_status = 'Defaulted' THEN 1 END) AS total_defaults,
# MAGIC   ROUND(AVG(DATEDIFF(l.default_date, l.disbursement_date)), 0) AS avg_days_to_default,
# MAGIC   ROUND(SUM(CASE WHEN l.loan_status = 'Defaulted' THEN l.loan_amount ELSE 0 END) / 10000000, 2) AS total_defaulted_cr
# MAGIC FROM sap_dev.edufin_regional.loans l
# MAGIC JOIN sap_dev.edufin_national.institutions i ON l.institution_id = i.institution_id
# MAGIC WHERE l.default_date IS NOT NULL
# MAGIC GROUP BY i.institution_name
# MAGIC HAVING total_defaults >= 5
# MAGIC ORDER BY avg_days_to_default ASC;

# COMMAND ----------

# DBTITLE 1,Query 3C - Cohort Default Tracking
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   DATE_FORMAT(disbursement_date, 'yyyy-MM') AS cohort_month,
# MAGIC   COUNT(*) AS total_loans,
# MAGIC   COUNT(CASE WHEN loan_status = 'Defaulted' THEN 1 END) AS defaulted_loans,
# MAGIC   ROUND((COUNT(CASE WHEN loan_status = 'Defaulted' THEN 1 END) * 100.0) / COUNT(*), 2) AS default_rate_pct,
# MAGIC   CASE 
# MAGIC     WHEN ROUND((COUNT(CASE WHEN loan_status = 'Defaulted' THEN 1 END) * 100.0) / COUNT(*), 2) < 5.00 THEN 'Normal'
# MAGIC     WHEN ROUND((COUNT(CASE WHEN loan_status = 'Defaulted' THEN 1 END) * 100.0) / COUNT(*), 2) >= 5.00 AND ROUND((COUNT(CASE WHEN loan_status = 'Defaulted' THEN 1 END) * 100.0) / COUNT(*), 2) < 10.00 THEN 'Warning'
# MAGIC     WHEN ROUND((COUNT(CASE WHEN loan_status = 'Defaulted' THEN 1 END) * 100.0) / COUNT(*), 2) >= 10.00 AND ROUND((COUNT(CASE WHEN loan_status = 'Defaulted' THEN 1 END) * 100.0) / COUNT(*), 2) < 15.00 THEN 'High Risk'
# MAGIC     WHEN ROUND((COUNT(CASE WHEN loan_status = 'Defaulted' THEN 1 END) * 100.0) / COUNT(*), 2) >= 15.00 THEN 'Severe Crisis'
# MAGIC   END AS risk_classification
# MAGIC FROM sap_dev.edufin_regional.loans
# MAGIC GROUP BY cohort_month
# MAGIC ORDER BY cohort_month;

# COMMAND ----------

# DBTITLE 1,Query 3D - Cumulative Financial Loss
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   DATE_FORMAT(default_date, 'yyyy-MM') AS default_month,
# MAGIC   ROUND(SUM(loan_amount) / 1000000, 2) AS monthly_loss_cr,
# MAGIC   ROUND(SUM(SUM(loan_amount)) OVER (ORDER BY DATE_FORMAT(default_date, 'yyyy-MM')) / 10000000, 2) AS cumulative_loss_cr,
# MAGIC   ROUND(AVG(DATEDIFF(default_date, disbursement_date)), 0) AS avg_survival_days
# MAGIC FROM sap_dev.edufin_regional.loans
# MAGIC WHERE loan_status = 'Defaulted' AND default_date IS NOT NULL
# MAGIC GROUP BY default_month
# MAGIC ORDER BY default_month;

# COMMAND ----------

# DBTITLE 1,Query 3E - Repayment Behavior Trends
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   DATE_FORMAT(disbursement_date, 'yyyy-MM') AS cohort_month,
# MAGIC   COUNT(*) AS total_loans,
# MAGIC   COUNT(CASE WHEN loan_status = 'Closed' THEN 1 END) AS closed_count,
# MAGIC   COUNT(CASE WHEN loan_status = 'Active' THEN 1 END) AS active_count,
# MAGIC   COUNT(CASE WHEN loan_status IN ('Defaulted', 'Overdue') THEN 1 END) AS at_risk_count,
# MAGIC   ROUND((COUNT(CASE WHEN loan_status = 'Closed' THEN 1 END) * 100.0) / COUNT(*), 2) AS closure_rate_pct
# MAGIC FROM sap_dev.edufin_regional.loans
# MAGIC GROUP BY cohort_month
# MAGIC ORDER BY cohort_month;

# COMMAND ----------

# DBTITLE 1,Data Validation Check (Run First)
# MAGIC %sql
# MAGIC -- Run this query to verify your environment is ready
# MAGIC -- This is NOT a graded query - just a system check
# MAGIC
# MAGIC SELECT 
# MAGIC   COUNT(*) AS total_loans,
# MAGIC   MIN(disbursement_date) AS earliest_disbursement,
# MAGIC   MAX(disbursement_date) AS latest_disbursement,
# MAGIC   COUNT(DISTINCT EXTRACT(YEAR FROM disbursement_date)) AS year_span,
# MAGIC   COUNT(CASE WHEN default_date IS NOT NULL THEN 1 END) AS loans_with_default_date
# MAGIC FROM sap_dev.edufin_regional.loans;
# MAGIC
# MAGIC -- Expected: ~5000 loans, date range 2020-2024, multiple years, some defaults with dates
# MAGIC -- If you see 0 loans or errors, contact support before proceeding