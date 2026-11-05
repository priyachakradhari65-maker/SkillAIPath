# Databricks notebook source
# DBTITLE 1,Time Analysis
# MAGIC %md
# MAGIC # Part 3: Time Trends Analysis
# MAGIC
# MAGIC **Business Question:** Are defaults accelerating, stabilizing, or improving over time?
# MAGIC
# MAGIC **Dataset:** `workspace.sap_main.loans` (500,000 rows)
# MAGIC
# MAGIC **Stakeholder:** Board of Directors needs to see if underwriting improvements are working.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Analysis Flow:
# MAGIC 1. Data validation check
# MAGIC 2. Monthly default rate trends (are defaults increasing?)
# MAGIC 3. Institution performance (which colleges' engineers default fastest?)
# MAGIC 4. Cohort analysis (are recent loans performing better?)
# MAGIC 5. Cumulative financial loss trajectory
# MAGIC 6. Repayment behavior trends (when do loans succeed vs fail?)

# COMMAND ----------

# DBTITLE 1,Query 3A - Monthly Default Rate Trend
# MAGIC %sql
# MAGIC -- Query 3A: Monthly Default Rate Trend
# MAGIC -- Business Question: Are defaults accelerating or improving?
# MAGIC -- Trend analysis: Look for upward/downward pattern in month_over_month_change
# MAGIC -- Board wants: Stable or declining trend to approve expansion
# MAGIC
# MAGIC SELECT 
# MAGIC   DATE_FORMAT(default_date, 'yyyy-MM') AS default_month,
# MAGIC   COUNT(*) AS total_defaults,
# MAGIC   ROUND((COUNT(*) * 100.0) / (SELECT COUNT(*) FROM workspace.sap_main.loans WHERE default_date IS NOT NULL), 2) AS default_pct,
# MAGIC   LAG(COUNT(*)) OVER (ORDER BY DATE_FORMAT(default_date, 'yyyy-MM')) AS prev_month_defaults,
# MAGIC   COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY DATE_FORMAT(default_date, 'yyyy-MM')) AS month_over_month_change
# MAGIC FROM workspace.sap_main.loans
# MAGIC WHERE default_date IS NOT NULL
# MAGIC GROUP BY default_month
# MAGIC ORDER BY default_month;

# COMMAND ----------

# DBTITLE 1,Query 3B - Institutional Default Timing
# MAGIC %sql
# MAGIC -- Query 3B: Institution Performance (Early Warning)
# MAGIC -- Business Question: Which colleges' engineers default fastest?
# MAGIC -- Key metric: avg_days_to_default (lower = defaults happen sooner = riskier)
# MAGIC -- Decision: Should EduFin blacklist certain institutions?
# MAGIC -- Filter: Only institutions with 5+ defaults for statistical significance
# MAGIC
# MAGIC SELECT 
# MAGIC   i.institution_name,
# MAGIC   COUNT(CASE WHEN l.loan_status = 'Defaulted' THEN 1 END) AS total_defaults,
# MAGIC   ROUND(AVG(DATEDIFF(l.default_date, l.disbursement_date)), 0) AS avg_days_to_default,
# MAGIC   ROUND(SUM(CASE WHEN l.loan_status = 'Defaulted' THEN l.loan_amount ELSE 0 END) / 10000000, 2) AS total_defaulted_cr
# MAGIC FROM workspace.sap_main.loans l
# MAGIC JOIN workspace.sap_main.institutions i ON l.institution_id = i.institution_id
# MAGIC WHERE l.default_date IS NOT NULL
# MAGIC GROUP BY i.institution_name
# MAGIC HAVING total_defaults >= 5
# MAGIC ORDER BY avg_days_to_default ASC;

# COMMAND ----------

# DBTITLE 1,Query 3C - Cohort Default Tracking
# MAGIC %sql
# MAGIC -- Query 3C: Cohort Default Tracking
# MAGIC -- Business Question: Are recent loan batches performing better?
# MAGIC -- Cohort = All loans originated in same month
# MAGIC -- Pattern to find: Do 2024 cohorts have lower default_rate_pct than 2023?
# MAGIC -- Evidence of: Underwriting improvements working (or not)
# MAGIC
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
# MAGIC FROM workspace.sap_main.loans
# MAGIC GROUP BY cohort_month
# MAGIC ORDER BY cohort_month;

# COMMAND ----------

# DBTITLE 1,Query 3D - Cumulative Financial Loss
# MAGIC %sql
# MAGIC -- Query 3D: Cumulative Financial Loss Trajectory
# MAGIC -- Business Question: Is financial loss accelerating or slowing?
# MAGIC -- Key metric: cumulative_loss_cr (running total of defaults)
# MAGIC -- Critical: Is the curve steepening (bad) or flattening (good)?
# MAGIC -- avg_survival_days: How long before typical loan defaults?
# MAGIC
# MAGIC SELECT 
# MAGIC   DATE_FORMAT(default_date, 'yyyy-MM') AS default_month,
# MAGIC   ROUND(SUM(loan_amount) / 1000000, 2) AS monthly_loss_cr,
# MAGIC   ROUND(SUM(SUM(loan_amount)) OVER (ORDER BY DATE_FORMAT(default_date, 'yyyy-MM')) / 10000000, 2) AS cumulative_loss_cr,
# MAGIC   ROUND(AVG(DATEDIFF(default_date, disbursement_date)), 0) AS avg_survival_days
# MAGIC FROM workspace.sap_main.loans
# MAGIC WHERE loan_status = 'Defaulted' AND default_date IS NOT NULL
# MAGIC GROUP BY default_month
# MAGIC ORDER BY default_month;

# COMMAND ----------

# DBTITLE 1,Query 3E - Repayment Behavior Trends
# MAGIC %sql
# MAGIC -- Query 3E: Repayment Behavior Trends
# MAGIC -- Business Question: When do loans succeed vs fail?
# MAGIC -- Good sign: High closure_rate_pct (loans getting repaid)
# MAGIC -- Bad sign: High at_risk_count (many Defaulted + Overdue)
# MAGIC -- Trend: Are recent cohorts closing successfully?
# MAGIC
# MAGIC SELECT 
# MAGIC   DATE_FORMAT(disbursement_date, 'yyyy-MM') AS cohort_month,
# MAGIC   COUNT(*) AS total_loans,
# MAGIC   COUNT(CASE WHEN loan_status = 'Closed' THEN 1 END) AS closed_count,
# MAGIC   COUNT(CASE WHEN loan_status = 'Active' THEN 1 END) AS active_count,
# MAGIC   COUNT(CASE WHEN loan_status IN ('Defaulted', 'Overdue') THEN 1 END) AS at_risk_count,
# MAGIC   ROUND((COUNT(CASE WHEN loan_status = 'Closed' THEN 1 END) * 100.0) / COUNT(*), 2) AS closure_rate_pct
# MAGIC FROM workspace.sap_main.loans
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
# MAGIC FROM workspace.sap_main.loans;
# MAGIC
# MAGIC -- Expected: ~5000 loans, date range 2020-2024, multiple years, some defaults with dates
# MAGIC -- If you see 0 loans or errors, contact support before proceeding

# COMMAND ----------

