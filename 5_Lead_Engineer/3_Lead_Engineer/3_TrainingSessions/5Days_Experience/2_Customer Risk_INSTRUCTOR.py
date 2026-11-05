# Databricks notebook source
# DBTITLE 1,Customer Risk Analysis
# MAGIC %md
# MAGIC # Part 2: Customer Risk Analysis
# MAGIC
# MAGIC **Business Question:** Which customers are causing defaults, and what patterns do they share?
# MAGIC
# MAGIC **Dataset:** `workspace.sap_main.loans` (500,000 rows)
# MAGIC
# MAGIC **Goal:** Identify high-risk customer segments and provide actionable recommendations for Collections team.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Analysis Flow:
# MAGIC 1. Data validation
# MAGIC 2. CIBIL score segmentation (credit score risk)
# MAGIC 3. Age-based risk patterns
# MAGIC 4. Income-based default analysis
# MAGIC 5. Employment + Income combined view
# MAGIC 6. Top 50 highest-risk customers for Collections priority

# COMMAND ----------

# DBTITLE 1,Data Validation
# MAGIC %sql
# MAGIC -- Data Validation: Verify dataset completeness before analysis
# MAGIC -- Expected: 500K+ loans, 30K+ customers, realistic loan range
# MAGIC SELECT 
# MAGIC   COUNT(*) AS total_loans,
# MAGIC   COUNT(DISTINCT customer_id) AS unique_customers,
# MAGIC   COUNT(DISTINCT l.customer_id) AS customers_with_loans,
# MAGIC   MIN(loan_amount) AS min_loan,
# MAGIC   MAX(loan_amount) AS max_loan,
# MAGIC   COUNT(DISTINCT CASE WHEN loan_status = 'Defaulted' THEN customer_id END) AS customers_with_defaults
# MAGIC FROM workspace.sap_main.loans l;

# COMMAND ----------

# DBTITLE 1,Query 2A - CIBIL Segmentation
# MAGIC %sql
# MAGIC -- Query 2A: CIBIL Score Segmentation
# MAGIC -- Business Question: Which CIBIL ranges have highest default rates?
# MAGIC -- Expected insight: Lower CIBIL (<600) should show higher defaults
# MAGIC -- Decision: Should EduFin set a minimum CIBIL cutoff?
# MAGIC
# MAGIC SELECT 
# MAGIC   CASE 
# MAGIC     WHEN c.cibil_score < 600 THEN '<600'
# MAGIC     WHEN c.cibil_score >= 600 AND c.cibil_score <= 650 THEN '600-650'
# MAGIC     WHEN c.cibil_score >= 651 AND c.cibil_score <= 700 THEN '651-700'
# MAGIC     WHEN c.cibil_score >= 701 AND c.cibil_score <= 750 THEN '701-750'
# MAGIC     WHEN c.cibil_score > 750 THEN '750+'
# MAGIC   END AS cibil_segment,
# MAGIC   COUNT(l.loan_id) AS total_customers,
# MAGIC   COUNT(CASE WHEN l.loan_status = 'Defaulted' THEN 1 END) AS defaulted_count,
# MAGIC   ROUND((COUNT(CASE WHEN l.loan_status = 'Defaulted' THEN 1 END) * 100) / COUNT(*), 2) AS default_rate_pct
# MAGIC FROM workspace.sap_main.loans l
# MAGIC JOIN workspace.sap_main.customers c ON l.customer_id = c.customer_id
# MAGIC GROUP BY cibil_segment
# MAGIC ORDER BY 
# MAGIC   CASE 
# MAGIC     WHEN cibil_segment = '<600' THEN 1
# MAGIC     WHEN cibil_segment = '600-650' THEN 2
# MAGIC     WHEN cibil_segment = '651-700' THEN 3
# MAGIC     WHEN cibil_segment = '701-750' THEN 4
# MAGIC     WHEN cibil_segment = '750+' THEN 5
# MAGIC   END;

# COMMAND ----------

# DBTITLE 1,Query 2B - Age-Based Risk
# MAGIC %sql
# MAGIC -- Query 2B: Age-Based Risk Analysis
# MAGIC -- Business Question: Which age groups default most frequently?
# MAGIC -- Hypothesis: Younger borrowers (18-24) may have higher risk
# MAGIC -- Metric: total_exposure_cr shows financial impact per age group
# MAGIC
# MAGIC SELECT 
# MAGIC   CASE 
# MAGIC     WHEN c.age < 25 THEN '18-24'
# MAGIC     WHEN c.age >= 25 AND c.age <= 35 THEN '25-35'
# MAGIC     WHEN c.age >= 36 AND c.age <= 45 THEN '36-45'
# MAGIC     WHEN c.age >= 46 AND c.age <= 60 THEN '46-60'
# MAGIC     WHEN c.age > 60 THEN '60+'
# MAGIC   END AS age_segment,
# MAGIC   COUNT(*) AS total_loans,
# MAGIC   COUNT(CASE WHEN l.loan_status = 'Defaulted' THEN 1 END) AS defaulted_count,
# MAGIC   ROUND((COUNT(CASE WHEN l.loan_status = 'Defaulted' THEN 1 END) * 100.0) / COUNT(*), 2) AS default_rate_pct,
# MAGIC   ROUND(SUM(l.loan_amount) / 10000000, 2) AS total_exposure_cr
# MAGIC FROM workspace.sap_main.loans l
# MAGIC JOIN workspace.sap_main.customers c ON l.customer_id = c.customer_id
# MAGIC GROUP BY age_segment
# MAGIC ORDER BY 
# MAGIC   CASE 
# MAGIC     WHEN age_segment = '18-24' THEN 1
# MAGIC     WHEN age_segment = '25-35' THEN 2
# MAGIC     WHEN age_segment = '36-45' THEN 3
# MAGIC     WHEN age_segment = '46-60' THEN 4
# MAGIC     WHEN age_segment = '60+' THEN 5
# MAGIC   END;

# COMMAND ----------

# DBTITLE 1,Query 2C - Income-Based Default Analysis
# MAGIC %sql
# MAGIC -- Query 2C: Income-Based Default Analysis
# MAGIC -- Business Question: Is there a minimum safe income threshold?
# MAGIC -- Key metric: defaulted_amount_cr (financial loss per income segment)
# MAGIC -- Order: Sorted by loss amount to prioritize segments by financial impact
# MAGIC
# MAGIC SELECT 
# MAGIC   CASE 
# MAGIC     WHEN c.annual_income < 300000 THEN '<3L'
# MAGIC     WHEN c.annual_income >= 300000 AND c.annual_income <= 600000 THEN '3L-6L'
# MAGIC     WHEN c.annual_income >= 600001 AND c.annual_income <= 1200000 THEN '6L-12L'
# MAGIC     WHEN c.annual_income > 1200000 THEN '12L+'
# MAGIC   END AS income_segment,
# MAGIC   COUNT(*) AS total_loans,
# MAGIC   SUM(CASE WHEN l.loan_status = 'Defaulted' THEN 1 ELSE 0 END) AS defaulted_count,
# MAGIC   ROUND((SUM(CASE WHEN l.loan_status = 'Defaulted' THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 2) AS default_rate_pct,
# MAGIC   ROUND(SUM(CASE WHEN l.loan_status = 'Defaulted' THEN l.loan_amount ELSE 0 END) / 100000, 2) AS defaulted_amount_cr
# MAGIC FROM workspace.sap_main.loans l
# MAGIC JOIN workspace.sap_main.customers c ON l.customer_id = c.customer_id
# MAGIC GROUP BY income_segment
# MAGIC ORDER BY defaulted_amount_cr DESC;

# COMMAND ----------

# DBTITLE 1,Query 2D - Employment + Income Combined
# MAGIC %sql
# MAGIC -- Query 2D: Employment + Income Combined View
# MAGIC -- Business Question: Which employment+income combinations are riskiest?
# MAGIC -- Pattern to find: e.g., "Unemployed + Low Income" segment
# MAGIC -- Order: Sorted by default_rate_pct DESC to surface worst combinations first
# MAGIC
# MAGIC SELECT 
# MAGIC   c.employment_type,
# MAGIC   CASE 
# MAGIC     WHEN c.annual_income < 300000 THEN '<3L'
# MAGIC     WHEN c.annual_income >= 300000 AND c.annual_income <= 600000 THEN '3L-6L'
# MAGIC     WHEN c.annual_income >= 600001 AND c.annual_income <= 1200000 THEN '6L-12L'
# MAGIC     WHEN c.annual_income > 1200000 THEN '12L+'
# MAGIC   END AS income_segment,
# MAGIC   COUNT(*) AS total_loans,
# MAGIC   COUNT(CASE WHEN l.loan_status = 'Defaulted' THEN 1 END) AS defaulted_count,
# MAGIC   ROUND((COUNT(CASE WHEN l.loan_status = 'Defaulted' THEN 1 END) * 100.0) / COUNT(*), 2) AS default_rate_pct
# MAGIC FROM workspace.sap_main.loans l
# MAGIC JOIN workspace.sap_main.customers c ON l.customer_id = c.customer_id
# MAGIC GROUP BY c.employment_type, income_segment
# MAGIC ORDER BY default_rate_pct DESC;

# COMMAND ----------

# DBTITLE 1,Query 2E - Customer Risk Priority
# MAGIC %sql
# MAGIC -- Query 2E: Top 50 High-Risk Customers (Collections Priority List)
# MAGIC -- Business Question: Who should Collections contact first?
# MAGIC -- Priority Score Formula:
# MAGIC --   (default_count × 2) = Number of defaults (weight: high)
# MAGIC --   + (total_default_amount / 100000) = Financial exposure
# MAGIC --   + ((800 - cibil_score) / 10) = Credit quality penalty
# MAGIC -- Result: Customer IDs ranked by urgency for Collections team
# MAGIC
# MAGIC SELECT 
# MAGIC   l.customer_id,
# MAGIC   COUNT(CASE WHEN l.loan_status = 'Defaulted' THEN 1 END) AS default_count,
# MAGIC   SUM(CASE WHEN l.loan_status = 'Defaulted' THEN l.loan_amount ELSE 0 END) AS total_default_amount,
# MAGIC   MIN(c.cibil_score) AS min_cibil_score,
# MAGIC   (
# MAGIC     (COUNT(CASE WHEN l.loan_status = 'Defaulted' THEN 1 END) * 2) + 
# MAGIC     (SUM(CASE WHEN l.loan_status = 'Defaulted' THEN l.loan_amount ELSE 0 END) / 100000) + 
# MAGIC     (800 - MIN(c.cibil_score)) / 10
# MAGIC   ) AS priority_score
# MAGIC FROM workspace.sap_main.loans l
# MAGIC JOIN workspace.sap_main.customers c ON l.customer_id = c.customer_id
# MAGIC GROUP BY l.customer_id
# MAGIC HAVING default_count > 0
# MAGIC ORDER BY priority_score DESC
# MAGIC LIMIT 50;

# COMMAND ----------

