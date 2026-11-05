# Databricks notebook source
# MAGIC %md
# MAGIC # Part 4: DBU Cost Tracking & Budget Analysis
# MAGIC
# MAGIC **Business Question:** What does it cost to run this analysis, and how can we optimize it?
# MAGIC
# MAGIC **Data Source:** `system.billing.usage` (Databricks billing data)
# MAGIC
# MAGIC **Stakeholder:** CFO / Infrastructure Team
# MAGIC
# MAGIC **Why This Matters:**
# MAGIC Real data teams are accountable for infrastructure costs. Knowing:
# MAGIC - "I can write SQL" = Junior analyst
# MAGIC - "I can write cost-efficient SQL" = Mid-level analyst
# MAGIC - "I can architect cost-efficient data systems" = Senior engineer
# MAGIC
# MAGIC This week: You're learning Part 2.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Analysis Flow:
# MAGIC 1. Total DBU usage (entire 5-day analysis)
# MAGIC 2. Daily breakdown (identify peak days)
# MAGIC 3. Monthly budget projection (if this runs recurring)
# MAGIC 4. Budget report template
# MAGIC 5. Optimization checklist

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 4A: Total DBU Usage (5-Day Analysis)
# MAGIC -- Business Question: How much compute did we consume?
# MAGIC -- DBU = Databricks Unit (compute measure)
# MAGIC -- Cost estimate: 1 DBU ≈ ₹5 (approximate)
# MAGIC -- Filters: Last 7 days, SQL warehouse only
# MAGIC
# MAGIC SELECT 
# MAGIC   COUNT(DISTINCT usage_date) AS days_tracked,
# MAGIC   ROUND(SUM(usage_quantity), 2) AS total_dbus,
# MAGIC   ROUND(SUM(usage_quantity) * 5, 2) AS estimated_cost_inr,
# MAGIC   COUNT(*) AS query_count
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= CURRENT_DATE() - 7
# MAGIC   AND workspace_id = 'YOUR_WORKSPACE_ID'
# MAGIC   AND sku_name LIKE '%SQL%';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 4B: Daily DBU Breakdown
# MAGIC -- Business Question: Which days consumed most compute?
# MAGIC -- Use case: Identify peak days to optimize expensive queries
# MAGIC -- Pattern: Day 3-4 usually highest (Customer Risk + Time Analysis on 500K rows)
# MAGIC
# MAGIC SELECT 
# MAGIC   usage_date,
# MAGIC   ROUND(SUM(usage_quantity), 2) AS dbus_consumed,
# MAGIC   ROUND(SUM(usage_quantity) * 5, 2) AS cost_inr,
# MAGIC   COUNT(*) AS queries_run
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= CURRENT_DATE() - 7
# MAGIC   AND workspace_id = 'YOUR_WORKSPACE_ID'
# MAGIC   AND sku_name LIKE '%SQL%'
# MAGIC GROUP BY usage_date
# MAGIC ORDER BY usage_date DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 4C: Monthly Budget Projection
# MAGIC -- Business Question: What's the monthly cost if this runs recurring?
# MAGIC -- Logic: Calculate avg daily DBU, multiply by 30 days
# MAGIC -- Critical for CFO: Monthly infrastructure budget estimate
# MAGIC -- Annual projection: Shows full-year commitment
# MAGIC
# MAGIC WITH daily_average AS (
# MAGIC   SELECT 
# MAGIC     AVG(daily_dbu) AS avg_dbu_per_day
# MAGIC   FROM (
# MAGIC     SELECT 
# MAGIC       usage_date,
# MAGIC       SUM(usage_quantity) AS daily_dbu
# MAGIC     FROM system.billing.usage
# MAGIC     WHERE usage_date >= CURRENT_DATE() - 7
# MAGIC       AND workspace_id = 'YOUR_WORKSPACE_ID'
# MAGIC       AND sku_name LIKE '%SQL%'
# MAGIC     GROUP BY usage_date
# MAGIC   )
# MAGIC )
# MAGIC SELECT 
# MAGIC   ROUND(avg_dbu_per_day, 2) AS avg_daily_dbu,
# MAGIC   ROUND(avg_dbu_per_day * 30, 2) AS monthly_dbu_estimate,
# MAGIC   ROUND(avg_dbu_per_day * 30 * 5, 2) AS monthly_cost_inr,
# MAGIC   ROUND(avg_dbu_per_day * 365 * 5, 2) AS annual_cost_inr
# MAGIC FROM daily_average;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Your Budget Report
# MAGIC
# MAGIC **Instructions:** Run queries 1-3 above, then fill in this template for your final presentation.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **This Week (5-Day Analysis):**
# MAGIC - Days tracked: _____
# MAGIC - Total DBUs: _____
# MAGIC - Estimated cost: ₹_____
# MAGIC
# MAGIC **Monthly Estimate (if running monthly):**
# MAGIC - Avg daily DBUs: _____
# MAGIC - Monthly DBUs: _____
# MAGIC - Monthly cost: ₹_____
# MAGIC
# MAGIC **Optimization Impact:**
# MAGIC - Current monthly cost: ₹_____
# MAGIC - After optimization: ₹_____
# MAGIC - Savings: ₹_____ (_____% reduction)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💡 Optimization Checklist
# MAGIC
# MAGIC **Quick wins to reduce costs:**
# MAGIC
# MAGIC ✓ **Filter early** - Add `WHERE loan_date >= '2024-01-01'` to limit data scanned
# MAGIC
# MAGIC ✓ **Select specific columns** - Use `SELECT loan_id, amount` instead of `SELECT *`
# MAGIC
# MAGIC ✓ **Limit during testing** - Add `LIMIT 100` when developing queries
# MAGIC
# MAGIC ✓ **Cache repeated joins** - Create temp table for customer demographics if joining multiple times
# MAGIC
# MAGIC ✓ **Incremental refresh** - For monthly reports, only process new data since last run
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📈 Your Recommendation
# MAGIC
# MAGIC **Current setup:**
# MAGIC - Serverless SQL (✓ good choice for ad-hoc analysis)
# MAGIC - Estimated monthly cost: ₹_____ (fill from Query 3)
# MAGIC
# MAGIC **If costs exceed ₹5,000/month:**
# MAGIC - Switch to dedicated SQL warehouse
# MAGIC - Implement incremental processing
# MAGIC - Pre-compute common aggregations