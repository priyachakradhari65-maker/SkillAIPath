# Databricks notebook source
# DBTITLE 1,SQL Alerts - Data Quality Monitoring
# MAGIC %md
# MAGIC # SQL Alerts - Data Quality Monitoring
# MAGIC **SkillAIPath Module 23 - Lakeview & Genie**
# MAGIC
# MAGIC ## What are SQL Alerts?
# MAGIC
# MAGIC **SQL Alerts** = Automated threshold monitoring on SQL queries. Get notifications when:
# MAGIC - Data quality issues detected (null values, duplicates)
# MAGIC - Business metrics exceed thresholds (budget overrun, churn spike)
# MAGIC - Pipeline failures (zero rows ingested)
# MAGIC
# MAGIC **Key Features:**
# MAGIC - SQL-based conditions (no Python required)
# MAGIC - Email/Slack/webhook notifications
# MAGIC - Schedule (hourly, daily, phase-based)
# MAGIC - FREE (included in all Databricks editions)
# MAGIC
# MAGIC **Launched:** 2022 (DBSQL feature)
# MAGIC
# MAGIC **Use Case:** Proactive monitoring without manual checks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Why SQL Alerts for Interviews?
# MAGIC
# MAGIC **Common Interview Question:**
# MAGIC "How do you ensure data quality in production pipelines?"
# MAGIC
# MAGIC **Answer Options:**
# MAGIC 1. Manual validation queries (slow, inconsistent)
# MAGIC 2. dbt tests (requires dbt setup)
# MAGIC 3. Great Expectations (Python framework, heavy)
# MAGIC 4. **SQL Alerts** (lightweight, SQL-based, built-in)
# MAGIC
# MAGIC **Portfolio Value:**
# MAGIC "Implemented automated data quality monitoring with SQL alerts, catching 95% of issues before business impact (avg detection time: 15 minutes)"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## This Notebook
# MAGIC
# MAGIC **Goal:** Set up SQL alerts for SkillAIPath data quality monitoring
# MAGIC
# MAGIC **Alerts:**
# MAGIC 1. **Data freshness:** No new sales data in 24 hours
# MAGIC 2. **Null check:** Critical columns have null values
# MAGIC 3. **Duplicate check:** Duplicate transactions detected
# MAGIC 4. **Business metric:** Revenue drop >20% vs prior week
# MAGIC
# MAGIC **Time:** 20 minutes setup

# COMMAND ----------

# DBTITLE 1,Config & Prerequisites
# MAGIC %run /Workspace/Shared/dev/SkillAIPath/00_UTILS/01_config
# MAGIC
# MAGIC from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring
# MAGIC
# MAGIC print("="*70)
# MAGIC print("SQL ALERTS SETUP")
# MAGIC print("="*70)
# MAGIC print(f"Environment: {Config.ENVIRONMENT}")
# MAGIC print(f"Monitoring tables: {Config.SCHEMA_GOLD}")
# MAGIC print("="*70)
# MAGIC print()
# MAGIC print("PREREQUISITES:")
# MAGIC print("- SQL Warehouses enabled (for alert queries)")
# MAGIC print("- Email/Slack configured for notifications")
# MAGIC print("- Gold marts populated (Module 05 completed)")
# MAGIC print("="*70)

# COMMAND ----------

# DBTITLE 1,Alert 1: Data Freshness Check
# MAGIC %md
# MAGIC ## Alert 1: Data Freshness Check
# MAGIC
# MAGIC **Scenario:** Pipeline stopped running - no new sales data
# MAGIC
# MAGIC **SQL Query (save as "Data Freshness - Sales"):**
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC     MAX(Date) AS last_sale_date,
# MAGIC     DATEDIFF(CURRENT_DATE, MAX(Date)) AS days_since_last_sale,
# MAGIC     CASE
# MAGIC         WHEN DATEDIFF(CURRENT_DATE, MAX(Date)) > 1 THEN 'ALERT'
# MAGIC         ELSE 'OK'
# MAGIC     END AS status
# MAGIC FROM sap_gold.mart_revenue_dashboard
# MAGIC ```
# MAGIC
# MAGIC **Alert Condition:**
# MAGIC - Trigger when: `days_since_last_sale > 1`
# MAGIC - Schedule: Hourly (business hours)
# MAGIC - Notification: Email to data team
# MAGIC
# MAGIC **Setup:**
# MAGIC 1. Go to SQL Editor
# MAGIC 2. Create query above
# MAGIC 3. Click **Alert** button
# MAGIC 4. Set condition: `days_since_last_sale > 1`
# MAGIC 5. Add destination: data-team@example.com
# MAGIC 6. Schedule: Every hour (9 AM - 6 PM)
# MAGIC 7. Save
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Result:** Get notified within 1 hour if pipeline stops

# COMMAND ----------

# DBTITLE 1,Alert 2: Null Value Check
# MAGIC %md
# MAGIC ## Alert 2: Null Value Check
# MAGIC
# MAGIC **Scenario:** Critical columns have unexpected nulls
# MAGIC
# MAGIC **SQL Query (save as "Data Quality - Null Check"):**
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     SUM(CASE WHEN ProductName IS NULL THEN 1 ELSE 0 END) AS null_product,
# MAGIC     SUM(CASE WHEN Revenue IS NULL THEN 1 ELSE 0 END) AS null_revenue,
# MAGIC     SUM(CASE WHEN Date IS NULL THEN 1 ELSE 0 END) AS null_date,
# MAGIC     CASE
# MAGIC         WHEN SUM(CASE WHEN ProductName IS NULL OR Revenue IS NULL OR Date IS NULL THEN 1 ELSE 0 END) > 0
# MAGIC         THEN 'ALERT'
# MAGIC         ELSE 'OK'
# MAGIC     END AS status
# MAGIC FROM sap_gold.mart_revenue_dashboard
# MAGIC WHERE Date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC ```
# MAGIC
# MAGIC **Alert Condition:**
# MAGIC - Trigger when: `null_product > 0 OR null_revenue > 0 OR null_date > 0`
# MAGIC - Schedule: Daily at 9 AM
# MAGIC - Notification: Slack #data-quality
# MAGIC
# MAGIC **Setup:**
# MAGIC 1. Create query in SQL Editor
# MAGIC 2. Click **Alert** button
# MAGIC 3. Set condition: `status = 'ALERT'`
# MAGIC 4. Add destination: Slack webhook
# MAGIC 5. Schedule: Daily 9 AM
# MAGIC 6. Save
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Result:** Daily report of null value violations

# COMMAND ----------

# DBTITLE 1,Alert 3: Duplicate Transaction Check
# MAGIC %md
# MAGIC ## Alert 3: Duplicate Transaction Check
# MAGIC
# MAGIC **Scenario:** Same transaction ID appears multiple times
# MAGIC
# MAGIC **SQL Query (save as "Data Quality - Duplicates"):**
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_transactions,
# MAGIC     COUNT(DISTINCT TransactionID) AS unique_transactions,
# MAGIC     COUNT(*) - COUNT(DISTINCT TransactionID) AS duplicate_count,
# MAGIC     CASE
# MAGIC         WHEN COUNT(*) > COUNT(DISTINCT TransactionID) THEN 'ALERT'
# MAGIC         ELSE 'OK'
# MAGIC     END AS status
# MAGIC FROM sap_silver.fact_sales
# MAGIC WHERE Date >= CURRENT_DATE - INTERVAL 1 DAY
# MAGIC ```
# MAGIC
# MAGIC **Alert Condition:**
# MAGIC - Trigger when: `duplicate_count > 0`
# MAGIC - Schedule: Hourly
# MAGIC - Notification: Email + Slack
# MAGIC
# MAGIC **Setup:**
# MAGIC 1. Create query in SQL Editor
# MAGIC 2. Click **Alert** button
# MAGIC 3. Set condition: `status = 'ALERT'`
# MAGIC 4. Add destinations:
# MAGIC    - Email: data-team@example.com
# MAGIC    - Slack: #data-quality
# MAGIC 5. Schedule: Every hour
# MAGIC 6. Save
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Result:** Immediate notification if duplicates detected

# COMMAND ----------

# DBTITLE 1,Alert 4: Revenue Drop Detection
# MAGIC %md
# MAGIC ## Alert 4: Revenue Drop Detection
# MAGIC
# MAGIC **Scenario:** Revenue drops >20% vs prior week (business issue)
# MAGIC
# MAGIC **SQL Query (save as "Business Metric - Revenue Drop"):**
# MAGIC
# MAGIC ```sql
# MAGIC WITH phase-based_revenue AS (
# MAGIC     SELECT
# MAGIC         YEAR(Date) AS year,
# MAGIC         WEEKOFYEAR(Date) AS week,
# MAGIC         SUM(DailyRevenue) AS total_revenue
# MAGIC     FROM sap_gold.mart_revenue_dashboard
# MAGIC     WHERE Date >= CURRENT_DATE - INTERVAL 14 DAYS
# MAGIC     GROUP BY YEAR(Date), WEEKOFYEAR(Date)
# MAGIC ),
# MAGIC revenue_comparison AS (
# MAGIC     SELECT
# MAGIC         curr.year,
# MAGIC         curr.week,
# MAGIC         curr.total_revenue AS current_week_revenue,
# MAGIC         LAG(curr.total_revenue) OVER (ORDER BY curr.year, curr.week) AS prior_week_revenue,
# MAGIC         (curr.total_revenue - LAG(curr.total_revenue) OVER (ORDER BY curr.year, curr.week)) * 100.0
# MAGIC             / LAG(curr.total_revenue) OVER (ORDER BY curr.year, curr.week) AS pct_change
# MAGIC     FROM phase-based_revenue curr
# MAGIC )
# MAGIC SELECT
# MAGIC     year,
# MAGIC     week,
# MAGIC     ROUND(current_week_revenue, 2) AS current_revenue,
# MAGIC     ROUND(prior_week_revenue, 2) AS prior_revenue,
# MAGIC     ROUND(pct_change, 2) AS pct_change,
# MAGIC     CASE
# MAGIC         WHEN pct_change < -20 THEN 'ALERT'
# MAGIC         ELSE 'OK'
# MAGIC     END AS status
# MAGIC FROM revenue_comparison
# MAGIC ORDER BY year DESC, week DESC
# MAGIC LIMIT 1
# MAGIC ```
# MAGIC
# MAGIC **Alert Condition:**
# MAGIC - Trigger when: `pct_change < -20`
# MAGIC - Schedule: Daily at 9 AM
# MAGIC - Notification: Email to executives
# MAGIC
# MAGIC **Setup:**
# MAGIC 1. Create query in SQL Editor
# MAGIC 2. Click **Alert** button
# MAGIC 3. Set condition: `status = 'ALERT'`
# MAGIC 4. Add destination: executives@example.com
# MAGIC 5. Schedule: Daily 9 AM
# MAGIC 6. Save
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Result:** Executive team notified of significant revenue drops

# COMMAND ----------

# DBTITLE 1,Alert 5: Cost Budget Overrun
# MAGIC %md
# MAGIC ## Alert 5: Cost Budget Overrun
# MAGIC
# MAGIC **Scenario:** Databricks compute costs exceed monthly budget
# MAGIC
# MAGIC **SQL Query (save as "Cost Monitoring - Budget Alert"):**
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC     SUM(cost_usd) AS month_to_date_cost,
# MAGIC     5000 AS monthly_budget,
# MAGIC     (SUM(cost_usd) * 100.0 / 5000) AS pct_of_budget,
# MAGIC     CASE
# MAGIC         WHEN SUM(cost_usd) > 5000 * 0.8 THEN 'ALERT'
# MAGIC         ELSE 'OK'
# MAGIC     END AS status
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= DATE_TRUNC('MONTH', CURRENT_DATE)
# MAGIC ```
# MAGIC
# MAGIC **Alert Condition:**
# MAGIC - Trigger when: `pct_of_budget > 80`
# MAGIC - Schedule: Daily at 8 AM
# MAGIC - Notification: Email to finance + engineering
# MAGIC
# MAGIC **Setup:**
# MAGIC 1. Create query in SQL Editor
# MAGIC 2. Click **Alert** button
# MAGIC 3. Set condition: `status = 'ALERT'`
# MAGIC 4. Add destinations:
# MAGIC    - finance@example.com
# MAGIC    - engineering-leads@example.com
# MAGIC 5. Schedule: Daily 8 AM
# MAGIC 6. Save
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Result:** Proactive cost management (see Module 14_COST_MONITORING)

# COMMAND ----------

# DBTITLE 1,Alert Management Best Practices
# MAGIC %md
# MAGIC ## Alert Management Best Practices
# MAGIC
# MAGIC ### 1. Alert Fatigue Prevention
# MAGIC
# MAGIC **Problem:** Too many alerts = ignored alerts
# MAGIC
# MAGIC **Solution:**
# MAGIC - Limit to 5-10 critical alerts
# MAGIC - Use different severity levels (email vs Slack vs PagerDuty)
# MAGIC - Add cooldown periods (don't alert every hour for same issue)
# MAGIC
# MAGIC ### 2. Alert Hierarchy
# MAGIC
# MAGIC **Tier 1 (Critical - PagerDuty):**
# MAGIC - Pipeline stopped (data freshness)
# MAGIC - Production database down
# MAGIC - Revenue processing failure
# MAGIC
# MAGIC **Tier 2 (High - Slack):**
# MAGIC - Data quality issues (nulls, duplicates)
# MAGIC - Business metric anomalies (revenue drop)
# MAGIC - Cost overruns
# MAGIC
# MAGIC **Tier 3 (Low - Email):**
# MAGIC - Weekly summary reports
# MAGIC - Non-critical warnings
# MAGIC
# MAGIC ### 3. Alert Response Playbook
# MAGIC
# MAGIC **For Each Alert:**
# MAGIC 1. Define expected response time (15 min, 1 hour, 1 day)
# MAGIC 2. Document troubleshooting steps
# MAGIC 3. Assign owner (who gets notified)
# MAGIC 4. Track false positives (refine thresholds)
# MAGIC
# MAGIC ### 4. Alert Testing
# MAGIC
# MAGIC **Before Production:**
# MAGIC - Test alert query manually
# MAGIC - Verify notification delivery
# MAGIC - Confirm threshold triggers correctly
# MAGIC - Document alert purpose + runbook
# MAGIC
# MAGIC ### 5. Alert Documentation
# MAGIC
# MAGIC **For Each Alert, Document:**
# MAGIC - What it monitors
# MAGIC - Why threshold was chosen
# MAGIC - Who to notify
# MAGIC - How to troubleshoot
# MAGIC - Runbook link (Module 03_DEBUGGING)

# COMMAND ----------

# DBTITLE 1,Portfolio Tips
# MAGIC %md
# MAGIC ## Portfolio Tips - SQL Alerts
# MAGIC
# MAGIC **Resume Bullet:**
# MAGIC "Implemented automated data quality monitoring with 15+ SQL alerts, reducing mean-time-to-detection (MTTD) from 4 hours to 15 minutes and preventing 95% of data quality issues from reaching production dashboards"
# MAGIC
# MAGIC **Interview Talking Points:**
# MAGIC
# MAGIC 1. **Alert Strategy:**
# MAGIC    - 3-tier severity system (critical/high/low)
# MAGIC    - Threshold tuning (95% confidence intervals)
# MAGIC    - Alert cooldown to prevent fatigue
# MAGIC
# MAGIC 2. **Metrics to Track:**
# MAGIC    - MTTD (mean time to detection): 15 minutes
# MAGIC    - MTTR (mean time to resolution): 1 hour
# MAGIC    - False positive rate: <5%
# MAGIC    - Alert coverage: 95% of production tables
# MAGIC
# MAGIC 3. **Real-World Example:**
# MAGIC    "We had a pipeline that occasionally stopped at 3 AM. Without alerts, we'd discover it at 9 AM when dashboards were stale. After implementing data freshness alerts, we detect failures within 15 minutes and auto-trigger restart workflows."
# MAGIC
# MAGIC 4. **Cost Impact:**
# MAGIC    - SQL alerts are FREE (no compute when not running)
# MAGIC    - Prevented $50K revenue loss from dashboard outages
# MAGIC    - Reduced on-call incidents by 60%
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Common Mistake:**
# MAGIC Don't alert on every small variation - use statistical thresholds (2-3 standard deviations) to reduce noise.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Module 23 Complete:**
# MAGIC - 01 Lakeview dashboards
# MAGIC - 02 Genie Spaces
# MAGIC - 03 Self-service Excel to Genie
# MAGIC - 04 SQL alerts (you are here)
# MAGIC
# MAGIC **Next:** Update MODULE_INDEX files to include Module 23

# COMMAND ----------

