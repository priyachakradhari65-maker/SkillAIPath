# Databricks notebook source
# DBTITLE 1,Lakeview Dashboard Setup
# MAGIC %md
# MAGIC # Lakeview Dashboards - Complete Walkthrough
# MAGIC **SkillAIPath Module 23 - Lakeview & Genie**
# MAGIC
# MAGIC ## What is Lakeview?
# MAGIC
# MAGIC **Lakeview** is Databricks' native dashboarding solution - think Tableau built directly into Databricks.
# MAGIC
# MAGIC **Key Features:**
# MAGIC - Drag-and-drop visualization builder
# MAGIC - SQL-based queries (no Python required)
# MAGIC - Real-time refresh from Unity Catalog tables
# MAGIC - Embedding in external apps
# MAGIC - Filters, parameters, drill-down
# MAGIC - FREE (included in all Databricks editions)
# MAGIC
# MAGIC **Launched:** Mid-2023 (GA)
# MAGIC
# MAGIC **Use Case:** Build interactive dashboards on top of your Gold marts without leaving Databricks.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Why Lakeview for Interviews?
# MAGIC
# MAGIC **Common Interview Question:**
# MAGIC "We need business users to explore data without SQL knowledge. What would you recommend?"
# MAGIC
# MAGIC **Answer Options:**
# MAGIC 1. Power BI / Tableau (extra licensing cost, data export required)
# MAGIC 2. Streamlit apps (requires Python, maintenance overhead)
# MAGIC 3. **Lakeview dashboards** (FREE, no data movement, drag-and-drop)
# MAGIC
# MAGIC **Portfolio Value:** Show you can deliver end-to-end solutions (pipeline + dashboard).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## This Notebook
# MAGIC
# MAGIC **Goal:** Create a Lakeview dashboard from SkillAIPath Gold marts
# MAGIC
# MAGIC **Data Source:** `sap_gold.mart_revenue_dashboard` (from Module 05)
# MAGIC
# MAGIC **Deliverable:** Interactive dashboard with:
# MAGIC - Revenue trends (line chart)
# MAGIC - Top products (bar chart)
# MAGIC - Sales by region (map)
# MAGIC - Filters (date range, store, product category)
# MAGIC
# MAGIC **Time:** 15 minutes (manual UI walkthrough)

# COMMAND ----------

# DBTITLE 1,Config & Prerequisites
# MAGIC %run /Workspace/Shared/dev/SkillAIPath/00_UTILS/01_config
# MAGIC
# MAGIC from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring
# MAGIC
# MAGIC print("="*70)
# MAGIC print("LAKEVIEW DASHBOARD SETUP")
# MAGIC print("="*70)
# MAGIC print(f"Environment: {Config.ENVIRONMENT}")
# MAGIC print(f"Gold Table: {Config.TBL_GOLD_REVENUE_DASHBOARD}")
# MAGIC print("="*70)
# MAGIC print()
# MAGIC print("PREREQUISITES:")
# MAGIC print("- Module 05_GOLD completed (mart_revenue_dashboard exists)")
# MAGIC print("- Databricks workspace with Lakeview enabled (all editions)")
# MAGIC print("="*70)

# COMMAND ----------

# DBTITLE 1,Verify Gold Data Exists
# Check if revenue dashboard mart exists
try:
    df_revenue = spark.read.table(Config.TBL_GOLD_REVENUE_DASHBOARD)
    record_count = df_revenue.count()

    print(f"SUCCESS: Found {record_count:,} records in {Config.TBL_GOLD_REVENUE_DASHBOARD}")
    print()
    print("Sample data:")
    df_revenue.limit(5).display()

except Exception as e:
    print(f"ERROR: Gold mart not found - {str(e)}")
    print()
    print("SOLUTION: Run Module 05_GOLD/04_mart_revenue_dashboard first")
    dbutils.notebook.exit("ERROR: Gold mart missing")

# COMMAND ----------

# DBTITLE 1,Step 1: Navigate to Lakeview
# MAGIC %md
# MAGIC ## Step 1: Navigate to Lakeview
# MAGIC
# MAGIC **UI Path:**
# MAGIC 1. Click **SQL** in left sidebar
# MAGIC 2. Click **Dashboards** (under SQL section)
# MAGIC 3. Click **Create Dashboard** button (top right)
# MAGIC
# MAGIC **Alternative:**
# MAGIC - Go directly to: `https://<your-workspace>.cloud.databricks.com/sql/dashboards`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **What You'll See:**
# MAGIC - Blank canvas with drag-and-drop interface
# MAGIC - Visualization gallery (bar, line, pie, scatter, etc.)
# MAGIC - Query editor (SQL-based)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **IMPORTANT:** Lakeview is a UI feature - this notebook guides you through manual setup (cannot be fully automated via API yet).

# COMMAND ----------

# DBTITLE 1,Step 2: Create Query for Revenue Trends
# MAGIC %md
# MAGIC ## Step 2: Create Query for Revenue Trends
# MAGIC
# MAGIC **In Lakeview Dashboard:**
# MAGIC 1. Click **Add** > **Visualization**
# MAGIC 2. Click **Create New Query**
# MAGIC 3. Paste this SQL:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC     Date,
# MAGIC     SUM(DailyRevenue) AS TotalRevenue,
# MAGIC     SUM(TransactionCount) AS Transactions
# MAGIC FROM sap_gold.mart_revenue_dashboard
# MAGIC WHERE Date >= CURRENT_DATE - INTERVAL 90 DAYS
# MAGIC GROUP BY Date
# MAGIC ORDER BY Date
# MAGIC ```
# MAGIC
# MAGIC 4. Click **Run Query**
# MAGIC 5. Choose **Line Chart** visualization
# MAGIC 6. Configure:
# MAGIC    - X-axis: Date
# MAGIC    - Y-axis: TotalRevenue
# MAGIC    - Series: (none)
# MAGIC 7. Click **Save** > Name it "Revenue Trends (Last 90 Days)"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Result:** Line chart showing daily revenue over last 90 days

# COMMAND ----------

# DBTITLE 1,Step 3: Add Top Products Bar Chart
# MAGIC %md
# MAGIC ## Step 3: Add Top Products Bar Chart
# MAGIC
# MAGIC **In Lakeview Dashboard:**
# MAGIC 1. Click **Add** > **Visualization**
# MAGIC 2. Click **Create New Query**
# MAGIC 3. Paste this SQL:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC     ProductName,
# MAGIC     SUM(DailyRevenue) AS TotalRevenue
# MAGIC FROM sap_gold.mart_revenue_dashboard
# MAGIC WHERE Date >= CURRENT_DATE - INTERVAL 90 DAYS
# MAGIC GROUP BY ProductName
# MAGIC ORDER BY TotalRevenue DESC
# MAGIC LIMIT 10
# MAGIC ```
# MAGIC
# MAGIC 4. Click **Run Query**
# MAGIC 5. Choose **Bar Chart** visualization
# MAGIC 6. Configure:
# MAGIC    - X-axis: ProductName
# MAGIC    - Y-axis: TotalRevenue
# MAGIC    - Sort: Descending
# MAGIC 7. Click **Save** > Name it "Top 10 Products (Last 90 Days)"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Result:** Horizontal bar chart of top-selling products

# COMMAND ----------

# DBTITLE 1,Step 4: Add Date Range Filter
# MAGIC %md
# MAGIC ## Step 4: Add Date Range Filter
# MAGIC
# MAGIC **Make Dashboard Interactive:**
# MAGIC
# MAGIC 1. Click **Add** > **Parameter**
# MAGIC 2. Configure:
# MAGIC    - Name: `start_date`
# MAGIC    - Type: Date
# MAGIC    - Default: 90 days ago
# MAGIC 3. Click **Add** > **Parameter**
# MAGIC 4. Configure:
# MAGIC    - Name: `end_date`
# MAGIC    - Type: Date
# MAGIC    - Default: Today
# MAGIC
# MAGIC 5. **Update Queries** to use parameters:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC     Date,
# MAGIC     SUM(DailyRevenue) AS TotalRevenue
# MAGIC FROM sap_gold.mart_revenue_dashboard
# MAGIC WHERE Date BETWEEN {{ start_date }} AND {{ end_date }}
# MAGIC GROUP BY Date
# MAGIC ORDER BY Date
# MAGIC ```
# MAGIC
# MAGIC 6. Click **Save**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Result:** SkillAIPath Developers can now filter dashboard by custom date range

# COMMAND ----------

# DBTITLE 1,Step 5: Publish and Share
# MAGIC %md
# MAGIC ## Step 5: Publish and Share
# MAGIC
# MAGIC **Make Dashboard Available:**
# MAGIC
# MAGIC 1. Click **Publish** (top right)
# MAGIC 2. Configure:
# MAGIC    - Dashboard name: "SkillAIPath Revenue Dashboard"
# MAGIC    - Refresh schedule: Daily at 8 AM
# MAGIC    - Permissions: Share with team
# MAGIC
# MAGIC 3. Copy dashboard URL
# MAGIC 4. Share with business stakeholders
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Embedding Option:**
# MAGIC
# MAGIC ```html
# MAGIC <iframe
# MAGIC   src="https://<workspace>.databricks.com/sql/dashboards/<dashboard-id>?embed=true"
# MAGIC   width="100%"
# MAGIC   height="800px"
# MAGIC   frameborder="0">
# MAGIC </iframe>
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Result:** Live dashboard accessible via URL or embedded in internal apps

# COMMAND ----------

# DBTITLE 1,Portfolio Tips
# MAGIC %md
# MAGIC ## Portfolio Tips - Lakeview Dashboards
# MAGIC
# MAGIC **Resume Bullet:**
# MAGIC "Built interactive Lakeview dashboards on Unity Catalog gold marts, enabling self-service analytics for 50+ business users (zero SQL knowledge required)"
# MAGIC
# MAGIC **Interview Talking Points:**
# MAGIC 1. **Why Lakeview vs Power BI?**
# MAGIC    - Zero data movement (queries Unity Catalog directly)
# MAGIC    - No extra licensing ($10/user/month savings)
# MAGIC    - Real-time refresh (no ETL lag)
# MAGIC
# MAGIC 2. **Performance Optimization:**
# MAGIC    - Use liquid clustering on gold marts (Module 07)
# MAGIC    - Add date filters to limit scan size
# MAGIC    - Schedule dashboard refresh during off-peak hours
# MAGIC
# MAGIC 3. **Security:**
# MAGIC    - Row-level security via ABAC policies (Module 10)
# MAGIC    - Dashboard permissions inherit Unity Catalog grants
# MAGIC    - Audit trail for dashboard access
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Common Mistake:**
# MAGIC Don't query raw bronze/silver tables in dashboards - always use pre-aggregated gold marts for performance.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Next Steps:**
# MAGIC - Module 23 Notebook 02: Genie Spaces (conversational analytics)
# MAGIC - Module 23 Notebook 03: Self-service Excel to Genie automation

# COMMAND ----------

