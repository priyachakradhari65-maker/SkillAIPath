# Databricks notebook source
# DBTITLE 1,SkillAIPath Production Troubleshooting Runbook
# MAGIC %md
# MAGIC # SkillAIPath Production Troubleshooting Runbook
# MAGIC **Systematic Debugging for Streaming CDC Pipelines**
# MAGIC
# MAGIC ## Quick Reference - Common Issues
# MAGIC
# MAGIC | Symptom | Likely Cause | Check |
# MAGIC |---------|--------------|-------|
# MAGIC | Dashboard shows wrong revenue | Gold aggregation duplicate JOIN | Section 1 |
# MAGIC | Missing customer records | SCD2 missing IsCurrent filter | Section 2 |
# MAGIC | CDC not processing | Watermark table stale | Section 3 |
# MAGIC | Streaming job stuck | Checkpoint corrupted | Section 4 |
# MAGIC | Fact table growing too fast | Duplicate detection broken | Section 5 |
# MAGIC | Gold mart empty | Silver-to-Gold job failed | Section 6 |
# MAGIC
# MAGIC ## Debugging Workflow
# MAGIC
# MAGIC ```
# MAGIC 1. DEFINE problem (symptom + expected behavior)
# MAGIC    ↓
# MAGIC 2. ISOLATE layer (Bronze → Silver → Gold)
# MAGIC    ↓
# MAGIC 3. COMPARE row counts (detect data loss/duplication)
# MAGIC    ↓
# MAGIC 4. VALIDATE calculations (detect fan-out joins)
# MAGIC    ↓
# MAGIC 5. CHECK assumptions (date filters, NULL handling, SCD2)
# MAGIC ```
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC Run Config notebook first:
# MAGIC ```python
# MAGIC %run ../1_Setup/1_config
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Setup Diagnostic Environment
# SkillAIPath Troubleshooting - Setup

import sys
import os
from pyspark.sql.functions import *
from datetime import datetime, timedelta

# Load SkillAIPath Config
current_dir = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
project_root = os.path.dirname(current_dir)
config_path = os.path.join(project_root, "00_UTILS")
sys.path.append(config_path)
from Config import Config

print("="*70)
print("SKILLAIPPATH PRODUCTION TROUBLESHOOTING")
print("="*70)
print(f"Environment: {Config.ENVIRONMENT}")
print(f"Catalog: {Config.CATALOG}")
print("="*70)

# Diagnostic widgets
dbutils.widgets.dropdown("issue_category", "data_quality", 
                        ["data_quality", "performance", "streaming", "cdc", "scd2"], 
                        "Issue Category")
dbutils.widgets.text("batch_date", "2024-05-27", "Batch Date (YYYY-MM-DD)")

issue_category = dbutils.widgets.get("issue_category")
batch_date = dbutils.widgets.get("batch_date")

print(f"\nDiagnostic Mode: {issue_category}")
print(f"Target Date: {batch_date}")

# COMMAND ----------

# DBTITLE 1,Section 1 - Gold Layer Issues (Wrong Dashboard Numbers)
# MAGIC %md
# MAGIC ## Section 1: Gold Layer - Wrong Dashboard Numbers
# MAGIC
# MAGIC ### Problem Pattern
# MAGIC
# MAGIC **Symptom:** Dashboard shows ₹45.2 Cr but Finance calculates ₹38.7 Cr manually
# MAGIC
# MAGIC **Root Cause:** Duplicate records from fan-out JOIN
# MAGIC
# MAGIC ### Diagnostic Steps
# MAGIC
# MAGIC **Step 1:** Verify Silver fact table is correct
# MAGIC
# MAGIC **Step 2:** Check Gold mart for duplicates
# MAGIC
# MAGIC **Step 3:** Identify problematic JOIN
# MAGIC
# MAGIC **Step 4:** Fix and validate

# COMMAND ----------

# DBTITLE 1,Diagnostic 1.1 - Verify Silver Fact Integrity
# MAGIC %sql
# MAGIC -- Check if silver fact_sales has duplicates
# MAGIC
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_records,
# MAGIC   COUNT(DISTINCT SalesSK) as unique_sales_keys,
# MAGIC   CASE 
# MAGIC     WHEN COUNT(*) = COUNT(DISTINCT SalesSK) THEN '[OK] No duplicates in Silver'
# MAGIC     ELSE '[ERROR] Duplicates detected in Silver fact table!'
# MAGIC   END as status,
# MAGIC   ROUND(SUM(FinalPrice) / 10000000, 2) as total_revenue_cr
# MAGIC FROM {Config.SCHEMA_SILVER}.fact_sales
# MAGIC WHERE BatchDate = '${batch_date}';
# MAGIC
# MAGIC -- EXPECTED: total_records = unique_sales_keys
# MAGIC -- If not equal: Fix Silver CDC logic before proceeding

# COMMAND ----------

# DBTITLE 1,Diagnostic 1.2 - Detect Fan-Out Joins in Gold
# MAGIC %sql
# MAGIC -- Identify gold mart queries causing duplication
# MAGIC
# MAGIC WITH silver_baseline AS (
# MAGIC   SELECT 
# MAGIC     COUNT(*) as silver_count,
# MAGIC     ROUND(SUM(FinalPrice) / 10000000, 2) as silver_revenue_cr
# MAGIC   FROM {Config.SCHEMA_SILVER}.fact_sales
# MAGIC   WHERE BatchDate = '${batch_date}'
# MAGIC ),
# MAGIC gold_aggregated AS (
# MAGIC   SELECT 
# MAGIC     COUNT(*) as gold_count,
# MAGIC     ROUND(SUM(TotalRevenue) / 10000000, 2) as gold_revenue_cr
# MAGIC   FROM {Config.SCHEMA_GOLD}.mart_product_performance
# MAGIC   WHERE aggregation_date = '${batch_date}'
# MAGIC )
# MAGIC SELECT 
# MAGIC   s.silver_count,
# MAGIC   s.silver_revenue_cr,
# MAGIC   g.gold_count,
# MAGIC   g.gold_revenue_cr,
# MAGIC   CASE 
# MAGIC     WHEN g.gold_revenue_cr > s.silver_revenue_cr * 1.1 THEN '[ERROR] Gold revenue inflated (fan-out JOIN)'
# MAGIC     WHEN g.gold_revenue_cr < s.silver_revenue_cr * 0.9 THEN '[ERROR] Gold revenue deflated (data loss)'
# MAGIC     ELSE '[OK] Gold matches Silver'
# MAGIC   END as status
# MAGIC FROM silver_baseline s
# MAGIC CROSS JOIN gold_aggregated g;
# MAGIC
# MAGIC -- EXPECTED: gold_revenue_cr ≈ silver_revenue_cr (within 5%)
# MAGIC -- If gold > silver: Fan-out JOIN (dimension has duplicates)
# MAGIC -- If gold < silver: Missing JOIN or WHERE filter too strict

# COMMAND ----------

# DBTITLE 1,Diagnostic 1.3 - Identify Problematic Dimension
# Test each dimension join individually to find the culprit

from pyspark.sql.functions import count

print("="*70)
print("TESTING DIMENSION JOINS")
print("="*70)

df_fact = spark.table(Config.TBL_FACT_SALES).filter(f"{Config.COL_BATCH_DATE} = '{batch_date}'")
fact_count = df_fact.count()

print(f"\nBaseline (fact_sales only): {fact_count:,} rows")

# Test each dimension
dimensions_to_test = [
    ("Customer", Config.TBL_DIM_CUSTOMER, "CustomerSK"),
    ("Product", Config.TBL_DIM_PRODUCT, "ProductSK"),
    ("Category", Config.TBL_DIM_CATEGORY, "CategoryID"),
    ("Warehouse", Config.TBL_DIM_WAREHOUSE, "WarehouseSK")
]

for dim_name, dim_table, join_key in dimensions_to_test:
    df_dim = spark.table(dim_table)
    
    # Check for duplicates in dimension
    dup_check = df_dim.groupBy(join_key).count().filter("count > 1")
    dup_count = dup_check.count()
    
    if dup_count > 0:
        print(f"\n[WARNING] {dim_name}: {dup_count} duplicate {join_key} values")
        print(f"  Potential cause: Missing IsCurrent filter in SCD2")
        display(dup_check.limit(5))
    
    # Test join
    try:
        if join_key in df_fact.columns:
            df_joined = df_fact.join(df_dim, join_key, "inner")
            joined_count = df_joined.count()
            
            if joined_count > fact_count:
                print(f"\n[ERROR] {dim_name} JOIN caused fan-out!")
                print(f"  Before: {fact_count:,} rows")
                print(f"  After: {joined_count:,} rows")
                print(f"  Multiplier: {joined_count / fact_count:.2f}x")
                print(f"\n  FIX: Add WHERE {Config.SCD_IS_CURRENT_COL} = TRUE in {dim_name} JOIN")
            else:
                print(f"\n[OK] {dim_name}: {joined_count:,} rows (no fan-out)")
    except Exception as e:
        print(f"\n[SKIP] {dim_name}: {str(e)}")

print("\n" + "="*70)

# COMMAND ----------

# DBTITLE 1,Section 2 - SCD Type 2 Issues (Missing/Duplicate Customers)
# MAGIC %md
# MAGIC ## Section 2: SCD Type 2 Issues
# MAGIC
# MAGIC ### Problem Patterns
# MAGIC
# MAGIC **A. Missing active customers** - Forgot `WHERE IsCurrent = TRUE`
# MAGIC
# MAGIC **B. Duplicate customer records** - SCD2 history causing fan-out
# MAGIC
# MAGIC **C. Point-in-time lookup wrong** - Using current record for historical fact
# MAGIC
# MAGIC ### Diagnostic Steps
# MAGIC
# MAGIC **Step 1:** Validate SCD2 structure (StartDate, EndDate, IsCurrent)
# MAGIC
# MAGIC **Step 2:** Check for overlapping validity periods
# MAGIC
# MAGIC **Step 3:** Verify point-in-time JOIN logic

# COMMAND ----------

# DBTITLE 1,Diagnostic 2.1 - Validate SCD2 Structure
# MAGIC %sql
# MAGIC -- Check dim_customer SCD2 integrity
# MAGIC
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_rows,
# MAGIC   COUNT(DISTINCT CustomerID) as unique_customers,
# MAGIC   SUM(CASE WHEN IsCurrent = TRUE THEN 1 ELSE 0 END) as active_count,
# MAGIC   SUM(CASE WHEN IsCurrent = FALSE THEN 1 ELSE 0 END) as historical_count,
# MAGIC   SUM(CASE WHEN EndDate = '9999-12-31' THEN 1 ELSE 0 END) as open_ended_count,
# MAGIC   CASE 
# MAGIC     WHEN SUM(CASE WHEN IsCurrent = TRUE THEN 1 ELSE 0 END) = COUNT(DISTINCT CustomerID) 
# MAGIC       THEN '[OK] Exactly one active record per customer'
# MAGIC     ELSE '[ERROR] Multiple active records detected!'
# MAGIC   END as status
# MAGIC FROM {Config.SCHEMA_SILVER}.dim_customer;
# MAGIC
# MAGIC -- EXPECTED:
# MAGIC -- total_rows > unique_customers (history exists)
# MAGIC -- active_count = unique_customers (one current per customer)
# MAGIC -- open_ended_count = active_count (current records have EndDate = 9999-12-31)

# COMMAND ----------

# DBTITLE 1,Diagnostic 2.2 - Detect Overlapping SCD2 Periods
# MAGIC %sql
# MAGIC -- Find customers with overlapping validity periods
# MAGIC
# MAGIC WITH overlaps AS (
# MAGIC   SELECT 
# MAGIC     c1.CustomerID,
# MAGIC     c1.CustomerSK as SK1,
# MAGIC     c1.StartDate as Start1,
# MAGIC     c1.EndDate as End1,
# MAGIC     c2.CustomerSK as SK2,
# MAGIC     c2.StartDate as Start2,
# MAGIC     c2.EndDate as End2
# MAGIC   FROM {Config.SCHEMA_SILVER}.dim_customer c1
# MAGIC   INNER JOIN {Config.SCHEMA_SILVER}.dim_customer c2 
# MAGIC     ON c1.CustomerID = c2.CustomerID
# MAGIC     AND c1.CustomerSK != c2.CustomerSK
# MAGIC   WHERE c1.StartDate < c2.EndDate
# MAGIC     AND c2.StartDate < c1.EndDate
# MAGIC )
# MAGIC SELECT 
# MAGIC   COUNT(*) as overlap_count,
# MAGIC   CASE 
# MAGIC     WHEN COUNT(*) = 0 THEN '[OK] No overlapping periods'
# MAGIC     ELSE '[ERROR] Overlapping SCD2 validity periods detected!'
# MAGIC   END as status
# MAGIC FROM overlaps;
# MAGIC
# MAGIC -- Display examples if overlaps exist
# MAGIC SELECT * FROM (
# MAGIC   SELECT 
# MAGIC     c1.CustomerID,
# MAGIC     c1.CustomerSK as SK1,
# MAGIC     c1.StartDate as Start1,
# MAGIC     c1.EndDate as End1,
# MAGIC     c2.CustomerSK as SK2,
# MAGIC     c2.StartDate as Start2,
# MAGIC     c2.EndDate as End2
# MAGIC   FROM {Config.SCHEMA_SILVER}.dim_customer c1
# MAGIC   INNER JOIN {Config.SCHEMA_SILVER}.dim_customer c2 
# MAGIC     ON c1.CustomerID = c2.CustomerID
# MAGIC     AND c1.CustomerSK != c2.CustomerSK
# MAGIC   WHERE c1.StartDate < c2.EndDate
# MAGIC     AND c2.StartDate < c1.EndDate
# MAGIC ) LIMIT 10;
# MAGIC
# MAGIC -- FIX: Review CDC SCD2 MERGE logic - ensure proper EndDate updates

# COMMAND ----------

# DBTITLE 1,Section 3 - CDC Processing Issues
# MAGIC %md
# MAGIC ## Section 3: CDC Processing Issues
# MAGIC
# MAGIC ### Problem Patterns
# MAGIC
# MAGIC **A. CDC not processing new changes** - Watermark not advancing
# MAGIC
# MAGIC **B. Duplicate CDC events** - Event ID deduplication broken
# MAGIC
# MAGIC **C. Out-of-order events** - Late-arriving data not handled
# MAGIC
# MAGIC ### Diagnostic Steps
# MAGIC
# MAGIC **Step 1:** Check CDC watermark table
# MAGIC
# MAGIC **Step 2:** Verify bronze CDC source has new data
# MAGIC
# MAGIC **Step 3:** Test CDC MERGE logic manually

# COMMAND ----------

# DBTITLE 1,Diagnostic 3.1 - Check CDC Watermarks
# MAGIC %sql
# MAGIC -- Check CDC processing status
# MAGIC
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   last_processed_timestamp,
# MAGIC   TIMESTAMPDIFF(HOUR, last_processed_timestamp, CURRENT_TIMESTAMP()) as hours_since_last_run,
# MAGIC   CASE 
# MAGIC     WHEN TIMESTAMPDIFF(HOUR, last_processed_timestamp, CURRENT_TIMESTAMP()) > 6 
# MAGIC       THEN '[ERROR] CDC watermark stale (>6 hours)'
# MAGIC     ELSE '[OK] Recent CDC processing'
# MAGIC   END as status
# MAGIC FROM {Config.SCHEMA_SILVER}.cdc_watermarks
# MAGIC ORDER BY last_processed_timestamp DESC;
# MAGIC
# MAGIC -- EXPECTED: last_processed_timestamp within last 1-2 hours
# MAGIC -- If stale: Check streaming job status or manual CDC notebook execution

# COMMAND ----------

# DBTITLE 1,Diagnostic 3.2 - Verify Bronze CDC Source Data
# MAGIC %sql
# MAGIC -- Check if bronze CDC source has unprocessed data
# MAGIC
# MAGIC WITH watermark AS (
# MAGIC   SELECT last_processed_timestamp
# MAGIC   FROM {Config.SCHEMA_SILVER}.cdc_watermarks
# MAGIC   WHERE table_name = 'dim_customer'
# MAGIC )
# MAGIC SELECT 
# MAGIC   COUNT(*) as unprocessed_count,
# MAGIC   MIN(_processing_time) as oldest_unprocessed,
# MAGIC   MAX(_processing_time) as newest_unprocessed,
# MAGIC   CASE 
# MAGIC     WHEN COUNT(*) = 0 THEN '[OK] No unprocessed CDC events'
# MAGIC     ELSE CONCAT('[INFO] ', COUNT(*), ' CDC events waiting to be processed')
# MAGIC   END as status
# MAGIC FROM {Config.SCHEMA_BRONZE}.customer_changes_raw
# MAGIC WHERE _processing_time > (SELECT last_processed_timestamp FROM watermark);
# MAGIC
# MAGIC -- If unprocessed_count > 0: CDC processor needs to run
# MAGIC -- If unprocessed_count = 0 but silver not updated: CDC MERGE logic issue

# COMMAND ----------

# DBTITLE 1,Section 4 - Streaming Job Issues
# MAGIC %md
# MAGIC ## Section 4: Streaming Job Issues
# MAGIC
# MAGIC ### Problem Patterns
# MAGIC
# MAGIC **A. Streaming query stuck** - Checkpoint corrupted
# MAGIC
# MAGIC **B. High watermark lag** - Processing slower than ingestion
# MAGIC
# MAGIC **C. Missing data** - Trigger interval too long
# MAGIC
# MAGIC ### Diagnostic Steps
# MAGIC
# MAGIC **Step 1:** Check streaming query status
# MAGIC
# MAGIC **Step 2:** Analyze checkpoint health
# MAGIC
# MAGIC **Step 3:** Review trigger settings

# COMMAND ----------

# DBTITLE 1,Diagnostic 4.1 - Check Streaming Query Status
# Check active streaming queries

print("="*70)
print("ACTIVE STREAMING QUERIES")
print("="*70)

try:
    active_streams = spark.streams.active
    
    if not active_streams:
        print("\n[WARNING] No active streaming queries found")
        print("  Expected: Bronze ingestion streams should be running")
    else:
        print(f"\nFound {len(active_streams)} active stream(s):\n")
        
        for stream in active_streams:
            print(f"Stream ID: {stream.id}")
            print(f"  Name: {stream.name}")
            print(f"  Status: {stream.status}")
            print(f"  Recent progress: {stream.lastProgress}")
            print("")
            
            # Check for issues
            if stream.lastProgress:
                progress = stream.lastProgress
                if 'numInputRows' in progress:
                    input_rows = progress['numInputRows']
                    if input_rows == 0:
                        print(f"  [WARNING] No new data in last batch")
                
                if 'processedRowsPerSecond' in progress:
                    throughput = progress['processedRowsPerSecond']
                    if throughput < 100:
                        print(f"  [WARNING] Low throughput: {throughput:.1f} rows/sec")
except Exception as e:
    print(f"[ERROR] Failed to check streaming status: {str(e)}")

print("="*70)

# COMMAND ----------

# DBTITLE 1,Section 5 - Data Quality Issues
# MAGIC %md
# MAGIC ## Section 5: Data Quality Issues
# MAGIC
# MAGIC ### Problem Patterns
# MAGIC
# MAGIC **A. Orphan facts** - Fact references non-existent dimension
# MAGIC
# MAGIC **B. NULL values** - Missing business keys
# MAGIC
# MAGIC **C. Data type mismatches** - String in numeric column
# MAGIC
# MAGIC **D. Out-of-range values** - Negative prices, future dates
# MAGIC
# MAGIC ### Diagnostic Steps
# MAGIC
# MAGIC **Step 1:** Check for orphan fact records
# MAGIC
# MAGIC **Step 2:** Validate NULL patterns
# MAGIC
# MAGIC **Step 3:** Test business rules

# COMMAND ----------

# DBTITLE 1,Diagnostic 5.1 - Detect Orphan Facts
# MAGIC %sql
# MAGIC -- Find fact records with missing dimension lookups
# MAGIC
# MAGIC WITH fact_keys AS (
# MAGIC   SELECT DISTINCT CustomerSK FROM {Config.SCHEMA_SILVER}.fact_sales
# MAGIC ),
# MAGIC dim_keys AS (
# MAGIC   SELECT DISTINCT CustomerSK FROM {Config.SCHEMA_SILVER}.dim_customer
# MAGIC )
# MAGIC SELECT 
# MAGIC   COUNT(*) as orphan_customer_keys,
# MAGIC   CASE 
# MAGIC     WHEN COUNT(*) = 0 THEN '[OK] All fact CustomerSK values have dimension match'
# MAGIC     ELSE '[ERROR] Orphan customer keys detected - dimension load may have failed'
# MAGIC   END as status
# MAGIC FROM fact_keys f
# MAGIC LEFT JOIN dim_keys d ON f.CustomerSK = d.CustomerSK
# MAGIC WHERE d.CustomerSK IS NULL
# MAGIC   AND f.CustomerSK != -1;  -- Exclude intentional unknown keys
# MAGIC
# MAGIC -- Repeat for other dimensions (ProductSK, WarehouseSK, etc.)
# MAGIC -- FIX: Ensure dimension notebooks run BEFORE fact notebooks in orchestrator

# COMMAND ----------

# DBTITLE 1,Diagnostic 5.2 - Validate Business Rules
# MAGIC %sql
# MAGIC -- Check for business rule violations
# MAGIC
# MAGIC SELECT 
# MAGIC   SUM(CASE WHEN FinalPrice <= 0 THEN 1 ELSE 0 END) as negative_price_count,
# MAGIC   SUM(CASE WHEN Quantity <= 0 THEN 1 ELSE 0 END) as negative_quantity_count,
# MAGIC   SUM(CASE WHEN SaleDate > CURRENT_DATE() THEN 1 ELSE 0 END) as future_date_count,
# MAGIC   SUM(CASE WHEN SaleDate < '2020-01-01' THEN 1 ELSE 0 END) as too_old_date_count,
# MAGIC   COUNT(*) as total_records,
# MAGIC   CASE 
# MAGIC     WHEN SUM(CASE WHEN FinalPrice <= 0 OR Quantity <= 0 THEN 1 ELSE 0 END) = 0 
# MAGIC       THEN '[OK] No business rule violations'
# MAGIC     ELSE '[ERROR] Data quality issues detected'
# MAGIC   END as status
# MAGIC FROM {Config.SCHEMA_SILVER}.fact_sales
# MAGIC WHERE BatchDate = '${batch_date}';
# MAGIC
# MAGIC -- FIX: Add validation in Silver transformation notebooks

# COMMAND ----------

# DBTITLE 1,Production Runbook Summary
# MAGIC %md
# MAGIC # Production Troubleshooting Checklist
# MAGIC
# MAGIC ## When Dashboard Shows Wrong Numbers
# MAGIC
# MAGIC 1. Run Diagnostic 1.1: Verify Silver fact integrity
# MAGIC 2. Run Diagnostic 1.2: Detect fan-out joins
# MAGIC 3. Run Diagnostic 1.3: Identify problematic dimension
# MAGIC 4. **Fix:** Add `WHERE IsCurrent = TRUE` to SCD2 dimension joins
# MAGIC 5. Rebuild Gold mart
# MAGIC 6. Validate results
# MAGIC
# MAGIC ## When CDC Not Processing
# MAGIC
# MAGIC 1. Run Diagnostic 3.1: Check watermark timestamps
# MAGIC 2. Run Diagnostic 3.2: Verify bronze source data
# MAGIC 3. Check streaming job logs
# MAGIC 4. **Fix:** Restart CDC processor notebook or streaming job
# MAGIC 5. Monitor watermark advancement
# MAGIC
# MAGIC ## When Streaming Job Stuck
# MAGIC
# MAGIC 1. Run Diagnostic 4.1: Check streaming query status
# MAGIC 2. Review checkpoint directory
# MAGIC 3. Check for backpressure (input rate > processing rate)
# MAGIC 4. **Fix:** Increase cluster size or reduce trigger interval
# MAGIC 5. If corrupted: Delete checkpoint and restart (loses state)
# MAGIC
# MAGIC ## Escalation Path
# MAGIC
# MAGIC **Severity 1 (Production Down):**
# MAGIC * Alert: Slack #data-engineering-oncall
# MAGIC * Contact: Data Engineering Manager
# MAGIC * SLA: 1 hour response
# MAGIC
# MAGIC **Severity 2 (Data Quality Issue):**
# MAGIC * Create: Jira ticket with diagnostic notebook link
# MAGIC * Assign: Data Engineer on rotation
# MAGIC * SLA: 4 hour response
# MAGIC
# MAGIC **Severity 3 (Performance Degradation):**
# MAGIC * Document: Run benchmark notebook
# MAGIC * Compare: vs baseline
# MAGIC * Optimize: Run table optimization if needed
# MAGIC
# MAGIC ## Monthly Health Check
# MAGIC
# MAGIC  Run all diagnostics on first Sunday of month:
# MAGIC ```python
# MAGIC dbutils.widgets.set("issue_category", "data_quality")
# MAGIC dbutils.widgets.set("batch_date", "<last_month_end_date>")
# MAGIC # Execute all diagnostic cells
# MAGIC ```