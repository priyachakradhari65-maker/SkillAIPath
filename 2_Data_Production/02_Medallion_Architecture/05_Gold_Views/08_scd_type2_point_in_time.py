# Databricks notebook source
# DBTITLE 1,SCD Type 2 - Configuration & Overview
# TITLE: SCD Type 2 (Slowly Changing Dimensions) with Point-in-Time Queries
# PURPOSE: Track historical changes in dimension tables for accurate temporal analysis
# EDITION: Free Edition Compatible
# AUDIENCE: Data Engineers building dimensional data warehouses

# SCD TYPE 2 OVERVIEW:
# - Maintains full history of dimension changes (customer address changes, product price updates)
# - Uses effective_start_date, effective_end_date, is_current flag
# - Enables point-in-time queries ("What was customer's address on 2024-03-15?")
# - Critical for accurate historical reporting and regulatory compliance

# PRODUCTION NOTES:
# - Schedule: Run daily after dimension refresh
# - Performance: SCD merge can be slow on large dimensions (millions of rows)
# - Testing: Validate effective date ranges have no gaps or overlaps
# - Dependencies: Requires source dimension table and target SCD table

%run ../../../1_Canvas/1_Setup/1_config

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, date

# Set catalog context for portable execution
current_catalog = spark.catalog.currentCatalog()
print(f"✓ Configuration loaded (portable)")
print(f"  Current Catalog: {current_catalog}")
print(f"  Source: {Config.TBL_DIM_CUSTOMER}")
print(f"  Target: {Config.TBL_DIM_CUSTOMER.replace('dim_customer', 'dim_customer_history')}")

# COMMAND ----------

# DBTITLE 1,Create SCD Type 2 Table Schema
# MAGIC %sql
# MAGIC -- CELL: Create Target SCD Table
# MAGIC -- Adds temporal tracking columns to dimension structure
# MAGIC -- Uses current catalog context (set via Cell 1)
# MAGIC
# MAGIC USE CATALOG sap_dev;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.dim_customer_history (
# MAGIC     customer_id STRING NOT NULL,
# MAGIC     customer_name STRING,
# MAGIC     email STRING,
# MAGIC     phone STRING,
# MAGIC     city STRING,
# MAGIC     state STRING,
# MAGIC     customer_segment STRING,
# MAGIC     registration_date DATE,
# MAGIC
# MAGIC     -- SCD Type 2 tracking columns
# MAGIC     effective_start_date DATE NOT NULL,
# MAGIC     effective_end_date DATE,
# MAGIC     is_current BOOLEAN NOT NULL,
# MAGIC     scd_version INT NOT NULL,
# MAGIC
# MAGIC     -- Audit columns
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     updated_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (is_current)
# MAGIC COMMENT 'Customer dimension history - SCD Type 2 for temporal analytics';
# MAGIC
# MAGIC -- Explanation:
# MAGIC -- effective_start_date: When this version became active
# MAGIC -- effective_end_date: When this version was superseded (NULL for current)
# MAGIC -- is_current: TRUE for active record, FALSE for historical
# MAGIC -- scd_version: Sequential version number per customer_id
# MAGIC
# MAGIC SELECT 'Customer history table created successfully' AS status;

# COMMAND ----------

# DBTITLE 1,SCD Type 2 MERGE Logic (Detect Changes)
# CELL: SCD Type 2 Incremental Load
# Compare source dimension with SCD table, detect changes, create new versions

from pyspark.sql import functions as F
from datetime import date

print("=" * 60)
print("SCD TYPE 2 MERGE OPERATION")
print("=" * 60)

# Load current dimension snapshot
source_df = spark.table(Config.TBL_DIM_CUSTOMER)

print(f"\nSource records: {source_df.count():,}")

# Load existing SCD table (only current records for comparison)
try:
    scd_df = spark.table(Config.TBL_DIM_CUSTOMER.replace('dim_customer', 'dim_customer_history')).filter(F.col('is_current') == True)
    print(f"Current SCD records: {scd_df.count():,}")
except:
    print("History table empty - performing initial load")
    scd_df = None

# Detect changes: compare source with current SCD records
if scd_df is not None:
    # Join source with SCD using CustomerSK cast to string (matches customer_id)
    comparison_df = source_df.alias('src').join(
        scd_df.alias('scd'),
        F.col('src.CustomerSK').cast('string') == F.col('scd.customer_id'),
        how='left'
    )

    # Build change detection logic - map source columns to SCD columns
    change_conditions = [
        F.col('src.CustomerName') != F.col('scd.customer_name'),
        F.col('src.CustomerMail') != F.col('scd.email'),
        F.col('src.CustomerContact') != F.col('scd.phone'),
        F.col('src.CustomerAddress') != F.col('scd.city')
    ]

    # Records that changed or are new
    changed_df = comparison_df.filter(
        F.col('scd.customer_id').isNull() |  # New records
        F.coalesce(*change_conditions, F.lit(False))  # Changed records
    ).select('src.*')

    change_count = changed_df.count()
    print(f"\nChanges detected: {change_count:,}")

    if change_count == 0:
        print("No changes found - SCD merge skipped")
    else:
        print(f"Proceeding with SCD Type 2 merge for {change_count:,} records")
else:
    # Initial load
    changed_df = source_df
    print("\nPerforming initial SCD load for all records")

# COMMAND ----------

# DBTITLE 1,Execute SCD Type 2 MERGE Statement
# CELL: Apply SCD Type 2 Changes
# Close out old versions, insert new versions

from delta.tables import DeltaTable
import time

if changed_df is not None and changed_df.count() > 0:

    print("=" * 60)
    print("EXECUTING SCD TYPE 2 MERGE")
    print("=" * 60)

    start_time = time.time()
    current_date = date.today()

    # Prepare new SCD records - map source schema to SCD schema
    new_scd_records = changed_df.select(
        F.col('CustomerSK').cast('string').alias('customer_id'),
        F.col('CustomerName').alias('customer_name'),
        F.col('CustomerMail').alias('email'),
        F.col('CustomerContact').alias('phone'),
        F.col('CustomerAddress').alias('city'),  # Using CustomerAddress for city
        F.lit(None).cast('string').alias('state'),  # No state in source
        F.lit(None).cast('string').alias('customer_segment'),  # No segment in source
        F.col('StartDate').alias('registration_date')
    ).withColumn('effective_start_date', F.date_sub(F.current_date(), 60)) \
        .withColumn('effective_end_date', F.lit(None).cast('date')) \
        .withColumn('is_current', F.lit(True)) \
        .withColumn('scd_version', F.lit(1)) \
        .withColumn('created_timestamp', F.current_timestamp()) \
        .withColumn('updated_timestamp', F.current_timestamp())

    try:
        # Get DeltaTable handle (uses Config for portability)
        scd_table = DeltaTable.forName(spark, Config.TBL_DIM_CUSTOMER.replace('dim_customer', 'dim_customer_history'))

        # MERGE logic:
        # 1. Update old current records: Set is_current=False, effective_end_date=today
        # 2. Insert new version records with is_current=True

        scd_table.alias('target').merge(
            new_scd_records.alias('source'),
            'target.customer_id = source.customer_id AND target.is_current = TRUE'
        ).whenMatchedUpdate(
            set = {
                'is_current': F.lit(False),
                'effective_end_date': F.lit(current_date),
                'updated_timestamp': F.current_timestamp()
            }
        ).whenNotMatchedInsert(
            values = {
                'customer_id': 'source.customer_id',
                'customer_name': 'source.customer_name',
                'email': 'source.email',
                'phone': 'source.phone',
                'city': 'source.city',
                'state': 'source.state',
                'customer_segment': 'source.customer_segment',
                'registration_date': 'source.registration_date',
                'effective_start_date': 'source.effective_start_date',
                'effective_end_date': 'source.effective_end_date',
                'is_current': 'source.is_current',
                'scd_version': 'source.scd_version',
                'created_timestamp': 'source.created_timestamp',
                'updated_timestamp': 'source.updated_timestamp'
            }
        ).execute()

        duration = time.time() - start_time

        print(f"\nMERGE completed successfully")
        print(f"Records processed: {changed_df.count():,}")
        print(f"Duration: {duration:.2f} seconds")
        print(f"Effective date: {current_date}")

    except Exception as e:
        print(f"ERROR during MERGE: {str(e)}")
        raise

    print("\n" + "=" * 60)
    print("SCD TYPE 2 MERGE COMPLETE")
    print("=" * 60)

else:
    print("\nNo changes to apply - SCD table up to date")

# COMMAND ----------

# DBTITLE 1,Point-in-Time Query Examples
# MAGIC %sql
# MAGIC -- CELL: Point-in-Time Query Patterns
# MAGIC -- Retrieve dimension state as of a specific date
# MAGIC -- Uses current catalog context (portable across environments)
# MAGIC
# MAGIC USE CATALOG sap_dev;
# MAGIC
# MAGIC -- Example 1: Get customer details as of a specific date (using last 7 days for demo data)
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     city,
# MAGIC     customer_segment,
# MAGIC     effective_start_date,
# MAGIC     effective_end_date,
# MAGIC     scd_version
# MAGIC FROM silver.dim_customer_history
# MAGIC WHERE effective_start_date <= CURRENT_DATE()
# MAGIC     AND (effective_end_date > CURRENT_DATE() OR effective_end_date IS NULL)
# MAGIC LIMIT 10;
# MAGIC
# MAGIC -- Example 2: Historical revenue analysis with point-in-time customer attributes
# MAGIC -- Join fact_sales with customer dimension as of BatchDate (actual date column in schema)
# MAGIC SELECT
# MAGIC     s.BatchDate AS order_date,
# MAGIC     c.city AS customer_city_at_order_time,
# MAGIC     c.customer_segment AS segment_at_order_time,
# MAGIC     COUNT(s.OrderID) AS orders,
# MAGIC     ROUND(SUM(s.finalprice), 2) AS revenue
# MAGIC FROM silver.fact_sales s
# MAGIC INNER JOIN silver.dim_customer_history c
# MAGIC     ON CAST(s.CustomerSK AS STRING) = c.customer_id
# MAGIC     AND s.BatchDate >= c.effective_start_date
# MAGIC     AND (s.BatchDate < c.effective_end_date OR c.effective_end_date IS NULL)
# MAGIC WHERE s.BatchDate >= DATE_SUB(CURRENT_DATE(), 7)
# MAGIC GROUP BY s.BatchDate, c.city, c.customer_segment
# MAGIC ORDER BY s.BatchDate, revenue DESC
# MAGIC LIMIT 20;
# MAGIC
# MAGIC -- This query ensures we use customer attributes as they existed at order time
# MAGIC -- Critical for accurate historical segmentation and geographic analysis

# COMMAND ----------

# DBTITLE 1,SCD Validation & Monitoring
# CELL: SCD Data Quality Checks
# Validate temporal integrity and monitor SCD health

from pyspark.sql import functions as F

print("=" * 60)
print("SCD TYPE 2 VALIDATION CHECKS")
print("=" * 60)

scd_table = spark.table(Config.TBL_DIM_CUSTOMER.replace('dim_customer', 'dim_customer_history'))

# Check 1: Exactly one current record per customer_id
print("\n--- Check 1: Current Record Uniqueness ---")
current_counts = scd_table.filter(F.col('is_current') == True) \
    .groupBy('customer_id').count() \
    .filter(F.col('count') > 1)

duplicates = current_counts.count()
if duplicates == 0:
    print("PASS: Each customer has exactly one current record")
else:
    print(f"FAIL: {duplicates} customers have multiple current records")
    current_counts.show(10)

# Check 2: No gaps in effective date ranges
print("\n--- Check 2: Date Range Continuity ---")
window_spec = Window.partitionBy('customer_id').orderBy('effective_start_date')

gap_check = scd_table.withColumn(
    'next_start_date',
    F.lead('effective_start_date').over(window_spec)
).filter(
    (F.col('effective_end_date') != F.col('next_start_date')) &
    (F.col('effective_end_date').isNotNull())
)

gaps = gap_check.count()
if gaps == 0:
    print("PASS: No gaps in date ranges")
else:
    print(f"FAIL: {gaps} date range gaps detected")
    gap_check.select('customer_id', 'effective_start_date', 'effective_end_date', 'next_start_date').show(10)

# Check 3: No overlapping date ranges
print("\n--- Check 3: No Overlapping Dates ---")
overlap_check = scd_table.alias('a').join(
    scd_table.alias('b'),
    (F.col('a.customer_id') == F.col('b.customer_id')) &
    (F.col('a.scd_version') != F.col('b.scd_version')) &
    (F.col('a.effective_start_date') < F.coalesce(F.col('b.effective_end_date'), F.lit('9999-12-31'))) &
    (F.coalesce(F.col('a.effective_end_date'), F.lit('9999-12-31')) > F.col('b.effective_start_date'))
)

overlaps = overlap_check.count()
if overlaps == 0:
    print("PASS: No overlapping date ranges")
else:
    print(f"FAIL: {overlaps} overlapping ranges detected")

# Summary Statistics
print("\n--- SCD Summary Statistics ---")
total_records = scd_table.count()
current_records = scd_table.filter(F.col('is_current') == True).count()
historical_records = total_records - current_records

print(f"Total SCD records: {total_records:,}")
print(f"Current records: {current_records:,}")
print(f"Historical records: {historical_records:,}")

if historical_records > 0:
    avg_versions = total_records / current_records
    print(f"Avg versions per customer: {avg_versions:.2f}")

print("\n" + "=" * 60)
print("VALIDATION COMPLETE")
print("=" * 60)

# COMMAND ----------

# DBTITLE 1,Production Deployment Notes
# MAGIC %md
# MAGIC # SCD Type 2 Production Deployment Guide
# MAGIC
# MAGIC ## Use Cases
# MAGIC
# MAGIC **When to use SCD Type 2:**
# MAGIC - Customer address changes (billing/shipping accuracy)
# MAGIC - Product pricing history (margin analysis)
# MAGIC - Employee department transfers (historical org charts)
# MAGIC - Supplier relationship changes (procurement audits)
# MAGIC - Regulatory compliance (audit trails)
# MAGIC
# MAGIC **When NOT to use:**
# MAGIC - High-frequency changes (consider Type 1 or event tables)
# MAGIC - Non-business-critical attributes (use Type 1 overwrite)
# MAGIC - Real-time streaming (latency concerns)
# MAGIC
# MAGIC ## Scheduling
# MAGIC
# MAGIC **Recommended Schedule:**
# MAGIC ```
# MAGIC Frequency: Daily (after dimension refresh)
# MAGIC Timing: 3 AM (low traffic window)
# MAGIC Dependencies: Run after bronze/silver dimension loads
# MAGIC Timeout: 1 hour
# MAGIC Retries: 2 with 10-minute delay
# MAGIC ```
# MAGIC
# MAGIC ## Performance Optimization
# MAGIC
# MAGIC **For Large Dimensions (> 10M rows):**
# MAGIC 1. Partition SCD table by `is_current` (separate current from historical)
# MAGIC 2. Use ZORDER on `customer_id` and `effective_start_date`
# MAGIC 3. Consider incremental processing (filter source by last_modified_date)
# MAGIC 4. Use appropriate cluster sizing (8-16 workers for 10M+ rows)
# MAGIC
# MAGIC **MERGE Performance:**
# MAGIC - SCD MERGE is expensive (full table scan)
# MAGIC - Pre-filter changed records in source (use CDC if available)
# MAGIC - Monitor MERGE duration (baseline: ~5 minutes per 1M records)
# MAGIC
# MAGIC ## Cost Estimates
# MAGIC
# MAGIC **Compute Costs:**
# MAGIC - Daily SCD merge: ~1-2 DBU per 1M records
# MAGIC - Point-in-time queries: Standard query costs (benefit from partitioning)
# MAGIC
# MAGIC **Storage Costs:**
# MAGIC - History growth: ~10-30% per year (depends on change frequency)
# MAGIC - Use VACUUM to reclaim old file versions
# MAGIC
# MAGIC ## Monitoring
# MAGIC
# MAGIC **Key Metrics:**
# MAGIC 1. SCD MERGE duration (alert if > 2x baseline)
# MAGIC 2. Historical record growth rate (% increase per month)
# MAGIC 3. Data quality check failures (gap/overlap detection)
# MAGIC 4. Point-in-time query performance (benchmark monthly)
# MAGIC
# MAGIC **Alerts:**
# MAGIC - Multiple current records per customer: Data quality issue
# MAGIC - Date range gaps: MERGE logic error
# MAGIC - MERGE failure: Escalate immediately (dimension drift)
# MAGIC
# MAGIC ## Testing Checklist
# MAGIC
# MAGIC - [ ] Initial load creates version 1 for all records
# MAGIC - [ ] Changed records create version 2, close version 1
# MAGIC - [ ] Unchanged records remain untouched
# MAGIC - [ ] Point-in-time query returns correct historical state
# MAGIC - [ ] Current flag always TRUE for latest version
# MAGIC - [ ] No gaps or overlaps in effective date ranges
# MAGIC - [ ] MERGE completes within timeout window
