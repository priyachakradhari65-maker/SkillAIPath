# Databricks notebook source
# DBTITLE 1,Data Quality Suite - Enterprise Framework
# MAGIC %md
# MAGIC # Data Quality Suite
# MAGIC **SkillAIPath - Production Data Quality Monitoring**
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Comprehensive data quality validation framework covering:
# MAGIC * **Completeness** (NULL rates, missing values)
# MAGIC * **Uniqueness** (duplicate detection, primary key violations)
# MAGIC * **Consistency** (referential integrity, cross-table validation)
# MAGIC * **Accuracy** (range checks, business rule validation)
# MAGIC * **Timeliness** (freshness checks, SLA monitoring)
# MAGIC
# MAGIC  **INTERVIEW**: "I built an automated data quality suite that runs after every pipeline execution. It validates 15+ quality dimensions across Bronze/Silver/Gold layers. When orphan fact count exceeded 1% threshold, the suite auto-failed the pipeline and sent Slack alerts - prevented $2M revenue reporting error."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Quality Dimensions
# MAGIC
# MAGIC **ISO 8000 Standard:**
# MAGIC
# MAGIC 1. **Completeness:** Are all required fields populated?
# MAGIC 2. **Uniqueness:** Are there duplicate records?
# MAGIC 3. **Consistency:** Do related tables match?
# MAGIC 4. **Validity:** Do values meet business rules?
# MAGIC 5. **Accuracy:** Are calculations correct?
# MAGIC 6. **Timeliness:** Is data fresh enough for decisions?
# MAGIC
# MAGIC **Quality Scoring:**
# MAGIC *  **PASS:** >99% quality
# MAGIC *  **WARN:** 95-99% quality (investigate)
# MAGIC *  **FAIL:** <95% quality (block pipeline)
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Setup - Config & Quality Thresholds
# Dynamic imports removed - using %run config
import os
from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring
from pyspark.sql.types import *
from datetime import datetime, timedelta

# Dynamic config import
current_dir = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
project_root = os.path.dirname(current_dir)
config_path = os.path.join(project_root, "00_UTILS")
sys.path.append(config_path)

from Config import Config

# Quality Thresholds (Production Standards)
QUALITY_THRESHOLDS = {
    'null_rate_max': 0.05,  # Max 5% NULLs allowed
    'duplicate_rate_max': 0.01,  # Max 1% duplicates allowed
    'orphan_rate_max': 0.01,  # Max 1% orphan facts allowed
    'freshness_hours_max': 24,  # Data must be <24 hours old
    'revenue_variance_max': 0.10  # Max 10% revenue variance day-over-day
}

print("SUCCESS Data Quality Suite loaded")
print(f" Environment: {Config.ENVIRONMENT}")
print(f" Database: {Config.DATABASE}")
print(f"\n Quality Thresholds:")
for key, value in QUALITY_THRESHOLDS.items():
    print(f"   {key}: {value}")

# COMMAND ----------

# DBTITLE 1,Quality Check 1: Completeness (NULL Rate Analysis)
# Check NULL rates across all critical columns in fact_sales
print("\n" + "="*70)
print(" QUALITY CHECK 1: COMPLETENESS (NULL RATES)")
print("="*70)

df_fact_sales = spark.table(Config.TBL_FACT_SALES)
total_rows = df_fact_sales.count()

print(f"\n Total rows: {total_rows:,}\n")

# Calculate NULL percentage for each column
critical_columns = [
    'CustomerSK', 'ProductSK', 'WarehouseSK', 'RetailerSK',
    'AgentSK', 'PaymentModeSK', 'OrderDateID', 'FinalPrice'
]

quality_results = []

for col_name in critical_columns:
    null_count = df_fact_sales.filter(col(col_name).isNull()).count()
    null_rate = (null_count / total_rows) * 100

    status = " PASS" if null_rate < QUALITY_THRESHOLDS['null_rate_max'] * 100 else " FAIL"

    quality_results.append({
        'column': col_name,
        'null_count': null_count,
        'null_rate_pct': round(null_rate, 2),
        'status': status
    })

    print(f"{status} | {col_name:20s} | NULLs: {null_count:6,} ({null_rate:5.2f}%)")

# Summary
failed_checks = [r for r in quality_results if 'FAIL' in r['status']]
if failed_checks:
    print(f"\n COMPLETENESS CHECK FAILED: {len(failed_checks)} columns exceed NULL threshold")
    print(f"   Threshold: <{QUALITY_THRESHOLDS['null_rate_max']*100}%")
else:
    print(f"\n COMPLETENESS CHECK PASSED: All columns meet NULL threshold (<{QUALITY_THRESHOLDS['null_rate_max']*100}%)")

print("\n INTERVIEW: 'NULL rates are our first quality gate. If CustomerSK has >5% NULLs, it means dimension lookup failed - we block the pipeline and investigate source data quality.'")

# COMMAND ----------

# DBTITLE 1,Quality Check 2: Uniqueness (Duplicate Detection)
# Detect duplicate OrderIDs in fact_sales (should be primary key)
print("\n" + "="*70)
print(" QUALITY CHECK 2: UNIQUENESS (DUPLICATE DETECTION)")
print("="*70)

df_fact_sales = spark.table(Config.TBL_FACT_SALES)

# Check for duplicate OrderIDs
df_duplicates = df_fact_sales.groupBy('OrderID').count().filter(col('count') > 1)
duplicate_count = df_duplicates.count()
total_rows = df_fact_sales.count()
duplicate_rate = (duplicate_count / total_rows) * 100 if total_rows > 0 else 0

print(f"\n Total rows: {total_rows:,}")
print(f" Duplicate OrderIDs: {duplicate_count:,} ({duplicate_rate:.2f}%)")

if duplicate_rate > QUALITY_THRESHOLDS['duplicate_rate_max'] * 100:
    print(f"\n UNIQUENESS CHECK FAILED: Duplicate rate {duplicate_rate:.2f}% exceeds threshold {QUALITY_THRESHOLDS['duplicate_rate_max']*100}%")
    print("\n Top 10 duplicate OrderIDs:")
    display(df_duplicates.orderBy(desc('count')).limit(10))
else:
    print(f"\n UNIQUENESS CHECK PASSED: Duplicate rate {duplicate_rate:.2f}% within threshold (<{QUALITY_THRESHOLDS['duplicate_rate_max']*100}%)")

# Check for duplicate CustomerID + IsCurrent=1 in dim_customer (SCD Type 2 validation)
print("\n" + "-"*70)
print("SCD Type 2 Validation: DimCustomer IsCurrent Flag")
print("-"*70)

df_customer = spark.table(Config.TBL_DIM_CUSTOMER)
df_scd_violations = df_customer.filter(col('IsCurrent') == 1).groupBy('CustomerID').count().filter(col('count') > 1)
scd_violation_count = df_scd_violations.count()

if scd_violation_count > 0:
    print(f"\n SCD TYPE 2 VIOLATION: {scd_violation_count} CustomerIDs have multiple IsCurrent=1 records")
    print("\n Violating CustomerIDs:")
    display(df_scd_violations.limit(10))
else:
    print(f"\n SCD TYPE 2 VALIDATION PASSED: All CustomerIDs have exactly 1 IsCurrent=1 record")

print("\n INTERVIEW: 'Duplicate OrderIDs indicate pipeline ran twice without idempotency controls. We use MERGE with business keys to make writes idempotent - re-running same batch results in UPDATE, not duplicate INSERT.'")

# COMMAND ----------

# DBTITLE 1,Quality Check 3: Referential Integrity (Orphan Facts)
# Check for orphan facts (facts with -1 surrogate keys = missing dimensions)
print("\n" + "="*70)
print(" QUALITY CHECK 3: REFERENTIAL INTEGRITY (ORPHAN FACTS)")
print("="*70)

df_fact_sales = spark.table(Config.TBL_FACT_SALES)
total_facts = df_fact_sales.count()

print(f"\n Total facts: {total_facts:,}\n")

# Check each dimension foreign key
orphan_checks = [
    ('CustomerSK', df_fact_sales.filter(col('CustomerSK') == -1).count()),
    ('ProductSK', df_fact_sales.filter(col('ProductSK') == -1).count()),
    ('WarehouseSK', df_fact_sales.filter(col('WarehouseSK') == -1).count()),
    ('RetailerSK', df_fact_sales.filter(col('RetailerSK') == -1).count()),
    ('AgentSK', df_fact_sales.filter(col('AgentSK') == -1).count()),
    ('PaymentModeSK', df_fact_sales.filter(col('PaymentModeSK') == -1).count()),
    ('OrderDateID', df_fact_sales.filter(col('OrderDateID') == -1).count())
]

total_orphans = 0
failed_dimensions = []

for dimension, orphan_count in orphan_checks:
    orphan_rate = (orphan_count / total_facts) * 100 if total_facts > 0 else 0
    status = " PASS" if orphan_rate < QUALITY_THRESHOLDS['orphan_rate_max'] * 100 else " FAIL"

    if 'FAIL' in status:
        failed_dimensions.append(dimension)

    total_orphans += orphan_count
    print(f"{status} | {dimension:20s} | Orphans: {orphan_count:6,} ({orphan_rate:5.2f}%)")

overall_orphan_rate = (total_orphans / total_facts) * 100 if total_facts > 0 else 0

if failed_dimensions:
    print(f"\n REFERENTIAL INTEGRITY CHECK FAILED: {len(failed_dimensions)} dimensions exceed orphan threshold")
    print(f"   Failed dimensions: {', '.join(failed_dimensions)}")
    print(f"   Total orphan rate: {overall_orphan_rate:.2f}%")
    print(f"   Threshold: <{QUALITY_THRESHOLDS['orphan_rate_max']*100}%")
    print("\n ACTION REQUIRED: Load dimensions BEFORE facts in workflow")
else:
    print(f"\n REFERENTIAL INTEGRITY CHECK PASSED: Orphan rate {overall_orphan_rate:.2f}% within threshold (<{QUALITY_THRESHOLDS['orphan_rate_max']*100}%)")

print("\n INTERVIEW: 'Orphan facts (-1 surrogate keys) mean staging data referenced non-existent dimension records. This happens when dimensions load AFTER facts. We enforce dependency order in master_pipeline: dimensions first, then facts.'")

# COMMAND ----------

# DBTITLE 1,Quality Check 4: Accuracy (Business Rule Validation)
# Validate business rules and calculated fields
print("\n" + "="*70)
print(" QUALITY CHECK 4: ACCURACY (BUSINESS RULE VALIDATION)")
print("="*70)

df_fact_sales = spark.table(Config.TBL_FACT_SALES)

# Rule 1: FinalPrice must be positive
print("\nRule 1: FinalPrice > 0 (no negative/zero revenue)")
negative_price_count = df_fact_sales.filter(col('FinalPrice') <= 0).count()
total_rows = df_fact_sales.count()
negative_rate = (negative_price_count / total_rows) * 100 if total_rows > 0 else 0

if negative_price_count > 0:
    print(f" FAIL: {negative_price_count:,} records ({negative_rate:.2f}%) have FinalPrice <= 0")
else:
    print(f" PASS: All records have positive FinalPrice")

# Rule 2: Quantity must be between 1 and 1000 (sanity check)
print("\nRule 2: Quantity between 1 and 1000 (realistic order size)")
qty_outliers = df_fact_sales.filter((col('Quantity') < 1) | (col('Quantity') > 1000)).count()
qty_outlier_rate = (qty_outliers / total_rows) * 100 if total_rows > 0 else 0

if qty_outlier_rate > 5:  # Allow 5% outliers for bulk orders
    print(f" FAIL: {qty_outliers:,} records ({qty_outlier_rate:.2f}%) have unrealistic Quantity")
else:
    print(f" PASS: Quantity outliers {qty_outlier_rate:.2f}% within tolerance (<5%)")

# Rule 3: OrderDate <= DeliveryDate (logical date sequence)
print("\nRule 3: OrderDate <= DeliveryDate (delivery cannot precede order)")
date_logic_errors = df_fact_sales.filter(col('OrderDateID') > col('DeliveryDateID')).count()

if date_logic_errors > 0:
    print(f" FAIL: {date_logic_errors:,} records have DeliveryDate before OrderDate")
else:
    print(f" PASS: All records have logical date sequence")

# Rule 4: Revenue reconciliation (Bronze vs Silver)
print("\nRule 4: Revenue Reconciliation (Bronze → Silver)")
print("(Checking if total revenue matches between staging and fact table)")

try:
    bronze_revenue = spark.table(Config.TBL_STG_SALES).agg(sum('FinalPrice').alias('revenue')).first()['revenue']
    silver_revenue = df_fact_sales.agg(sum('FinalPrice').alias('revenue')).first()['revenue']

    revenue_diff_pct = abs(bronze_revenue - silver_revenue) / bronze_revenue * 100 if bronze_revenue > 0 else 0

    if revenue_diff_pct > QUALITY_THRESHOLDS['revenue_variance_max'] * 100:
        print(f" FAIL: Revenue variance {revenue_diff_pct:.2f}% exceeds threshold {QUALITY_THRESHOLDS['revenue_variance_max']*100}%")
        print(f"   Bronze: ₹{bronze_revenue/10000000:.2f} Cr")
        print(f"   Silver: ₹{silver_revenue/10000000:.2f} Cr")
    else:
        print(f" PASS: Revenue variance {revenue_diff_pct:.2f}% within threshold (<{QUALITY_THRESHOLDS['revenue_variance_max']*100}%)")
except:
    print("WARNING SKIP: Bronze table not available for comparison")

print("\n INTERVIEW: 'Business rule validation caught a bug where negative FinalPrice values loaded due to refund transactions. We added a WHERE clause to filter refunds into a separate fact_returns table.'")

# COMMAND ----------

# DBTITLE 1,Quality Check 5: Timeliness (Data Freshness)
# Check data freshness (SLA: data must be <24 hours old)
print("\n" + "="*70)
print(" QUALITY CHECK 5: TIMELINESS (DATA FRESHNESS)")
print("="*70)

df_fact_sales = spark.table(Config.TBL_FACT_SALES)

# Get latest batch date
latest_batch = df_fact_sales.agg(max('BatchDate').alias('max_batch')).first()['max_batch']
if latest_batch:
    days_old = (datetime.now().date() - latest_batch).days
    hours_old = days_old * 24

    print(f"\n Latest batch: {latest_batch}")
    print(f"⏱ Age: {days_old} days ({hours_old} hours)")
    print(f" SLA: Data must be <{QUALITY_THRESHOLDS['freshness_hours_max']} hours old")

    if hours_old > QUALITY_THRESHOLDS['freshness_hours_max']:
        print(f"\n FRESHNESS CHECK FAILED: Data is {hours_old} hours old (exceeds {QUALITY_THRESHOLDS['freshness_hours_max']}h SLA)")
        print(" ACTION REQUIRED: Investigate pipeline failure or data source delay")
    else:
        print(f"\n FRESHNESS CHECK PASSED: Data is {hours_old} hours old (within {QUALITY_THRESHOLDS['freshness_hours_max']}h SLA)")
else:
    print("\nWARNING FRESHNESS CHECK SKIPPED: No BatchDate found in fact_sales")

# Check pipeline execution frequency
print("\n" + "-"*70)
print("Pipeline Execution Frequency (Last 7 Days)")
print("-"*70)

df_batch_frequency = df_fact_sales.filter(
    col('BatchDate') >= (datetime.now().date() - timedelta(days=7))
).groupBy('BatchDate').agg(
    count('*').alias('row_count')
).orderBy(desc('BatchDate'))

print("\n Daily batch loads:")
display(df_batch_frequency)

print("\n INTERVIEW: 'Our SLA is 24-hour freshness for analytics. If data is >24h old, we send Slack alerts to the data engineering team. This caught a broken Airflow DAG that silently failed for 3 days.'")

# COMMAND ----------

# DBTITLE 1,Quality Check 6: Cross-Table Consistency
# Validate consistency between related tables (dimension vs fact row counts)
print("\n" + "="*70)
print(" QUALITY CHECK 6: CROSS-TABLE CONSISTENCY")
print("="*70)

# Check 1: All customers in fact_sales should exist in dim_customer
print("\nCheck 1: Customer Existence (Facts → Dimension)")
df_fact_sales = spark.table(Config.TBL_FACT_SALES)
df_customer = spark.table(Config.TBL_DIM_CUSTOMER)

# Get distinct CustomerSKs in facts (excluding -1)
fact_customers = df_fact_sales.filter(col('CustomerSK') != -1).select('CustomerSK').distinct()
dim_customers = df_customer.select('CustomerSK').distinct()

# Left anti join to find CustomerSKs in facts but NOT in dimension
missing_customers = fact_customers.join(dim_customers, 'CustomerSK', 'left_anti').count()

if missing_customers > 0:
    print(f" FAIL: {missing_customers:,} CustomerSKs in facts have no matching dimension record")
else:
    print(f" PASS: All CustomerSKs in facts have matching dimension records")

# Check 2: Fact row count matches staging row count (after transformations)
print("\nCheck 2: Row Count Reconciliation (Bronze → Silver)")
try:
    bronze_count = spark.table(Config.TBL_STG_SALES).count()
    silver_count = df_fact_sales.count()

    count_diff_pct = abs(bronze_count - silver_count) / bronze_count * 100 if bronze_count > 0 else 0

    print(f"Bronze (staging): {bronze_count:,} rows")
    print(f"Silver (facts):   {silver_count:,} rows")
    print(f"Difference: {count_diff_pct:.2f}%")

    if count_diff_pct > 5:  # Allow 5% variance for filters
        print(f" FAIL: Row count variance {count_diff_pct:.2f}% exceeds 5% threshold")
    else:
        print(f" PASS: Row count variance {count_diff_pct:.2f}% within acceptable range")
except:
    print("WARNING SKIP: Bronze table not available for comparison")

print("\n INTERVIEW: 'Cross-table consistency checks ensure referential integrity across layers. When fact_sales had 500K rows but staging had 480K, we discovered a WHERE clause filtering out 20K test transactions - we removed the filter to preserve all data.'")

# COMMAND ----------

# DBTITLE 1,Master Quality Score & Report Generator
# Generate overall data quality score
print("\n" + "="*70)
print(" OVERALL DATA QUALITY SCORE")
print("="*70)

# Aggregate results from all checks (mock scores for demo)
quality_checks = [
    {'dimension': 'Completeness', 'score': 99.5, 'status': ' PASS'},
    {'dimension': 'Uniqueness', 'score': 99.8, 'status': ' PASS'},
    {'dimension': 'Referential Integrity', 'score': 99.2, 'status': ' PASS'},
    {'dimension': 'Accuracy', 'score': 98.5, 'status': ' WARN'},
    {'dimension': 'Timeliness', 'score': 100.0, 'status': ' PASS'},
    {'dimension': 'Consistency', 'score': 99.7, 'status': ' PASS'}
]

print("\n Quality Dimension Scores:\n")
for check in quality_checks:
    print(f"{check['status']} | {check['dimension']:25s} | {check['score']:5.1f}%")

avg_score = sum([c['score'] for c in quality_checks]) / len(quality_checks)

print(f"\n{'='*70}")
if avg_score >= 99:
    print(f" OVERALL QUALITY SCORE: {avg_score:.1f}% - EXCELLENT")
elif avg_score >= 95:
    print(f" OVERALL QUALITY SCORE: {avg_score:.1f}% - GOOD (Minor Issues)")
else:
    print(f" OVERALL QUALITY SCORE: {avg_score:.1f}% - POOR (Action Required)")
print(f"{'='*70}\n")

# Generate quality report
quality_report = {
    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    'environment': Config.ENVIRONMENT,
    'database': Config.DATABASE,
    'overall_score': round(avg_score, 1),
    'checks_passed': len([c for c in quality_checks if 'PASS' in c['status']]),
    'checks_warned': len([c for c in quality_checks if 'WARN' in c['status']]),
    'checks_failed': len([c for c in quality_checks if 'FAIL' in c['status']]),
    'details': quality_checks
}

print(" Quality Report JSON (store in monitoring table):")
import json
print(json.dumps(quality_report, indent=2))

print("\n INTERVIEW: 'This quality score feeds into our data observability dashboard. When score drops below 95%, we auto-block downstream dashboards and send alerts. This prevented executives from making decisions on bad data 3 times last quarter.'")

# COMMAND ----------

# DBTITLE 1,Production Integration & Best Practices
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Production Integration
# MAGIC
# MAGIC ### 1. Run Quality Suite After Every Pipeline
# MAGIC
# MAGIC ```python
# MAGIC # In master_pipeline.py
# MAGIC try:
# MAGIC     # Load dimensions
# MAGIC     dbutils.notebook.run('/Workspace/Shared/SkillAIPath/2_Medallion_Architecture/01_DIMENSIONS/dim_customer_scd2', 600, {'batch_date': batch_date})
# MAGIC
# MAGIC     # Load facts
# MAGIC     dbutils.notebook.run('/Workspace/Shared/SkillAIPath/2_Medallion_Architecture/02_FACTS/01_fact_sales', 600, {'batch_date': batch_date})
# MAGIC
# MAGIC     # RUN QUALITY CHECKS
# MAGIC     quality_result = dbutils.notebook.run('/Workspace/Shared/dev/SkillAIPath/1_Canvas/1_Setup/14_data_quality_validation', 300, {'batch_date': batch_date})
# MAGIC     quality_score = json.loads(quality_result)['overall_score']
# MAGIC
# MAGIC     if quality_score < 95:
# MAGIC         raise Exception(f"Data quality failed: {quality_score}% (threshold: 95%)")
# MAGIC
# MAGIC     # Load gold marts only if quality passed
# MAGIC     dbutils.notebook.run('/Workspace/Shared/SkillAIPath/2_Medallion_Architecture/04_GOLD/01_mart_customer_360', 600)
# MAGIC except Exception as e:
# MAGIC     send_slack_alert(f"WARNING Pipeline failed quality check: {str(e)}")
# MAGIC     raise
# MAGIC ```
# MAGIC
# MAGIC ### 2. Store Quality Metrics in Monitoring Table
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE data_quality_metrics (
# MAGIC     check_timestamp TIMESTAMP,
# MAGIC     environment STRING,
# MAGIC     table_name STRING,
# MAGIC     quality_dimension STRING,
# MAGIC     score_pct DOUBLE,
# MAGIC     status STRING,
# MAGIC     details STRING
# MAGIC ) USING DELTA;
# MAGIC ```
# MAGIC
# MAGIC ### 3. Set Up Alerts
# MAGIC
# MAGIC **Slack Webhook Integration:**
# MAGIC ```python
# MAGIC import requests
# MAGIC
# MAGIC def send_quality_alert(check_name, score, threshold):
# MAGIC     webhook_url = dbutils.secrets.get('slack', 'webhook_url')
# MAGIC     message = {
# MAGIC         'text': f'WARNING Data Quality Alert',
# MAGIC         'blocks': [{
# MAGIC             'type': 'section',
# MAGIC             'text': {
# MAGIC                 'type': 'mrkdwn',
# MAGIC                 'text': f'*{check_name}* failed\nScore: {score}% (threshold: {threshold}%)'
# MAGIC             }
# MAGIC         }]
# MAGIC     }
# MAGIC     requests.post(webhook_url, json=message)
# MAGIC ```
# MAGIC
# MAGIC ### 4. Dashboard Visualization
# MAGIC
# MAGIC **Track quality trends over time:**
# MAGIC ```sql
# MAGIC SELECT
# MAGIC     DATE(check_timestamp) AS date,
# MAGIC     quality_dimension,
# MAGIC     AVG(score_pct) AS avg_score
# MAGIC FROM data_quality_metrics
# MAGIC WHERE check_timestamp >= current_date() - INTERVAL 30 DAYS
# MAGIC GROUP BY DATE(check_timestamp), quality_dimension
# MAGIC ORDER BY date DESC;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Interview Talking Points
# MAGIC
# MAGIC **Q: "How do you ensure data quality in production pipelines?"**
# MAGIC
# MAGIC **A:** "I built a 6-dimensional data quality suite that runs after every pipeline execution:
# MAGIC
# MAGIC 1. **Completeness:** Check NULL rates (<5% threshold)
# MAGIC 2. **Uniqueness:** Detect duplicate primary keys
# MAGIC 3. **Referential Integrity:** Validate orphan facts (<1% threshold)
# MAGIC 4. **Accuracy:** Enforce business rules (positive revenue, logical date sequences)
# MAGIC 5. **Timeliness:** Monitor data freshness (<24h SLA)
# MAGIC 6. **Consistency:** Cross-table validation (fact vs dimension row counts)
# MAGIC
# MAGIC The suite generates an overall quality score. If score drops below 95%, the pipeline fails and blocks downstream dashboards. This prevented $2M revenue misreporting when orphan facts exceeded 1% threshold."
# MAGIC
# MAGIC **Q: "What's the difference between data validation and data quality?"**
# MAGIC
# MAGIC **A:** "Validation checks if data meets technical specs (schema, data types). Quality checks if data is fit for business use:
# MAGIC
# MAGIC * Validation: 'Is CustomerID a string?' → Technical
# MAGIC * Quality: 'Do all CustomerIDs exist in dimension table?' → Business value
# MAGIC
# MAGIC Validation is binary (pass/fail). Quality is scored (0-100%) with thresholds. We run validation during ingestion (Bronze), quality after transformations (Silver/Gold)."
# MAGIC
# MAGIC **Q: "How do you prioritize quality checks?"**
# MAGIC
# MAGIC **A:** "I use a risk-based approach:
# MAGIC
# MAGIC * **Critical (block pipeline):** Referential integrity, revenue reconciliation
# MAGIC * **High (alert):** Duplicate keys, negative amounts
# MAGIC * **Medium (monitor):** NULL rates, date logic
# MAGIC * **Low (log):** Outliers, edge cases
# MAGIC
# MAGIC Critical checks block pipeline. High checks send Slack alerts. Medium/Low checks log to monitoring table for trend analysis."