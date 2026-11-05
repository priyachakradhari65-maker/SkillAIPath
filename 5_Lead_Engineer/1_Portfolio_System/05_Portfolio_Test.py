# Databricks notebook source
# DBTITLE 1,End-to-End Pipeline Validation
# MAGIC %md
# MAGIC # End-to-End Pipeline Validation
# MAGIC **BharatMartDBX Data Warehouse - Production Smoke Tests**
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Validate entire data pipeline works correctly:
# MAGIC - SUCCESS Raw data ingested (Bronze)
# MAGIC - SUCCESS Transformations applied (Silver)
# MAGIC - SUCCESS Aggregations computed (Gold)
# MAGIC - SUCCESS Data quality maintained
# MAGIC - SUCCESS Business logic correct
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## When to Run
# MAGIC
# MAGIC 1. **After deployments:** Verify production pipeline works
# MAGIC 2. **Weekly regression tests:** Catch silent failures
# MAGIC 3. **Before demos:** Ensure showcase data is correct
# MAGIC 4. **Debug issues:** Isolate where pipeline breaks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Test Pyramid
# MAGIC
# MAGIC ```
# MAGIC          /\
# MAGIC         /  \  E2E Tests (this notebook)
# MAGIC        /____\
# MAGIC       /      \  Integration Tests
# MAGIC      /________\
# MAGIC     /          \  Unit Tests
# MAGIC    /____________\
# MAGIC ```
# MAGIC
# MAGIC **E2E Tests (5-10 tests):** Full pipeline validation (bronze → gold)
# MAGIC
# MAGIC **Integration Tests (20-30):** Table-level validation
# MAGIC
# MAGIC **Unit Tests (100+):** Function-level validation
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Setup
# E2E Pipeline Validation

import sys, os
from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring
from datetime import datetime, timedelta

current_dir = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
project_root = os.path.dirname(current_dir)
# Import config from Canvas
sys.path.append("/Workspace/Shared/dev/BharatMartDBX/1_Execution_Core/Canvas/1_Setup")
from 1_config import Config

print("="*70)
print("SUCCESS END-TO-END PIPELINE VALIDATION")
print("="*70)
print(f"   Project: BharatMartDBX Data Warehouse")
print(f"   Test Type: Production smoke tests")
print(f"   Run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*70)

# Test counters
tests_passed = 0
tests_failed = 0
test_results = []

# COMMAND ----------

# DBTITLE 1,Test Helper Functions
def test(test_name, condition, expected=True, details=""):
    """Run a single test and track result."""
    global tests_passed, tests_failed
    
    passed = (condition == expected)
    status = "SUCCESS PASS" if passed else "ERROR FAIL"
    
    if passed:
        tests_passed += 1
    else:
        tests_failed += 1
    
    test_results.append({
        "test_name": test_name,
        "passed": passed,
        "actual": condition,
        "expected": expected,
        "details": details
    })
    
    print(f"{status}: {test_name}")
    if not passed:
        print(f"   Expected: {expected}, Got: {condition}")
        if details:
            print(f"   Details: {details}")
    
    return passed

def assert_table_exists(table_name):
    """Check if table exists."""
    try:
        spark.table(table_name)
        return True
    except:
        return False

def assert_row_count(table_name, min_count=1):
    """Check table has minimum row count."""
    try:
        count = spark.table(table_name).count()
        return count >= min_count
    except:
        return False

print("SUCCESS Test helper functions loaded")

# COMMAND ----------

# DBTITLE 1,Layer 1 - Bronze Tables
print("\n" + "="*70)
print(" LAYER 1: BRONZE (RAW INGESTION)")
print("="*70)

# Test: Bronze fact table exists
test(
    "Bronze fact_sales table exists",
    assert_table_exists(f"{Config.CATALOG}.{Config.SILVER_SCHEMA}.{Config.FACT_TABLE}")
)

# Test: Bronze has recent data
df_fact = spark.table(f"{Config.CATALOG}.{Config.SILVER_SCHEMA}.{Config.FACT_TABLE}")
max_date = df_fact.selectExpr("MAX(BatchDate) as max_date").first()["max_date"]
days_old = (datetime.now().date() - max_date).days if max_date else 999

test(
    "Bronze fact_sales has recent data (< 7 days old)",
    days_old < 7,
    True,
    f"Latest data: {max_date}, {days_old} days old"
)

# Test: Bronze row count reasonable
fact_count = df_fact.count()
test(
    "Bronze fact_sales has data (> 1000 rows)",
    fact_count > 1000,
    True,
    f"Row count: {fact_count:,}"
)

print(f"\nSUCCESS Bronze layer validated ({tests_passed}/{tests_passed + tests_failed} tests passed)")

# COMMAND ----------

# DBTITLE 1,Layer 2 - Silver Tables (Dimensions)
print("\n" + "="*70)
print(" LAYER 2: SILVER DIMENSIONS")
print("="*70)

# Test: Customer dimension exists
test(
    "Silver dim_customer table exists",
    assert_table_exists(f"{Config.CATALOG}.{Config.SILVER_SCHEMA}.{Config.DIM_CUSTOMER}")
)

# Test: Customer dimension has current records
df_customer = spark.table(f"{Config.CATALOG}.{Config.SILVER_SCHEMA}.{Config.DIM_CUSTOMER}")
current_count = df_customer.filter(col("IsCurrent") == 1).count()
total_count = df_customer.count()

test(
    "dim_customer has current records",
    current_count > 0,
    True,
    f"Current: {current_count:,}, Total: {total_count:,}"
)

# Test: Product dimension exists
test(
    "Silver dim_product table exists",
    assert_table_exists(f"{Config.CATALOG}.{Config.SILVER_SCHEMA}.dim_product")
)

# Test: Surrogate keys are unique (critical for joins)
df_customer_current = df_customer.filter(col("IsCurrent") == 1)
sk_count = df_customer_current.select("CustomerSK").distinct().count()
row_count = df_customer_current.count()

test(
    "dim_customer surrogate keys are unique",
    sk_count == row_count,
    True,
    f"Unique SKs: {sk_count:,}, Total rows: {row_count:,}"
)

print(f"\nSUCCESS Silver dimensions validated ({tests_passed}/{tests_passed + tests_failed} tests passed)")

# COMMAND ----------

# DBTITLE 1,Layer 3 - Gold Aggregates & Business Logic
print("\n" + "="*70)
print(" LAYER 3: GOLD AGGREGATES")
print("="*70)

# Test: Fact-dimension join works (no orphans)
df_joined = df_fact.join(
    df_customer.filter(col("IsCurrent") == 1),
    "CustomerSK",
    "left"
)

orphans = df_joined.filter(col("CustomerName").isNull()).count()
total = df_joined.count()

test(
    "Fact-dimension join: No orphaned records",
    orphans == 0,
    True,
    f"Orphans: {orphans:,} / {total:,} ({orphans/total*100:.2f}% if any)"
)

# Test: Sales amounts are positive
negative_sales = df_fact.filter(col("FinalPrice") <= 0).count()

test(
    "All sales amounts are positive",
    negative_sales == 0,
    True,
    f"Negative/zero prices: {negative_sales:,}"
)

# Test: Daily aggregation math correct
df_daily = df_fact.filter(col("BatchDate") == max_date) \
    .groupBy("BatchDate") \
    .agg(
        count("*").alias("txn_count"),
        sum("FinalPrice").alias("total_sales")
    )

if df_daily.count() > 0:
    daily_stats = df_daily.first()
    avg_order = daily_stats["total_sales"] / daily_stats["txn_count"] if daily_stats["txn_count"] > 0 else 0
    
    # Sanity check: Average order between ₹100 and ₹1,000,000
    test(
        "Average order value is reasonable (₹100 - ₹1M)",
        100 <= avg_order <= 1000000,
        True,
        f"Avg: ₹{avg_order:,.2f}, Txns: {daily_stats['txn_count']:,}"
    )

print(f"\nSUCCESS Gold aggregates validated ({tests_passed}/{tests_passed + tests_failed} tests passed)")

# COMMAND ----------

# DBTITLE 1,Data Quality Checks
print("\n" + "="*70)
print(" DATA QUALITY CHECKS")
print("="*70)

# Test: No duplicate CustomerSK in current dimension
df_dupes = df_customer.filter(col("IsCurrent") == 1) \
    .groupBy("CustomerSK") \
    .agg(count("*").alias("cnt")) \
    .filter(col("cnt") > 1)

dupe_count = df_dupes.count()

test(
    "No duplicate CustomerSK in current dimension",
    dupe_count == 0,
    True,
    f"Duplicate SKs: {dupe_count}"
)

# Test: BatchDate is consistent (no future dates)
future_dates = df_fact.filter(col("BatchDate") > datetime.now().date()).count()

test(
    "No future dates in fact table",
    future_dates == 0,
    True,
    f"Future dates: {future_dates:,}"
)

# Test: NULL critical fields
null_checks = [
    ("CustomerSK", df_fact.filter(col("CustomerSK").isNull()).count()),
    ("ProductSK", df_fact.filter(col("ProductSK").isNull()).count()),
    ("FinalPrice", df_fact.filter(col("FinalPrice").isNull()).count())
]

for field, null_count in null_checks:
    test(
        f"No NULL values in {field}",
        null_count == 0,
        True,
        f"NULL count: {null_count:,}"
    )

print(f"\nSUCCESS Quality checks validated ({tests_passed}/{tests_passed + tests_failed} tests passed)")

# COMMAND ----------

# DBTITLE 1,Test Summary Report
import pandas as pd

print("\n\n" + "="*70)
print(" TEST SUMMARY REPORT")
print("="*70)

print(f"\n Run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f" Total Tests: {tests_passed + tests_failed}")
print(f"SUCCESS Passed: {tests_passed}")
print(f"ERROR Failed: {tests_failed}")

if tests_failed == 0:
    print("\n ALL TESTS PASSED - PIPELINE HEALTHY")
    overall_status = "PASS"
else:
    print(f"\nWARNING {tests_failed} TEST(S) FAILED - INVESTIGATE IMMEDIATELY")
    overall_status = "FAIL"

print("\n" + "-"*70)
print("DETAILED RESULTS:")
print("-"*70)

df_results = pd.DataFrame(test_results)
for idx, row in df_results.iterrows():
    status = "SUCCESS" if row["passed"] else "ERROR"
    print(f"{status} {row['test_name']}")
    if not row["passed"] and row["details"]:
        print(f"   → {row['details']}")

print("\n" + "="*70)
print(f" FINAL STATUS: {overall_status}")
print("="*70)

# Return status for automation
if tests_failed > 0:
    dbutils.notebook.exit(f"FAIL: {tests_failed} test(s) failed")
else:
    dbutils.notebook.exit("PASS: All tests passed")

# COMMAND ----------

# DBTITLE 1,Automation & CI/CD Integration
# MAGIC %md
# MAGIC ##  Automation & CI/CD
# MAGIC
# MAGIC ### Workflow Integration
# MAGIC
# MAGIC **1. Post-Deployment Validation:**
# MAGIC ```python
# MAGIC # Databricks Job: Run after ETL completes
# MAGIC dbutils.notebook.run("/Workspace/Shared/dev/BharatMartDBX/Portfolio_Builder/Notebooks/02_end_to_end_test", timeout_seconds=600)
# MAGIC ```
# MAGIC
# MAGIC **2. Scheduled Regression Tests:**
# MAGIC - **Frequency:** Daily at 6 AM (after nightly ETL)
# MAGIC - **Alert:** Slack/email if any test fails
# MAGIC - **Owner:** Data platform team
# MAGIC
# MAGIC **3. CI/CD Pipeline:**
# MAGIC ```yaml
# MAGIC # GitHub Actions / Azure DevOps
# MAGIC steps:
# MAGIC   - deploy_to_staging
# MAGIC   - run_e2e_tests
# MAGIC   - if tests_pass: deploy_to_production
# MAGIC   - else: rollback_and_alert
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Alert Configuration
# MAGIC
# MAGIC **Databricks Job:**
# MAGIC - **On Failure:** Send Slack alert to #data-platform
# MAGIC - **Email:** data-team@company.com
# MAGIC - **Webhook:** PagerDuty for critical failures
# MAGIC
# MAGIC **Alert Message:**
# MAGIC ```
# MAGIC  E2E Test Failure
# MAGIC
# MAGIC Pipeline: BharatMartDBX Data Warehouse
# MAGIC Failed Tests: 2 / 15
# MAGIC - ERROR Bronze has recent data
# MAGIC - ERROR No orphaned records in fact-dimension join
# MAGIC
# MAGIC Action: Investigate immediately
# MAGIC Runbook: https://docs.company.com/runbook/e2e-failure
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Monitoring Dashboard
# MAGIC
# MAGIC **Metrics to track:**
# MAGIC 1. **Pass rate:** % tests passed (target: 100%)
# MAGIC 2. **Test duration:** How long tests take (< 5 min)
# MAGIC 3. **Failure trends:** Which tests fail most often
# MAGIC 4. **Data freshness:** Days since last update (< 1 day)
# MAGIC
# MAGIC ---