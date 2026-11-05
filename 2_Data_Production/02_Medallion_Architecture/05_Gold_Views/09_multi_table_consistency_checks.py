# Databricks notebook source
# DBTITLE 1,Multi-Table Consistency - Configuration
# TITLE: Multi-Table Consistency & Referential Integrity Checks
# PURPOSE: Validate data quality across fact and dimension tables in gold layer
# EDITION: Free Edition Compatible
# AUDIENCE: Data Engineers ensuring enterprise data quality standards

# CONSISTENCY CHECKS OVERVIEW:
# 1. Referential Integrity: All foreign keys in facts exist in dimensions
# 2. Metric Reconciliation: Aggregated metrics match across tables
# 3. Temporal Consistency: Date ranges align across facts and dimensions
# 4. Completeness: No unexpected NULLs in critical columns
# 5. Cross-Table Aggregation Validation: Revenue totals match across views

# PRODUCTION NOTES:
# - Schedule: Run after daily gold layer refresh (6 AM)
# - Alerting: Send notifications on any check failures
# - SLA: All checks should complete within 10 minutes
# - Dependencies: Requires all gold layer tables

import sys
sys.path.append('/Workspace/Shared/dev/SkillAIPath/00_UTILS')

%run ../../../1_Canvas/1_Setup/1_config

from pyspark.sql import functions as F
from datetime import datetime
import json

print("Multi-Table Consistency Validation Framework")
print("Gold Layer: SkillAIPath")
print(f"Execution Time: {datetime.now().isoformat()}")

# COMMAND ----------

# DBTITLE 1,Check 1: Referential Integrity (Foreign Keys)
# CELL: Referential Integrity Validation
# Ensure all foreign keys in fact tables have matching dimension records

print("=" * 60)
print("CHECK 1: REFERENTIAL INTEGRITY")
print("=" * 60)

integrity_results = []

# Load tables
fact_sales = spark.table(Config.TBL_FACT_SALES)
dim_customer = spark.table(Config.TBL_DIM_CUSTOMER)
dim_product = spark.table(Config.TBL_DIM_PRODUCT)

print(f"\nFact Sales Records: {fact_sales.count():,}")

# Check 1.1: Customer FK
print("\n--- Checking CustomerSK foreign key ---")
orphaned_customers = fact_sales.select('CustomerSK').distinct() \
    .join(dim_customer.select('CustomerSK'), on='CustomerSK', how='left_anti')

orphan_count = orphaned_customers.count()
if orphan_count == 0:
    print("PASS: All CustomerSK values exist in dim_customer")
    integrity_results.append({'check': 'customer_fk', 'status': 'PASS', 'orphans': 0})
else:
    print(f"FAIL: {orphan_count} orphaned CustomerSK values found")
    orphaned_customers.show(10)
    integrity_results.append({'check': 'customer_fk', 'status': 'FAIL', 'orphans': orphan_count})

# Check 1.2: Product FK
print("\n--- Checking ProductSK foreign key ---")
orphaned_products = fact_sales.select('ProductSK').distinct() \
    .join(dim_product.select('ProductSK'), on='ProductSK', how='left_anti')

orphan_count = orphaned_products.count()
if orphan_count == 0:
    print("PASS: All ProductSK values exist in dim_product")
    integrity_results.append({'check': 'product_fk', 'status': 'PASS', 'orphans': 0})
else:
    print(f"FAIL: {orphan_count} orphaned ProductSK values found")
    orphaned_products.show(10)
    integrity_results.append({'check': 'product_fk', 'status': 'FAIL', 'orphans': orphan_count})

print("\n" + "=" * 60)
failed_checks = [r for r in integrity_results if r['status'] == 'FAIL']
if len(failed_checks) == 0:
    print("REFERENTIAL INTEGRITY: ALL CHECKS PASSED")
else:
    print(f"REFERENTIAL INTEGRITY: {len(failed_checks)} CHECKS FAILED")
print("=" * 60)

# COMMAND ----------

# DBTITLE 1,Check 2: Metric Reconciliation Across Tables
# CELL: Cross-Table Metric Reconciliation
# Verify that aggregated metrics match across different gold layer tables/views

print("=" * 60)
print("CHECK 2: METRIC RECONCILIATION")
print("=" * 60)

metric_results = []

# Load materialized views - test with count() to trigger lazy evaluation
mv_daily_sales = None
mv_product_perf = None

try:
    # Fully qualified table name with catalog
    _temp = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_GOLD.split('.')[-1]}.daily_sales")
    _temp.count()  # Trigger evaluation to check table exists
    mv_daily_sales = _temp
except Exception:
    pass

try:
    _temp = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_GOLD.split('.')[-1]}.product_performance")
    _temp.count()
    mv_product_perf = _temp
except Exception:
    pass

if mv_daily_sales is not None:
    # Check 2.1: Total revenue reconciliation
    print("\n--- Checking total revenue across sources ---")

    # Revenue from fact_sales
    fact_revenue = fact_sales.agg(
        F.sum('finalprice').alias('revenue')
    ).collect()[0]['revenue']

    # Revenue from daily_sales MV
    mv_revenue = mv_daily_sales.agg(
        F.sum('total_sales').alias('revenue')
    ).collect()[0]['revenue']

    # Calculate difference
    if fact_revenue and mv_revenue:
        diff = abs(fact_revenue - mv_revenue)
        diff_pct = (diff / fact_revenue) * 100

        print(f"Fact Sales Revenue: ${fact_revenue:,.2f}")
        print(f"Daily Sales MV Revenue: ${mv_revenue:,.2f}")
        print(f"Difference: ${diff:,.2f} ({diff_pct:.2f}%)")

        # Tolerance: 0.01% (rounding differences acceptable)
        if diff_pct < 0.01:
            print("PASS: Revenue totals match within tolerance")
            metric_results.append({'check': 'revenue_reconciliation', 'status': 'PASS', 'diff_pct': diff_pct})
        else:
            print(f"FAIL: Revenue mismatch exceeds tolerance ({diff_pct:.2f}%)")
            metric_results.append({'check': 'revenue_reconciliation', 'status': 'FAIL', 'diff_pct': diff_pct})
    else:
        print("SKIP: NULL revenue values detected")

    # Check 2.2: Order count reconciliation
    print("\n--- Checking order count consistency ---")
    fact_orders = fact_sales.select('OrderID').distinct().count()
    mv_orders = mv_daily_sales.agg(F.sum('order_count')).collect()[0][0]

    if mv_orders:
        print(f"Fact Sales Orders: {fact_orders:,}")
        print(f"Daily Sales MV Orders: {int(mv_orders):,}")

        if fact_orders == int(mv_orders):
            print("PASS: Order counts match exactly")
            metric_results.append({'check': 'order_count', 'status': 'PASS'})
        else:
            print(f"FAIL: Order count mismatch ({abs(fact_orders - int(mv_orders)):,} difference)")
            metric_results.append({'check': 'order_count', 'status': 'FAIL'})
else:
    print("\nSKIP: Materialized views not found - cannot validate metric reconciliation")

print("\n" + "=" * 60)
failed_metrics = [r for r in metric_results if r['status'] == 'FAIL']
if len(failed_metrics) == 0:
    print("METRIC RECONCILIATION: ALL CHECKS PASSED")
else:
    print(f"METRIC RECONCILIATION: {len(failed_metrics)} CHECKS FAILED")
print("=" * 60)

# COMMAND ----------

# DBTITLE 1,Check 3: Temporal Consistency (Date Ranges)
# CELL: Temporal Consistency Validation
# Verify date ranges align across facts and dimensions

print("=" * 60)
print("CHECK 3: TEMPORAL CONSISTENCY")
print("=" * 60)

temporal_results = []

# Check 3.1: Order dates within valid range
print("\n--- Checking BatchDate validity ---")
fact_date_range = fact_sales.agg(
    F.min('BatchDate').alias('min_date'),
    F.max('BatchDate').alias('max_date')
).collect()[0]

min_date = fact_date_range['min_date']
max_date = fact_date_range['max_date']

print(f"Order Date Range: {min_date} to {max_date}")

# Validate dates are not in future
from datetime import date
today = date.today()

if max_date <= today:
    print("PASS: No future order dates")
    temporal_results.append({'check': 'future_dates', 'status': 'PASS'})
else:
    print(f"FAIL: Future order dates detected (max: {max_date})")
    temporal_results.append({'check': 'future_dates', 'status': 'FAIL'})

# Check 3.2: Customer registration dates before order dates
print("\n--- Checking StartDate vs BatchDate ---")
invalid_dates = fact_sales.alias('f').join(
    dim_customer.alias('c'),
    F.col('f.CustomerSK') == F.col('c.CustomerSK')
).filter(
    F.col('f.BatchDate') < F.col('c.StartDate')
)

invalid_count = invalid_dates.count()
if invalid_count == 0:
    print("PASS: All orders after customer registration")
    temporal_results.append({'check': 'registration_vs_order', 'status': 'PASS'})
else:
    print(f"FAIL: {invalid_count} orders before customer registration")
    invalid_dates.select(F.col('f.CustomerSK'), F.col('f.BatchDate'), F.col('c.StartDate')).show(10)
    temporal_results.append({'check': 'registration_vs_order', 'status': 'FAIL', 'violations': invalid_count})

# Check 3.3: No NULL critical dates
print("\n--- Checking for NULL date values ---")
null_order_dates = fact_sales.filter(F.col('BatchDate').isNull()).count()
null_reg_dates = dim_customer.filter(F.col('StartDate').isNull()).count()

print(f"NULL BatchDate: {null_order_dates:,}")
print(f"NULL StartDate: {null_reg_dates:,}")

if null_order_dates == 0 and null_reg_dates == 0:
    print("PASS: No NULL critical dates")
    temporal_results.append({'check': 'null_dates', 'status': 'PASS'})
else:
    print("FAIL: NULL dates detected in critical columns")
    temporal_results.append({'check': 'null_dates', 'status': 'FAIL'})

print("\n" + "=" * 60)
failed_temporal = [r for r in temporal_results if r['status'] == 'FAIL']
if len(failed_temporal) == 0:
    print("TEMPORAL CONSISTENCY: ALL CHECKS PASSED")
else:
    print(f"TEMPORAL CONSISTENCY: {len(failed_temporal)} CHECKS FAILED")
print("=" * 60)

# COMMAND ----------

# DBTITLE 1,Check 4: Completeness (NULL Analysis)
# CELL: Data Completeness Validation
# Check for unexpected NULLs in critical columns

print("=" * 60)
print("CHECK 4: DATA COMPLETENESS")
print("=" * 60)

completeness_results = []

# Define critical columns that should never be NULL
critical_columns = {
    Config.TBL_FACT_SALES: ['OrderID', 'CustomerSK', 'ProductSK', 'BatchDate', 'ProductQuantity', 'finalprice'],
    Config.TBL_DIM_CUSTOMER: ['CustomerSK', 'CustomerName'],
    Config.TBL_DIM_PRODUCT: ['ProductSK', 'ProductName']
}

for table_name, columns in critical_columns.items():
    print(f"\n--- Checking {table_name} ---")

    try:
        table_df = spark.table(table_name)
        total_rows = table_df.count()

        table_issues = []

        for col in columns:
            null_count = table_df.filter(F.col(col).isNull()).count()
            null_pct = (null_count / total_rows) * 100 if total_rows > 0 else 0

            if null_count > 0:
                print(f"  {col}: {null_count:,} NULLs ({null_pct:.2f}%)")
                table_issues.append({'column': col, 'null_count': null_count, 'null_pct': null_pct})
            else:
                print(f"  {col}: 0 NULLs")

        if len(table_issues) == 0:
            print(f"  PASS: No NULLs in critical columns")
            completeness_results.append({'table': table_name, 'status': 'PASS'})
        else:
            print(f"  FAIL: {len(table_issues)} columns have NULLs")
            completeness_results.append({'table': table_name, 'status': 'FAIL', 'issues': table_issues})

    except Exception as e:
        print(f"  ERROR: {str(e)}")
        completeness_results.append({'table': table_name, 'status': 'ERROR', 'error': str(e)})

print("\n" + "=" * 60)
failed_completeness = [r for r in completeness_results if r['status'] == 'FAIL']
if len(failed_completeness) == 0:
    print("DATA COMPLETENESS: ALL CHECKS PASSED")
else:
    print(f"DATA COMPLETENESS: {len(failed_completeness)} TABLES WITH ISSUES")
print("=" * 60)

# COMMAND ----------

# DBTITLE 1,Check 5: Business Rule Validation
# CELL: Business Rule Consistency
# Validate domain-specific business logic

print("=" * 60)
print("CHECK 5: BUSINESS RULE VALIDATION")
print("=" * 60)

business_results = []

# Rule 5.1: Quantity must be positive
print("\n--- Checking ProductQuantity > 0 ---")
invalid_qty = fact_sales.filter(F.col('ProductQuantity') <= 0)
invalid_qty_count = invalid_qty.count()

if invalid_qty_count == 0:
    print("PASS: All quantities are positive")
    business_results.append({'rule': 'positive_quantity', 'status': 'PASS'})
else:
    print(f"FAIL: {invalid_qty_count} orders with ProductQuantity <= 0")
    invalid_qty.select('OrderID', 'ProductQuantity').show(10)
    business_results.append({'rule': 'positive_quantity', 'status': 'FAIL', 'violations': invalid_qty_count})

# Rule 5.2: Selling price must be positive
print("\n--- Checking sellingPrice > 0 ---")
invalid_price = fact_sales.filter(F.col('sellingPrice') <= 0)
invalid_price_count = invalid_price.count()

if invalid_price_count == 0:
    print("PASS: All selling prices are positive")
    business_results.append({'rule': 'positive_unit_price', 'status': 'PASS'})
else:
    print(f"FAIL: {invalid_price_count} orders with sellingPrice <= 0")
    invalid_price.select('OrderID', 'ProductSK', 'sellingPrice').show(10)
    business_results.append({'rule': 'positive_unit_price', 'status': 'FAIL', 'violations': invalid_price_count})

# Rule 5.3: Final price validation (after discount)
print("\n--- Checking finalprice > 0 ---")
# finalprice should be positive (it's selling price after discount)
invalid_total = fact_sales.filter(F.col('finalprice') <= 0)

invalid_total_count = invalid_total.count()
if invalid_total_count == 0:
    print("PASS: All finalprice values are positive")
    business_results.append({'rule': 'positive_finalprice', 'status': 'PASS'})
else:
    print(f"FAIL: {invalid_total_count} orders with finalprice <= 0")
    invalid_total.select('OrderID', 'sellingPrice', 'discount', 'finalprice').show(10)
    business_results.append({'rule': 'positive_finalprice', 'status': 'FAIL', 'violations': invalid_total_count})

# Rule 5.4: No duplicate OrderID
print("\n--- Checking for duplicate OrderID ---")
duplicate_orders = fact_sales.groupBy('OrderID').count().filter(F.col('count') > 1)
duplicate_count = duplicate_orders.count()

if duplicate_count == 0:
    print("PASS: No duplicate OrderID values")
    business_results.append({'rule': 'unique_order_id', 'status': 'PASS'})
else:
    print(f"FAIL: {duplicate_count} duplicate OrderID values found")
    duplicate_orders.show(10)
    business_results.append({'rule': 'unique_order_id', 'status': 'FAIL', 'violations': duplicate_count})

print("\n" + "=" * 60)
failed_business = [r for r in business_results if r['status'] == 'FAIL']
if len(failed_business) == 0:
    print("BUSINESS RULE VALIDATION: ALL CHECKS PASSED")
else:
    print(f"BUSINESS RULE VALIDATION: {len(failed_business)} RULES VIOLATED")
print("=" * 60)

# COMMAND ----------

# DBTITLE 1,Generate Consistency Check Report
# CELL: Generate Final Consistency Report
# Aggregate all check results and log for monitoring

import json
from datetime import datetime

print("=" * 60)
print("MULTI-TABLE CONSISTENCY REPORT")
print("=" * 60)

# Aggregate all results
all_results = {
    'timestamp': datetime.now().isoformat(),
    'checks': {
        'referential_integrity': integrity_results,
        'metric_reconciliation': metric_results,
        'temporal_consistency': temporal_results,
        'data_completeness': completeness_results,
        'business_rules': business_results
    }
}

# Calculate summary statistics
total_checks = (
    len(integrity_results) +
    len(metric_results) +
    len(temporal_results) +
    len(completeness_results) +
    len(business_results)
)

failed_checks = (
    len([r for r in integrity_results if r['status'] == 'FAIL']) +
    len([r for r in metric_results if r['status'] == 'FAIL']) +
    len([r for r in temporal_results if r['status'] == 'FAIL']) +
    len([r for r in completeness_results if r['status'] == 'FAIL']) +
    len([r for r in business_results if r['status'] == 'FAIL'])
)

passed_checks = total_checks - failed_checks
pass_rate = (passed_checks / total_checks) * 100 if total_checks > 0 else 0

print(f"\nExecution Time: {all_results['timestamp']}")
print(f"\nTotal Checks: {total_checks}")
print(f"Passed: {passed_checks}")
print(f"Failed: {failed_checks}")
print(f"Pass Rate: {pass_rate:.1f}%")

# Check category breakdown
print("\n--- Category Breakdown ---")
for category, results in all_results['checks'].items():
    category_failed = len([r for r in results if r.get('status') == 'FAIL'])
    category_total = len(results)
    print(f"{category}: {category_total - category_failed}/{category_total} passed")

# Overall status
print("\n" + "=" * 60)
if failed_checks == 0:
    print("OVERALL STATUS: ALL CHECKS PASSED")
    print("Gold layer data quality: EXCELLENT")
else:
    print(f"OVERALL STATUS: {failed_checks} CHECKS FAILED")
    print("Gold layer data quality: NEEDS ATTENTION")
    print("\nFailed Checks:")
    for category, results in all_results['checks'].items():
        for result in results:
            if result.get('status') == 'FAIL':
                print(f"  - {category}: {result}")
print("=" * 60)

# Optionally log to monitoring table
# report_df = spark.createDataFrame([{
#     'check_timestamp': all_results['timestamp'],
#     'total_checks': total_checks,
#     'passed_checks': passed_checks,
#     'failed_checks': failed_checks,
#     'pass_rate_pct': pass_rate,
#     'check_details': json.dumps(all_results['checks'])
# }])
# report_df.write.mode('append').saveAsTable(f'{Config.SCHEMA_MONITORING}.consistency_check_log')

# COMMAND ----------

# DBTITLE 1,Production Deployment Notes
# MAGIC %md
# MAGIC # Multi-Table Consistency Checks - Production Guide
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC This framework validates data quality across the entire gold layer, catching:
# MAGIC - **Data pipeline bugs** (missing dimension updates)
# MAGIC - **ETL logic errors** (incorrect aggregations)
# MAGIC - **Business rule violations** (negative quantities, wrong calculations)
# MAGIC - **Temporal anomalies** (future dates, registration-after-order)
# MAGIC
# MAGIC ## Scheduling
# MAGIC
# MAGIC **Recommended Schedule:**
# MAGIC ```
# MAGIC Frequency: Daily (after gold layer refresh)
# MAGIC Timing: 6 AM (after all upstream jobs complete)
# MAGIC Dependencies: Run AFTER all gold MVs refreshed
# MAGIC Timeout: 15 minutes
# MAGIC Retries: 1 (with 5-minute delay)
# MAGIC Alerts: Email + Slack on ANY check failure
# MAGIC ```
# MAGIC
# MAGIC ## Alerting Configuration
# MAGIC
# MAGIC **Critical Failures (Immediate Alert):**
# MAGIC - Referential integrity violations (orphaned FKs)
# MAGIC - Metric reconciliation failures > 1%
# MAGIC - Business rule violations (negative amounts, duplicate orders)
# MAGIC
# MAGIC **Warning Failures (Review in AM):**
# MAGIC - NULL values in non-critical columns
# MAGIC - Temporal anomalies < 10 violations
# MAGIC
# MAGIC **Alert Channels:**
# MAGIC ```python
# MAGIC # Databricks Job Alert Settings
# MAGIC if failed_checks > 0:
# MAGIC     - Email: data-eng-team@company.com
# MAGIC     - Slack: #data-quality-alerts
# MAGIC     - PagerDuty: For production-critical failures
# MAGIC ```
# MAGIC
# MAGIC ## Performance Optimization
# MAGIC
# MAGIC **For Large Fact Tables (> 100M rows):**
# MAGIC 1. Use incremental checks (filter by process_date)
# MAGIC 2. Sample for business rule checks (10% sample acceptable)
# MAGIC 3. Cache dimension tables before FK checks
# MAGIC 4. Parallelize independent checks
# MAGIC
# MAGIC **Estimated Runtime:**
# MAGIC - Small dataset (< 1M rows): 2-3 minutes
# MAGIC - Medium dataset (1-10M rows): 5-8 minutes
# MAGIC - Large dataset (> 10M rows): 10-15 minutes
# MAGIC
# MAGIC ## Cost Considerations
# MAGIC
# MAGIC **Compute Costs:**
# MAGIC - Daily consistency checks: ~0.5-1 DBU
# MAGIC - One-time execution: Negligible (< 0.1 DBU)
# MAGIC - Use shared cluster if available (reduce cold start cost)
# MAGIC
# MAGIC ## Remediation Runbook
# MAGIC
# MAGIC **When Check Fails:**
# MAGIC
# MAGIC 1. **Referential Integrity Failure:**
# MAGIC    - Investigate dimension refresh job (did it run?)
# MAGIC    - Check for upstream source data issues
# MAGIC    - Manually add missing dimension records if urgent
# MAGIC    - Re-run fact load after dimension fix
# MAGIC
# MAGIC 2. **Metric Reconciliation Failure:**
# MAGIC    - Compare MV definition with fact table logic
# MAGIC    - Check for timezone/date conversion issues
# MAGIC    - Refresh MV manually: `REFRESH MATERIALIZED VIEW <view_name>`
# MAGIC    - Investigate stale MV refresh schedule
# MAGIC
# MAGIC 3. **Temporal Consistency Failure:**
# MAGIC    - Review source data extraction logic
# MAGIC    - Check for timezone issues in ingestion
# MAGIC    - Quarantine invalid records (move to error table)
# MAGIC
# MAGIC 4. **Business Rule Violation:**
# MAGIC    - Trace back to source system
# MAGIC    - Coordinate with upstream data owners
# MAGIC    - Implement validation in bronze/silver layers
# MAGIC
# MAGIC ## Testing Checklist
# MAGIC
# MAGIC - [ ] All checks complete within timeout (15 min)
# MAGIC - [ ] Failed check generates alert
# MAGIC - [ ] Report logs to monitoring table
# MAGIC - [ ] Re-run is idempotent (same results)
# MAGIC - [ ] Works on empty tables (no errors)
# MAGIC - [ ] Catches intentionally injected errors
# MAGIC
# MAGIC ## Free Edition Compatibility
# MAGIC
# MAGIC - All checks work in Community Edition
# MAGIC - No Unity Catalog-specific features used
# MAGIC - Replace Config.TBL_* with hardcoded table names if needed
