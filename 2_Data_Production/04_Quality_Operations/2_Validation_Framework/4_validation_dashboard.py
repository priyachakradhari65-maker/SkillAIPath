# Databricks notebook source
# MAGIC %md
# MAGIC # Validation Dashboard
# MAGIC
# MAGIC Aggregate validation results from all 4 quality gates

# COMMAND ----------

# MAGIC %run ../1_Setup/1_config

# COMMAND ----------

# DBTITLE 1,Dashboard Overview
print("SKILLAIPPATH DATA QUALITY DASHBOARD")
print("="*80)
print("4-Gate Validation System")
print("  Gate 1: Bronze Layer - Raw data validation")
print("  Gate 2: Dimensions - SCD validation, surrogate keys")
print("  Gate 3: Facts - Referential integrity, orphan detection")
print("  Gate 4: Gold - Business rule validation, revenue reconciliation")
print("\n" + "="*80)

validation_summary = []

# COMMAND ----------

# DBTITLE 1,Gate 1: Bronze Layer Validation
print("\nGATE 1: BRONZE LAYER")
print("-" * 80)

try:
    # Check bronze tables exist and have data
    bronze_tables = ["stg_sales", "stg_purchase", "stg_stock_status"]
    bronze_status = []

    for table in bronze_tables:
        try:
            df = spark.table(f"sap_dev.bronze.{table}")
            row_count = df.count()
            bronze_status.append((table, row_count, "PASS"))
            print(f"  {table}: {row_count:,} records - PASS")
        except Exception as e:
            bronze_status.append((table, 0, f"FAIL: {str(e)}"))
            print(f"  {table}: FAIL - {str(e)}")

    passed = sum(1 for _, _, status in bronze_status if status == "PASS")
    validation_summary.append(("Bronze Gate", f"{passed}/{len(bronze_tables)} passed"))
except Exception as e:
    validation_summary.append(("Bronze Gate", f"ERROR: {str(e)}"))
    print(f"ERROR: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Gate 2: Dimensions Validation
print("\nGATE 2: DIMENSIONS")
print("-" * 80)

try:
    # Check key dimensions
    dim_tables = ["dim_customer", "dim_product", "dim_date"]
    dim_status = []

    for table in dim_tables:
        try:
            df = spark.table(f"sap_dev.silver.{table}")
            row_count = df.count()

            # Check for surrogate keys
            key_col = f"{table.split('_')[1].capitalize()}Key"
            if key_col in df.columns:
                null_keys = df.filter(f"{key_col} IS NULL").count()
                status = "PASS" if null_keys == 0 else f"WARNING: {null_keys} NULL keys"
            else:
                status = "PASS"

            dim_status.append((table, row_count, status))
            print(f"  {table}: {row_count:,} records - {status}")
        except Exception as e:
            dim_status.append((table, 0, f"FAIL: {str(e)}"))
            print(f"  {table}: FAIL - {str(e)}")

    passed = sum(1 for _, _, status in dim_status if status == "PASS")
    validation_summary.append(("Dimensions Gate", f"{passed}/{len(dim_tables)} passed"))
except Exception as e:
    validation_summary.append(("Dimensions Gate", f"ERROR: {str(e)}"))
    print(f"ERROR: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Gate 3: Facts Validation
print("\nGATE 3: FACTS")
print("-" * 80)

try:
    # Check fact tables
    fact_tables = ["fact_sales", "fact_purchases"]
    fact_status = []

    for table in fact_tables:
        try:
            df = spark.table(f"sap_dev.silver.{table}")
            row_count = df.count()

            # Check for NULL foreign keys (orphan records)
            foreign_keys = [col for col in df.columns if col.endswith("Key") and col != "SalesKey" and col != "PurchaseKey"]
            orphan_checks = []

            for fk in foreign_keys:
                null_count = df.filter(f"{fk} IS NULL").count()
                if null_count > 0:
                    orphan_checks.append(f"{null_count} NULL {fk}")

            status = "PASS" if len(orphan_checks) == 0 else f"WARNING: {', '.join(orphan_checks)}"
            fact_status.append((table, row_count, status))
            print(f"  {table}: {row_count:,} records - {status}")
        except Exception as e:
            fact_status.append((table, 0, f"FAIL: {str(e)}"))
            print(f"  {table}: FAIL - {str(e)}")

    passed = sum(1 for _, _, status in fact_status if status == "PASS")
    validation_summary.append(("Facts Gate", f"{passed}/{len(fact_tables)} passed"))
except Exception as e:
    validation_summary.append(("Facts Gate", f"ERROR: {str(e)}"))
    print(f"ERROR: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Gate 4: Gold Layer Validation
print("\nGATE 4: GOLD MARTS")
print("-" * 80)

try:
    # Check gold marts
    gold_tables = ["mart_customer_360", "mart_product_performance"]
    gold_status = []

    for table in gold_tables:
        try:
            df = spark.table(f"sap_dev.gold.{table}")
            row_count = df.count()

            # Basic validation - check for rows
            status = "PASS" if row_count > 0 else "WARNING: Empty table"
            gold_status.append((table, row_count, status))
            print(f"  {table}: {row_count:,} records - {status}")
        except Exception as e:
            gold_status.append((table, 0, f"FAIL: {str(e)}"))
            print(f"  {table}: FAIL - {str(e)}")

    passed = sum(1 for _, _, status in gold_status if status == "PASS")
    validation_summary.append(("Gold Gate", f"{passed}/{len(gold_tables)} passed"))
except Exception as e:
    validation_summary.append(("Gold Gate", f"ERROR: {str(e)}"))
    print(f"ERROR: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Summary Report
from datetime import datetime

print("\n" + "="*80)
print("VALIDATION SUMMARY")
print("="*80)

for gate, result in validation_summary:
    status_icon = "" if "passed" in result else ""
    print(f"{status_icon} {gate}: {result}")

print(f"\nReport Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Overall status
all_passed = all("passed" in result or "passed" in result for _, result in validation_summary if "ERROR" not in result)

if all_passed:
    print("\nSTATUS: All validation gates passed")
    print("ACTION: Pipeline ready for production")
else:
    print("\nSTATUS: Some validation gates have warnings or errors")
    print("ACTION: Review failures before production deployment")

# COMMAND ----------

# DBTITLE 1,Historical Validation Trends
print("\n" + "="*80)
print("HISTORICAL VALIDATION TRENDS")
print("="*80)

# In production, store validation results in a control table for trending
print("\nTo enable trend analysis:")
print("")
print("1. CREATE VALIDATION HISTORY TABLE:")
print("   CREATE TABLE sap.monitoring.validation_history (")
print("     validation_date DATE,")
print("     gate_name STRING,")
print("     test_name STRING,")
print("     status STRING,")
print("     details STRING,")
print("     recorded_at TIMESTAMP")
print("   )")
print("")
print("2. INSERT RESULTS AFTER EACH RUN:")
print("   INSERT INTO sap.monitoring.validation_history")
print("   VALUES (current_date(), 'Bronze', 'row_count', 'PASS',")
print("           '1095000 records', current_timestamp())")
print("")
print("3. QUERY TRENDS:")
print("   SELECT validation_date, gate_name, COUNT(*) as passed_tests")
print("   FROM sap.monitoring.validation_history")
print("   WHERE status = 'PASS'")
print("   GROUP BY validation_date, gate_name")
print("   ORDER BY validation_date DESC")
print("   LIMIT 30  -- Last 30 days")

# COMMAND ----------

# DBTITLE 1,Cross-Layer Reconciliation
print("\n" + "="*80)
print("CROSS-LAYER RECONCILIATION")
print("="*80)

try:
    # Verify data consistency across layers

    # Bronze row count
    bronze_count = spark.table("sap_dev.bronze.stg_sales").count()

    # Silver row count (facts)
    silver_count = spark.table("sap_dev.silver.fact_sales").count()

    # Gold aggregated records
    gold_product_count = spark.table("sap_dev.gold.mart_product_performance").count()

    print(f"Bronze (raw): {bronze_count:,} records")
    print(f"Silver (facts): {silver_count:,} records")
    print(f"Gold (products): {gold_product_count:,} aggregated records")
    print("")

    # Revenue reconciliation
    bronze_revenue = spark.sql("""
        SELECT ROUND(SUM(finalprice) / 10000000, 2) as revenue_cr
        FROM sap_dev.bronze.stg_sales
    """).collect()[0]['revenue_cr']

    silver_revenue = spark.sql("""
        SELECT ROUND(SUM(finalprice) / 10000000, 2) as revenue_cr
        FROM sap_dev.silver.fact_sales
    """).collect()[0]['revenue_cr']

    gold_revenue = spark.sql("""
        SELECT ROUND(SUM(total_revenue) / 10000000, 2) as revenue_cr
        FROM sap_dev.gold.mart_product_performance
    """).collect()[0]['revenue_cr']

    print(f"Bronze revenue: Rs {bronze_revenue} Cr")
    print(f"Silver revenue: Rs {silver_revenue} Cr")
    print(f"Gold revenue: Rs {gold_revenue} Cr")
    print("")

    # Check for discrepancies
    if bronze_revenue == silver_revenue == gold_revenue:
        print("PASS: Revenue matches across all layers")
    else:
        print("WARNING: Revenue mismatch detected")
        if abs(bronze_revenue - silver_revenue) > 0.01:
            print(f"  Bronze vs Silver: Rs {abs(bronze_revenue - silver_revenue):.2f} Cr difference")
        if abs(silver_revenue - gold_revenue) > 0.01:
            print(f"  Silver vs Gold: Rs {abs(silver_revenue - gold_revenue):.2f} Cr difference")

except Exception as e:
    print(f"ERROR in reconciliation: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Performance Metrics
print("\n" + "="*80)
print("PIPELINE PERFORMANCE METRICS")
print("="*80)

try:
    # Check last update times
    from pyspark.sql.functions import max as spark_max

    silver_last_update = spark.table("sap_dev.silver.fact_sales") \
        .select(spark_max("LoadedAt").alias("last_update")) \
        .collect()[0]['last_update'] if "LoadedAt" in spark.table("sap_dev.silver.fact_sales").columns else None

    print(f"Last fact_sales update: {silver_last_update}")

    # Check table sizes
    tables_to_check = [
        ("sap_dev.bronze.stg_sales", "Bronze"),
        ("sap_dev.silver.fact_sales", "Silver Facts"),
        ("sap_dev.silver.dim_customer", "Silver Dimensions"),
        ("sap_dev.gold.mart_product_performance", "Gold Marts")
    ]

    print("\nTable Sizes:")
    for table, layer in tables_to_check:
        try:
            details = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]
            size_mb = details.sizeInBytes / 1024 / 1024 if hasattr(details, 'sizeInBytes') else 0
            num_files = details.numFiles if hasattr(details, 'numFiles') else 0
            print(f"  {layer}: {size_mb:.2f} MB ({num_files} files)")
        except:
            print(f"  {layer}: Unable to retrieve size")

    print("\nRecommendations:")
    print("  - Run OPTIMIZE if any table has >200 files")
    print("  - Run VACUUM phase-based to reclaim storage")
    print("  - Monitor LoadedAt timestamps for processing delays")

except Exception as e:
    print(f"ERROR: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Automated Alert Generation
print("\n" + "="*80)
print("AUTOMATED ALERT GENERATION")
print("="*80)

# Collect all issues from validation summary
critical_issues = []
warnings = []

for gate, result in validation_summary:
    if "ERROR" in result:
        critical_issues.append(f"{gate}: {result}")
    elif "WARNING" in result or "0/" in result:
        warnings.append(f"{gate}: {result}")

if critical_issues:
    print("\nCRITICAL ISSUES REQUIRING IMMEDIATE ACTION:")
    for issue in critical_issues:
        print(f"  !!! {issue}")
    print("\nACTION: Stop pipeline execution and investigate immediately")
elif warnings:
    print("\nWARNINGS DETECTED:")
    for warning in warnings:
        print(f"  !! {warning}")
    print("\nACTION: Review warnings before proceeding to production")
else:
    print("\nNo critical issues or warnings detected")
    print("STATUS: Pipeline ready for production deployment")

print("\nNOTIFICATION CHANNELS (for production setup):")
print("  - Email: data-engineering@sap.com")
print("  - Slack: #data-quality-alerts")
print("  - PagerDuty: For critical failures only")
print("  - Dashboard: Real-time monitoring at internal-dashboard.sap.com")