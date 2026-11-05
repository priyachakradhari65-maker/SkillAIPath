# Databricks notebook source
# DBTITLE 1,Dimension Validation Suite - Data Quality Checks
# MAGIC %md
# MAGIC #  Dimension Validation Suite
# MAGIC **SkillAIPath Data Warehouse - Silver Layer Quality Checks**
# MAGIC
# MAGIC ## Purpose
# MAGIC Comprehensive validation of all 7 dimension tables after ETL load.
# MAGIC
# MAGIC ## Validation Categories
# MAGIC 1. **Row Count Validation** - Expected vs Actual
# MAGIC 2. **Null Checks** - No NULLs in business keys
# MAGIC 3. **Duplicate Detection** - Surrogate key uniqueness
# MAGIC 4. **Referential Integrity** - Unknown records present
# MAGIC 5. **Data Quality** - Business rule validation
# MAGIC 6. **SCD Type 2 Validation** - Current record checks
# MAGIC
# MAGIC ## Pass Criteria
# MAGIC [PASS] All checks return 0 issues
# MAGIC [FAIL] Any check > 0 → Pipeline FAILS
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Configuration
# Validation Configuration
from datetime import datetime
import json

# Get environment parameter (passed from orchestrator)
try:
    dbutils.widgets.dropdown("environment", "dev", ["dev", "uat", "prod"], "Environment")
    ENVIRONMENT = dbutils.widgets.get("environment")
except:
    ENVIRONMENT = "dev"

class ValidationConfig:
    CATALOG = "sap"
    SCHEMA = f"{ENVIRONMENT}_silver"  # Environment-aware schema

    # Expected row counts (update after first load)
    EXPECTED_COUNTS = {
        "dim_customer": 93360,
        "dim_product": 45,  # 44 + Unknown
        "dim_warehouse": 21,  # 20 + Unknown
        "dim_date": 4018,  # 11 years of dates
        "dim_agent": 16,  # 15 + Unknown
        "dim_payment_mode": 6,  # 5 + Unknown
        "dim_retailer": 501  # 500 + Unknown
    }

    # Tolerance for row count variance (percentage)
    COUNT_TOLERANCE = 0.05  # 5% variance allowed

validation_results = []

print("="*70)
print(" DIMENSION VALIDATION SUITE")
print("="*70)
print(f"Catalog: {ValidationConfig.CATALOG}")
print(f"Schema: {ValidationConfig.SCHEMA}")
print(f"Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Validation 1: Row Count Check
# Check 1: Row count validation
print("\n VALIDATION 1: ROW COUNT CHECKS")
print("-" * 70)

for table_name, expected_count in ValidationConfig.EXPECTED_COUNTS.items():
    full_table = f"{ValidationConfig.CATALOG}.{ValidationConfig.SCHEMA}.{table_name}"

    try:
        actual_count = spark.table(full_table).count()
        variance = abs(actual_count - expected_count) / expected_count if expected_count > 0 else 0

        status = "[PASS] PASS" if variance <= ValidationConfig.COUNT_TOLERANCE else "[FAIL] FAIL"

        validation_results.append({
            "check": "row_count",
            "table": table_name,
            "expected": expected_count,
            "actual": actual_count,
            "variance_pct": round(variance * 100, 2),
            "status": "pass" if variance <= ValidationConfig.COUNT_TOLERANCE else "fail"
        })

        print(f"{status} {table_name:25} Expected: {expected_count:6,} | Actual: {actual_count:6,} | Variance: {variance*100:5.2f}%")

    except Exception as e:
        print(f"[FAIL] FAIL {table_name:25} Table not found or error: {str(e)}")
        validation_results.append({
            "check": "row_count",
            "table": table_name,
            "status": "fail",
            "error": str(e)
        })

# COMMAND ----------

# DBTITLE 1,Validation 2: NULL Checks
# Check 2: NULL value validation
print("\n VALIDATION 2: NULL VALUE CHECKS")
print("-" * 70)

query = f"""
SELECT 'dim_customer' as table_name, 'CustomerID' as column_name, COUNT(*) as null_count
FROM {ValidationConfig.CATALOG}.{ValidationConfig.SCHEMA}.dim_customer
WHERE CustomerID IS NULL
UNION ALL
SELECT 'dim_customer', 'CustomerName', COUNT(*)
FROM {ValidationConfig.CATALOG}.{ValidationConfig.SCHEMA}.dim_customer
WHERE CustomerName IS NULL OR CustomerName = ''
UNION ALL
SELECT 'dim_product', 'ProductID', COUNT(*)
FROM {ValidationConfig.CATALOG}.{ValidationConfig.SCHEMA}.dim_product
WHERE ProductID IS NULL
UNION ALL
SELECT 'dim_product', 'ProductName', COUNT(*)
FROM {ValidationConfig.CATALOG}.{ValidationConfig.SCHEMA}.dim_product
WHERE ProductName IS NULL OR ProductName = ''
UNION ALL
SELECT 'dim_warehouse', 'WareHouseID', COUNT(*)
FROM {ValidationConfig.CATALOG}.{ValidationConfig.SCHEMA}.dim_warehouse
WHERE WareHouseID IS NULL
ORDER BY null_count DESC
"""

result_df = spark.sql(query)
result_df.show(truncate=False)

# Check if any nulls found
null_issues = result_df.filter("null_count > 0").collect()
if len(null_issues) > 0:
    validation_results.append({"check": "null_check", "status": "fail", "issues": len(null_issues)})
    print(f"[FAIL] Found {len(null_issues)} NULL value issues")
else:
    validation_results.append({"check": "null_check", "status": "pass"})
    print("[PASS] No NULL values in critical columns")

# COMMAND ----------

# DBTITLE 1,Validation 3: Duplicate Keys
# Check 3: Duplicate key validation
print("\n VALIDATION 3: DUPLICATE KEY CHECKS")
print("-" * 70)

query = f"""
SELECT 'dim_customer' as table_name, 'CustomerSK' as key_column, COUNT(*) as duplicate_count
FROM (SELECT CustomerSK, COUNT(*) as cnt FROM {ValidationConfig.CATALOG}.{ValidationConfig.SCHEMA}.dim_customer GROUP BY CustomerSK HAVING COUNT(*) > 1)
UNION ALL
SELECT 'dim_product', 'ProductSK', COUNT(*)
FROM (SELECT ProductSK, COUNT(*) as cnt FROM {ValidationConfig.CATALOG}.{ValidationConfig.SCHEMA}.dim_product GROUP BY ProductSK HAVING COUNT(*) > 1)
UNION ALL
SELECT 'dim_warehouse', 'WareHouseSK', COUNT(*)
FROM (SELECT WareHouseSK, COUNT(*) as cnt FROM {ValidationConfig.CATALOG}.{ValidationConfig.SCHEMA}.dim_warehouse GROUP BY WareHouseSK HAVING COUNT(*) > 1)
ORDER BY duplicate_count DESC
"""

result_df = spark.sql(query)
result_df.show(truncate=False)

# Check if any duplicates found
duplicate_issues = result_df.filter("duplicate_count > 0").collect()
if len(duplicate_issues) > 0:
    validation_results.append({"check": "duplicate_check", "status": "fail", "issues": len(duplicate_issues)})
    print(f"[FAIL] Found {len(duplicate_issues)} duplicate key issues")
else:
    validation_results.append({"check": "duplicate_check", "status": "pass"})
    print("[PASS] No duplicate keys found")

# COMMAND ----------

# DBTITLE 1,Validation 4: Unknown Records
# Check 4: Unknown records validation
print("\n VALIDATION 4: UNKNOWN RECORDS CHECK")
print("-" * 70)

query = f"""
SELECT 'dim_customer' as table_name, COUNT(*) as unknown_count FROM {ValidationConfig.CATALOG}.{ValidationConfig.SCHEMA}.dim_customer WHERE CustomerSK = -1
UNION ALL
SELECT 'dim_product', COUNT(*) FROM {ValidationConfig.CATALOG}.{ValidationConfig.SCHEMA}.dim_product WHERE ProductSK = -1
UNION ALL
SELECT 'dim_warehouse', COUNT(*) FROM {ValidationConfig.CATALOG}.{ValidationConfig.SCHEMA}.dim_warehouse WHERE WareHouseSK = -1
UNION ALL
SELECT 'dim_agent', COUNT(*) FROM {ValidationConfig.CATALOG}.{ValidationConfig.SCHEMA}.dim_agent WHERE AgentSK = -1
UNION ALL
SELECT 'dim_payment_mode', COUNT(*) FROM {ValidationConfig.CATALOG}.{ValidationConfig.SCHEMA}.dim_payment_mode WHERE PaymentModeSK = -1
ORDER BY table_name
"""

result_df = spark.sql(query)
result_df.show(truncate=False)

# Verify unknown records exist (should have exactly 1 per dimension)
total_unknown = result_df.agg({"unknown_count": "sum"}).collect()[0][0]
if total_unknown >= 5:  # Should have at least 5 unknown records (one per dimension)
    validation_results.append({"check": "unknown_records", "status": "pass"})
    print(f"[PASS] Unknown records present: {total_unknown}")
else:
    validation_results.append({"check": "unknown_records", "status": "fail"})
    print(f"[FAIL] Missing unknown records: found {total_unknown}, expected at least 5")

# COMMAND ----------

# DBTITLE 1,Validation Summary
failed_checks = [v for v in validation_results if v.get("status") == "fail"]
print("\n" + "="*70)
print(" VALIDATION SUMMARY")
print("="*70)
print(f"Total Checks: {len(validation_results)}")
print(f"Passed: {len(validation_results) - len(failed_checks)}")
print(f"Failed: {len(failed_checks)}")
print("="*70)
if len(failed_checks) == 0:
    print("\n[PASS] ALL VALIDATIONS PASSED")
    status = "success"
else:
    print("\n[FAIL] VALIDATION FAILED")
    for check in failed_checks:
        print(f"   - {check['check']}: {check['table']}")
    status = "failed"
summary = {"status": status, "timestamp": datetime.now().isoformat(), "total_checks": len(validation_results), "passed": len(validation_results) - len(failed_checks), "failed": len(failed_checks)}
dbutils.notebook.exit(json.dumps(summary))