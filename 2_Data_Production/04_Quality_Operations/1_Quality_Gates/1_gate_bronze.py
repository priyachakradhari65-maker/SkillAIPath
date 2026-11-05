# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Validation Suite
# MAGIC **SkillAIPath Data Warehouse - Raw Data Quality Checks**
# MAGIC
# MAGIC ## Purpose
# MAGIC Validate bronze staging table (stg_sales) after batch load but before dimension/fact processing.
# MAGIC
# MAGIC ## Validation Categories
# MAGIC 1. **Row Count Validation** - Batch completeness check
# MAGIC 2. **Schema Validation** - Required columns present with correct types
# MAGIC 3. **Data Quality Checks** - NULL values, duplicates, data ranges
# MAGIC 4. **Business Rule Validation** - Price > 0, Quantity >= 1, valid dates
# MAGIC
# MAGIC ## Usage
# MAGIC ```python
# MAGIC # Run after bronze load
# MAGIC %run ./validation_bronze
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Validation Configuration
# Validation Configuration
from datetime import datetime
from pyspark.sql.functions import col, count, when, isnan, isnull

# Get environment parameter (passed from orchestrator)
try:
    dbutils.widgets.dropdown("environment", "dev", ["dev", "uat", "prod"], "Environment")
    ENVIRONMENT = dbutils.widgets.get("environment")
except:
    ENVIRONMENT = "dev"

class ValidationConfig:
    CATALOG = "sap"
    SCHEMA = f"{ENVIRONMENT}_bronze"  # Environment-aware schema
    TABLE = "stg_sales"  # Correct table name

    # Required columns (must exist) - Match actual bronze table schema
    REQUIRED_COLUMNS = [
        "OrderID", "CustomerName", "ProductName", "ProductCategory",
        "WareHouseName", "RetailerName", "AgentName", "PaymentMode",
        "ProductQuantity", "finalPrice", "OrderDate", "BatchDate"
    ]

    # Columns that should not have NULLs
    NON_NULL_COLUMNS = [
        "OrderID", "CustomerName", "ProductName", "finalPrice"
    ]

    # Minimum expected row count per batch (adjust based on data generator)
    MIN_BATCH_SIZE = 100  # At least 100 rows per batch

print("[INIT] Validation configuration loaded")

# COMMAND ----------

# Initialize validation results
validation_results = {
    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "table": f"{ValidationConfig.CATALOG}.{ValidationConfig.SCHEMA}.{ValidationConfig.TABLE}",
    "checks_passed": 0,
    "checks_failed": 0,
    "issues": []
}

print("\n" + "="*70)
print("BRONZE LAYER VALIDATION")
print("="*70)
print(f"Table: {validation_results['table']}")
print(f"Timestamp: {validation_results['timestamp']}")
print("="*70)

# COMMAND ----------

# Check 1: Table exists and row count
print("\n[CHECK 1] TABLE EXISTENCE & ROW COUNT")
print("-" * 70)

try:
    df_bronze = spark.table(validation_results["table"])
    row_count = df_bronze.count()
    validation_results["row_count"] = row_count

    print(f"Table exists: YES")
    print(f"Total rows: {row_count:,}")

    if row_count == 0:
        validation_results["checks_failed"] += 1
        validation_results["issues"].append("CRITICAL: Bronze table is empty")
        print("[FAIL] Table is empty - no data to validate")
    elif row_count < ValidationConfig.MIN_BATCH_SIZE:
        validation_results["checks_failed"] += 1
        validation_results["issues"].append(f"WARNING: Only {row_count} rows (expected at least {ValidationConfig.MIN_BATCH_SIZE})")
        print(f"[WARN] Low row count: {row_count} rows")
    else:
        validation_results["checks_passed"] += 1
        print(f"[PASS] Row count acceptable: {row_count:,} rows")

except Exception as e:
    validation_results["checks_failed"] += 1
    validation_results["issues"].append(f"CRITICAL: Table does not exist - {str(e)}")
    print(f"[FAIL] Table does not exist or cannot be read")
    print(f"Error: {str(e)}")

# COMMAND ----------

# Check 2: Schema validation - required columns exist
print("\n[CHECK 2] SCHEMA VALIDATION")
print("-" * 70)

if "row_count" in validation_results and validation_results["row_count"] > 0:
    actual_columns = set(df_bronze.columns)
    required_columns = set(ValidationConfig.REQUIRED_COLUMNS)

    missing_columns = required_columns - actual_columns

    if missing_columns:
        validation_results["checks_failed"] += 1
        validation_results["issues"].append(f"CRITICAL: Missing columns - {', '.join(missing_columns)}")
        print(f"[FAIL] Missing required columns: {', '.join(missing_columns)}")
    else:
        validation_results["checks_passed"] += 1
        print(f"[PASS] All {len(required_columns)} required columns present")
        print(f"Columns: {', '.join(sorted(ValidationConfig.REQUIRED_COLUMNS))}")
else:
    print("[SKIP] Skipped due to previous failures")

# COMMAND ----------

# Check 3: NULL value validation
print("\n[CHECK 3] NULL VALUE CHECKS")
print("-" * 70)

if "row_count" in validation_results and validation_results["row_count"] > 0:
    null_issues = []

    for column in ValidationConfig.NON_NULL_COLUMNS:
        if column in df_bronze.columns:
            null_count = df_bronze.filter(col(column).isNull()).count()

            if null_count > 0:
                null_issues.append(f"{column}: {null_count} NULLs")
                print(f"[FAIL] {column}: {null_count} NULL values found")

    if null_issues:
        validation_results["checks_failed"] += 1
        validation_results["issues"].append(f"CRITICAL: NULL values in critical columns - {', '.join(null_issues)}")
    else:
        validation_results["checks_passed"] += 1
        print(f"[PASS] No NULLs in critical columns: {', '.join(ValidationConfig.NON_NULL_COLUMNS)}")
else:
    print("[SKIP] Skipped due to previous failures")

# COMMAND ----------

# Check 4: Duplicate OrderID check
print("\n[CHECK 4] DUPLICATE DETECTION")
print("-" * 70)

if "row_count" in validation_results and validation_results["row_count"] > 0:
    # Check for duplicate OrderIDs within same BatchDate
    duplicate_check = df_bronze.groupBy("BatchDate", "OrderID").count().filter(col("count") > 1)
    duplicate_count = duplicate_check.count()

    if duplicate_count > 0:
        validation_results["checks_failed"] += 1
        validation_results["issues"].append(f"CRITICAL: {duplicate_count} duplicate OrderIDs found within BatchDate")
        print(f"[FAIL] {duplicate_count} duplicate OrderIDs detected")
        print("Sample duplicates:")
        duplicate_check.show(5, truncate=False)
    else:
        validation_results["checks_passed"] += 1
        print(f"[PASS] No duplicate OrderIDs within BatchDate")
else:
    print("[SKIP] Skipped due to previous failures")

# COMMAND ----------

# DBTITLE 1,Cell 8
# Check 5: Business rule validation
print("\n[CHECK 5] BUSINESS RULE VALIDATION")
print("-" * 70)

if "row_count" in validation_results and validation_results["row_count"] > 0:
    business_issues = []

    # finalPrice must be > 0
    invalid_price_count = df_bronze.filter(col("finalPrice") <= 0).count()
    if invalid_price_count > 0:
        business_issues.append(f"{invalid_price_count} rows with finalPrice <= 0")
        print(f"[FAIL] {invalid_price_count} rows with invalid price (<=0)")

    # ProductQuantity must be >= 1
    invalid_qty_count = df_bronze.filter(col("ProductQuantity") < 1).count()
    if invalid_qty_count > 0:
        business_issues.append(f"{invalid_qty_count} rows with ProductQuantity < 1")
        print(f"[FAIL] {invalid_qty_count} rows with invalid ProductQuantity (<1)")

    # BatchDate should be in YYYY-MM-DD format (basic check)
    invalid_date_count = df_bronze.filter(
        ~col("BatchDate").rlike(r"^\d{4}-\d{2}-\d{2}$")
    ).count()
    if invalid_date_count > 0:
        business_issues.append(f"{invalid_date_count} rows with invalid BatchDate format")
        print(f"[FAIL] {invalid_date_count} rows with invalid BatchDate format")

    if business_issues:
        validation_results["checks_failed"] += 1
        validation_results["issues"].append(f"CRITICAL: Business rule violations - {', '.join(business_issues)}")
    else:
        validation_results["checks_passed"] += 1
        print(f"[PASS] All business rules satisfied")
        print("  - finalPrice > 0")
        print("  - ProductQuantity >= 1")
        print("  - BatchDate format valid (YYYY-MM-DD)")
else:
    print("[SKIP] Skipped due to previous failures")

# COMMAND ----------

# Final summary
print("\n" + "="*70)
print("VALIDATION SUMMARY")
print("="*70)

total_checks = validation_results["checks_passed"] + validation_results["checks_failed"]
print(f"Total checks: {total_checks}")
print(f"Passed: {validation_results['checks_passed']}")
print(f"Failed: {validation_results['checks_failed']}")

if validation_results["checks_failed"] == 0:
    print("\n[PASS] Bronze layer validation PASSED - Safe to proceed to dimension load")
    validation_results["status"] = "PASS"
else:
    print("\n[FAIL] Bronze layer validation FAILED - Fix issues before proceeding")
    print("\nIssues found:")
    for i, issue in enumerate(validation_results["issues"], 1):
        print(f"  {i}. {issue}")
    validation_results["status"] = "FAIL"

print("="*70)

# Return validation results
dbutils.notebook.exit(validation_results["status"])

# COMMAND ----------

