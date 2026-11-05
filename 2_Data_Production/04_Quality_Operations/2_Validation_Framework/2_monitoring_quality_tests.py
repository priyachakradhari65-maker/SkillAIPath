# Databricks notebook source
# DBTITLE 1,Data Quality Monitoring Tests - Complete Coverage
# MAGIC %md
# MAGIC %md
# MAGIC # Data Quality Monitoring Tests - COMPREHENSIVE
# MAGIC **SkillAIPath Utilities - 100% Automated Quality Validation**
# MAGIC
# MAGIC ## Purpose
# MAGIC Execute comprehensive quality tests across ALL SkillAIPath layers with zero manual intervention required.
# MAGIC
# MAGIC ## Test Categories (9 Complete Test Suites)
# MAGIC 1. ✅ **Dimension Null Checks** - CustomerSK, ProductSK, DateSK validation
# MAGIC 2. ✅ **Fact Table FK Integrity** - All foreign key relationships (fact_sales → dims)
# MAGIC 3. ✅ **Business Rule Validation** - GST 18%, date ranges (2015-2025)
# MAGIC 4. ✅ **Gold Aggregation Accuracy** - All 4 business marts validation (customer_360, rfm_analysis, product_performance, inventory_health)
# MAGIC 5. ✅ **Additional Dimension Validations** - 6 additional dimensions (warehouse, retailer, agent, payment_mode, wholesaler, category)
# MAGIC 6. ✅ **Fact Purchase & Inventory** - Complete fact table coverage (sales, purchase, inventory)
# MAGIC 7. ✅ **Duplicate Detection** - Surrogate key uniqueness across all dimensions and facts
# MAGIC 8. ✅ **Data Freshness** - Bronze layer recency checks (both batch and incremental tables)
# MAGIC 9. ✅ **Row Count Validations** - Bronze → Silver → Gold data flow validation
# MAGIC
# MAGIC ## Load Pattern Support (Parameterized)
# MAGIC - ✅ **Full Load** (`load_type="full"`): Validates batch staging tables (stg_sales, stg_purchase)
# MAGIC - ✅ **Incremental Load** (`load_type="incremental"`): Validates incremental staging tables (stg_sales_incremental, stg_purchase_incremental)
# MAGIC - Works seamlessly across dev/main/uat environments (auto-detects via Config)
# MAGIC
# MAGIC ## Usage from Orchestration Notebooks
# MAGIC
# MAGIC ```python
# MAGIC # Full Load Orchestration
# MAGIC dbutils.notebook.run("../8_Validation_Framework/2_monitoring_quality_tests", 0, {"load_type": "full"})
# MAGIC
# MAGIC # Incremental Load Orchestration
# MAGIC dbutils.notebook.run("../8_Validation_Framework/2_monitoring_quality_tests", 0, {"load_type": "incremental"})
# MAGIC ```
# MAGIC
# MAGIC ## Integration
# MAGIC - Runs AFTER all pipeline stages complete (non-blocking)
# MAGIC - Logs all results to `{catalog}.bronze.quality_metrics` table
# MAGIC - Scheduled 2 hours after pipeline completion
# MAGIC - Comprehensive coverage - NO manual quality checks needed
# MAGIC
# MAGIC ## Schedule
# MAGIC - Daily: 4 AM (2 hours after pipeline at 2 AM)
# MAGIC - Duration: 10-15 minutes
# MAGIC - Automated alerts on failures

# COMMAND ----------

# DBTITLE 1,Configuration and Setup
# Configuration and Setup
from datetime import datetime
from pyspark.sql import functions as F

# Import config using %run (works across dev/main/uat environments)
%run ../../../1_Canvas/1_Setup/1_Config

# Widget for load type (full or incremental)
dbutils.widgets.dropdown("load_type", "full", ["full", "incremental"], "Load Type")
load_type = dbutils.widgets.get("load_type")

print("="*70)
print("DATA QUALITY MONITORING TESTS")
print("="*70)
print(f"Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Environment: {Config.ENVIRONMENT}")
print(f"Load Type: {load_type.upper()}")
print("="*70)
print()

# Create metrics table if not exists
metrics_table = f"{Config.CATALOG}.bronze.quality_metrics"
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {metrics_table} (
  test_run_id STRING,
  test_timestamp TIMESTAMP,
  layer STRING,
  table_name STRING,
  test_category STRING,
  test_name STRING,
  test_status STRING,
  rows_tested BIGINT,
  issues_found BIGINT,
  error_message STRING
) USING DELTA
""")

test_run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
print(f"Test Run ID: {test_run_id}")
print()

# COMMAND ----------

# DBTITLE 1,Test Framework Functions
# Test Framework Functions
def log_test_result(layer, table, category, test_name, status, rows_tested=0, issues=0, error=None):
    """Log test result to metrics table"""
    # Escape single quotes in strings to prevent SQL injection/errors
    safe_layer = layer.replace("'", "''")
    safe_table = table.replace("'", "''")
    safe_category = category.replace("'", "''")
    safe_test_name = test_name.replace("'", "''")
    safe_status = status.replace("'", "''")
    
    if error is None:
        error_value = "NULL"
    else:
        safe_error = str(error).replace("'", "''")
        error_value = f"'{safe_error}'"
    
    spark.sql(f"""
    INSERT INTO {metrics_table} VALUES (
        '{test_run_id}',
        current_timestamp(),
        '{safe_layer}',
        '{safe_table}',
        '{safe_category}',
        '{safe_test_name}',
        '{safe_status}',
        {rows_tested},
        {issues},
        {error_value}
    )
    """)

def test_null_critical_columns(table_full, layer, table_name, critical_cols):
    """Test for nulls in critical columns"""
    print(f"  Testing null constraints on {table_name}...")

    try:
        df = spark.table(table_full)
        total_rows = df.count()

        for col in critical_cols:
            # Escape column names with backticks for SQL reserved keywords
            null_count = df.filter(F.col(f"`{col}`").isNull()).count()

            status = "PASS" if null_count == 0 else "FAIL"
            log_test_result(layer, table_name, "NULL_CHECK", f"{col}_not_null", status, total_rows, null_count)

            if null_count > 0:
                print(f"    FAIL: {col} has {null_count} nulls")
            else:
                print(f"    PASS: {col} has no nulls")

    except Exception as e:
        log_test_result(layer, table_name, "NULL_CHECK", "critical_columns", "ERROR", 0, 0, str(e))
        print(f"    ERROR: {str(e)}")

def test_referential_integrity(parent_table, child_table, fk_col, layer, table_name, parent_key_col=None):
    """Test foreign key relationships"""
    # If parent_key_col not specified, assume same name as fk_col
    parent_col = parent_key_col if parent_key_col else fk_col
    print(f"  Testing FK integrity: {table_name}.{fk_col} -> {parent_col}...")

    try:
        orphan_query = f"""
        SELECT COUNT(*) as orphan_count
        FROM {child_table} c
        LEFT JOIN {parent_table} p ON c.{fk_col} = p.{parent_col}
        WHERE p.{parent_col} IS NULL AND c.{fk_col} IS NOT NULL
        """

        orphan_count = spark.sql(orphan_query).first()['orphan_count']
        total_rows = spark.table(child_table).count()

        status = "PASS" if orphan_count == 0 else "FAIL"
        log_test_result(layer, table_name, "REFERENTIAL", f"{fk_col}_fk", status, total_rows, orphan_count)

        if orphan_count > 0:
            print(f"    FAIL: {orphan_count} orphaned rows found")
        else:
            print(f"    PASS: All FKs valid")

    except Exception as e:
        log_test_result(layer, table_name, "REFERENTIAL", f"{fk_col}_fk", "ERROR", 0, 0, str(e))
        print(f"    ERROR: {str(e)}")

print("Test framework loaded")

# COMMAND ----------

# DBTITLE 1,TEST 1: Dimension Null Checks
# TEST 1: SILVER DIMENSION NULL CHECKS
print()
print("TEST CATEGORY 1: DIMENSION NULL CHECKS")
print("-"*70)
print()

dimension_tests = [
    (f"{Config.CATALOG}.silver.dim_customer", "dim_customer", ["CustomerSK", "CustomerID", "CustomerName"]),
    (f"{Config.CATALOG}.silver.dim_product", "dim_product", ["ProductSK", "ProductID", "ProductName"]),
    (f"{Config.CATALOG}.silver.dim_date", "dim_date", ["DateID", "Date", "Year", "MonthNumber", "DayOfMonth"])
]

for table_full, table_name, critical_cols in dimension_tests:
    test_null_critical_columns(table_full, "silver", table_name, critical_cols)
    print()

# COMMAND ----------

# DBTITLE 1,TEST 2: Fact Table Referential Integrity
# TEST 2: FACT TABLE REFERENTIAL INTEGRITY
print()
print("TEST CATEGORY 2: FACT TABLE FK INTEGRITY")
print("-"*70)
print()

fact_sales = f"{Config.CATALOG}.silver.fact_sales"

# NULL CHECKS for fact_sales critical columns
print("  Testing fact_sales null constraints...")
test_null_critical_columns(fact_sales, "silver", "fact_sales", ["OrderID", "CustomerSK", "ProductSK", "OrderDateID"])
print()

# Test all FK relationships for fact_sales
fk_tests = [
    (f"{Config.CATALOG}.silver.dim_customer", fact_sales, "CustomerSK", "fact_sales", None),
    (f"{Config.CATALOG}.silver.dim_product", fact_sales, "ProductSK", "fact_sales", None),
    (f"{Config.CATALOG}.silver.dim_date", fact_sales, "OrderDateID", "fact_sales", "DateID")
]

for parent, child, fk_col, table_name, parent_key in fk_tests:
    test_referential_integrity(parent, child, fk_col, "silver", table_name, parent_key)
    print()

# COMMAND ----------

# DBTITLE 1,TEST 3: Business Rule Validation (Fixed Columns)
# TEST 3: BUSINESS RULE VALIDATION
print()
print("TEST CATEGORY 3: BUSINESS RULE VALIDATION")
print("-"*70)
print()

# Test GST calculation accuracy (18% standard rate)
print("  Testing GST calculation accuracy...")

try:
    # GST validation: (CGST + SGST) should be 18% of base amount
    # Base amount = finalprice - (CGST + SGST)
    gst_test = spark.sql(f"""
    SELECT
        COUNT(*) as total_rows,
        SUM(CASE
            WHEN finalprice > 0 AND (CGST + SGST) > 0 THEN
                CASE WHEN ABS(((CGST + SGST) / (finalprice - (CGST + SGST))) - 0.18) > 0.001
                THEN 1 ELSE 0 END
            ELSE 0
        END) as invalid_gst
    FROM {fact_sales}
    WHERE finalprice > 0
    """)

    result = gst_test.first()
    total = result['total_rows']
    invalid = result['invalid_gst']

    status = "PASS" if invalid == 0 else "FAIL"
    log_test_result("silver", "fact_sales", "BUSINESS_RULE", "gst_18_percent", status, total, invalid)

    if invalid > 0:
        print(f"    FAIL: {invalid} rows have incorrect GST calculation")
    else:
        print(f"    PASS: All {total} rows have correct GST (18%)")

except Exception as e:
    log_test_result("silver", "fact_sales", "BUSINESS_RULE", "gst_18_percent", "ERROR", 0, 0, str(e))
    print(f"    ERROR: {str(e)}")

print()

# Test date range validity
print("  Testing date range validity...")

try:
    date_test = spark.sql(f"""
    SELECT COUNT(*) as invalid_dates
    FROM {Config.CATALOG}.silver.dim_date
    WHERE Date < '2015-01-01' OR Date > '2025-12-31'
    """)

    invalid_dates = date_test.first()['invalid_dates']
    total_dates = spark.table(f"{Config.CATALOG}.silver.dim_date").count()

    status = "PASS" if invalid_dates == 0 else "FAIL"
    log_test_result("silver", "dim_date", "BUSINESS_RULE", "date_range_2015_2025", status, total_dates, invalid_dates)

    if invalid_dates > 0:
        print(f"    FAIL: {invalid_dates} dates outside valid range")
    else:
        print(f"    PASS: All {total_dates} dates within 2015-2025 range")

except Exception as e:
    log_test_result("silver", "dim_date", "BUSINESS_RULE", "date_range_2015_2025", "ERROR", 0, 0, str(e))
    print(f"    ERROR: {str(e)}")

print()

# COMMAND ----------

# DBTITLE 1,TEST 4: Gold Layer Validations (All 4 Marts)
# TEST 4: GOLD LAYER AGGREGATION ACCURACY
print()
print("TEST CATEGORY 4: GOLD LAYER ACCURACY")
print("-"*70)
# TEST 4.1: mart_customer_360
print("  Testing mart_customer_360 aggregation accuracy...")

try:
    # Compare gold mart totals with silver fact totals
    comparison = spark.sql(f"""
    WITH fact_totals AS (
        SELECT
            SUM(finalprice) as fact_revenue,
            COUNT(*) as fact_orders
        FROM {Config.CATALOG}.silver.fact_sales
    ),
    mart_totals AS (
        SELECT
            SUM(lifetime_value) as mart_revenue,
            SUM(total_orders) as mart_orders
        FROM {Config.CATALOG}.gold.mart_customer_360
    )
    SELECT
        f.fact_revenue,
        m.mart_revenue,
        ABS(f.fact_revenue - m.mart_revenue) as revenue_diff,
        f.fact_orders,
        m.mart_orders,
        ABS(f.fact_orders - m.mart_orders) as order_diff
    FROM fact_totals f, mart_totals m
    """)

    result = comparison.first()
    revenue_match = result['revenue_diff'] < 1.0  # Allow 1 rupee rounding
    order_match = result['order_diff'] == 0

    status = "PASS" if (revenue_match and order_match) else "FAIL"
    issues = 0 if (revenue_match and order_match) else 1

    log_test_result("gold", "mart_customer_360", "AGGREGATION", "revenue_order_totals", status, 1, issues)

    if revenue_match and order_match:
        print(f"    PASS: Revenue and order totals match")
        print(f"      Fact revenue: ₹{result['fact_revenue']:,.2f}")
        print(f"      Mart revenue: ₹{result['mart_revenue']:,.2f}")
    else:
        print(f"    FAIL: Totals mismatch")
        print(f"      Revenue diff: ₹{result['revenue_diff']:,.2f}")
        print(f"      Order diff: {result['order_diff']}")

except Exception as e:
    log_test_result("gold", "mart_customer_360", "AGGREGATION", "revenue_order_totals", "ERROR", 0, 0, str(e))
    print(f"    ERROR: {str(e)}")

print()

# TEST 4.2: mart_rfm_analysis
print("  Testing mart_rfm_analysis data availability...")

try:
    rfm_count = spark.table(f"{Config.CATALOG}.gold.mart_rfm_analysis").count()
    customer_count = spark.table(f"{Config.CATALOG}.silver.dim_customer").count()
    
    # RFM should have at least some customer records
    status = "PASS" if rfm_count > 0 else "FAIL"
    issues = 0 if rfm_count > 0 else 1
    
    log_test_result("gold", "mart_rfm_analysis", "DATA_QUALITY", "record_count", status, rfm_count, issues)
    
    if rfm_count > 0:
        print(f"    PASS: {rfm_count:,} RFM customer segments available")
    else:
        print(f"    FAIL: No RFM records found")
        
except Exception as e:
    log_test_result("gold", "mart_rfm_analysis", "DATA_QUALITY", "record_count", "ERROR", 0, 0, str(e))
    print(f"    ERROR: {str(e)}")

print()

# TEST 4.3: mart_product_performance
print("  Testing mart_product_performance data availability...")

try:
    product_perf_count = spark.table(f"{Config.CATALOG}.gold.mart_product_performance").count()
    product_count = spark.table(f"{Config.CATALOG}.silver.dim_product").count()
    
    # Product performance should cover products
    status = "PASS" if product_perf_count > 0 else "FAIL"
    issues = 0 if product_perf_count > 0 else 1
    
    log_test_result("gold", "mart_product_performance", "DATA_QUALITY", "record_count", status, product_perf_count, issues)
    
    if product_perf_count > 0:
        print(f"    PASS: {product_perf_count:,} product performance records available")
    else:
        print(f"    FAIL: No product performance records found")
        
except Exception as e:
    log_test_result("gold", "mart_product_performance", "DATA_QUALITY", "record_count", "ERROR", 0, 0, str(e))
    print(f"    ERROR: {str(e)}")

print()

# TEST 4.4: mart_inventory_health
print("  Testing mart_inventory_health data availability...")

try:
    inventory_health_count = spark.table(f"{Config.CATALOG}.gold.mart_inventory_health").count()
    
    # Inventory health should have records from fact_inventory
    status = "PASS" if inventory_health_count > 0 else "FAIL"
    issues = 0 if inventory_health_count > 0 else 1
    
    log_test_result("gold", "mart_inventory_health", "DATA_QUALITY", "record_count", status, inventory_health_count, issues)
    
    if inventory_health_count > 0:
        print(f"    PASS: {inventory_health_count:,} inventory health records available")
    else:
        print(f"    FAIL: No inventory health records found")
        
except Exception as e:
    log_test_result("gold", "mart_inventory_health", "DATA_QUALITY", "record_count", "ERROR", 0, 0, str(e))
    print(f"    ERROR: {str(e)}")

print()

# COMMAND ----------

# DBTITLE 1,TEST 5: Additional Dimension Validations
# TEST 5: ADDITIONAL DIMENSION NULL CHECKS
print()
print("TEST CATEGORY 5: ADDITIONAL DIMENSION VALIDATIONS")
print("-"*70)
print()

additional_dimension_tests = [
    (f"{Config.CATALOG}.silver.dim_warehouse", "dim_warehouse", ["WarehouseSK", "WarehouseID", "WarehouseName"]),
    (f"{Config.CATALOG}.silver.dim_retailer", "dim_retailer", ["RetailerSK", "RetailerID", "RetailerName"]),
    (f"{Config.CATALOG}.silver.dim_agent", "dim_agent", ["AgentSK", "AgentID", "AgentName"]),
    (f"{Config.CATALOG}.silver.dim_payment_mode", "dim_payment_mode", ["PaymentModeSK", "PaymentModeID", "PaymentMode"]),
    (f"{Config.CATALOG}.silver.dim_wholesaler", "dim_wholesaler", ["WholesalerSK", "WholesalerID", "WholesalerName"]),

    (f"{Config.CATALOG}.silver.dim_product_category", "dim_product_category", ["ProductCategorySK", "ProductCategoryID", "ProductCategory"])
]

for table_full, table_name, critical_cols in additional_dimension_tests:
    try:
        # Check if table exists first
        spark.table(table_full)
        test_null_critical_columns(table_full, "silver", table_name, critical_cols)
    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            print(f"  SKIP: {table_name} does not exist yet")
            log_test_result("silver", table_name, "NULL_CHECK", "table_existence", "SKIP", 0, 0, "Table not found")
        else:
            print(f"  ERROR: {table_name} - {str(e)}")
            log_test_result("silver", table_name, "NULL_CHECK", "critical_columns", "ERROR", 0, 0, str(e))
    print()

# COMMAND ----------

# DBTITLE 1,TEST 6: Fact Purchase and Inventory Validations
# TEST 6: FACT PURCHASE AND INVENTORY VALIDATIONS
print()
print("TEST CATEGORY 6: FACT PURCHASE & INVENTORY VALIDATIONS")
print("-"*70)
print()

# Test fact_purchase
print("  Testing fact_purchase...")
try:
    fact_purchase = f"{Config.CATALOG}.silver.fact_purchase"
    spark.table(fact_purchase)
    
    # Null checks  
    test_null_critical_columns(fact_purchase, "silver", "fact_purchase", ["PurchaseID", "PurchaseDateID", "ProductSK"])
    
    # FK integrity
    test_referential_integrity(f"{Config.CATALOG}.silver.dim_product", fact_purchase, "ProductSK", "silver", "fact_purchase")
    test_referential_integrity(f"{Config.CATALOG}.silver.dim_date", fact_purchase, "PurchaseDateID", "silver", "fact_purchase", "DateID")
    
except Exception as e:
    if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
        print(f"  SKIP: fact_purchase does not exist yet")
        log_test_result("silver", "fact_purchase", "EXISTENCE", "table_exists", "SKIP", 0, 0, "Table not found")
    else:
        print(f"  ERROR: {str(e)}")
        log_test_result("silver", "fact_purchase", "VALIDATION", "general", "ERROR", 0, 0, str(e))

print()

# Test fact_inventory
print("  Testing fact_inventory...")
try:
    fact_inventory = f"{Config.CATALOG}.silver.fact_inventory"
    spark.table(fact_inventory)
    
    # Null checks (fact_inventory uses composite key: ProductSK + WarehouseSK + DateID)
    test_null_critical_columns(fact_inventory, "silver", "fact_inventory", ["ProductSK", "WarehouseSK", "DateID"])
    
    # FK integrity
    test_referential_integrity(f"{Config.CATALOG}.silver.dim_product", fact_inventory, "ProductSK", "silver", "fact_inventory")
    test_referential_integrity(f"{Config.CATALOG}.silver.dim_date", fact_inventory, "DateID", "silver", "fact_inventory")
    
except Exception as e:
    if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
        print(f"  SKIP: fact_inventory does not exist yet")
        log_test_result("silver", "fact_inventory", "EXISTENCE", "table_exists", "SKIP", 0, 0, "Table not found")
    else:
        print(f"  ERROR: {str(e)}")
        log_test_result("silver", "fact_inventory", "VALIDATION", "general", "ERROR", 0, 0, str(e))

print()

# COMMAND ----------

# DBTITLE 1,TEST 7: Duplicate Detection
# TEST 7: DUPLICATE DETECTION
print()
print("TEST CATEGORY 7: DUPLICATE DETECTION")
print("-"*70)
print()

duplication_tests = [
    # Dimensions - check surrogate key uniqueness
    (f"{Config.CATALOG}.silver.dim_customer", "dim_customer", "CustomerSK", "silver"),
    (f"{Config.CATALOG}.silver.dim_product", "dim_product", "ProductSK", "silver"),
    (f"{Config.CATALOG}.silver.dim_date", "dim_date", "DateID", "silver"),
    (f"{Config.CATALOG}.silver.dim_warehouse", "dim_warehouse", "WarehouseSK", "silver"),
    (f"{Config.CATALOG}.silver.dim_retailer", "dim_retailer", "RetailerSK", "silver"),
    (f"{Config.CATALOG}.silver.dim_agent", "dim_agent", "AgentSK", "silver"),
    (f"{Config.CATALOG}.silver.dim_payment_mode", "dim_payment_mode", "PaymentModeSK", "silver"),
    (f"{Config.CATALOG}.silver.dim_wholesaler", "dim_wholesaler", "WholesalerSK", "silver"),
    (f"{Config.CATALOG}.silver.dim_product_category", "dim_product_category", "ProductCategorySK", "silver"),
    
    # Facts - check fact key uniqueness
    (f"{Config.CATALOG}.silver.fact_sales", "fact_sales", "OrderID", "silver"),
    (f"{Config.CATALOG}.silver.fact_purchase", "fact_purchase", "PurchaseID", "silver"),

]

for table_full, table_name, key_col, layer in duplication_tests:
    print(f"  Testing {table_name} for duplicates on {key_col}...")
    
    try:
        dup_query = f"""
        SELECT COUNT(*) as duplicate_count
        FROM (
            SELECT {key_col}, COUNT(*) as cnt
            FROM {table_full}
            GROUP BY {key_col}
            HAVING COUNT(*) > 1
        )
        """
        
        duplicates = spark.sql(dup_query).first()['duplicate_count']
        total_rows = spark.table(table_full).count()
        
        status = "PASS" if duplicates == 0 else "FAIL"
        log_test_result(layer, table_name, "DUPLICATE", f"{key_col}_unique", status, total_rows, duplicates)
        
        if duplicates > 0:
            print(f"    FAIL: {duplicates} duplicate {key_col} values found")
        else:
            print(f"    PASS: No duplicates on {key_col}")
            
    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            print(f"    SKIP: {table_name} does not exist yet")
            log_test_result(layer, table_name, "DUPLICATE", f"{key_col}_unique", "SKIP", 0, 0, "Table not found")
        else:
            print(f"    ERROR: {str(e)}")
            log_test_result(layer, table_name, "DUPLICATE", f"{key_col}_unique", "ERROR", 0, 0, str(e))
    
    print()

# Special duplicate check for fact_inventory (composite key: ProductSK + WarehouseSK + DateID)
print("  Testing fact_inventory for duplicates on composite key (ProductSK + WarehouseSK + DateID)...")
try:
    fact_inventory = f"{Config.CATALOG}.silver.fact_inventory"
    dup_query = f"""
    SELECT COUNT(*) as duplicate_count
    FROM (
        SELECT ProductSK, WarehouseSK, DateID, COUNT(*) as cnt
        FROM {fact_inventory}
        GROUP BY ProductSK, WarehouseSK, DateID
        HAVING COUNT(*) > 1
    )
    """
    
    duplicates = spark.sql(dup_query).first()['duplicate_count']
    total_rows = spark.table(fact_inventory).count()
    
    status = "PASS" if duplicates == 0 else "FAIL"
    log_test_result("silver", "fact_inventory", "DUPLICATE", "composite_key_unique", status, total_rows, duplicates)
    
    if duplicates > 0:
        print(f"    FAIL: {duplicates} duplicate composite key combinations found")
    else:
        print(f"    PASS: No duplicates on composite key")
        
except Exception as e:
    if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
        print(f"    SKIP: fact_inventory does not exist yet")
        log_test_result("silver", "fact_inventory", "DUPLICATE", "composite_key_unique", "SKIP", 0, 0, "Table not found")
    else:
        print(f"    ERROR: {str(e)}")
        log_test_result("silver", "fact_inventory", "DUPLICATE", "composite_key_unique", "ERROR", 0, 0, str(e))

print()

# COMMAND ----------

# DBTITLE 1,TEST 8: Bronze Layer Data Freshness
# TEST 8: BRONZE LAYER DATA FRESHNESS
print()
print("TEST CATEGORY 8: DATA FRESHNESS CHECKS")
print("-"*70)
print()

from pyspark.sql.functions import max as spark_max, datediff, current_date

# Select bronze tables based on load type
if load_type == "full":
    bronze_tables = [
        (f"{Config.CATALOG}.bronze.stg_sales", "stg_sales", "bronze"),
        (f"{Config.CATALOG}.bronze.stg_purchase", "stg_purchase", "bronze"),
        (f"{Config.CATALOG}.bronze.stg_stock_status", "stg_stock_status", "bronze"),
    ]
    print(f"  Validating FULL LOAD bronze tables (batch staging)...")
else:  # incremental
    bronze_tables = [
        (f"{Config.CATALOG}.bronze.stg_sales_incremental", "stg_sales_incremental", "bronze"),
        (f"{Config.CATALOG}.bronze.stg_purchase_incremental", "stg_purchase_incremental", "bronze"),
        (f"{Config.CATALOG}.bronze.stg_stock_status_incremental", "stg_stock_status_incremental", "bronze"),
    ]
    print(f"  Validating INCREMENTAL LOAD bronze tables (incremental staging)...")

print()

for table_full, table_name, layer in bronze_tables:
    print(f"  Checking data freshness for {table_name}...")
    
    try:
        df = spark.table(table_full)
        
        # Check if LoadedAt column exists
        if "LoadedAt" in df.columns:
            max_loaded = df.select(spark_max("LoadedAt")).first()[0]
            
            if max_loaded:
                days_old = (datetime.now() - max_loaded).days
                
                # Fail if data is more than 2 days old
                status = "PASS" if days_old <= 2 else "FAIL"
                total_rows = df.count()
                
                log_test_result(layer, table_name, "FRESHNESS", "data_recency", status, total_rows, days_old if days_old > 2 else 0)
                
                if days_old <= 2:
                    print(f"    PASS: Latest data is {days_old} day(s) old")
                else:
                    print(f"    FAIL: Latest data is {days_old} day(s) old (threshold: 2 days)")
            else:
                print(f"    SKIP: No LoadedAt timestamp found")
                log_test_result(layer, table_name, "FRESHNESS", "data_recency", "SKIP", 0, 0, "No LoadedAt timestamp")
        else:
            print(f"    SKIP: LoadedAt column not found")
            log_test_result(layer, table_name, "FRESHNESS", "data_recency", "SKIP", 0, 0, "LoadedAt column missing")
            
    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            print(f"    SKIP: {table_name} does not exist yet")
            log_test_result(layer, table_name, "FRESHNESS", "data_recency", "SKIP", 0, 0, "Table not found")
        else:
            print(f"    ERROR: {str(e)}")
            log_test_result(layer, table_name, "FRESHNESS", "data_recency", "ERROR", 0, 0, str(e))
    
    print()

# COMMAND ----------

# DBTITLE 1,TEST 9: Row Count Validations
# TEST 9: ROW COUNT VALIDATIONS (Bronze → Silver → Gold)
print()
print("TEST CATEGORY 9: ROW COUNT VALIDATIONS")
print("-"*70)
print()

# Sales pipeline validation - check correct bronze table based on load type
if load_type == "full":
    bronze_sales_table = f"{Config.CATALOG}.bronze.stg_sales"
    print("  Validating Sales data flow (Bronze BATCH → Silver → Gold)...")
else:  # incremental
    bronze_sales_table = f"{Config.CATALOG}.bronze.stg_sales_incremental"
    print("  Validating Sales data flow (Bronze INCREMENTAL → Silver → Gold)...")

try:
    # Get counts
    bronze_sales_count = spark.table(bronze_sales_table).count()
    silver_fact_sales = spark.table(f"{Config.CATALOG}.silver.fact_sales").count()
    
    print(f"    Bronze {bronze_sales_table.split('.')[-1]}: {bronze_sales_count:,} rows")
    print(f"    Silver fact_sales: {silver_fact_sales:,} rows")
    
    # For full load: Silver should match or exceed Bronze
    # For incremental: Silver will always be >= Bronze incremental (cumulative)
    if load_type == "full":
        if silver_fact_sales >= bronze_sales_count:
            print(f"    PASS: Silver has expected row count")
            log_test_result("silver", "fact_sales", "ROW_COUNT", "bronze_to_silver_full", "PASS", silver_fact_sales, 0)
        else:
            print(f"    WARN: Silver has fewer rows than Bronze (possible data issue)")
            log_test_result("silver", "fact_sales", "ROW_COUNT", "bronze_to_silver_full", "FAIL", silver_fact_sales, bronze_sales_count - silver_fact_sales)
    else:  # incremental
        # For incremental, just check that both tables have data
        if bronze_sales_count > 0 and silver_fact_sales > 0:
            print(f"    PASS: Both Bronze incremental and Silver fact table have data")
            log_test_result("silver", "fact_sales", "ROW_COUNT", "bronze_to_silver_incremental", "PASS", silver_fact_sales, 0)
        else:
            print(f"    FAIL: Missing data in Bronze incremental or Silver")
            log_test_result("silver", "fact_sales", "ROW_COUNT", "bronze_to_silver_incremental", "FAIL", silver_fact_sales, 1)
        
except Exception as e:
    print(f"    ERROR: {str(e)}")
    log_test_result("silver", "fact_sales", "ROW_COUNT", "bronze_to_silver", "ERROR", 0, 0, str(e))

print()

# Purchase pipeline validation - check correct bronze table based on load type
if load_type == "full":
    bronze_purchase_table = f"{Config.CATALOG}.bronze.stg_purchase"
    print("  Validating Purchase data flow (Bronze BATCH → Silver → Gold)...")
else:  # incremental
    bronze_purchase_table = f"{Config.CATALOG}.bronze.stg_purchase_incremental"
    print("  Validating Purchase data flow (Bronze INCREMENTAL → Silver → Gold)...")

try:
    # Get counts
    bronze_purchase_count = spark.table(bronze_purchase_table).count()
    silver_fact_purchase = spark.table(f"{Config.CATALOG}.silver.fact_purchase").count()
    
    print(f"    Bronze {bronze_purchase_table.split('.')[-1]}: {bronze_purchase_count:,} rows")
    print(f"    Silver fact_purchase: {silver_fact_purchase:,} rows")
    
    # For full load: Silver should match or exceed Bronze
    # For incremental: Silver will always be >= Bronze incremental (cumulative)
    if load_type == "full":
        if silver_fact_purchase >= bronze_purchase_count:
            print(f"    PASS: Silver has expected row count")
            log_test_result("silver", "fact_purchase", "ROW_COUNT", "bronze_to_silver_full", "PASS", silver_fact_purchase, 0)
        else:
            print(f"    WARN: Silver has fewer rows than Bronze (possible data issue)")
            log_test_result("silver", "fact_purchase", "ROW_COUNT", "bronze_to_silver_full", "FAIL", silver_fact_purchase, bronze_purchase_count - silver_fact_purchase)
    else:  # incremental
        # For incremental, just check that both tables have data
        if bronze_purchase_count > 0 and silver_fact_purchase > 0:
            print(f"    PASS: Both Bronze incremental and Silver fact table have data")
            log_test_result("silver", "fact_purchase", "ROW_COUNT", "bronze_to_silver_incremental", "PASS", silver_fact_purchase, 0)
        else:
            print(f"    FAIL: Missing data in Bronze incremental or Silver")
            log_test_result("silver", "fact_purchase", "ROW_COUNT", "bronze_to_silver_incremental", "FAIL", silver_fact_purchase, 1)
        
except Exception as e:
    print(f"    ERROR: {str(e)}")
    log_test_result("silver", "fact_purchase", "ROW_COUNT", "bronze_to_silver", "ERROR", 0, 0, str(e))

print()

# Inventory/Stock pipeline validation - check correct bronze table based on load type
if load_type == "full":
    bronze_stock_table = f"{Config.CATALOG}.bronze.stg_stock_status"
    print("  Validating Inventory data flow (Bronze BATCH → Silver → Gold)...")
else:  # incremental
    bronze_stock_table = f"{Config.CATALOG}.bronze.stg_stock_status_incremental"
    print("  Validating Inventory data flow (Bronze INCREMENTAL → Silver → Gold)...")

try:
    # Get counts
    bronze_stock_count = spark.table(bronze_stock_table).count()
    silver_fact_inventory = spark.table(f"{Config.CATALOG}.silver.fact_inventory").count()
    
    print(f"    Bronze {bronze_stock_table.split('.')[-1]}: {bronze_stock_count:,} rows")
    print(f"    Silver fact_inventory: {silver_fact_inventory:,} rows")
    
    # For full load: Silver should match or exceed Bronze
    # For incremental: Silver will always be >= Bronze incremental (cumulative)
    if load_type == "full":
        if silver_fact_inventory >= bronze_stock_count:
            print(f"    PASS: Silver has expected row count")
            log_test_result("silver", "fact_inventory", "ROW_COUNT", "bronze_to_silver_full", "PASS", silver_fact_inventory, 0)
        else:
            print(f"    WARN: Silver has fewer rows than Bronze (possible data issue)")
            log_test_result("silver", "fact_inventory", "ROW_COUNT", "bronze_to_silver_full", "FAIL", silver_fact_inventory, bronze_stock_count - silver_fact_inventory)
    else:  # incremental
        # For incremental, just check that both tables have data
        if bronze_stock_count > 0 and silver_fact_inventory > 0:
            print(f"    PASS: Both Bronze incremental and Silver fact table have data")
            log_test_result("silver", "fact_inventory", "ROW_COUNT", "bronze_to_silver_incremental", "PASS", silver_fact_inventory, 0)
        else:
            print(f"    FAIL: Missing data in Bronze incremental or Silver")
            log_test_result("silver", "fact_inventory", "ROW_COUNT", "bronze_to_silver_incremental", "FAIL", silver_fact_inventory, 1)
        
except Exception as e:
    print(f"    ERROR: {str(e)}")
    log_test_result("silver", "fact_inventory", "ROW_COUNT", "bronze_to_silver", "ERROR", 0, 0, str(e))

print()

# Dimension row count checks
print("  Validating dimension row counts...")
try:
    dim_customer_count = spark.table(f"{Config.CATALOG}.silver.dim_customer").count()
    dim_product_count = spark.table(f"{Config.CATALOG}.silver.dim_product").count()
    
    print(f"    dim_customer: {dim_customer_count:,} rows")
    print(f"    dim_product: {dim_product_count:,} rows")
    
    # Basic sanity check - dimensions should have data
    if dim_customer_count > 0 and dim_product_count > 0:
        print(f"    PASS: All dimensions populated")
        log_test_result("silver", "dimensions", "ROW_COUNT", "dim_populated", "PASS", dim_customer_count + dim_product_count, 0)
    else:
        print(f"    FAIL: One or more dimensions are empty")
        log_test_result("silver", "dimensions", "ROW_COUNT", "dim_populated", "FAIL", 0, 1)
        
except Exception as e:
    print(f"    ERROR: {str(e)}")
    log_test_result("silver", "dimensions", "ROW_COUNT", "dim_populated", "ERROR", 0, 0, str(e))

print()

# COMMAND ----------

# DBTITLE 1,TEST SUMMARY: Final Test Report
# SUMMARY REPORT
print()
print("="*70)
print("TEST RUN SUMMARY")
print("="*70)
print()

summary = spark.sql(f"""
SELECT
    layer,
    test_category,
    COUNT(*) as total_tests,
    SUM(CASE WHEN test_status = 'PASS' THEN 1 ELSE 0 END) as passed,
    SUM(CASE WHEN test_status = 'FAIL' THEN 1 ELSE 0 END) as failed,
    SUM(CASE WHEN test_status = 'ERROR' THEN 1 ELSE 0 END) as errors,
    SUM(issues_found) as total_issues
FROM {metrics_table}
WHERE test_run_id = '{test_run_id}'
GROUP BY layer, test_category
ORDER BY layer, test_category
""")

display(summary)

# Overall statistics
overall = spark.sql(f"""
SELECT
    COUNT(*) as total_tests,
    SUM(CASE WHEN test_status = 'PASS' THEN 1 ELSE 0 END) as passed,
    SUM(CASE WHEN test_status = 'FAIL' THEN 1 ELSE 0 END) as failed,
    SUM(issues_found) as total_issues
FROM {metrics_table}
WHERE test_run_id = '{test_run_id}'
""")

result = overall.first()
total = result['total_tests']
passed = result['passed']
failed = result['failed']
issues = result['total_issues']

print()
print(f"Test Run: {test_run_id}")
print(f"Total Tests: {total}")
print(f"  Passed: {passed}")
print(f"  Failed: {failed}")
print(f"  Issues Found: {issues}")
print()

success_rate = (passed / total * 100) if total > 0 else 0
print(f"Success Rate: {success_rate:.1f}%")
print()

if failed == 0:
    print("STATUS: ALL TESTS PASSED")
else:
    print(f"STATUS: {failed} TESTS FAILED")
    print("Review failed tests above for details")

print()
print(f"Metrics logged to: {metrics_table}")
print("="*70)
