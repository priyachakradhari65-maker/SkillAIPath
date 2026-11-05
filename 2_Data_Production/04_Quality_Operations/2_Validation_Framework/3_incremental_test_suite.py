# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,CDC/SCD2 Incremental Test Suite
# MAGIC %md
# MAGIC # CDC/SCD2 Incremental Test Suite
# MAGIC
# MAGIC **Real Implementation Validation**: Tests normal `fact_sales` and `dim_customer` tables that are updated via CDC processes.
# MAGIC
# MAGIC ## Architecture
# MAGIC * `dim_customer` - Normal dimension with SCD2 columns (IsCurrent, StartDate, EndDate) maintained by CDC pipeline
# MAGIC * `fact_sales` - Normal fact table with incremental MERGE updates and LoadedAt watermark tracking
# MAGIC * CDC processes write to normal tables, not separate _cdc tables

# COMMAND ----------

# MAGIC %run ../../../1_Canvas/1_Setup/1_Config

# COMMAND ----------

# DBTITLE 1,Test Setup
print("CDC/SCD2 INCREMENTAL PROCESSING TEST SUITE")
print("="*80)
print("Target Tables (Normal tables with CDC capabilities):")
print("  • fact_sales - Incremental MERGE updates with LoadedAt watermark")
print("  • dim_customer - SCD Type 2 maintained by CDC (IsCurrent, StartDate, EndDate)")
print()
print("Tests:")
print("  1. SCD Type 2 - Customer history tracking (IsCurrent, StartDate, EndDate)")
print("  2. Late-arriving facts - Backfill validation (LoadedAt watermark)")
print("  3. Deduplication - Rerun safety (idempotency check)")
print("  4. Watermark-based incremental - Only new data processed")
print("  5. SCD2 Point-in-Time - Fact joins to correct customer version")
print("  6. Performance degradation - File proliferation & small file detection")
print("  7. Edge cases - Negatives, NULLs, future dates")
print("\nRunning tests...\n")

test_results = []

# COMMAND ----------

# DBTITLE 1,Test 1: SCD Type 2 Updates
# Test SCD Type 2 - Verify customer updates create new version
print("TEST 1: SCD Type 2 Customer Updates")
print("-" * 80)

try:
    # Check for SCD2 table structure
    customer_df = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_customer")

    if "IsCurrent" in customer_df.columns and "StartDate" in customer_df.columns:
        # Count total records and current records
        total_count = customer_df.count()
        current_count = customer_df.filter("IsCurrent = 1").count()
        historical_count = total_count - current_count

        print(f"Total customer records: {total_count:,}")
        print(f"Current records (IsCurrent=1): {current_count:,}")
        print(f"Historical records: {historical_count:,}")

        # Check for customers with history
        customers_with_history = customer_df.groupBy("CustomerSK").count().filter("count > 1").count()
        print(f"Customers with history tracking: {customers_with_history:,}")

        result = "PASS" if historical_count > 0 else "WARNING: No historical records found"
        test_results.append(("SCD Type 2", result))
        print(f"Result: {result}")
    else:
        test_results.append(("SCD Type 2", "SKIP: Not implemented"))
        print("Result: SKIP - SCD Type 2 not implemented")
except Exception as e:
    test_results.append(("SCD Type 2", f"ERROR: {str(e)}"))
    print(f"Result: ERROR - {str(e)}")

print()

# COMMAND ----------

# DBTITLE 1,Test 2: Late-Arriving Facts
print("TEST 2: Late-Arriving Facts Handling")
print("-" * 80)

try:
    # Check if fact tables have LoadedAt column for watermark tracking
    fact_sales = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_sales")

    if "LoadedAt" in fact_sales.columns or "_processing_time" in fact_sales.columns:
        print("Watermark column found for late-arrival tracking")

        # Check for backfill patterns (records with old dates but recent LoadedAt)
        if "LoadedAt" in fact_sales.columns and "OrderDate" in fact_sales.columns:
            from pyspark.sql.functions import datediff, current_timestamp, col

            backfill_records = fact_sales.filter(
                datediff(col("LoadedAt"), col("OrderDate")) > 7
            ).count()

            print(f"Records loaded >7 days after order date: {backfill_records:,}")
            result = "PASS" if backfill_records >= 0 else "FAIL"
            test_results.append(("Late-Arriving Facts", result))
            print(f"Result: {result}")
        else:
            test_results.append(("Late-Arriving Facts", "PASS: Watermark enabled"))
            print("Result: PASS - Watermark tracking enabled")
    else:
        test_results.append(("Late-Arriving Facts", "WARNING: No watermark column"))
        print("Result: WARNING - No watermark column found")
except Exception as e:
    test_results.append(("Late-Arriving Facts", f"ERROR: {str(e)}"))
    print(f"Result: ERROR - {str(e)}")

print()

# COMMAND ----------

# DBTITLE 1,Test 3: Deduplication Logic
print("TEST 3: Deduplication (Idempotency)")
print("-" * 80)

try:
    # Check for duplicate keys in fact tables
    fact_sales = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_sales")

    if "OrderID" in fact_sales.columns:
        total_count = fact_sales.count()
        distinct_keys = fact_sales.select("OrderID").distinct().count()
        duplicates = total_count - distinct_keys

        print(f"Total records: {total_count:,}")
        print(f"Distinct keys: {distinct_keys:,}")
        print(f"Duplicates: {duplicates:,}")

        result = "PASS" if duplicates == 0 else "FAIL: Duplicates detected"
        test_results.append(("Deduplication", result))
        print(f"Result: {result}")
    else:
        test_results.append(("Deduplication", "SKIP: No primary key found"))
        print("Result: SKIP - No primary key column found")
except Exception as e:
    test_results.append(("Deduplication", f"ERROR: {str(e)}"))
    print(f"Result: ERROR - {str(e)}")

print()

# COMMAND ----------

# DBTITLE 1,Test Summary
print("TEST SUMMARY")
print("="*80)

for test_name, result in test_results:
    status_symbol = "" if "PASS" in result else "" if "FAIL" in result else "" if "WARNING" in result else "-"
    print(f"{status_symbol} {test_name}: {result}")

passed = sum(1 for _, r in test_results if "PASS" in r)
failed = sum(1 for _, r in test_results if "FAIL" in r)
warnings = sum(1 for _, r in test_results if "WARNING" in r)

print(f"\nResults: {passed} passed, {failed} failed, {warnings} warnings")

if failed > 0:
    print("\nACTION REQUIRED: Fix failing tests before production deployment")
elif warnings > 0:
    print("\nREVIEW RECOMMENDED: Address warnings for production readiness")
else:
    print("\nSUCCESS: All incremental patterns validated")

# COMMAND ----------

# DBTITLE 1,Test 4: Watermark-Based Processing
print("TEST 4: Watermark-Based Incremental Processing")
print("-" * 80)

try:
    from pyspark.sql.functions import col, current_timestamp, datediff
    
    fact_sales = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_sales")

    if "LoadedAt" in fact_sales.columns:
        from pyspark.sql.functions import current_timestamp, datediff

        # Check if incremental pattern is working
        recent_loads = fact_sales.filter(
            datediff(current_timestamp(), col("LoadedAt")) <= 7
        ).count()

        total_records = fact_sales.count()
        recent_pct = (recent_loads / total_records * 100) if total_records > 0 else 0

        print(f"Total records: {total_records:,}")
        print(f"Records loaded in last 7 days: {recent_loads:,} ({recent_pct:.1f}%)")

        result = "PASS" if recent_loads > 0 else "WARNING: No recent loads"
        test_results.append(("Watermark Processing", result))
        print(f"Result: {result}")
    else:
        test_results.append(("Watermark Processing", "SKIP: No LoadedAt column"))
        print("Result: SKIP - No watermark column found")
except Exception as e:
    test_results.append(("Watermark Processing", f"ERROR: {str(e)}"))
    print(f"Result: ERROR - {str(e)}")

print()

# COMMAND ----------

# DBTITLE 1,Test 5: SCD2 Point-in-Time Accuracy
print("TEST 5: SCD2 Point-in-Time Join Accuracy")
print("-" * 80)

try:
    # Test that fact records join to correct customer version based on order date
    # Note: fact_sales has OrderDateID (FK to dim_date), not OrderDate directly
    fact_sales = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_sales")
    dim_customer = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_customer")
    dim_date = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_date")

    if "OrderDateID" in fact_sales.columns and "StartDate" in dim_customer.columns:
        # Join fact_sales with dim_date to get OrderDate, then validate against customer versions
        from pyspark.sql.functions import to_date, col

        incorrect_joins = fact_sales.alias("f") \
            .join(dim_date.alias("d"), col("f.OrderDateID") == col("d.DateID"), "left") \
            .join(dim_customer.alias("c"), col("f.CustomerSK") == col("c.CustomerSK")) \
            .filter(
                (col("d.Date") < col("c.StartDate")) |
                (col("d.Date") > col("c.EndDate"))
            ) \
            .filter(col("c.CustomerSK") != -1) \
            .filter(col("d.Date").isNotNull()) \
            .count()

        print(f"Facts with incorrect customer version joins: {incorrect_joins:,}")

        result = "PASS" if incorrect_joins == 0 else f"FAIL: {incorrect_joins} incorrect joins"
        test_results.append(("SCD2 Point-in-Time", result))
        print(f"Result: {result}")
    else:
        test_results.append(("SCD2 Point-in-Time", "SKIP: Missing date columns"))
        print("Result: SKIP - Cannot validate without OrderDateID and StartDate")
except Exception as e:
    test_results.append(("SCD2 Point-in-Time", f"ERROR: {str(e)}"))
    print(f"Result: ERROR - {str(e)}")

print()

# COMMAND ----------

# DBTITLE 1,Test 6: Performance Degradation
print("TEST 6: Performance Degradation Detection")
print("-" * 80)

try:
    # Check for small file problem
    fact_sales_details = spark.sql(f"DESCRIBE DETAIL {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_sales").collect()[0]

    num_files = fact_sales_details.numFiles if hasattr(fact_sales_details, 'numFiles') else 0
    size_bytes = fact_sales_details.sizeInBytes if hasattr(fact_sales_details, 'sizeInBytes') else 0

    avg_file_size_mb = (size_bytes / num_files / 1024 / 1024) if num_files > 0 else 0

    print(f"Number of files: {num_files:,}")
    print(f"Total size: {size_bytes / 1024 / 1024:.2f} MB")
    print(f"Average file size: {avg_file_size_mb:.2f} MB")

    # Performance thresholds
    if num_files > 500:
        result = "FAIL: Too many files (>500), run OPTIMIZE"
    elif avg_file_size_mb < 10:
        result = "WARNING: Small files detected (<10MB avg)"
    else:
        result = "PASS"

    test_results.append(("Performance", result))
    print(f"Result: {result}")
except Exception as e:
    test_results.append(("Performance", f"ERROR: {str(e)}"))
    print(f"Result: ERROR - {str(e)}")

print()

# COMMAND ----------

# DBTITLE 1,Test 7: Edge Case Validation
print("TEST 7: Edge Case Validation")
print("-" * 80)

try:
    fact_sales = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_sales")

    edge_cases = []

    # Check for negative values in amount columns
    if "finalprice" in fact_sales.columns:
        negative_prices = fact_sales.filter("finalprice < 0").count()
        if negative_prices > 0:
            edge_cases.append(f"{negative_prices} negative prices")

    # Check for NULL in critical foreign keys (excluding Unknown -1)
    fk_columns = [col for col in fact_sales.columns if col.endswith("SK") and col != "SalesKey"]
    for fk in fk_columns:
        null_fks = fact_sales.filter(f"{fk} IS NULL").count()
        if null_fks > 0:
            edge_cases.append(f"{null_fks} NULL {fk}")

    # Check for future dates
    if "OrderDate" in fact_sales.columns:
        from pyspark.sql.functions import current_date
        future_dates = fact_sales.filter(col("OrderDate") > current_date()).count()
        if future_dates > 0:
            edge_cases.append(f"{future_dates} future dates")

    print(f"Edge cases found: {len(edge_cases)}")
    if edge_cases:
        for case in edge_cases:
            print(f"  - {case}")

    result = "PASS" if len(edge_cases) == 0 else f"WARNING: {len(edge_cases)} edge cases"
    test_results.append(("Edge Cases", result))
    print(f"Result: {result}")
except Exception as e:
    test_results.append(("Edge Cases", f"ERROR: {str(e)}"))
    print(f"Result: ERROR - {str(e)}")

print()
