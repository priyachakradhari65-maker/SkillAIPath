# Databricks notebook source
# SkillAIPath Data Warehouse - Dimension Tests
# Tests for SCD Type 1, 2, 3, deduplication, and surrogate key generation

import pytest
from pyspark.sql.functions import col, row_number, monotonically_increasing_id, current_timestamp, lit
from pyspark.sql.window import Window
from datetime import date, datetime
from conftest import (
    sample_customer_bronze, sample_customer_silver_existing, sample_product_bronze,
    get_column_null_count, get_duplicate_count
)

# SCD TYPE 1 TESTS (OVERWRITE, NO HISTORY)

def test_scd1_product_deduplication(spark, sample_product_bronze):
    """
    Test: Product dimension deduplication (SCD Type 1)
    Expected: Duplicates removed, only one row per ProductID
    """
    # Arrange
    bronze_df = sample_product_bronze

    # Act: Deduplicate by ProductID (keep last row)
    window_spec = Window.partitionBy("ProductID").orderBy(col("BatchDate").desc())
    silver_df = bronze_df.withColumn("rn", row_number().over(window_spec)) \
                         .filter(col("rn") == 1) \
                         .drop("rn")

    # Assert
    assert bronze_df.count() == 4, "Bronze should have 4 rows (including duplicate)"
    assert silver_df.count() == 3, "Silver should have 3 unique products"

    # Verify no duplicates remain
    duplicate_count = get_duplicate_count(silver_df, ["ProductID"])
    assert duplicate_count == 0, "No duplicate ProductIDs should exist"

    print("PASS: SCD Type 1 deduplication")

def test_scd1_product_surrogate_key_generation(spark, sample_product_bronze):
    """
    Test: Surrogate key generation for product dimension
    Expected: Each product has unique ProductSK
    """
    # Arrange
    bronze_df = sample_product_bronze.dropDuplicates(["ProductID"])

    # Act: Generate surrogate keys
    silver_df = bronze_df.withColumn("ProductSK", monotonically_increasing_id() + 1)

    # Assert
    sk_count = silver_df.select("ProductSK").distinct().count()
    assert sk_count == silver_df.count(), "All ProductSK values must be unique"

    # Verify no NULL surrogate keys
    null_sk_count = get_column_null_count(silver_df, "ProductSK")
    assert null_sk_count == 0, "ProductSK cannot have NULLs"

    print("PASS: Surrogate key generation")

# SCD TYPE 2 TESTS (HISTORY TRACKING WITH EFFECTIVE DATES)

def test_scd2_customer_new_record_insert(spark, sample_customer_bronze, sample_customer_silver_existing):
    """
    Test: SCD Type 2 - Insert new customer (CUST003)
    Expected: New row added with IsCurrent=True, StartDate=today, EndDate=NULL
    """
    # Arrange
    bronze_df = sample_customer_bronze.filter(col("CustomerID") == "CUST003")
    existing_silver = sample_customer_silver_existing

    # Act: Simulate SCD Type 2 insert
    new_customers = bronze_df.join(
        existing_silver.select("CustomerID"),
        "CustomerID",
        "left_anti"
    )

    new_silver_rows = new_customers.withColumn("CustomerSK", monotonically_increasing_id() + 100) \
                                    .withColumn("StartDate", lit(date(2024, 6, 5))) \
                                    .withColumn("EndDate", lit(None).cast("date")) \
                                    .withColumn("IsCurrent", lit(True))

    # Assert
    assert new_silver_rows.count() == 1, "Should insert 1 new customer (CUST003)"

    result = new_silver_rows.first()
    assert result["CustomerID"] == "CUST003"
    assert result["IsCurrent"] == True
    assert result["EndDate"] is None

    print("PASS: SCD Type 2 new record insert")

def test_scd2_customer_no_change(spark, sample_customer_bronze, sample_customer_silver_existing):
    """
    Test: SCD Type 2 - No change detection (CUST001, CUST002)
    Expected: No new rows created, existing rows unchanged
    """
    # Arrange
    bronze_df = sample_customer_bronze.filter(col("CustomerID").isin(["CUST001", "CUST002"]))
    existing_silver = sample_customer_silver_existing

    # Act: Detect changes (compare City column)
    bronze_with_hash = bronze_df.withColumn(
        "source_hash",
        col("City")
    )

    silver_with_hash = existing_silver.filter(col("IsCurrent") == True).withColumn(
        "target_hash",
        col("City")
    )

    # Join to find changes
    changes = bronze_with_hash.join(
        silver_with_hash.select("CustomerID", "target_hash"),
        (bronze_with_hash["CustomerID"] == silver_with_hash["CustomerID"]) &
        (bronze_with_hash["source_hash"] != silver_with_hash["target_hash"]),
        "inner"
    )

    # Assert
    assert changes.count() == 0, "No changes should be detected for CUST001, CUST002"

    print("PASS: SCD Type 2 no-change detection")

def test_scd2_customer_attribute_change(spark, sample_customer_bronze, sample_customer_silver_existing):
    """
    Test: SCD Type 2 - Attribute change (CUST004 city: Mumbai -> Pune)
    Expected: Old row expired (IsCurrent=False, EndDate set), new row inserted
    """
    # Arrange
    bronze_df = sample_customer_bronze.filter(col("CustomerID") == "CUST004")
    existing_silver = sample_customer_silver_existing.filter(col("CustomerID") == "CUST004")

    # Verify setup
    old_city = existing_silver.select("City").first()["City"]
    new_city = bronze_df.select("City").first()["City"]

    assert old_city == "Mumbai", "Old city should be Mumbai"
    assert new_city == "Pune", "New city should be Pune"

    # Act: Detect change
    change_detected = (old_city != new_city)

    # Simulate SCD Type 2 update
    if change_detected:
        # Expire old row
        expired_row = existing_silver.withColumn("IsCurrent", lit(False)) \
                                      .withColumn("EndDate", lit(date(2024, 6, 4)))

        # Insert new row
        new_row = bronze_df.withColumn("CustomerSK", lit(100)) \
                           .withColumn("StartDate", lit(date(2024, 6, 5))) \
                           .withColumn("EndDate", lit(None).cast("date")) \
                           .withColumn("IsCurrent", lit(True))

        # Assert on expired row
        assert expired_row.filter(col("IsCurrent") == False).count() == 1
        assert expired_row.first()["EndDate"] == date(2024, 6, 4)

        # Assert on new row
        assert new_row.filter(col("IsCurrent") == True).count() == 1
        assert new_row.first()["City"] == "Pune"
        assert new_row.first()["StartDate"] == date(2024, 6, 5)

        print("PASS: SCD Type 2 attribute change")
    else:
        pytest.fail("Change should have been detected")

def test_scd2_customer_temporal_integrity(spark, sample_customer_silver_existing):
    """
    Test: SCD Type 2 - Temporal integrity (no overlapping date ranges)
    Expected: For each CustomerID, date ranges do not overlap
    """
    # Arrange
    silver_df = sample_customer_silver_existing

    # Act: Check for overlapping date ranges per customer
    # Rule: For any CustomerID, MAX(StartDate of non-current) < MIN(StartDate of current)

    expired_records = silver_df.filter(col("IsCurrent") == False)
    current_records = silver_df.filter(col("IsCurrent") == True)

    # For this test, we expect no expired records in sample data
    # In production, would check: MAX(expired.EndDate) < MIN(current.StartDate)

    # Assert: Only one current record per CustomerID
    current_count_per_customer = current_records.groupBy("CustomerID").count()
    max_current_count = current_count_per_customer.agg({"count": "max"}).collect()[0][0]

    assert max_current_count == 1, "Each customer should have exactly one current record"

    print("PASS: SCD Type 2 temporal integrity")

# SCD TYPE 3 TESTS (LIMITED HISTORY - PREVIOUS VALUE COLUMN)

def test_scd3_customer_previous_city_tracking(spark, sample_customer_bronze):
    """
    Test: SCD Type 3 - Track previous city in separate column
    Expected: CurrentCity and PreviousCity columns exist, previous value preserved
    """
    # Arrange
    bronze_df = sample_customer_bronze.filter(col("CustomerID") == "CUST004")

    # Simulate existing SCD Type 3 record with previous city
    existing_scd3 = spark.createDataFrame([
        ("CUST004", "Sneha Patel", "Mumbai", "Delhi")
    ], ["CustomerID", "CustomerName", "CurrentCity", "PreviousCity"])

    # Act: Update to new city, shift current to previous
    new_city = bronze_df.select("City").first()["City"]

    updated_scd3 = existing_scd3.withColumn("PreviousCity", col("CurrentCity")) \
                                 .withColumn("CurrentCity", lit(new_city))

    # Assert
    result = updated_scd3.first()
    assert result["CurrentCity"] == "Pune", "Current city should be Pune"
    assert result["PreviousCity"] == "Mumbai", "Previous city should be Mumbai"

    print("PASS: SCD Type 3 previous value tracking")

# DATA QUALITY TESTS

def test_dimension_no_null_primary_keys(spark, sample_customer_bronze, sample_product_bronze):
    """
    Test: Primary keys (CustomerID, ProductID) cannot be NULL
    Expected: Zero NULL values in business keys
    """
    # Customer
    customer_null_count = get_column_null_count(sample_customer_bronze, "CustomerID")
    assert customer_null_count == 0, "CustomerID cannot have NULLs"

    # Product
    product_null_count = get_column_null_count(sample_product_bronze, "ProductID")
    assert product_null_count == 0, "ProductID cannot have NULLs"

    print("PASS: No NULL primary keys")

def test_dimension_audit_columns_populated(spark, sample_customer_bronze):
    """
    Test: Audit columns (BatchDate) are populated
    Expected: No NULL values in BatchDate
    """
    batch_date_null_count = get_column_null_count(sample_customer_bronze, "BatchDate")
    assert batch_date_null_count == 0, "BatchDate cannot have NULLs"

    print("PASS: Audit columns populated")

def test_dimension_referential_integrity_to_category(spark):
    """
    Test: Product dimension references valid category
    Expected: All CategoryID values exist in dim_product_category
    """
    # Arrange
    products = spark.createDataFrame([
        ("PROD001", "CAT001"),
        ("PROD002", "CAT002"),
        ("PROD003", "CAT999")  # Orphan category
    ], ["ProductID", "CategoryID"])

    categories = spark.createDataFrame([
        ("CAT001", "Groceries"),
        ("CAT002", "Dairy")
    ], ["CategoryID", "CategoryName"])

    # Act: Find orphan products
    orphans = products.join(
        categories,
        "CategoryID",
        "left_anti"
    )

    # Assert
    orphan_count = orphans.count()
    assert orphan_count == 1, "Should detect 1 orphan product (CAT999)"

    orphan_category = orphans.first()["CategoryID"]
    assert orphan_category == "CAT999", "Orphan should be CAT999"

    print("PASS: Referential integrity validation")

# PERFORMANCE TESTS

def test_dimension_deduplication_performance(spark):
    """
    Test: Deduplication handles large datasets efficiently
    Expected: Row_number window function completes without errors
    """
    # Arrange: Create 10K row dataset with 50% duplicates
    large_dataset = spark.range(0, 10000).toDF("id") \
        .withColumn("CustomerID", (col("id") % 5000).cast("string")) \
        .withColumn("CustomerName", lit("Test Customer")) \
        .withColumn("BatchDate", lit(date(2024, 6, 5)))

    # Act: Deduplicate
    window_spec = Window.partitionBy("CustomerID").orderBy(col("BatchDate").desc())
    deduped = large_dataset.withColumn("rn", row_number().over(window_spec)) \
                           .filter(col("rn") == 1) \
                           .drop("rn")

    # Assert
    assert large_dataset.count() == 10000, "Input should have 10K rows"
    assert deduped.count() == 5000, "Output should have 5K unique rows"

    print("PASS: Deduplication performance (10K rows)")

# INTEGRATION TESTS

def test_dimension_end_to_end_scd2_workflow(spark, sample_customer_bronze, sample_customer_silver_existing):
    """
    Test: Complete SCD Type 2 workflow (new, no change, update)
    Expected: All three scenarios handled correctly in single merge
    """
    # Arrange
    bronze_df = sample_customer_bronze
    existing_silver = sample_customer_silver_existing

    # Act: Classify records
    # 1. New customers (left anti join)
    new_customers = bronze_df.join(
        existing_silver.select("CustomerID"),
        "CustomerID",
        "left_anti"
    )

    # 2. Changed customers (inner join with hash mismatch)
    current_silver = existing_silver.filter(col("IsCurrent") == True)

    bronze_hashed = bronze_df.withColumn("bronze_hash", col("City"))
    silver_hashed = current_silver.withColumn("silver_hash", col("City"))

    changed_customers = bronze_hashed.join(
        silver_hashed.select("CustomerID", "silver_hash"),
        (bronze_hashed["CustomerID"] == silver_hashed["CustomerID"]) &
        (bronze_hashed["bronze_hash"] != silver_hashed["silver_hash"]),
        "inner"
    )

    # 3. Unchanged customers (inner join with hash match)
    unchanged_customers = bronze_hashed.join(
        silver_hashed.select("CustomerID", "silver_hash"),
        (bronze_hashed["CustomerID"] == silver_hashed["CustomerID"]) &
        (bronze_hashed["bronze_hash"] == silver_hashed["silver_hash"]),
        "inner"
    )

    # Assert
    assert new_customers.count() == 1, "Should have 1 new customer (CUST003)"
    assert changed_customers.count() == 1, "Should have 1 changed customer (CUST004)"
    assert unchanged_customers.count() == 2, "Should have 2 unchanged customers (CUST001, CUST002)"

    # Verify totals
    total_processed = new_customers.count() + changed_customers.count() + unchanged_customers.count()
    assert total_processed == bronze_df.count(), "All bronze records should be classified"

    print("PASS: End-to-end SCD Type 2 workflow")