# Databricks notebook source
# SkillAIPath Data Warehouse - Data Quality Tests
# Tests for validation gates, NULL checks, schema enforcement, and data profiling

import pytest
from datetime import date

# SCHEMA VALIDATION TESTS

def test_bronze_customer_schema_enforcement(spark):
    """
    Test: Bronze customer schema matches expected structure
    Expected: All required columns present with correct data types
    """
    # Arrange: Expected schema
    expected_schema = StructType([
        StructField("CustomerID", StringType(), False),
        StructField("CustomerName", StringType(), True),
        StructField("Email", StringType(), True),
        StructField("Phone", StringType(), True),
        StructField("City", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Address", StringType(), True),
        StructField("BatchDate", DateType(), False)
    ])

    # Act: Create sample data with expected schema
    sample_data = [
        ("CUST001", "Raj Kumar", "raj@email.com", "9876543210", "Mumbai", "Maharashtra", "Andheri", date(2024, 6, 5))
    ]

    df = spark.createDataFrame(sample_data, expected_schema)

    # Assert: Schema matches
    assert df.schema == expected_schema, "Bronze customer schema must match expected structure"

    # Verify column count
    assert len(df.columns) == 8, "Should have exactly 8 columns"

    print("PASS: Bronze customer schema enforcement")

def test_silver_dimension_schema_scd2(spark):
    """
    Test: Silver dimension (SCD Type 2) has required temporal columns
    Expected: CustomerSK, StartDate, EndDate, IsCurrent columns present
    """
    # Arrange
    expected_columns = [
        "CustomerSK", "CustomerID", "CustomerName",
        "StartDate", "EndDate", "IsCurrent"
    ]

    sample_data = [
        (1, "CUST001", "Raj Kumar", date(2024, 1, 1), None, True)
    ]

    schema = StructType([
        StructField("CustomerSK", IntegerType(), False),
        StructField("CustomerID", StringType(), False),
        StructField("CustomerName", StringType(), True),
        StructField("StartDate", DateType(), False),
        StructField("EndDate", DateType(), True),
        StructField("IsCurrent", BooleanType(), False)
    ])

    df = spark.createDataFrame(sample_data, schema)

    # Assert: All SCD Type 2 columns present
    for col_name in expected_columns:
        assert col_name in df.columns, f"{col_name} must be present in SCD Type 2 dimension"

    print("PASS: Silver dimension SCD Type 2 schema")

# NULL VALIDATION TESTS

def test_critical_columns_no_nulls(spark):
    """
    Test: Critical business columns cannot have NULLs
    Expected: CustomerID, ProductID, SalesDate have zero NULLs
    """
    # Arrange: Sample data with potential NULL values
    customer_data = [
        ("CUST001", "Raj Kumar", "Mumbai"),
        (None, "Invalid Customer", "Delhi"),  # NULL CustomerID
        ("CUST003", "Amit Sharma", "Bangalore")
    ]

    df = spark.createDataFrame(customer_data, ["CustomerID", "CustomerName", "City"])

    # Act: Count NULLs in CustomerID
    null_count = get_column_null_count(df, "CustomerID")
    total_count = df.count()

    # Assert: Critical columns cannot have NULLs
    assert null_count > 0, "Test data should contain NULL for validation"

    # Validation rule: Reject records with NULL CustomerID
    validated_df = df.filter(col("CustomerID").isNotNull())
    assert validated_df.count() == 2, "Should filter out 1 record with NULL CustomerID"

    print("PASS: Critical column NULL validation")

def test_null_percentage_threshold(spark):
    """
    Test: Non-critical columns can have NULLs but below threshold
    Expected: NULL percentage <= 5% for optional columns
    """
    # Arrange: 100 customers, 3 with NULL email (3%)
    data = [(f"CUST{i:03d}", None if i < 3 else f"cust{i}@email.com") for i in range(100)]
    df = spark.createDataFrame(data, ["CustomerID", "Email"])

    # Act: Calculate NULL percentage
    total_count = df.count()
    null_count = get_column_null_count(df, "Email")
    null_percentage = (null_count / total_count * 100) if total_count > 0 else 0

    # Assert: Within acceptable threshold
    assert null_percentage == 3.0, "Should have 3% NULLs"
    assert null_percentage <= MAX_NULL_PERCENTAGE, f"NULL percentage {null_percentage}% exceeds threshold {MAX_NULL_PERCENTAGE}%"

    print("PASS: NULL percentage threshold validation")

# DATA COMPLETENESS TESTS

def test_row_count_validation(spark):
    """
    Test: Bronze to Silver row count reconciliation
    Expected: No unexpected data loss during transformation
    """
    # Arrange: Bronze layer (5 rows)
    bronze_data = [(f"CUST{i:03d}", f"Customer {i}") for i in range(5)]
    bronze_df = spark.createDataFrame(bronze_data, ["CustomerID", "CustomerName"])

    # Act: Transform to Silver (deduplication)
    silver_df = bronze_df.dropDuplicates(["CustomerID"])

    # Assert: Row counts reconcile
    bronze_count = bronze_df.count()
    silver_count = silver_df.count()

    assert bronze_count == 5, "Bronze should have 5 rows"
    assert silver_count == 5, "Silver should have 5 unique rows (no duplicates in test data)"

    print("PASS: Row count validation")

def test_column_completeness_check(spark):
    """
    Test: All required columns present after transformation
    Expected: No columns dropped accidentally
    """
    # Arrange: Bronze data
    bronze_data = [("CUST001", "Raj Kumar", "Mumbai")]
    bronze_df = spark.createDataFrame(bronze_data, ["CustomerID", "CustomerName", "City"])

    # Act: Transform (simulated - add surrogate key)
    silver_df = bronze_df.withColumn("CustomerSK", col("CustomerID"))

    # Assert: All bronze columns still present + new column
    bronze_columns = set(bronze_df.columns)
    silver_columns = set(silver_df.columns)

    assert bronze_columns.issubset(silver_columns), "All bronze columns must be preserved in silver"
    assert "CustomerSK" in silver_columns, "Surrogate key column must be added"

    print("PASS: Column completeness check")

# DATA FORMAT VALIDATION TESTS

def test_email_format_validation(spark):
    """
    Test: Email addresses follow valid format
    Expected: Contains @ symbol and domain
    """
    # Arrange
    data = [
        ("CUST001", "raj@email.com"),      # Valid
        ("CUST002", "invalid.email"),      # Invalid (no @)
        ("CUST003", "priya@domain.co.in")  # Valid
    ]
    df = spark.createDataFrame(data, ["CustomerID", "Email"])

    # Act: Validate email format
    validated_df = df.withColumn(
        "IsValidEmail",
        col("Email").rlike(r"^[\w\.-]+@[\w\.-]+\.\w+$")
    )

    # Assert: Invalid emails detected
    invalid_count = validated_df.filter(col("IsValidEmail") == False).count()
    assert invalid_count == 1, "Should detect 1 invalid email"

    invalid_email = validated_df.filter(col("IsValidEmail") == False).first()["Email"]
    assert invalid_email == "invalid.email"

    print("PASS: Email format validation")

def test_phone_format_validation(spark):
    """
    Test: Indian phone numbers have 10 digits
    Expected: Length = 10, all numeric
    """
    # Arrange
    data = [
        ("CUST001", "9876543210"),   # Valid
        ("CUST002", "987654321"),    # Invalid (9 digits)
        ("CUST003", "98765432AB")    # Invalid (non-numeric)
    ]
    df = spark.createDataFrame(data, ["CustomerID", "Phone"])

    # Act: Validate phone format
    validated_df = df.withColumn(
        "IsValidPhone",
        (length(col("Phone")) == 10) & col("Phone").rlike(r"^[0-9]{10}$")
    )

    # Assert: Invalid phones detected
    valid_count = validated_df.filter(col("IsValidPhone") == True).count()
    assert valid_count == 1, "Should have 1 valid phone number"

    print("PASS: Phone format validation")

def test_date_range_validation(spark):
    """
    Test: Dates fall within expected business range
    Expected: SalesDate between 2020-01-01 and today
    """
    # Arrange
    data = [
        ("SALE001", date(2024, 6, 5)),   # Valid
        ("SALE002", date(2019, 12, 31)), # Invalid (too old)
        ("SALE003", date(2025, 12, 31))  # Invalid (future date)
    ]
    df = spark.createDataFrame(data, ["SalesID", "SalesDate"])

    # Act: Validate date range
    min_date = date(2020, 1, 1)
    max_date = date(2024, 12, 31)

    validated_df = df.withColumn(
        "IsValidDate",
        (col("SalesDate") >= min_date) & (col("SalesDate") <= max_date)
    )

    # Assert: Out-of-range dates detected
    invalid_count = validated_df.filter(col("IsValidDate") == False).count()
    assert invalid_count == 2, "Should detect 2 invalid dates"

    print("PASS: Date range validation")

# BUSINESS RULE VALIDATION TESTS

def test_sales_amount_positive_validation(spark):
    """
    Test: Sales amounts must be positive
    Expected: Negative or zero amounts flagged
    """
    # Arrange
    data = [
        ("SALE001", Decimal("100.00")),  # Valid
        ("SALE002", Decimal("0.00")),    # Invalid (zero)
        ("SALE003", Decimal("-50.00"))   # Invalid (negative)
    ]
    df = spark.createDataFrame(data, ["SalesID", "TotalAmount"])

    # Act: Validate positive amounts
    validated_df = df.withColumn(
        "IsValidAmount",
        col("TotalAmount") > 0
    )

    # Assert: Invalid amounts detected
    invalid_count = validated_df.filter(col("IsValidAmount") == False).count()
    assert invalid_count == 2, "Should detect 2 invalid amounts"

    print("PASS: Sales amount positive validation")

def test_quantity_positive_integer_validation(spark):
    """
    Test: Order quantities must be positive integers
    Expected: Negative, zero, or non-integer quantities flagged
    """
    # Arrange
    data = [
        ("SALE001", 10),   # Valid
        ("SALE002", 0),    # Invalid (zero)
        ("SALE003", -5)    # Invalid (negative)
    ]
    df = spark.createDataFrame(data, ["SalesID", "Quantity"])

    # Act: Validate positive integers
    validated_df = df.withColumn(
        "IsValidQuantity",
        col("Quantity") > 0
    )

    # Assert: Invalid quantities detected
    valid_count = validated_df.filter(col("IsValidQuantity") == True).count()
    assert valid_count == 1, "Should have 1 valid quantity"

    print("PASS: Quantity positive integer validation")

def test_cibil_score_range_validation(spark):
    """
    Test: CIBIL scores must be between 300 and 900
    Expected: Out-of-range scores flagged
    """
    # Arrange
    data = [
        ("CUST001", 750),  # Valid
        ("CUST002", 250),  # Invalid (below range)
        ("CUST003", 950)   # Invalid (above range)
    ]
    df = spark.createDataFrame(data, ["CustomerID", "CibilScore"])

    # Act: Validate CIBIL score range
    validated_df = df.withColumn(
        "IsValidCibil",
        (col("CibilScore") >= 300) & (col("CibilScore") <= 900)
    )

    # Assert: Invalid scores detected
    invalid_count = validated_df.filter(col("IsValidCibil") == False).count()
    assert invalid_count == 2, "Should detect 2 invalid CIBIL scores"

    print("PASS: CIBIL score range validation")

# DUPLICATE DETECTION TESTS

def test_duplicate_primary_key_detection(spark):
    """
    Test: Detect duplicate primary keys in source data
    Expected: Duplicates identified for removal
    """
    # Arrange: Data with duplicates
    data = [
        ("CUST001", "Raj Kumar"),
        ("CUST002", "Priya Singh"),
        ("CUST001", "Raj Kumar Duplicate")  # Duplicate CustomerID
    ]
    df = spark.createDataFrame(data, ["CustomerID", "CustomerName"])

    # Act: Detect duplicates
    duplicate_check = df.groupBy("CustomerID").count().filter(col("count") > 1)

    # Assert: Duplicates detected
    duplicate_count = duplicate_check.count()
    assert duplicate_count == 1, "Should detect 1 duplicate CustomerID"

    duplicate_id = duplicate_check.first()["CustomerID"]
    assert duplicate_id == "CUST001"

    print("PASS: Duplicate primary key detection")

def test_duplicate_natural_key_combination(spark):
    """
    Test: Detect duplicates based on composite natural key
    Expected: Same customer-product-date combinations flagged
    """
    # Arrange: Sales data with duplicate natural keys
    data = [
        ("SALE001", "CUST001", "PROD001", date(2024, 6, 5)),
        ("SALE002", "CUST001", "PROD002", date(2024, 6, 5)),
        ("SALE003", "CUST001", "PROD001", date(2024, 6, 5))  # Duplicate natural key
    ]
    df = spark.createDataFrame(data, ["SalesID", "CustomerID", "ProductID", "SalesDate"])

    # Act: Detect composite key duplicates
    composite_key = ["CustomerID", "ProductID", "SalesDate"]
    duplicate_check = df.groupBy(composite_key).count().filter(col("count") > 1)

    # Assert: Composite duplicates detected
    assert duplicate_check.count() == 1, "Should detect 1 duplicate composite key"

    print("PASS: Duplicate composite natural key detection")

# DATA PROFILING TESTS

def test_data_profiling_summary_statistics(spark):
    """
    Test: Generate summary statistics for numeric columns
    Expected: Min, max, avg, stddev calculated correctly
    """
    # Arrange: Sample sales data
    data = [(100.0,), (200.0,), (300.0,), (400.0,), (500.0,)]
    df = spark.createDataFrame(data, ["Amount"])

    # Act: Calculate summary statistics
    stats = df.select(
        count("Amount").alias("Count"),
        _sum("Amount").alias("Total"),
        avg("Amount").alias("Average")
    ).first()

    # Assert: Statistics correct
    assert stats["Count"] == 5
    assert stats["Total"] == 1500.0
    assert stats["Average"] == 300.0

    print("PASS: Data profiling summary statistics")

def test_data_profiling_distinct_value_count(spark):
    """
    Test: Count distinct values (cardinality check)
    Expected: Distinct count matches expected cardinality
    """
    # Arrange: City data with duplicates
    data = [("Mumbai",), ("Delhi",), ("Mumbai",), ("Bangalore",), ("Delhi",)]
    df = spark.createDataFrame(data, ["City"])

    # Act: Count distinct cities
    distinct_count = df.select("City").distinct().count()

    # Assert: 3 unique cities
    assert distinct_count == 3, "Should have 3 distinct cities"

    print("PASS: Data profiling distinct value count")

# INTEGRATION TEST

def test_end_to_end_data_quality_gate(spark):
    """
    Test: Complete data quality validation gate
    Expected: Invalid records rejected, valid records pass through
    """
    # Arrange: Mixed quality data
    data = [
        ("CUST001", "Raj Kumar", "raj@email.com", "9876543210", 750, Decimal("100.00")),  # Valid
        (None, "Invalid", "bad.email", "987654321", 750, Decimal("100.00")),               # NULL CustomerID
        ("CUST003", "Amit", "amit@email.com", "9876543210", 250, Decimal("100.00")),      # Invalid CIBIL
        ("CUST004", "Priya", "priya@email.com", "9876543210", 800, Decimal("-50.00"))    # Negative amount
    ]

    schema = ["CustomerID", "CustomerName", "Email", "Phone", "CibilScore", "Amount"]
    df = spark.createDataFrame(data, schema)

    # Act: Apply all validations
    validated_df = df.withColumn(
        "IsValid",
        col("CustomerID").isNotNull() &
        col("Email").rlike(r"^[\w\.-]+@[\w\.-]+\.\w+$") &
        (length(col("Phone")) == 10) &
        (col("CibilScore") >= 300) & (col("CibilScore") <= 900) &
        (col("Amount") > 0)
    )

    # Assert: Only 1 valid record
    valid_records = validated_df.filter(col("IsValid") == True)
    invalid_records = validated_df.filter(col("IsValid") == False)

    assert valid_records.count() == 1, "Should have 1 valid record"
    assert invalid_records.count() == 3, "Should have 3 invalid records"

    print("PASS: End-to-end data quality gate")