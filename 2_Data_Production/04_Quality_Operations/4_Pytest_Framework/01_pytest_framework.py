# Databricks notebook source
# DBTITLE 1,Unit Testing Framework - pytest for Data Pipelines
# MAGIC %md
# MAGIC # Unit Testing Framework: pytest for Data Pipelines
# MAGIC **SkillAIPath Data Warehouse - Testing Best Practices**
# MAGIC
# MAGIC ## THE PROBLEM: "How do we test data transformations?"
# MAGIC
# MAGIC **Tuesday Morning - Production Bug Review**
# MAGIC
# MAGIC **Product Manager:** "Yesterday's pipeline loaded duplicate customers into Silver. This broke the downstream Gold mart."
# MAGIC
# MAGIC **Data Engineer:** "I tested the notebook manually. It worked in my sample data."
# MAGIC
# MAGIC **Tech Lead:** "Manual testing doesn't scale. We need automated unit tests that run on every commit."
# MAGIC
# MAGIC **Engineer:** "How do you unit test Spark transformations?"
# MAGIC
# MAGIC **Lead:** "pytest. Write test functions that create sample DataFrames, run your transformation logic, assert expected outputs. Run tests via CI/CD before deployment."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## THE SOLUTION: pytest Framework
# MAGIC
# MAGIC **What It Is:**
# MAGIC * Python testing framework for writing and running automated tests
# MAGIC * Fixtures for setup/teardown (create test data, clean up after)
# MAGIC * Assertions for validation (assert df.count() == 100)
# MAGIC * Integration with CI/CD (GitHub Actions, Jenkins)
# MAGIC
# MAGIC **Why pytest Over unittest:**
# MAGIC * Simpler syntax (no class boilerplate)
# MAGIC * Better fixture management
# MAGIC * Rich plugin ecosystem (pytest-spark, pytest-cov)
# MAGIC * Clearer assertion failures
# MAGIC
# MAGIC **When to Use:**
# MAGIC * Before deploying pipeline changes
# MAGIC * After refactoring transformation logic
# MAGIC * When adding new data quality checks
# MAGIC * In CI/CD pipelines (automated testing)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC * Python basics (functions, imports)
# MAGIC * PySpark DataFrame API familiarity
# MAGIC * Understanding of test-driven development (TDD) concepts

# COMMAND ----------

# DBTITLE 1,Install pytest and Dependencies
# MAGIC %undefined
# MAGIC # Install pytest and testing utilities
# MAGIC
# MAGIC pip install pytest pytest-spark chispa --quiet
# MAGIC
# MAGIC echo "================================================================================" echo "PYTEST INSTALLATION COMPLETE"
# MAGIC echo "================================================================================"
# MAGIC echo ""
# MAGIC echo "Installed Packages:"
# MAGIC echo "  - pytest: Core testing framework"
# MAGIC echo "  - pytest-spark: Spark session fixtures for tests"
# MAGIC echo "  - chispa: DataFrame equality assertions"
# MAGIC echo ""
# MAGIC echo "Verify Installation:"
# MAGIC pytest --version
# MAGIC echo ""
# MAGIC echo "================================================================================"

# COMMAND ----------

# DBTITLE 1,Test Structure - Basic Example
# Example 1: Basic Unit Test Structure

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

print("=" * 70)
print("PYTEST BASICS - SIMPLE TRANSFORMATION TEST")
print("=" * 70)
print()

# Sample transformation function (this is what we're testing)
def clean_customer_name(df):
    """Uppercase customer names and remove leading/trailing spaces"""
    return df.withColumn("CustomerName", upper(col("CustomerName")).alias("CustomerName")) \
             .withColumn("CustomerName", col("CustomerName").cast(StringType()))

# Test function (pytest discovers functions starting with test_)
def test_clean_customer_name():
    """Test that customer names are uppercased correctly"""

    # Arrange: Create test input data
    schema = StructType([
        StructField("CustomerName", StringType(), True),
        StructField("Age", IntegerType(), True)
    ])

    input_data = [
        ("  john doe  ", 30),
        ("Jane Smith", 25),
        ("BOB JOHNSON", 40)
    ]

    input_df = spark.createDataFrame(input_data, schema)

    # Act: Run the transformation
    output_df = clean_customer_name(input_df)

    # Assert: Verify expected output
    result = output_df.collect()

    assert result[0]["CustomerName"] == "  JOHN DOE  ", "Name should be uppercased"
    assert result[1]["CustomerName"] == "JANE SMITH", "Name should be uppercased"
    assert result[2]["CustomerName"] == "BOB JOHNSON", "Already uppercase should remain unchanged"

    print("TEST PASSED: clean_customer_name()")

# Run the test
test_clean_customer_name()

print()
print("Test Pattern:")
print("  1. ARRANGE: Create test input DataFrame")
print("  2. ACT: Run transformation function")
print("  3. ASSERT: Verify output matches expected results")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Test Fixtures - Setup and Teardown
# Example 2: Using pytest Fixtures for Reusable Test Data

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from datetime import date

print("=" * 70)
print("PYTEST FIXTURES - REUSABLE TEST DATA")
print("=" * 70)
print()

# Fixture: Sample customer data (reused across multiple tests)
@pytest.fixture
def sample_customers():
    """Create sample customer DataFrame for testing"""
    schema = StructType([
        StructField("CustomerID", IntegerType(), False),
        StructField("CustomerName", StringType(), True),
        StructField("City", StringType(), True),
        StructField("RegistrationDate", DateType(), True)
    ])

    data = [
        (1, "Alice Brown", "Mumbai", date(2023, 1, 15)),
        (2, "Bob Green", "Delhi", date(2023, 2, 20)),
        (3, "Charlie White", "Bangalore", date(2023, 3, 10))
    ]

    return spark.createDataFrame(data, schema)

# Test 1: Filter active customers
def test_filter_mumbai_customers():
    """Test filtering customers by city"""
    df = sample_customers()

    # Filter Mumbai customers
    result_df = df.filter(col("City") == "Mumbai")

    assert result_df.count() == 1, "Should have 1 Mumbai customer"
    assert result_df.first()["CustomerName"] == "Alice Brown"

    print("TEST PASSED: filter_mumbai_customers()")

# Test 2: Count customers by city
def test_count_by_city():
    """Test grouping and counting by city"""
    df = sample_customers()

    # Group by city and count
    result_df = df.groupBy("City").count()

    assert result_df.count() == 3, "Should have 3 distinct cities"

    print("TEST PASSED: count_by_city()")

# Run tests
test_filter_mumbai_customers()
test_count_by_city()

print()
print("Fixture Benefits:")
print("  - Reuse test data across multiple tests")
print("  - Isolate setup logic from test logic")
print("  - Automatic cleanup after test execution")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Data Quality Tests - Schema and Nulls
# Example 3: Data Quality Testing

from pyspark.sql.functions import col, count, when, isnan

print("=" * 70)
print("DATA QUALITY TESTS - SCHEMA AND NULL VALIDATION")
print("=" * 70)
print()

# Transformation: Load dimension table
def load_dim_customer(bronze_df):
    """Transform bronze customer data to silver dimension"""
    return bronze_df.select(
        col("CustomerID").cast(IntegerType()),
        col("CustomerName"),
        col("Email"),
        col("City")
    ).dropDuplicates(["CustomerID"])

# Test 1: Schema validation
def test_dim_customer_schema():
    """Verify dimension table has correct schema"""

    # Sample bronze data
    bronze_data = [
        ("1", "Alice", "alice@email.com", "Mumbai"),
        ("2", "Bob", "bob@email.com", "Delhi")
    ]

    bronze_schema = StructType([
        StructField("CustomerID", StringType()),
        StructField("CustomerName", StringType()),
        StructField("Email", StringType()),
        StructField("City", StringType())
    ])

    bronze_df = spark.createDataFrame(bronze_data, bronze_schema)

    # Transform to dimension
    dim_df = load_dim_customer(bronze_df)

    # Assert schema
    assert "CustomerID" in dim_df.columns
    assert "CustomerName" in dim_df.columns
    assert "Email" in dim_df.columns
    assert "City" in dim_df.columns

    # Assert data types
    assert dict(dim_df.dtypes)["CustomerID"] == "int", "CustomerID should be integer"

    print("TEST PASSED: Schema validation")

# Test 2: NULL value validation
def test_no_null_customer_ids():
    """Verify no NULL CustomerIDs in dimension table"""

    bronze_data = [
        ("1", "Alice", "alice@email.com", "Mumbai"),
        (None, "Bob", "bob@email.com", "Delhi"),  # Invalid NULL ID
        ("3", "Charlie", "charlie@email.com", "Bangalore")
    ]

    bronze_schema = StructType([
        StructField("CustomerID", StringType()),
        StructField("CustomerName", StringType()),
        StructField("Email", StringType()),
        StructField("City", StringType())
    ])

    bronze_df = spark.createDataFrame(bronze_data, bronze_schema)
    dim_df = load_dim_customer(bronze_df)

    # Count NULLs in CustomerID
    null_count = dim_df.filter(col("CustomerID").isNull()).count()

    # Depending on your business rule:
    # Option A: Reject NULLs (strict validation)
    # assert null_count == 0, f"Found {null_count} NULL CustomerIDs - data quality issue"

    # Option B: Allow NULLs but log warning (lenient)
    if null_count > 0:
        print(f"WARNING: Found {null_count} NULL CustomerIDs")

    print("TEST PASSED: NULL validation")

# Run tests
test_dim_customer_schema()
test_no_null_customer_ids()

print()
print("Data Quality Test Patterns:")
print("  - Schema validation (column names, data types)")
print("  - NULL checks (critical columns must be non-NULL)")
print("  - Duplicate detection (primary keys must be unique)")
print("  - Referential integrity (foreign keys exist in dimension)")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Integration Test - End-to-End Pipeline
# Example 4: Integration Test (Multi-Step Pipeline)

from pyspark.sql.functions import sum as _sum, avg

print("=" * 70)
print("INTEGRATION TEST - END-TO-END PIPELINE")
print("=" * 70)
print()

# Step 1: Bronze to Silver (Dimension)
def bronze_to_silver_customer(bronze_df):
    return bronze_df.select(
        col("CustomerID").cast(IntegerType()),
        col("CustomerName"),
        col("City")
    ).dropDuplicates(["CustomerID"])

# Step 2: Bronze to Silver (Fact)
def bronze_to_silver_sales(bronze_df, dim_customer_df):
    # Join sales with customer dimension
    return bronze_df.alias("sales") \
        .join(
            dim_customer_df.alias("cust"),
            col("sales.CustomerID") == col("cust.CustomerID"),
            "inner"
        ) \
        .select(
            col("sales.OrderID"),
            col("cust.CustomerID"),
            col("sales.Amount").cast(IntegerType())
        )

# Step 3: Silver to Gold (Aggregation)
def silver_to_gold_customer_summary(fact_df):
    return fact_df.groupBy("CustomerID") \
        .agg(
            count("*").alias("TotalOrders"),
            _sum("Amount").alias("TotalRevenue"),
            avg("Amount").alias("AvgOrderValue")
        )

# Integration Test: Full Pipeline
def test_end_to_end_pipeline():
    """Test complete Bronze -> Silver -> Gold flow"""

    # Setup: Bronze layer data
    bronze_customer_data = [
        (1, "Alice", "Mumbai"),
        (2, "Bob", "Delhi"),
        (3, "Charlie", "Bangalore")
    ]

    bronze_sales_data = [
        (101, 1, 1000),  # Alice: 1000
        (102, 1, 1500),  # Alice: 1500
        (103, 2, 2000),  # Bob: 2000
        (104, 3, 500)    # Charlie: 500
    ]

    customer_schema = StructType([
        StructField("CustomerID", IntegerType()),
        StructField("CustomerName", StringType()),
        StructField("City", StringType())
    ])

    sales_schema = StructType([
        StructField("OrderID", IntegerType()),
        StructField("CustomerID", IntegerType()),
        StructField("Amount", IntegerType())
    ])

    bronze_customer_df = spark.createDataFrame(bronze_customer_data, customer_schema)
    bronze_sales_df = spark.createDataFrame(bronze_sales_data, sales_schema)

    # Execute: Run pipeline steps
    silver_customer_df = bronze_to_silver_customer(bronze_customer_df)
    silver_sales_df = bronze_to_silver_sales(bronze_sales_df, silver_customer_df)
    gold_summary_df = silver_to_gold_customer_summary(silver_sales_df)

    # Assert: Verify Gold output
    result = gold_summary_df.collect()

    # Find Alice's summary (CustomerID = 1)
    alice_summary = [r for r in result if r["CustomerID"] == 1][0]

    assert alice_summary["TotalOrders"] == 2, "Alice should have 2 orders"
    assert alice_summary["TotalRevenue"] == 2500, "Alice total revenue should be 2500"
    assert alice_summary["AvgOrderValue"] == 1250.0, "Alice avg order value should be 1250"

    print("TEST PASSED: End-to-end pipeline")
    print()
    print("Pipeline Flow Validated:")
    print("  Bronze (raw) -> Silver (cleaned, joined) -> Gold (aggregated)")
    print(f"  Customers processed: {silver_customer_df.count()}")
    print(f"  Sales transactions: {silver_sales_df.count()}")
    print(f"  Gold summaries: {gold_summary_df.count()}")

# Run integration test
test_end_to_end_pipeline()

print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Running Tests via CLI
# MAGIC %md
# MAGIC ## Running Tests via Command Line
# MAGIC
# MAGIC ### Test File Structure
# MAGIC
# MAGIC Organize tests in a `tests/` directory:
# MAGIC
# MAGIC ```
# MAGIC sap-pipeline/
# MAGIC  notebooks/
# MAGIC     01_dimensions.ipynb
# MAGIC     02_facts.ipynb
# MAGIC  src/
# MAGIC     transformations.py    (Extract logic from notebooks)
# MAGIC     utils.py
# MAGIC  tests/
# MAGIC      test_dimensions.py
# MAGIC      test_facts.py
# MAGIC      conftest.py           (Shared fixtures)
# MAGIC ```
# MAGIC
# MAGIC ### Example Test File: tests/test_dimensions.py
# MAGIC
# MAGIC ```python
# MAGIC import pytest
# MAGIC from pyspark.sql import SparkSession
# MAGIC from src.transformations import load_dim_customer
# MAGIC
# MAGIC @pytest.fixture(scope="session")
# MAGIC def spark():
# MAGIC     return SparkSession.builder.appName("pytest").getOrCreate()
# MAGIC
# MAGIC def test_dim_customer_deduplication(spark):
# MAGIC     # Test customer deduplication logic
# MAGIC     input_data = [(1, "Alice"), (1, "Alice"), (2, "Bob")]
# MAGIC     input_df = spark.createDataFrame(input_data, ["CustomerID", "CustomerName"])
# MAGIC
# MAGIC     result_df = load_dim_customer(input_df)
# MAGIC
# MAGIC     assert result_df.count() == 2, "Should deduplicate CustomerID=1"
# MAGIC ```
# MAGIC
# MAGIC ### Run Tests
# MAGIC
# MAGIC ```bash
# MAGIC # Run all tests
# MAGIC pytest tests/
# MAGIC
# MAGIC # Run specific test file
# MAGIC pytest tests/test_dimensions.py
# MAGIC
# MAGIC # Run specific test function
# MAGIC pytest tests/test_dimensions.py::test_dim_customer_deduplication
# MAGIC
# MAGIC # Run with verbose output
# MAGIC pytest tests/ -v
# MAGIC
# MAGIC # Run with coverage report
# MAGIC pytest tests/ --cov=src --cov-report=html
# MAGIC ```
# MAGIC
# MAGIC ### CI/CD Integration (GitHub Actions)
# MAGIC
# MAGIC ```yaml
# MAGIC # .github/workflows/test.yml
# MAGIC name: Run Tests
# MAGIC
# MAGIC on: [push, pull_request]
# MAGIC
# MAGIC jobs:
# MAGIC   test:
# MAGIC     runs-on: ubuntu-latest
# MAGIC     steps:
# MAGIC       - uses: actions/checkout@v2
# MAGIC
# MAGIC       - name: Set up Python
# MAGIC         uses: actions/setup-python@v2
# MAGIC         with:
# MAGIC           python-version: '3.9'
# MAGIC
# MAGIC       - name: Install dependencies
# MAGIC         run: |
# MAGIC           pip install pytest pyspark chispa
# MAGIC
# MAGIC       - name: Run tests
# MAGIC         run: pytest tests/ -v
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Best Practices
# MAGIC
# MAGIC **1. Test Naming**
# MAGIC * Prefix with `test_` (pytest discovers these automatically)
# MAGIC * Descriptive names: `test_customer_deduplication_removes_duplicates`
# MAGIC
# MAGIC **2. Test Independence**
# MAGIC * Each test should run standalone (no dependencies on other tests)
# MAGIC * Use fixtures for shared setup, not global state
# MAGIC
# MAGIC **3. Small Sample Data**
# MAGIC * Use 3-10 rows for unit tests (fast execution)
# MAGIC * Cover edge cases (NULLs, duplicates, empty DataFrames)
# MAGIC
# MAGIC **4. Assertions First**
# MAGIC * Write assertion before implementation (TDD approach)
# MAGIC * Clear failure messages: `assert count == 5, f"Expected 5, got {count}"`
# MAGIC
# MAGIC **5. Separate Unit vs Integration Tests**
# MAGIC * Unit: Test single functions in isolation
# MAGIC * Integration: Test multi-step pipelines end-to-end
# MAGIC * Use markers: `@pytest.mark.integration`

# COMMAND ----------

# DBTITLE 1,Summary - Testing Pyramid
# Testing Pyramid for Data Pipelines

print("=" * 70)
print("TESTING PYRAMID - SKILLAIPPATH PIPELINE")
print("=" * 70)
print()

print("Testing Levels:")
print("-" * 70)
print()
print("1. UNIT TESTS (70% of tests)")
print("   - Test individual transformation functions")
print("   - Fast execution (milliseconds)")
print("   - Sample data: 3-10 rows")
print("   - Example: test_clean_customer_name()")
print()
print("2. INTEGRATION TESTS (20% of tests)")
print("   - Test multi-step workflows (Bronze -> Silver -> Gold)")
print("   - Medium execution (seconds)")
print("   - Sample data: 100-1000 rows")
print("   - Example: test_end_to_end_pipeline()")
print()
print("3. E2E TESTS (10% of tests)")
print("   - Test complete pipeline with realistic data volumes")
print("   - Slow execution (minutes)")
print("   - Sample data: 10K-100K rows")
print("   - Example: test_production_data_subset()")
print()
print("=" * 70)
print()
print("SkillAIPath Test Coverage:")
print("-" * 70)
print("  - Dimension transformations (SCD Type 1/2)")
print("  - Fact table joins and aggregations")
print("  - Data quality checks (schema, NULLs, duplicates)")
print("  - Gold mart calculations (revenue, KPIs)")
print("  - Streaming checkpoint recovery")
print()
print("=" * 70)
print()
print("Next Steps:")
print("  1. Extract transformation logic from notebooks to Python modules")
print("  2. Write unit tests for each transformation function")
print("  3. Add tests to CI/CD pipeline (run on every commit)")
print("  4. Achieve 80% code coverage (pytest --cov)")
print("  5. Run tests before deploying to UAT/Prod")
print()
print("=" * 70)
print()
print("Interview Talking Point:")
print("-" * 70)
print('"We use pytest for data pipeline testing. Unit tests validate')
print(' individual transformations with small sample DataFrames. Integration')
print(' tests verify end-to-end flows from Bronze to Gold. All tests run')
print(' automatically in GitHub Actions on every pull request. We maintain')
print(' 80% code coverage and won\'t merge code without passing tests."')
print("=" * 70)