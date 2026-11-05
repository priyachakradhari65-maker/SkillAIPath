# Databricks notebook source
# SkillAIPath Data Warehouse - pytest Configuration
# Shared fixtures and utilities for all test modules

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DecimalType, DateType, TimestampType, BooleanType
)
from datetime import date, datetime, timedelta
from decimal import Decimal

# Spark Session Fixture

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .appName("SkillAIPath-Tests") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .getOrCreate()

    yield spark
    spark.stop()

# Dimension Fixtures

@pytest.fixture
def sample_customer_bronze(spark):
    schema = StructType([
        StructField("CustomerID", StringType(), False),
        StructField("CustomerName", StringType(), True),
        StructField("Email", StringType(), True),
        StructField("Phone", StringType(), True),
        StructField("City", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Address", StringType(), True),
        StructField("BatchDate", DateType(), False)
    ])

    data = [
        ("CUST001", "Raj Kumar", "raj@email.com", "9876543210", "Mumbai", "Maharashtra", "Andheri", date(2024, 6, 5)),
        ("CUST002", "Priya Singh", "priya@email.com", "9876543211", "Delhi", "Delhi", "Connaught Place", date(2024, 6, 5)),
        ("CUST003", "Amit Sharma", "amit@email.com", "9876543212", "Bangalore", "Karnataka", "Koramangala", date(2024, 6, 5)),
        ("CUST004", "Sneha Patel", "sneha@email.com", "9876543213", "Pune", "Maharashtra", "Viman Nagar", date(2024, 6, 5))
    ]

    return spark.createDataFrame(data, schema)

@pytest.fixture
def sample_customer_silver_existing(spark):
    schema = StructType([
        StructField("CustomerSK", IntegerType(), False),
        StructField("CustomerID", StringType(), False),
        StructField("CustomerName", StringType(), True),
        StructField("Email", StringType(), True),
        StructField("Phone", StringType(), True),
        StructField("City", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Address", StringType(), True),
        StructField("StartDate", DateType(), False),
        StructField("EndDate", DateType(), True),
        StructField("IsCurrent", BooleanType(), False)
    ])

    data = [
        (1, "CUST001", "Raj Kumar", "raj@email.com", "9876543210", "Mumbai", "Maharashtra", "Andheri", date(2024, 1, 1), None, True),
        (2, "CUST002", "Priya Singh", "priya@email.com", "9876543211", "Delhi", "Delhi", "Connaught Place", date(2024, 1, 1), None, True),
        (3, "CUST004", "Sneha Patel", "sneha@email.com", "9876543213", "Mumbai", "Maharashtra", "Bandra", date(2024, 1, 1), None, True)
    ]

    return spark.createDataFrame(data, schema)

@pytest.fixture
def sample_product_bronze(spark):
    schema = StructType([
        StructField("ProductID", StringType(), False),
        StructField("ProductName", StringType(), True),
        StructField("CategoryID", StringType(), True),
        StructField("Brand", StringType(), True),
        StructField("UnitPrice", DecimalType(10, 2), True),
        StructField("BatchDate", DateType(), False)
    ])

    data = [
        ("PROD001", "Tata Salt 1kg", "CAT001", "Tata", Decimal("20.00"), date(2024, 6, 5)),
        ("PROD002", "Amul Butter 100g", "CAT002", "Amul", Decimal("50.00"), date(2024, 6, 5)),
        ("PROD003", "Maggi Noodles", "CAT003", "Nestle", Decimal("12.00"), date(2024, 6, 5)),
        ("PROD001", "Tata Salt 1kg", "CAT001", "Tata", Decimal("20.00"), date(2024, 6, 5))
    ]

    return spark.createDataFrame(data, schema)

# Fact Fixtures

@pytest.fixture
def sample_fact_sales_bronze(spark):
    schema = StructType([
        StructField("SalesID", StringType(), False),
        StructField("CustomerID", StringType(), False),
        StructField("ProductID", StringType(), False),
        StructField("WarehouseID", StringType(), False),
        StructField("Quantity", IntegerType(), True),
        StructField("UnitPrice", DecimalType(10, 2), True),
        StructField("TotalAmount", DecimalType(10, 2), True),
        StructField("SalesDate", DateType(), False),
        StructField("BatchDate", DateType(), False)
    ])

    data = [
        ("SALE001", "CUST001", "PROD001", "WH001", 10, Decimal("20.00"), Decimal("200.00"), date(2024, 6, 5), date(2024, 6, 5)),
        ("SALE002", "CUST002", "PROD002", "WH001", 5, Decimal("50.00"), Decimal("250.00"), date(2024, 6, 5), date(2024, 6, 5)),
        ("SALE003", "CUST003", "PROD003", "WH002", 20, Decimal("12.00"), Decimal("240.00"), date(2024, 6, 5), date(2024, 6, 5)),
        ("SALE004", "CUST999", "PROD001", "WH001", 15, Decimal("20.00"), Decimal("300.00"), date(2024, 6, 5), date(2024, 6, 5))
    ]

    return spark.createDataFrame(data, schema)

@pytest.fixture
def sample_dim_customer_for_facts(spark):
    schema = StructType([
        StructField("CustomerSK", IntegerType(), False),
        StructField("CustomerID", StringType(), False)
    ])

    data = [
        (1, "CUST001"),
        (2, "CUST002"),
        (3, "CUST003")
    ]

    return spark.createDataFrame(data, schema)

@pytest.fixture
def sample_dim_product_for_facts(spark):
    schema = StructType([
        StructField("ProductSK", IntegerType(), False),
        StructField("ProductID", StringType(), False)
    ])

    data = [
        (1, "PROD001"),
        (2, "PROD002"),
        (3, "PROD003")
    ]

    return spark.createDataFrame(data, schema)

# Utilities

def assert_dataframe_equal(df1, df2, check_order=False):
    assert df1.schema == df2.schema, f"Schemas differ"
    count1 = df1.count()
    count2 = df2.count()
    assert count1 == count2, f"Row counts differ: {count1} vs {count2}"

    if not check_order:
        df1 = df1.sort(df1.columns)
        df2 = df2.sort(df2.columns)

    diff = df1.subtract(df2)
    assert diff.count() == 0, "DataFrames differ"

def get_column_null_count(df, column_name):
    return df.filter(df[column_name].isNull()).count()

def get_duplicate_count(df, key_columns):
    return df.groupBy(key_columns).count().filter("count > 1").count()

TEST_CATALOG = "sap"
TEST_BRONZE_SCHEMA = "test_bronze"
TEST_SILVER_SCHEMA = "test_silver"
TEST_GOLD_SCHEMA = "test_gold"

MIN_CUSTOMER_COUNT = 1000
MAX_CUSTOMER_COUNT = 50000
MIN_PRODUCT_COUNT = 100
MAX_PRODUCT_COUNT = 10000
MAX_NULL_PERCENTAGE = 5.0
MAX_DUPLICATE_PERCENTAGE = 1.0