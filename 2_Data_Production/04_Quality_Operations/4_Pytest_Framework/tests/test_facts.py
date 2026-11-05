# Databricks notebook source
# SkillAIPath Data Warehouse - Fact Table Tests
# Tests for star schema joins, referential integrity, aggregations, and fact patterns

import pytest
from datetime import date
    sample_fact_sales_bronze, sample_dim_customer_for_facts, sample_dim_product_for_facts,
    get_column_null_count
)

# FACT TABLE JOIN TESTS

def test_fact_sales_join_to_customer_dimension(spark, sample_fact_sales_bronze, sample_dim_customer_for_facts):
    """
    Test: Fact table joins to customer dimension via CustomerID
    Expected: Surrogate key (CustomerSK) populated for valid customers
    """
    # Arrange
    fact_df = sample_fact_sales_bronze
    dim_customer = sample_dim_customer_for_facts

    # Act: Join fact to dimension (INNER JOIN to enforce referential integrity)
    enriched_fact = fact_df.join(
        dim_customer,
        fact_df["CustomerID"] == dim_customer["CustomerID"],
        "inner"
    ).select(
        fact_df["SalesID"],
        fact_df["CustomerID"],
        dim_customer["CustomerSK"],
        fact_df["TotalAmount"]
    )

    # Assert
    # Original has 4 sales, 1 orphan (CUST999)
    # INNER JOIN should return 3 sales
    assert enriched_fact.count() == 3, "Should have 3 sales with valid customers"

    # Verify CustomerSK populated
    null_sk_count = get_column_null_count(enriched_fact, "CustomerSK")
    assert null_sk_count == 0, "CustomerSK cannot have NULLs after join"

    print("PASS: Fact-to-dimension join")

def test_fact_sales_multi_dimension_join(spark, sample_fact_sales_bronze,
                                          sample_dim_customer_for_facts, sample_dim_product_for_facts):
    """
    Test: Fact table joins to multiple dimensions (customer AND product)
    Expected: Both surrogate keys populated
    """
    # Arrange
    fact_df = sample_fact_sales_bronze
    dim_customer = sample_dim_customer_for_facts
    dim_product = sample_dim_product_for_facts

    # Act: Join to both dimensions
    enriched_fact = fact_df \
        .join(dim_customer, fact_df["CustomerID"] == dim_customer["CustomerID"], "inner") \
        .join(dim_product, fact_df["ProductID"] == dim_product["ProductID"], "inner") \
        .select(
            fact_df["SalesID"],
            dim_customer["CustomerSK"],
            dim_product["ProductSK"],
            fact_df["TotalAmount"]
        )

    # Assert
    assert enriched_fact.count() == 3, "Should have 3 sales with valid customer AND product"

    # Verify both surrogate keys populated
    assert get_column_null_count(enriched_fact, "CustomerSK") == 0
    assert get_column_null_count(enriched_fact, "ProductSK") == 0

    print("PASS: Multi-dimension join")

# REFERENTIAL INTEGRITY TESTS

def test_fact_sales_orphan_detection(spark, sample_fact_sales_bronze, sample_dim_customer_for_facts):
    """
    Test: Detect orphan fact records (foreign key not in dimension)
    Expected: CUST999 detected as orphan
    """
    # Arrange
    fact_df = sample_fact_sales_bronze
    dim_customer = sample_dim_customer_for_facts

    # Act: LEFT ANTI JOIN to find orphans
    orphans = fact_df.join(
        dim_customer,
        fact_df["CustomerID"] == dim_customer["CustomerID"],
        "left_anti"
    )

    # Assert
    orphan_count = orphans.count()
    assert orphan_count == 1, "Should detect 1 orphan (CUST999)"

    orphan_customer = orphans.first()["CustomerID"]
    assert orphan_customer == "CUST999", "Orphan should be CUST999"

    print("PASS: Orphan detection")

def test_fact_sales_referential_integrity_enforcement(spark, sample_fact_sales_bronze,
                                                       sample_dim_customer_for_facts):
    """
    Test: Referential integrity enforcement (reject orphans)
    Expected: Orphan records excluded from fact table
    """
    # Arrange
    fact_df = sample_fact_sales_bronze
    dim_customer = sample_dim_customer_for_facts

    # Act: Enforce referential integrity (INNER JOIN)
    validated_fact = fact_df.join(
        dim_customer.select("CustomerID"),
        "CustomerID",
        "inner"
    )

    # Assert
    original_count = fact_df.count()
    validated_count = validated_fact.count()

    assert original_count == 4, "Original fact has 4 records"
    assert validated_count == 3, "Validated fact has 3 records (1 orphan excluded)"

    # Verify no orphans remain
    remaining_customers = validated_fact.select("CustomerID").distinct().collect()
    remaining_customer_ids = [row["CustomerID"] for row in remaining_customers]

    assert "CUST999" not in remaining_customer_ids, "Orphan CUST999 should be excluded"

    print("PASS: Referential integrity enforcement")

# AGGREGATION TESTS

def test_fact_sales_total_revenue_aggregation(spark, sample_fact_sales_bronze):
    """
    Test: Aggregate total revenue from fact table
    Expected: Sum of TotalAmount matches expected total
    """
    # Arrange
    fact_df = sample_fact_sales_bronze

    # Act: Calculate total revenue
    total_revenue = fact_df.agg(_sum("TotalAmount").alias("TotalRevenue")).first()["TotalRevenue"]

    # Expected: 200 + 250 + 240 + 300 = 990
    expected_revenue = Decimal("990.00")

    # Assert
    assert total_revenue == expected_revenue, f"Total revenue should be {expected_revenue}, got {total_revenue}"

    print("PASS: Total revenue aggregation")

def test_fact_sales_customer_aggregation(spark, sample_fact_sales_bronze, sample_dim_customer_for_facts):
    """
    Test: Aggregate sales by customer
    Expected: Per-customer totals calculated correctly
    """
    # Arrange
    fact_df = sample_fact_sales_bronze
    dim_customer = sample_dim_customer_for_facts

    # Act: Aggregate by customer (exclude orphans)
    customer_sales = fact_df \
        .join(dim_customer, "CustomerID", "inner") \
        .groupBy("CustomerID") \
        .agg(
            count("*").alias("OrderCount"),
            _sum("TotalAmount").alias("TotalRevenue"),
            avg("TotalAmount").alias("AvgOrderValue")
        )

    # Assert
    assert customer_sales.count() == 3, "Should have 3 customers with sales"

    # Verify CUST001 totals (1 order, 200.00)
    cust001_row = customer_sales.filter(col("CustomerID") == "CUST001").first()
    assert cust001_row["OrderCount"] == 1
    assert cust001_row["TotalRevenue"] == Decimal("200.00")
    assert cust001_row["AvgOrderValue"] == Decimal("200.00")

    print("PASS: Customer aggregation")

def test_fact_sales_grain_validation(spark, sample_fact_sales_bronze):
    """
    Test: Fact table grain validation (one row per transaction)
    Expected: Each SalesID appears exactly once
    """
    # Arrange
    fact_df = sample_fact_sales_bronze

    # Act: Check for duplicate SalesIDs
    sales_id_counts = fact_df.groupBy("SalesID").count()
    duplicates = sales_id_counts.filter(col("count") > 1)

    # Assert
    duplicate_count = duplicates.count()
    assert duplicate_count == 0, "No duplicate SalesIDs should exist (grain = 1 row per transaction)"

    print("PASS: Grain validation")

# DATA QUALITY TESTS

def test_fact_sales_no_null_measures(spark, sample_fact_sales_bronze):
    """
    Test: Measures (Quantity, TotalAmount) cannot be NULL
    Expected: Zero NULL values in measure columns
    """
    # Arrange
    fact_df = sample_fact_sales_bronze

    # Act & Assert
    quantity_null_count = get_column_null_count(fact_df, "Quantity")
    amount_null_count = get_column_null_count(fact_df, "TotalAmount")

    assert quantity_null_count == 0, "Quantity cannot have NULLs"
    assert amount_null_count == 0, "TotalAmount cannot have NULLs"

    print("PASS: No NULL measures")

def test_fact_sales_no_null_foreign_keys(spark, sample_fact_sales_bronze):
    """
    Test: Foreign keys (CustomerID, ProductID) cannot be NULL
    Expected: Zero NULL values in foreign key columns
    """
    # Arrange
    fact_df = sample_fact_sales_bronze

    # Act & Assert
    customer_null_count = get_column_null_count(fact_df, "CustomerID")
    product_null_count = get_column_null_count(fact_df, "ProductID")

    assert customer_null_count == 0, "CustomerID cannot have NULLs"
    assert product_null_count == 0, "ProductID cannot have NULLs"

    print("PASS: No NULL foreign keys")

def test_fact_sales_positive_measures(spark, sample_fact_sales_bronze):
    """
    Test: Measures must be positive (no negative quantities or amounts)
    Expected: All measures > 0
    """
    # Arrange
    fact_df = sample_fact_sales_bronze

    # Act: Check for negative values
    negative_quantity = fact_df.filter(col("Quantity") <= 0).count()
    negative_amount = fact_df.filter(col("TotalAmount") <= 0).count()

    # Assert
    assert negative_quantity == 0, "Quantity must be positive"
    assert negative_amount == 0, "TotalAmount must be positive"

    print("PASS: Positive measures validation")

def test_fact_sales_amount_calculation(spark, sample_fact_sales_bronze):
    """
    Test: TotalAmount = Quantity * UnitPrice
    Expected: Calculation validation passes for all rows
    """
    # Arrange
    fact_df = sample_fact_sales_bronze

    # Act: Validate calculation
    validated_df = fact_df.withColumn(
        "CalculatedAmount",
        col("Quantity") * col("UnitPrice")
    ).withColumn(
        "IsValid",
        col("TotalAmount") == col("CalculatedAmount")
    )

    invalid_count = validated_df.filter(col("IsValid") == False).count()

    # Assert
    assert invalid_count == 0, "All TotalAmount values should match Quantity * UnitPrice"

    print("PASS: Amount calculation validation")

# PERFORMANCE TESTS

def test_fact_sales_large_volume_aggregation(spark):
    """
    Test: Aggregation performance on large fact table
    Expected: 1M row aggregation completes without errors
    """
    # Arrange: Create 1M row fact table
    large_fact = spark.range(0, 1000000).toDF("SalesID") \
        .withColumn("CustomerID", (col("SalesID") % 10000).cast("string")) \
        .withColumn("TotalAmount", lit(Decimal("100.00")))

    # Act: Aggregate by customer
    customer_totals = large_fact.groupBy("CustomerID") \
        .agg(_sum("TotalAmount").alias("TotalRevenue"))

    # Assert
    assert large_fact.count() == 1000000, "Input should have 1M rows"
    assert customer_totals.count() == 10000, "Output should have 10K customers"

    # Verify aggregation correctness
    sample_customer = customer_totals.first()
    expected_revenue = Decimal("100.00") * 100  # Each customer has 100 sales
    assert sample_customer["TotalRevenue"] == expected_revenue

    print("PASS: Large volume aggregation (1M rows)")

# INTEGRATION TESTS

def test_fact_sales_end_to_end_pipeline(spark, sample_fact_sales_bronze,
                                         sample_dim_customer_for_facts, sample_dim_product_for_facts):
    """
    Test: Complete fact table pipeline (bronze -> silver with enrichment)
    Expected: Orphans excluded, surrogate keys populated, aggregations correct
    """
    # Arrange
    bronze_fact = sample_fact_sales_bronze
    dim_customer = sample_dim_customer_for_facts
    dim_product = sample_dim_product_for_facts

    # Act: Complete pipeline
    # Step 1: Enrich with surrogate keys
    enriched_fact = bronze_fact \
        .join(dim_customer, bronze_fact["CustomerID"] == dim_customer["CustomerID"], "inner") \
        .join(dim_product, bronze_fact["ProductID"] == dim_product["ProductID"], "inner") \
        .select(
            bronze_fact["SalesID"],
            bronze_fact["SalesDate"],
            dim_customer["CustomerSK"],
            dim_product["ProductSK"],
            bronze_fact["Quantity"],
            bronze_fact["UnitPrice"],
            bronze_fact["TotalAmount"]
        )

    # Step 2: Calculate aggregates
    daily_totals = enriched_fact.groupBy("SalesDate") \
        .agg(
            count("*").alias("TransactionCount"),
            _sum("TotalAmount").alias("DailyRevenue")
        )

    # Assert
    # Enrichment
    assert enriched_fact.count() == 3, "Should have 3 enriched sales (1 orphan excluded)"
    assert get_column_null_count(enriched_fact, "CustomerSK") == 0
    assert get_column_null_count(enriched_fact, "ProductSK") == 0

    # Aggregation
    assert daily_totals.count() == 1, "All sales on same date"
    daily_total = daily_totals.first()
    assert daily_total["TransactionCount"] == 3
    assert daily_total["DailyRevenue"] == Decimal("690.00")  # 200 + 250 + 240

    print("PASS: End-to-end fact pipeline")