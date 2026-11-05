# Databricks notebook source
# DBTITLE 1,SCD Type 3 - Limited History Tracking
# MAGIC %md
# MAGIC # Dimension: Customer SCD Type 3 - Limited History Tracking
# MAGIC
# MAGIC ## Pattern Overview
# MAGIC
# MAGIC **SCD Type 3** maintains current + previous values in the same row, providing one-level history without creating new records.
# MAGIC
# MAGIC **Business Use Cases:**
# MAGIC * Regulatory compliance: Track previous CIBIL score for fraud detection
# MAGIC * Risk management: Compare current vs previous credit rating (improvement/decline)
# MAGIC * Contact management: Keep old email for bounce tracking
# MAGIC * Location tracking: Previous city for customer movement analysis
# MAGIC
# MAGIC **Schema Design:**
# MAGIC
# MAGIC ```
# MAGIC CustomerSK INT (surrogate key)
# MAGIC CustomerName STRING (business key)
# MAGIC CurrentCity STRING
# MAGIC PreviousCity STRING
# MAGIC CurrentCIBILScore INT
# MAGIC PreviousCIBILScore INT
# MAGIC CurrentEmail STRING
# MAGIC PreviousEmail STRING
# MAGIC LastChangeDate TIMESTAMP
# MAGIC IsCurrent BOOLEAN
# MAGIC ```
# MAGIC
# MAGIC ## When to Use SCD Type 3
# MAGIC
# MAGIC **Advantages:**
# MAGIC * Storage efficient: One row per customer (vs multiple rows in SCD2)
# MAGIC * Query simplicity: No date filtering required
# MAGIC * Before/after comparisons: Easy to calculate deltas
# MAGIC
# MAGIC **Limitations:**
# MAGIC * Only one level of history (can't see full audit trail)
# MAGIC * Not suitable for compliance requiring complete lineage
# MAGIC * Limited temporal analysis capabilities
# MAGIC
# MAGIC **Decision Matrix:**
# MAGIC * Need full history → Use SCD Type 2
# MAGIC * Need before/after snapshot → Use SCD Type 3
# MAGIC * No history needed → Use SCD Type 1

# COMMAND ----------

# DBTITLE 1,Configuration and Setup
# MAGIC %run ../../../1_Canvas/1_Setup/1_config

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql.functions import col, lit, current_timestamp, when, coalesce, row_number
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime

print("="*70)
print("SCD TYPE 3: CUSTOMER DIMENSION")
print("="*70)
print(f"Target: {Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_customer_scd3")
print(f"Pattern: Current + Previous values (one-level history)")
print(f"Source: {Config.TBL_STG_SALES_INCREMENTAL}")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Create SCD Type 3 Table
# Create table with Current/Previous column pairs
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_customer_scd3 (
        CustomerSK INT,
        CustomerName STRING,
        CurrentCity STRING,
        PreviousCity STRING,
        CurrentCIBILScore INT,
        PreviousCIBILScore INT,
        CurrentEmail STRING,
        PreviousEmail STRING,
        LastChangeDate TIMESTAMP,
        IsCurrent BOOLEAN,
        RecordCreatedDate TIMESTAMP,
        RecordUpdatedDate TIMESTAMP
    )
    USING DELTA
    COMMENT 'SCD Type 3: Customer dimension with one-level history (Current + Previous)'
""")

print("SUCCESS Table created: dim_customer_scd3")
print("Schema: Current/Previous column pairs for City, CIBIL Score, Email")

# COMMAND ----------

# DBTITLE 1,Initial Load - Establish Current Values
# Extract unique customers from bronze layer incremental table
from pyspark.sql.functions import concat, regexp_replace, lower, monotonically_increasing_id

df_bronze = spark.table(Config.TBL_STG_SALES_INCREMENTAL)

# Get distinct customers with latest record per customer
window_spec = Window.partitionBy("CustomerName").orderBy(col("OrderDate").desc())

df_customers = df_bronze.withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .select(
        col("CustomerName"),
        col("CustomerName").alias("CurrentCity"),  # Using name as proxy for demo
        lit(650).alias("CurrentCIBILScore"),  # Default credit score
        concat(lower(regexp_replace(col("CustomerName"), " ", ".")), lit("@email.com")).alias("CurrentEmail")
    ).distinct()

# Add surrogate key
df_customers = df_customers.withColumn("CustomerSK", monotonically_increasing_id().cast("int"))

# Initial load: Current values populated, Previous = NULL
df_initial = df_customers.select(
    col("CustomerSK"),
    col("CustomerName"),
    col("CurrentCity"),
    lit(None).cast(StringType()).alias("PreviousCity"),
    col("CurrentCIBILScore"),
    lit(None).cast(IntegerType()).alias("PreviousCIBILScore"),
    col("CurrentEmail"),
    lit(None).cast(StringType()).alias("PreviousEmail"),
    current_timestamp().alias("LastChangeDate"),
    lit(True).alias("IsCurrent"),
    current_timestamp().alias("RecordCreatedDate"),
    current_timestamp().alias("RecordUpdatedDate")
)

df_initial.write.mode("overwrite").saveAsTable(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_customer_scd3")

record_count = df_initial.count()
print(f"\nSUCCESS Initial load complete: {record_count:,} customers")
print("Status: Current values populated, Previous values = NULL")
print("\nSample records:")
display(df_initial.limit(10))

# COMMAND ----------

# DBTITLE 1,Simulate Customer Changes - CDC Input
# Simulate incoming CDC changes (new city, CIBIL score changes, email updates)
# In production, this would come from CDC feed or change tracking

changed_customers = [
    ("Amit Kumar", "Bangalore", 720, "amit.kumar.new@email.com"),
    ("Priya Sharma", "Mumbai", 680, "priya.sharma.updated@email.com"),
    ("Raj Patel", "Delhi", 590, "raj.patel.new@email.com"),
    ("Sneha Gupta", "Pune", 700, "sneha.gupta@email.com"),
    ("Vikram Singh", "Hyderabad", 750, "vikram.singh@email.com")
]

df_changes = spark.createDataFrame(changed_customers,
    ["CustomerName", "NewCity", "NewCIBILScore", "NewEmail"])

print("Incoming CDC changes:")
print(f"Total changes: {df_changes.count()}")
print("\nChanged customer records:")
display(df_changes)

# COMMAND ----------

# DBTITLE 1,SCD Type 3 Update Logic - Shift Current to Previous
# Read existing dimension
df_existing = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_customer_scd3")

# Join changes with existing records
# For changed records: Current → Previous, New → Current
# For unchanged records: Keep as-is

df_updated = df_existing.alias("e").join(
    df_changes.alias("c"),
    col("e.CustomerName") == col("c.CustomerName"),
    "left"
).select(
    col("e.CustomerSK"),
    col("e.CustomerName"),

    # City: If change detected, update Current and preserve old Current as Previous
    when(col("c.CustomerName").isNotNull(), col("c.NewCity")) \
        .otherwise(col("e.CurrentCity")).alias("CurrentCity"),
    when(col("c.CustomerName").isNotNull(), col("e.CurrentCity")) \
        .otherwise(col("e.PreviousCity")).alias("PreviousCity"),

    # CIBIL Score: Same logic
    when(col("c.CustomerName").isNotNull(), col("c.NewCIBILScore").cast("int")) \
        .otherwise(col("e.CurrentCIBILScore")).alias("CurrentCIBILScore"),
    when(col("c.CustomerName").isNotNull(), col("e.CurrentCIBILScore")) \
        .otherwise(col("e.PreviousCIBILScore")).alias("PreviousCIBILScore"),

    # Email: Same logic
    when(col("c.CustomerName").isNotNull(), col("c.NewEmail")) \
        .otherwise(col("e.CurrentEmail")).alias("CurrentEmail"),
    when(col("c.CustomerName").isNotNull(), col("e.CurrentEmail")) \
        .otherwise(col("e.PreviousEmail")).alias("PreviousEmail"),

    # Update LastChangeDate only for changed records
    when(col("c.CustomerName").isNotNull(), current_timestamp()) \
        .otherwise(col("e.LastChangeDate")).alias("LastChangeDate"),

    col("e.IsCurrent"),
    col("e.RecordCreatedDate"),

    # Update RecordUpdatedDate for changed records
    when(col("c.CustomerName").isNotNull(), current_timestamp()) \
        .otherwise(col("e.RecordUpdatedDate")).alias("RecordUpdatedDate")
)

# Overwrite table with updated records
df_updated.write.mode("overwrite").saveAsTable(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_customer_scd3")

changed_count = df_changes.count()
print(f"\nSUCCESS SCD Type 3 update complete")
print(f"Records updated: {changed_count}")
print("Logic: Current → Previous, New → Current")
print("\nCustomers with change history:")
display(df_updated.filter(col("PreviousCity").isNotNull()).limit(10))

# COMMAND ----------

# DBTITLE 1,Before/After Comparison Analysis
df_scd3 = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_customer_scd3")

# Show customers with history (Previous values populated)
df_with_history = df_scd3.filter(col("PreviousCity").isNotNull()).select(
    col("CustomerName"),
    col("PreviousCity"),
    col("CurrentCity"),
    col("PreviousCIBILScore"),
    col("CurrentCIBILScore"),
    (col("CurrentCIBILScore") - col("PreviousCIBILScore")).alias("CIBILChange"),
    col("LastChangeDate")
).orderBy(col("CIBILChange").desc())

print("="*70)
print("BEFORE/AFTER ANALYSIS: CUSTOMERS WITH CHANGE HISTORY")
print("="*70)
print(f"Total customers with history: {df_with_history.count()}")
print("\nTop 10 customers by CIBIL score improvement:")
display(df_with_history.limit(10))

# Business insight: Customers with score improvement vs decline
improvements = df_with_history.filter(col("CIBILChange") > 0).count()
declines = df_with_history.filter(col("CIBILChange") < 0).count()

print(f"\nCredit Score Trends:")
print(f"  Improvements: {improvements} customers")
print(f"  Declines: {declines} customers")
print(f"  Net trend: {'Positive' if improvements > declines else 'Negative'}")

# COMMAND ----------

# DBTITLE 1,SCD Pattern Comparison - Type 1 vs Type 2 vs Type 3
# MAGIC %md
# MAGIC ## SCD Pattern Comparison: Storage & Query Trade-offs
# MAGIC
# MAGIC ### Query Complexity Comparison
# MAGIC
# MAGIC **SCD Type 1 (No History):**
# MAGIC ```sql
# MAGIC SELECT CustomerName, City, CIBILScore
# MAGIC FROM dim_customer_scd1;
# MAGIC ```
# MAGIC * Simplest query
# MAGIC * Always current values
# MAGIC * No history available
# MAGIC
# MAGIC **SCD Type 2 (Full History):**
# MAGIC ```sql
# MAGIC SELECT
# MAGIC     CustomerName,
# MAGIC     City AS CurrentCity,
# MAGIC     LAG(City) OVER (PARTITION BY CustomerName ORDER BY StartDate) AS PreviousCity,
# MAGIC     CIBILScore AS CurrentScore,
# MAGIC     LAG(CIBILScore) OVER (PARTITION BY CustomerName ORDER BY StartDate) AS PreviousScore
# MAGIC FROM dim_customer_scd2
# MAGIC WHERE IsCurrent = TRUE;
# MAGIC ```
# MAGIC * Requires window functions for before/after comparison
# MAGIC * Full audit trail available
# MAGIC * More complex queries
# MAGIC
# MAGIC **SCD Type 3 (Limited History):**
# MAGIC ```sql
# MAGIC SELECT
# MAGIC     CustomerName,
# MAGIC     CurrentCity,
# MAGIC     PreviousCity,
# MAGIC     CurrentCIBILScore,
# MAGIC     PreviousCIBILScore,
# MAGIC     CurrentCIBILScore - PreviousCIBILScore AS CIBILChange
# MAGIC FROM dim_customer_scd3
# MAGIC WHERE PreviousCity IS NOT NULL;
# MAGIC ```
# MAGIC * Simple column comparison
# MAGIC * No date filtering needed
# MAGIC * Easy before/after analysis
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Storage Footprint Comparison
# MAGIC
# MAGIC Assume 5,000 customers with 3 changes each over time:
# MAGIC
# MAGIC | Pattern | Rows | Storage Multiplier | Query Performance |
# MAGIC |---------|------|-------------------|-------------------|
# MAGIC | SCD Type 1 | 5,000 | 1x (baseline) | Fastest |
# MAGIC | SCD Type 2 | 20,000 | 4x | Slower (date filtering) |
# MAGIC | SCD Type 3 | 5,000 | 1x | Fast (no joins/windows) |
# MAGIC
# MAGIC **Storage Formula:**
# MAGIC * SCD1: `customers × 1` row
# MAGIC * SCD2: `customers × (1 + avg_changes)` rows
# MAGIC * SCD3: `customers × 1` row (extra columns, not rows)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Decision Framework
# MAGIC
# MAGIC **Use SCD Type 1 when:**
# MAGIC * No history tracking required
# MAGIC * Always use latest values
# MAGIC * Storage optimization critical
# MAGIC * Simple reporting needs
# MAGIC
# MAGIC **Use SCD Type 2 when:**
# MAGIC * Full audit trail required (compliance, regulations)
# MAGIC * Point-in-time queries needed ("What was customer's city on Jan 1, 2025?")
# MAGIC * Data lineage tracking essential
# MAGIC * Multiple changes must be preserved
# MAGIC
# MAGIC **Use SCD Type 3 when:**
# MAGIC * Need before/after snapshot (not full history)
# MAGIC * Regulatory requirement for "previous value"
# MAGIC * Query simplicity matters (business users writing SQL)
# MAGIC * Storage optimization important
# MAGIC * Only recent change matters (fraud detection, risk management)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Real-World Example: Credit Risk
# MAGIC
# MAGIC **Scenario:** Bank needs to identify customers whose CIBIL scores declined by >50 points.
# MAGIC
# MAGIC **SCD Type 2 Approach:**
# MAGIC ```sql
# MAGIC WITH ranked_scores AS (
# MAGIC     SELECT
# MAGIC         CustomerName,
# MAGIC         CIBILScore,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY CustomerName ORDER BY StartDate DESC) as rn
# MAGIC     FROM dim_customer_scd2
# MAGIC )
# MAGIC SELECT
# MAGIC     curr.CustomerName,
# MAGIC     prev.CIBILScore AS PreviousScore,
# MAGIC     curr.CIBILScore AS CurrentScore,
# MAGIC     prev.CIBILScore - curr.CIBILScore AS ScoreDecline
# MAGIC FROM ranked_scores curr
# MAGIC JOIN ranked_scores prev
# MAGIC     ON curr.CustomerName = prev.CustomerName
# MAGIC     AND curr.rn = 1
# MAGIC     AND prev.rn = 2
# MAGIC WHERE (prev.CIBILScore - curr.CIBILScore) > 50;
# MAGIC ```
# MAGIC
# MAGIC **SCD Type 3 Approach:**
# MAGIC ```sql
# MAGIC SELECT
# MAGIC     CustomerName,
# MAGIC     PreviousCIBILScore,
# MAGIC     CurrentCIBILScore,
# MAGIC     PreviousCIBILScore - CurrentCIBILScore AS ScoreDecline
# MAGIC FROM dim_customer_scd3
# MAGIC WHERE (PreviousCIBILScore - CurrentCIBILScore) > 50;
# MAGIC ```
# MAGIC
# MAGIC **Result:** SCD Type 3 is 5x simpler for this use case.

# COMMAND ----------

# DBTITLE 1,Validation - Data Quality Checks
df_scd3 = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_customer_scd3")

print("="*70)
print("VALIDATION: SCD TYPE 3 DATA QUALITY CHECKS")
print("="*70)

# Check 1: Primary key uniqueness
print("\nCheck 1: Primary Key Uniqueness")
total_customers = df_scd3.count()
distinct_customers = df_scd3.select("CustomerName").distinct().count()

if total_customers == distinct_customers:
    print(f"PASS: {total_customers:,} customers, all unique")
else:
    print(f"FAIL: {total_customers:,} total rows, {distinct_customers:,} distinct customers")
    print("Issue: Duplicate CustomerName records detected")

# Check 2: IsCurrent flag validation
print("\nCheck 2: IsCurrent Flag Validation")
all_current = df_scd3.filter(col("IsCurrent") == False).count()

if all_current == 0:
    print(f"PASS: All {total_customers:,} records have IsCurrent = True")
else:
    print(f"FAIL: {all_current} records have IsCurrent = False")
    print("Issue: SCD Type 3 should have no historical records (all current)")

# Check 3: NULL handling validation
print("\nCheck 3: NULL Handling in Current Columns")
null_current_city = df_scd3.filter(col("CurrentCity").isNull()).count()
null_current_cibil = df_scd3.filter(col("CurrentCIBILScore").isNull()).count()
null_current_email = df_scd3.filter(col("CurrentEmail").isNull()).count()

if null_current_city == 0 and null_current_cibil == 0 and null_current_email == 0:
    print("PASS: No NULLs in Current columns")
else:
    print(f"FAIL: NULLs detected")
    print(f"  CurrentCity: {null_current_city}")
    print(f"  CurrentCIBILScore: {null_current_cibil}")
    print(f"  CurrentEmail: {null_current_email}")

# Check 4: Change tracking coverage
print("\nCheck 4: Change Tracking Coverage")
customers_with_history = df_scd3.filter(col("PreviousCity").isNotNull()).count()
history_coverage_pct = (customers_with_history / total_customers) * 100 if total_customers > 0 else 0

print(f"Customers with history: {customers_with_history:,}")
print(f"Customers without history: {total_customers - customers_with_history:,}")
print(f"History coverage: {history_coverage_pct:.1f}%")

if history_coverage_pct > 0:
    print("PASS: Change tracking operational")
else:
    print("INFO: No changes detected yet (expected on initial load)")

# Check 5: Record counts match source
print("\nCheck 5: Record Count Reconciliation")
df_bronze = spark.table(Config.TBL_STG_SALES_INCREMENTAL)
bronze_distinct_customers = df_bronze.select("CustomerName").distinct().count()

if total_customers == bronze_distinct_customers:
    print(f"PASS: {total_customers:,} customers match bronze layer")
else:
    diff = abs(total_customers - bronze_distinct_customers)
    print(f"WARNING: Difference of {diff:,} customers")
    print(f"  SCD3: {total_customers:,}")
    print(f"  Bronze: {bronze_distinct_customers:,}")

print("\n" + "="*70)
print("VALIDATION COMPLETE")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Production Implementation Notes
# MAGIC %md
# MAGIC ## Production Implementation Checklist
# MAGIC
# MAGIC ### 1. CDC Integration
# MAGIC
# MAGIC In production, changes come from CDC feeds (Change Data Capture):
# MAGIC
# MAGIC ```python
# MAGIC # Read CDC feed
# MAGIC df_cdc = spark.readStream \
# MAGIC     .format("delta") \
# MAGIC     .table("bronze.customer_changes_cdc")
# MAGIC
# MAGIC # Filter for UPDATE operations only
# MAGIC df_updates = df_cdc.filter(col("_change_type") == "update_postimage")
# MAGIC
# MAGIC # Apply SCD Type 3 logic in streaming mode
# MAGIC ```
# MAGIC
# MAGIC ### 2. Idempotency Handling
# MAGIC
# MAGIC **Challenge:** Prevent duplicate processing if pipeline runs twice.
# MAGIC
# MAGIC **Solution:** Track processed CDC sequence numbers:
# MAGIC ```sql
# MAGIC CREATE TABLE IF NOT EXISTS audit.scd3_processed_sequences (
# MAGIC     table_name STRING,
# MAGIC     last_processed_sequence BIGINT,
# MAGIC     processed_timestamp TIMESTAMP
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ### 3. Performance Optimization
# MAGIC
# MAGIC **Technique:** Partition by change date for efficient incremental processing:
# MAGIC ```sql
# MAGIC CREATE TABLE dim_customer_scd3 (
# MAGIC     ...
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (date(LastChangeDate));
# MAGIC ```
# MAGIC
# MAGIC ### 4. NULL Handling Strategy
# MAGIC
# MAGIC **Decision Rules:**
# MAGIC * `PreviousCity IS NULL` → No change yet (initial load)
# MAGIC * `CurrentCity IS NULL` → Data quality issue (investigate)
# MAGIC * Both NULL → Invalid record (reject)
# MAGIC
# MAGIC ### 5. Workflow Integration
# MAGIC
# MAGIC **Dependency Order:**
# MAGIC ```
# MAGIC 1. Bronze: Load staging data
# MAGIC 2. Silver: Process dimension SCD Type 3
# MAGIC 3. Facts: Join to dimension (use CurrentCity, CurrentCIBILScore)
# MAGIC 4. Gold: Aggregate metrics
# MAGIC ```
# MAGIC
# MAGIC ### 6. Monitoring & Alerts
# MAGIC
# MAGIC **Metrics to track:**
# MAGIC * Change rate: % of customers updated per batch
# MAGIC * NULL rate in Previous columns (should increase over time)
# MAGIC * Score improvement vs decline ratio
# MAGIC * Processing latency
# MAGIC
# MAGIC **Alert thresholds:**
# MAGIC * Change rate >20% in single batch (data issue?)
# MAGIC * NULL rate in Current columns >0% (data quality failure)
# MAGIC * Processing time >5 minutes (performance degradation)
# MAGIC
# MAGIC ### 7. Data Retention Policy
# MAGIC
# MAGIC SCD Type 3 only keeps one level of history. For compliance:
# MAGIC * Archive full history to SCD Type 2 table monthly
# MAGIC * Retain SCD Type 3 for operational use (last change only)
# MAGIC * Store complete audit trail in separate compliance table

# COMMAND ----------

