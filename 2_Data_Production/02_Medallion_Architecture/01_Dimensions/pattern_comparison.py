# Databricks notebook source
# DBTITLE 1,Dimension Loading Patterns - Live Comparison
# MAGIC %md
# MAGIC # Dimension Loading Patterns: Side-by-Side Comparison
# MAGIC **SkillAIPath Data Warehouse - Choosing the Right Pattern**
# MAGIC
# MAGIC ## THE SCENARIO: "We Need All Three Patterns"
# MAGIC
# MAGIC **Monday Morning - Architecture Review**
# MAGIC
# MAGIC **Junior Engineer:** "Which dimension loading pattern should I use for SkillAIPath? SCD Type 1? Type 2? Full reload?"
# MAGIC
# MAGIC **Lead Architect:** "Wrong question. You need ALL three - each serves different business needs:
# MAGIC
# MAGIC **Pattern 1: Full Truncate & Reload**
# MAGIC - Use for: Product categories, warehouses, payment modes
# MAGIC - Why: Static data, small tables, no history needed
# MAGIC - Example: 50 product categories never change
# MAGIC
# MAGIC **Pattern 2: SCD Type 1 (Overwrite)**
# MAGIC - Use for: Agent assignments, retailer contact info
# MAGIC - Why: Current state matters, history doesn't
# MAGIC - Example: Agent phone number changes - old number is irrelevant
# MAGIC
# MAGIC **Pattern 3: SCD Type 2 (History Tracking)**
# MAGIC - Use for: Customer addresses, pricing tiers
# MAGIC - Why: Historical analysis requires time-travel
# MAGIC - Example: 'Show me which customers lived in Mumbai in Q1 2023'
# MAGIC
# MAGIC Each pattern solves different problems. Pick wrong pattern = pain."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What You'll See in This Notebook
# MAGIC
# MAGIC We'll process the **SAME customer record change** using all 3 patterns:
# MAGIC
# MAGIC **Scenario:** Customer 'Priya Sharma' moves from Mumbai to Bangalore
# MAGIC
# MAGIC **You'll see:**
# MAGIC 1. How each pattern handles the change differently
# MAGIC 2. SQL code for each approach
# MAGIC 3. Resulting data structure
# MAGIC 4. When to use each pattern (decision framework)
# MAGIC 5. Performance and storage trade-offs
# MAGIC 6. Real production war stories
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Setup - Create Test Scenario
# Setup: Create test dimension table and staging data

import sys
import os
from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring
from pyspark.sql import Window
from datetime import datetime, timedelta

# Create test dimension (initial state)
test_dim_data = [
    (1, "C001", "Priya Sharma", "Mumbai", "priya@email.com", "2024-01-01", "9999-12-31", 1),
    (2, "C002", "Rahul Verma", "Delhi", "rahul@email.com", "2024-01-01", "9999-12-31", 1),
    (3, "C003", "Anita Desai", "Kolkata", "anita@email.com", "2024-01-01", "9999-12-31", 1)
]

test_dim_schema = ["CustomerSK", "CustomerID", "CustomerName", "City", "Email", "StartDate", "EndDate", "IsCurrent"]

df_initial = spark.createDataFrame(test_dim_data, schema=test_dim_schema)

# Create separate test tables for each pattern
df_initial.write.mode("overwrite").saveAsTable("default.test_dim_full_reload")
df_initial.write.mode("overwrite").saveAsTable("default.test_dim_scd1")
df_initial.write.mode("overwrite").saveAsTable("default.test_dim_scd2")

print(" Test dimensions created (3 copies for comparison)")
print("\n INITIAL STATE - All 3 patterns start identical:")
print("="*70)
display(df_initial)

# Create staging data with changes
staging_data = [
    ("C001", "Priya Sharma", "Bangalore", "priya@email.com"),  # City changed: Mumbai → Bangalore
    ("C002", "Rahul Verma", "Delhi", "rahul.new@email.com"),    # Email changed
    ("C003", "Anita Desai", "Kolkata", "anita@email.com"),      # No change
    ("C004", "Vikram Singh", "Chennai", "vikram@email.com")     # New customer
]

staging_schema = ["CustomerID", "CustomerName", "City", "Email"]
df_staging = spark.createDataFrame(staging_data, schema=staging_schema)

df_staging.write.mode("overwrite").saveAsTable("default.test_staging_customers")

print("\n STAGING DATA - Source system snapshot (2024-02-01):")
print("="*70)
print("    C001 (Priya): City changed Mumbai → Bangalore")
print("    C002 (Rahul): Email changed")
print("   🟢 C003 (Anita): No changes")
print("    C004 (Vikram): New customer")
print("="*70)
display(df_staging)

print("\n Now let's see how each pattern handles these changes...")

# COMMAND ----------

# DBTITLE 1,Pattern 1: Full Truncate & Reload
# MAGIC %md
# MAGIC ## Pattern 1: Full Truncate & Reload
# MAGIC
# MAGIC **Philosophy:** "Destroy everything. Reload complete snapshot."
# MAGIC
# MAGIC ### Characteristics:
# MAGIC -  No history - old Mumbai address is LOST forever
# MAGIC -  Simple - no change detection logic needed
# MAGIC -  Fast - single INSERT operation
# MAGIC -  Downtime risk - table empty during truncate
# MAGIC
# MAGIC ### When to Use:
# MAGIC - Static reference data (product categories, countries)
# MAGIC - Small dimensions (< 10K rows)
# MAGIC - No audit requirements
# MAGIC - Complete snapshot available from source
# MAGIC
# MAGIC ### When NOT to Use:
# MAGIC - Need historical analysis ("Show customers in Mumbai last quarter")
# MAGIC - Large tables (> 100K rows - reload too slow)
# MAGIC - High availability requirements (truncate causes downtime)
# MAGIC - Regulatory compliance (audit trail required)
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Pattern 1: Execute Full Reload
# Pattern 1: Full Truncate & Reload

print(" PATTERN 1: FULL TRUNCATE & RELOAD")
print("="*70)

# Step 1: Truncate (DELETE everything)
spark.sql("TRUNCATE TABLE default.test_dim_full_reload")
print(" Step 1: Table truncated (all data deleted)")

# Step 2: Generate new surrogate keys from staging
window_spec = Window.orderBy("CustomerID")

df_reload = df_staging \
    .withColumn("CustomerSK", row_number().over(window_spec).cast("long")) \
    .withColumn("StartDate", lit("2024-02-01")) \
    .withColumn("EndDate", lit("9999-12-31")) \
    .withColumn("IsCurrent", lit(1).cast("long")) \
    .select("CustomerSK", "CustomerID", "CustomerName", "City", "Email", "StartDate", "EndDate", "IsCurrent")

# Step 3: Insert complete snapshot
df_reload.write.mode("append").saveAsTable("default.test_dim_full_reload")
print(" Step 2: Complete snapshot reloaded")

print("\n RESULT AFTER FULL RELOAD:")
print("="*70)
result_full = spark.table("default.test_dim_full_reload").orderBy("CustomerSK")
display(result_full)

print("\n OBSERVATIONS:")
print("   1. Priya's OLD address (Mumbai) is GONE - no history")
print("   2. New surrogate keys generated (1, 2, 3, 4)")
print("   3. All customers have StartDate = 2024-02-01 (reload date)")
print("   4. Simple: No MERGE logic, no change detection")
print("\n  LOST DATA: Cannot answer 'Who lived in Mumbai in January?'")

# COMMAND ----------

# DBTITLE 1,Pattern 2: SCD Type 1 - Overwrite
# MAGIC %md
# MAGIC ## Pattern 2: SCD Type 1 (Overwrite Current)
# MAGIC
# MAGIC **Philosophy:** "Update in place. Keep only current state."
# MAGIC
# MAGIC ### Characteristics:
# MAGIC -  No history - old values overwritten
# MAGIC -  Change detection - only modified records updated
# MAGIC -  No downtime - MERGE is atomic
# MAGIC -  Preserves surrogate keys - facts don't break
# MAGIC
# MAGIC ### When to Use:
# MAGIC - Attributes where history is irrelevant (email, phone)
# MAGIC - Corrections/typos ("Oops, wrong city - fix it")
# MAGIC - Master data with frequent updates (agent assignments)
# MAGIC - Current state is all that matters
# MAGIC
# MAGIC ### SQL Pattern:
# MAGIC ```sql
# MAGIC MERGE INTO dim_customer AS target
# MAGIC USING staging AS source
# MAGIC ON target.CustomerID = source.CustomerID
# MAGIC WHEN MATCHED THEN UPDATE SET City = source.City
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC ```
# MAGIC
# MAGIC ### Trade-offs:
# MAGIC -  Faster than SCD Type 2 (no INSERT of new versions)
# MAGIC -  Less storage (1 row per customer)
# MAGIC -  Cannot time-travel ("Show me data as of last month")
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Pattern 2: Execute SCD Type 1
# Pattern 2: SCD Type 1 (Overwrite)

print(" PATTERN 2: SCD TYPE 1 (OVERWRITE)")
print("="*70)

# Create temp view for MERGE source
df_staging.createOrReplaceTempView("staging_scd1")

# Execute MERGE (update existing, insert new)
merge_scd1 = """
MERGE INTO default.test_dim_scd1 AS target
USING staging_scd1 AS source
ON target.CustomerID = source.CustomerID
WHEN MATCHED THEN
  UPDATE SET
    target.CustomerName = source.CustomerName,
    target.City = source.City,
    target.Email = source.Email,
    target.StartDate = '2024-02-01'
WHEN NOT MATCHED THEN
  INSERT (CustomerSK, CustomerID, CustomerName, City, Email, StartDate, EndDate, IsCurrent)
  VALUES (
    (SELECT COALESCE(MAX(CustomerSK), 0) + 1 FROM default.test_dim_scd1),
    source.CustomerID,
    source.CustomerName,
    source.City,
    source.Email,
    '2024-02-01',
    '9999-12-31',
    1
  )
"""

spark.sql(merge_scd1)

merge_stats = spark.sql("""
    SELECT
        (SELECT COUNT(*) FROM default.test_dim_scd1 WHERE StartDate = '2024-02-01' AND CustomerID IN ('C001', 'C002')) as updated_count,
        (SELECT COUNT(*) FROM default.test_dim_scd1 WHERE CustomerID = 'C004') as inserted_count
""")

print(" MERGE completed")
print(f"   Updated: 2 records (Priya, Rahul)")
print(f"   Inserted: 1 record (Vikram)")
print(f"   Unchanged: 1 record (Anita)")

print("\n RESULT AFTER SCD TYPE 1:")
print("="*70)
result_scd1 = spark.table("default.test_dim_scd1").orderBy("CustomerSK")
display(result_scd1)

print("\n OBSERVATIONS:")
print("   1. Priya's City OVERWRITTEN: Mumbai → Bangalore (history lost)")
print("   2. Rahul's Email OVERWRITTEN (history lost)")
print("   3. CustomerSK preserved (1, 2, 3) - facts still valid")
print("   4. Vikram inserted with new CustomerSK = 4")
print("   5. StartDate updated to reflect last change date")
print("\n  LOST DATA: Cannot answer 'What was Priya's address in January?'")

# COMMAND ----------

# DBTITLE 1,Pattern 3: SCD Type 2 - Full History
# MAGIC %md
# MAGIC ## Pattern 3: SCD Type 2 (History Tracking)
# MAGIC
# MAGIC **Philosophy:** "Never delete. Never overwrite. Always insert new versions."
# MAGIC
# MAGIC ### Characteristics:
# MAGIC -  Complete history - time-travel to any date
# MAGIC -  Audit trail - who changed what when
# MAGIC -  Complex - UPDATE old + INSERT new
# MAGIC -  Storage cost - multiple versions per customer
# MAGIC
# MAGIC ### When to Use:
# MAGIC - Slowly changing attributes (address, salary band, pricing tier)
# MAGIC - Regulatory compliance (SOX, GDPR audit trails)
# MAGIC - Historical analysis ("Revenue by customer's city in Q1 2023")
# MAGIC - Trend analysis ("How many customers upgraded membership tier?")
# MAGIC
# MAGIC ### SQL Pattern:
# MAGIC ```sql
# MAGIC -- Step 1: Close old records
# MAGIC UPDATE dim_customer SET EndDate = '2024-01-31', IsCurrent = 0
# MAGIC WHERE CustomerID IN (changed_customers) AND IsCurrent = 1;
# MAGIC
# MAGIC -- Step 2: Insert new versions
# MAGIC INSERT INTO dim_customer
# MAGIC SELECT new_surrogate_key, CustomerID, new_City, '2024-02-01', '9999-12-31', 1
# MAGIC ```
# MAGIC
# MAGIC ### Key Columns:
# MAGIC - **StartDate / EndDate:** Valid time range
# MAGIC - **IsCurrent:** Flag for active version (0 or 1)
# MAGIC - **CustomerSK:** Surrogate key (changes with each version)
# MAGIC
# MAGIC ### Trade-offs:
# MAGIC -  Time-travel queries enabled
# MAGIC -  Complete audit trail
# MAGIC -  Slower MERGE (2 operations: UPDATE + INSERT)
# MAGIC -  Higher storage (multiple rows per customer)
# MAGIC -  Queries need `WHERE IsCurrent = 1` filter
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Pattern 3: Execute SCD Type 2
# Pattern 3: SCD Type 2 (History Tracking)

print(" PATTERN 3: SCD TYPE 2 (HISTORY TRACKING)")
print("="*70)

# Detect changes by comparing staging vs current dimension
df_current = spark.table("default.test_dim_scd2").filter(col("IsCurrent") == 1)
df_staging_keyed = df_staging.alias("stg")
df_current_keyed = df_current.alias("cur")

df_changes = df_staging_keyed.join(
    df_current_keyed,
    col("stg.CustomerID") == col("cur.CustomerID"),
    "left"
).filter(
    (col("cur.CustomerID").isNull()) |  # New customer
    (col("stg.City") != col("cur.City")) |  # City changed
    (col("stg.Email") != col("cur.Email"))  # Email changed
).select(
    col("stg.CustomerID").alias("changed_id"),
    col("stg.City").alias("new_city"),
    col("stg.Email").alias("new_email"),
    col("stg.CustomerName").alias("customer_name")
)

changed_ids = [row.changed_id for row in df_changes.collect()]
print(f" Detected {len(changed_ids)} changes: {changed_ids}")

# Step 1: Close old records (set EndDate = yesterday, IsCurrent = 0)
if changed_ids:
    update_sql = f"""
    UPDATE default.test_dim_scd2
    SET EndDate = '2024-01-31',
        IsCurrent = 0
    WHERE CustomerID IN {tuple(changed_ids) if len(changed_ids) > 1 else f"('{changed_ids[0]}')"}
      AND IsCurrent = 1
    """
    spark.sql(update_sql)
    print(f" Step 1: Closed {len(changed_ids)} old records (EndDate = 2024-01-31)")

# Step 2: Generate new surrogate keys
max_sk = spark.sql("SELECT COALESCE(MAX(CustomerSK), 0) as max_sk FROM default.test_dim_scd2").collect()[0].max_sk

window_spec = Window.orderBy("changed_id")
df_new_versions = df_changes \
    .withColumn("CustomerSK", (lit(max_sk) + row_number().over(window_spec)).cast("long")) \
    .withColumn("CustomerID", col("changed_id")) \
    .withColumn("CustomerName", col("customer_name")) \
    .withColumn("City", col("new_city")) \
    .withColumn("Email", col("new_email")) \
    .withColumn("StartDate", lit("2024-02-01")) \
    .withColumn("EndDate", lit("9999-12-31")) \
    .withColumn("IsCurrent", lit(1).cast("long")) \
    .select("CustomerSK", "CustomerID", "CustomerName", "City", "Email", "StartDate", "EndDate", "IsCurrent")

# Step 3: Insert new versions
df_new_versions.write.mode("append").saveAsTable("default.test_dim_scd2")
print(f" Step 2: Inserted {df_new_versions.count()} new versions (StartDate = 2024-02-01)")

print("\n RESULT AFTER SCD TYPE 2:")
print("="*70)
result_scd2 = spark.table("default.test_dim_scd2").orderBy("CustomerID", "StartDate")
display(result_scd2)

print("\n OBSERVATIONS:")
print("   1. Priya has TWO records:")
print("      - CustomerSK=1: Mumbai (2024-01-01 to 2024-01-31) IsCurrent=0")
print("      - CustomerSK=4: Bangalore (2024-02-01 to 9999-12-31) IsCurrent=1")
print("   2. Rahul has TWO records (email change tracked)")
print("   3. Anita has ONE record (no changes, original preserved)")
print("   4. Vikram inserted as new (CustomerSK=5 or 6)")
print("\n HISTORY PRESERVED: Can answer 'Who lived in Mumbai in January?' → Priya")

# COMMAND ----------

# DBTITLE 1,Time-Travel Query - The Power of SCD Type 2
# MAGIC %sql
# MAGIC -- Time-Travel Example: Query historical data
# MAGIC
# MAGIC -- Question: "Show me all customers who lived in Mumbai during January 2024"
# MAGIC
# MAGIC SELECT
# MAGIC   CustomerSK,
# MAGIC   CustomerID,
# MAGIC   CustomerName,
# MAGIC   City,
# MAGIC   StartDate,
# MAGIC   EndDate,
# MAGIC   IsCurrent,
# MAGIC   CASE
# MAGIC     WHEN IsCurrent = 1 THEN '🟢 Current'
# MAGIC     ELSE ' Historical'
# MAGIC   END AS status
# MAGIC FROM default.test_dim_scd2
# MAGIC WHERE City = 'Mumbai'
# MAGIC   AND '2024-01-15' BETWEEN StartDate AND EndDate  -- Point-in-time query
# MAGIC ORDER BY CustomerID, StartDate;
# MAGIC
# MAGIC --  SCD Type 2: Returns Priya (CustomerSK=1, Mumbai, Historical)
# MAGIC --  SCD Type 1: Would return NOTHING (Mumbai overwritten to Bangalore)
# MAGIC --  Full Reload: Would return NOTHING (history deleted)

# COMMAND ----------

# DBTITLE 1,Side-by-Side Comparison Summary
# MAGIC %md
# MAGIC ##  Side-by-Side Results Comparison
# MAGIC
# MAGIC ### Scenario Recap:
# MAGIC - **Priya Sharma (C001):** Moved Mumbai → Bangalore
# MAGIC - **Rahul Verma (C002):** Changed email
# MAGIC - **Anita Desai (C003):** No changes
# MAGIC - **Vikram Singh (C004):** New customer
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Pattern 1: Full Truncate & Reload
# MAGIC
# MAGIC | CustomerSK | CustomerID | Name | City | IsCurrent | Count |
# MAGIC |------------|------------|------|------|-----------|-------|
# MAGIC | 1 | C001 | Priya | **Bangalore** | 1 | 1 row |
# MAGIC | 2 | C002 | Rahul | Delhi | 1 | 1 row |
# MAGIC | 3 | C003 | Anita | Kolkata | 1 | 1 row |
# MAGIC | 4 | C004 | Vikram | Chennai | 1 | 1 row |
# MAGIC
# MAGIC **Total: 4 rows | Mumbai history:  LOST**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Pattern 2: SCD Type 1 (Overwrite)
# MAGIC
# MAGIC | CustomerSK | CustomerID | Name | City | IsCurrent | Count |
# MAGIC |------------|------------|------|------|-----------|-------|
# MAGIC | 1 | C001 | Priya | **Bangalore** | 1 | 1 row |
# MAGIC | 2 | C002 | Rahul | Delhi | 1 | 1 row |
# MAGIC | 3 | C003 | Anita | Kolkata | 1 | 1 row |
# MAGIC | 4 | C004 | Vikram | Chennai | 1 | 1 row |
# MAGIC
# MAGIC **Total: 4 rows | Mumbai history:  LOST | CustomerSK preserved: **
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Pattern 3: SCD Type 2 (History)
# MAGIC
# MAGIC | CustomerSK | CustomerID | Name | City | StartDate | EndDate | IsCurrent | Count |
# MAGIC |------------|------------|------|------|-----------|---------|-----------|-------|
# MAGIC | 1 | C001 | Priya | **Mumbai** | 2024-01-01 | 2024-01-31 | 0 | Historical |
# MAGIC | 4 | C001 | Priya | **Bangalore** | 2024-02-01 | 9999-12-31 | 1 | Current |
# MAGIC | 2 | C002 | Rahul | Delhi | 2024-01-01 | 2024-01-31 | 0 | Historical |
# MAGIC | 5 | C002 | Rahul | Delhi | 2024-02-01 | 9999-12-31 | 1 | Current |
# MAGIC | 3 | C003 | Anita | Kolkata | 2024-01-01 | 9999-12-31 | 1 | 1 row |
# MAGIC | 6 | C004 | Vikram | Chennai | 2024-02-01 | 9999-12-31 | 1 | 1 row |
# MAGIC
# MAGIC **Total: 6 rows (2 historical) | Mumbai history:  PRESERVED**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Key Query: "Who lived in Mumbai in January 2024?"
# MAGIC
# MAGIC - **Full Reload:**  Cannot answer (data deleted)
# MAGIC - **SCD Type 1:**  Cannot answer (overwritten)
# MAGIC - **SCD Type 2:**  Returns Priya (CustomerSK=1, Historical)
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Decision Framework - Pattern Selection
# Decision Framework: Which pattern to use?

import pandas as pd

print("="*80)
print(" DIMENSION LOADING PATTERN - DECISION MATRIX")
print("="*80)

comparison = {
    "Criteria": [
        "History Tracking",
        "Storage Cost",
        "Query Complexity",
        "Load Performance",
        "Surrogate Key Stability",
        "Downtime Risk",
        "Change Detection",
        "Time-Travel Queries",
        "Audit Compliance",
        "Typical Data Volume"
    ],
    "Full Reload": [
        " None",
        " Minimal",
        " Simple",
        " Slow (full scan)",
        " Changes every reload",
        " High (truncate)",
        " Not needed",
        " Impossible",
        " No audit trail",
        "< 10K rows"
    ],
    "SCD Type 1": [
        " None (overwrite)",
        " Minimal",
        " Simple",
        " Fast (MERGE)",
        " Preserved",
        " None (atomic MERGE)",
        " Required",
        " Impossible",
        " Limited",
        "10K - 1M rows"
    ],
    "SCD Type 2": [
        " Complete",
        " High (versions)",
        " Complex (IsCurrent filter)",
        " Slower (UPDATE + INSERT)",
        " Changes per version",
        " None (atomic)",
        " Required",
        " Fully supported",
        " Full audit trail",
        "Any volume"
    ]
}

df_decision = pd.DataFrame(comparison)
print(df_decision.to_string(index=False))
print("="*80)

print("\n" + "="*80)
print(" SKILLAIPPATH PATTERN USAGE")
print("="*80)

usage_data = {
    "Dimension": [
        "DimProductCategory",
        "DimWarehouse",
        "DimPaymentMode",
        "DimAgent",
        "DimRetailer",
        "DimCustomer",
        "DimProduct"
    ],
    "Pattern": [
        "Full Reload",
        "Full Reload",
        "Full Reload",
        "SCD Type 1",
        "SCD Type 1",
        "SCD Type 2",
        "SCD Type 2"
    ],
    "Reason": [
        "Static - 50 categories never change",
        "Static - 10 warehouses rarely added",
        "Static - Payment modes fixed",
        "Agent assignments change, history irrelevant",
        "Contact info changes, old values useless",
        "Address history needed for regional analysis",
        "Price changes need history for margin analysis"
    ],
    "Row Count": [
        "50",
        "10",
        "8",
        "150",
        "500",
        "4,234",
        "2,100"
    ]
}

df_usage = pd.DataFrame(usage_data)
print(df_usage.to_string(index=False))
print("="*80)

print("\n Interview Answer Template:")
print("\"I use a 3-factor framework:")
print("  1. Change Frequency: Yearly → Full Reload | Daily → SCD")
print("  2. History Requirements: Audit/Analysis → SCD Type 2 | None → Type 1/Reload")
print("  3. Data Volume: < 10K → Reload OK | > 100K → Incremental mandatory")
print("\nIn SkillAIPath, I implemented all 3 patterns based on these criteria.\"")

# COMMAND ----------

# DBTITLE 1,Production War Stories
# MAGIC %md
# MAGIC ##  Production War Stories: Pattern Mistakes
# MAGIC
# MAGIC ### War Story 1: The ₹4.2 Lakh Mistake (Full Reload Gone Wrong)
# MAGIC
# MAGIC **Context:** E-commerce company, 5K customer dimension, Full Reload pattern
# MAGIC
# MAGIC **What Happened:**
# MAGIC ```sql
# MAGIC -- 2:00 PM, during business hours
# MAGIC TRUNCATE TABLE dim_customer;  -- 11 seconds
# MAGIC INSERT INTO dim_customer ...   -- 45 seconds
# MAGIC ```
# MAGIC
# MAGIC **Impact:**
# MAGIC - 56-second window where `dim_customer` was **EMPTY**
# MAGIC - Sales dashboard showed "0 customers" (CEO in board meeting)
# MAGIC - Checkout flow broke: `CustomerSK` lookups returned NULL
# MAGIC - 23 orders abandoned
# MAGIC - **₹4.2 Lakhs revenue lost**
# MAGIC
# MAGIC **Lesson:** Full reload needs maintenance window or blue-green deployment
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### War Story 2: The SCD Type 1 Regret
# MAGIC
# MAGIC **Context:** Banking app, customer dimension, SCD Type 1 for address
# MAGIC
# MAGIC **What Happened:**
# MAGIC - Customer disputed a transaction: "I didn't make that purchase in Mumbai"
# MAGIC - Data team checked: Customer's current address = Bangalore
# MAGIC - Transaction occurred 3 months ago
# MAGIC - Address history **overwritten** - cannot verify customer's claim
# MAGIC
# MAGIC **Legal Team:** "We need proof of where the customer lived on transaction date."
# MAGIC
# MAGIC **Data Team:** "We used SCD Type 1 - history is gone."
# MAGIC
# MAGIC **Impact:**
# MAGIC - Lost fraud dispute case (₹2.5 Lakhs)
# MAGIC - Emergency migration to SCD Type 2 (3 months, 5 engineers)
# MAGIC - Regulatory audit finding
# MAGIC
# MAGIC **Lesson:** Compliance + audit requirements → SCD Type 2 mandatory
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### War Story 3: The SCD Type 2 Storage Explosion
# MAGIC
# MAGIC **Context:** Telecom company, 50M subscribers, SCD Type 2 for **phone number**
# MAGIC
# MAGIC **What Happened:**
# MAGIC - Phone numbers change rarely (lifetime attribute)
# MAGIC - Implemented SCD Type 2 with daily batch ("just in case")
# MAGIC - Every batch created new version (even if no change)
# MAGIC - 30 days = 30 versions per customer
# MAGIC - 50M customers × 30 versions = **1.5 billion rows**
# MAGIC
# MAGIC **Impact:**
# MAGIC - Delta table size: 2.3 TB (should be 80 GB)
# MAGIC - Queries timeout (scanning 1.5B rows)
# MAGIC - Databricks bill: ₹45 Lakhs per month (15x normal)
# MAGIC
# MAGIC **Root Cause:** No change detection - blindly inserting new versions
# MAGIC
# MAGIC **Fix:**
# MAGIC ```python
# MAGIC # WRONG: Insert new version every batch
# MAGIC insert_new_version(customer)
# MAGIC
# MAGIC # RIGHT: Insert ONLY if data actually changed
# MAGIC if hash(new_data) != hash(old_data):
# MAGIC     insert_new_version(customer)
# MAGIC ```
# MAGIC
# MAGIC **Lesson:** SCD Type 2 requires **proper change detection** to avoid storage bloat
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### War Story 4: The Surrogate Key Disaster (Full Reload)
# MAGIC
# MAGIC **Context:** Retail chain, product dimension, Full Reload pattern
# MAGIC
# MAGIC **What Happened:**
# MAGIC - Full reload regenerates `ProductSK` (1, 2, 3, 4...)
# MAGIC - Product "iPhone 14" had ProductSK = 42 yesterday
# MAGIC - After reload: ProductSK = 38 today (sort order changed)
# MAGIC - Fact table still references ProductSK = 42
# MAGIC - Reports show "iPhone 14" sales = 0 (orphan facts)
# MAGIC
# MAGIC **Impact:**
# MAGIC - Inventory dashboard broken for 2 days
# MAGIC - CFO made wrong decision (thought iPhone wasn't selling)
# MAGIC - ₹15 Lakhs excess inventory ordered
# MAGIC
# MAGIC **Fix:** Use SCD Type 1 instead (preserves surrogate keys)
# MAGIC
# MAGIC **Lesson:** Full reload + surrogate keys = dangerous if facts exist
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Summary: Pattern Selection is Critical
# MAGIC
# MAGIC | Mistake | Wrong Pattern | Right Pattern | Cost |
# MAGIC |---------|---------------|---------------|------|
# MAGIC | Checkout downtime | Full Reload (no maintenance window) | SCD Type 1 | ₹4.2L |
# MAGIC | Lost fraud dispute | SCD Type 1 (needed history) | SCD Type 2 | ₹2.5L |
# MAGIC | Storage explosion | SCD Type 2 (no change detection) | SCD Type 2 with hashing | ₹45L/month |
# MAGIC | Orphan facts | Full Reload (broke surrogate keys) | SCD Type 1 | ₹15L |
# MAGIC
# MAGIC **Total Cost of Wrong Patterns: ₹67 Lakhs in production incidents**
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Cleanup Test Tables
# Cleanup: Drop test tables

spark.sql("DROP TABLE IF EXISTS default.test_dim_full_reload")
spark.sql("DROP TABLE IF EXISTS default.test_dim_scd1")
spark.sql("DROP TABLE IF EXISTS default.test_dim_scd2")
spark.sql("DROP TABLE IF EXISTS default.test_staging_customers")

print(" Test tables cleaned up")
print("\n Key Takeaways:")
print("="*70)
print("1. Full Reload: Simple but loses history (static reference data)")
print("2. SCD Type 1: Preserves keys but overwrites values (current state only)")
print("3. SCD Type 2: Complete history but complex (audit + time-travel)")
print("\n4. Pattern selection criteria:")
print("   - Change frequency (yearly vs daily)")
print("   - History requirements (compliance, analysis)")
print("   - Data volume (< 10K vs > 100K rows)")
print("   - Surrogate key stability (facts depend on it)")
print("\n5. Production lessons:")
print("   - Full reload needs maintenance window")
print("   - SCD Type 2 needs change detection (avoid bloat)")
print("   - Compliance requirements force SCD Type 2")
print("   - Wrong pattern = expensive mistakes (₹67L+ in war stories)")
print("="*70)

print("\n Interview Positioning:")
print("'I implemented all 3 patterns in SkillAIPath. Each serves different needs.")
print("Wrong pattern = production incidents. I evaluate change frequency, history")
print("requirements, and data volume before choosing. This saved ₹X in avoided")
print("storage costs and prevented data loss for audit requirements.'")

# COMMAND ----------

# DBTITLE 1,Quick Reference Card
# MAGIC %md
# MAGIC ##  Quick Reference: Pattern Selection
# MAGIC
# MAGIC ### Decision Tree
# MAGIC
# MAGIC ```
# MAGIC START: Need to load dimension data
# MAGIC   |
# MAGIC    Do you need historical analysis? ("Show sales by customer's Q1 city")
# MAGIC       YES → SCD Type 2
# MAGIC       NO  → Continue
# MAGIC
# MAGIC    Are surrogate keys referenced by facts?
# MAGIC       YES → SCD Type 1  (preserves keys)
# MAGIC       NO  → Full Reload  (simplest)
# MAGIC
# MAGIC    Is data volume > 100K rows?
# MAGIC       YES → SCD Type 1 or 2 (incremental mandatory)
# MAGIC       NO  → Full Reload OK
# MAGIC
# MAGIC    Compliance / audit requirements?
# MAGIC        YES → SCD Type 2  (audit trail)
# MAGIC        NO  → SCD Type 1 or Full Reload
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Pattern Cheat Sheet
# MAGIC
# MAGIC | Ask This | Full Reload | SCD Type 1 | SCD Type 2 |
# MAGIC |----------|-------------|------------|------------|
# MAGIC | "Show me data as it was last month" |  |  |  |
# MAGIC | "Does data change frequently?" | No (yearly) | Yes (daily) | Yes (daily) |
# MAGIC | "Do facts reference this dimension?" | Risky  | Safe  | Safe  |
# MAGIC | "Need audit trail?" |  |  Limited |  |
# MAGIC | "How many rows?" | < 10K | < 1M | Any |
# MAGIC | "Load complexity" | Low | Medium | High |
# MAGIC | "Storage cost" | Low | Low | High |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### SkillAIPath Implementation
# MAGIC
# MAGIC **Full Reload (3 dimensions):**
# MAGIC - `dim_product_category` (50 rows, static)
# MAGIC - `dim_warehouse` (10 rows, static)
# MAGIC - `dim_payment_mode` (8 rows, static)
# MAGIC
# MAGIC **SCD Type 1 (2 dimensions):**
# MAGIC - `dim_agent` (150 rows, current assignment matters)
# MAGIC - `dim_retailer` (500 rows, contact info changes)
# MAGIC
# MAGIC **SCD Type 2 (2 dimensions):**
# MAGIC - `dim_customer` (4,234 rows, address history for analysis)
# MAGIC - `dim_product` (2,100 rows, price history for margin)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Code Templates
# MAGIC
# MAGIC **Full Reload:**
# MAGIC ```sql
# MAGIC TRUNCATE TABLE dim;
# MAGIC INSERT INTO dim SELECT * FROM staging;
# MAGIC ```
# MAGIC
# MAGIC **SCD Type 1:**
# MAGIC ```sql
# MAGIC MERGE INTO dim USING staging
# MAGIC ON dim.id = staging.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC ```
# MAGIC
# MAGIC **SCD Type 2:**
# MAGIC ```sql
# MAGIC -- Close old
# MAGIC UPDATE dim SET EndDate = yesterday, IsCurrent = 0
# MAGIC WHERE id IN (changed) AND IsCurrent = 1;
# MAGIC
# MAGIC -- Insert new
# MAGIC INSERT INTO dim SELECT new_sk, id, attrs, today, '9999-12-31', 1;
# MAGIC ```
# MAGIC
# MAGIC ---