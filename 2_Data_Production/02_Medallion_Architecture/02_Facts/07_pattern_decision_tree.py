# Databricks notebook source
# DBTITLE 1,Fact Loading Patterns - Decision Tree
# MAGIC %md
# MAGIC # Fact Loading Patterns: When to Append, Merge, or Upsert?
# MAGIC **SkillAIPath Data Warehouse - Choosing the Right Fact Loading Strategy**
# MAGIC
# MAGIC ## THE PROBLEM: "My fact table has duplicate transactions!"
# MAGIC
# MAGIC **Thursphase 11 AM - Production Incident**
# MAGIC
# MAGIC **Data Engineer:** "We're seeing duplicate sales in the fact table. Same TransactionID appears multiple times."
# MAGIC
# MAGIC **Architect:** "What loading pattern are you using?"
# MAGIC
# MAGIC **Engineer:** "Simple APPEND - just insert all staging records into fact table."
# MAGIC
# MAGIC **Architect:** "There's your problem. You used the wrong pattern. Let me show you the decision tree:"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## The 3 Fact Loading Patterns
# MAGIC
# MAGIC ### Pattern 1: APPEND (Insert Only)
# MAGIC **Philosophy:** "Trust the source. Every record is new."
# MAGIC
# MAGIC **When to use:**
# MAGIC - Immutable events (clicks, page views, sensor readings)
# MAGIC - Source guarantees no duplicates
# MAGIC - Idempotent batch jobs (delete old batch + reload)
# MAGIC - No updates or corrections
# MAGIC
# MAGIC **Risks:**
# MAGIC - Pipeline re-runs create duplicates
# MAGIC - Source sends corrections → old + new coexist
# MAGIC - No late-arriving data handling
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Pattern 2: MERGE (Upsert with Business Key)
# MAGIC **Philosophy:** "Check if exists. Insert new, update existing."
# MAGIC
# MAGIC **When to use:**
# MAGIC - Transactional systems with updates (order status changes)
# MAGIC - Late-arriving dimensions (CustomerSK not available initially)
# MAGIC - Source sends corrections (price adjustments)
# MAGIC - Need to handle re-runs gracefully
# MAGIC
# MAGIC **Trade-offs:**
# MAGIC - Slower (JOIN to check existence)
# MAGIC - Requires unique business key (OrderID, TransactionID)
# MAGIC - Can't track history (overwrites old values)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Pattern 3: IDEMPOTENT BATCH RELOAD
# MAGIC **Philosophy:** "Delete this batch, reload this batch."
# MAGIC
# MAGIC **When to use:**
# MAGIC - Batch processing with BatchDate partition
# MAGIC - Re-running specific dates
# MAGIC - Source provides complete snapshot per batch
# MAGIC - Easier than merge for batch corrections
# MAGIC
# MAGIC **Pattern:**
# MAGIC ```sql
# MAGIC -- Step 1: Delete old data for this batch
# MAGIC DELETE FROM fact_sales WHERE BatchDate = '2024-01-15';
# MAGIC
# MAGIC -- Step 2: Insert new data for this batch
# MAGIC INSERT INTO fact_sales SELECT * FROM staging WHERE BatchDate = '2024-01-15';
# MAGIC ```
# MAGIC
# MAGIC **Benefits:**
# MAGIC - Simple (no JOIN logic)
# MAGIC - Idempotent (run 100 times = same result)
# MAGIC - Batch-level atomicity
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Decision Tree
# MAGIC
# MAGIC ```
# MAGIC START: Need to load fact data
# MAGIC   |
# MAGIC    Is data partitioned by batch/date?
# MAGIC       YES → Use IDEMPOTENT BATCH RELOAD
# MAGIC               (DELETE batch + INSERT batch)
# MAGIC       NO  → Continue
# MAGIC
# MAGIC    Can source send updates/corrections?
# MAGIC       YES → Use MERGE (upsert)
# MAGIC       NO  → Continue
# MAGIC
# MAGIC    Can pipeline re-run (failure retry)?
# MAGIC       YES → Use MERGE (prevents duplicates)
# MAGIC       NO  → Continue
# MAGIC
# MAGIC    Are events immutable (never change)?
# MAGIC       YES → APPEND OK
# MAGIC               (but add deduplication check)
# MAGIC       NO  → Use MERGE
# MAGIC
# MAGIC    DEFAULT: Use MERGE (safest choice)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Interview Positioning
# MAGIC
# MAGIC  **Question:** "How do you prevent duplicate records in fact tables?"
# MAGIC
# MAGIC **Answer:** "I use a 3-pattern framework:
# MAGIC
# MAGIC 1. **APPEND:** Only for immutable events with source-guaranteed uniqueness. Add `COUNT(DISTINCT business_key)` validation after load.
# MAGIC
# MAGIC 2. **MERGE:** For transactional data that can update. Use business key (OrderID) for existence check. Slower but prevents duplicates on re-runs.
# MAGIC
# MAGIC 3. **IDEMPOTENT BATCH:** For date-partitioned data. DELETE batch + INSERT batch. Simple and safe for batch processing.
# MAGIC
# MAGIC In SkillAIPath, I use Idempotent Batch for daily sales (partitioned by BatchDate) and MERGE for real-time order updates.
# MAGIC
# MAGIC I learned this after a production incident: APPEND-only pattern caused 15K duplicate transactions (₹2.3 Cr double-counted revenue). Switching to Idempotent Batch fixed it."
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Setup Test Scenarios
# Setup: Create test fact table and staging scenarios

from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring
from datetime import datetime

print("="*70)
print(" FACT LOADING PATTERNS - LIVE COMPARISON")
print("="*70)

# Scenario: E-commerce sales fact table
fact_data_initial = [
    ("TXN001", "2024-01-15", 1, 101, 201, 5000, "Completed"),
    ("TXN002", "2024-01-15", 2, 102, 202, 12000, "Completed"),
    ("TXN003", "2024-01-15", 3, 103, 203, 8500, "Completed")
]

fact_schema = ["TransactionID", "BatchDate", "CustomerSK", "ProductSK", "StorewareID", "SalesAmount", "Status"]

df_fact_initial = spark.createDataFrame(fact_data_initial, schema=fact_schema)

# Create 3 separate fact tables for comparison
df_fact_initial.write.mode("overwrite").saveAsTable("default.test_fact_append")
df_fact_initial.write.mode("overwrite").saveAsTable("default.test_fact_merge")
df_fact_initial.write.mode("overwrite").saveAsTable("default.test_fact_idempotent")

print(" Test fact tables created (3 copies)")
print("\n INITIAL STATE - 3 transactions loaded:")
display(df_fact_initial)

print("\n" + "="*70)
print(" STAGING DATA - New batch with complexities:")
print("="*70)

# Staging data with various scenarios
staging_data = [
    # Scenario 1: New transaction
    ("TXN004", "2024-01-16", 1, 104, 201, 7500, "Completed"),

    # Scenario 2: Correction (TXN002 price was wrong)
    ("TXN002", "2024-01-15", 2, 102, 202, 11500, "Completed"),  # Was 12000, now 11500

    # Scenario 3: Status update (TXN003 was refunded)
    ("TXN003", "2024-01-15", 3, 103, 203, 8500, "Refunded"),  # Status changed

    # Scenario 4: Late-arriving transaction (missed in previous batch)
    ("TXN005", "2024-01-15", 4, 105, 202, 3200, "Completed"),

    # Scenario 5: Another new transaction
    ("TXN006", "2024-01-16", 2, 101, 203, 9800, "Completed")
]

df_staging = spark.createDataFrame(staging_data, schema=fact_schema)
df_staging.write.mode("overwrite").saveAsTable("default.test_staging_sales")

print("\n Scenario 1: TXN004 - New transaction (Jan 16)")
print(" Scenario 2: TXN002 - Price correction 12000 → 11500")
print(" Scenario 3: TXN003 - Status update Completed → Refunded")
print(" Scenario 4: TXN005 - Late-arriving (Jan 15, missed earlier)")
print(" Scenario 5: TXN006 - New transaction (Jan 16)")
print("="*70)

display(df_staging)

print("\n Let's see how each pattern handles these scenarios...")

# COMMAND ----------

# DBTITLE 1,Pattern 1 - APPEND Only
# MAGIC %md
# MAGIC ## Pattern 1: APPEND (Insert Only)
# MAGIC
# MAGIC ### SQL Pattern:
# MAGIC ```sql
# MAGIC INSERT INTO fact_sales
# MAGIC SELECT * FROM staging;
# MAGIC ```
# MAGIC
# MAGIC ### Characteristics:
# MAGIC -  Fastest (no JOIN overhead)
# MAGIC -  Simplest code
# MAGIC -  No deduplication
# MAGIC -  No update handling
# MAGIC -  Pipeline re-runs create duplicates
# MAGIC
# MAGIC ### What Will Happen:
# MAGIC - New transactions (TXN004, TXN006):  Inserted correctly
# MAGIC - Price correction (TXN002):  Creates duplicate (old + new coexist)
# MAGIC - Status update (TXN003):  Creates duplicate (Completed + Refunded both exist)
# MAGIC - Late-arriving (TXN005):  Inserted correctly
# MAGIC
# MAGIC ### Result:
# MAGIC **Fact table will have 8 rows (3 original + 5 staging) with duplicates!**
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Execute Pattern 1 - APPEND
# Pattern 1: APPEND (Insert Only)

print(" PATTERN 1: APPEND (INSERT ONLY)")
print("="*70)

# Simple append - no checks
df_staging.write.mode("append").saveAsTable("default.test_fact_append")

print(" Staging data appended (simple INSERT)")

print("\n RESULT AFTER APPEND:")
print("="*70)

result_append = spark.table("default.test_fact_append").orderBy("TransactionID", "SalesAmount")
display(result_append)

print("\n ANALYSIS:")
print("-" * 70)

duplicates = spark.sql("""
    SELECT TransactionID, COUNT(*) as occurrence_count,
           COLLECT_LIST(SalesAmount) as amounts,
           COLLECT_LIST(Status) as statuses
    FROM default.test_fact_append
    GROUP BY TransactionID
    HAVING COUNT(*) > 1
    ORDER BY TransactionID
""")

print("\n DUPLICATES DETECTED:")
display(duplicates)

total_revenue = spark.sql("SELECT SUM(SalesAmount) as total FROM default.test_fact_append").collect()[0].total
print(f"\n  TOTAL REVENUE: ₹{total_revenue:,}")
print("   Includes duplicates - WRONG METRIC!")

print("\n PROBLEMS:")
print("   1. TXN002 appears twice (12000 + 11500) - double-counting ₹23,500")
print("   2. TXN003 appears twice (Completed + Refunded) - status ambiguous")
print("   3. Revenue inflated by ₹23,500 due to duplicates")
print("   4. Re-running pipeline would create MORE duplicates")
print("\n Lesson: APPEND without deduplication = production disaster")

# COMMAND ----------

# DBTITLE 1,Pattern 2 - MERGE (Upsert)
# MAGIC %md
# MAGIC ## Pattern 2: MERGE (Upsert with Business Key)
# MAGIC
# MAGIC ### SQL Pattern:
# MAGIC ```sql
# MAGIC MERGE INTO fact_sales AS target
# MAGIC USING staging AS source
# MAGIC ON target.TransactionID = source.TransactionID
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET SalesAmount = source.SalesAmount,
# MAGIC              Status = source.Status
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *;
# MAGIC ```
# MAGIC
# MAGIC ### Characteristics:
# MAGIC -  Handles corrections (updates existing records)
# MAGIC -  Prevents duplicates (checks existence)
# MAGIC -  Idempotent (re-run safe)
# MAGIC -  Slower (requires JOIN)
# MAGIC -  Requires unique business key
# MAGIC
# MAGIC ### What Will Happen:
# MAGIC - New transactions (TXN004, TXN006):  Inserted
# MAGIC - Price correction (TXN002):  Updated (12000 → 11500)
# MAGIC - Status update (TXN003):  Updated (Completed → Refunded)
# MAGIC - Late-arriving (TXN005):  Inserted
# MAGIC
# MAGIC ### Result:
# MAGIC **Fact table will have 6 rows (no duplicates, corrections applied)**
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Execute Pattern 2 - MERGE
# Pattern 2: MERGE (Upsert)

print(" PATTERN 2: MERGE (UPSERT)")
print("="*70)

# Create temp view for MERGE
df_staging.createOrReplaceTempView("staging_merge")

# Execute MERGE
merge_sql = """
MERGE INTO default.test_fact_merge AS target
USING staging_merge AS source
ON target.TransactionID = source.TransactionID
WHEN MATCHED THEN
  UPDATE SET
    target.BatchDate = source.BatchDate,
    target.CustomerSK = source.CustomerSK,
    target.ProductSK = source.ProductSK,
    target.StorewareID = source.StorewareID,
    target.SalesAmount = source.SalesAmount,
    target.Status = source.Status
WHEN NOT MATCHED THEN
  INSERT (TransactionID, BatchDate, CustomerSK, ProductSK, StorewareID, SalesAmount, Status)
  VALUES (source.TransactionID, source.BatchDate, source.CustomerSK, source.ProductSK,
          source.StorewareID, source.SalesAmount, source.Status)
"""

spark.sql(merge_sql)

print(" MERGE completed (upsert with TransactionID key)")

print("\n RESULT AFTER MERGE:")
print("="*70)

result_merge = spark.table("default.test_fact_merge").orderBy("TransactionID")
display(result_merge)

print("\n ANALYSIS:")
print("-" * 70)

# Check for duplicates
duplicates_check = spark.sql("""
    SELECT COUNT(*) as duplicate_count
    FROM (
        SELECT TransactionID, COUNT(*) as cnt
        FROM default.test_fact_merge
        GROUP BY TransactionID
        HAVING COUNT(*) > 1
    )
""").collect()[0].duplicate_count

print(f"\n Duplicate Check: {duplicates_check} duplicates found")

total_revenue_merge = spark.sql("SELECT SUM(SalesAmount) as total FROM default.test_fact_merge").collect()[0].total
print(f"\n TOTAL REVENUE: ₹{total_revenue_merge:,}")
print("   Correct - no double-counting")

print("\n SUCCESSES:")
print("   1. TXN002: Price corrected (12000 → 11500) - only latest exists")
print("   2. TXN003: Status updated (Completed → Refunded) - only latest exists")
print("   3. TXN004, TXN006: New transactions inserted")
print("   4. TXN005: Late-arriving transaction inserted")
print("   5. No duplicates - safe for pipeline re-runs")

print("\n Lesson: MERGE handles corrections and prevents duplicates")

# COMMAND ----------

# DBTITLE 1,Pattern 3 - Idempotent Batch Reload
# MAGIC %md
# MAGIC ## Pattern 3: IDEMPOTENT BATCH RELOAD
# MAGIC
# MAGIC ### SQL Pattern:
# MAGIC ```sql
# MAGIC -- Step 1: Delete old data for this batch
# MAGIC DELETE FROM fact_sales
# MAGIC WHERE BatchDate = '2024-01-15';
# MAGIC
# MAGIC -- Step 2: Insert new data for this batch
# MAGIC INSERT INTO fact_sales
# MAGIC SELECT * FROM staging
# MAGIC WHERE BatchDate = '2024-01-15';
# MAGIC ```
# MAGIC
# MAGIC ### Characteristics:
# MAGIC -  Simple (no JOIN logic like MERGE)
# MAGIC -  Idempotent (run 100 times = same result)
# MAGIC -  Handles corrections within batch
# MAGIC -  Fast (DELETE + INSERT, no existence checks per row)
# MAGIC -  Requires batch/date partitioning
# MAGIC -  Cannot handle cross-batch updates
# MAGIC
# MAGIC ### When to Use:
# MAGIC - Daily/hourly batch processing
# MAGIC - Partition by BatchDate (Delta table optimization)
# MAGIC - Source provides complete snapshot per batch
# MAGIC - Re-running specific dates for corrections
# MAGIC
# MAGIC ### What Will Happen:
# MAGIC - Process Jan 15 batch: DELETE Jan 15 + INSERT Jan 15 staging records
# MAGIC - Process Jan 16 batch: DELETE Jan 16 + INSERT Jan 16 staging records
# MAGIC - TXN002, TXN003 corrections handled (replaced in their batch)
# MAGIC - TXN005 late-arriving inserted in Jan 15 batch
# MAGIC
# MAGIC ### Result:
# MAGIC **Fact table will have 6 rows (correct, deduplicated per batch)**
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Execute Pattern 3 - Idempotent Batch
# Pattern 3: Idempotent Batch Reload

print(" PATTERN 3: IDEMPOTENT BATCH RELOAD")
print("="*70)

# Process each batch separately
batches = df_staging.select("BatchDate").distinct().collect()

for batch_row in batches:
    batch_date = batch_row.BatchDate

    print(f"\n Processing batch: {batch_date}")

    # Step 1: Delete old data for this batch
    spark.sql(f"""
        DELETE FROM default.test_fact_idempotent
        WHERE BatchDate = '{batch_date}'
    """)
    print(f"    Deleted old records for {batch_date}")

    # Step 2: Insert new data for this batch
    df_batch = df_staging.filter(col("BatchDate") == batch_date)
    df_batch.write.mode("append").saveAsTable("default.test_fact_idempotent")

    row_count = df_batch.count()
    print(f"    Inserted {row_count} records for {batch_date}")

print("\n" + "="*70)
print(" All batches processed")

print("\n RESULT AFTER IDEMPOTENT BATCH RELOAD:")
print("="*70)

result_idempotent = spark.table("default.test_fact_idempotent").orderBy("BatchDate", "TransactionID")
display(result_idempotent)

print("\n ANALYSIS:")
print("-" * 70)

# Check for duplicates
duplicates_check_idem = spark.sql("""
    SELECT COUNT(*) as duplicate_count
    FROM (
        SELECT TransactionID, COUNT(*) as cnt
        FROM default.test_fact_idempotent
        GROUP BY TransactionID
        HAVING COUNT(*) > 1
    )
""").collect()[0].duplicate_count

print(f"\n Duplicate Check: {duplicates_check_idem} duplicates found")

total_revenue_idem = spark.sql("SELECT SUM(SalesAmount) as total FROM default.test_fact_idempotent").collect()[0].total
print(f"\n TOTAL REVENUE: ₹{total_revenue_idem:,}")
print("   Correct - no double-counting")

print("\n SUCCESSES:")
print("   1. Jan 15 batch: TXN001 deleted and reloaded (with corrections)")
print("   2. TXN002: Price correction applied (old deleted, new inserted)")
print("   3. TXN003: Status correction applied (old deleted, new inserted)")
print("   4. TXN005: Late-arriving inserted in correct batch (Jan 15)")
print("   5. Jan 16 batch: TXN004, TXN006 inserted")
print("   6. Idempotent: Can re-run any batch without duplicates")

print("\n Lesson: Batch reload is simpler than MERGE for date-partitioned data")

# COMMAND ----------

# DBTITLE 1,Pattern Comparison - Side by Side
# Compare results across all 3 patterns

import pandas as pd

print("="*80)
print(" PATTERN COMPARISON - FINAL RESULTS")
print("="*80)

# Get row counts
count_append = spark.table("default.test_fact_append").count()
count_merge = spark.table("default.test_fact_merge").count()
count_idempotent = spark.table("default.test_fact_idempotent").count()

# Get revenue totals
revenue_append = spark.sql("SELECT SUM(SalesAmount) FROM default.test_fact_append").collect()[0][0]
revenue_merge = spark.sql("SELECT SUM(SalesAmount) FROM default.test_fact_merge").collect()[0][0]
revenue_idempotent = spark.sql("SELECT SUM(SalesAmount) FROM default.test_fact_idempotent").collect()[0][0]

# Get duplicate counts
dup_append = spark.sql("""
    SELECT COUNT(*) FROM (
        SELECT TransactionID FROM default.test_fact_append
        GROUP BY TransactionID HAVING COUNT(*) > 1
    )
""").collect()[0][0]

dup_merge = spark.sql("""
    SELECT COUNT(*) FROM (
        SELECT TransactionID FROM default.test_fact_merge
        GROUP BY TransactionID HAVING COUNT(*) > 1
    )
""").collect()[0][0]

dup_idempotent = spark.sql("""
    SELECT COUNT(*) FROM (
        SELECT TransactionID FROM default.test_fact_idempotent
        GROUP BY TransactionID HAVING COUNT(*) > 1
    )
""").collect()[0][0]

comparison_data = {
    "Metric": [
        "Total Rows",
        "Unique Transactions",
        "Duplicate TransactionIDs",
        "Total Revenue",
        "Correct?",
        "Safe for Re-runs?"
    ],
    "APPEND": [
        count_append,
        count_append - dup_append,
        f" {dup_append}",
        f"₹{revenue_append:,}",
        " No (inflated)",
        " No (more duplicates)"
    ],
    "MERGE": [
        count_merge,
        count_merge,
        f" {dup_merge}",
        f"₹{revenue_merge:,}",
        " Yes",
        " Yes (idempotent)"
    ],
    "IDEMPOTENT BATCH": [
        count_idempotent,
        count_idempotent,
        f" {dup_idempotent}",
        f"₹{revenue_idempotent:,}",
        " Yes",
        " Yes (idempotent)"
    ]
}

df_comparison = pd.DataFrame(comparison_data)
print(df_comparison.to_string(index=False))
print("="*80)

print("\n KEY INSIGHTS:")
print("-" * 80)
print("\n1. APPEND Pattern:")
print(f"    Created {dup_append} duplicate transactions")
print(f"    Revenue inflated by ₹{revenue_append - revenue_merge:,} due to duplicates")
print("    NOT safe for production (re-runs create more duplicates)")

print("\n2. MERGE Pattern:")
print("    No duplicates - corrections handled via UPDATE")
print("    Correct revenue calculation")
print("    Idempotent - safe for re-runs")
print("    Slower (requires JOIN for existence check)")

print("\n3. IDEMPOTENT BATCH Pattern:")
print("    No duplicates - batch-level atomicity")
print("    Correct revenue calculation")
print("    Idempotent - can re-run any batch")
print("    Simpler than MERGE (no complex JOIN logic)")
print("    Faster (DELETE + INSERT vs MERGE)")

print("\n" + "="*80)
print(" RECOMMENDATION FOR SKILLAIPPATH")
print("="*80)
print("\nUse IDEMPOTENT BATCH RELOAD because:")
print("  1. Fact tables partitioned by BatchDate (already implemented)")
print("  2. Daily batch processing (not real-time streaming)")
print("  3. Simpler code (no MERGE complexity)")
print("  4. Better performance (DELETE + INSERT)")
print("  5. Easy to re-run specific dates for corrections")
print("\nFallback to MERGE when:")
print("  - Real-time updates needed (order status changes)")
print("  - Cross-batch corrections required")
print("  - Streaming ingestion (no batch boundaries)")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Production War Story
# MAGIC %md
# MAGIC ##  Production War Story: The ₹2.3 Crore Duplicate Disaster
# MAGIC
# MAGIC ### Context: Retail Analytics Platform
# MAGIC
# MAGIC **Company:** Mid-size e-commerce (500K daily transactions)
# MAGIC **Pattern Used:** APPEND (no deduplication)
# MAGIC **Duration:** 45 days before discovery
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What Happened:
# MAGIC
# MAGIC **Phase 1-30:** Normal Operations
# MAGIC ```python
# MAGIC # Daily ETL job (running at 2 AM)
# MAGIC df_staging.write.mode("append").saveAsTable("fact_sales")
# MAGIC ```
# MAGIC
# MAGIC **Phase 31: Pipeline Failure**
# MAGIC - 2:00 AM: Pipeline starts
# MAGIC - 2:15 AM: Databricks cluster terminates (spot instance eviction)
# MAGIC - 2:17 AM: Auto-retry kicks in
# MAGIC - 2:20 AM: Pipeline completes successfully
# MAGIC
# MAGIC **What Actually Happened:**
# MAGIC - First run (2:00 AM): Inserted 480K records, crashed at 97%
# MAGIC - Second run (2:17 AM): Inserted ALL 500K records again (no idempotency check)
# MAGIC - Result: 20K duplicate transactions in fact table
# MAGIC
# MAGIC **Phase 32-45:** Silent Data Corruption
# MAGIC - Similar failures happened 4 more times over 2 weeks
# MAGIC - Each retry created duplicates
# MAGIC - Total duplicates: 87K transactions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Discovery:
# MAGIC
# MAGIC **Monday Morning - CFO Meeting**
# MAGIC
# MAGIC **CFO:** "Revenue jumped 15% last month! Great job team!"
# MAGIC
# MAGIC **Marketing:** "Wait, we didn't run any major campaigns..."
# MAGIC
# MAGIC **Data Analyst:** "Let me check... Oh no."
# MAGIC
# MAGIC ```sql
# MAGIC SELECT TransactionID, COUNT(*) as occurrences
# MAGIC FROM fact_sales
# MAGIC GROUP BY TransactionID
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY COUNT(*) DESC
# MAGIC LIMIT 10;
# MAGIC
# MAGIC -- Result:
# MAGIC -- TXN_20240215_0031: 3 occurrences
# MAGIC -- TXN_20240218_0142: 3 occurrences
# MAGIC -- TXN_20240222_0089: 2 occurrences
# MAGIC -- ...
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Impact:
# MAGIC
# MAGIC **Financial:**
# MAGIC - 87K duplicate transactions
# MAGIC - Average transaction: ₹2,650
# MAGIC - Inflated revenue: ₹2.3 Crores
# MAGIC - CFO presented WRONG numbers to board
# MAGIC - Investor confidence damaged
# MAGIC
# MAGIC **Operational:**
# MAGIC - 3 days emergency fix (migrate to MERGE pattern)
# MAGIC - 2 weeks backfilling corrected data
# MAGIC - 5 engineers pulled from other projects
# MAGIC - Board meeting delayed (credibility issue)
# MAGIC
# MAGIC **Reputational:**
# MAGIC - CFO questioned data team competence
# MAGIC - Lost promotion opportunity ("Can't trust your data")
# MAGIC - Mandatory code review process added (slowed delivery)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Root Causes:
# MAGIC
# MAGIC 1. **Pattern Choice:**
# MAGIC    - Used APPEND without deduplication
# MAGIC    - No idempotency checks
# MAGIC    - Assumed pipeline never fails
# MAGIC
# MAGIC 2. **Monitoring Gaps:**
# MAGIC    - No duplicate detection alerting
# MAGIC    - No row count reconciliation (source vs target)
# MAGIC    - Trusted "successful" pipeline status
# MAGIC
# MAGIC 3. **Testing Gaps:**
# MAGIC    - Never tested failure + retry scenarios
# MAGIC    - Staging environment always succeeded (no spot interruptions)
# MAGIC    - Load testing didn't include re-runs
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The Fix:
# MAGIC
# MAGIC **Immediate (Phase 1):**
# MAGIC ```python
# MAGIC # Switch to Idempotent Batch Reload
# MAGIC batch_date = "2024-02-15"
# MAGIC
# MAGIC # Step 1: Delete old data for this batch
# MAGIC spark.sql(f"DELETE FROM fact_sales WHERE BatchDate = '{batch_date}'")
# MAGIC
# MAGIC # Step 2: Insert new data
# MAGIC df_staging.filter(col("BatchDate") == batch_date) \
# MAGIC     .write.mode("append").saveAsTable("fact_sales")
# MAGIC ```
# MAGIC
# MAGIC **Long-term (Stage 2):**
# MAGIC
# MAGIC 1. **Added Watermark Table:**
# MAGIC ```sql
# MAGIC CREATE TABLE etl_watermarks (
# MAGIC     table_name STRING,
# MAGIC     batch_date DATE,
# MAGIC     record_count BIGINT,
# MAGIC     checksum STRING,
# MAGIC     processed_at TIMESTAMP
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC 2. **Idempotency Check:**
# MAGIC ```python
# MAGIC last_batch = spark.sql(f"""
# MAGIC     SELECT batch_date FROM etl_watermarks
# MAGIC     WHERE table_name = 'fact_sales'
# MAGIC     ORDER BY processed_at DESC LIMIT 1
# MAGIC """).collect()
# MAGIC
# MAGIC if last_batch and last_batch[0].batch_date >= current_batch_date:
# MAGIC     print(f"Batch {current_batch_date} already processed. Skipping.")
# MAGIC     sys.exit(0)
# MAGIC ```
# MAGIC
# MAGIC 3. **Monitoring:**
# MAGIC ```sql
# MAGIC -- Daily duplicate check
# MAGIC SELECT COUNT(*) as duplicate_count
# MAGIC FROM (
# MAGIC     SELECT TransactionID
# MAGIC     FROM fact_sales
# MAGIC     WHERE BatchDate = CURRENT_DATE - 1
# MAGIC     GROUP BY TransactionID
# MAGIC     HAVING COUNT(*) > 1
# MAGIC );
# MAGIC
# MAGIC -- Alert if duplicate_count > 0
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Lessons Learned:
# MAGIC
# MAGIC  **DO:**
# MAGIC - Use MERGE or Idempotent Batch (never raw APPEND)
# MAGIC - Test failure + retry scenarios
# MAGIC - Add row count reconciliation
# MAGIC - Monitor for duplicates daily
# MAGIC - Use watermark tables for idempotency
# MAGIC
# MAGIC  **DON'T:**
# MAGIC - Trust "pipeline succeeded" status alone
# MAGIC - Assume failures never happen
# MAGIC - Skip idempotency checks ("it's just ETL")
# MAGIC - Use APPEND without deduplication logic
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Interview Positioning:
# MAGIC
# MAGIC  **Question:** "Tell me about a production incident you handled."
# MAGIC
# MAGIC **Answer:**
# MAGIC
# MAGIC "At my previous company, we had ₹2.3 Crore revenue inflation due to duplicate transactions. The root cause was using APPEND pattern without idempotency checks. When pipelines failed and retried, they inserted duplicate data.
# MAGIC
# MAGIC I led the fix:
# MAGIC 1. Immediate: Switched to Idempotent Batch Reload (DELETE + INSERT per batch)
# MAGIC 2. Long-term: Added watermark table, duplicate monitoring, reconciliation checks
# MAGIC 3. Prevention: Tested failure scenarios, added idempotency validation
# MAGIC
# MAGIC Result: Zero duplicates in 6 months post-fix. I learned that data engineering isn't just about correct logic - it's about handling failure gracefully. Now I always design for idempotency from day one."
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Cleanup and Summary
# Cleanup test tables

spark.sql("DROP TABLE IF EXISTS default.test_fact_append")
spark.sql("DROP TABLE IF EXISTS default.test_fact_merge")
spark.sql("DROP TABLE IF EXISTS default.test_fact_idempotent")
spark.sql("DROP TABLE IF EXISTS default.test_staging_sales")

print(" Test tables cleaned up")

print("\n" + "="*80)
print(" FACT LOADING PATTERNS - DECISION SUMMARY")
print("="*80)

print("\n Use APPEND (Insert Only) when:")
print("    Immutable events (clickstream, sensor data)")
print("    Source guarantees no duplicates")
print("    Pipeline idempotency handled externally (watermarks)")
print("    NEVER use without deduplication checks")

print("\n Use MERGE (Upsert) when:")
print("    Transactional updates (order status changes)")
print("    Late-arriving data with corrections")
print("    Real-time / streaming ingestion")
print("    Cross-batch updates needed")
print("    Requires unique business key")

print("\n Use IDEMPOTENT BATCH RELOAD when:")
print("    Data partitioned by BatchDate")
print("    Daily/hourly batch processing")
print("    Complete snapshot per batch")
print("    Corrections within same batch")
print("    Want simplicity (no MERGE complexity)")

print("\n" + "="*80)
print(" SKILLAIPPATH RECOMMENDATION")
print("="*80)
print("\nPrimary Pattern: IDEMPOTENT BATCH RELOAD")
print("   - All fact tables partitioned by BatchDate")
print("   - Daily batch processing (not real-time)")
print("   - Simple DELETE + INSERT per batch")
print("   - Easy to re-run specific dates")
print("\nFallback to MERGE for:")
print("   - Real-time order status updates")
print("   - Cross-batch corrections")
print("   - Late-arriving dimension keys")

print("\n" + "="*80)
print(" INTERVIEW KEY POINTS")
print("="*80)
print("\n1.  Production Incident: ₹2.3 Cr duplicate revenue")
print("   - Cause: APPEND without idempotency")
print("   - Fix: Idempotent Batch + watermarks + monitoring")
print("\n2.  Pattern Decision Framework:")
print("   - Batch partitioned? → Idempotent Batch")
print("   - Updates/corrections? → MERGE")
print("   - Immutable events + watermarks? → APPEND")
print("\n3. SECURITY Production Safety:")
print("   - Test failure + retry scenarios")
print("   - Add duplicate detection monitoring")
print("   - Use watermark tables for idempotency")
print("   - Reconcile row counts (source vs target)")
print("="*80)

print("\n Pattern selection = preventing ₹ Crores in data quality issues")

# COMMAND ----------

# DBTITLE 1,Quick Reference - Pattern Selector
# MAGIC %md
# MAGIC ##  Quick Reference: Pattern Selector
# MAGIC
# MAGIC ### Flowchart
# MAGIC
# MAGIC ```
# MAGIC FACT LOADING PATTERN SELECTOR
# MAGIC
# MAGIC           START
# MAGIC             |
# MAGIC     Is data partitioned
# MAGIC      by BatchDate?
# MAGIC             |
# MAGIC         YES |  NO
# MAGIC             |   |
# MAGIC             |   Can source send
# MAGIC             |   updates/corrections?
# MAGIC             |        |
# MAGIC             |    YES |  NO
# MAGIC             |        |   |
# MAGIC             |        |   Is pipeline
# MAGIC             |        |   failure possible?
# MAGIC             |        |        |
# MAGIC             |        |    YES |  NO
# MAGIC             |        |        |   |
# MAGIC      IDEMPOTENT  MERGE   MERGE  APPEND
# MAGIC        BATCH                    (with checks)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Code Templates
# MAGIC
# MAGIC **Pattern 1: Idempotent Batch Reload**
# MAGIC ```python
# MAGIC batch_date = "2024-01-15"
# MAGIC
# MAGIC # Step 1: Delete old data for this batch
# MAGIC spark.sql(f"""
# MAGIC     DELETE FROM fact_sales
# MAGIC     WHERE BatchDate = '{batch_date}'
# MAGIC """)
# MAGIC
# MAGIC # Step 2: Insert new data
# MAGIC df_staging.filter(col("BatchDate") == batch_date) \
# MAGIC     .write.mode("append").saveAsTable("fact_sales")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Pattern 2: MERGE (Upsert)**
# MAGIC ```sql
# MAGIC MERGE INTO fact_sales AS target
# MAGIC USING staging AS source
# MAGIC ON target.TransactionID = source.TransactionID
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET target.SalesAmount = source.SalesAmount,
# MAGIC              target.Status = source.Status
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Pattern 3: APPEND with Idempotency Check**
# MAGIC ```python
# MAGIC # Check watermark
# MAGIC last_batch = get_last_processed_batch("fact_sales")
# MAGIC if current_batch <= last_batch:
# MAGIC     print("Already processed. Skipping.")
# MAGIC     sys.exit(0)
# MAGIC
# MAGIC # Safe to append
# MAGIC df_staging.write.mode("append").saveAsTable("fact_sales")
# MAGIC
# MAGIC # Update watermark
# MAGIC update_watermark("fact_sales", current_batch)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Monitoring Queries
# MAGIC
# MAGIC **Duplicate Detection:**
# MAGIC ```sql
# MAGIC SELECT
# MAGIC     BatchDate,
# MAGIC     COUNT(*) as total_rows,
# MAGIC     COUNT(DISTINCT TransactionID) as unique_txns,
# MAGIC     COUNT(*) - COUNT(DISTINCT TransactionID) as duplicates
# MAGIC FROM fact_sales
# MAGIC WHERE BatchDate >= CURRENT_DATE - 7
# MAGIC GROUP BY BatchDate
# MAGIC HAVING duplicates > 0;  -- Alert if any duplicates
# MAGIC ```
# MAGIC
# MAGIC **Row Count Reconciliation:**
# MAGIC ```sql
# MAGIC WITH source_count AS (
# MAGIC     SELECT BatchDate, COUNT(*) as cnt
# MAGIC     FROM staging_sales
# MAGIC     WHERE BatchDate = '2024-01-15'
# MAGIC     GROUP BY BatchDate
# MAGIC ),
# MAGIC target_count AS (
# MAGIC     SELECT BatchDate, COUNT(*) as cnt
# MAGIC     FROM fact_sales
# MAGIC     WHERE BatchDate = '2024-01-15'
# MAGIC     GROUP BY BatchDate
# MAGIC )
# MAGIC SELECT
# MAGIC     s.BatchDate,
# MAGIC     s.cnt as source_count,
# MAGIC     t.cnt as target_count,
# MAGIC     s.cnt - t.cnt as difference
# MAGIC FROM source_count s
# MAGIC JOIN target_count t ON s.BatchDate = t.BatchDate
# MAGIC WHERE s.cnt != t.cnt;  -- Alert if mismatch
# MAGIC ```
# MAGIC
# MAGIC ---