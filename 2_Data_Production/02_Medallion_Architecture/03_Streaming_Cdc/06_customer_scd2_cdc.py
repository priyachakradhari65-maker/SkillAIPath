# Databricks notebook source
# DBTITLE 1,Load Config
# MAGIC %run ../../../1_Canvas/1_Setup/1_Config

# COMMAND ----------

# DBTITLE 1,CDC Pattern - Customer SCD Type 2
# MAGIC %md
# MAGIC # Production CDC Pipeline: Customer SCD Type 2
# MAGIC **SkillAIPath Data Warehouse - Incremental Change Processing**
# MAGIC
# MAGIC ## THE SITUATION: "Customer Moved Cities - History is Lost"
# MAGIC
# MAGIC **Monday 9 AM - Data Quality Alert**
# MAGIC
# MAGIC **Analyst:** "Customer Ramesh Sharma shows City='Chennai' for all historical orders, but he actually moved from Mumbai to Chennai in March 2024. His Q1 sales are attributed to the wrong region!"
# MAGIC
# MAGIC **Data Engineer:** "Our daily full-reload overwrites everything. We need CDC (Change Data Capture) with SCD Type 2 to track history."
# MAGIC
# MAGIC **CEO:** "If we can't trust regional sales trends, we can't make market expansion decisions."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## CDC Architecture
# MAGIC
# MAGIC **Bronze Layer (Source):**
# MAGIC ```
# MAGIC sap_bronze_cdc_customers
# MAGIC  CustomerID (business key)
# MAGIC  CustomerName
# MAGIC  City
# MAGIC  ChangeType (INSERT/UPDATE/DELETE)
# MAGIC  BatchDate (CDC partition)
# MAGIC  SourceTimestamp (capture time)
# MAGIC ```
# MAGIC
# MAGIC **Silver Layer (Target):**
# MAGIC ```
# MAGIC sap_silver_dim_customer
# MAGIC  CustomerSK (surrogate key)
# MAGIC  CustomerID (business key)
# MAGIC  CustomerName
# MAGIC  City
# MAGIC  StartDate (effective from)
# MAGIC  EndDate (effective to, 9999-12-31 for current)
# MAGIC  IsCurrent (1=current, 0=historical)
# MAGIC  RecordHash (change detection)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Processing Logic
# MAGIC
# MAGIC **INSERT:** New customer arrives
# MAGIC → Generate new CustomerSK
# MAGIC → Insert with StartDate=BatchDate, IsCurrent=1
# MAGIC
# MAGIC **UPDATE:** Customer city changes
# MAGIC → Close old record (EndDate=BatchDate-1, IsCurrent=0)
# MAGIC → Insert new record (new CustomerSK, StartDate=BatchDate, IsCurrent=1)
# MAGIC
# MAGIC **DELETE:** Customer deleted (GDPR right to be forgotten)
# MAGIC → Close record (EndDate=BatchDate, IsCurrent=0)
# MAGIC → Flag as deleted (optional IsDeleted column)
# MAGIC
# MAGIC **NO-CHANGE:** Customer exists but no field changes
# MAGIC → Skip processing (idempotent)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Dependencies
# MAGIC
# MAGIC **Prerequisite Notebooks:**
# MAGIC 1. [config](#file-config.py) - Centralized table names
# MAGIC 2. [watermark_manager](#notebook-1521555442239544) - Track processed CDC batches
# MAGIC 3. [cdc_processor_utility](#notebook-1521555442239543) - Generic CDC functions
# MAGIC
# MAGIC **Builds Incrementally On:**
# MAGIC * 00_UTILS/generate_batch - Creates source data with BatchDate
# MAGIC * 04_CDC/cdc_source_generator - Simulates CDC feed (INSERT/UPDATE/DELETE events)
# MAGIC * 04_CDC/watermark_manager - Prevents duplicate processing
# MAGIC
# MAGIC  **Interview Value:** "I built an end-to-end CDC pipeline for SCD Type 2 dimensions that processes 5K daily changes in 2 minutes, maintains complete audit history, and integrates with watermark-based idempotency controls."

# COMMAND ----------

# DBTITLE 1,Import & Read CDC Feed
# Import dependencies
from datetime import datetime, timedelta
from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring, md5
from pyspark.sql import Window
import hashlib

# Parameters - FIX: Use today's date since CDC data is from past 30 days (Jun 8 - Jul 8, 2026)
from datetime import date
today = date.today().strftime("%Y-%m-%d")
dbutils.widgets.text("process_date", today, "Process Date (filter CDC up to this date)")
process_date = dbutils.widgets.get("process_date")

print("="*70)
print("CDC PIPELINE - CUSTOMER SCD TYPE 2")
print("="*70)
print(f"Process Date: {process_date}")
print(f"Source: {Config.TBL_BRONZE_CUSTOMER_CDC}")
print(f"Target: {Config.TBL_DIM_CUSTOMER}")
print(f"Watermark Table: {Config.TBL_CDC_WATERMARKS}")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Idempotency Check - Watermark Integration
# Check if batch already processed (integrate with watermark_manager)
try:
    # Get last processed batch from watermark table
    last_batch = spark.sql(f"""
        SELECT last_processed_value
        FROM {Config.TBL_CDC_WATERMARKS}
        WHERE table_name = 'customer_changes_raw'
        ORDER BY updated_at DESC
        LIMIT 1
    """).collect()

    if last_batch and last_batch[0]['last_processed_value'] >= process_date:
        print(f"Process date {process_date} already processed")
        print(f"   Last processed: {last_batch[0]['last_processed_value']}")
        print("   Skipping duplicate processing (idempotency)")

        summary = {"status": "skipped", "reason": "already_processed", "process_date": process_date}
        dbutils.notebook.exit(str(summary))
except Exception as e:
    print(f"Watermark table not found: {str(e)}")
    print("   First run - proceeding without idempotency check")

print("Idempotency check passed - proceeding")

# COMMAND ----------

# DBTITLE 1,Read CDC Feed - Filter by Batch
# Read CDC feed - FIX: Use ChangeTimestamp and Operation columns
cdc_table = Config.TBL_BRONZE_CUSTOMER_CDC

try:
    # Filter CDC events up to process_date (ChangeTimestamp is string format 'YYYY-MM-DD HH:MM:SS')
    df_cdc = spark.table(cdc_table).filter(to_date(col('ChangeTimestamp')) <= process_date)

    # >>> GUARD (Phase P1, 2026-11-03): forward-only feed filter <<<
    # Jobs 07/24 generators append customer_changes_raw rows for BatchDates that
    # Job 05 already processed (watermark 'customer_changes_raw'). Without this
    # guard those stale rows are re-applied: every UPDATE row closes+reopens a
    # version (fake SCD2 changes from regenerated random data). Only process events
    # strictly NEWER than the last processed batch.
    try:
        _wm = spark.sql(f"""
            SELECT last_processed_value FROM {Config.TBL_CDC_WATERMARKS}
            WHERE table_name = 'customer_changes_raw' ORDER BY updated_at DESC LIMIT 1
        """).collect()
        last_cdc_batch = _wm[0]['last_processed_value'] if _wm else None
    except Exception:
        last_cdc_batch = None
    if last_cdc_batch:
        _stale = spark.table(cdc_table).filter(
            (to_date(col('ChangeTimestamp')) <= process_date) &
            (to_date(col('ChangeTimestamp')) <= to_date(lit(str(last_cdc_batch))))
        ).count()
        df_cdc = df_cdc.filter(to_date(col('ChangeTimestamp')) > to_date(lit(str(last_cdc_batch))))
        if _stale > 0:
            print(f"   SHIELD: skipped {_stale:,} stale CDC events (ChangeTimestamp <= last processed batch {last_cdc_batch}) — regenerated duplicates are NOT re-applied to dim_customer")

    cdc_count = df_cdc.count()

    if cdc_count == 0:
        print(f"No CDC records up to {process_date}")
        print("   CDC data may not exist or hasn't been generated yet")

        summary = {"status": "skipped", "reason": "no_data", "process_date": process_date}
        dbutils.notebook.exit(str(summary))

    print(f"CDC Feed Loaded: {cdc_count:,} changes up to {process_date}")

    # Breakdown by operation type (INSERT/UPDATE/DELETE)
    change_breakdown = df_cdc.groupBy('Operation').count().collect()
    for row in change_breakdown:
        print(f"   {row['Operation']}: {row['count']:,} records")

    display(df_cdc.limit(5))

except Exception as e:
    print(f"CDC Feed Read Failed: {str(e)}")
    print(f"   Table: {cdc_table}")
    print("   Check if CDC source table exists")
    dbutils.notebook.exit(str({"status": "failed", "error": str(e)}))

# COMMAND ----------

# DBTITLE 1,Pre-Processing - Add Record Hash
# Add MD5 hash for change detection (compare current vs new record)
def compute_hash(*cols):
    """Generate MD5 hash from column values for change detection"""
    return md5(concat_ws('|', *[coalesce(c.cast('string'), lit('')) for c in cols]))

# Keep CDC column names, just add RecordHash
df_cdc_hashed = df_cdc.withColumn(
    "RecordHash",
    compute_hash(
        col("CustomerName"), 
        col("CustomerAddress"),
        col("CustomerContact"),
        col("CustomerMail"),
        col("CustomerAge"),
        col("CustomerGender")
    )
)

print("✅ Record hashing complete (detects actual data changes)")
print(f"   CDC columns: {df_cdc_hashed.columns}")

# COMMAND ----------

# DBTITLE 1,Process INSERT - New Customers
# Handle INSERTs: New customers arriving - FIX: Use 'Operation' column
from pyspark.sql.functions import current_timestamp

df_inserts = df_cdc_hashed.filter(col('Operation') == 'INSERT')

# >>> GUARD (Phase P1 / Phase A2 fix, 2026-11-03): INSERT-path existence check <<<
# A CDC INSERT for a CustomerID already present in dim_customer (any version) must
# NOT create a duplicate customer — the Nov 2 4AM run created 5 duplicate-current
# pairs (SKs 7019/20, 7040/41, 7043/44, 7055/56, 7143/44) exactly this way. Known
# IDs are dropped here; genuine attribute changes for known customers are handled
# by the UPDATE path instead.
_known_ids = [str(r['CustomerID']) for r in spark.sql(
    f"SELECT DISTINCT CustomerID FROM {Config.TBL_DIM_CUSTOMER}"
).collect()]
if _known_ids:
    df_inserts = df_inserts.filter(~col("CustomerID").cast("string").isin(_known_ids))

insert_count = df_inserts.count()

if insert_count > 0:
    print(f" Processing {insert_count:,} INSERTs...")

    # Get max surrogate key
    max_sk = spark.sql(f"""
        SELECT COALESCE(MAX(CustomerSK), 0) as max_sk
        FROM {Config.TBL_DIM_CUSTOMER}
    """).collect()[0]['max_sk']

    # Generate new surrogate keys - column names now match directly
    df_new_customers = df_inserts \
        .withColumn("CustomerSK", row_number().over(Window.orderBy("CustomerID")) + max_sk) \
        .withColumn("StartDate", lit(process_date).cast("date")) \
        .withColumn("EndDate", lit("9999-12-31").cast("date")) \
        .withColumn("IsCurrent", lit(1)) \
        .withColumn("LoadedAt", current_timestamp()) \
        .select(
            col("CustomerSK"),
            col("CustomerID"),
            col("CustomerName"),
            col("CustomerMail"),
            col("CustomerAge"),
            col("CustomerGender"),
            col("CustomerContact"),
            col("CustomerAddress"),
            col("StartDate"),
            col("EndDate"),
            col("IsCurrent"),
            col("LoadedAt")
        )

    df_new_customers.write.mode("append").format("delta").saveAsTable(Config.TBL_DIM_CUSTOMER)

    print(f" Inserted {insert_count:,} new customers")
    print(f"   Surrogate keys: {max_sk + 1} to {max_sk + insert_count}")
else:
    print("⏩ No INSERTs in this batch")

# COMMAND ----------

# DBTITLE 1,Process UPDATE - Track History (SCD Type 2)
# Handle UPDATEs: Customer changes - FIX: Use 'Operation' column
df_updates = df_cdc_hashed.filter(col('Operation') == 'UPDATE')

# >>> GUARD (Phase P1, 2026-11-03): one change per customer per batch <<<
# Multiple UPDATE events for the same CustomerID in one batch (regenerated overlap
# feeds) would insert multiple new current versions. Keep the latest event only.
df_updates = df_updates.withColumn(
    "_rn", row_number().over(Window.partitionBy("CustomerID").orderBy(col("ChangeTimestamp").desc()))
).filter(col("_rn") == 1).drop("_rn")

update_count = df_updates.count()

if update_count > 0:
    print(f" Processing {update_count:,} UPDATEs...")

    # Read current dimension records
    df_current = spark.table(Config.TBL_DIM_CUSTOMER).filter(col(Config.SCD_IS_CURRENT_COL) == 1)

    # Join to find records to update - select dim keys + CDC data directly (no double aliasing)
    # >>> GUARD (Phase P1 / Phase A2 fix, 2026-11-03): attribute-equality filter <<<
    # RecordHash was computed (previous cell) but never compared (dead code) — every
    # UPDATE event closed+reopened a version even when attributes were IDENTICAL,
    # inflating SCD2 history on reprocessed/regenerated feed rows. Only process rows
    # whose attributes genuinely differ from the current dim version.
    df_dim_hashed = df_current.withColumn(
        "DimRecordHash",
        compute_hash(
            col("CustomerName"),
            col("CustomerAddress"),
            col("CustomerContact"),
            col("CustomerMail"),
            col("CustomerAge"),
            col("CustomerGender")
        )
    )
    df_changed = df_updates.alias("cdc") \
        .join(
            df_dim_hashed.alias("dim"),
            col("cdc.CustomerID") == col("dim.CustomerID"),
            "inner"
        ) \
        .filter(col("cdc.RecordHash") != col("dim.DimRecordHash")) \
        .select(
            col("dim.CustomerSK"),
            col("cdc.CustomerID"),
            col("cdc.CustomerName"),
            col("cdc.CustomerAddress"),
            col("cdc.CustomerContact"),
            col("cdc.CustomerMail"),
            col("cdc.CustomerAge"),
            col("cdc.CustomerGender")
        )

    # >>> FIX (Phase A, 2026-11-02): materialize df_changed BEFORE mutating the dim table <<<
    # df_changed is LAZY. Step 1's UPDATE closes the joined IsCurrent=1 rows, so an
    # unmaterialized df_changed re-evaluates against the mutated table in Step 2 and
    # matches 0 rows — new versions are never inserted (Nov 2 run: 194 closed, 0 inserted).
    # Regression-validated 24/25 on isolated DEEP CLONE (PhaseA_06cdc_SCD2_LazyFix_Regression).
    df_changed = df_changed.cache()
    try:
        actual_changes = df_changed.count()   # ACTION -> cache materialized

        if actual_changes > 0:
            # Step 1: Close old records (set EndDate, IsCurrent=0)
            yesterday = (datetime.strptime(process_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")

            customer_ids_to_close = [str(row['CustomerID']) for row in df_changed.select('CustomerID').distinct().collect()]

            spark.sql(f"""
                UPDATE {Config.TBL_DIM_CUSTOMER}
                SET EndDate = '{yesterday}', IsCurrent = 0
                WHERE CustomerID IN ('{"', '".join(customer_ids_to_close)}')
                  AND IsCurrent = 1
            """)

            print(f" Closed {actual_changes:,} old records (EndDate={yesterday})")

            # Step 2: Insert new records with updated values
            # (reads the CACHED pre-UPDATE df_changed — no lazy re-evaluation)
            max_sk = spark.sql(f"SELECT MAX(CustomerSK) as max_sk FROM {Config.TBL_DIM_CUSTOMER}").collect()[0]['max_sk']

            df_new_versions = df_changed \
                .withColumn("CustomerSK", row_number().over(Window.orderBy("CustomerID")) + max_sk) \
                .withColumn("StartDate", lit(process_date).cast("date")) \
                .withColumn("EndDate", lit("9999-12-31").cast("date")) \
                .withColumn("IsCurrent", lit(1)) \
                .withColumn("LoadedAt", current_timestamp()) \
                .select(
                    col("CustomerSK"),
                    col("CustomerID"),
                    col("CustomerName"),
                    col("CustomerMail"),
                    col("CustomerAge"),
                    col("CustomerGender"),
                    col("CustomerContact"),
                    col("CustomerAddress"),
                    col("StartDate"),
                    col("EndDate"),
                    col("IsCurrent"),
                    col("LoadedAt")
                )

            df_new_versions.write.mode("append").format("delta").saveAsTable(Config.TBL_DIM_CUSTOMER)

            print(f" Inserted {actual_changes:,} new versions")
            print(f" SCD Type 2: History preserved with StartDate={process_date}")
        else:
            print("⏩ No actual data changes (hash match - false positives)")
    finally:
        # Release the frozen snapshot even if Step 1/2 raise
        df_changed.unpersist()
else:
    print("⏩ No UPDATEs in this batch")

# COMMAND ----------

# DBTITLE 1,Process DELETE - Soft Delete
# Handle DELETEs: Customer removed (GDPR, account closure) - FIX: Use 'Operation' column
df_deletes = df_cdc_hashed.filter(col('Operation') == 'DELETE')
delete_count = df_deletes.count()

if delete_count > 0:
    print(f" Processing {delete_count:,} DELETEs...")

    customer_ids_to_delete = [str(row['CustomerID']) for row in df_deletes.select('CustomerID').distinct().collect()]

    # Soft delete: Close record with EndDate = process_date
    spark.sql(f"""
        UPDATE {Config.TBL_DIM_CUSTOMER}
        SET EndDate = '{process_date}', IsCurrent = 0
        WHERE CustomerID IN ('{"', '".join(customer_ids_to_delete)}')
          AND IsCurrent = 1
    """)

    print(f" Soft-deleted {delete_count:,} customers (EndDate={process_date})")
    print(" Audit trail preserved - records not physically deleted")
else:
    print("⏩ No DELETEs in this batch")

# COMMAND ----------

# DBTITLE 1,Update Watermark - Atomic Commit
# Update watermark AFTER successful processing
try:
    total_processed = insert_count + actual_changes + delete_count

    spark.sql(f"""
        MERGE INTO {Config.TBL_CDC_WATERMARKS} AS target
        USING (SELECT
            'customer_changes_raw' as table_name,
            '{process_date}' as last_processed_value,
            current_timestamp() as updated_at,
            current_user() as updated_by,
            {total_processed} as rows_processed
        ) AS source
        ON target.table_name = source.table_name
        WHEN MATCHED THEN UPDATE SET
            target.last_processed_value = source.last_processed_value,
            target.updated_at = source.updated_at,
            target.updated_by = source.updated_by,
            target.rows_processed = target.rows_processed + source.rows_processed
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"✅ Watermark updated: {process_date}")
    print("Idempotency: Next run will skip this batch")
except Exception as e:
    print(f"Watermark update failed: {str(e)}")
    print("   Processing succeeded, but idempotency not guaranteed")

# COMMAND ----------

# DBTITLE 1,Validation 1 - Check SCD Type 2 History
# MAGIC %sql
# MAGIC -- Verify SCD Type 2 history for customers with changes
# MAGIC SELECT
# MAGIC     CustomerID,
# MAGIC     CustomerSK,
# MAGIC     CustomerName,
# MAGIC     CustomerAddress,
# MAGIC     StartDate,
# MAGIC     EndDate,
# MAGIC     IsCurrent
# MAGIC FROM sap_dev.silver.dim_customer
# MAGIC WHERE CustomerID IN (
# MAGIC     SELECT DISTINCT CustomerID
# MAGIC     FROM sap_dev.bronze.customer_changes_raw
# MAGIC     WHERE Operation = 'UPDATE'
# MAGIC     LIMIT 5
# MAGIC )
# MAGIC ORDER BY CustomerID, StartDate;
# MAGIC
# MAGIC --  Interview: Shows complete audit trail for customer changes

# COMMAND ----------

# DBTITLE 1,Validation 2 - IsCurrent Integrity Check
# MAGIC %sql
# MAGIC -- Ensure only ONE current record per customer
# MAGIC SELECT
# MAGIC     CustomerID,
# MAGIC     COUNT(*) as current_count
# MAGIC FROM sap_dev.silver.dim_customer
# MAGIC WHERE IsCurrent = 1
# MAGIC GROUP BY CustomerID
# MAGIC HAVING COUNT(*) > 1;
# MAGIC
# MAGIC -- Expected: 0 rows (each customer has exactly 1 current record)
# MAGIC --  Interview: "I validate SCD Type 2 integrity with this query in production"

# COMMAND ----------

# DBTITLE 1,Validation 3 - Orphan Detection
# MAGIC %sql
# MAGIC -- Find facts pointing to non-existent customers
# MAGIC SELECT COUNT(*) as orphan_facts
# MAGIC FROM sap_dev.silver.fact_sales
# MAGIC WHERE CustomerSK NOT IN (SELECT CustomerSK FROM sap_dev.silver.dim_customer);
# MAGIC
# MAGIC -- Expected: 0 (all facts have valid dimension keys)
# MAGIC --  CDC must not break referential integrity

# COMMAND ----------

# DBTITLE 1,Final Summary & Exit
# Final summary
end_time = datetime.now()

summary = {
    "status": "success",
    "process_date": process_date,
    "inserts": insert_count,
    "updates": actual_changes if update_count > 0 else 0,
    "deletes": delete_count,
    "total_processed": insert_count + (actual_changes if update_count > 0 else 0) + delete_count,
    "completed_at": end_time.strftime("%Y-%m-%d %H:%M:%S")
}

print("\n" + "="*60)
print(" CDC PROCESSING COMPLETE")
print("="*60)
for key, value in summary.items():
    print(f"   {key}: {value}")
print("="*60)

print("\n Interview Talking Points:")
print("   1. SCD Type 2 maintains complete audit history")
print("   2. Watermark-based idempotency prevents duplicates")
print("   3. Record hashing detects actual changes (not just UPDATE flags)")
print("   4. Soft deletes preserve audit trail for compliance")
print("   5. Validation queries ensure referential integrity")

dbutils.notebook.exit(str(summary))

# COMMAND ----------

# DBTITLE 1,Detect Changes & Apply
# MAGIC %sql
# MAGIC -- ALTERNATIVE: MERGE logic to apply CDC changes
# MAGIC -- ⚠️ DO NOT RUN after cells 7-9! Use EITHER Python (cells 7-9) OR SQL MERGE (this cell), never both.
# MAGIC -- NOTE: If you see low/incorrect numbers here, it's because cells 7-9 already processed the changes.
# MAGIC -- This cell shows SQL MERGE approach as a reference alternative only.
# MAGIC
# MAGIC MERGE INTO sap_dev.silver.dim_customer target
# MAGIC USING (
# MAGIC   SELECT 
# MAGIC     CustomerID, 
# MAGIC     CustomerName, 
# MAGIC     CustomerAddress,
# MAGIC     CustomerContact,
# MAGIC     CustomerMail,
# MAGIC     CustomerAge,
# MAGIC     CustomerGender,
# MAGIC     Operation
# MAGIC   FROM sap_dev.bronze.customer_changes_raw
# MAGIC   WHERE to_date(ChangeTimestamp) <= '${process_date}'
# MAGIC ) source
# MAGIC ON target.CustomerID = source.CustomerID AND target.IsCurrent = 1
# MAGIC WHEN MATCHED AND source.Operation = 'UPDATE' THEN
# MAGIC   UPDATE SET IsCurrent = 0, EndDate = CURRENT_DATE()
# MAGIC WHEN NOT MATCHED AND source.Operation = 'INSERT' THEN
# MAGIC   INSERT (CustomerID, CustomerName, CustomerAddress, CustomerContact, CustomerMail, CustomerAge, CustomerGender, StartDate, EndDate, IsCurrent)
# MAGIC   VALUES (source.CustomerID, source.CustomerName, source.CustomerAddress, source.CustomerContact, source.CustomerMail, source.CustomerAge, source.CustomerGender, CURRENT_DATE(), '9999-12-31', 1);