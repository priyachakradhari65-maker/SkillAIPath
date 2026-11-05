# Databricks notebook source
# DBTITLE 1,Sales Fact CDC - Incremental MERGE Pattern
# MAGIC %md
# MAGIC # Sales Fact CDC - Incremental MERGE
# MAGIC **SkillAIPath Real-Time Transaction Processing**
# MAGIC
# MAGIC ## ⚠️ CRITICAL PREREQUISITES (Run These FIRST)
# MAGIC
# MAGIC **MUST be loaded before running this notebook:**
# MAGIC 1. ✅ All dimension tables (dim_customer, dim_product, dim_warehouse, dim_retailer, dim_agent, dim_payment_mode, dim_product_category, dim_date)
# MAGIC 2. ✅ Each dimension must have -1 "Unknown" record for orphan handling
# MAGIC 3. ✅ dim_date must cover date range in source data (check min/max dates)
# MAGIC 4. ✅ Quarantine table must exist: fact_sales_quarantine
# MAGIC
# MAGIC **What happens if prerequisites are missing:**
# MAGIC - Missing dimensions → Notebook exits with clear error
# MAGIC - Missing -1 records → Orphan records get -1 assigned (safe fallback)
# MAGIC - dim_date gaps → Records with out-of-range dates quarantined
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC ```
# MAGIC Streaming Bronze (sales_raw)
# MAGIC          ↓
# MAGIC     Read Changes (watermark-based)
# MAGIC          ↓
# MAGIC     Dimension Health Check (NEW)
# MAGIC          ↓
# MAGIC     Transform & Validate
# MAGIC          ↓
# MAGIC     Quarantine Rejected (NEW)
# MAGIC          ↓
# MAGIC MERGE into Silver (fact_sales)
# MAGIC          ↓
# MAGIC     Update Watermark (validated only)
# MAGIC ```
# MAGIC
# MAGIC ## Business Requirements
# MAGIC
# MAGIC **Late Arrivals:** Yesterday's sales may arrive today (system delays, offline POS)
# MAGIC **Price Corrections:** Retroactive discounts require UPDATE on existing transaction_id
# MAGIC **Refunds:** Same-day refunds update original transaction
# MAGIC **Duplicates:** Multiple systems may send same transaction_id
# MAGIC **Orphan Records:** New dimension values get -1 (Unknown) until dimension is updated
# MAGIC
# MAGIC ## Solution: Idempotent MERGE with Orphan Handling
# MAGIC
# MAGIC **Key:** OrderID (business key for deduplication)
# MAGIC **Logic:**
# MAGIC * MATCHED: Update amounts, dates, updated_at
# MAGIC * NOT MATCHED: Insert new transaction
# MAGIC * NULL dimension keys → COALESCE to -1 (Unknown)
# MAGIC * Validation failures → Quarantine for investigation
# MAGIC
# MAGIC **Result:** Re-running this notebook never creates duplicates, no data loss

# COMMAND ----------

# DBTITLE 1,How to Create -1 Unknown Records
# MAGIC %md
# MAGIC ## 🛠️ How to Create -1 "Unknown" Records in All Dimensions
# MAGIC
# MAGIC **Run this ONCE per dimension before first CDC run:**
# MAGIC
# MAGIC ```sql
# MAGIC -- For dim_customer
# MAGIC INSERT INTO sap_dev.silver.dim_customer VALUES (
# MAGIC   -1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 
# MAGIC   '1900-01-01', '9999-12-31', 1, current_timestamp(), current_timestamp(), 
# MAGIC   current_user(), current_user(), current_date()
# MAGIC );
# MAGIC
# MAGIC -- For dim_product  
# MAGIC INSERT INTO sap_dev.silver.dim_product VALUES (
# MAGIC   -1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 
# MAGIC   current_timestamp(), current_timestamp(), current_user(), current_user(), current_date()
# MAGIC );
# MAGIC
# MAGIC -- Repeat for: dim_warehouse, dim_retailer, dim_agent, dim_payment_mode, dim_product_category
# MAGIC -- For dim_date: Include -1 DateID with '1900-01-01' date
# MAGIC ```
# MAGIC
# MAGIC **Purpose:** When a new dimension value appears in sales data (e.g., new retailer), the fact record gets -1 instead of NULL, preventing data loss while signaling investigation needed.

# COMMAND ----------

# DBTITLE 1,configuration
# MAGIC %run ../../../1_Canvas/1_Setup/1_Config

# COMMAND ----------

# DBTITLE 1,Setup - Config & CDC Utilities
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

print("="*70)
print("Sales Fact CDC Processor")
print("="*70)
print(f"Source: Bronze sales data")
print(f"Target: Silver fact_sales table")
print(f"Watermark Table: {Config.TBL_CDC_WATERMARKS}")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Load Watermark Functions
# MAGIC %run ./09_watermark_manager

# COMMAND ----------

# DBTITLE 1,Load CDC Processor Functions
# MAGIC %run ./08_cdc_processor_utility

# COMMAND ----------

# DBTITLE 1,Get Last Processed Watermark
# Get last processed timestamp for sales
last_watermark = get_watermark(
    table_name='sales_raw',
    default_value='1970-01-01 00:00:00'
)

print(f"\nProcessing records after: {last_watermark}")

# COMMAND ----------

# Verify all dimension tables exist and are populated
print("\n" + "="*70)
print("DIMENSION HEALTH CHECK")
print("="*70)

dimensions_to_check = [
    ('dim_customer', 'CustomerSK'),
    ('dim_product', 'ProductSK'),
    ('dim_warehouse', 'WarehouseSK'),
    ('dim_retailer', 'RetailerSK'),
    ('dim_agent', 'AgentSK'),
    ('dim_payment_mode', 'PaymentModeSK'),
    ('dim_product_category', 'ProductCategorySK'),
    ('dim_date', 'DateID')
]

all_checks_passed = True
missing_tables = []
empty_tables = []

for table_name, key_col in dimensions_to_check:
    full_table_name = f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.{table_name}"
    try:
        df_check = spark.table(full_table_name)
        row_count = df_check.count()
        
        if row_count == 0:
            empty_tables.append(table_name)
            all_checks_passed = False
            print(f"  ❌ {table_name}: EXISTS but EMPTY (0 rows)")
        else:
            # Check if -1 unknown record exists
            unknown_count = df_check.filter(col(key_col) == -1).count()
            if unknown_count > 0:
                print(f"  ✅ {table_name}: {row_count:,} rows (includes -1 Unknown record)")
            else:
                print(f"  ⚠️  {table_name}: {row_count:,} rows (WARNING: No -1 Unknown record)")
    except Exception as e:
        missing_tables.append(table_name)
        all_checks_passed = False
        print(f"  ❌ {table_name}: DOES NOT EXIST - {str(e)}")

print("="*70)

if not all_checks_passed:
    error_msg = "DIMENSION PREREQUISITE FAILURE:\n"
    if missing_tables:
        error_msg += f"  Missing tables: {', '.join(missing_tables)}\n"
    if empty_tables:
        error_msg += f"  Empty tables: {', '.join(empty_tables)}\n"
    error_msg += "\n➡️  ACTION REQUIRED: Load dimension tables FIRST before running this CDC notebook"
    print(error_msg)
    dbutils.notebook.exit(f"FAILED: {error_msg}")
else:
    print("✅ All dimension health checks PASSED - safe to proceed")
    print("="*70)

# COMMAND ----------

# DBTITLE 1,Ensure Quarantine Table Exists
# Create quarantine table if it doesn't exist
quarantine_table = f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_sales_quarantine"

try:
    spark.sql(f"DESCRIBE TABLE {quarantine_table}")
    print(f"✅ Quarantine table exists: {quarantine_table}")
except Exception:
    print(f"⚙️  Creating quarantine table: {quarantine_table}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {quarantine_table} (
            OrderID BIGINT,
            CustomerSK INT,
            ProductSK INT,
            WarehouseSK INT,
            RetailerSK INT,
            AgentSK INT,
            PaymentModeSK INT,
            ProductCategorySK INT,
            OrderDateID INT,
            MFDDateID INT,
            EXPDateID INT,
            DeliveryDateID INT,
            ProductQuantity INT,
            CostPrice DOUBLE,
            sellingPrice DOUBLE,
            discount DOUBLE,
            CGST DOUBLE,
            SGST DOUBLE,
            finalprice DOUBLE,
            BatchDate DATE,
            LoadedAt TIMESTAMP,
            rejection_reason STRING,
            quarantine_timestamp TIMESTAMP,
            batch_id STRING
        )
        USING DELTA
        COMMENT 'Quarantine table for sales records that failed validation - enables investigation and recovery'
    """)
    print(f"✅ Quarantine table created successfully")

# COMMAND ----------

# DBTITLE 1,Read Incremental Changes from Bronze
# Read only new/changed records since last watermark
# Use stg_sales_incremental (daily delta landing zone) for efficient incremental processing
# >>> GUARD (Phase P1, 2026-11-03): forward-only batch filter <<<
# Jobs 07/24 generators re-write the SAME BatchDate staging rows (different random
# data) AFTER Job 05 already processed them. The LoadedAt watermark alone lets those
# stale rows through and they UPDATE-overwrite valid history in silver.fact_sales
# (proven Nov 3: Nov-2 fact rows were silently replaced by the job-07 regen).
# Only process batches strictly NEWER than the newest batch already in fact_sales.
max_fact_batch = spark.sql(
    f"SELECT COALESCE(MAX(BatchDate), DATE'1900-01-01') AS b FROM {Config.TBL_FACT_SALES}"
).collect()[0]['b']
stale_staging_rows = spark.sql(
    f"SELECT COUNT(*) c FROM {Config.CATALOG}.{Config.SCHEMA_BRONZE}.stg_sales_incremental WHERE BatchDate <= DATE'{max_fact_batch}'"
).collect()[0]['c']
if stale_staging_rows > 0:
    print(f"SHIELD: skipping {stale_staging_rows:,} staging rows for already-processed batches (BatchDate <= {max_fact_batch}) — regenerated duplicates are NOT merged over valid fact history")
df_sales_changes = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_BRONZE}.stg_sales_incremental").filter(
    (col('LoadedAt') > last_watermark) & (col('BatchDate') > max_fact_batch)
)

record_count = df_sales_changes.count()

if record_count == 0:
    print("No new records to process")
    dbutils.notebook.exit("SUCCESS: No changes detected")
else:
    print(f"Found {record_count:,} new/changed records to process")
    display(df_sales_changes.limit(5))

# COMMAND ----------

# DBTITLE 1,Transform and Validate
# Join with ALL dimension tables to lookup surrogate keys
# CRITICAL: Point-in-time SCD2 join for dim_customer to ensure historical accuracy
# This ensures each sale gets the customer attributes that were valid at OrderDate
dim_customer = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_customer")
dim_product = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_product")
dim_warehouse = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_warehouse")
dim_retailer = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_retailer")
dim_agent = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_agent")
dim_payment_mode = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_payment_mode")
dim_product_category = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_product_category")
dim_date = spark.table(f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_date")

# Get dim_date range for validation
date_range = dim_date.agg(min('Date').alias('min_date'), max('Date').alias('max_date')).first()
print(f"\ndim_date coverage: {date_range['min_date']} to {date_range['max_date']}")

# Perform all dimension lookups
# POINT-IN-TIME SCD2 JOIN: Match customer record that was active on OrderDate
# >>> Phase P1 fix (2026-11-03): join dim_customer by CustomerID, not CustomerName <<<
# The CDC-owned dim_customer (06_customer_scd2_cdc, Job 05) keys versions by
# CustomerID and legitimately contains duplicate CustomerNames — a name join
# fans out rows / mis-assigns SKs. Point-in-time window picks the version valid
# at OrderDate (IsCurrent rows accept any OrderDate >= StartDate, which also
# covers current rows whose EndDate predates the order, e.g. legacy dbt-built rows).
# NOTE (2026-11-05): comments must sit ABOVE the statement — placing this block
# between `df_sales_changes \` and `.join(` breaks the line continuation
# (IndentationError, Nov 4 12:00 UTC run failure).
df_with_dims = df_sales_changes \
    .join(
        dim_customer,
        (df_sales_changes.CustomerID == dim_customer.CustomerID) &
        (df_sales_changes.OrderDate >= dim_customer.StartDate) &
        ((dim_customer.IsCurrent == 1) | (df_sales_changes.OrderDate <= dim_customer.EndDate)),
        'left'
    ) \
    .join(
        dim_product,
        df_sales_changes.ProductName == dim_product.ProductName,
        'left'
    ) \
    .join(
        dim_warehouse,
        df_sales_changes.WareHouseName == dim_warehouse.WareHouseName,
        'left'
    ) \
    .join(
        dim_retailer,
        df_sales_changes.RetailerName == dim_retailer.RetailerName,
        'left'
    ) \
    .join(
        dim_agent,
        df_sales_changes.AgentName == dim_agent.AgentName,
        'left'
    ) \
    .join(
        dim_payment_mode,
        df_sales_changes.PaymentMode == dim_payment_mode.PaymentMode,
        'left'
    ) \
    .join(
        dim_product_category,
        df_sales_changes.ProductCategory == dim_product_category.ProductCategory,
        'left'
    ) \
    .join(
        dim_date.alias('dim_order_date'),
        df_sales_changes.OrderDate == col('dim_order_date.Date'),
        'left'
    ) \
    .join(
        dim_date.alias('dim_mfd_date'),
        df_sales_changes.MFDDate == col('dim_mfd_date.Date'),
        'left'
    ) \
    .join(
        dim_date.alias('dim_exp_date'),
        df_sales_changes.EXPDate == col('dim_exp_date.Date'),
        'left'
    ) \
    .join(
        dim_date.alias('dim_delivery_date'),
        df_sales_changes.DeliveryDate == col('dim_delivery_date.Date'),
        'left'
    )

# Select fact table columns with ALL dimension keys
# Use COALESCE to default NULL dimension lookups to -1 (Unknown)
# This prevents data loss when new dimension values arrive before dimension loads
df_fact = df_with_dims.select(
    df_sales_changes.OrderID,
    # >>> FIX (Phase P1b, 2026-11-05): expose CustomerID <<<
    # dbt's fact_sales incremental MERGE (schema autoMerge) added a CustomerID column
    # to sap_dev.silver.fact_sales on 2026-11-03 22:58 (delta v16 ADD COLUMNS). This
    # notebook's upsert_merge uses `WHEN NOT MATCHED THEN INSERT *`, which requires
    # the source to carry EVERY target column — without CustomerID the MERGE fails
    # with DELTA_MERGE_UNRESOLVED_EXPRESSION (Nov 5 09:40 repair-run failure).
    df_sales_changes.CustomerID,
    coalesce(col('CustomerSK'), lit(-1)).alias('CustomerSK'),
    coalesce(col('ProductSK'), lit(-1)).alias('ProductSK'),
    coalesce(col('WarehouseSK'), lit(-1)).alias('WarehouseSK'),
    coalesce(col('RetailerSK'), lit(-1)).alias('RetailerSK'),
    coalesce(col('AgentSK'), lit(-1)).alias('AgentSK'),
    coalesce(col('PaymentModeSK'), lit(-1)).alias('PaymentModeSK'),
    coalesce(col('ProductCategorySK'), lit(-1)).alias('ProductCategorySK'),
    coalesce(col('dim_order_date.DateID'), lit(-1)).alias('OrderDateID'),
    coalesce(col('dim_mfd_date.DateID'), lit(-1)).alias('MFDDateID'),
    coalesce(col('dim_exp_date.DateID'), lit(-1)).alias('EXPDateID'),
    coalesce(col('dim_delivery_date.DateID'), lit(-1)).alias('DeliveryDateID'),
    df_sales_changes.ProductQuantity,
    df_sales_changes.CostPrice,
    df_sales_changes.SellingPrice.alias('sellingPrice'),
    df_sales_changes.Discount.alias('discount'),
    df_sales_changes.CGST,
    df_sales_changes.SGST,
    df_sales_changes.finalPrice.alias('finalprice'),
    df_sales_changes.BatchDate,
    df_sales_changes.LoadedAt
)

# RELAXED validation - only critical business rules, NOT dimension key NULLs
# Dimension NULLs are already handled by COALESCE to -1 (Unknown)
# This prevents data loss while maintaining data quality
df_validated = df_fact.filter(
    (col('OrderID').isNotNull()) &  # Business key required
    (col('finalprice') > 0) &       # Financial integrity
    (col('OrderDateID') != -1)      # Order date must exist in dim_date
)

# Identify orphan records (records with -1 dimension keys)
df_with_orphan_flag = df_validated.withColumn(
    'has_orphan',
    (col('CustomerSK') == -1) |
    (col('ProductSK') == -1) |
    (col('WarehouseSK') == -1) |
    (col('RetailerSK') == -1) |
    (col('AgentSK') == -1) |
    (col('PaymentModeSK') == -1) |
    (col('ProductCategorySK') == -1) |
    (col('MFDDateID') == -1) |
    (col('EXPDateID') == -1) |
    (col('DeliveryDateID') == -1)
)

orphan_count = df_with_orphan_flag.filter(col('has_orphan') == True).count()
valid_count = df_validated.count()
invalid_count = record_count - valid_count

print(f"\n" + "="*70)
print("VALIDATION RESULTS")
print("="*70)
print(f"Total records processed: {record_count:,}")
print(f"Valid records (passed): {valid_count:,}")
print(f"Orphan records (has -1 keys but kept): {orphan_count:,}")
print(f"Rejected records (critical failures): {invalid_count:,}")
print("="*70)

# Alert on high orphan rate (>20%)
if valid_count > 0:
    orphan_rate = (orphan_count / valid_count) * 100
    if orphan_rate > 20:
        print(f"\n🚨 ALERT: High orphan rate {orphan_rate:.1f}% - Check dimension loads!")
    elif orphan_rate > 5:
        print(f"\n⚠️  WARNING: Elevated orphan rate {orphan_rate:.1f}%")
    else:
        print(f"\n✅ Orphan rate: {orphan_rate:.1f}% (acceptable)")

# Drop the orphan flag before merge
df_final = df_with_orphan_flag.drop('has_orphan')

# COMMAND ----------

# DBTITLE 1,Quarantine Rejected Records
# Quarantine records that failed critical validation
if invalid_count > 0:
    # Identify rejected records by anti-joining with validated set
    df_rejected = df_fact.join(
        df_validated.select('OrderID'),
        on='OrderID',
        how='left_anti'
    )
    
    # Add rejection metadata
    df_quarantine = df_rejected.withColumn(
        'rejection_reason',
        when(col('OrderID').isNull(), 'NULL OrderID')
        .when(col('finalprice') <= 0, 'Invalid price (<=0)')
        .when(col('OrderDateID') == -1, 'OrderDate outside dim_date range')
        .otherwise('Unknown validation failure')
    ).withColumn(
        'quarantine_timestamp',
        current_timestamp()
    ).withColumn(
        'batch_id',
        lit(datetime.now().strftime('%Y%m%d_%H%M%S'))
    )
    
    # Write to quarantine table for investigation
    quarantine_table = f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_sales_quarantine"
    
    try:
        df_quarantine.write.mode('append').saveAsTable(quarantine_table)
        print(f"\n✅ {invalid_count:,} rejected records written to quarantine table")
        print(f"   Table: {quarantine_table}")
        print(f"\n   Top rejection reasons:")
        df_quarantine.groupBy('rejection_reason').count().orderBy(col('count').desc()).show(5, truncate=False)
    except Exception as e:
        print(f"\n⚠️  WARNING: Could not write to quarantine table: {e}")
        print(f"   Rejected records details:")
        display(df_quarantine.select('OrderID', 'rejection_reason', 'OrderDateID', 'finalprice').limit(10))
else:
    print("\n✅ No records rejected - all passed validation")

# COMMAND ----------

# DBTITLE 1,MERGE into Silver Fact Table
# Execute idempotent MERGE
if valid_count > 0:
    # All dimension keys are now properly looked up - using COALESCE for orphan handling
    
    # Deduplicate by (OrderID, BatchDate) - keep most recent record per grain based on LoadedAt
    # COMPOSITE KEY FIX (Nov 2): incremental generator OrderIDs restart at 1 every run, so an
    # OrderID-only dedup/MERGE key would destructively UPDATE historical fact_sales rows from
    # other BatchDates. Composite grain matches dbt fact_sales unique_key ['OrderID','BatchDate']
    # and the 5_ArchiveAndCleanup bridge merge key.
    from pyspark.sql.window import Window
    window_spec = Window.partitionBy('OrderID', 'BatchDate').orderBy(col('LoadedAt').desc())
    df_for_merge = df_final.withColumn('row_num', row_number().over(window_spec)) \
        .filter(col('row_num') == 1) \
        .drop('row_num')
    
    merge_stats = upsert_merge(
        source_df=df_for_merge,
        target_table=Config.TBL_FACT_SALES,
        merge_keys=['OrderID', 'BatchDate'],
        batch_date=datetime.now().strftime('%Y-%m-%d')
    )
    
    print(f"\nMERGE completed:")
    print(f"  Target: {merge_stats['target_table']}")
    print(f"  Records processed: {merge_stats['source_rows']:,}")
else:
    print("No valid records to merge")
    dbutils.notebook.exit("SUCCESS: No valid changes")

# COMMAND ----------

# DBTITLE 1,Update Watermark
# Check if upstream processing completed
try:
    # Get max processing timestamp from VALIDATED records only
    # This ensures rejected records can be reprocessed after dimension fixes
    max_processing_time = df_final.agg(
        max('LoadedAt')
    ).first()[0]
    
    # Update watermark for idempotency - inline implementation to avoid utility function bugs
    watermark_name = 'stg_sales_cdc'
    current_user = spark.sql("SELECT current_user()").first()[0]
    spark.sql(f"""
        MERGE INTO {Config.TBL_CDC_WATERMARKS} AS target
        USING (SELECT 
            '{watermark_name}' AS table_name,
            '{str(max_processing_time)}' AS last_processed_value,
            current_timestamp() AS updated_at,
            '{current_user}' AS updated_by,
            {valid_count} AS rows_processed
        ) AS source
        ON target.table_name = source.table_name
        WHEN MATCHED THEN UPDATE SET
            target.last_processed_value = source.last_processed_value,
            target.updated_at = source.updated_at,
            target.updated_by = source.updated_by,
            target.rows_processed = source.rows_processed
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"✓ Watermark updated: {watermark_name} -> {max_processing_time}")
    
    print("\n" + "="*70)
    print("✅ Sales Fact CDC Processing Complete")
    print("="*70)
    print(f"Total records in batch: {record_count:,}")
    print(f"Successfully loaded: {valid_count:,}")
    if orphan_count > 0:
        print(f"Orphan records (with -1 keys): {orphan_count:,}")
    if invalid_count > 0:
        print(f"Quarantined (critical failures): {invalid_count:,}")
    print(f"\nWatermark updated to: {max_processing_time}")
    print(f"Next run will process records after this timestamp")
    print("="*70)
except NameError:
    print("ERROR: Upstream cells (7-9) must be run before updating watermark")
    print("Please run cells 7, 8, and 9 first.")
    dbutils.notebook.exit("FAILED: Missing upstream processing")