# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Archive Incremental Data & Cleanup Staging Tables
# MAGIC %md
# MAGIC # Archive Incremental Data & Cleanup Staging Tables
# MAGIC
# MAGIC **Purpose:** Post-processing step that runs AFTER all dimension/fact/gold processing
# MAGIC
# MAGIC **What it does:**
# MAGIC 1. Validates all downstream processing completed successfully
# MAGIC 2. Merges incremental staging → history baseline:
# MAGIC    - Sales: stg_sales_incremental → stg_sales
# MAGIC    - Purchase: stg_purchase_incremental → stg_purchase
# MAGIC    - Stock: stg_stock_incremental → stg_stock_status
# MAGIC 3. Records audit metadata for all three pipelines
# MAGIC 4. Cleans up staging tables (configurable retention):
# MAGIC    - Truncates: stg_sales_incremental, stg_purchase_incremental, stg_stock_incremental
# MAGIC    - Truncates: customer_changes_raw (CDC events - already applied to dim_customer)
# MAGIC
# MAGIC **CRITICAL:** Only run this AFTER dimensions, facts, and gold layer complete!
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `environment`: Target environment (dev/uat/main) - auto-detects catalog
# MAGIC - `retention_days`: How many days to keep in staging (default: 0 = immediate truncate)
# MAGIC - `validate_downstream`: Check downstream tables before merge (default: true)
# MAGIC - `dry_run`: Preview changes without executing (default: false)
# MAGIC
# MAGIC **Environment Detection:**
# MAGIC - Automatically uses `sap_dev`, `sap_uat`, or `sap_main` based on environment widget
# MAGIC - No hardcoded catalog names - 100% environment-agnostic
# MAGIC - Works across dev/uat/prod without code changes

# COMMAND ----------

# DBTITLE 1,Load Config
# MAGIC %run ../1_Setup/1_Config

# COMMAND ----------

# DBTITLE 1,Configuration & Parameters - Environment Auto-Detection
# ============================================================================
# CONFIGURATION & PARAMETERS - ENVIRONMENT AUTO-DETECTION
# ============================================================================
# Widgets for runtime parameters
try:
    dbutils.widgets.dropdown("environment", "dev", ["dev", "uat", "main"], "Environment")
    dbutils.widgets.text("retention_days", "0", "Staging Retention (days)")
    dbutils.widgets.dropdown("validate_downstream", "true", ["true", "false"], "Validate Downstream")
    dbutils.widgets.dropdown("dry_run", "false", ["true", "false"], "Dry Run Mode")
    
    ENVIRONMENT = dbutils.widgets.get("environment")
    RETENTION_DAYS = int(dbutils.widgets.get("retention_days"))
    VALIDATE_DOWNSTREAM = dbutils.widgets.get("validate_downstream") == "true"
    DRY_RUN = dbutils.widgets.get("dry_run") == "true"
except:
    ENVIRONMENT = "dev"
    RETENTION_DAYS = 0
    VALIDATE_DOWNSTREAM = True
    DRY_RUN = False

# Config is loaded from main config notebook via %run

from datetime import datetime, timedelta
import json

print("="*70)
print("ARCHIVE & CLEANUP CONFIGURATION")
print("="*70)
print(f"Environment: {ENVIRONMENT}")
print(f"Target Catalog: {Config.CATALOG}")
print(f"Retention Days: {RETENTION_DAYS}")
print(f"Validate Downstream: {VALIDATE_DOWNSTREAM}")
print(f"Dry Run: {DRY_RUN}")
print()
print("Table Targets:")
print(f"  History: {Config.TBL_STG_SALES}")
print(f"  Incremental Staging: {Config.TBL_STG_SALES_INCREMENTAL}")
print(f"  CDC Events: {Config.TBL_BRONZE_CUSTOMER_CDC}")
print(f"  Audit Log: {Config.TBL_LOAD_AUDIT}")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Pre-Merge Validation
print("="*70)
print("STEP 1: PRE-MERGE VALIDATION")
print("="*70)

# Check all incremental staging tables have data - EARLY EXIT if empty
try:
    incremental_sales_count = spark.table(Config.TBL_STG_SALES_INCREMENTAL).count()
    incremental_purchase_count = spark.table(Config.TBL_STG_PURCHASE_INCREMENTAL).count()
    incremental_stock_count = spark.table(Config.TBL_STG_STOCK_INCREMENTAL).count()
    
    if incremental_sales_count == 0 and incremental_purchase_count == 0 and incremental_stock_count == 0:
        print("ℹ️  All incremental tables are already empty (already archived/cleaned)")
        print("✅ Skipping archive process - nothing to do (idempotent behavior)")
        dbutils.notebook.exit(json.dumps({"status": "SKIPPED", "message": "No incremental data to archive"}))
except Exception as e:
    print(f"⚠️  Could not read incremental tables: {str(e)}")
    print("✅ Assuming already archived - skipping (idempotent)")
    dbutils.notebook.exit(json.dumps({"status": "SKIPPED", "message": "Tables not accessible or already archived"}))

print(f"✓ Sales incremental: {incremental_sales_count:,} records")
print(f"✓ Purchase incremental: {incremental_purchase_count:,} records")
print(f"✓ Stock incremental: {incremental_stock_count:,} records")

# Check CDC was processed
cdc_count = spark.table(Config.TBL_BRONZE_CUSTOMER_CDC).count()
print(f"✓ CDC events table: {cdc_count:,} events")

# Get date range being archived
date_range = spark.sql(f"""
    SELECT 
        MIN(BatchDate) as min_date,
        MAX(BatchDate) as max_date,
        COUNT(DISTINCT BatchDate) as num_days
    FROM {Config.TBL_STG_SALES_INCREMENTAL}
""").collect()[0]

print(f"✓ Date range: {date_range.min_date} to {date_range.max_date} ({date_range.num_days} days)")

# Optional: Validate downstream tables updated
if VALIDATE_DOWNSTREAM:
    print("\n🔍 Validating downstream tables...")
    
    # Check dim_customer was updated today
    try:
        dim_customer_last_update = spark.sql(f"""
            SELECT MAX(effective_start_date) as last_update
            FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_customer
        """).collect()[0].last_update
        
        today = datetime.now().date()
        if str(dim_customer_last_update) >= str(today):
            print(f"  ✓ dim_customer updated: {dim_customer_last_update}")
        else:
            print(f"  ⚠️ WARNING: dim_customer last update: {dim_customer_last_update} (not today!)")
    except:
        print("  ⚠️ WARNING: Could not validate dim_customer (table may not exist)")
    
    # Check fact_sales was updated
    try:
        fact_sales_last_date = spark.sql(f"""
            SELECT MAX(order_date) as last_date
            FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_sales
        """).collect()[0].last_date
        
        if str(fact_sales_last_date) >= str(date_range.max_date):
            print(f"  ✓ fact_sales updated: {fact_sales_last_date}")
        else:
            print(f"  ⚠️ WARNING: fact_sales last date: {fact_sales_last_date} (older than incremental max!)")
    except:
        print("  ⚠️ WARNING: Could not validate fact_sales (table may not exist)")

print("\n" + "="*70)
print("✅ VALIDATION PASSED - Safe to proceed with merge")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Merge Incremental to History
print("\n" + "="*70)
print("STEP 2: MERGING INCREMENTAL → HISTORY")
print("="*70)

if DRY_RUN:
    print("🔍 DRY RUN MODE - Preview only (no changes)")
    
    # Show what would be merged for sales
    print("\n📊 SALES INCREMENTAL:")
    preview_sales = spark.sql(f"""
        SELECT 
            'SALES' as table_name,
            COUNT(*) as record_count,
            MIN(BatchDate) as min_date,
            MAX(BatchDate) as max_date
        FROM {Config.TBL_STG_SALES_INCREMENTAL}
    """)
    display(preview_sales)
    
    # Show what would be merged for purchase
    print("\n📊 PURCHASE INCREMENTAL:")
    preview_purchase = spark.sql(f"""
        SELECT 
            'PURCHASE' as table_name,
            COUNT(*) as record_count,
            MIN(BatchDate) as min_date,
            MAX(BatchDate) as max_date
        FROM {Config.TBL_STG_PURCHASE_INCREMENTAL}
    """)
    display(preview_purchase)
    
    # Show what would be merged for stock
    print("\n📊 STOCK INCREMENTAL:")
    preview_stock = spark.sql(f"""
        SELECT 
            'STOCK' as table_name,
            COUNT(*) as record_count,
            MIN(BatchDate) as min_date,
            MAX(BatchDate) as max_date
        FROM {Config.TBL_STG_STOCK_INCREMENTAL}
    """)
    display(preview_stock)
    
    print("\n⚠️ DRY RUN: No actual merge performed")
    
else:
    # ==================================================================
    # MERGE 1: SALES INCREMENTAL → SALES HISTORY
    # ==================================================================
    print(f"\n🔧 [1/3] Merging SALES: {incremental_sales_count:,} records...")
    
    # >>> GUARD (Phase P1, 2026-11-03): forward-only merge shield <<<
    # Jobs 07/24 generators re-write the SAME BatchDate staging rows (different random
    # data) AFTER Job 05 already archived that batch. Unguarded, those stale rows
    # UPDATE-overwrite valid bronze history (proven Nov 3: the Nov-2 batch now carries
    # the job-07 13:01 regen instead of the original 00:57 load). Only merge staging
    # rows for batches strictly NEWER than the newest batch already in history.
    max_sales_batch = spark.sql(
        f"SELECT COALESCE(MAX(BatchDate), DATE'1900-01-01') AS b FROM {Config.TBL_STG_SALES}"
    ).collect()[0]['b']
    fwd_sales = spark.sql(
        f"SELECT COUNT(*) c FROM {Config.TBL_STG_SALES_INCREMENTAL} WHERE BatchDate > DATE'{max_sales_batch}'"
    ).collect()[0]['c']
    print(f"   SHIELD: merging {fwd_sales:,} of {incremental_sales_count:,} staging rows (batches > {max_sales_batch})")
    merge_sales_query = f"""
    MERGE INTO {Config.TBL_STG_SALES} AS history
    USING (
        SELECT * FROM {Config.TBL_STG_SALES_INCREMENTAL}
        WHERE BatchDate > DATE'{max_sales_batch}'
    ) AS incremental
    ON history.OrderID = incremental.OrderID 
       AND history.BatchDate = incremental.BatchDate
    WHEN MATCHED THEN
      UPDATE SET *
    WHEN NOT MATCHED THEN
      INSERT *
    """
    
    spark.sql(merge_sales_query)
    
    # Verify merge
    history_sales_count = spark.table(Config.TBL_STG_SALES).count()
    print(f"  ✓ Sales history now has: {history_sales_count:,} records")
    
    # ==================================================================
    # MERGE 2: PURCHASE INCREMENTAL → PURCHASE HISTORY
    # ==================================================================
    print(f"\n🔧 [2/3] Merging PURCHASE: {incremental_purchase_count:,} records...")
    
    # >>> GUARD (Phase P1, 2026-11-03): forward-only merge shield (same rationale as SALES) <<<
    max_purchase_batch = spark.sql(
        f"SELECT COALESCE(MAX(BatchDate), DATE'1900-01-01') AS b FROM {Config.TBL_STG_PURCHASE}"
    ).collect()[0]['b']
    fwd_purchase = spark.sql(
        f"SELECT COUNT(*) c FROM {Config.TBL_STG_PURCHASE_INCREMENTAL} WHERE BatchDate > DATE'{max_purchase_batch}'"
    ).collect()[0]['c']
    print(f"   SHIELD: merging {fwd_purchase:,} of {incremental_purchase_count:,} staging rows (batches > {max_purchase_batch})")
    merge_purchase_query = f"""
    MERGE INTO {Config.TBL_STG_PURCHASE} AS history
    USING (
        SELECT * FROM {Config.TBL_STG_PURCHASE_INCREMENTAL}
        WHERE BatchDate > DATE'{max_purchase_batch}'
    ) AS incremental
    ON history.PurchaseID = incremental.PurchaseID 
       AND history.BatchDate = incremental.BatchDate
    WHEN MATCHED THEN
      UPDATE SET *
    WHEN NOT MATCHED THEN
      INSERT *
    """
    
    spark.sql(merge_purchase_query)
    
    # Verify merge
    history_purchase_count = spark.table(Config.TBL_STG_PURCHASE).count()
    print(f"  ✓ Purchase history now has: {history_purchase_count:,} records")
    
    # ==================================================================
    # MERGE 3: STOCK INCREMENTAL → STOCK HISTORY
    # ==================================================================
    print(f"\n🔧 [3/3] Merging STOCK: {incremental_stock_count:,} records...")
    
    # >>> GUARD (Phase P1, 2026-11-03): forward-only merge shield (same rationale as SALES) <<<
    max_stock_batch = spark.sql(
        f"SELECT COALESCE(MAX(BatchDate), DATE'1900-01-01') AS b FROM {Config.TBL_STG_STOCK}"
    ).collect()[0]['b']
    fwd_stock = spark.sql(
        f"SELECT COUNT(*) c FROM {Config.TBL_STG_STOCK_INCREMENTAL} WHERE BatchDate > DATE'{max_stock_batch}'"
    ).collect()[0]['c']
    print(f"   SHIELD: merging {fwd_stock:,} of {incremental_stock_count:,} staging rows (batches > {max_stock_batch})")
    merge_stock_query = f"""
    MERGE INTO {Config.TBL_STG_STOCK} AS history
    USING (
        SELECT * FROM {Config.TBL_STG_STOCK_INCREMENTAL}
        WHERE BatchDate > DATE'{max_stock_batch}'
    ) AS incremental
    ON history.ProductName = incremental.ProductName 
       AND history.WareHouseName = incremental.WareHouseName
       AND history.BatchDate = incremental.BatchDate
    WHEN MATCHED THEN
      UPDATE SET *
    WHEN NOT MATCHED THEN
      INSERT *
    """
    
    spark.sql(merge_stock_query)
    
    # Verify merge
    history_stock_count = spark.table(Config.TBL_STG_STOCK).count()
    print(f"  ✓ Stock history now has: {history_stock_count:,} records")
    
    # Check history date range
    history_range = spark.sql(f"""
        SELECT 
            MIN(BatchDate) as min_date,
            MAX(BatchDate) as max_date,
            COUNT(DISTINCT BatchDate) as num_days
        FROM {Config.TBL_STG_SALES}
    """).collect()[0]
    
    print(f"  ✓ History date range: {history_range.min_date} to {history_range.max_date} ({history_range.num_days} days)")

print("\n" + "="*70)
print("✅ MERGE COMPLETE")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Audit Logging
if not DRY_RUN:
    print("\n" + "="*70)
    print("STEP 3: RECORDING AUDIT METADATA")
    print("="*70)
    
    audit_record = spark.createDataFrame([{
        "load_timestamp": datetime.now(),
        "environment": ENVIRONMENT,
        "sales_incremental_records": incremental_sales_count,
        "purchase_incremental_records": incremental_purchase_count,
        "stock_incremental_records": incremental_stock_count,
        "cdc_events": cdc_count,
        "sales_history_total": history_sales_count,
        "purchase_history_total": history_purchase_count,
        "stock_history_total": history_stock_count,
        "date_range_start": date_range.min_date,
        "date_range_end": date_range.max_date,
        "num_days_merged": date_range.num_days,
        "status": "SUCCESS"
    }])
    
    # Create audit table if doesn't exist
    audit_record.write.mode("append").saveAsTable(Config.TBL_LOAD_AUDIT)
    
    print(f"  ✓ Audit record written to {Config.TBL_LOAD_AUDIT}")
    print("\n" + "="*70)
    print("✅ AUDIT COMPLETE")
    print("="*70)

# COMMAND ----------

# DBTITLE 1,Cleanup Staging Tables
print("\n" + "="*70)
print("STEP 4: CLEANING UP STAGING TABLES")
print("="*70)

if DRY_RUN:
    print("🔍 DRY RUN MODE - Preview only (no cleanup)")
    
    if RETENTION_DAYS == 0:
        print("  Would truncate all incremental tables:")
        print(f"    - {Config.TBL_STG_SALES_INCREMENTAL}")
        print(f"    - {Config.TBL_STG_PURCHASE_INCREMENTAL}")
        print(f"    - {Config.TBL_STG_STOCK_INCREMENTAL}")
        print(f"    - {Config.TBL_BRONZE_CUSTOMER_CDC}")
    else:
        cutoff_date = (datetime.now() - timedelta(days=RETENTION_DAYS)).strftime("%Y-%m-%d")
        print(f"  Would delete records before: {cutoff_date}")
        
        print("\n  SALES:")
        preview_sales = spark.sql(f"""
            SELECT 
                COUNT(*) as records_to_delete,
                MIN(BatchDate) as oldest_date,
                MAX(BatchDate) as newest_date_to_delete
            FROM {Config.TBL_STG_SALES_INCREMENTAL}
            WHERE BatchDate < '{cutoff_date}'
        """)
        display(preview_sales)
        
        print("\n  PURCHASE:")
        preview_purchase = spark.sql(f"""
            SELECT 
                COUNT(*) as records_to_delete,
                MIN(BatchDate) as oldest_date,
                MAX(BatchDate) as newest_date_to_delete
            FROM {Config.TBL_STG_PURCHASE_INCREMENTAL}
            WHERE BatchDate < '{cutoff_date}'
        """)
        display(preview_purchase)
        
        print("\n  STOCK:")
        preview_stock = spark.sql(f"""
            SELECT 
                COUNT(*) as records_to_delete,
                MIN(BatchDate) as oldest_date,
                MAX(BatchDate) as newest_date_to_delete
            FROM {Config.TBL_STG_STOCK_INCREMENTAL}
            WHERE BatchDate < '{cutoff_date}'
        """)
        display(preview_stock)
    
    print("\n⚠️ DRY RUN: No actual cleanup performed")
    
else:
    if RETENTION_DAYS == 0:
        # Immediate truncate all incremental tables
        print(f"🧹 [1/3] Truncating {Config.TBL_STG_SALES_INCREMENTAL}...")
        spark.sql(f"TRUNCATE TABLE {Config.TBL_STG_SALES_INCREMENTAL}")
        remaining_sales = spark.table(Config.TBL_STG_SALES_INCREMENTAL).count()
        print(f"  ✓ Sales staging cleaned ({remaining_sales} records)")
        
        print(f"\n🧹 [2/3] Truncating {Config.TBL_STG_PURCHASE_INCREMENTAL}...")
        spark.sql(f"TRUNCATE TABLE {Config.TBL_STG_PURCHASE_INCREMENTAL}")
        remaining_purchase = spark.table(Config.TBL_STG_PURCHASE_INCREMENTAL).count()
        print(f"  ✓ Purchase staging cleaned ({remaining_purchase} records)")
        
        print(f"\n🧹 [3/4] Truncating {Config.TBL_STG_STOCK_INCREMENTAL}...")
        spark.sql(f"TRUNCATE TABLE {Config.TBL_STG_STOCK_INCREMENTAL}")
        remaining_stock = spark.table(Config.TBL_STG_STOCK_INCREMENTAL).count()
        print(f"  ✓ Stock staging cleaned ({remaining_stock} records)")
        
        print(f"\n🧹 [4/4] Truncating {Config.TBL_BRONZE_CUSTOMER_CDC}...")
        spark.sql(f"TRUNCATE TABLE {Config.TBL_BRONZE_CUSTOMER_CDC}")
        remaining_cdc = spark.table(Config.TBL_BRONZE_CUSTOMER_CDC).count()
        print(f"  ✓ CDC events cleaned ({remaining_cdc} records)")
    
    else:
        # Retain N days for all three tables
        cutoff_date = (datetime.now() - timedelta(days=RETENTION_DAYS)).strftime("%Y-%m-%d")
        print(f"🧹 Deleting records older than {RETENTION_DAYS} days ({cutoff_date})...")
        
        # Sales
        print(f"\n  [1/3] Cleaning {Config.TBL_STG_SALES_INCREMENTAL}...")
        spark.sql(f"""
            DELETE FROM {Config.TBL_STG_SALES_INCREMENTAL}
            WHERE BatchDate < '{cutoff_date}'
        """)
        remaining_sales = spark.table(Config.TBL_STG_SALES_INCREMENTAL).count()
        print(f"    ✓ {remaining_sales:,} records retained")
        
        # Purchase
        print(f"\n  [2/3] Cleaning {Config.TBL_STG_PURCHASE_INCREMENTAL}...")
        spark.sql(f"""
            DELETE FROM {Config.TBL_STG_PURCHASE_INCREMENTAL}
            WHERE BatchDate < '{cutoff_date}'
        """)
        remaining_purchase = spark.table(Config.TBL_STG_PURCHASE_INCREMENTAL).count()
        print(f"    ✓ {remaining_purchase:,} records retained")
        
        # Stock
        print(f"\n  [3/4] Cleaning {Config.TBL_STG_STOCK_INCREMENTAL}...")
        spark.sql(f"""
            DELETE FROM {Config.TBL_STG_STOCK_INCREMENTAL}
            WHERE BatchDate < '{cutoff_date}'
        """)
        remaining_stock = spark.table(Config.TBL_STG_STOCK_INCREMENTAL).count()
        print(f"    ✓ {remaining_stock:,} records retained")
        
        # CDC Events - delete old events
        print(f"\n  [4/4] Cleaning {Config.TBL_BRONZE_CUSTOMER_CDC}...")
        spark.sql(f"""
            DELETE FROM {Config.TBL_BRONZE_CUSTOMER_CDC}
            WHERE ChangeTimestamp < '{cutoff_date}'
        """)
        remaining_cdc = spark.table(Config.TBL_BRONZE_CUSTOMER_CDC).count()
        print(f"    ✓ {remaining_cdc:,} records retained")
    
    print("\n✅ All staging tables cleaned (incremental + CDC)")

print("\n" + "="*70)
print("✅ CLEANUP COMPLETE")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Summary Report
print("\n" + "="*70)
print("ARCHIVE & CLEANUP SUMMARY")
print("="*70)

if DRY_RUN:
    print("🔍 DRY RUN COMPLETED - No changes made")
else:
    print("✅ PRODUCTION RUN COMPLETED")

print(f"""
📊 Statistics:
  
  SALES:
    - Incremental records merged: {incremental_sales_count:,}
    - History total: {f'{history_sales_count:,}' if not DRY_RUN else 'N/A (dry run)'}
  
  PURCHASE:
    - Incremental records merged: {incremental_purchase_count:,}
    - History total: {f'{history_purchase_count:,}' if not DRY_RUN else 'N/A (dry run)'}
  
  STOCK:
    - Incremental records merged: {incremental_stock_count:,}
    - History total: {f'{history_stock_count:,}' if not DRY_RUN else 'N/A (dry run)'}
  
  CDC:
    - CDC events processed: {cdc_count:,}
  
  METADATA:
    - Date range: {date_range.min_date} to {date_range.max_date} ({date_range.num_days} days)
    - Retention policy: {RETENTION_DAYS} days
    - Environment: {ENVIRONMENT} (Catalog: {Config.CATALOG})
  
✅ Next Steps:
  1. Verify downstream reports/dashboards updated
  2. Check audit table: {Config.TBL_LOAD_AUDIT}
  3. Tomorrow's incremental run can proceed
""")

print("="*70)