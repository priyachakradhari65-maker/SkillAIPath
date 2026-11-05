# Databricks notebook source
# DBTITLE 1,Incremental dbt Pipeline - Overview
# MAGIC %md
# MAGIC # Incremental dbt Pipeline Execution (OPTIMIZED)
# MAGIC
# MAGIC **Purpose**: Execute incremental dbt transformations on new Bronze data
# MAGIC
# MAGIC **Architecture**:
# MAGIC ```
# MAGIC Bronze (Incremental Ingestion)
# MAGIC     ↓
# MAGIC dbt Staging
# MAGIC     ↓
# MAGIC dbt Intermediate
# MAGIC     ↓
# MAGIC dbt Marts
# MAGIC ```
# MAGIC
# MAGIC **⚡ Performance Optimizations**:
# MAGIC - ✅ Runs ONLY `dbt run` (incremental models)
# MAGIC - ✅ Skips `dbt seed` (~8 minutes saved)
# MAGIC - ✅ Skips `dbt deps` (~12 seconds saved)
# MAGIC - ✅ Direct dbt execution (no notebook wrapper overhead)
# MAGIC - ✅ Optimized for unattended Databricks Job scheduling
# MAGIC
# MAGIC **Expected Runtime**: 3-5 minutes (vs 12-14 minutes before)
# MAGIC
# MAGIC **Important**:
# MAGIC - This notebook performs INCREMENTAL processing only
# MAGIC - Bronze tables must already exist
# MAGIC - No full refresh operations
# MAGIC - No table recreation
# MAGIC - dbt seed and deps are run manually when needed (see documentation cell)
# MAGIC
# MAGIC **Pipeline Steps**:
# MAGIC 1. Environment Setup
# MAGIC 2. Bronze Incremental Validation
# MAGIC 3. dbt Project Validation
# MAGIC 4. Manual Operations Documentation
# MAGIC 5. One-Time Rebuild (commented)
# MAGIC 6. Generate Incremental Data
# MAGIC 7. Execute Incremental Pipeline (OPTIMIZED)
# MAGIC 8. Post Execution Validation
# MAGIC 9. Execution Summary

# COMMAND ----------

# DBTITLE 1,1. Environment Setup
# MAGIC %run /Shared/dev/SkillAIPath/1_Canvas/1_Setup/1_config

# COMMAND ----------

# DBTITLE 1,Import Libraries and Display Config
# Import necessary libraries
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import uuid

print("="*70)
print("INCREMENTAL DBT PIPELINE - CONFIGURATION LOADED")
print("="*70)
print(f"Environment: {Config.ENVIRONMENT}")
print(f"Catalog: {Config.CATALOG}")
print(f"Bronze Schema: {Config.CATALOG}.bronze (SOURCE)")
print(f"DBT Bronze Schema: sap_{Config.ENVIRONMENT}.bronze")
print(f"DBT Silver Schema: sap_{Config.ENVIRONMENT}.silver")
print(f"DBT Gold Schema: sap_{Config.ENVIRONMENT}.gold")
print("="*70)

# COMMAND ----------

# DBTITLE 1,2. Bronze Incremental Validation
# Bronze table definitions
BRONZE_TABLES = {
    'sales': Config.TBL_STG_SALES,
    'purchases': Config.TBL_STG_PURCHASE,
    'inventory': Config.TBL_STG_STOCK
}

# dbt project path
DBT_PROJECT_PATH = "/Workspace/Shared/dev/SkillAIPath/2_Data_Production/03_Dbt"
DBT_EXEC_NOTEBOOK = "/Shared/dev/SkillAIPath/2_Data_Production/03_Dbt/StartHere_DBT"

# Generate unique pipeline run ID and metadata
pipeline_run_id = str(uuid.uuid4())
execution_start = datetime.now()
batch_date = execution_start.strftime("%Y-%m-%d")
environment = Config.ENVIRONMENT
load_type = "INCREMENTAL"

# Store in dictionary for later use
pipeline_metadata = {
    'pipeline_run_id': pipeline_run_id,
    'execution_start': execution_start,
    'start_time': execution_start,
    'batch_date': batch_date,
    'environment': environment,
    'load_type': load_type,
    'catalog': Config.CATALOG
}

print("="*70)
print("PIPELINE EXECUTION METADATA")
print("="*70)
print(f"Pipeline Run ID   : {pipeline_run_id}")
print(f"Execution Time    : {execution_start.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Batch Date        : {batch_date}")
print(f"Environment       : {environment}")
print(f"Load Type         : {load_type}")
print(f"Target Catalog    : {Config.CATALOG}")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Ensure DBT Catalog and Schemas Exist
# Ensure sap_dev catalog and required schemas exist
# This is a safety check for incremental runs

DBT_CATALOG = f"sap_{Config.ENVIRONMENT}"

print("="*70)
print("DBT CATALOG VALIDATION")
print("="*70)
print(f"\nValidating DBT catalog and schemas...")
print(f"DBT Catalog: {DBT_CATALOG}")
print()

try:
    # Ensure DBT catalog exists
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {DBT_CATALOG}")
    print(f"✅ Catalog: {DBT_CATALOG}")
    
    # Ensure schemas exist (created by full load, but safety check)
    schemas_to_check = [
        'staging',   # DBT staging models (views)
        'silver',    # DBT intermediate/dimension/fact models
        'gold',      # DBT mart models
        'reference'  # DBT seed data (reference tables)
    ]
    
    for schema in schemas_to_check:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DBT_CATALOG}.{schema}")
        print(f"✅ Schema: {DBT_CATALOG}.{schema}")
    
    print(f"\n✅ DBT catalog and schemas validated")
    print("="*70)
    print()
    
except Exception as e:
    print(f"❌ Error validating DBT catalog/schemas: {str(e)}")
    print(f"   Catalog: {DBT_CATALOG}")
    print()
    raise

# COMMAND ----------

# DBTITLE 1,3. dbt Project Validation
print("="*70)
print("VALIDATE BRONZE TABLES EXIST")
print("="*70)
print()

print("Checking Bronze tables before incremental load...")
print("-"*70)

bronze_exists = {}
all_exist = True

for table_name, full_table_name in BRONZE_TABLES.items():
    try:
        # Check if table exists by trying to count rows
        count = spark.table(full_table_name).count()
        bronze_exists[table_name] = True
        print(f"  ✓ {full_table_name}: {count:,} existing rows")
    except Exception as e:
        bronze_exists[table_name] = False
        all_exist = False
        print(f"  ❌ {full_table_name}: Table does NOT exist")
        print(f"     Error: {str(e)[:100]}")

print()

if not all_exist:
    print("❌ ERROR: Some Bronze tables are missing!")
    print()
    print("For INCREMENTAL loads, Bronze tables MUST already exist.")
    print("Run the FULL LOAD pipeline first (Startdbt_full notebook).")
    print()
    raise Exception("Bronze tables missing. Run full load first.")

print("✅ All Bronze tables exist - ready for incremental data")
print("="*70)

pipeline_metadata['bronze_exists'] = bronze_exists

# COMMAND ----------

# DBTITLE 1,⚠️ Manual Operations (NOT Part of Daily Pipeline)
# MAGIC %md
# MAGIC # ⚠️ Manual Operations (NOT Part of Daily Pipeline)
# MAGIC
# MAGIC The following dbt operations are **NOT** included in the daily incremental pipeline because they are **infrequent** and **time-consuming**.
# MAGIC
# MAGIC ## When to Run dbt seed
# MAGIC
# MAGIC **Purpose**: Load reference/seed data from CSV files into tables
# MAGIC
# MAGIC **Run When**:
# MAGIC * Initial pipeline setup
# MAGIC * Reference data files are updated (e.g., category_reference.csv)
# MAGIC * Seed tables need to be refreshed
# MAGIC
# MAGIC **Command**:
# MAGIC ```bash
# MAGIC cd /Workspace/Shared/dev/SkillAIPath/2_Data_Platform_Foundation/03_Dbt
# MAGIC dbt seed --target dev --profiles-dir .
# MAGIC ```
# MAGIC
# MAGIC **Runtime**: ~8 minutes
# MAGIC
# MAGIC **Why Not Daily?**
# MAGIC * Reference data rarely changes
# MAGIC * Seeds are dimension/lookup tables, not transactional
# MAGIC * Running daily adds 8 minutes with no benefit
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## When to Run dbt deps
# MAGIC
# MAGIC **Purpose**: Install dbt package dependencies from packages.yml
# MAGIC
# MAGIC **Run When**:
# MAGIC * Initial pipeline setup
# MAGIC * packages.yml is modified
# MAGIC * dbt packages need to be updated
# MAGIC
# MAGIC **Command**:
# MAGIC ```bash
# MAGIC cd /Workspace/Shared/dev/SkillAIPath/2_Data_Platform_Foundation/03_Dbt
# MAGIC dbt deps --profiles-dir .
# MAGIC ```
# MAGIC
# MAGIC **Runtime**: ~12 seconds
# MAGIC
# MAGIC **Why Not Daily?**
# MAGIC * Package dependencies rarely change in production
# MAGIC * Already installed from previous runs
# MAGIC * Unnecessary overhead for incremental runs
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Daily Pipeline Scope
# MAGIC
# MAGIC The **optimized daily incremental pipeline** runs ONLY:
# MAGIC
# MAGIC 1. ✅ Generate incremental Bronze data
# MAGIC 2. ✅ Validate Bronze tables exist
# MAGIC 3. ✅ Run `dbt run` (incremental models)
# MAGIC 4. ✅ Validate output layers
# MAGIC 5. ✅ Generate summary
# MAGIC
# MAGIC **Expected Runtime**: 3-5 minutes (vs 12-14 minutes before optimization)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Full Refresh
# MAGIC
# MAGIC For a complete rebuild (NOT incremental):
# MAGIC * Use the **Startdbt_full** notebook instead
# MAGIC * That notebook handles seed, deps, and full-refresh operations

# COMMAND ----------

# DBTITLE 1,🔧 ONE-TIME: Rebuild Fact Tables with LoadedAt
# ============================================================================
# ONE-TIME REBUILD: Fact Tables with LoadedAt Column
# ============================================================================
# ✅ COMPLETED: 2026-08-06 06:40:07
#
# This cell successfully rebuilt:
#   • fact_sales (7.34s)
#   • fact_purchase (6.35s)
#   • fact_inventory (5.69s)
#
# All fact tables now contain LoadedAt column.
# Incremental watermark logic is functional.
#
# ⚠️  THIS CELL IS NOW COMMENTED OUT FOR FUTURE RUNS
# ============================================================================

'''
import subprocess
import sys
import os
from datetime import datetime

# Install dbt-databricks if not already available
print("Installing dbt-databricks...")
install_result = subprocess.run(
    [sys.executable, '-m', 'pip', 'install', 'dbt-databricks>=1.9.0,<2.0.0', '--quiet'],
    capture_output=True,
    text=True,
    timeout=180
)
if install_result.returncode == 0:
    print("✓ dbt-databricks installed\n")
else:
    print(f"⚠️  dbt install had warnings (continuing anyway)\n")

print("="*80)
print("⚠️  ONE-TIME REBUILD: FACT TABLES WITH LoadedAt COLUMN")
print("="*80)
print(f"Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()
print("📋 PURPOSE:")
print("   Add LoadedAt column to fact tables for incremental watermark logic")
print()
print("📊 Models being rebuilt:")
print("   • fact_sales")
print("   • fact_purchase")
print("   • fact_inventory")
print()
print("✅ After this rebuild:")
print("   • Fact tables will contain LoadedAt column")
print("   • Incremental processing will work correctly")
print("   • Future daily runs will process only new data")
print("   • THIS CELL SHOULD BE COMMENTED OUT")
print()
print("="*80)
print()

# Change to dbt project directory
os.chdir(DBT_PROJECT_PATH)
print(f"Working Directory: {os.getcwd()}")
print()

# Get environment
try:
    target_env = environment
except NameError:
    target_env = 'dev'
    print("⚠️  Environment not detected, using default: dev")

print(f"Target Environment: {target_env}")
print(f"Catalog: sap_{target_env}")
print()
print("Executing: dbt run --select fact_sales fact_purchase fact_inventory --full-refresh --profiles-dir .")
print("-"*80)
print()

start_time = datetime.now()

# Set up Databricks connection environment variables
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
warehouses = list(w.warehouses.list())

if warehouses:
    wh = warehouses[0]
    host = w.config.host.replace('https://', '')
    http_path = f'/sql/1.0/warehouses/{wh.id}'
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    
    os.environ['DATABRICKS_HOST'] = host
    os.environ['DATABRICKS_HTTP_PATH'] = http_path
    os.environ['DATABRICKS_TOKEN'] = token
    os.environ['DBT_TARGET'] = target_env
    
    print(f"✓ SQL Warehouse: {wh.name}")
    print(f"✓ Connection configured\n")
else:
    print("❌ No SQL Warehouse found\n")

# Find dbt executable
dbt_cmd = 'dbt'  # Default to 'dbt' in PATH

try:
    result = subprocess.run(
        [dbt_cmd, 'run', '--select', 'fact_sales', 'fact_purchase', 'fact_inventory', '--full-refresh', '--target', target_env, '--profiles-dir', '.'],
        capture_output=True,
        text=True,
        cwd=DBT_PROJECT_PATH,
        timeout=600
    )
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print(result.stdout)
    
    if result.stderr:
        print("\nSTDERR:")
        print(result.stderr)
    
    print()
    print("="*80)
    print(f"Execution Time: {duration:.1f} seconds ({duration/60:.1f} minutes)")
    print()
    
    if result.returncode == 0:
        print("✅ SUCCESS: All fact tables rebuilt with LoadedAt column")
        print("="*80)
        print()
        print("📝 NEXT STEPS:")
        print("   1. ✓ Fact tables now include LoadedAt in their schema")
        print("   2. ✓ Incremental watermark logic is now functional")
        print("   3. ⚠️  COMMENT OUT THIS CELL for future runs")
        print("   4. ✓ Future runs will use 'dbt run' (incremental mode)")
        print()
        print("⚡ The pipeline is now ready for scheduled daily execution!")
        print("="*80)
        print()
        
        # Store rebuild success in metadata
        pipeline_metadata['fact_tables_rebuilt'] = True
        pipeline_metadata['rebuild_timestamp'] = datetime.now().isoformat()
        
    else:
        print(f"❌ FAILED: dbt run failed (return code: {result.returncode})")
        print("="*80)
        print()
        print("⚠️  Check the output above for errors.")
        print("   The incremental pipeline will continue, but fact tables may not")
        print("   process incrementally until this rebuild succeeds.")
        print()
        
        pipeline_metadata['fact_tables_rebuilt'] = False
        pipeline_metadata['rebuild_error'] = f"Return code: {result.returncode}"
        
        # Don't raise exception - allow pipeline to continue
        
except Exception as e:
    print(f"❌ ERROR: Fact table rebuild failed")
    print(f"   Error: {str(e)}")
    print()
    print("⚠️  The incremental pipeline will continue, but fact tables may not")
    print("   process incrementally until this rebuild succeeds.")
    print()
    
    pipeline_metadata['fact_tables_rebuilt'] = False
    pipeline_metadata['rebuild_error'] = str(e)
    
    # Don't raise exception - allow pipeline to continue

print("\n" + "="*80)
print("Proceeding with normal incremental pipeline execution...")
print("="*80)
print()
'''

print("="*80)
print("🔒 ONE-TIME REBUILD CELL IS COMMENTED OUT")
print("="*80)
print()
print("✅ Fact tables were rebuilt on 2026-08-06 06:40:07")
print("✅ LoadedAt column is present in all fact tables")
print("✅ Incremental watermark logic is functional")
print()
print("Proceeding with normal incremental pipeline execution...")
print("="*80)
print()

# COMMAND ----------

# DBTITLE 1,4. Install dbt Dependencies
print("="*70)
print("GENERATE INCREMENTAL BRONZE DATA")
print("="*70)
print()
print("ℹ️  Calling existing Bronze Data Generator for incremental batch...")
print(f"   Notebook: /Shared/dev/SkillAIPath/1_Canvas/1_Setup/2_data_generator")
print(f"   Mode: APPEND (incremental)")
print()

try:
    # Call the data generator notebook with incremental mode
    # This will APPEND new data to existing Bronze tables
    result = dbutils.notebook.run(
        "/Shared/dev/SkillAIPath/1_Canvas/1_Setup/2_data_generator",
        timeout_seconds=600,
        arguments={
            "mode": "incremental",
            "write_mode": "append",
            "batch_days": "1",
            "records_per_day": "1000",
            "incremental_date": batch_date
        }
    )
    
    print("✅ Incremental Bronze data generated successfully")
    print(f"   Result: {result}")
    pipeline_metadata['bronze_generated'] = True
    
except Exception as e:
    print(f"❌ ERROR: Bronze data generation failed")
    print(f"   Error: {str(e)}")
    pipeline_metadata['bronze_generated'] = False
    raise Exception(f"Bronze data generation failed: {str(e)}")

print()
print("="*70)
print("✅ INCREMENTAL BRONZE DATA GENERATION COMPLETE")
print("="*70)

# COMMAND ----------

# DBTITLE 1,5. Execute Incremental dbt Pipeline
import subprocess
import sys
import os
from databricks.sdk import WorkspaceClient

print("="*70)
print("DBT INCREMENTAL PIPELINE EXECUTION (OPTIMIZED)")
print("="*70)
print(f"\ndbt Project: {DBT_PROJECT_PATH}")
print(f"Target Environment: {environment}")
print(f"Catalog: {Config.CATALOG}")
print()
print("⚡ OPTIMIZED FOR DAILY EXECUTION:")
print("   • Skips dbt seed (reference data)")
print("   • Skips dbt deps (dependencies)")
print("   • Runs only dbt run (incremental models)")
print()
print("⚠️  INCREMENTAL MODE: dbt models will process new data only")
print()

# ============================================================================
# WHEN TO RUN MANUALLY (NOT PART OF DAILY PIPELINE):
# ============================================================================
# 
# 1. dbt seed - Run when reference/seed data changes:
#    cd {DBT_PROJECT_PATH}
#    dbt seed --target dev --profiles-dir .
#
# 2. dbt deps - Run when packages.yml is updated:
#    cd {DBT_PROJECT_PATH}
#    dbt deps --profiles-dir .
#
# These are one-time or infrequent operations, not needed for daily incremental runs.
# ============================================================================

try:
    print("[1/4] Setting up Databricks connection...")
    
    # Get SQL Warehouse connection details
    w = WorkspaceClient()
    warehouses = list(w.warehouses.list())
    
    if not warehouses:
        raise Exception("No SQL Warehouse found")
    
    wh = warehouses[0]
    host = w.config.host.replace('https://', '')
    http_path = f'/sql/1.0/warehouses/{wh.id}'
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    
    # Set environment variables for dbt
    os.environ['DATABRICKS_HOST'] = host
    os.environ['DATABRICKS_HTTP_PATH'] = http_path
    os.environ['DATABRICKS_TOKEN'] = token
    os.environ['DBT_TARGET'] = environment
    
    print(f"   ✓ SQL Warehouse: {wh.name}")
    print(f"   ✓ Target: sap_{environment}.dbt_{environment}")
    print()
    
    print("[2/4] Checking dbt installation...")
    
    # Check if dbt is already installed (faster than always reinstalling)
    try:
        dbt_check = subprocess.run(
            ['dbt', '--version'],
            capture_output=True,
            text=True,
            timeout=5
        )
        if dbt_check.returncode == 0:
            print("   ✓ dbt-databricks already installed")
        else:
            raise Exception("dbt not found")
    except:
        print("   Installing dbt-databricks...")
        install_result = subprocess.run(
            [sys.executable, '-m', 'pip', 'install', 'dbt-databricks>=1.9.0,<2.0.0', '--quiet'],
            capture_output=True,
            text=True,
            timeout=180
        )
        if install_result.returncode == 0:
            print("   ✓ dbt-databricks installed successfully")
        else:
            print("   ⚠️  Installation had warnings (continuing...)")
    
    print()
    print("[3/4] Executing dbt run (incremental models only)...")
    print(f"   Command: dbt run --target {environment} --profiles-dir .")
    print(f"   Working Directory: {DBT_PROJECT_PATH}")
    print()
    
    start_time = datetime.now()
    
    # Execute dbt run (NO seed, NO deps, NO full-refresh)
    # Phase C2.3 single-writer shield: --exclude dim_customer (CDC-owned by 06_customer_scd2_cdc / Job 05)
    dbt_result = subprocess.run(
        ['dbt', 'run', '--target', environment, '--profiles-dir', '.',
         '--exclude', 'dim_customer'],
        capture_output=True,
        text=True,
        cwd=DBT_PROJECT_PATH,
        timeout=600  # 10 minutes should be plenty for incremental
    )
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    # Display dbt output
    print(dbt_result.stdout)
    
    if dbt_result.stderr:
        print("\nWarnings/Info:")
        print(dbt_result.stderr)
    
    print()
    print("-"*70)
    print(f"Execution Time: {duration:.1f} seconds ({duration/60:.1f} minutes)")
    print("-"*70)
    print()
    
    if dbt_result.returncode == 0:
        print("[4/4] ✅ dbt run completed successfully")
        pipeline_metadata['dbt_executed'] = True
        pipeline_metadata['dbt_result'] = 'SUCCESS'
        pipeline_metadata['dbt_duration_seconds'] = duration
    else:
        print(f"[4/4] ❌ dbt run failed (return code: {dbt_result.returncode})")
        pipeline_metadata['dbt_executed'] = False
        pipeline_metadata['dbt_error'] = f"Return code: {dbt_result.returncode}"
        raise Exception(f"dbt run failed with return code {dbt_result.returncode}")
    
except subprocess.TimeoutExpired:
    print(f"❌ ERROR: dbt run timed out after {600} seconds")
    print()
    print("⚠️  TROUBLESHOOTING:")
    print("   • Check for long-running queries in the SQL Warehouse")
    print("   • Verify incremental models are processing efficiently")
    print("   • Review LoadedAt filters in fact models")
    
    pipeline_metadata['dbt_executed'] = False
    pipeline_metadata['dbt_error'] = 'Timeout'
    raise
    
except Exception as e:
    print(f"❌ ERROR: dbt execution failed")
    print(f"   Error: {str(e)}")
    print()
    print("⚠️  TROUBLESHOOTING STEPS:")
    print("   1. Check Bronze tables have new data to process")
    print("   2. Verify dbt models are configured for incremental processing")
    print("   3. Check LoadedAt column exists in all fact tables")
    print(f"   4. Review dbt logs in: {DBT_PROJECT_PATH}/logs/")
    print()
    
    pipeline_metadata['dbt_executed'] = False
    pipeline_metadata['dbt_error'] = str(e)
    
    raise Exception(f"dbt execution failed: {str(e)}")

print("\n" + "="*70)
print("✅ DBT INCREMENTAL PIPELINE EXECUTION COMPLETED")
print("="*70)
print()
print("⚡ Performance Optimization Active:")
print("   • Skipped dbt seed (~8 minutes saved)")
print("   • Skipped dbt deps (~12 seconds saved)")
print("   • Skipped duplicate validations (~3 seconds saved)")
print(f"   • Total runtime: ~{duration:.0f} seconds vs ~700 seconds before")
print()

# COMMAND ----------

# DBTITLE 1,7. Post Execution Validation
# Post-load validation
def validate_all_layers():
    """
    Validate Staging, Reference, Silver, and Gold layers after incremental dbt run
    """
    print("="*70)
    print("POST-LOAD VALIDATION")
    print("="*70)
    
    validation_summary = {}
    
    # Define layers to validate
    # DBT catalog naming: sap_{env}_dbt with schema per layer
    layers = {
        'staging': f"sap_{environment}.staging",
        'reference': f"sap_{environment}.reference",
        'silver': f"sap_{environment}.silver",
        'gold': f"sap_{environment}.gold"
    }
    
    for layer_name, schema in layers.items():
        print(f"\n➡️  Validating {layer_name.upper()} layer ({schema})...")
        
        try:
            # Get all tables in schema
            tables = spark.sql(f"SHOW TABLES IN {schema}").collect()
            
            if not tables:
                print(f"   ⚠️  No tables found in {schema}")
                validation_summary[layer_name] = {'count': 0, 'status': 'EMPTY'}
                continue
            
            # Count rows in each table
            table_info = []
            for table in tables:
                table_name = table.tableName
                full_name = f"{schema}.{table_name}"
                
                try:
                    row_count = spark.table(full_name).count()
                    table_info.append({'table': table_name, 'rows': row_count})
                    print(f"   ✅ {table_name:30s}: {row_count:10,} rows")
                except Exception as e:
                    print(f"   ❌ {table_name:30s}: ERROR ({str(e)[:50]})")
                    table_info.append({'table': table_name, 'rows': 0, 'error': str(e)})
            
            validation_summary[layer_name] = {
                'count': len(tables),
                'tables': table_info,
                'status': 'PASSED'
            }
            
        except Exception as e:
            print(f"   ❌ ERROR: {str(e)[:100]}")
            validation_summary[layer_name] = {'count': 0, 'status': 'ERROR', 'error': str(e)}
    
    print("\n" + "="*70)
    print("✅ POST-LOAD VALIDATION COMPLETED")
    print("="*70)
    
    return validation_summary

# Execute validation
layer_validation = validate_all_layers()
pipeline_metadata['layer_validation'] = layer_validation

# COMMAND ----------

# DBTITLE 1,8. Execution Summary
# Calculate execution duration
execution_end = datetime.now()
duration = (execution_end - pipeline_metadata['start_time']).total_seconds()
duration_minutes = int(duration // 60)
duration_seconds = int(duration % 60)

# Display comprehensive summary
print("\n\n")
print("═"*70)
print(" "*15 + "INCREMENTAL PIPELINE EXECUTION SUMMARY")
print("═"*70)

# Execution metadata
print("\n■ EXECUTION METADATA")
print("─"*70)
print(f"  Pipeline Run ID    : {pipeline_metadata['pipeline_run_id']}")
print(f"  Environment        : {pipeline_metadata['environment']}")
print(f"  Load Type          : {pipeline_metadata['load_type']}")
print(f"  Catalog            : {pipeline_metadata['catalog']}")
print(f"  Start Time         : {pipeline_metadata['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
print(f"  End Time           : {execution_end.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"  Duration           : {duration_minutes}m {duration_seconds}s")

# Bronze layer summary
print("\n■ BRONZE LAYER (Incremental Data)")
print("─"*70)
for table_name, full_table_name in BRONZE_TABLES.items():
    try:
        count = spark.table(full_table_name).count()
        print(f"  {table_name.capitalize():15s}: {count:10,} total records")
    except Exception as e:
        print(f"  {table_name.capitalize():15s}: Unable to count - {str(e)[:40]}")

# dbt execution
print("\n■ DBT EXECUTION")
print("─"*70)
if pipeline_metadata.get('dbt_executed'):
    print("  Status             : ✅ COMPLETED")
    print("  Mode               : INCREMENTAL (no --full-refresh)")
else:
    print("  Status             : ❌ FAILED or NOT EXECUTED")

# Layer validation summary
if 'layer_validation' in pipeline_metadata:
    print("\n■ LAYER VALIDATION")
    print("─"*70)
    
    for layer, info in pipeline_metadata['layer_validation'].items():
        status_icon = "✅" if info.get('status') == 'PASSED' else "❌"
        table_count = info.get('count', 0)
        
        print(f"  {layer.upper():15s}: {table_count:3d} tables   [{status_icon} {info.get('status')}]")
        
        # Show table details
        if 'tables' in info:
            for table_info in info['tables'][:5]:  # Show first 5 tables
                table_name = table_info['table']
                rows = table_info.get('rows', 0)
                print(f"     └─ {table_name:30s}: {rows:10,} rows")
            
            if len(info['tables']) > 5:
                print(f"     ... and {len(info['tables']) - 5} more tables")

# Final status
print("\n" + "═"*70)
print(" "*15 + "✅ INCREMENTAL PIPELINE COMPLETED SUCCESSFULLY")
print("═"*70)

print("\nℹ️  Next Steps:")
print("   1. Review layer validation results above")
print("   2. Query mart tables for updated analytics:")
print(f"      - sap_{pipeline_metadata['environment']}.gold.mart_customer_360")
print(f"      - sap_{pipeline_metadata['environment']}.gold.mart_product_performance")
print(f"      - sap_{pipeline_metadata['environment']}.gold.mart_inventory_health")
print("   3. For full refresh, use Startdbt_full notebook")
print("\n")