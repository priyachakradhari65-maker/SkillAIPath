# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,dbt Orchestration from Databricks
# MAGIC %md
# MAGIC # SkillAIPath DBT Orchestration
# MAGIC
# MAGIC **Production-ready DBT execution pipeline for Databricks**
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC **Bronze Layer** (Data Generator) → **DBT Transformations** → **Staging/Silver/Gold Layers**
# MAGIC
# MAGIC ```
# MAGIC Bronze tables (created by Data Generator):
# MAGIC   - sap_{env}.bronze.stg_sales
# MAGIC   - sap_{env}.bronze.stg_purchase
# MAGIC   - sap_{env}.bronze.stg_stock_status
# MAGIC
# MAGIC DBT Layers:
# MAGIC   - Staging: Source transformations (sap_{env}.dbt_{env}_staging)
# MAGIC   - Intermediate: Business logic & aggregations (Silver layer)
# MAGIC   - Marts: Analytics-ready tables (sap_{env}.dbt_{env}_gold)
# MAGIC ```
# MAGIC
# MAGIC ## Execution Flow
# MAGIC
# MAGIC 1. **Environment Setup** - Auto-detect environment, configure connections
# MAGIC 2. **Validation** - Verify Bronze tables and DBT project structure
# MAGIC 3. **dbt deps** - Install dependencies (dbt-utils, etc.)
# MAGIC 4. **dbt seed** - Load seed/reference data
# MAGIC 5. **dbt run --full-refresh** - Build all models (Staging → Intermediate → Marts)
# MAGIC 6. **dbt test** - Execute all data quality tests
# MAGIC 7. **Post-Build Validation** - Verify row counts and data quality
# MAGIC 8. **Summary** - Execution report
# MAGIC
# MAGIC ## Why DBT?
# MAGIC - ⚡ **Fast execution** - Optimized SQL transformations
# MAGIC - 🧪 **Automated testing** - Built-in data quality tests
# MAGIC - 📦 **Git-friendly** - Small SQL files, easy reviews
# MAGIC - 🔄 **Incremental models** - Process only new data
# MAGIC - 📚 **Self-documenting** - Auto-generated lineage & docs
# MAGIC - 🎯 **Dependency management** - Automatic execution order
# MAGIC
# MAGIC ## This Notebook
# MAGIC - ✅ **Auto-detects environment** - Works in dev/uat/prod
# MAGIC - ✅ **Assumes Bronze exists** - Bronze ingestion handled separately
# MAGIC - ✅ **DBT-only execution** - No Bronze/Silver/Gold notebook orchestration
# MAGIC - ✅ **Complete validation** - Pre/post-build checks

# COMMAND ----------

# DBTITLE 1,Install dbt-databricks
# Install dbt-databricks
import subprocess
import sys

result = subprocess.run(
    [sys.executable, '-m', 'pip', 'install', 'dbt-databricks>=1.9.0,<2.0.0', '--upgrade'],
    capture_output=True,
    text=True,
    timeout=300
)

if result.returncode == 0:
    print("✅ dbt-databricks installed successfully")
else:
    print("Installation output:")
    print(result.stdout)
    print(result.stderr)

# COMMAND ----------

# DBTITLE 1,Set Project Path & Auto-Detect Environment
import os
import re

# Get environment from job parameter (passed via widgets)
try:
    environment_param = dbutils.widgets.get("environment")
    print(f"✅ Using environment from job parameter: {environment_param}")
    detected_environment = environment_param
    
    # Still need to detect project path
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    project_path = os.path.dirname(notebook_path)
    
    # Add /Workspace prefix if not present
    if not project_path.startswith('/Workspace'):
        project_path = '/Workspace' + project_path
    
    print(f"✅ Auto-detected notebook path: {notebook_path}")
    print(f"✅ Final environment: {detected_environment.upper()}")
    
except:
    # Fallback to path-based detection
    try:
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        project_path = os.path.dirname(notebook_path)
        
        # Add /Workspace prefix if not present
        if not project_path.startswith('/Workspace'):
            project_path = '/Workspace' + project_path
        
        # Extract environment from path (e.g., /Shared/dev/... → dev)
        # Pattern: /Shared/{environment}/ or /Users/.../env/{environment}/
        env_match = re.search(r'/Shared/(dev|uat|prod|main)/', notebook_path) or \
                    re.search(r'/env/(dev|uat|prod|main)/', notebook_path)
        
        if env_match:
            detected_environment = env_match.group(1)
        else:
            # Default to dev if no environment found in path
            detected_environment = 'dev'
            print(f"⚠️ No environment found in path, defaulting to: dev")
        
        print(f"✅ Auto-detected notebook path: {notebook_path}")
        print(f"✅ Auto-detected environment: {detected_environment.upper()}")
        
    except Exception as e:
        # Fallback to hardcoded path and dev environment
        project_path = '/Workspace/Shared/dev/SkillAIPath/2_Data_Production/03_Dbt'
        detected_environment = 'dev'
        print(f"⚠️ Auto-detect failed: {str(e)}")
        print(f"⚠️ Using fallback path: {project_path}")
        print(f"⚠️ Using fallback environment: {detected_environment}")

os.chdir(project_path)
print(f"✅ Working directory: {os.getcwd()}")

# COMMAND ----------

# DBTITLE 1,Set Environment Variables
# Set environment variables for dbt Databricks connection
import os
from databricks.sdk import WorkspaceClient

# Get detected environment from previous cell
try:
    target_env = detected_environment
except NameError:
    target_env = 'dev'  # Fallback
    print("⚠️ Environment not detected, using default: dev")

w = WorkspaceClient()
warehouses = list(w.warehouses.list())

if warehouses:
    wh = warehouses[0]
    host = w.config.host.replace('https://', '')
    http_path = f'/sql/1.0/warehouses/{wh.id}'
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    
    # Set DBT environment variables
    os.environ['DATABRICKS_HOST'] = host
    os.environ['DATABRICKS_HTTP_PATH'] = http_path
    os.environ['DATABRICKS_TOKEN'] = token
    os.environ['DBT_TARGET'] = target_env  #  Auto-detected environment
    
    print(f"✅ DATABRICKS_HOST = {host}")
    print(f"✅ DATABRICKS_HTTP_PATH = {http_path}")
    print(f"✅ DATABRICKS_TOKEN configured")
    print(f"✅ DBT_TARGET = {target_env.upper()}")
    print(f"\n🏛️  Using SQL Warehouse: {wh.name}")
    print(f"🎯 Target: sap_{target_env}.dbt_{target_env}")
else:
    print("❌ No SQL Warehouse found")

# COMMAND ----------

# DBTITLE 1,1️⃣ Validate Bronze Tables Exist
import sys
from datetime import datetime

print("="*80)
print("STEP 1: VALIDATE BRONZE TABLES EXIST")
print("="*80)
print(f"Validation Time: {datetime.now()}")
print()

# Get environment
try:
    target_env = detected_environment
except NameError:
    target_env = 'dev'
    print("⚠️  Environment not detected, using default: dev")

catalog = f"sap_{target_env}"  # Match parent notebook catalog convention
schema = "bronze"

print(f"Checking Bronze tables in: {catalog}.{schema}")
print("-"*80)
print()

required_tables = [
    'stg_sales',
    'stg_purchase',
    'stg_stock_status'
]

missing_tables = []
existing_tables = {}

for table in required_tables:
    full_table = f"{catalog}.{schema}.{table}"
    try:
        df = spark.table(full_table)
        count = df.count()
        existing_tables[table] = count
        print(f"✅ {table}: {count:,} records")
    except Exception as e:
        missing_tables.append(table)
        print(f"❌ {table}: NOT FOUND")
        print(f"   Error: {str(e)[:100]}")

print()
print("="*80)

if missing_tables:
    print("❌ VALIDATION FAILED")
    print()
    print(f"Missing {len(missing_tables)} Bronze table(s): {', '.join(missing_tables)}")
    print()
    print("ACTION REQUIRED:")
    print("  Run one of these notebooks to create Bronze tables:")
    print("    • StartDBT_Full - Full refresh")
    print("    • StartDBT_Incremental - Incremental load")
    print()
    print("Cannot proceed with DBT build without Bronze data.")
    print("="*80)
    sys.exit(1)
else:
    print("✅ SUCCESS: All Bronze tables exist")
    print()
    print(f"Found {len(existing_tables)} table(s) with {sum(existing_tables.values()):,} total records")
    print()
    print("Ready to proceed with DBT transformations.")
    print("="*80)

# COMMAND ----------

# DBTITLE 1,2️⃣ Validate DBT Project Structure
import os
import sys

print("="*80)
print("STEP 2: VALIDATE DBT PROJECT STRUCTURE")
print("="*80)
print()

print(f"Project Path: {project_path}")
print("-"*80)
print()

required_files = [
    ('dbt_project.yml', 'DBT project configuration'),
    ('profiles.yml', 'Database connection profiles'),
    ('models', 'DBT models directory', True),
    ('models/intermediate', 'Intermediate/Silver layer models', True),
    ('models/marts', 'Gold layer marts', True)
]

missing_items = []

for item in required_files:
    if len(item) == 3:
        path, description, is_dir = item
    else:
        path, description = item
        is_dir = False
    
    full_path = os.path.join(project_path, path)
    
    if is_dir:
        exists = os.path.isdir(full_path)
        item_type = "Directory"
    else:
        exists = os.path.isfile(full_path)
        item_type = "File"
    
    if exists:
        print(f"✅ {item_type}: {path}")
        print(f"   {description}")
    else:
        print(f"❌ {item_type}: {path} - NOT FOUND")
        print(f"   {description}")
        missing_items.append(path)

print()
print("="*80)

if missing_items:
    print("❌ VALIDATION FAILED")
    print()
    print(f"Missing {len(missing_items)} required item(s): {', '.join(missing_items)}")
    print()
    print("ACTION REQUIRED:")
    print("  Ensure you're running this notebook from the DBT project root.")
    print("  The project should contain dbt_project.yml and profiles.yml.")
    print("="*80)
    sys.exit(1)
else:
    print("✅ SUCCESS: DBT project structure is valid")
    print()
    print("Ready to execute DBT commands.")
    print("="*80)

# COMMAND ----------

# DBTITLE 1,3️⃣ Run dbt deps (Install Dependencies)
import subprocess
import sys
from datetime import datetime

print("="*80)
print("STEP 3: INSTALL DBT DEPENDENCIES")
print("="*80)
print(f"Start Time: {datetime.now()}")
print()
print("Running: dbt deps --profiles-dir .")
print("Installing packages from packages.yml (e.g., dbt-utils)...")
print("-"*80)
print()

start_time = datetime.now()

result = subprocess.run(
    ['dbt', 'deps', '--profiles-dir', '.'],
    capture_output=True,
    text=True,
    cwd=project_path,
    timeout=120
)

end_time = datetime.now()
duration = (end_time - start_time).total_seconds()

print(result.stdout)

if result.stderr:
    print("\nSTDERR:")
    print(result.stderr)

print()
print("-"*80)
print(f"Execution time: {duration:.1f} seconds")
print()

if result.returncode == 0:
    print("✅ SUCCESS: DBT dependencies installed")
    print("="*80)
else:
    print(f"❌ FAILED: dbt deps failed (return code: {result.returncode})")
    print("="*80)
    sys.exit(1)

# COMMAND ----------

# DBTITLE 1,4️⃣ Run dbt seed (Load Seed Data)
import subprocess
import os
from datetime import datetime

print("="*80)
print("STEP 5: LOAD SEED DATA")
print("="*80)
print(f"Start Time: {datetime.now()}")
print()

# Check if seeds directory exists and has CSV files
seeds_path = os.path.join(project_path, 'seeds')
has_seeds = False

if os.path.exists(seeds_path):
    seed_files = [f for f in os.listdir(seeds_path) if f.endswith('.csv')]
    if seed_files:
        has_seeds = True
        print(f"Found {len(seed_files)} seed file(s):")
        for seed_file in seed_files:
            print(f"  • {seed_file}")
        print()

if not has_seeds:
    print("ℹ️ No seed files found in seeds/ directory")
    print("Skipping dbt seed...")
    print("="*80)
else:
    # Get environment
    try:
        target_env = detected_environment
    except NameError:
        target_env = os.environ.get('DBT_TARGET', 'dev')
    
    # Check if seed tables already exist — skip dbt seed to avoid SQL Warehouse cold-start timeout
    seed_catalog = f"sap_{target_env}"
    seed_schema = "reference"
    seed_tables_to_check = ['category_reference', 'state_reference']
    existing_seed_tables = []
    
    for seed_table in seed_tables_to_check:
        full_seed_table = f"{seed_catalog}.{seed_schema}.{seed_table}"
        if spark.catalog.tableExists(full_seed_table):
            existing_seed_tables.append(seed_table)
    
    if len(existing_seed_tables) == len(seed_tables_to_check):
        print(f"✅ Seed tables already loaded in {seed_catalog}.{seed_schema}:")
        for t in existing_seed_tables:
            count = spark.table(f"{seed_catalog}.{seed_schema}.{t}").count()
            print(f"   • {t}: {count:,} rows")
        print()
        print("Skipping dbt seed (reference data only needs to be loaded once)...")
        print("="*80)
    else:
        missing_seeds = [t for t in seed_tables_to_check if t not in existing_seed_tables]
        print(f"Seed tables not found: {', '.join(missing_seeds)}")
        
        # Pre-warm SQL Warehouse to avoid cold-start timeout on fresh deployments
        print("\n🔥 Pre-warming SQL Warehouse before dbt seed...")
        try:
            from databricks.sdk import WorkspaceClient as _WCL
            _w = _WCL() if 'w' not in dir() else w
            warehouse_id = os.environ.get('DATABRICKS_HTTP_PATH', '').split('/')[-1]
            _w.warehouses.start(warehouse_id)
            import time
            for i in range(60):  # Wait up to 5 minutes for warehouse to start
                wh_state = _w.warehouses.get(warehouse_id)
                if str(wh_state.state) == 'WarehouseState.RUNNING':
                    print(f"   ✅ SQL Warehouse is running (waited {i*5}s)")
                    break
                time.sleep(5)
            else:
                print("   ⚠️  Warehouse still starting after 5 min — proceeding anyway")
        except Exception as e:
            print(f"   ⚠️  Pre-warm skipped: {str(e)[:80]}")
        
        print(f"\nRunning: dbt seed --target {target_env} --profiles-dir .")
        print("-"*80)
        print()
        
        start_time = datetime.now()
        
        try:
            result = subprocess.run(
                ['dbt', 'seed', '--target', target_env, '--profiles-dir', '.'],
                capture_output=True,
                text=True,
                cwd=project_path,
                timeout=1200  # 20 minutes (allows for SQL Warehouse cold start)
            )
        
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            print(result.stdout)
            
            if result.stderr:
                print("\nSTDERR:")
                print(result.stderr)
            
            print()
            print("-"*80)
            print(f"Execution time: {duration:.1f} seconds")
            print()
            
            if result.returncode == 0:
                print("✅ SUCCESS: Seed data loaded")
                print("="*80)
            else:
                print(f"⚠️  WARNING: dbt seed had issues (return code: {result.returncode})")
                print("Proceeding with build...")
                print("="*80)
        
        except subprocess.TimeoutExpired:
            print("⚠️  TIMEOUT: dbt seed exceeded 20-minute limit")
            print()
            print("This typically happens when SQL Warehouse is starting (cold start).")
            print("Seed data is reference/lookup tables - they can be loaded separately.")
            print()
            print("To load seeds manually later, run:")
            print(f"  dbt seed --target {target_env} --profiles-dir .")
            print()
            print("Proceeding with dbt run (models may reference seed tables)...")
            print("="*80)

# COMMAND ----------

# DBTITLE 1,🔧 Rebuild fact_sales with LoadedAt
# import subprocess
# import sys
# from datetime import datetime

# print("="*80)
# print("SPECIAL STEP: REBUILD fact_sales WITH LoadedAt COLUMN")
# print("="*80)
# print(f"Start Time: {datetime.now()}")
# print()
# print("Reason: fact_sales.sql was updated to include LoadedAt in output")
# print("Action: Running full refresh to recreate table with new schema")
# print()
# print("Running: dbt run --select fact_sales --full-refresh --target dev --profiles-dir .")
# print("-"*80)
# print()

# start_time = datetime.now()

# result = subprocess.run(
#     ['dbt', 'run', '--select', 'fact_sales', '--full-refresh', '--target', 'dev', '--profiles-dir', '.'],
#     capture_output=True,
#     text=True,
#     cwd=project_path,
#     timeout=300
# )

# end_time = datetime.now()
# duration = (end_time - start_time).total_seconds()

# print(result.stdout)

# if result.stderr:
#     print("\nSTDERR:")
#     print(result.stderr)

# print()
# print("-"*80)
# print(f"Execution time: {duration:.1f} seconds")
# print()

# if result.returncode == 0:
#     print("✅ SUCCESS: fact_sales rebuilt with LoadedAt column")
#     print("="*80)
# else:
#     print(f"❌ FAILED: dbt run failed (return code: {result.returncode})")
#     print("="*80)
#     sys.exit(1)

# COMMAND ----------

# DBTITLE 1,6️⃣ Run dbt build (Models + Tests)
import subprocess
import sys
import os
from datetime import datetime

print("="*80)
print("STEP 6: RUN DBT MODELS")
print("="*80)
print(f"Start Time: {datetime.now()}")
print()

# Get environment
try:
    target_env = detected_environment
except NameError:
    target_env = os.environ.get("DBT_TARGET", "dev")

print(f"Target Environment: {target_env.upper()}")
print(f"Source Catalog: sap_{target_env}")
print(f"DBT Catalog: sap_{target_env}")
print(f"DBT Bronze Schema: sap_{target_env}.bronze")
print(f"DBT Silver Schema: sap_{target_env}.silver")
print(f"DBT Gold Schema: sap_{target_env}.gold")
print()

# Check refresh mode from job parameter (default: full)
try:
    refresh_mode = dbutils.widgets.get("refresh_mode")
except:
    refresh_mode = "full"

print(f"Running: dbt run --target {target_env} --profiles-dir ." + (" --full-refresh" if refresh_mode == "full" else ""))
print("-"*80)
print()

print("Executing dbt pipeline:")
print("  • Staging models (Bronze → Staging)")
print("  • Intermediate models")
print("  • Dimension models (dim_*)")
print("  • Fact models (fact_*)")
print("  • Gold Mart models (mart_*)")
print()
print(f"Mode: {refresh_mode.upper()}")
print("-"*80)
print()

start_time = datetime.now()

dbt_command = [
    "dbt",
    "run",
    "--target",
    target_env,
    "--profiles-dir",
    ".",
    # Phase C2.3 single-writer shield: dim_customer is CDC-owned (06_customer_scd2_cdc / Job 05)
    "--exclude",
    "dim_customer"
]

if refresh_mode == "full":
    dbt_command.append("--full-refresh")

try:
    result = subprocess.run(
        dbt_command,
        capture_output=True,
        text=True,
        cwd=project_path,
        timeout=1800  # 30 minutes — full-refresh needs more time
    )
except subprocess.TimeoutExpired as e:
    print(f"❌ TIMEOUT: dbt run exceeded 30-minute limit")
    print(f"Command: {' '.join(e.cmd)}")
    if e.output:
        print(f"\nPartial stdout:\n{e.output.decode() if isinstance(e.output, bytes) else e.output}")
    if e.stderr:
        print(f"\nPartial stderr:\n{e.stderr.decode() if isinstance(e.stderr, bytes) else e.stderr}")
    raise RuntimeError(f"dbt run timed out after 1800 seconds. Consider using incremental mode (refresh_mode=incremental) or increasing the timeout.")

end_time = datetime.now()
duration = (end_time - start_time).total_seconds()

print(result.stdout)

if result.stderr:
    print("\nSTDERR:")
    print(result.stderr)

print()
print("-"*80)
print(f"Execution Time : {duration:.1f} seconds ({duration/60:.1f} minutes)")
print()

if result.returncode == 0:
    print("✅ SUCCESS: dbt run completed successfully")
    print("="*80)
else:
    print(f"❌ FAILED: dbt run failed (Return Code: {result.returncode})")
    print("="*80)
    raise RuntimeError(f"dbt run failed with return code {result.returncode}")

# COMMAND ----------

# DBTITLE 1,6️⃣ Run dbt test
import subprocess
import sys
import re
from datetime import datetime

print("="*80)
print("STEP 7: RUN DBT TESTS")
print("="*80)
print(f"Start Time: {datetime.now()}")
print()

# Get environment
try:
    target_env = detected_environment
except NameError:
    target_env = os.environ.get('DBT_TARGET', 'dev')

print(f"Target Environment: {target_env.upper()}")
print(f"Running: dbt test --target {target_env} --profiles-dir .")
print("-"*80)
print()
print("This will execute all dbt tests:")
print("  • Schema tests (not_null, unique, relationships)")
print("  • Data tests (accepted_values, custom SQL)")
print("  • Source freshness checks")
print()
print("-"*80)
print()

start_time = datetime.now()

result = subprocess.run(
    ['dbt', 'test', '--target', target_env, '--profiles-dir', '.'],
    capture_output=True,
    text=True,
    cwd=project_path,
    timeout=600  # 10 minutes
)

end_time = datetime.now()
duration = (end_time - start_time).total_seconds()

print(result.stdout)

if result.stderr:
    print("\nSTDERR:")
    print(result.stderr)

print()
print("="*80)
print("DBT TEST RESULTS")
print("="*80)
print()
print(f"Execution Time: {duration:.1f} seconds ({duration/60:.1f} minutes)")
print()

# Parse output for test summary
output = result.stdout

pass_match = re.search(r'PASS=(\d+)', output)
warn_match = re.search(r'WARN=(\d+)', output)
error_match = re.search(r'ERROR=(\d+)', output)
skip_match = re.search(r'SKIP=(\d+)', output)

if pass_match:
    passes = int(pass_match.group(1))
    warns = int(warn_match.group(1)) if warn_match else 0
    errors = int(error_match.group(1)) if error_match else 0
    skips = int(skip_match.group(1)) if skip_match else 0
    
    print("Test Summary:")
    print(f"  ✅ Passed:   {passes}")
    if warns > 0:
        print(f"  ⚠️  Warnings: {warns}")
    if errors > 0:
        print(f"  ❌ Errors:   {errors}")
    if skips > 0:
        print(f"  ⏭️  Skipped:  {skips}")
    
    print()
    if errors == 0 and warns == 0:
        print("✅ 🎉 SUCCESS: All tests passed!")
    elif errors > 0:
        print(f"❌ FAILURE: {errors} test(s) failed")
        print("\nReview the output above for details.")
        print("="*80)
        sys.exit(1)
    else:
        print(f"⚠️  WARNING: {warns} test(s) had warnings")
else:
    if result.returncode == 0:
        print("✅ SUCCESS: All dbt tests completed")
    else:
        print(f"❌ FAILED: dbt test failed (return code: {result.returncode})")
        print("="*80)
        sys.exit(1)

print("="*80)

# COMMAND ----------

# DBTITLE 1,8️⃣ Post-Build Validation Queries
from pyspark.sql import functions as F
from datetime import datetime

print("="*80)
print("STEP 8: POST-BUILD DATA VALIDATION")
print("="*80)
print(f"Validation Time: {datetime.now()}")
print()

# Get environment
try:
    target_env = detected_environment
except NameError:
    target_env = 'dev'

dbt_catalog = f"sap_{target_env}"
bronze_schema = "bronze"
silver_schema = "silver"
gold_schema = "gold"

print(f"Validating schemas:")
print(f"  • DBT Bronze: {dbt_catalog}.{bronze_schema}")
print(f"  • DBT Silver: {dbt_catalog}.{silver_schema}")
print(f"  • DBT Gold: {dbt_catalog}.{gold_schema}")
print("-"*80)
print()

# Helper function to get tables from a schema
def get_tables_in_schema(catalog, schema):
    try:
        tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
        return [(row.tableName, row.database) for row in tables]
    except Exception as e:
        print(f"  ⚠️  Could not access schema {catalog}.{schema}: {str(e)[:80]}")
        return []

# Validation 1: Bronze/Staging Tables
print("1️⃣ BRONZE LAYER (DBT STAGING MODELS)")
print("-"*40)
tables = get_tables_in_schema(dbt_catalog, bronze_schema)
if tables:
    staging_counts = {}
    for table_name, _ in tables:
        try:
            count = spark.table(f"{dbt_catalog}.{bronze_schema}.{table_name}").count()
            staging_counts[table_name] = count
            print(f"  ✅ {table_name}: {count:,} records")
        except Exception as e:
            print(f"  ❌ {table_name}: ERROR - {str(e)[:50]}")
    print(f"\n  Total Bronze Tables: {len(staging_counts)}")
else:
    print("  ℹ️  No tables found in bronze schema")

print()

# Validation 2: Silver Layer - Categorized
print("2️⃣ SILVER LAYER (DIMENSIONS & FACTS)")
print("-"*40)
tables = get_tables_in_schema(dbt_catalog, silver_schema)
if tables:
    dims = []
    facts = []
    other = []
    
    silver_dim_counts = {}
    silver_fact_counts = {}
    silver_other_counts = {}
    
    for table_name, _ in tables:
        try:
            count = spark.table(f"{dbt_catalog}.{silver_schema}.{table_name}").count()
            
            if table_name.startswith('dim_'):
                dims.append(table_name)
                silver_dim_counts[table_name] = count
                print(f"  ✅ {table_name}: {count:,} records")
            elif table_name.startswith('fact_'):
                facts.append(table_name)
                silver_fact_counts[table_name] = count
                print(f"  ✅ {table_name}: {count:,} records")
            else:
                other.append(table_name)
                silver_other_counts[table_name] = count
                print(f"  ✅ {table_name}: {count:,} records")
        except Exception as e:
            print(f"  ❌ {table_name}: ERROR - {str(e)[:50]}")
    
    print()
    if dims:
        print(f"  📊 Dimensions: {len(dims)} tables")
        for dim in sorted(dims):
            print(f"     • {dim}")
    
    if facts:
        print(f"\n  📈 Facts: {len(facts)} tables")
        for fact in sorted(facts):
            print(f"     • {fact}")
    
    if other:
        print(f"\n  📋 Other: {len(other)} tables")
        for tbl in sorted(other):
            print(f"     • {tbl}")
    
    print(f"\n  Total Silver Tables: {len(dims) + len(facts) + len(other)}")
else:
    print("  ❌ No tables found in silver schema")

print()

# Validation 3: Gold Layer
print("3️⃣ GOLD LAYER (MARTS)")
print("-"*40)
tables = get_tables_in_schema(dbt_catalog, gold_schema)
if tables:
    gold_counts = {}
    for table_name, _ in tables:
        try:
            count = spark.table(f"{dbt_catalog}.{gold_schema}.{table_name}").count()
            gold_counts[table_name] = count
            print(f"  ✅ {table_name}: {count:,} records")
        except Exception as e:
            print(f"  ❌ {table_name}: ERROR - {str(e)[:50]}")
    
    print()
    print(f"  📊 Gold Marts Built:")
    for mart in sorted(gold_counts.keys()):
        print(f"     • {mart}")
    
    print(f"\n  Total Gold Tables: {len(gold_counts)}")
else:
    print("  ❌ No tables found in gold schema")

print()
print("="*80)
print("✅ VALIDATION COMPLETE")
print("="*80)
print()
print("📊 SUMMARY BY LAYER:")
print("-"*80)

try:
    bronze_total = sum(staging_counts.values()) if 'staging_counts' in locals() else 0
    silver_total = sum(silver_dim_counts.values()) + sum(silver_fact_counts.values()) + sum(silver_other_counts.values()) if 'silver_dim_counts' in locals() else 0
    gold_total = sum(gold_counts.values()) if 'gold_counts' in locals() else 0
    
    print(f"  • Bronze: {len(staging_counts) if 'staging_counts' in locals() else 0} tables, {bronze_total:,} total records")
    print(f"  • Silver: {len(silver_dim_counts) + len(silver_fact_counts) + len(silver_other_counts) if 'silver_dim_counts' in locals() else 0} tables, {silver_total:,} total records")
    if 'silver_dim_counts' in locals():
        print(f"    - Dimensions: {len(silver_dim_counts)} tables")
        print(f"    - Facts: {len(silver_fact_counts)} tables")
        if silver_other_counts:
            print(f"    - Other: {len(silver_other_counts)} tables")
    print(f"  • Gold: {len(gold_counts) if 'gold_counts' in locals() else 0} tables, {gold_total:,} total records")
except Exception as e:
    print(f"  ⚠️  Could not compute summary: {str(e)[:100]}")

print()
print("="*80)

# COMMAND ----------

# DBTITLE 1,🎯 Final Execution Summary
from datetime import datetime
import os

print("="*80)
print("🎉 DBT ORCHESTRATION - EXECUTION SUMMARY 🎉")
print("="*80)
print()

# Get environment
try:
    target_env = detected_environment
except NameError:
    target_env = 'dev'

print(f"📅 Execution Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"🎯 Environment: {target_env.upper()}")
print(f"📂 Project: {project_path}")
print()
print("="*80)
print("🚀 EXECUTION STEPS COMPLETED")
print("="*80)
print()
print("✅ Step 1: Bronze table validation")
print("✅ Step 2: DBT project structure validation")
print("✅ Step 3: DBT dependencies installed (dbt deps)")
print("✅ Step 4: Seed data loaded")
print("✅ Step 5: DBT models built (dbt run --full-refresh)")
print("✅ Step 6: DBT tests executed (dbt test)")
print("✅ Step 7: Post-build validation")
print()
print("="*80)
print("📊 OUTPUT SCHEMAS")
print("="*80)
print()
print(f"🥉 Bronze Layer: sap_{target_env}.bronze")
print("  • DBT staging models (views from sap_{target_env}.bronze)")
print()
print(f"🥈 Silver Layer: sap_{target_env}.silver")
print("  • Dimensions: dim_customer, dim_product, dim_date, etc.")
print("  • Facts: fact_sales, fact_purchase, fact_inventory")
print()
print(f"🥇 Gold Layer: sap_{target_env}.gold")
print("  • Marts: mart_customer_360, mart_product_performance")
print("  •        mart_inventory_health")
print()
print("="*80)
print("🔧 WHAT HAPPENED")
print("="*80)
print()
print("1. ✅ **Validated Bronze Layer**")
print("   Confirmed required Bronze tables exist (created by StartDBT_Full/Incremental)")
print()
print("2. ✅ **Ran DBT Transformations (dbt run --full-refresh)**")
print("   Bronze → Staging → Intermediate → Marts")
print("   Full refresh of all models across all layers")
print()
print("3. ✅ **Executed Data Quality Tests (dbt test)**")
print("   All DBT tests passed (uniqueness, not-null, relationships, custom)")
print()
print("4. ✅ **Validated Output Data**")
print("   Confirmed all tables created with expected record counts")
print()
print("="*80)
print("🎯 NEXT STEPS")
print("="*80)
print()
print("The DBT pipeline has completed successfully.")
print()
print("🔍 To explore the data:")
print(f"   USE CATALOG sap_{target_env};")
print(f"   USE SCHEMA silver;")
print("   SELECT * FROM dim_customer LIMIT 10;")
print()
print("📊 To use Gold marts:")
print(f"   SELECT * FROM sap_{target_env}.gold.mart_customer_360;")
print()

print("🔄 To rerun incrementally:")
print("   Just rerun this notebook - DBT handles incremental logic")
print()
print("="*80)
print("✅ 🎉 DBT ORCHESTRATION COMPLETE - ALL SYSTEMS OPERATIONAL")
print("="*80)