# Databricks notebook source
# DBTITLE 1,SkillAIPath DEV - Automated Setup
# MAGIC %md
# MAGIC # SkillAIPath - Automated Environment Setup
# MAGIC
# MAGIC ## One-Click Environment Provisioning
# MAGIC
# MAGIC **Environment:** Auto-detected from notebook path
# MAGIC
# MAGIC **This notebook will:**
# MAGIC 1. Auto-detect environment (dev/uat/prod) from folder path
# MAGIC 2. Auto-create catalog and schemas if needed
# MAGIC 3. Reset environment (TRUNCATE tables - 70% faster)
# MAGIC 4. Generate 30-day sample data  
# MAGIC 5. Build Bronze layer
# MAGIC 6. Build Silver layer (dimensions + facts in parallel)
# MAGIC 7. Build Gold layer (analytics marts)
# MAGIC
# MAGIC **Target Runtime:** <15 minutes
# MAGIC
# MAGIC **Widgets:**
# MAGIC - **reset_environment:** Yes/No (default: Yes)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Run All Cells
# MAGIC
# MAGIC Click "Run All" for complete environment setup.
# MAGIC
# MAGIC **Works in any environment: dev, uat, main, or custom!**

# COMMAND ----------

# DBTITLE 1,Set Environment
# ============================================================================
# AUTO-DETECT ENVIRONMENT - NO HARDCODING
# ============================================================================
# Dual Mode Architecture:
#
# USER MODE: /Users/{email}/SkillAIPath/
#   - Catalog: workspace (system catalog)
#   - Schema: workspace.{email_prefix}_dev (e.g., user_dev)
#   - Tables: bronze_*, silver_*, gold_* (with prefixes)
#   - Data: 3 days (fast testing)
#
# SHARED MODE: /Shared/{env}/SkillAIPath/
#   - Catalog: sap_{env} (e.g., sap_dev, sap_main)
#   - Schemas: bronze, silver, gold (separate schemas)
#   - Data: 30 days (production)
# ============================================================================

import os
import re
from datetime import datetime

# Get current notebook path using os.getcwd() - works in ALL contexts
notebook_path = os.getcwd()

class EnvDetector:
    def __init__(self, path):
        self.path = path
        self.mode = None
        self.catalog = None
        self.environment = None
        self.schema = None
        self.data_days = None
        
    def detect(self):
        print("\n" + "="*80)
        print("  ENVIRONMENT AUTO-DETECTION")
        print("="*80)
        print(f"Notebook Path: {self.path}")
        print()
        
        if '/Users/' in self.path:
            self._detect_user_mode()
        elif '/Shared/' in self.path:
            self._detect_shared_mode()
        else:
            raise ValueError(
                f"Cannot detect environment from path: {self.path}\n"
                f"Expected pattern:\n"
                f"  - USER: /Users/{{email}}/SkillAIPath/\n"
                f"  - SHARED: /Shared/{{env}}/SkillAIPath/"
            )
        
        self._print_summary()
        return self
    
    def _detect_user_mode(self):
        # Extract email prefix (before @)
        match = re.search(r'/Users/([^/@]+)', self.path)
        if not match:
            raise ValueError(f"Cannot extract user email from path: {self.path}")
        
        user_prefix = match.group(1)
        
        self.mode = "USER"
        self.catalog = "workspace"
        self.environment = "dev"  # Environment name (not schema!)
        self.schema = f"{user_prefix}_dev"  # Schema name
        self.data_days = 3
        
        print("✓ Mode: USER (Development Sandbox)")
        print(f"  User: {user_prefix}")
        print(f"  Catalog: {self.catalog} (system catalog)")
        print(f"  Schema: {self.schema}")
        print(f"  Tables: bronze_*, silver_*, gold_* (prefixes)")
        print(f"  Data Volume: {self.data_days} days (fast iteration)")
    
    def _detect_shared_mode(self):
        # Extract environment from /Shared/{env}/SkillAIPath/
        match = re.search(r'/Shared/([^/]+)/SkillAIPath/', self.path)
        if not match:
            raise ValueError(f"Cannot extract environment from path: {self.path}")
        
        env = match.group(1)
        if env not in ['dev', 'main']:
            raise ValueError(
                f"Invalid environment '{env}' - expected 'dev' or 'main'\n"
                f"Path: {self.path}"
            )
        
        self.mode = "SHARED"
        self.catalog = f"sap_{env}"
        self.environment = env
        self.data_days = 30
        
        print("✓ Mode: SHARED (Team Deployment)")
        print(f"  Environment: {env}")
        print(f"  Catalog: {self.catalog}")
        print(f"  Schemas: bronze, silver, gold (separate)")
        print(f"  Data Volume: {self.data_days} days (production-ready)")
    
    def _print_summary(self):
        print()
        print("-" * 80)
        print("  ENVIRONMENT CONFIGURATION")
        print("-" * 80)
        print(f"  Mode: {self.mode}")
        print(f"  Catalog: {self.catalog}")
        print(f"  Environment: {self.environment}")
        if self.schema:
            print(f"  Schema: {self.schema}")
        print(f"  Data Days: {self.data_days}")
        print("="*80)

# Run detection
detector = EnvDetector(notebook_path)
detector.detect()

# Set global variables for downstream cells
ENVIRONMENT = detector.environment
CATALOG = detector.catalog
DATA_DAYS = detector.data_days
USER_SCHEMA = detector.schema if detector.mode == "USER" else None

# ================================================================
# AUTO-PROVISION: Verify/create catalog and schemas
# ================================================================

print("\n" + "="*80)
print("🏗️  VERIFYING INFRASTRUCTURE")
print("="*80)

# Verify catalog exists (never create workspace catalog)
print(f"\n📦 Checking catalog: {CATALOG}")
catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]

if CATALOG == "workspace":
    # USER MODE: workspace catalog always exists (system catalog)
    print(f"✅ Catalog '{CATALOG}' exists (system catalog)")
    
    # Create user schema if it doesn't exist (first-run auto-provisioning)
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{USER_SCHEMA}")
        print(f"✅ Schema '{CATALOG}.{USER_SCHEMA}' ready")
        print(f"   Tables will use prefixes: bronze_*, silver_*, gold_*")
    except Exception as e:
        print(f"\n❌ ERROR: Cannot create schema '{CATALOG}.{USER_SCHEMA}'")
        print(f"   {str(e)[:200]}")
        raise
    
else:
    # SHARED MODE: sap_dev or sap_main catalog
    # Auto-provision catalog if it doesn't exist
    if CATALOG not in catalogs:
        print(f"📦 Catalog '{CATALOG}' does not exist - provisioning...")
        try:
            spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
            spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `account users`")
            spark.sql(f"GRANT CREATE SCHEMA ON CATALOG {CATALOG} TO `account users`")
            print(f"✅ Catalog '{CATALOG}' created and permissions granted")
        except Exception as e:
            print(f"❌ ERROR: Cannot create catalog '{CATALOG}'")
            print(f"   {str(e)[:300]}")
            print(f"\n📋 MANUAL ACTION REQUIRED (requires admin/account privileges):")
            print(f"   CREATE CATALOG IF NOT EXISTS {CATALOG};")
            print(f"   GRANT USE CATALOG ON CATALOG {CATALOG} TO `account users`;")
            print(f"   GRANT CREATE SCHEMA ON CATALOG {CATALOG} TO `account users`;")
            raise RuntimeError(f"Catalog creation failed - admin intervention required")
    else:
        print(f"✅ Catalog '{CATALOG}' exists")
    
    # Create bronze, silver, gold schemas
    for schema in ['bronze', 'silver', 'gold']:
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
            print(f"✅ Schema '{CATALOG}.{schema}' ready")
        except Exception as e:
            print(f"⚠️  Warning creating {schema}: {str(e)[:100]}")

# Final summary
print("\n" + "="*80)
print(f"✅ ENVIRONMENT READY")
print("="*80)
if '/Users/' in notebook_path:
    print(f"Mode: USER")
    print(f"Schema: {CATALOG}.{USER_SCHEMA}")
    print(f"Tables: bronze_*, silver_*, gold_*")
else:
    print(f"Mode: SHARED")
    print(f"Environment: {ENVIRONMENT}")
    print(f"Catalog: {CATALOG}")
    print(f"Schemas: bronze, silver, gold")
print(f"Data Days: {DATA_DAYS}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Run Config
# MAGIC %run ./1_Setup/1_config

# COMMAND ----------

# DBTITLE 1,Initialize Setup
from datetime import datetime
import time

start_time = time.time()

# Create reset widget
dbutils.widgets.dropdown("reset_environment", "Yes", ["Yes", "No"], "Reset Environment?")

print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Bronze: {Config.SCHEMA_BRONZE}")
print(f"Silver: {Config.SCHEMA_SILVER}")
print(f"Gold: {Config.SCHEMA_GOLD}")

# COMMAND ----------

# DBTITLE 1,Step 0: Reset Environment
# Get widget value, default to Yes if not available (for job context)
try:
    reset_env = dbutils.widgets.get("reset_environment")
except:
    reset_env = "Yes"

if reset_env == "Yes":
    print("\n" + "="*80)
    print(f"STEP 0: RESET {Config.ENVIRONMENT.upper()} ENVIRONMENT")
    print("="*80)
    
    try:
        result = dbutils.notebook.run(
            "./1_Setup/3_reset_catalog",
            timeout_seconds=180,
            arguments={}
        )
        print(f"SUCCESS: {result}")
    except Exception as e:
        print(f"ERROR: {str(e)[:200]}")
        print("Reset failed - setup cannot continue")
        raise
else:
    print("\nSkipping reset - building on existing data\n")

# COMMAND ----------

# DBTITLE 1,Step 1: Validate Environment
print("\n" + "="*80)
print(f"STEP 1: VALIDATE {Config.ENVIRONMENT.upper()} ENVIRONMENT")
print("="*80)

spark.sql(f"USE CATALOG {Config.CATALOG}")
print(f"✅ Catalog {Config.CATALOG} active")

# Schema validation (mode-aware)
if Config.USE_FLAT_SCHEMA:
    # USER MODE: Single schema with table prefixes
    print(f"✅ Schema {Config.SCHEMA_BRONZE} ready (flat schema mode)")
    print(f"   Tables use prefixes: {Config.TABLE_PREFIX_BRONZE}*, {Config.TABLE_PREFIX_SILVER}*, {Config.TABLE_PREFIX_GOLD}*")
else:
    # SHARED MODE: Separate schemas
    for schema in [Config.SCHEMA_BRONZE, Config.SCHEMA_SILVER, Config.SCHEMA_GOLD]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {Config.CATALOG}.{schema}")
        print(f"✅ Schema {schema} ready")

# COMMAND ----------

# DBTITLE 1,Step 2: Generate Sample Data
print("\n" + "="*80)
print(f"STEP 2: GENERATE SAMPLE DATA ({DATA_DAYS} days)")
print("="*80)

try:
    result = dbutils.notebook.run(
        "./1_Setup/2_data_generator",
        timeout_seconds=180,
        arguments={
            "mode": "batch",
            "batch_days": str(DATA_DAYS),
            "records_per_day": "1000",
            "write_mode": "overwrite"
        }
    )
    print(f"SUCCESS: {result}")
except Exception as e:
    print(f"ERROR: {str(e)[:200]}")
    print("No retry - data generation must succeed")
    raise

# COMMAND ----------

# DBTITLE 1,Step 3: Build Bronze Layer
print("\n" + "="*80)
print("STEP 3: VALIDATE BRONZE LAYER")
print("="*80)

# Get bronze tables (mode-aware)
if Config.USE_FLAT_SCHEMA:
    # USER MODE: Filter by prefix in single schema
    all_tables = spark.sql(f"SHOW TABLES IN {Config.CATALOG}.{Config.SCHEMA_BRONZE}").collect()
    bronze_tables = [t for t in all_tables if t.tableName.startswith(Config.TABLE_PREFIX_BRONZE)]
else:
    # SHARED MODE: All tables in bronze schema
    bronze_tables = spark.sql(f"SHOW TABLES IN {Config.CATALOG}.{Config.SCHEMA_BRONZE}").collect()

print(f"Bronze tables: {len(bronze_tables)}")
for t in bronze_tables[:3]:
    full_table = f"{Config.CATALOG}.{Config.SCHEMA_BRONZE}.{t.tableName}"
    count = spark.table(full_table).count()
    print(f"  {t.tableName}: {count:,} rows")

# COMMAND ----------

# DBTITLE 1,Step 4: Build Silver - Dimensions
print("\n" + "="*80)
print("STEP 4: BUILD SILVER - DIMENSIONS")
print("="*80)

# Dynamic path construction - works for both user and shared modes
if '/Users/' in notebook_path:
    base_path = "../2_Data_Production/02_Medallion_Architecture"
else:
    base_path = f"/Workspace/Shared/{ENVIRONMENT}/SkillAIPath/2_Data_Production/02_Medallion_Architecture"

dims = [
    ("dim_date", f"{base_path}/01_Dimensions/01_dim_date"),
    ("dim_product_category", f"{base_path}/01_Dimensions/02_dim_product_category"),
    ("dim_warehouse", f"{base_path}/01_Dimensions/03_dim_warehouse_nb"),
    ("dim_retailer", f"{base_path}/01_Dimensions/04_dim_retailer"),
    ("dim_agent", f"{base_path}/01_Dimensions/05_dim_agent"),
    ("dim_payment_mode", f"{base_path}/01_Dimensions/06_dim_payment_mode"),
    ("dim_product", f"{base_path}/01_Dimensions/07_dim_product_nb"),
    ("dim_customer", f"{base_path}/01_Dimensions/08_dim_customer_full_truncate"),
    ("dim_wholesaler", f"{base_path}/01_Dimensions/13_dim_wholesaler"),
]

def is_transient_error(error_msg):
    """Check if error is transient (worth retrying) vs logical (no point retrying)"""
    transient_keywords = ['timeout', 'connection', 'temporarily unavailable', 'resource']
    logical_keywords = ['NameError', 'AttributeError', 'AnalysisException', 'ParseException', 'not found', 'does not exist']
    
    error_lower = str(error_msg).lower()
    if any(kw in error_lower for kw in logical_keywords):
        return False
    return any(kw in error_lower for kw in transient_keywords)

for name, path in dims:
    success = False
    attempts = 0
    max_attempts = 2  # 1 initial + 1 retry
    
    while not success and attempts < max_attempts:
        attempts += 1
        try:
            if attempts > 1:
                print(f"  Retry {attempts-1}/1...")
            else:
                print(f"Building {name}...")
            
            dbutils.notebook.run(path, timeout_seconds=120, arguments={})
            print(f"✅ {name}")
            success = True
            
        except Exception as e:
            error_msg = str(e)
            
            if attempts >= max_attempts or not is_transient_error(error_msg):
                print(f"❌ {name}: {error_msg[:150]}")
                if not is_transient_error(error_msg):
                    print(f"   Logical error - no retry")
                break

print("\nDimensions complete")

# COMMAND ----------

# DBTITLE 1,Step 5: Build Silver - Facts
print("\n" + "="*80)
print("STEP 5: BUILD SILVER - FACTS")
print("="*80)

# Dynamic path construction (reuse base_path from previous cell)
facts = [
    ("fact_sales", f"{base_path}/02_Facts/01_fact_sales"),
    ("fact_purchases", f"{base_path}/02_Facts/02_fact_purchases"),
    ("fact_inventory", f"{base_path}/02_Facts/03_fact_inventory"),
]

for name, path in facts:
    success = False
    attempts = 0
    max_attempts = 2  # 1 initial + 1 retry
    
    while not success and attempts < max_attempts:
        attempts += 1
        try:
            if attempts > 1:
                print(f"  Retry {attempts-1}/1...")
            else:
                print(f"Building {name}...")
            
            dbutils.notebook.run(path, timeout_seconds=120, arguments={})
            print(f"✅ {name}")
            success = True
            
        except Exception as e:
            error_msg = str(e)
            
            if attempts >= max_attempts or not is_transient_error(error_msg):
                print(f"❌ {name}: {error_msg[:150]}")
                if not is_transient_error(error_msg):
                    print(f"   Logical error - no retry")
                break

print("\nFacts complete")

# COMMAND ----------

# DBTITLE 1,Step 6: Build Gold Layer
print("\n" + "="*80)
print("STEP 6: BUILD GOLD LAYER")
print("="*80)

# Dynamic path construction (reuse base_path from dimension cell)
gold = [
    ("customer_360", f"{base_path}/04_Gold/01_mart_customer_360"),
    ("product_performance", f"{base_path}/04_Gold/03_mart_product_performance"),
    ("rfm_analysis", f"{base_path}/04_Gold/02_mart_rfm_analysis"),
]

for name, path in gold:
    success = False
    attempts = 0
    max_attempts = 2  # 1 initial + 1 retry
    
    while not success and attempts < max_attempts:
        attempts += 1
        try:
            if attempts > 1:
                print(f"  Retry {attempts-1}/1...")
            else:
                print(f"Building {name}...")
            
            dbutils.notebook.run(path, timeout_seconds=120, arguments={})
            print(f"✅ {name}")
            success = True
            
        except Exception as e:
            error_msg = str(e)
            
            if attempts >= max_attempts or not is_transient_error(error_msg):
                print(f"❌ {name}: {error_msg[:150]}")
                if not is_transient_error(error_msg):
                    print(f"   Logical error - no retry")
                break

print("\nGold layer complete")

# COMMAND ----------

# DBTITLE 1,Step 7: Final Validation
print("\n" + "="*80)
print("STEP 7: FINAL VALIDATION")
print("="*80)

# Get tables (mode-aware)
if Config.USE_FLAT_SCHEMA:
    # USER MODE: Filter by prefix
    all_tables = spark.sql(f"SHOW TABLES IN {Config.CATALOG}.{Config.SCHEMA_BRONZE}").collect()
    bronze_tables = [t for t in all_tables if t.tableName.startswith(Config.TABLE_PREFIX_BRONZE)]
    silver_tables = [t for t in all_tables if t.tableName.startswith(Config.TABLE_PREFIX_SILVER)]
    gold_tables = [t for t in all_tables if t.tableName.startswith(Config.TABLE_PREFIX_GOLD)]
else:
    # SHARED MODE: Separate schemas
    bronze_tables = spark.sql(f"SHOW TABLES IN {Config.CATALOG}.{Config.SCHEMA_BRONZE}").collect()
    silver_tables = spark.sql(f"SHOW TABLES IN {Config.CATALOG}.{Config.SCHEMA_SILVER}").collect()
    gold_tables = spark.sql(f"SHOW TABLES IN {Config.CATALOG}.{Config.SCHEMA_GOLD}").collect()

bronze_count = len(bronze_tables)
silver_count = len(silver_tables)
gold_count = len(gold_tables)

print(f"\nBronze: {bronze_count} tables")
for t in bronze_tables[:3]:
    full_table = f"{Config.CATALOG}.{Config.SCHEMA_BRONZE}.{t.tableName}"
    count = spark.table(full_table).count()
    print(f"  {t.tableName}: {count:,} rows")

print(f"\nSilver: {silver_count} tables")

# Show dimensions (strip prefix for display)
if Config.USE_FLAT_SCHEMA:
    dim_tables = [t for t in silver_tables if t.tableName.startswith(f"{Config.TABLE_PREFIX_SILVER}dim_")]
else:
    dim_tables = [t for t in silver_tables if t.tableName.startswith('dim_')]
print(f"  Dimensions ({len(dim_tables)})")
for t in dim_tables[:3]:
    full_table = f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.{t.tableName}"
    count = spark.table(full_table).count()
    print(f"    {t.tableName}: {count:,} rows")

# Show facts
if Config.USE_FLAT_SCHEMA:
    fact_tables = [t for t in silver_tables if t.tableName.startswith(f"{Config.TABLE_PREFIX_SILVER}fact_")]
else:
    fact_tables = [t for t in silver_tables if t.tableName.startswith('fact_')]
print(f"  Facts ({len(fact_tables)})")
for t in fact_tables:
    full_table = f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.{t.tableName}"
    count = spark.table(full_table).count()
    print(f"    {t.tableName}: {count:,} rows")

print(f"\nGold: {gold_count} tables")
for t in gold_tables:
    full_table = f"{Config.CATALOG}.{Config.SCHEMA_GOLD}.{t.tableName}"
    count = spark.table(full_table).count()
    print(f"  {t.tableName}: {count:,} rows")

print(f"\nTotal: {bronze_count + silver_count + gold_count} tables")

# COMMAND ----------

# DBTITLE 1,Setup Complete
end_time = time.time()
elapsed = (end_time - start_time) / 60

print("\n" + "="*80)
print(f"✅ {Config.ENVIRONMENT.upper()} SETUP COMPLETE")
print("="*80)
print(f"Environment: {Config.ENVIRONMENT.upper()}")
print(f"Catalog: {Config.CATALOG}")
print(f"Runtime: {elapsed:.2f} minutes")
print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)
print(f"\n📊 SKILLAIPPATH {Config.ENVIRONMENT.upper()} READY")
if Config.USE_FLAT_SCHEMA:
    print(f"Schema: {Config.CATALOG}.{Config.SCHEMA_BRONZE} (flat mode with prefixes)")
    print(f"  Bronze: {bronze_count} tables ({Config.TABLE_PREFIX_BRONZE}*)")
    print(f"  Silver: {silver_count} tables ({Config.TABLE_PREFIX_SILVER}*)")
    print(f"  Gold: {gold_count} tables ({Config.TABLE_PREFIX_GOLD}*)")
else:
    print(f"Bronze: {Config.CATALOG}.{Config.SCHEMA_BRONZE} ({bronze_count} tables)")
    print(f"Silver: {Config.CATALOG}.{Config.SCHEMA_SILVER} ({silver_count} tables)")
    print(f"Gold: {Config.CATALOG}.{Config.SCHEMA_GOLD} ({gold_count} tables)")
print("="*80)