# Databricks notebook source
# DBTITLE 1,SkillAIPath Configuration - Single Source of Truth
# MAGIC %md
# MAGIC # Complete Workspace Reset - Clean Start
# MAGIC
# MAGIC This notebook provides a complete cleanup of your SkillAIPath environment.
# MAGIC
# MAGIC ## What This Does
# MAGIC
# MAGIC DROP CASCADE - Deletes EVERYTHING:
# MAGIC - All tables in bronze/silver/gold schemas
# MAGIC - All schemas
# MAGIC - Entire catalog
# MAGIC - Creates fresh empty catalog and schemas
# MAGIC
# MAGIC ## When to Use
# MAGIC
# MAGIC - Fresh start for development
# MAGIC - Clean up after testing
# MAGIC - Reset before running full pipeline
# MAGIC - Fix corrupted state
# MAGIC
# MAGIC ## Usage
# MAGIC
# MAGIC **Prerequisites:** Config must be loaded by caller (e.g., StartHere loads it first)
# MAGIC
# MAGIC Run all cells:
# MAGIC 1. Environment parameter override (optional widget)
# MAGIC 2. TRUNCATE tables (fast cleanup - keeps structure)
# MAGIC 3. Create fresh schemas if needed (SHARED mode only)
# MAGIC
# MAGIC ## Safety
# MAGIC
# MAGIC - Environment-specific: Only affects dev/uat/prod catalog you're targeting
# MAGIC - Other environments remain untouched
# MAGIC - No way to undo - make sure you want a clean slate
# MAGIC
# MAGIC ## Next Steps After Reset
# MAGIC
# MAGIC 1. Run data_generator to create bronze data
# MAGIC 2. Run pipeline orchestrators to build silver/gold
# MAGIC 3. Create dashboards and analytics

# COMMAND ----------

# DBTITLE 1,Load Config
# Load config inline - %run doesn't work reliably with dbutils.notebook.run()
import os
import re

# Detect environment from current path
current_path = os.getcwd()
print(f"Current path: {current_path}")

# Detect mode
if '/Users/' in current_path:
    # USER MODE
    match = re.search(r'/Users/([^/@]+)', current_path)
    user_prefix = match.group(1) if match else "user"
    CATALOG = "workspace"
    SCHEMA_BRONZE = f"{user_prefix}_dev"
    SCHEMA_SILVER = f"{user_prefix}_dev"
    SCHEMA_GOLD = f"{user_prefix}_dev"
    TABLE_PREFIX_BRONZE = "bronze_"
    TABLE_PREFIX_SILVER = "silver_"
    TABLE_PREFIX_GOLD = "gold_"
    ENVIRONMENT = "dev"
    print(f"USER MODE: {CATALOG}.{SCHEMA_BRONZE}")
elif '/Shared/' in current_path:
    # SHARED MODE
    match = re.search(r'/Shared/([^/]+)/SkillAIPath/', current_path)
    env = match.group(1) if match else "dev"
    CATALOG = f"sap_{env}"
    SCHEMA_BRONZE = "bronze"
    SCHEMA_SILVER = "silver"
    SCHEMA_GOLD = "gold"
    TABLE_PREFIX_BRONZE = ""
    TABLE_PREFIX_SILVER = ""
    TABLE_PREFIX_GOLD = ""
    ENVIRONMENT = env
    print(f"SHARED MODE: {CATALOG} with {SCHEMA_BRONZE}/{SCHEMA_SILVER}/{SCHEMA_GOLD} schemas")
else:
    raise ValueError(f"Cannot detect environment from: {current_path}")

# COMMAND ----------

# DBTITLE 1,Import Centralized Config
# Display detected config
print("="*80)
print("RESET CATALOG")
print("="*80)
print(f"Environment: {ENVIRONMENT}")
print(f"Catalog: {CATALOG}")
print(f"Bronze Schema: {SCHEMA_BRONZE}")
print(f"Silver Schema: {SCHEMA_SILVER}")
print(f"Gold Schema: {SCHEMA_GOLD}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Reset Environment - USER or SHARED Mode
# ============================================================================
# TRUNCATE TABLES - Fast Reset (100x faster than DROP CASCADE)
# ============================================================================
# Strategy: Delete data from tables, keep catalog/schema structure intact
# 
# USER MODE: workspace.{user}_dev schema
#   - Never touch workspace catalog (system catalog)
#   - TRUNCATE tables: bronze_*, silver_*, gold_*
#
# SHARED MODE: sap_dev or sap_main catalogs
#   - Catalog created once (first time only)
#   - TRUNCATE tables in bronze/silver/gold schemas
#   - Never DROP CASCADE after initial setup
# ============================================================================

import logging
from datetime import datetime

# Setup structured logging
class ResetLogger:
    def __init__(self, mode, catalog, schema=None):
        self.mode = mode
        self.catalog = catalog
        self.schema = schema
        self.start_time = datetime.now()
        
    def header(self, title):
        print("\n" + "="*80)
        print(f"  {title}")
        print("="*80)
        
    def section(self, title):
        print(f"\n[{self.mode}] {title}")
        print("-" * 60)
        
    def info(self, msg, indent=0):
        prefix = "  " * indent
        print(f"{prefix}✓ {msg}")
        
    def warn(self, msg, indent=0):
        prefix = "  " * indent
        print(f"{prefix}⚠ {msg}")
        
    def error(self, msg, indent=0):
        prefix = "  " * indent
        print(f"{prefix}✗ {msg}")
        
    def summary(self, tables_cleaned, total_tables, errors=0):
        duration = (datetime.now() - self.start_time).total_seconds()
        self.header("RESET SUMMARY")
        print(f"  Mode: {self.mode}")
        print(f"  Catalog: {self.catalog}")
        if self.schema:
            print(f"  Schema: {self.schema}")
        print(f"  Tables Cleaned: {tables_cleaned}/{total_tables}")
        if errors > 0:
            print(f"  Errors: {errors}")
        print(f"  Duration: {duration:.1f}s")
        print(f"  Status: {'✓ SUCCESS' if errors == 0 else '⚠ PARTIAL SUCCESS'}")
        print("="*80)

# Detect mode
USER_MODE = CATALOG == "workspace"
logger = ResetLogger(
    mode="USER" if USER_MODE else "SHARED",
    catalog=CATALOG,
    schema=SCHEMA_BRONZE if USER_MODE else None
)

logger.header("ENVIRONMENT RESET - TRUNCATE TABLES")
print(f"  Environment: {ENVIRONMENT}")
print(f"  Catalog: {CATALOG}")
print(f"  Mode: {'USER (table prefixes)' if USER_MODE else 'SHARED (separate schemas)'}")
print(f"  Strategy: TRUNCATE tables (keep structure)")
print("="*80)

if USER_MODE:
    # ========================================================================
    # USER MODE: workspace.{user}_dev schema with table prefixes
    # ========================================================================
    
    schema = SCHEMA_BRONZE  # All tables in one schema
    
    logger.section(f"Scanning schema: {CATALOG}.{schema}")
    
    try:
        tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}").collect()
        
        # Filter tables by prefix
        bronze_tables = [t.tableName for t in tables if t.tableName.startswith('bronze_')]
        silver_tables = [t.tableName for t in tables if t.tableName.startswith('silver_')]
        gold_tables = [t.tableName for t in tables if t.tableName.startswith('gold_')]
        
        all_tables = bronze_tables + silver_tables + gold_tables
        total_tables = len(all_tables)
        
        logger.info(f"Bronze: {len(bronze_tables)} tables", indent=1)
        logger.info(f"Silver: {len(silver_tables)} tables", indent=1)
        logger.info(f"Gold: {len(gold_tables)} tables", indent=1)
        logger.info(f"Total: {total_tables} tables", indent=1)
        
        if total_tables == 0:
            logger.warn("No tables found - schema is empty (fresh start)", indent=1)
        else:
            logger.section("Truncating tables (deleting data)")
            
            cleaned = 0
            errors = 0
            
            for table_name in all_tables:
                try:
                    spark.sql(f"TRUNCATE TABLE {CATALOG}.{schema}.{table_name}")
                    cleaned += 1
                    logger.info(f"{table_name}", indent=1)
                except Exception as e:
                    errors += 1
                    error_msg = str(e)
                    if "PERMISSION" in error_msg.upper():
                        logger.warn(f"{table_name}: No MODIFY permission - will recreate", indent=1)
                    else:
                        logger.error(f"{table_name}: {error_msg[:50]}", indent=1)
            
            logger.summary(cleaned, total_tables, errors)
            
    except Exception as e:
        error_msg = str(e)
        if "not exist" in error_msg.lower():
            logger.warn(f"Schema does not exist - fresh start")
            logger.summary(0, 0, 0)
        else:
            logger.error(f"Schema scan failed: {error_msg[:100]}")
            raise
    
    dbutils.notebook.exit("SUCCESS")

else:
    # ========================================================================
    # SHARED MODE: sap_dev or sap_main with bronze/silver/gold schemas
    # ========================================================================
    
    logger.section("Checking catalog existence")
    
    try:
        catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
        catalog_exists = CATALOG in catalogs
        
        if not catalog_exists:
            # Catalog must be pre-created by admin (requires CREATE CATALOG permission)
            logger.error(f"Catalog {CATALOG} does not exist")
            print("\n📋 REQUIRED ACTION (Admin/Owner):")
            print(f"   Run this command with admin rights:")
            print(f"   \n   CREATE CATALOG IF NOT EXISTS {CATALOG};")
            print(f"   GRANT USE CATALOG ON CATALOG {CATALOG} TO `account users`;")
            print(f"   GRANT CREATE SCHEMA ON CATALOG {CATALOG} TO `account users`;")
            print(f"\n   Then re-run this notebook.")
            raise RuntimeError(f"Catalog {CATALOG} must exist before reset")
        else:
            logger.info(f"Catalog {CATALOG} exists", indent=1)
        
        # Truncate tables in each schema
        schemas = [SCHEMA_BRONZE, SCHEMA_SILVER, SCHEMA_GOLD]
        total_cleaned = 0
        total_tables = 0
        total_errors = 0
        
        for schema in schemas:
            logger.section(f"Processing schema: {CATALOG}.{schema}")
            
            try:
                # Check if schema exists, create if needed
                schema_exists = spark.sql(f"SHOW SCHEMAS IN {CATALOG}").filter(f"databaseName = '{schema}'").count() > 0
                
                if not schema_exists:
                    logger.warn(f"Schema {schema} does not exist - creating", indent=1)
                    spark.sql(f"CREATE SCHEMA {CATALOG}.{schema}")
                    logger.info(f"Schema {schema} created", indent=1)
                    continue
                
                # Get tables in schema
                tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}").collect()
                table_names = [t.tableName for t in tables]
                
                if len(table_names) == 0:
                    logger.info(f"No tables in {schema} - empty", indent=1)
                    continue
                
                logger.info(f"Found {len(table_names)} tables", indent=1)
                total_tables += len(table_names)
                
                # Truncate each table
                for table_name in table_names:
                    try:
                        spark.sql(f"TRUNCATE TABLE {Config.CATALOG}.{schema}.{table_name}")
                        total_cleaned += 1
                        logger.info(f"{table_name}", indent=2)
                    except Exception as e:
                        total_errors += 1
                        error_msg = str(e)
                        if "PERMISSION" in error_msg.upper():
                            logger.warn(f"{table_name}: No MODIFY permission", indent=2)
                        else:
                            logger.error(f"{table_name}: {error_msg[:50]}", indent=2)
                
            except Exception as e:
                logger.error(f"Schema {schema} processing failed: {str(e)[:100]}", indent=1)
                total_errors += 1
        
        logger.summary(total_cleaned, total_tables, total_errors)
        
        dbutils.notebook.exit("SUCCESS")
            
    except Exception as e:
        logger.error(f"Catalog operations failed: {str(e)}")
        raise

# COMMAND ----------

# DBTITLE 1,DevOps Configuration - Environment Isolation
# MAGIC %md
# MAGIC ## Multi-Catalog Architecture - Environment Isolation
# MAGIC
# MAGIC **SEPARATE CATALOGS PER ENVIRONMENT:**
# MAGIC
# MAGIC ```
# MAGIC sap_dev/                      # Dev catalog
# MAGIC ├── bronze/                          # Bronze schema
# MAGIC │   ├── stg_sales
# MAGIC │   ├── stg_purchase
# MAGIC │   └── customer_changes_raw
# MAGIC ├── silver/                          # Silver schema
# MAGIC │   ├── dim_customer
# MAGIC │   ├── fact_sales
# MAGIC │   └── cdc_watermarks
# MAGIC └── gold/                            # Gold schema
# MAGIC     └── mart_customer_360
# MAGIC
# MAGIC sap_uat/                      # UAT catalog (same structure)
# MAGIC ├── bronze/
# MAGIC ├── silver/
# MAGIC └── gold/
# MAGIC
# MAGIC sap_main/                     # Prod catalog (same structure)
# MAGIC ├── bronze/
# MAGIC ├── silver/
# MAGIC └── gold/
# MAGIC ```
# MAGIC
# MAGIC **Environment Selection:**
# MAGIC - **Asset Bundles Deployment:** Environment passed as job parameter
# MAGIC - **Manual Testing:** Use widget to switch environments
# MAGIC - **Catalog Auto-Select:** Config class uses `f"sap_{ENVIRONMENT}"` pattern
# MAGIC - **Isolation:** Each environment has separate catalog, complete separation
# MAGIC
# MAGIC **GitHub Actions Workflow:**
# MAGIC ```yaml
# MAGIC targets:
# MAGIC   uat:
# MAGIC     variables:
# MAGIC       environment: uat  # Passed to notebooks, targets sap_uat catalog
# MAGIC   prod:
# MAGIC     variables:
# MAGIC       environment: main  # Passed to notebooks, targets sap_main catalog
# MAGIC ```
# MAGIC
# MAGIC **Example Table Paths:**
# MAGIC - Dev: `sap_dev.bronze.stg_sales`
# MAGIC - UAT: `sap_uat.bronze.stg_sales`
# MAGIC - Prod: `sap_main.bronze.stg_sales`
# MAGIC
# MAGIC **Benefits:**
# MAGIC - Clear catalog-level isolation (dev/uat/prod completely separate)
# MAGIC - Standard Databricks pattern (catalog.schema.table)
# MAGIC - Simple schema names (bronze/silver/gold, no prefixes)
# MAGIC - Zero code changes when promoting between environments
# MAGIC - Config class handles all catalog switching automatically

# COMMAND ----------

# DBTITLE 1,Create Fresh Environment
# Step 0B: Create schemas (SHARED MODE only)
# USER MODE: Schema already exists (workspace.{user}_dev)

print("=" * 80)
print(f"VERIFYING {ENVIRONMENT.upper()} ENVIRONMENT")
print("=" * 80)
print()
print(f"Environment: {ENVIRONMENT}")
print(f"Catalog: {CATALOG}")
print()

USER_MODE = CATALOG == "workspace"

if USER_MODE:
    print("MODE: USER - schema already exists")
    print(f"Schema: {CATALOG}.{SCHEMA_BRONZE}")
    print("No schema creation needed (using table prefixes)")
    print()
    print("✅ Ready for data generation")
    dbutils.notebook.exit("SUCCESS: Reset complete (USER MODE - truncated tables)")

# Create dev schemas
schemas_to_create = [
    (SCHEMA_BRONZE, "Raw staging data with audit trail"),
    (SCHEMA_SILVER, "Conformed dimensions and validated facts"),
    (SCHEMA_GOLD, "Business intelligence marts")
]

print("Creating schemas...")
print("-" * 80)

for schema, description in schemas_to_create:
    try:
        # Create schema
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
        
        # Add comment
        spark.sql(f"""
            ALTER SCHEMA {CATALOG}.{schema} 
            SET PROPERTIES ('comment' = '{description}')
        """)
        
        print(f"  Created: {CATALOG}.{schema}")
        print(f"           {description}")
    except Exception as e:
        print(f"  ERROR creating {schema}: {str(e)[:100]}")

print()

# Verify creation
print("Verifying schemas...")
print("-" * 80)

try:
    schemas_df = spark.sql(f"SHOW SCHEMAS IN {CATALOG}")
    created_schemas = [row.databaseName for row in schemas_df.collect()]
    
    print(f"\nSchemas in {CATALOG}:")
    for schema in created_schemas:
        print(f"  - {schema}")
    
    # Check if all required schemas exist
    required = [SCHEMA_BRONZE, SCHEMA_SILVER, SCHEMA_GOLD]
    missing = [s for s in required if s not in created_schemas]
    
    if missing:
        print(f"\nWARNING: Missing schemas: {missing}")
    else:
        print("\nSUCCESS: All required schemas created")
except Exception as e:
    print(f"ERROR verifying schemas: {e}")

print()
print("=" * 80)
print("DEV ENVIRONMENT READY")
print("=" * 80)
print()
print("Schema Structure:")
print(f"  {CATALOG}.{SCHEMA_BRONZE}/  <- Raw staging tables")
print(f"  {CATALOG}.{SCHEMA_SILVER}/  <- Dimensions + Facts")
print(f"  {CATALOG}.{SCHEMA_GOLD}/    <- Business marts")
print()
print("Next steps:")
print("  1. Run: 1_Setup/2_data_generator (generate bronze data)")
print("  2. Run: 06_ORCHESTRATION/02_sap_pipeline_orchestrator (build pipeline)")

# Clean exit for orchestration
dbutils.notebook.exit("SUCCESS")
print("  3. Verify: Tables created in bronze/silver/gold schemas")