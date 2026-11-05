# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Config
# License check temporarily disabled — running in open mode
LICENSE_TIER = "ENT"
FEATURE_GATES = {}

def check_job_access(job_name, tier=None):
    return True

# COMMAND ----------

# DBTITLE 1,SkillAIPath Configuration - Single Source of Truth
# MAGIC %md
# MAGIC # SkillAIPath Configuration
# MAGIC
# MAGIC Centralized configuration for all pipeline parameters.
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC **Pattern:** Single Config notebook imported via %run across all notebooks
# MAGIC
# MAGIC **Benefits:**
# MAGIC - Change table name once, applies everywhere
# MAGIC - No hardcoding across 40+ notebooks
# MAGIC - Environment consistency (dev/uat/prod)
# MAGIC - Widget-based environment selection
# MAGIC - Fully portable - NO hardcoded workspace paths
# MAGIC
# MAGIC ## Usage
# MAGIC
# MAGIC ```python
# MAGIC # Import configuration - path depends on your notebook location:
# MAGIC
# MAGIC # From 2_Data_Production folders:
# MAGIC %run ../../1_Canvas/1_Setup/1_config
# MAGIC
# MAGIC # From 3_AI_Platform folders:
# MAGIC %run ../../1_Canvas/1_Setup/1_config
# MAGIC
# MAGIC # From 0_Canvas (StartHere, orchestrators):
# MAGIC %run ./1_Setup/1_config
# MAGIC
# MAGIC # Access configuration
# MAGIC df.write.mode("append").saveAsTable(Config.TBL_FACT_SALES)
# MAGIC
# MAGIC # Use environment-specific catalog
# MAGIC print(f"Target catalog: {Config.CATALOG}")
# MAGIC print(f"Environment: {Config.ENVIRONMENT}")
# MAGIC ```
# MAGIC
# MAGIC ## Environment Switching
# MAGIC
# MAGIC Set the environment widget to switch between dev/uat/prod:
# MAGIC - All table references automatically update
# MAGIC - No code changes required
# MAGIC - Single source of truth

# COMMAND ----------

# DBTITLE 1,Configuration Class - Plug and Play
# SkillAIPath Configuration - Unity Catalog
# CI/CD READY: Parameter-based environment selection

import re

# Detect DLT pipeline execution: widgets are unavailable there, and the pipeline's
# own catalog/target settings must govern unqualified table names
try:
    import dlt  # noqa: F401
    IN_DLT_PIPELINE = True
except ImportError:
    IN_DLT_PIPELINE = False

def _default_environment() -> str:
    """Derive environment from this notebook's workspace path (/Shared/<env>/SkillAIPath/...)."""
    nb_path = ""
    try:
        nb_path = spark.conf.get("spark.databricks.notebook.path")
    except Exception:
        pass
    if not nb_path:
        try:
            nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        except Exception:
            nb_path = ""
    match = re.match(r"^/Shared/([^/]+)/SkillAIPath/", nb_path or "")
    return match.group(1) if match else "dev"

try:
    dbutils.widgets.text('environment', _default_environment(), 'Environment (dev/main)')
    ENVIRONMENT = dbutils.widgets.get('environment')
    print(f"✓ Environment: {ENVIRONMENT} (from parameter)")
except Exception as e:
    ENVIRONMENT = _default_environment()
    print(f"✓ Environment: {ENVIRONMENT} (fallback)")

# Mode detection (USER vs SHARED)
import os
current_path = os.getcwd()
print(f"Current path: {current_path}")

if '/Users/' in current_path:
    USE_FLAT_SCHEMA = True
    USER_CATALOG = "workspace"
    match = re.search(r'/Users/([^/@]+)', current_path)
    user_prefix = match.group(1) if match else "user"
    USER_SCHEMA_NAME = f"{user_prefix}_dev"
    print(f"✓ USER MODE: {USER_CATALOG}.{USER_SCHEMA_NAME}")
else:
    USE_FLAT_SCHEMA = False
    USER_CATALOG = None
    USER_SCHEMA_NAME = None
    print(f"✓ SHARED MODE: sap_{ENVIRONMENT} catalog")

# Set catalog context and auto-initialize if needed
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

if not USE_FLAT_SCHEMA and not IN_DLT_PIPELINE:
    # Production-portable: Create catalog and schemas if they don't exist
    # (skipped inside DLT pipelines - their catalog/target settings govern table placement)
    catalog_name = f"sap_{ENVIRONMENT}"
    
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
        print(f"✓ Catalog: {catalog_name} (created/verified)")
    except Exception as e:
        print(f"⚠️  Catalog creation skipped: {e}")
    
    # Create all required schemas
    required_schemas = ['bronze', 'silver', 'gold', 'staging', 'reference']
    for schema in required_schemas:
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema}")
        except Exception as e:
            print(f"⚠️  Schema {schema} creation skipped: {e}")
    
    print(f"✓ Schemas: {', '.join(required_schemas)}")
    
    # Set catalog context
    spark.sql(f"USE CATALOG {catalog_name}")
    print(f"✓ Catalog context: {catalog_name}")

class Config:
    # Unity Catalog Configuration - Multi-Mode Strategy
    ENVIRONMENT = ENVIRONMENT
    USE_UNITY_CATALOG = True
    USE_FLAT_SCHEMA = USE_FLAT_SCHEMA
    
    # Catalog and Schema Names (mode-dependent)
    if USE_FLAT_SCHEMA:
        # USER MODE with flat schema: workspace.{username}_dev with table prefixes
        CATALOG = USER_CATALOG
        SCHEMA_BRONZE = USER_SCHEMA_NAME
        SCHEMA_SILVER = USER_SCHEMA_NAME
        SCHEMA_GOLD = USER_SCHEMA_NAME
        TABLE_PREFIX_BRONZE = "bronze_"
        TABLE_PREFIX_SILVER = "silver_"
        TABLE_PREFIX_GOLD = "gold_"
    else:
        # Multi-schema mode: Bronze in sap_{env}, Silver/Gold handled by DBT
        # Bronze uses sap_{env} catalog to colocate with DBT outputs
        CATALOG = USER_CATALOG if USER_CATALOG else f"sap_{ENVIRONMENT}"
        SCHEMA_BRONZE = "bronze"
        SCHEMA_SILVER = "silver"
        SCHEMA_GOLD = "gold"
        TABLE_PREFIX_BRONZE = ""
        TABLE_PREFIX_SILVER = ""
        TABLE_PREFIX_GOLD = ""
    
    # Bronze Layer - Batch Tables (Historical/Main Storage)
    TBL_STG_SALES = f"{CATALOG}.{SCHEMA_BRONZE}.{TABLE_PREFIX_BRONZE}stg_sales"
    TBL_STG_PURCHASE = f"{CATALOG}.{SCHEMA_BRONZE}.{TABLE_PREFIX_BRONZE}stg_purchase"
    TBL_STG_STOCK = f"{CATALOG}.{SCHEMA_BRONZE}.{TABLE_PREFIX_BRONZE}stg_stock_status"
    
    # Bronze Layer - Incremental Staging (Daily Delta Landing Zone)
    TBL_STG_SALES_INCREMENTAL = f"{CATALOG}.{SCHEMA_BRONZE}.{TABLE_PREFIX_BRONZE}stg_sales_incremental"
    TBL_STG_PURCHASE_INCREMENTAL = f"{CATALOG}.{SCHEMA_BRONZE}.{TABLE_PREFIX_BRONZE}stg_purchase_incremental"
    TBL_STG_STOCK_INCREMENTAL = f"{CATALOG}.{SCHEMA_BRONZE}.{TABLE_PREFIX_BRONZE}stg_stock_status_incremental"
    
    # Bronze Layer - Streaming CDC Sources
    TBL_BRONZE_CUSTOMER_CDC = f"{CATALOG}.{SCHEMA_BRONZE}.{TABLE_PREFIX_BRONZE}customer_changes_raw"
    TBL_BRONZE_SALES_RAW = f"{CATALOG}.{SCHEMA_BRONZE}.{TABLE_PREFIX_BRONZE}sales_raw"
    TBL_BRONZE_PRODUCT_CDC = f"{CATALOG}.{SCHEMA_BRONZE}.{TABLE_PREFIX_BRONZE}product_changes_raw"
    TBL_BRONZE_INVENTORY_STATUS = f"{CATALOG}.{SCHEMA_BRONZE}.{TABLE_PREFIX_BRONZE}inventory_status"
    
    # Silver Layer - Dimensions
    TBL_DIM_CUSTOMER = f"{CATALOG}.{SCHEMA_SILVER}.{TABLE_PREFIX_SILVER}dim_customer"
    TBL_DIM_PRODUCT = f"{CATALOG}.{SCHEMA_SILVER}.{TABLE_PREFIX_SILVER}dim_product"
    TBL_DIM_CATEGORY = f"{CATALOG}.{SCHEMA_SILVER}.{TABLE_PREFIX_SILVER}dim_product_category"
    TBL_DIM_WAREHOUSE = f"{CATALOG}.{SCHEMA_SILVER}.{TABLE_PREFIX_SILVER}dim_warehouse"
    TBL_DIM_RETAILER = f"{CATALOG}.{SCHEMA_SILVER}.{TABLE_PREFIX_SILVER}dim_retailer"
    TBL_DIM_AGENT = f"{CATALOG}.{SCHEMA_SILVER}.{TABLE_PREFIX_SILVER}dim_agent"
    TBL_DIM_PAYMENT_MODE = f"{CATALOG}.{SCHEMA_SILVER}.{TABLE_PREFIX_SILVER}dim_payment_mode"
    TBL_DIM_WHOLESALER = f"{CATALOG}.{SCHEMA_SILVER}.{TABLE_PREFIX_SILVER}dim_wholesaler"
    TBL_DIM_PRODUCT_STATUS = f"{CATALOG}.{SCHEMA_SILVER}.{TABLE_PREFIX_SILVER}dim_product_status"
    TBL_DIM_DATE = f"{CATALOG}.{SCHEMA_SILVER}.{TABLE_PREFIX_SILVER}dim_date"
    
    # Silver Layer - Facts
    TBL_FACT_SALES = f"{CATALOG}.{SCHEMA_SILVER}.{TABLE_PREFIX_SILVER}fact_sales"
    TBL_FACT_PURCHASE = f"{CATALOG}.{SCHEMA_SILVER}.{TABLE_PREFIX_SILVER}fact_purchase"
    TBL_FACT_PURCHASES = f"{CATALOG}.{SCHEMA_SILVER}.{TABLE_PREFIX_SILVER}fact_purchase"
    TBL_FACT_STOCK = f"{CATALOG}.{SCHEMA_SILVER}.{TABLE_PREFIX_SILVER}fact_inventory"
    TBL_FACT_INVENTORY = f"{CATALOG}.{SCHEMA_SILVER}.{TABLE_PREFIX_SILVER}fact_inventory"
    
    # CDC Tracking
    TBL_CDC_WATERMARKS = f"{CATALOG}.{SCHEMA_SILVER}.{TABLE_PREFIX_SILVER}cdc_watermarks"
    
    # Audit and Load Tracking
    TBL_LOAD_AUDIT = f"{CATALOG}.{SCHEMA_BRONZE}.{TABLE_PREFIX_BRONZE}load_audit"
    
    # Gold Layer - Full Names
    TBL_MART_CUSTOMER_RFM = f"{CATALOG}.{SCHEMA_GOLD}.{TABLE_PREFIX_GOLD}mart_rfm_analysis"
    TBL_MART_REVENUE_DASHBOARD = f"{CATALOG}.{SCHEMA_GOLD}.{TABLE_PREFIX_GOLD}mart_revenue_dashboard"
    TBL_MART_PRODUCT_PERFORMANCE = f"{CATALOG}.{SCHEMA_GOLD}.{TABLE_PREFIX_GOLD}mart_product_performance"
    TBL_MART_CUSTOMER_360 = f"{CATALOG}.{SCHEMA_GOLD}.{TABLE_PREFIX_GOLD}mart_customer_360"
    TBL_MART_INVENTORY_HEALTH = f"{CATALOG}.{SCHEMA_GOLD}.{TABLE_PREFIX_GOLD}mart_inventory_health"
    
    # Gold Layer - Shortened Aliases
    TBL_MART_RFM = f"{CATALOG}.{SCHEMA_GOLD}.{TABLE_PREFIX_GOLD}mart_rfm_analysis"
    TBL_MART_PRODUCT = f"{CATALOG}.{SCHEMA_GOLD}.{TABLE_PREFIX_GOLD}mart_product_performance"
    TBL_MART_INVENTORY = f"{CATALOG}.{SCHEMA_GOLD}.{TABLE_PREFIX_GOLD}mart_inventory_health"
    
    # Workspace Root - dynamically set based on environment
    WORKSPACE_ROOT = f"/Shared/{ENVIRONMENT}/SkillAIPath"
    
    # Gold Layer - Streaming Tables
    TBL_STREAMING_CUSTOMER_360 = f"{CATALOG}.{SCHEMA_GOLD}.{TABLE_PREFIX_GOLD}streaming_customer_360"
    TBL_STREAMING_RFM_LIVE = f"{CATALOG}.{SCHEMA_GOLD}.{TABLE_PREFIX_GOLD}streaming_rfm_live"
    TBL_STREAMING_ALERTS_LOG = f"{CATALOG}.{SCHEMA_GOLD}.{TABLE_PREFIX_GOLD}streaming_alerts_log"
    TBL_STREAMING_PRODUCT_PERFORMANCE = f"{CATALOG}.{SCHEMA_GOLD}.{TABLE_PREFIX_GOLD}streaming_product_performance"
    
    # Checkpoint volumes for streaming
    VOL_CHECKPOINTS = f"/Volumes/{CATALOG}/gold/checkpoints"
    
    # Audit Columns
    COL_BATCH_DATE = "BatchDate"
    COL_LOADED_AT = "LoadedAt"
    COL_SOURCE_FILE = "SourceFile"
    COL_PIPELINE_RUN_ID = "PipelineRunID"
    
    # SCD Type 2
    SCD_START_DATE_COL = "StartDate"
    SCD_END_DATE_COL = "EndDate"
    SCD_IS_CURRENT_COL = "IsCurrent"
    SCD_HIGH_DATE = "9999-12-31"
    
    # Business Reference Data
    PRODUCT_CATEGORIES = [
        "Electronics", "Clothing", "Groceries", "Furniture",
        "Toys", "Books", "Sports", "Beauty"
    ]
    
    PAYMENT_MODES = ["Cash", "Card", "UPI", "NetBanking", "Wallet"]
    PRODUCT_STATUS = ["In Stock", "Low Stock", "Out of Stock", "Discontinued"]
    
   
    # STREAMING TEST INFRASTRUCTURE   
    
    # Streaming test data volume and paths
    # Note: Volumes use schema names, not table prefixes
    VOLUME_STREAMING_TEST = f"/Volumes/{CATALOG}/{SCHEMA_BRONZE}/streaming_test_data"
    VOLUME_PATH = VOLUME_STREAMING_TEST  # Alias for backward compatibility
    SOURCE_PATH = f"{VOLUME_STREAMING_TEST}/"  # No /batches/ - data is in volume root
    CHECKPOINT_BASE = f"{VOLUME_STREAMING_TEST}/_checkpoints"
    
    # Exactly-once semantics test (notebook 04)
    EXACTLY_ONCE_TABLE = f"{CATALOG}.{SCHEMA_BRONZE}.{TABLE_PREFIX_BRONZE}sales_exactly_once_test"
    EXACTLY_ONCE_CHECKPOINT = f"{CHECKPOINT_BASE}/exactly_once_test"
    
    # Auto Loader checkpoint (notebook 01)
    AUTOLOADER_CHECKPOINT = f"{CHECKPOINT_BASE}/autoloader_sales"


    # Tax Configuration (India GST)
    CGST_RATE = 0.09
    SGST_RATE = 0.09
    IGST_RATE = 0.18
    
    @staticmethod
    def print_config():
        print("="*70)
        print("SKILLAIPPATH CONFIGURATION")
        print("="*70)
        print(f"Environment: {Config.ENVIRONMENT.upper()}")
        print(f"Catalog: {Config.CATALOG}")
        print(f"Schemas: {Config.SCHEMA_BRONZE}, {Config.SCHEMA_SILVER}, {Config.SCHEMA_GOLD}")
        print("="*70)
        print("Sample Paths:")
        print(f"  Bronze: {Config.TBL_STG_SALES}")
        print(f"  Silver: {Config.TBL_DIM_CUSTOMER}")
        print(f"  Gold: {Config.TBL_MART_CUSTOMER_360}")
        print("="*70)

Config.print_config()
print("Config loaded - Use: %run ../../1_Canvas/1_Setup/1_config (adjust path based on your location)")
