# Databricks notebook source
# MAGIC %md
# MAGIC # BHARAT MART - FOUNDATION ARCHITECTURE
# MAGIC ## Layer 0: Environment Setup & Configuration
# MAGIC
# MAGIC **Self-Paced Learning - Foundation Architecture**
# MAGIC
# MAGIC **Foundation Architecture:**
# MAGIC - Set up Bharat Mart configuration
# MAGIC - Generate sample retail data
# MAGIC - Validate environment
# MAGIC
# MAGIC **Milestone:** Environment ready for data pipeline build

# COMMAND ----------

# DBTITLE 1,⚙️ Date Range Configuration
# ============================================================================
# DATE RANGE CONFIGURATION
# ============================================================================
# Smart default: 3 days for manual/notebook runs, 30 days for job runs
# User can override via widget parameter

import json

# Detect job context
def is_running_in_job():
    """Detect if notebook is running as part of a Databricks Job"""
    try:
        # Check notebook context
        context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
        
        # If jobId exists in context, we're running in a job
        job_id = context.get('tags', {}).get('jobId')
        run_id = context.get('tags', {}).get('runId')
        
        return job_id is not None or run_id is not None
    except:
        return False

# Set smart default
is_job = is_running_in_job()
default_days = 30 if is_job else 3

# Create widget with smart default
dbutils.widgets.text("date_range_days", str(default_days), "Date Range (days)")
date_range_days = int(dbutils.widgets.get("date_range_days"))

# Display configuration
run_mode = "Job/Workflow" if is_job else "Manual/Notebook"
print(f"="*80)
print(f"DATE RANGE CONFIGURATION")
print(f"="*80)
print(f"Run Mode: {run_mode}")
print(f"Default: {default_days} days")
print(f"Active: {date_range_days} days")
print(f"="*80)
print()

# Make available globally for all dependent notebooks
spark.conf.set("spark.databricks.sap15.date_range_days", str(date_range_days))

# COMMAND ----------

# Dynamic imports removed - using %run config
sys.path.append('/Workspace/Shared/dev/SkillAIPath/1_Canvas/1_Setup')
# Legacy import removed - using %run config instead
from datetime import datetime

# Config loaded via %run

print("=" * 80)
print("BHARAT MART - FOUNDATION ARCHITECTURE")
print("=" * 80)
print(f"Layer 0: Environment Setup")
print(f"Catalog: {config.catalog}")
print(f"Schema: {config.schema}")
print(f"Environment: {config.environment}")
print(f"Start Time: {datetime.now()}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Validate Environment

# COMMAND ----------

if not config.validate_environment():
    raise ValueError(f"Environment validation failed. Check catalog/schema exist.")

print("SUCCESS: Environment validated")
print(f"  Catalog {config.catalog} exists")
print(f"  Schema {config.schema} exists")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Generate Sample Data

# COMMAND ----------

try:
    dbutils.notebook.run(
        "/Workspace/Shared/dev/SkillAIPath/1_Canvas/1_Setup/data_generator",
        timeout_seconds=600,
        arguments={"catalog": config.catalog, "schema": config.schema}
    )
    print("SUCCESS: Sample data generated")
except Exception as e:
    print(f"ERROR: Data generation failed - {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Foundation Validation

# COMMAND ----------

print("=" * 80)
print("FOUNDATION VALIDATION")
print("=" * 80)

# Check if raw data tables exist
tables_expected = [
    "raw_transactions",
    "raw_customers",
    "raw_products"
]

tables_found = spark.sql(f"SHOW TABLES IN {config.catalog}.{config.schema}").collect()
table_names = [row.tableName for row in tables_found]

for expected_table in tables_expected:
    if expected_table in table_names:
        count = spark.table(f"{config.catalog}.{config.schema}.{expected_table}").count()
        print(f"  {expected_table}: {count:,} rows")
    else:
        print(f"  {expected_table}: NOT FOUND")

print("=" * 80)
print("FOUNDATION COMPLETE")
print("=" * 80)
print("Next: Medallion Architecture - Bronze Layer")
print("=" * 80)