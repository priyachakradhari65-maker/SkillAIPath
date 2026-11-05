# Databricks notebook source
# MAGIC %md
# MAGIC # BHARAT MART - STREAMING CDC ORCHESTRATION
# MAGIC ## Layer 9: Real-time Streaming & CDC
# MAGIC
# MAGIC **Streaming CDC Layer**
# MAGIC
# MAGIC **This Week:**
# MAGIC - Auto Loader patterns
# MAGIC - Bronze/Silver/Gold medallion architecture
# MAGIC - CDC (Change Data Capture)
# MAGIC
# MAGIC **Milestone:** Real-time data ingestion operational

# COMMAND ----------

from datetime import datetime

# Simple config object for orchestration
class Config:
    CATALOG = "sap_dev"
    SCHEMA_BRONZE = "bronze"
    SCHEMA_SILVER = "silver"
    SCHEMA_GOLD = "gold"
    
config = Config()

print("=" * 80)
print("BHARAT MART - STREAMING CDC ORCHESTRATION")
print("=" * 80)
print(f"Layer 9: Streaming & CDC")
print(f"Start Time: {datetime.now()}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## STREAMING CDC - Streaming Notebooks

# COMMAND ----------

week6_notebooks = [
    "02_streaming_silver_transforms",
    "06_customer_scd2_cdc"
]

for notebook in week6_notebooks:
    print(f"\nRunning: {notebook}")
    try:
        dbutils.notebook.run(
            f"/Workspace/Shared/dev/SkillAIPath/2_Data_Production/02_Medallion_Architecture/03_Streaming_Cdc/{notebook}",
            timeout_seconds=600,
            arguments={}
        )
        print(f"  SUCCESS: {notebook}")
    except Exception as e:
        print(f"  ERROR in {notebook}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## STREAMING CDC - Validation

# COMMAND ----------

print("=" * 80)
print("STREAMING CDC VALIDATION - STREAMING PIPELINE")
print("=" * 80)

streaming_tables = [
    "customer_changes_bronze",
    "customer_changes_silver",
    "customer_scd2_cdc"
]

for table in streaming_tables:
    full_table = f"{config.CATALOG}.{config.SCHEMA_SILVER}.{table}"
    try:
        if spark.catalog.tableExists(full_table):
            count = spark.table(full_table).count()
            print(f"  ✓ {table}: {count:,} rows")
        else:
            print(f"  ○ {table}: Not created (optional)")
    except:
        print(f"  ○ {table}: Streaming table (check separately)")

print("=" * 80)
print("STREAMING CDC COMPLETE")
print("=" * 80)
print("Next Week: Gold Marts Part 1 (Layer 10)")
print("=" * 80)