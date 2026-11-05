# Databricks notebook source
# MAGIC %md
# MAGIC # BHARAT MART - FACTS ORCHESTRATION (PART 1)
# MAGIC ## Layers 5-6: Fact Tables Part 1
# MAGIC
# MAGIC **Facts Layer - Part 1**
# MAGIC
# MAGIC **This Week:**
# MAGIC - fact_sales (transaction-level sales)
# MAGIC - fact_purchases (transaction-level purchases)
# MAGIC
# MAGIC **Milestone:** Star schema operational with first 2 fact tables

# COMMAND ----------

# DBTITLE 1,Load Config
# MAGIC %run ../../1_Setup/1_config

# COMMAND ----------

# Dynamic imports removed - using %run config
sys.path.append('/Workspace/Shared/dev/SkillAIPath/1_Canvas/1_Setup')
# Legacy import removed - using %run config instead
from datetime import datetime

# Config loaded via %run

print("=" * 80)
print("BHARAT MART - FACTS ORCHESTRATION (PART 1)")
print("=" * 80)
print(f"Layers 5-6: Fact Tables Part 1")
print(f"Start Time: {datetime.now()}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Notebooks - Part 1

# COMMAND ----------

week4_facts = [
    "01_fact_sales",
    "02_fact_purchases"
]

for fact in week4_facts:
    print(f"\nRunning: {fact}")
    try:
        dbutils.notebook.run(
            f"/Workspace/Shared/dev/SkillAIPath/2_Data_Production/02_Medallion_Architecture/02_FACTS/{fact}",
            timeout_seconds=600,
            arguments={"catalog": config.catalog, "schema": config.schema}
        )
        print(f"  SUCCESS: {fact}")
    except Exception as e:
        print(f"  ERROR in {fact}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation - Part 1

# COMMAND ----------

print("=" * 80)
print("FACTS VALIDATION - PART 1")
print("=" * 80)

facts_to_validate = [
    "fact_sales",
    "fact_purchases"
]

for fact in facts_to_validate:
    full_table = f"{config.catalog}.{config.schema}.{fact}"
    if spark.catalog.tableExists(full_table):
        count = spark.table(full_table).count()
        print(f"  ✓ {fact}: {count:,} rows")
    else:
        print(f"  ✗ {fact}: NOT FOUND - VALIDATION FAILED")

print("=" * 80)
print("FACTS PART 1 COMPLETE")
print("=" * 80)
print("Next Week: Inventory Fact Table (Layer 7-8)")
print("=" * 80)