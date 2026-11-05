# Databricks notebook source
# MAGIC %md
# MAGIC # BHARAT MART - GOLD PART 2 ORCHESTRATION
# MAGIC ## Layers 11-12: Gold Marts Part 2
# MAGIC
# MAGIC **Gold Layer - Part 2**
# MAGIC
# MAGIC **This Week:**
# MAGIC - mart_product_performance (product profitability)
# MAGIC - mart_inventory_health (inventory turnover)
# MAGIC
# MAGIC **Milestone:** TIER 1 COMPLETE - Full data pipeline operational

# COMMAND ----------

# Dynamic imports removed - using %run config
sys.path.append('/Workspace/Shared/dev/SkillAIPath/1_Canvas/1_Setup')
# Legacy import removed - using %run config instead
from datetime import datetime

# Config loaded via %run

print("=" * 80)
print("BHARAT MART - GOLD PART 2 ORCHESTRATION")
print("=" * 80)
print(f"Layers 11-12: Gold Marts Part 2")
print(f"Start Time: {datetime.now()}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## GOLD PART 2 - Gold Mart Notebooks

# COMMAND ----------

week8_marts = [
    "03_mart_product_performance",
    "04_mart_inventory_health"
]

for mart in week8_marts:
    print(f"\nRunning: {mart}")
    try:
        dbutils.notebook.run(
            f"/Workspace/Shared/dev/SkillAIPath/2_Data_Production/02_Medallion_Architecture/04_GOLD/{mart}",
            timeout_seconds=600,
            arguments={"catalog": config.catalog, "schema": config.schema}
        )
        print(f"  SUCCESS: {mart}")
    except Exception as e:
        print(f"  ERROR in {mart}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## GOLD PART 2 - Validation - Complete Gold Layer

# COMMAND ----------

print("=" * 80)
print("GOLD PART 2 VALIDATION - COMPLETE GOLD LAYER")
print("=" * 80)

all_marts = [
    "mart_customer_360",
    "mart_rfm_analysis",
    "mart_product_performance",
    "mart_inventory_health"
]

for mart in all_marts:
    full_table = f"{config.catalog}.{config.schema}.{mart}"
    if spark.catalog.tableExists(full_table):
        count = spark.table(full_table).count()
        print(f"  ✓ {mart}: {count:,} rows")
    else:
        print(f"  ✗ {mart}: NOT FOUND")

print("=" * 80)
print("TIER 1 COMPLETE - FULL DATA PIPELINE OPERATIONAL")
print("=" * 80)
print("Bronze → Silver → Gold")
print("Dimensions → Facts → Marts")
print("=" * 80)
print("Next Week: dbt Integration (Layer 13)")
print("=" * 80)