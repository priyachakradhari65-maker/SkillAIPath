# Databricks notebook source
# MAGIC %md
# MAGIC # BHARAT MART - GOLD PART 1 ORCHESTRATION
# MAGIC ## Layer 10: Gold Marts Part 1
# MAGIC
# MAGIC **Gold Layer - Part 1**
# MAGIC
# MAGIC **This Week:**
# MAGIC - mart_customer_360 (comprehensive customer view)
# MAGIC - mart_rfm_analysis (segmentation)
# MAGIC
# MAGIC **Milestone:** Business-ready analytics layer established

# COMMAND ----------

# Dynamic imports removed - using %run config
sys.path.append('/Workspace/Shared/dev/SkillAIPath/1_Canvas/1_Setup')
# Legacy import removed - using %run config instead
from datetime import datetime

# Config loaded via %run

print("=" * 80)
print("BHARAT MART - GOLD PART 1 ORCHESTRATION")
print("=" * 80)
print(f"Layer 10: Gold Marts Part 1")
print(f"Start Time: {datetime.now()}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## GOLD PART 1 - Gold Mart Notebooks

# COMMAND ----------

week7_marts = [
    "01_mart_customer_360",
    "02_mart_rfm_analysis"
]

for mart in week7_marts:
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
# MAGIC ## GOLD PART 1 - Validation

# COMMAND ----------

print("=" * 80)
print("GOLD PART 1 VALIDATION - GOLD MARTS")
print("=" * 80)

marts_to_validate = [
    "mart_customer_360",
    "mart_rfm_analysis"
]

for mart in marts_to_validate:
    full_table = f"{config.catalog}.{config.schema}.{mart}"
    if spark.catalog.tableExists(full_table):
        count = spark.table(full_table).count()
        print(f"  ✓ {mart}: {count:,} rows")
    else:
        print(f"  ✗ {mart}: NOT FOUND - VALIDATION FAILED")

print("=" * 80)
print("GOLD PART 1 COMPLETE")
print("=" * 80)
print("Next Week: Gold Marts Part 2 (Layers 11-12)")
print("=" * 80)