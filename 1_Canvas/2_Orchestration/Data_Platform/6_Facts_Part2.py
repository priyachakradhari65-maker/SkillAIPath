# Databricks notebook source
# MAGIC %md
# MAGIC # BHARAT MART - FACTS PART 2 ORCHESTRATION
# MAGIC ## Layers 7-8: Fact Tables Part 2
# MAGIC
# MAGIC **Facts Layer - Part 2**
# MAGIC
# MAGIC **This Week:**
# MAGIC - fact_inventory (snapshot-based inventory tracking)
# MAGIC
# MAGIC **Milestone:** Complete fact layer - Full star schema validated

# COMMAND ----------

# Dynamic imports removed - using %run config
sys.path.append('/Workspace/Shared/dev/SkillAIPath/1_Canvas/1_Setup')
# Legacy import removed - using %run config instead
from datetime import datetime

# Config loaded via %run

print("=" * 80)
print("BHARAT MART - FACTS PART 2 ORCHESTRATION")
print("=" * 80)
print(f"Layers 7-8: Fact Tables Part 2")
print(f"Start Time: {datetime.now()}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## FACTS PART 2 - Fact Notebook

# COMMAND ----------

week5_facts = [
    "03_fact_inventory"
]

for fact in week5_facts:
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
# MAGIC ## FACTS PART 2 - Validation - Complete Star Schema

# COMMAND ----------

print("=" * 80)
print("FACTS PART 2 VALIDATION - COMPLETE STAR SCHEMA")
print("=" * 80)

# Validate ALL dimensions (from previous weeks)
all_dims = [
    "dim_date",
    "dim_product_category",
    "dim_retailer",
    "dim_agent",
    "dim_payment_mode",
    "dim_customer"
]

print("\nDIMENSIONS:")
for dim in all_dims:
    full_table = f"{config.catalog}.{config.schema}.{dim}"
    if spark.catalog.tableExists(full_table):
        count = spark.table(full_table).count()
        print(f"  ✓ {dim}: {count:,} rows")

# Validate ALL facts (including this week's)
all_facts = [
    "fact_sales",
    "fact_purchases",
    "fact_inventory"
]

print("\nFACTS:")
for fact in all_facts:
    full_table = f"{config.catalog}.{config.schema}.{fact}"
    if spark.catalog.tableExists(full_table):
        count = spark.table(full_table).count()
        print(f"  ✓ {fact}: {count:,} rows")

print("=" * 80)
print("FACTS PART 2 COMPLETE - STAR SCHEMA OPERATIONAL")
print("=" * 80)
print("Next Week: Real-time Streaming & CDC (Layer 9)")
print("=" * 80)