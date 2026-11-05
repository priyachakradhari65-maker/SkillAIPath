# Databricks notebook source
# MAGIC %md
# MAGIC # BHARAT MART - MEDALLION ARCHITECTURE: BRONZE LAYER
# MAGIC ## Layers 1-2: First 3 Dimensions
# MAGIC
# MAGIC **Self-Paced Learning - Medallion Architecture: Bronze Layer**
# MAGIC
# MAGIC **Bronze Layer Dimensions:**
# MAGIC - dim_date (calendar dimension)
# MAGIC - dim_product_category (product hierarchy)
# MAGIC - dim_retailer (business partners)
# MAGIC
# MAGIC **Milestone:** Star schema foundation with first 3 dimensions

# COMMAND ----------

# Dynamic imports removed - using %run config
sys.path.append('/Workspace/Shared/dev/SkillAIPath/1_Canvas/1_Setup')
# Legacy import removed - using %run config instead
from datetime import datetime

# Config loaded via %run

print("=" * 80)
print("BHARAT MART - MEDALLION ARCHITECTURE: BRONZE LAYER")
print("=" * 80)
print(f"Layers 1-2: First 3 Dimensions")
print(f"Start Time: {datetime.now()}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer Dimension Notebooks

# COMMAND ----------

bronze_dims = [
    "01_dim_date",
    "02_dim_product_category", 
    "04_dim_retailer"
]

for i, dim in enumerate(bronze_dims):
    print(f"\nRunning: {dim}")
    try:
        dbutils.notebook.run(
            f"/Workspace/Shared/dev/SkillAIPath/2_Data_Production/02_Medallion_Architecture/01_DIMENSIONS/{dim}",
            timeout_seconds=300,
            arguments={"catalog": config.catalog, "schema": config.schema}
        )
        print(f"  SUCCESS: {dim}")
    except Exception as e:
        print(f"  ERROR in {dim}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer Validation

# COMMAND ----------

print("=" * 80)
print("BRONZE LAYER VALIDATION")
print("=" * 80)

dims_to_validate = [
    "dim_date",
    "dim_product_category",
    "dim_retailer"
]

for dim in dims_to_validate:
    full_table = f"{config.catalog}.{config.schema}.{dim}"
    if spark.catalog.tableExists(full_table):
        count = spark.table(full_table).count()
        print(f"  SUCCESS {dim}: {count:,} rows")
    else:
        print(f"  ERROR {dim}: NOT FOUND - VALIDATION FAILED")

print("=" * 80)
print("BRONZE LAYER COMPLETE")
print("=" * 80)
print("Next: Medallion Architecture - Dimensions (Layers 3-4)")
print("=" * 80)