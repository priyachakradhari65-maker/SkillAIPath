# Databricks notebook source
# MAGIC %md
# MAGIC # BHARAT MART - MEDALLION ARCHITECTURE: DIMENSIONS
# MAGIC ## Layers 0-4: Core Setup + All Dimensions
# MAGIC
# MAGIC **Self-Paced Learning - Medallion Architecture: Dimensions Layer**
# MAGIC
# MAGIC **Cumulative Build:**
# MAGIC - Foundation: Environment & Data (already complete)
# MAGIC - Bronze Layer: First 3 dimensions (already complete)
# MAGIC - Dimensions Layer: Remaining dimensions + SCD2 (NEW)
# MAGIC
# MAGIC **Milestone:** All dimension tables operational with SCD2 pattern

# COMMAND ----------

# Dynamic imports removed - using %run config
sys.path.append('/Workspace/Shared/dev/SkillAIPath/1_Canvas/1_Setup')
# Legacy import removed - using %run config instead
from datetime import datetime

# Config loaded via %run

print("=" * 80)
print("BHARAT MART - MEDALLION ARCHITECTURE: DIMENSIONS (CUMULATIVE)")
print("=" * 80)
print(f"Layers 0-4: Setup + All Dimensions")
print(f"Start Time: {datetime.now()}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensions (Bronze Layer - Already Complete)

# COMMAND ----------

# These should already exist from Bronze Layer, we're just validating
bronze_dims = [
    "01_dim_date",
    "02_dim_product_category",
    "03_dim_warehouse"
]

for dim in bronze_dims:
    table_name = dim.replace("01_", "").replace("02_", "").replace("03_", "")
    full_table = f"{config.catalog}.{config.schema}.{table_name}"
    
    if spark.catalog.tableExists(full_table):
        count = spark.table(full_table).count()
        print(f"  {table_name}: {count:,} rows (from Bronze Layer)")
    else:
        print(f"  WARNING: {table_name} not found, may need to re-run Bronze Layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## New Dimensions (Dimensions Layer)

# COMMAND ----------

dimensions_layer_dims = [
    "04_dim_retailer",
    "05_dim_agent",
    "06_dim_payment_mode",
    "10_dim_customer_scd2"
]

for dim in dimensions_layer_dims:
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
# MAGIC ## Dimensions Layer Validation

# COMMAND ----------

print("=" * 80)
print("DIMENSIONS LAYER VALIDATION - ALL DIMENSIONS")
print("=" * 80)

all_dims = [
    "dim_date",
    "dim_product_category",
    "dim_warehouse",
    "dim_retailer",
    "dim_agent",
    "dim_payment_mode",
    "dim_customer"  # SCD2 table
]

for dim in all_dims:
    full_table = f"{config.catalog}.{config.schema}.{dim}"
    if spark.catalog.tableExists(full_table):
        count = spark.table(full_table).count()
        print(f"  {dim}: {count:,} rows")
    else:
        print(f"  {dim}: NOT FOUND - VALIDATION FAILED")

print("=" * 80)
print("DIMENSIONS LAYER COMPLETE")
print("=" * 80)
print("Next: Medallion Architecture - Fact Tables (Layers 5-6)")
print("=" * 80)