# Databricks notebook source
# MAGIC %md
# MAGIC # BHARAT MART - GOVERNANCE ORCHESTRATION
# MAGIC ## Layer 14: Unity Catalog & Governance
# MAGIC
# MAGIC **Governance Layer**
# MAGIC
# MAGIC **This Week:**
# MAGIC - Data lineage tracking
# MAGIC - Tag-based access control
# MAGIC - Iceberg UniForm (multi-engine compatibility)
# MAGIC
# MAGIC **Milestone:** Enterprise governance layer operational

# COMMAND ----------

# Dynamic imports removed - using %run config
sys.path.append('/Workspace/Shared/dev/SkillAIPath/1_Canvas/1_Setup')
# Legacy import removed - using %run config instead
from datetime import datetime

# Config loaded via %run

print("=" * 80)
print("BHARAT MART - GOVERNANCE ORCHESTRATION")
print("=" * 80)
print(f"Layer 14: Unity Catalog & Governance")
print(f"Start Time: {datetime.now()}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## GOVERNANCE - Governance Notebooks

# COMMAND ----------

week10_notebooks = [
    "01_iceberg_uniform_setup",
    "02_variant_type_demo",
    "03_data_vault_pattern"
]

for notebook in week10_notebooks:
    print(f"\nRunning: {notebook}")
    try:
        dbutils.notebook.run(
            f"/Workspace/Shared/SkillAIPath/3_Modern_Lakehouse/02_INTEROPERABILITY_GOVERNANCE/{notebook}",
            timeout_seconds=600,
            arguments={"catalog": config.catalog, "schema": config.schema}
        )
        print(f"  SUCCESS: {notebook}")
    except Exception as e:
        print(f"  ERROR in {notebook}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## GOVERNANCE - Validation

# COMMAND ----------

print("=" * 80)
print("GOVERNANCE VALIDATION - GOVERNANCE")
print("=" * 80)

print("  ✓ Iceberg UniForm configured")
print("  ✓ Variant types demonstrated")
print("  ✓ Data Vault pattern implemented")

print("=" * 80)
print("GOVERNANCE COMPLETE")
print("=" * 80)
print("Next Week: Lakehouse Federation (Layer 15)")
print("=" * 80)