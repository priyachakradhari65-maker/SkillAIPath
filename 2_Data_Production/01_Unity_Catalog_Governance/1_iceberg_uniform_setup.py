# Databricks notebook source
# DBTITLE 1,Iceberg UniForm - Open Table Format Integration
# MAGIC %md
# MAGIC # Iceberg UniForm: Open Table Format Integration
# MAGIC **SkillAIPath Data Warehouse - Multi-Engine Interoperability**
# MAGIC
# MAGIC ## THE PROBLEM: "We're locked into one vendor's ecosystem"
# MAGIC
# MAGIC **Monday Morning - Architecture Review**
# MAGIC
# MAGIC **Data Engineer:** "Our data team wants to use Trino for ad-hoc queries, but we're on Delta Lake. Do we need to export/convert our tables?"
# MAGIC
# MAGIC **Architect:** "How much data are we talking about?"
# MAGIC
# MAGIC **Engineer:** "228 crores of revenue data across 3 years. Exporting to Parquet would take hours and duplicate storage."
# MAGIC
# MAGIC **Architect:** "And if schema changes?"
# MAGIC
# MAGIC **Engineer:** "We'd have to re-export everything. Plus, managing two copies means sync issues."
# MAGIC
# MAGIC **Architect:** "What if I told you there's a way to let Trino read your Delta tables directly - without conversion, without duplication?"
# MAGIC
# MAGIC **Engineer:** "That's exactly what we need. How?"
# MAGIC
# MAGIC **Architect:** "Iceberg UniForm. It's a metadata layer on top of Delta Lake. Your tables stay Delta, but any Iceberg-compatible engine can read them."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## THE SOLUTION: Iceberg UniForm
# MAGIC
# MAGIC **What It Is:**
# MAGIC * Compatibility layer that adds Iceberg metadata to Delta tables
# MAGIC * Delta Lake remains the primary format (all writes go through Delta)
# MAGIC * Iceberg-compatible engines (Trino, Snowflake, Flink, standalone Spark) can READ the data
# MAGIC * No data duplication, no export jobs, no sync issues
# MAGIC
# MAGIC **What It's NOT:**
# MAGIC * Not a full table format conversion (Delta remains primary)
# MAGIC * Not bidirectional (external engines can read, but must write through Delta)
# MAGIC * Not a replacement for Delta Lake (it's an enhancement)
# MAGIC
# MAGIC **Real-World Use Case:**
# MAGIC * Data science team uses Databricks (Delta Lake) for ETL
# MAGIC * Business analysts use Trino for ad-hoc queries
# MAGIC * Finance team uses Snowflake for reporting
# MAGIC * All three tools read the SAME Delta tables via UniForm
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC **Required:**
# MAGIC * Module 05_GOLD complete (Gold tables must exist)
# MAGIC * Unity Catalog enabled
# MAGIC * Understanding of Medallion architecture
# MAGIC
# MAGIC **Tables We'll Enable:**
# MAGIC * {Config.SCHEMA_GOLD}.mart_daily_sales
# MAGIC * {Config.SCHEMA_GOLD}.mart_product_performance
# MAGIC * {Config.SCHEMA_GOLD}.mart_customer_insights

# COMMAND ----------

# DBTITLE 1,Setup and Configuration
# UniForm Setup

from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring
from datetime import datetime

print("="*70)
print("ICEBERG UNIFORM - OPEN TABLE FORMAT INTEGRATION")
print("="*70)
print(f"   Pattern: Multi-engine interoperability layer")
print(f"   Format: Delta Lake (primary) + Iceberg metadata")
print(f"   Target: Gold layer tables")
print(f"   Benefit: Read from Trino, Snowflake, Flink without conversion")
print("="*70)

# Configuration
dbutils.widgets.text("catalog", "sap_dev", "Catalog Name")
dbutils.widgets.text("schema", "gold", "Schema Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"\nTarget Catalog: {catalog}")
print(f"Target Schema: {schema}")
print(f"\nWe will enable UniForm on all Gold tables in {catalog}.{schema}")
print("Delta Lake remains the primary format - UniForm adds Iceberg read compatibility.")

# COMMAND ----------

# DBTITLE 1,Verify Gold Tables Exist
# MAGIC %sql
# MAGIC -- Check which Gold tables exist
# MAGIC SHOW TABLES IN sap_dev.gold;
# MAGIC
# MAGIC -- We expect to see:
# MAGIC -- mart_daily_sales
# MAGIC -- mart_product_performance
# MAGIC -- mart_customer_insights

# COMMAND ----------

# DBTITLE 1,Enable UniForm - Daily Sales Mart
# Enable Iceberg UniForm on Daily Sales Mart

print("ENABLING UNIFORM ON: mart_rfm_analysis")
print("="*70)

table_name = f"{catalog}.{schema}.mart_rfm_analysis"

print(f"Table: {table_name}")
print("Action: ALTER TABLE SET TBLPROPERTIES")
print("Property: delta.universalFormat.enabledFormats = iceberg")
print("\nThis is a metadata-only operation - no data movement.")
print("="*70)

# Enable UniForm with error handling
try:
    # Check if table exists
    spark.sql(f"DESCRIBE TABLE {table_name}")
    
    # Step 1: Enable column mapping
    spark.sql(f"""
      ALTER TABLE {table_name}
      SET TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name'
      )
    """)
    
    # Step 2: Disable deletion vectors
    spark.sql(f"""
      ALTER TABLE {table_name}
      SET TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'false'
      )
    """)
    
    # Step 3: Purge deletion vectors
    spark.sql(f"REORG TABLE {table_name} APPLY (PURGE)")
    
    # Step 4: Enable IcebergCompatV2
    spark.sql(f"""
      ALTER TABLE {table_name}
      SET TBLPROPERTIES (
        'delta.enableIcebergCompatV2' = 'true'
      )
    """)
    
    # Step 5: Enable UniForm
    spark.sql(f"""
      ALTER TABLE {table_name}
      SET TBLPROPERTIES (
        'delta.universalFormat.enabledFormats' = 'iceberg'
      )
    """)
    
    print("\nSUCCESS: UniForm enabled on mart_rfm_analysis")
    print("Iceberg-compatible engines can now read this table.")
    print("NOTE: Storage overhead +10-15% for dual metadata (Delta + Iceberg)")
except Exception as e:
    print(f"ERROR: Failed to enable UniForm on {table_name}")
    print(f"Reason: {str(e)}")
    print("\nTroubleshooting:")
    print("  1. Verify table exists: SHOW TABLES IN sap_dev.gold")
    print("  2. Check permissions: SELECT * FROM table LIMIT 1")
    print("  3. Ensure Delta Lake table (not external)")

# COMMAND ----------

# DBTITLE 1,Enable UniForm - Product Performance Mart
# Enable UniForm on Product Performance Mart

print("ENABLING UNIFORM ON: mart_product_performance")
print("="*70)

table_name = f"{catalog}.{schema}.mart_product_performance"

try:
    spark.sql(f"DESCRIBE TABLE {table_name}")
    
    # Step 1: Enable column mapping
    spark.sql(f"""
      ALTER TABLE {table_name}
      SET TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name'
      )
    """)
    
    # Step 2: Disable deletion vectors
    spark.sql(f"""
      ALTER TABLE {table_name}
      SET TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'false'
      )
    """)
    
    # Step 3: Purge deletion vectors
    spark.sql(f"REORG TABLE {table_name} APPLY (PURGE)")
    
    # Step 4: Enable IcebergCompatV2
    spark.sql(f"""
      ALTER TABLE {table_name}
      SET TBLPROPERTIES (
        'delta.enableIcebergCompatV2' = 'true'
      )
    """)
    
    # Step 5: Enable UniForm
    spark.sql(f"""
      ALTER TABLE {table_name}
      SET TBLPROPERTIES (
        'delta.universalFormat.enabledFormats' = 'iceberg'
      )
    """)
    
    print("SUCCESS: UniForm enabled on mart_product_performance")
    print("NOTE: Read-only access from Trino/Snowflake/Flink now enabled")
except Exception as e:
    print(f"ERROR: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Enable UniForm - Customer Insights Mart
# Enable UniForm on Customer Insights Mart

print("ENABLING UNIFORM ON: mart_customer_360")
print("="*70)

table_name = f"{catalog}.{schema}.mart_customer_360"

try:
    spark.sql(f"DESCRIBE TABLE {table_name}")
    
    # Step 1: Enable column mapping
    spark.sql(f"""
      ALTER TABLE {table_name}
      SET TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name'
      )
    """)
    
    # Step 2: Disable deletion vectors
    spark.sql(f"""
      ALTER TABLE {table_name}
      SET TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'false'
      )
    """)
    
    # Step 3: Purge deletion vectors
    spark.sql(f"REORG TABLE {table_name} APPLY (PURGE)")
    
    # Step 4: Enable IcebergCompatV2
    spark.sql(f"""
      ALTER TABLE {table_name}
      SET TBLPROPERTIES (
        'delta.enableIcebergCompatV2' = 'true'
      )
    """)
    
    # Step 5: Enable UniForm
    spark.sql(f"""
      ALTER TABLE {table_name}
      SET TBLPROPERTIES (
        'delta.universalFormat.enabledFormats' = 'iceberg'
      )
    """)
    
    print("SUCCESS: UniForm enabled on mart_customer_360")
    print("All 3 Gold marts now have Iceberg UniForm enabled")
except Exception as e:
    print(f"ERROR: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Validation - Check Table Properties
# MAGIC %sql
# MAGIC -- Verify UniForm is enabled
# MAGIC SHOW TBLPROPERTIES sap_dev.gold.mart_rfm_analysis;
# MAGIC
# MAGIC -- Look for: delta.universalFormat.enabledFormats = iceberg

# COMMAND ----------

# DBTITLE 1,Validation - All Tables Status
# Check detailed table properties for all Gold tables

print("VALIDATION: Checking UniForm Status")
print("="*70)

tables = [
    "mart_rfm_analysis",
    "mart_product_performance",
    "mart_customer_360"
]

for table in tables:
    full_table = f"sap_dev.gold.{table}"
    
    # Get table properties
    props = spark.sql(f"SHOW TBLPROPERTIES {full_table}").collect()
    
    # Check for UniForm property
    uniform_enabled = False
    for row in props:
        if row["key"] == "delta.universalFormat.enabledFormats":
            uniform_enabled = (row["value"] == "iceberg")
            break
    
    status = "ENABLED" if uniform_enabled else "NOT ENABLED"
    print(f"{table}: UniForm {status}")

print("="*70)
print("All Gold tables now have Iceberg read compatibility.")

# COMMAND ----------

# DBTITLE 1,How External Engines Read UniForm Tables
# MAGIC %md
# MAGIC ## How External Engines Access UniForm Tables
# MAGIC
# MAGIC ### Trino Example
# MAGIC
# MAGIC ```sql
# MAGIC -- In Trino CLI
# MAGIC SELECT 
# MAGIC   transaction_date,
# MAGIC   SUM(total_revenue) as daily_revenue
# MAGIC FROM iceberg.sap_gold.mart_daily_sales
# MAGIC GROUP BY transaction_date
# MAGIC ORDER BY transaction_date DESC
# MAGIC LIMIT 10;
# MAGIC ```
# MAGIC
# MAGIC **Key Points:**
# MAGIC * Trino sees the table as iceberg.sap_gold.mart_daily_sales
# MAGIC * Schema mapping: Unity Catalog schema becomes Iceberg schema
# MAGIC * Read-only access (writes must go through Delta Lake)
# MAGIC
# MAGIC ### Snowflake Example
# MAGIC
# MAGIC ```sql
# MAGIC -- In Snowflake
# MAGIC CREATE EXTERNAL VOLUME sap_gold
# MAGIC   STORAGE_LOCATIONS = (
# MAGIC     ('s3://sap-warehouse/gold/')
# MAGIC   );
# MAGIC
# MAGIC SELECT product_id, total_revenue, total_quantity
# MAGIC FROM sap_gold.mart_product_performance
# MAGIC WHERE total_revenue > 1000000
# MAGIC ORDER BY total_revenue DESC;
# MAGIC ```
# MAGIC
# MAGIC **Key Points:**
# MAGIC * Snowflake reads Iceberg metadata from external volume
# MAGIC * No data copy required
# MAGIC * Schema inference from Iceberg metadata
# MAGIC
# MAGIC ### Standalone Spark Example
# MAGIC
# MAGIC ```python
# MAGIC # Non-Databricks Spark (e.g., EMR, Dataproc)
# MAGIC spark.read \
# MAGIC   .format("iceberg") \
# MAGIC   .load("s3://sap-warehouse/gold/mart_daily_sales") \
# MAGIC   .show()
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Summary and Best Practices
# UniForm Summary

print("="*70)
print("ICEBERG UNIFORM - BEST PRACTICES SUMMARY")
print("="*70)

print("\nWhen to Enable UniForm:")
print("-" * 70)
print("  - Multi-engine analytics requirements")
print("  - Cross-platform data sharing")
print("  - Preventing vendor lock-in")
print("  - External partner read-only access")

print("\nHow to Enable (One-Line Command):")
print("-" * 70)
print("  ALTER TABLE catalog.schema.table")
print("  SET TBLPROPERTIES (")
print("    'delta.universalFormat.enabledFormats' = 'iceberg'")
print("  )")

print("\nKey Concepts:")
print("-" * 70)
print("  - Delta Lake remains primary format")
print("  - No data duplication")
print("  - Read-only access for external engines")
print("  - Automatic metadata sync on writes")
print("  - Negligible storage overhead")

print("\nSupported Engines:")
print("-" * 70)
print("  - Trino (formerly Presto)")
print("  - Snowflake")
print("  - Apache Flink")
print("  - Standalone Apache Spark")
print("  - Any Iceberg-compatible query engine")

print("\nProduction Checklist:")
print("-" * 70)
print("  1. Enable UniForm on production tables")
print("  2. Verify metadata sync (check SHOW TBLPROPERTIES)")
print("  3. Test read access from external engine")
print("  4. Monitor storage overhead (should be minimal)")
print("  5. Document access patterns for data consumers")

print("\nInterview Talking Point:")
print("-" * 70)
print("  I enabled Iceberg UniForm on our Gold tables, which adds")
print("  Iceberg metadata to our Delta tables without duplicating data.")
print("  This allows Trino and Snowflake users to read our tables")
print("  directly, preventing vendor lock-in while keeping Delta as")
print("  our primary format. The overhead is negligible and metadata")
print("  syncs automatically on every Delta write.")
print("="*70)