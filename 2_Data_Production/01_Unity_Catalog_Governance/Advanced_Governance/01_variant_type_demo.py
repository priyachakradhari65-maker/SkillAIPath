# Databricks notebook source
# DBTITLE 1,VARIANT Type - Semi-Structured Data
# MAGIC %md
# MAGIC # VARIANT Type: Semi-Structured JSON Data
# MAGIC **SkillAIPath Data Warehouse - Flexible Schema Handling**
# MAGIC
# MAGIC ## THE PROBLEM: "Our JSON data keeps changing structure"
# MAGIC
# MAGIC **Tuesday Afternoon - Data Integration Meeting**
# MAGIC
# MAGIC **Data Engineer:** "We're getting JSON payloads from our mobile app - order metadata, device info, user preferences. The schema changes every release."
# MAGIC
# MAGIC **Architect:** "What's the current approach?"
# MAGIC
# MAGIC **Engineer:** "We flatten everything into 50+ columns. Every time the app team adds a field, we ALTER TABLE and backfill NULLs."
# MAGIC
# MAGIC **Architect:** "How often does this happen?"
# MAGIC
# MAGIC **Engineer:** "Every 2 weeks. Last month we added 12 new columns for the loyalty program. Half are empty for old records."
# MAGIC
# MAGIC **Architect:** "This is exactly what VARIANT type solves. Store the entire JSON as-is, query fields on-demand without schema changes."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## THE SOLUTION: VARIANT Type
# MAGIC
# MAGIC **What It Is:**
# MAGIC * Unity Catalog native type for semi-structured data (JSON, nested objects, arrays)
# MAGIC * Schema-on-read: Structure defined at query time, not table creation
# MAGIC * No ALTER TABLE needed when JSON structure changes
# MAGIC * Efficient storage (columnar compression even for nested data)
# MAGIC
# MAGIC **When to Use:**
# MAGIC * API responses with evolving schemas
# MAGIC * Event tracking (different event types, different fields)
# MAGIC * User preferences (varies by user segment)
# MAGIC * Metadata columns (product attributes, order extras)
# MAGIC
# MAGIC **When NOT to Use:**
# MAGIC * Data with stable schema (use typed columns instead)
# MAGIC * High-frequency analytical queries (typed columns are faster)
# MAGIC * Simple key-value pairs (use MAP type)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC **Required:**
# MAGIC * Unity Catalog enabled
# MAGIC * Understanding of JSON structure
# MAGIC * Basic SQL knowledge
# MAGIC
# MAGIC **Example Data:**
# MAGIC * Mobile app order metadata (device info, app version, preferences)
# MAGIC * SkillAIPath order extras (gift wrap, delivery instructions, promo codes)

# COMMAND ----------

# DBTITLE 1,Setup and Create Table
# VARIANT Type Setup

from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring
from pyspark.sql.types import *

print("="*70)
print("VARIANT TYPE - SEMI-STRUCTURED DATA HANDLING")
print("="*70)
print("   Pattern: Schema-on-read for flexible JSON")
print("   Use Case: Evolving schemas, API responses, metadata")
print("   Benefit: No ALTER TABLE on schema changes")
print("="*70)

# Configuration
dbutils.widgets.text("catalog", "sap_dev", "Catalog Name")
dbutils.widgets.text("schema", "bronze", "Schema Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table_name = f"{catalog}.{schema}.orders_with_metadata"

# Create table with VARIANT column
try:
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {table_name} (
        order_id STRING,
        customer_id STRING,
        order_date DATE,
        total_amount DECIMAL(10,2),
        metadata VARIANT COMMENT 'Flexible JSON metadata'
      ) USING DELTA
    """)
    
    print(f"\nTable created: {table_name}")
    print("metadata column: VARIANT (stores any JSON structure)")
    print("NOTE: Unity Catalog required for VARIANT type support")
except Exception as e:
    print(f"ERROR: Failed to create table {table_name}")
    print(f"Reason: {str(e)}")
    print("\nTroubleshooting:")
    print("  1. Verify Unity Catalog enabled")
    print("  2. Check schema exists: SHOW SCHEMAS IN sap_dev")
    print("  3. Verify VARIANT type support (DBR 13.3+)")

# COMMAND ----------

# DBTITLE 1,Insert Sample Data
# Read real JSON files from Unity Catalog Volume

from pyspark.sql.functions import parse_json, to_json, struct
from pyspark.sql.types import StringType

volume_path = "/Volumes/sap_dev/bronze/orders_metadata"

# Read all JSON files from volume
json_df = spark.read.option("multiLine", "true").json(f"{volume_path}/*.json")

print(f"\nLoaded {json_df.count()} orders from volume: {volume_path}")
print("Varying schemas detected (schema-on-read with VARIANT):")
json_df.printSchema()

# Transform to match table schema with VARIANT column
df_variant = json_df.select(
    col("order_id"),
    lit("CUST001").alias("customer_id"),
    lit("2026-07-09").cast("date").alias("order_date"),
    lit(1250.00).cast("decimal(10,2)").alias("total_amount"),
    to_json(struct("*")).alias("metadata_json")
).withColumn("metadata", parse_json(col("metadata_json"))).drop("metadata_json")

df_variant.write.mode("overwrite").saveAsTable(table_name)

print(f"\nInserted {df_variant.count()} orders with different JSON structures")
print("No schema changes needed when new fields appear")

# COMMAND ----------

# DBTITLE 1,Query VARIANT Fields
# MAGIC %sql
# MAGIC -- Extract fields from VARIANT column
# MAGIC SELECT 
# MAGIC   order_id,
# MAGIC   metadata:device::string as device,
# MAGIC   metadata:payment_method::string as payment_method,
# MAGIC   metadata:gift_wrap::boolean as gift_wrap,
# MAGIC   metadata:loyalty_points_used::int as loyalty_points
# MAGIC FROM sap_dev.bronze.orders_with_metadata;

# COMMAND ----------

# DBTITLE 1,Summary
print("="*70)
print("VARIANT TYPE - KEY TAKEAWAYS")
print("="*70)
print("\nWhen to Use:")
print("  - JSON from APIs with evolving schemas")
print("  - Event tracking with varying fields")
print("  - Metadata columns with flexible structure")
print("\nSyntax:")
print("  metadata:fieldname::type")
print("\nBenefits:")
print("  - No ALTER TABLE on schema changes")
print("  - Efficient columnar storage")
print("  - Query only needed fields")
print("="*70)