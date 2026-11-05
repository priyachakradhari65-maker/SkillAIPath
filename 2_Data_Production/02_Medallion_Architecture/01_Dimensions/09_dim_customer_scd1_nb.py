# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,SCD Type 1: Overwrite Pattern
# MAGIC %md
# MAGIC # Dimension: Customer (SCD Type 1 - Overwrite)
# MAGIC **SkillAIPath - Pattern: Overwrite Historical Changes**
# MAGIC
# MAGIC ## THE SITUATION: "We Don't Need History"
# MAGIC
# MAGIC **Analyst:** "Our customer email and phone number change frequently. We don't need to track the old values - just keep the latest."
# MAGIC
# MAGIC **Data Engineer:** "SCD Type 1 is perfect for this. We'll:
# MAGIC 1. Match on customer_id (business key)
# MAGIC 2. UPDATE existing records with new values
# MAGIC 3. INSERT new customers
# MAGIC 4. No history tracking - simple MERGE statement"
# MAGIC
# MAGIC **INTERVIEW HIGHLIGHT**: "I implemented SCD Type 1 for customer contact info where historical tracking wasn't required. Used MERGE with WHEN MATCHED THEN UPDATE to overwrite changes in-place."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Pattern Comparison
# MAGIC
# MAGIC | Pattern | Use Case | Records per Customer | Query Complexity |
# MAGIC |---------|----------|---------------------|------------------|
# MAGIC | **SCD1** | Contact info (email, phone) | 1 (always current) | Simple |
# MAGIC | **SCD2** | Address changes (auditable) | Multiple (history) | Medium |
# MAGIC | **Full Truncate** | Static dimensions (date, product_category) | 1 (reload all) | Simple |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %run ../../../1_Canvas/1_Setup/1_Config

# COMMAND ----------

# DBTITLE 1,Setup Parameters
# %run ../../1_Canvas/1_Setup/1_config

from pyspark.sql.functions import col, current_timestamp, lit

print("CUSTOMER DIMENSION (SCD TYPE 1)")
print(f"Source: {Config.TBL_STG_SALES_INCREMENTAL}")
print(f"Target: {Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_customer_scd1")
print(f"Pattern: Overwrite (no history tracking)")
print(f"Business Key: CustomerName")

# COMMAND ----------

# DBTITLE 1,Extract Source Data
# Read latest customer data from bronze staging incremental table
stg_customers = spark.table(Config.TBL_STG_SALES_INCREMENTAL).select(
    "CustomerName",
    "CustomerMail",
    "CustomerContact",
    "CustomerAddress",
    "CustomerAge",
    "CustomerGender"
).distinct()

print(f"Staging records: {stg_customers.count():,}")
print("\nSample staging data:")
display(stg_customers.limit(5))

# COMMAND ----------

# DBTITLE 1,Create Table Schema
from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql.functions import row_number, col

# Deduplicate by CustomerName + CustomerMail + CustomerContact - preserves distinct people with same name
dedup_window = Window.partitionBy("CustomerName", "CustomerMail", "CustomerContact").orderBy("CustomerName")
stg_customers = stg_customers.withColumn("_rn", row_number().over(dedup_window)).filter(col("_rn") == 1).drop("_rn")

# Prepare staging data with metadata - use ACTUAL customer data from stg_sales!
window_spec = Window.orderBy("CustomerName")

stg_df = stg_customers \
    .withColumn("customer_id", row_number().over(window_spec)) \
    .withColumn("customer_name", col("CustomerName")) \
    .withColumn("email", col("CustomerMail")) \
    .withColumn("phone", col("CustomerContact")) \
    .withColumn("city", col("CustomerAddress")) \
    .withColumn("age", col("CustomerAge")) \
    .withColumn("gender", col("CustomerGender")) \
    .withColumn("updated_at", current_timestamp()) \
    .select(
        "customer_id",
        "customer_name",
        "email",
        "phone",
        "city",
        "age",
        "gender",
        "updated_at"
    )

# Note: Removed columns not in data_gen:
# - state, pincode (not generated)
# - registration_date (not in stg_sales)

# Check if target table exists
table_name = f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_customer_scd1"

if spark.catalog.tableExists(table_name):
    print("Target table exists - cleaning duplicates then performing MERGE")
    
    # Pre-MERGE cleanup: Remove true duplicates (same name+email+phone), keep lowest customer_id
    spark.sql(f"""
        DELETE FROM {table_name}
        WHERE customer_id NOT IN (
            SELECT MIN(customer_id)
            FROM {table_name}
            GROUP BY customer_name, email, phone
        )
    """)
    print("Duplicate cleanup completed")
    
    # Load existing dimension
    target_table = DeltaTable.forName(spark, table_name)
    
    # SCD Type 1: MERGE with UPDATE (overwrite changes)
    # Note: Target table uses PascalCase column names
    # FIX: Use lowercase column names matching actual target table schema
    target_table.alias("target").merge(
        stg_df.alias("source"),
        "target.customer_name = source.customer_name AND target.email = source.email AND target.phone = source.phone"
    ).whenMatchedUpdate(
        set={
            "city": "source.city",
            "age": "source.age",
            "gender": "source.gender",
            "updated_at": "source.updated_at"
        }
    ).whenNotMatchedInsert(
        values={
            "customer_id": "source.customer_id",
            "customer_name": "source.customer_name",
            "email": "source.email",
            "phone": "source.phone",
            "city": "source.city",
            "age": "source.age",
            "gender": "source.gender",
            "updated_at": "source.updated_at"
        }
    ).execute()
    
    print("MERGE completed (overwrites + inserts)")
    
else:
    print("Target table doesn't exist - creating with initial load")
    stg_df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print("Initial load completed")

print("\nSCD1 Pattern: Simple MERGE - existing records updated in-place, no history")

# COMMAND ----------

# DBTITLE 1,Generate Surrogate Keys & MERGE
# Preview dim_customer_scd1
query = f"""
SELECT * FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_customer_scd1
LIMIT 20
"""

display(spark.sql(query))
