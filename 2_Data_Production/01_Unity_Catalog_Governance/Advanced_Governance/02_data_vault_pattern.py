# Databricks notebook source
# DBTITLE 1,Data Vault 2.0 Pattern
# MAGIC %md
# MAGIC # Data Vault 2.0: Enterprise Modeling Pattern
# MAGIC **SkillAIPath Data Warehouse - Architecture Comparison**
# MAGIC
# MAGIC ## THE PROBLEM: "How do we model data for strict audit requirements?"
# MAGIC
# MAGIC **Wednesday Morning - Compliance Discussion**
# MAGIC
# MAGIC **Compliance Officer:** "Our auditors need complete history - who changed what, when, and why. Can our current Medallion architecture provide this?"
# MAGIC
# MAGIC **Data Engineer:** "We use SCD-2 for dimension history. We track effective dates and current flags."
# MAGIC
# MAGIC **Officer:** "What about relationships? If a customer changes supplier, can we see the history of that relationship?"
# MAGIC
# MAGIC **Engineer:** "We'd need to join dimension histories. It gets complex."
# MAGIC
# MAGIC **Architect:** "This is where Data Vault shines. It separates business keys (Hubs), attributes (Satellites), and relationships (Links) - making audit trails natural."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## THE SOLUTION: Data Vault 2.0
# MAGIC
# MAGIC **Core Concepts:**
# MAGIC
# MAGIC **Hubs:** Business keys only
# MAGIC * hub_customer (customer_id, load_date)
# MAGIC * hub_product (product_id, load_date)
# MAGIC
# MAGIC **Satellites:** Descriptive attributes with history
# MAGIC * sat_customer_profile (name, email, phone, valid_from, valid_to)
# MAGIC * sat_product_details (name, category, price, valid_from, valid_to)
# MAGIC
# MAGIC **Links:** Relationships between hubs
# MAGIC * link_customer_product (customer_id, product_id, relationship_type, load_date)
# MAGIC
# MAGIC **When to Use:**
# MAGIC * Highly regulated industries (banking, healthcare, government)
# MAGIC * Complex audit requirements
# MAGIC * Many-to-many relationships with history
# MAGIC * Enterprise data warehouses with strict governance
# MAGIC
# MAGIC **When NOT to Use:**
# MAGIC * Simple reporting needs (Medallion + Star Schema sufficient)
# MAGIC * Small teams (Data Vault adds complexity)
# MAGIC * Fast-moving startups (overhead not justified)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC **Required:**
# MAGIC * Module 01_DIMENSIONS complete (understanding of SCD-2)
# MAGIC * Understanding of Medallion architecture
# MAGIC * Familiarity with dimensional modeling
# MAGIC
# MAGIC **Comparison:**
# MAGIC * We'll model supplier dimension in both approaches

# COMMAND ----------

# DBTITLE 1,Traditional SCD-2 Approach
# MAGIC %sql
# MAGIC -- Use existing bronze schema for Data Vault demo
# MAGIC -- Traditional SCD-2: supplier dimension with history
# MAGIC CREATE OR REPLACE TABLE sap_dev.bronze.dim_supplier_scd2 (
# MAGIC   supplier_id STRING,
# MAGIC   supplier_name STRING,
# MAGIC   city STRING,
# MAGIC   state STRING,
# MAGIC   category STRING,
# MAGIC   effective_date DATE,
# MAGIC   end_date DATE,
# MAGIC   is_current BOOLEAN
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Sample data
# MAGIC INSERT INTO sap_dev.bronze.dim_supplier_scd2 VALUES
# MAGIC   ('SUP001', 'ABC Foods', 'Mumbai', 'Maharashtra', 'Groceries', '2024-01-01', '2026-03-31', false),
# MAGIC   ('SUP001', 'ABC Foods Pvt Ltd', 'Pune', 'Maharashtra', 'Groceries', '2026-04-01', '9999-12-31', true);
# MAGIC
# MAGIC -- Query: Shows history but relationships to products lost

# COMMAND ----------

# DBTITLE 1,Data Vault - Hub Satellite Link
# MAGIC %sql
# MAGIC -- Data Vault Approach
# MAGIC
# MAGIC -- Hub: Business keys only
# MAGIC CREATE OR REPLACE TABLE sap_dev.bronze.hub_supplier (
# MAGIC   supplier_key STRING,
# MAGIC   supplier_id STRING,
# MAGIC   load_date TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Satellite: Descriptive attributes with history
# MAGIC CREATE OR REPLACE TABLE sap_dev.bronze.sat_supplier_profile (
# MAGIC   supplier_key STRING,
# MAGIC   supplier_name STRING,
# MAGIC   city STRING,
# MAGIC   state STRING,
# MAGIC   category STRING,
# MAGIC   valid_from TIMESTAMP,
# MAGIC   valid_to TIMESTAMP,
# MAGIC   is_current BOOLEAN,
# MAGIC   record_source STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Link: Relationships
# MAGIC CREATE OR REPLACE TABLE sap_dev.bronze.link_supplier_product (
# MAGIC   link_key STRING,
# MAGIC   supplier_key STRING,
# MAGIC   product_key STRING,
# MAGIC   relationship_type STRING,
# MAGIC   load_date TIMESTAMP
# MAGIC ) USING DELTA;

# COMMAND ----------

# DBTITLE 1,Load Hub and Satellite
from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring
from datetime import datetime

# Load Hub (business keys)
hub_data = [
    ('HUB_SUP001', 'SUP001', datetime(2024, 1, 1)),
    ('HUB_SUP002', 'SUP002', datetime(2024, 1, 1))
]
spark.createDataFrame(hub_data, ['supplier_key', 'supplier_id', 'load_date']).write.mode('overwrite').saveAsTable('sap_dev.bronze.hub_supplier')

# Load Satellite (attributes with history)
sat_data = [
    ('HUB_SUP001', 'ABC Foods', 'Mumbai', 'Maharashtra', 'Groceries', datetime(2024,1,1), datetime(2026,3,31,23,59,59), False, 'ERP'),
    ('HUB_SUP001', 'ABC Foods Pvt Ltd', 'Pune', 'Maharashtra', 'Groceries', datetime(2026,4,1), datetime(9999,12,31), True, 'ERP'),
]
spark.createDataFrame(sat_data, ['supplier_key', 'supplier_name', 'city', 'state', 'category', 'valid_from', 'valid_to', 'is_current', 'record_source']).write.mode('overwrite').saveAsTable('sap_dev.bronze.sat_supplier_profile')

print('Hub and Satellite loaded')
print('Separation: business keys vs attributes allows independent history tracking')

# COMMAND ----------

# DBTITLE 1,Query Historical Relationships
# MAGIC %sql
# MAGIC -- Data Vault query: Join Hub + Satellite for current state
# MAGIC SELECT 
# MAGIC   h.supplier_id,
# MAGIC   s.supplier_name,
# MAGIC   s.city,
# MAGIC   s.state
# MAGIC FROM sap_dev.bronze.hub_supplier h
# MAGIC JOIN sap_dev.bronze.sat_supplier_profile s
# MAGIC   ON h.supplier_key = s.supplier_key
# MAGIC WHERE s.is_current = true;
# MAGIC
# MAGIC -- Audit trail: Complete history of attribute changes
# MAGIC SELECT 
# MAGIC   h.supplier_id,
# MAGIC   s.supplier_name,
# MAGIC   s.city,
# MAGIC   s.valid_from,
# MAGIC   s.valid_to
# MAGIC FROM sap_dev.bronze.hub_supplier h
# MAGIC JOIN sap_dev.bronze.sat_supplier_profile s
# MAGIC   ON h.supplier_key = s.supplier_key
# MAGIC ORDER BY h.supplier_id, s.valid_from;

# COMMAND ----------

# DBTITLE 1,Summary and Comparison
print('='*70)
print('DATA VAULT VS MEDALLION ARCHITECTURE')
print('='*70)
print('\nMedallion (Dimensional Modeling):')
print('  - Simpler to understand and implement')
print('  - Faster query performance (denormalized)')
print('  - Suitable for most analytics use cases')
print('\nData Vault 2.0:')
print('  - Stricter audit trails (who, what, when)')
print('  - Relationship history preserved')
print('  - Complex queries (more joins)')
print('  - Overhead not justified for simple reporting')
print('\nSkillAIPath Recommendation:')
print('  - Use Medallion + SCD-2 for standard dimensions')
print('  - Consider Data Vault only if:')
print('    - Regulated industry with strict audit requirements')
print('    - Complex many-to-many relationships')
print('    - Enterprise-scale with dedicated data modeling team')
print('='*70)