# Databricks notebook source
# DBTITLE 1,Lakeflow Connect - External CDC Sources
# MAGIC %md
# MAGIC # Lakeflow Connect: Managed Ingestion from External Sources
# MAGIC **SkillAIPath Streaming - Zero-Code CDC Integration**
# MAGIC
# MAGIC ## THE PROBLEM: "How do we stream from MySQL without writing Kafka connectors?"
# MAGIC
# MAGIC **Data Engineer:** "Our orders table is in MySQL. We need real-time CDC. Do we set up Debezium, Kafka, schema registry?"
# MAGIC
# MAGIC **Architect:** "Lakeflow Connect does this for you - managed CDC ingestion from MySQL, Postgres, Salesforce, Kafka. No connectors to maintain."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## THE SOLUTION: Lakeflow Connect
# MAGIC
# MAGIC **What It Is:**
# MAGIC * Managed ingestion pipelines (formerly DLT with CDC sources)
# MAGIC * Zero-code CDC from databases (MySQL, Postgres, SQL Server)
# MAGIC * SaaS connectors (Salesforce, Workday, ServiceNow)
# MAGIC * Auto-managed schema evolution, checkpointing, error handling
# MAGIC
# MAGIC **Supported Sources:**
# MAGIC * Databases: MySQL, PostgreSQL, SQL Server, Oracle
# MAGIC * SaaS: Salesforce, Workday, ServiceNow
# MAGIC * Streaming: Kafka, Kinesis, Event Hubs
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC * Understanding of CDC concepts
# MAGIC * Database credentials / API tokens
# MAGIC * Unity Catalog catalog and schema

# COMMAND ----------

# DBTITLE 1,Lakeflow Connect Configuration
print('='*70)
print('LAKEFLOW CONNECT - MANAGED INGESTION')
print('='*70)
print('   Pattern: Zero-code CDC from external sources')
print('   Setup: UI-based pipeline creation')
print('   Output: Bronze Delta tables in Unity Catalog')
print('='*70)

print('\nConfiguration Example (MySQL):')
print('-'*70)
print('Source Type: MySQL')
print('Host: mysql.sap.com')
print('Port: 3306')
print('Database: ecommerce')
print('Tables: orders, order_items')
print('CDC Method: Binary Log (binlog)')
print('-'*70)
print('\nTarget:')
print('Catalog: sap')
print('Schema: bronze')
print('Tables: orders_cdc, order_items_cdc')
print('='*70)

# COMMAND ----------

# DBTITLE 1,CDC Metadata Columns
print('='*70)
print('CDC METADATA - TRACKING CHANGES')
print('='*70)

print('\nLakeflow Connect adds CDC metadata columns:')
print('-'*70)
print('_change_type:  INSERT, UPDATE, DELETE')
print('_commit_version:  Source transaction ID')
print('_commit_timestamp:  When change occurred in source')
print('_rescued_data:  Malformed records')
print('-'*70)

print('\nQuery Pattern:')
print('SELECT * FROM {Config.SCHEMA_BRONZE}.orders_cdc')
print('WHERE _change_type = "UPDATE"')
print('  AND _commit_timestamp > current_timestamp() - INTERVAL 1 HOUR')
print('='*70)

# COMMAND ----------

# DBTITLE 1,Summary
print('='*70)
print('LAKEFLOW CONNECT - KEY TAKEAWAYS')
print('='*70)
print('\nBenefits:')
print('  - No custom connector code')
print('  - Managed schema evolution')
print('  - Auto-checkpointing and error recovery')
print('  - Native Unity Catalog integration')

print('\nWhen to Use:')
print('  - External database CDC (MySQL, Postgres)')
print('  - SaaS data ingestion (Salesforce, Workday)')
print('  - Replacing custom Kafka connectors')

print('\nSetup Process:')
print('  1. Create connection (credentials, host)')
print('  2. Select tables to sync')
print('  3. Choose target catalog.schema')
print('  4. Start pipeline (CDC begins automatically)')

print('\nInterview Talking Point:')
print('  "For external CDC, we use Lakeflow Connect instead')
print('   of managing Debezium/Kafka. It handles binlog parsing,')
print('   schema evolution, and checkpointing. We just configure')
print('   source credentials and target Unity Catalog location."')
print('='*70)
