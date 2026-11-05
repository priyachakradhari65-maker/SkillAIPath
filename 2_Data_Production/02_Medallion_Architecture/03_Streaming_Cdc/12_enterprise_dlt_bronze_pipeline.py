# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # DLT Bronze Pipeline - Auto Loader Ingestion
# MAGIC
# MAGIC **🧪 USING STREAMING TEST DATA (from notebook 00)**
# MAGIC
# MAGIC Ingests streaming test data using Auto Loader with schema inference and evolution.
# MAGIC
# MAGIC Pipeline Configuration:
# MAGIC - Source: Streaming test volume (same as notebooks 00 → 01)
# MAGIC - Target: sap_dev.bronze schema
# MAGIC - Mode: Streaming (continuous)
# MAGIC - Features: Schema evolution, rescued data, audit columns, checkpoints

# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp, expr

# Load configuration
%run ../../../1_Canvas/1_Setup/1_Config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Table: Sales Transactions

# COMMAND ----------

# DBTITLE 1,Bronze Table: Sales Transactions
@dlt.table(
    name="strm_sales_raw",
    comment="Production streaming sales data ingested via Auto Loader from production volume",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect("has_customer_id", "CustomerID IS NOT NULL")  # Warning only
@dlt.expect("valid_amount", "TotalAmount >= 0")  # Warning only
def bronze_sales_test():
    # FIX (2026-11-05): env-aware volume paths. Was hardcoded 'sap_dev', which breaks any
    # non-dev environment (new-env Job 09 AnalysisException at .load()). The volume is created
    # and populated by 2_DataGenerator in INCREMENTAL mode only, so in a fresh environment
    # the producer (Job 05 generator task) must run at least once before this DLT pipeline.
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")  # Test data is CSV
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", f"{Config.VOL_CHECKPOINTS}/production_dlt/bronze_strm_sales_schema")
        .option("pathGlobFilter", "*.csv")  # Only read CSV files
        .load(f"/Volumes/{Config.CATALOG}/{Config.SCHEMA_BRONZE}/streaming_production_data")
        .withColumn("_input_file", expr("_metadata.file_path"))
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_system", expr("'PRODUCTION_STREAMING'"))
    )