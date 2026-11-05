# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Orchestration Documentation
# MAGIC %md
# MAGIC # Incremental Load - Daily Pipeline Orchestration
# MAGIC
# MAGIC ## Overview
# MAGIC This orchestration runs the complete incremental data pipeline for SkillAIPath, processing daily data from Bronze → Silver → Gold layers with CDC-enabled fact and dimension loading.
# MAGIC
# MAGIC ## Pipeline Stages
# MAGIC
# MAGIC ### 1. **Data Generation (Incremental Mode)**
# MAGIC    - Generates today's incremental data (~250K sales records)
# MAGIC    - Tables: `stg_sales_incremental`, `stg_purchase_incremental`, `customer_changes_raw`
# MAGIC    - Duration: ~2-3 minutes
# MAGIC
# MAGIC ### 2. **Dimensions (Incremental)**
# MAGIC    - **Dimensions:** Date, Product Category, Product, Warehouse, Retailer, Agent, Payment Mode, Wholesaler
# MAGIC    - **Customer (CDC SCD Type 2):** Processes customer changes via CDC events from `customer_changes_raw`
# MAGIC    - Pattern: MERGE by business key, CDC operations (INSERT/UPDATE/DELETE)
# MAGIC    - Duration: ~4-6 minutes
# MAGIC
# MAGIC ### 3. **Facts (Incremental - CDC)**
# MAGIC    - **Sales Fact (CDC):** MERGE by OrderID from `stg_sales_incremental`
# MAGIC    - **Purchase Fact:** APPEND from `stg_purchase_incremental`
# MAGIC    - **Inventory Fact:** APPEND from `stg_stock_incremental`
# MAGIC    - Duration: ~5-7 minutes
# MAGIC
# MAGIC ### 4. **Validation Framework (Quality Gate)**
# MAGIC    - **Run BEFORE Gold to validate Silver layer data quality**
# MAGIC    - Comprehensive quality tests on incremental processing
# MAGIC    - Tests: SCD2 integrity, late arrivals, deduplication, watermarks, point-in-time joins, performance, edge cases
# MAGIC    - Duration: ~1 minute
# MAGIC    - **Blocks Gold layer if critical failures detected**
# MAGIC
# MAGIC ### 5. **Gold Layer (Incremental - Business Marts)**
# MAGIC    - Pre-aggregated business marts for dashboarding and analytics
# MAGIC    - Marts: Customer 360, RFM Analysis, Product Performance, Inventory Health
# MAGIC    - Pattern: Incremental aggregation from validated Silver layer
# MAGIC    - Duration: ~3-5 minutes
# MAGIC
# MAGIC ### 6. **Archive & Cleanup**
# MAGIC    - Merges incremental staging → history tables
# MAGIC    - Truncates incremental staging tables
# MAGIC    - Records audit metadata
# MAGIC    - Duration: ~1-2 minutes
# MAGIC
# MAGIC ## Total Runtime
# MAGIC **~15-20 minutes** for complete pipeline execution
# MAGIC
# MAGIC ## Key Features
# MAGIC - ✅ **Incremental Processing:** Watermark-based, processes only new data
# MAGIC - ✅ **CDC Enabled:** Real-time MERGE operations for facts and dimensions
# MAGIC - ✅ **Idempotent:** Safe to re-run (no duplicates)
# MAGIC - ✅ **Validated:** Quality tests at each layer
# MAGIC - ✅ **Complete:** Execution tracking and summary report
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Config setup completed (`1_config` notebook)
# MAGIC - Bronze staging tables available
# MAGIC - Proper catalog/schema permissions
# MAGIC - All dimension notebooks exist and are executable
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Configuration
# MAGIC %run /Shared/dev/SkillAIPath/1_Canvas/1_Setup/1_config

# COMMAND ----------

# DBTITLE 1,Imports and Setup
from pyspark.sql.functions import *
from datetime import datetime
import json

# Pipeline execution tracking
pipeline_start_time = datetime.now()
pipeline_name = "Incremental_Load"
stage_results = []

print(f"🚀 Starting {pipeline_name}")
print(f"⏰ Start Time: {pipeline_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"📁 Catalog: {Config.CATALOG}")
print(f"🔧 Environment: {Config.ENVIRONMENT}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Helper Functions
def run_notebook(path, timeout_seconds=1800, params=None):
    """
    Run a notebook and track execution time
    """
    if params is None:
        params = {}
    
    notebook_name = path.split('/')[-1]
    stage_start = datetime.now()
    
    print(f"\n{'='*80}")
    print(f"📓 Running: {notebook_name}")
    print(f"📂 Path: {path}")
    print(f"⏱️  Start: {stage_start.strftime('%H:%M:%S')}")
    
    try:
        result = dbutils.notebook.run(path, timeout_seconds, params)
        stage_end = datetime.now()
        duration = (stage_end - stage_start).total_seconds()
        
        print(f"✅ Success: {notebook_name}")
        print(f"⏱️  Duration: {duration:.2f} seconds")
        print(f"📊 Result: {result}")
        
        stage_results.append({
            "notebook": notebook_name,
            "status": "SUCCESS",
            "duration_seconds": duration,
            "result": result
        })
        
        return True
        
    except Exception as e:
        stage_end = datetime.now()
        duration = (stage_end - stage_start).total_seconds()
        
        print(f"❌ Failed: {notebook_name}")
        print(f"⏱️  Duration: {duration:.2f} seconds")
        print(f"⚠️  Error: {str(e)}")
        
        stage_results.append({
            "notebook": notebook_name,
            "status": "FAILED",
            "duration_seconds": duration,
            "error": str(e)
        })
        
        raise

# COMMAND ----------

# DBTITLE 1,Run Data Generator
# ============================================================
# STAGE 1: Data Generation (Incremental Mode)
# ============================================================
print("\n🔵 STAGE 1: DATA GENERATION (INCREMENTAL)\n")

run_notebook(
    path=f"/Workspace/Shared/{Config.ENVIRONMENT}/SkillAIPath/1_Canvas/1_Setup/2_data_generator",
    timeout_seconds=600,
    params={"mode": "incremental"}
)

# COMMAND ----------

# DBTITLE 1,Run Dimensions
# ============================================================
# STAGE 2: DIMENSIONS (INCREMENTAL)
# ============================================================
print("\n🟢 STAGE 2: DIMENSIONS (INCREMENTAL)\n")

# Dynamic path construction
base_dim_path = f"/Workspace/Shared/{Config.ENVIRONMENT}/SkillAIPath/2_Data_Production/02_Medallion_Architecture/01_Dimensions"

dimensions = [
    ("dim_date", f"{base_dim_path}/01_dim_date"),
    ("dim_product_category", f"{base_dim_path}/02_dim_product_category"),
    ("dim_warehouse", f"{base_dim_path}/03_dim_warehouse_nb"),
    ("dim_retailer", f"{base_dim_path}/04_dim_retailer"),
    ("dim_agent", f"{base_dim_path}/05_dim_agent"),
    ("dim_payment_mode", f"{base_dim_path}/06_dim_payment_mode"),
    ("dim_product", f"{base_dim_path}/07_dim_product_nb"),
    ("dim_wholesaler", f"{base_dim_path}/13_dim_wholesaler"),
]

for name, path in dimensions:
    print(f"Building {name}...")
    run_notebook(path=path, timeout_seconds=600)
    print(f"✅ {name}")

# Customer Dimension (CDC SCD Type 2 - Incremental)
print("\n🔵 Building dim_customer (CDC SCD2)...")
run_notebook(
    path=f"/Workspace/Shared/{Config.ENVIRONMENT}/SkillAIPath/2_Data_Production/02_Medallion_Architecture/03_Streaming_Cdc/06_customer_scd2_cdc",
    timeout_seconds=900
)
print("✅ dim_customer")

# COMMAND ----------

# DBTITLE 1,Run Fact Tables
# ============================================================
# STAGE 3: FACT TABLES (INCREMENTAL - CDC)
# ============================================================
print("\n🟠 STAGE 3: FACT TABLES (INCREMENTAL - CDC)\n")

# Dynamic path construction
base_fact_path = f"/Workspace/Shared/{Config.ENVIRONMENT}/SkillAIPath/2_Data_Production/02_Medallion_Architecture"

facts = [
    ("fact_sales", f"{base_fact_path}/03_Streaming_Cdc/07_sales_fact_cdc"),
    ("fact_purchases", f"{base_fact_path}/02_Facts/02_fact_purchases"),
    ("fact_inventory", f"{base_fact_path}/02_Facts/03_fact_inventory"),
]

for name, path in facts:
    print(f"Building {name}...")
    run_notebook(path=path, timeout_seconds=1200)
    print(f"✅ {name}")

# COMMAND ----------

# DBTITLE 1,Run Validation Framework
# ============================================================
# STAGE 4: Validation Framework (Before Gold Layer)
# ============================================================
print("\n🔵 STAGE 4: VALIDATION FRAMEWORK\n")

run_notebook(
    path=f"/Workspace/Shared/{Config.ENVIRONMENT}/SkillAIPath/1_Canvas/8_Validation_Framework/3_incremental_test_suite",
    timeout_seconds=600
)

# COMMAND ----------

# DBTITLE 1,Run Gold Layer
# ============================================================
# STAGE 5: GOLD LAYER (INCREMENTAL - BUSINESS MARTS)
# ============================================================
print("\n🟣 STAGE 5: GOLD LAYER (INCREMENTAL - BUSINESS MARTS)\n")

# Dynamic path construction
base_gold_path = f"/Workspace/Shared/{Config.ENVIRONMENT}/SkillAIPath/2_Data_Production/02_Medallion_Architecture/04_Gold"

gold_marts = [
    ("mart_customer_360", f"{base_gold_path}/01_mart_customer_360"),
    ("mart_rfm_analysis", f"{base_gold_path}/02_mart_rfm_analysis"),
    ("mart_product_performance", f"{base_gold_path}/03_mart_product_performance"),
    ("mart_inventory_health", f"{base_gold_path}/04_mart_inventory_health"),
]

for name, path in gold_marts:
    print(f"Building {name}...")
    run_notebook(path=path, timeout_seconds=900)
    print(f"✅ {name}")

# COMMAND ----------

# DBTITLE 1,Stage 6: Data Quality Monitoring
# ============================================================
# STAGE 6: DATA QUALITY MONITORING
# ============================================================
print("\n🔵 STAGE 6: DATA QUALITY MONITORING\n")

run_notebook(
    path=f"/Workspace/Shared/{Config.ENVIRONMENT}/SkillAIPath/1_Canvas/8_Validation_Framework/2_monitoring_quality_tests",
    timeout_seconds=900,
    params={"load_type": "full"}
)

print("\n✅ Data quality validation completed")

# COMMAND ----------

# DBTITLE 1,Run Archive & Cleanup
# ============================================================
# STAGE 7: Archive & Cleanup
# ============================================================
print("\n⚫ STAGE 7: ARCHIVE & CLEANUP\n")

run_notebook(
    path=f"/Workspace/Shared/{Config.ENVIRONMENT}/SkillAIPath/1_Canvas/1_Setup/5_archive_and_cleanup",
    timeout_seconds=600,
    params={
        "environment": Config.ENVIRONMENT,
        "retention_days": "0",
        "validate_downstream": "true",
        "dry_run": "false"
    }
)

# COMMAND ----------

# DBTITLE 1,Generate Summary Report
pipeline_end_time = datetime.now()
total_duration = (pipeline_end_time - pipeline_start_time).total_seconds()

print("\n" + "="*80)
print("📊 PIPELINE EXECUTION SUMMARY")
print("="*80)
print(f"Pipeline: {pipeline_name}")
print(f"Start Time: {pipeline_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"End Time: {pipeline_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Total Duration: {total_duration:.2f} seconds ({total_duration/60:.2f} minutes)")
print("\n" + "-"*80)
print(f"{'Notebook':<50} {'Status':<10} {'Duration (s)':<15}")
print("-"*80)

total_success = 0
total_failed = 0

for result in stage_results:
    notebook = result['notebook']
    status = result['status']
    duration = result['duration_seconds']
    
    status_icon = "✅" if status == "SUCCESS" else "❌"
    print(f"{status_icon} {notebook:<48} {status:<10} {duration:>10.2f}")
    
    if status == "SUCCESS":
        total_success += 1
    else:
        total_failed += 1

print("-"*80)
print(f"\nTotal Notebooks: {len(stage_results)}")
print(f"✅ Successful: {total_success}")
print(f"❌ Failed: {total_failed}")
print("\n" + "="*80)

if total_failed == 0:
    print("\n🎉 PIPELINE COMPLETED SUCCESSFULLY!")
    dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "duration_seconds": total_duration}))
else:
    print("\n⚠️  PIPELINE COMPLETED WITH ERRORS")
    dbutils.notebook.exit(json.dumps({"status": "FAILED", "duration_seconds": total_duration, "failed_count": total_failed}))

# COMMAND ----------

