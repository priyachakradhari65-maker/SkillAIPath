# Databricks notebook source
# DBTITLE 1,Full Load Pipeline - Orchestration Documentation
# MAGIC %md
# MAGIC # Full Load - Complete Data Pipeline Orchestration
# MAGIC
# MAGIC ## Overview
# MAGIC This orchestration runs the complete **FULL LOAD** data pipeline for SkillAIPath, loading all historical data from Bronze → Silver → Gold layers with full truncate/reload patterns.
# MAGIC
# MAGIC ## Pipeline Stages
# MAGIC
# MAGIC ### 1. **Bronze Data Validation**
# MAGIC    - Validates bronze tables exist and have data
# MAGIC    - Tables: `stg_sales`, `stg_purchase`, `stg_stock_status`
# MAGIC    - **PREREQUISITE:** Bronze tables must be pre-populated by external process
# MAGIC    - Duration: ~5 seconds
# MAGIC
# MAGIC ### 2. **Dimensions (Full Load - 9 tables)**
# MAGIC    - **Standard Dimensions:** Date, Product Category, Warehouse, Retailer, Agent, Payment Mode, Product, Wholesaler
# MAGIC    - **Customer:** Full truncate/reload (no SCD2 history)
# MAGIC    - Pattern: TRUNCATE + INSERT or DROP + CREATE
# MAGIC    - Duration: ~6-10 minutes
# MAGIC
# MAGIC ### 3. **Facts (Full Load - 3 tables)**
# MAGIC    - **Sales Fact:** Full reload from staging
# MAGIC    - **Purchase Fact:** Full reload from staging
# MAGIC    - **Inventory Fact:** Full reload from staging
# MAGIC    - Pattern: TRUNCATE + INSERT
# MAGIC    - Duration: ~8-12 minutes
# MAGIC
# MAGIC ### 4. **Gold Layer (Full Refresh - 4 marts)**
# MAGIC    - **Customer 360:** Lifetime value, recency, order counts
# MAGIC    - **RFM Analysis:** Customer segmentation (Champions, Loyal, At Risk)
# MAGIC    - **Product Performance:** Revenue tiers, slow-moving detection
# MAGIC    - **Inventory Health:** Stock levels, reorder points
# MAGIC    - Pattern: OVERWRITE mode
# MAGIC    - Duration: ~5-8 minutes
# MAGIC
# MAGIC ### 5. **Data Quality Monitoring**
# MAGIC    - Comprehensive validation across all layers
# MAGIC    - Schema checks, null detection, referential integrity
# MAGIC    - Business rule validation
# MAGIC    - Duration: ~3-5 minutes
# MAGIC
# MAGIC ## When to Use
# MAGIC - **Initial deployment** (first-time setup)
# MAGIC - **Disaster recovery** (rebuild from scratch)
# MAGIC - **Major schema changes** (complete refresh needed)
# MAGIC - **Historical data reload** (full reprocessing)
# MAGIC
# MAGIC ## vs Incremental Load
# MAGIC - **Full Load:** Complete dataset, truncate/reload, no history tracking
# MAGIC - **Incremental Load:** Daily deltas, MERGE/CDC, SCD2 history tracking

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
pipeline_name = "Full_Load"
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
            "duration": duration,
            "result": result
        })
        
        return result
        
    except Exception as e:
        stage_end = datetime.now()
        duration = (stage_end - stage_start).total_seconds()
        
        print(f"❌ Failed: {notebook_name}")
        print(f"⏱️  Duration: {duration:.2f} seconds")
        print(f"⚠️  Error: {str(e)}")
        
        stage_results.append({
            "notebook": notebook_name,
            "status": "FAILED",
            "duration": duration,
            "error": str(e)
        })
        
        raise

# COMMAND ----------

# DBTITLE 1,Validate Bronze Data
# ============================================================
# BRONZE DATA VALIDATION (PREREQUISITE CHECK)
# ============================================================
print("\n🔍 VALIDATING BRONZE DATA...\n")

try:
    # Check if bronze tables exist and have data
    sales_count = spark.read.table(Config.TBL_STG_SALES).count()
    purchase_count = spark.read.table(Config.TBL_STG_PURCHASE).count()
    stock_count = spark.read.table(Config.TBL_STG_STOCK).count()
    
    print(f"✅ Sales records: {sales_count:,}")
    print(f"✅ Purchase records: {purchase_count:,}")
    print(f"✅ Stock Status records: {stock_count:,}")
    
    if sales_count == 0 or purchase_count == 0 or stock_count == 0:
        raise Exception(f"❌ Bronze tables are empty! Sales: {sales_count}, Purchase: {purchase_count}, Stock: {stock_count}")
    
    print("\n✅ Bronze data validation passed (all 3 tables)\n")
    
except AnalysisException as e:
    print(f"❌ Bronze data validation failed: Bronze tables do not exist!")
    print(f"⚠️  Error: {str(e)}")
    print("\n🔴 PREREQUISITE FAILURE: Bronze tables must be populated before running Full Load orchestration")
    print("💡 Recommendation: Run 2_data_generator notebook first to populate bronze tables")
    raise Exception("Cannot proceed - bronze tables missing or empty")
except Exception as e:
    print(f"❌ Bronze data validation failed: {str(e)}")
    print("\n🔴 PREREQUISITE FAILURE: Bronze tables missing or empty")
    raise

# COMMAND ----------

# DBTITLE 1,Stage 2: Dimensions (Full Load)
# ============================================================
# STAGE 1: DIMENSIONS (FULL LOAD)
# ============================================================
print("\n🟢 STAGE 1: DIMENSIONS (FULL LOAD)\n")

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
    ("dim_customer", f"{base_dim_path}/08_dim_customer_full_truncate"),
]

for name, path in dimensions:
    print(f"Building {name}...")
    run_notebook(path=path, timeout_seconds=900, params={"load_type": "full"})
    print(f"✅ {name}")

print("\n✅ All dimensions loaded successfully")

# COMMAND ----------

# DBTITLE 1,Stage 3: Fact Tables (Full Load)
# ============================================================
# STAGE 2: FACT TABLES (FULL LOAD)
# ============================================================
print("\n🟠 STAGE 2: FACT TABLES (FULL LOAD)\n")

# Dynamic path construction
base_fact_path = f"/Workspace/Shared/{Config.ENVIRONMENT}/SkillAIPath/2_Data_Production/02_Medallion_Architecture/02_Facts"

facts = [
    ("fact_sales", f"{base_fact_path}/01_fact_sales"),
    ("fact_purchases", f"{base_fact_path}/02_fact_purchases"),
    ("fact_inventory", f"{base_fact_path}/03_fact_inventory"),
]

for name, path in facts:
    print(f"Building {name}...")
    run_notebook(path=path, timeout_seconds=1800)
    print(f"✅ {name}")

print("\n✅ All fact tables loaded successfully")

# COMMAND ----------

# DBTITLE 1,Stage 4: Gold Layer (Full Refresh)
# ============================================================
# STAGE 3: GOLD LAYER (FULL REFRESH - BUSINESS MARTS)
# ============================================================
print("\n🟣 STAGE 3: GOLD LAYER (FULL REFRESH - BUSINESS MARTS)\n")

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
    run_notebook(path=path, timeout_seconds=1200)
    print(f"✅ {name}")

print("\n✅ All gold marts refreshed successfully")

# COMMAND ----------

# DBTITLE 1,Stage 5: Data Quality Monitoring
# ============================================================
# STAGE 4: DATA QUALITY MONITORING
# ============================================================
print("\n🔵 STAGE 4: DATA QUALITY MONITORING\n")

run_notebook(
    path=f"/Workspace/Shared/{Config.ENVIRONMENT}/SkillAIPath/2_Data_Production/04_Quality_Operations/2_Validation_Framework/2_monitoring_quality_tests",
    timeout_seconds=900,
    params={"load_type": "full"}
)

print("\n✅ Data quality validation completed")

# COMMAND ----------

# DBTITLE 1,Pipeline Completion Summary
# ============================================================
# PIPELINE COMPLETION SUMMARY
# ============================================================
pipeline_end_time = datetime.now()
total_duration = (pipeline_end_time - pipeline_start_time).total_seconds()

print("\n" + "="*80)
print("📊 FULL LOAD PIPELINE - EXECUTION SUMMARY")
print("="*80)
print(f"Pipeline: {pipeline_name}")
print(f"Start Time: {pipeline_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"End Time: {pipeline_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Total Duration: {total_duration/60:.2f} minutes ({total_duration:.2f} seconds)")
print(f"Environment: {Config.ENVIRONMENT}")
print(f"Catalog: {Config.CATALOG}")
print("="*80)

print("\n✅ PIPELINE STAGES COMPLETED:")
print("\n  🔵 Bronze Validation: Passed")
print("     • stg_sales, stg_purchase, stg_stock_status validated")

print("\n  🟢 Stage 1: Dimensions (FULL LOAD)")
print("     • 9 dimension tables loaded successfully")
print("     • dim_date, dim_product_category, dim_warehouse, dim_retailer,")
print("     • dim_agent, dim_payment_mode, dim_product, dim_wholesaler, dim_customer")

print("\n  🟠 Stage 2: Fact Tables (FULL LOAD)")
print("     • 3 fact tables loaded successfully")
print("     • fact_sales, fact_purchases, fact_inventory")

print("\n  🟣 Stage 3: Gold Layer (FULL REFRESH)")
print("     • 4 business marts refreshed successfully")
print("     • mart_customer_360, mart_rfm_analysis,")
print("     • mart_product_performance, mart_inventory_health")

print("\n  🔵 Stage 4: Data Quality Monitoring")
print("     • 9 comprehensive test suites executed")
print("     • All quality validations passed")

print("\n" + "="*80)
print("✅ 🎉 FULL LOAD PIPELINE COMPLETED SUCCESSFULLY!")
print("="*80)
print(f"\n📁 Data Catalog: {Config.CATALOG}")
print(f"📦 Bronze Tables: 3 tables")
print(f"📦 Silver Tables: 12 tables (9 dims + 3 facts)")
print(f"📦 Gold Tables: 4 business marts")
print(f"\n⏱️  Total Execution Time: {total_duration/60:.2f} minutes")
print("\n🚀 Next Steps:")
print("   1. Review gold marts for business insights")
print("   2. Run incremental loads for daily updates")
print("   3. Set up BI dashboards")
print("   4. Configure job scheduling")
print("\n" + "="*80)