# Databricks notebook source
# MAGIC %md
# MAGIC # BHARAT MART - MASTER ORCHESTRATOR
# MAGIC ## Single Entry Point for Architecture-Based Learning
# MAGIC
# MAGIC **Self-Paced Architecture Progression (12 Stages)**
# MAGIC
# MAGIC This notebook routes execution to the appropriate architecture orchestrator based on your learning stage.
# MAGIC
# MAGIC ## Usage
# MAGIC
# MAGIC 1. **Set stage number** via widget (1-12)
# MAGIC 2. **Run All** to execute that stage's pipeline
# MAGIC 3. **Review validation output** to confirm success
# MAGIC
# MAGIC ## Architecture Structure
# MAGIC
# MAGIC **LEARNING PATH 1: Core Data Pipeline (Stages 1-8)**
# MAGIC - Stage 1: Foundation - Environment & Configuration
# MAGIC - Stage 2-3: Medallion Architecture - Dimension Tables (7 tables + SCD2)
# MAGIC - Stage 4-5: Medallion Architecture - Fact Tables (3 tables)
# MAGIC - Stage 6: Streaming & CDC - Real-time Processing
# MAGIC - Stage 7-8: Gold Layer - Business Marts (4 marts)
# MAGIC
# MAGIC **LEARNING PATH 2: Modern Lakehouse (Stages 9-12)**
# MAGIC - Stage 9: dbt Integration - SQL Transformations
# MAGIC - Stage 10: Unity Catalog, Interoperability & Governance
# MAGIC - Stage 11: Lakehouse Federation
# MAGIC - Stage 12: External Sources - API Ingestion

# COMMAND ----------

# DBTITLE 1,⚙️ Date Range Configuration
# ============================================================================
# DATE RANGE CONFIGURATION
# ============================================================================
# Smart default: 3 days for manual/notebook runs, 30 days for job runs

import json

def is_running_in_job():
    """Detect if notebook is running as part of a Databricks Job"""
    try:
        context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
        job_id = context.get('tags', {}).get('jobId')
        run_id = context.get('tags', {}).get('runId')
        return job_id is not None or run_id is not None
    except:
        return False

is_job = is_running_in_job()
default_days = 30 if is_job else 3

dbutils.widgets.text("date_range_days", str(default_days), "Date Range (days)")
date_range_days = int(dbutils.widgets.get("date_range_days"))

run_mode = "Job/Workflow" if is_job else "Manual/Notebook"
print(f"Run Mode: {run_mode} | Date Range: {date_range_days} days")

# Store in Spark config for dependent notebooks
spark.conf.set("spark.databricks.sap15.date_range_days", str(date_range_days))

# COMMAND ----------

# Create stage selection widget
dbutils.widgets.dropdown("stage", "1", [str(i) for i in range(1, 13)], "Select Architecture Stage (1-12)")

stage_num = int(dbutils.widgets.get("stage"))

print("=" * 80)
print("BHARAT MART - MASTER ORCHESTRATOR")
print("=" * 80)
print(f"Selected Stage: {stage_num}/12")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage Mapping

# COMMAND ----------

# Define stage-to-orchestrator mapping
STAGE_MAPPING = {
    1: {
        "name": "Foundation Architecture",
        "path": "Learning Path 1",
        "orchestrator": "orch_00_foundation",
        "scope": "Layer 0: Configuration & Data Generation"
    },
    2: {
        "name": "Bronze Layer - Dimensions Part 1",
        "path": "Learning Path 1",
        "orchestrator": "orch_01_bronze_layer",
        "scope": "Layers 1-2: First 3 Dimensions"
    },
    3: {
        "name": "Dimensions Layer - Complete with SCD2",
        "path": "Learning Path 1",
        "orchestrator": "orch_02_dimensions",
        "scope": "Layers 3-4: Remaining Dimensions + SCD2"
    },
    4: {
        "name": "Facts Layer - Part 1",
        "path": "Learning Path 1",
        "orchestrator": "orch_03_facts_part1",
        "scope": "Layers 5-6: Sales & Purchases Facts"
    },
    5: {
        "name": "Facts Layer - Part 2",
        "path": "Learning Path 1",
        "orchestrator": "orch_04_facts_part2",
        "scope": "Layers 7-8: Inventory Fact"
    },
    6: {
        "name": "Streaming & CDC Architecture",
        "path": "Learning Path 1",
        "orchestrator": "orch_05_streaming_cdc",
        "scope": "Layer 9: Real-time Medallion Architecture"
    },
    7: {
        "name": "Gold Layer - Part 1",
        "path": "Learning Path 1",
        "orchestrator": "orch_06_gold_part1",
        "scope": "Layer 10: Customer 360 & RFM"
    },
    8: {
        "name": "Gold Layer - Part 2",
        "path": "Learning Path 1",
        "orchestrator": "orch_07_gold_part2",
        "scope": "Layers 11-12: Product & Inventory Marts"
    },
    9: {
        "name": "dbt Integration",
        "path": "Learning Path 2",
        "orchestrator": "orch_08_dbt_integration",
        "scope": "Layer 13: SQL-based Transformations"
    },
    10: {
        "name": "Unity Catalog, Interoperability & Governance",
        "path": "Learning Path 2",
        "orchestrator": "orch_09_governance",
        "scope": "Layer 14: Data Governance & Interoperability"
    },
    11: {
        "name": "Lakehouse Federation",
        "path": "Learning Path 2",
        "orchestrator": "orch_10_federation",
        "scope": "Layer 15: External Database Queries"
    },
    12: {
        "name": "External Sources Integration",
        "path": "Learning Path 2",
        "orchestrator": "orch_11_external_sources",
        "scope": "Layer 16: REST API & Kafka"
    }
}

# Display stage info
if stage_num in STAGE_MAPPING:
    stage_info = STAGE_MAPPING[stage_num]
    print(f"\nStage {stage_num}: {stage_info['name']}")
    print(f"Learning Path: {stage_info['path']}")
    print(f"Scope: {stage_info['scope']}")
    print(f"Orchestrator: {stage_info['orchestrator']}")
    print("=" * 80)
else:
    raise ValueError(f"Invalid stage number: {stage_num}. Must be 1-12.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Stage Orchestrator

# COMMAND ----------

from datetime import datetime

start_time = datetime.now()

print(f"\nStarting Stage {stage_num} orchestration...")
print(f"Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)

try:
    # Execute the selected stage's orchestrator
    orchestrator_path = f"/Workspace/Shared/SkillAIPath/0_Utils/02_ORCHESTRATION/{stage_info['orchestrator']}"
    
    result = dbutils.notebook.run(
        orchestrator_path,
        timeout_seconds=3600,  # 1 hour timeout
        arguments={}
    )
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print("=" * 80)
    print(f"SUCCESS: Stage {stage_num} orchestration completed")
    print(f"Duration: {duration:.1f} seconds")
    print(f"Result: {result if result else 'OK'}")
    print("=" * 80)
    
except Exception as e:
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print("=" * 80)
    print(f"ERROR: Stage {stage_num} orchestration failed")
    print(f"Duration: {duration:.1f} seconds")
    print(f"Error: {str(e)}")
    print("=" * 80)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps

# COMMAND ----------

# Show what's next
if stage_num < 12:
    next_stage = stage_num + 1
    next_info = STAGE_MAPPING[next_stage]
    print("=" * 80)
    print(f"NEXT STAGE: Stage {next_stage} - {next_info['name']}")
    print(f"Scope: {next_info['scope']}")
    print("=" * 80)
    print(f"\nTo continue, update the stage widget to {next_stage} and re-run.")
elif stage_num == 8:
    print("=" * 80)
    print("LEARNING PATH 1 COMPLETE - CORE DATA PIPELINE")
    print("=" * 80)
    print("\nYou now have a complete end-to-end data pipeline:")
    print("  - Bronze → Silver → Gold")
    print("  - Dimensions → Facts → Marts")
    print("  - This alone is portfolio-worthy")
    print("\nReady for LEARNING PATH 2 (Modern Lakehouse): Set stage to 9")
elif stage_num == 12:
    print("=" * 80)
    print("LEARNING PATH 2 COMPLETE - MODERN LAKEHOUSE ARCHITECTURE")
    print("=" * 80)
    print("\nYou now have production-grade data engineering capability:")
    print("  - Core pipeline operational")
    print("  - Modern lakehouse patterns implemented")
    print("  - Advanced data engineering skills demonstrated")
    print("\nNext: MLOps, GenAI, and Platform Applications (Tiers 4-7)")
else:
    print("=" * 80)
    print(f"Stage {stage_num} complete. Continue to next stage.")
    print("=" * 80)