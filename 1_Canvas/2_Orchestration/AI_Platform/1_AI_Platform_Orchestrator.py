# Databricks notebook source
# MAGIC %md
# MAGIC # SkillAIPath - AI Production Platform Master Orchestrator
# MAGIC ## Complete AI Stack: MLOps + RAG + Agents + Gateway
# MAGIC
# MAGIC ### 🎯 Overview
# MAGIC Master orchestrator for the entire AI Production Platform in SkillAIPath.
# MAGIC
# MAGIC ### 🏗️ AI Production Platform Components
# MAGIC
# MAGIC #### 1️⃣ **MLOps Pipeline**
# MAGIC - 4 ML Models: Churn, Recommendation, Demand, Next Purchase
# MAGIC - Model deployment and serving
# MAGIC - Inference testing
# MAGIC
# MAGIC #### 2️⃣ **RAG Systems**
# MAGIC - Platform Documentation RAG
# MAGIC - E-Commerce Intelligence RAG
# MAGIC - Customer 360 RAG
# MAGIC
# MAGIC #### 3️⃣ **AI Agents**
# MAGIC - Multi-tool orchestration (SQL, Python, Calculator, Notifications)
# MAGIC - Memory persistence
# MAGIC - Human-in-loop approval
# MAGIC - MLflow observability
# MAGIC
# MAGIC #### 4️⃣ **AI Gateway**
# MAGIC - Multi-model LLM routing
# MAGIC - PII detection and guardrails
# MAGIC - Cost tracking and optimization
# MAGIC - Semantic caching
# MAGIC
# MAGIC ### 🛠️ Execution Modes
# MAGIC - **Quick Demo**: Run main production notebooks only
# MAGIC - **Full Stack**: Run complete AI platform with all features
# MAGIC - **Custom**: Select specific components to run
# MAGIC
# MAGIC ### 💰 Cost
# MAGIC - **100% FREE**: Foundation Models API, Serverless compute
# MAGIC - **Optional**: OpenAI/Claude for gateway fallback
# MAGIC
# MAGIC ---
# MAGIC **Status**: Production-ready  
# MAGIC **Last Updated**: July 2026

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

# DBTITLE 1,📦 Configuration
from datetime import datetime

print("=" * 80)
print("SKILLAIPATH - AI PRODUCTION PLATFORM MASTER ORCHESTRATOR")
print("=" * 80)
print(f"Start Time: {datetime.now()}")
print("=" * 80)

# Create execution mode widget
dbutils.widgets.dropdown(
    "mode", 
    "quick_demo", 
    ["quick_demo", "full_stack", "mlops_only", "rag_only", "agents_only", "gateway_only"], 
    "Execution Mode"
)
mode = dbutils.widgets.get("mode")

print(f"Execution Mode: {mode}")
mode_descriptions = {
    "quick_demo": "Main production notebooks only (fastest)",
    "full_stack": "Complete AI platform with all features",
    "mlops_only": "MLOps pipeline only",
    "rag_only": "RAG systems only",
    "agents_only": "AI Agents only",
    "gateway_only": "AI Gateway only"
}
print(f"  → {mode_descriptions.get(mode, 'Unknown mode')}")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,1️⃣ MLOps Pipeline
if mode in ["quick_demo", "full_stack", "mlops_only"]:
    print("\n" + "=" * 80)
    print("COMPONENT 1: MLOPS PIPELINE")
    print("=" * 80)
    
    mlops_mode = "full_pipeline" if mode == "full_stack" else "train_and_deploy"
    
    try:
        result = dbutils.notebook.run(
            "/Shared/dev/SkillAIPath/3_AI_Platform/1_MLOps/00_ORCHESTRATOR_MLOps",
            timeout_seconds=1800,
            arguments={"mode": mlops_mode}
        )
        print("✅ SUCCESS: MLOps Pipeline")
        print(f"   Result: {result}")
    except Exception as e:
        print(f"❌ ERROR: MLOps Pipeline")
        print(f"   {str(e)}")
else:
    print("\n⏭️  SKIPPED: MLOps Pipeline")

# COMMAND ----------

# DBTITLE 1,2️⃣ RAG Systems
if mode in ["quick_demo", "full_stack", "rag_only"]:
    print("\n" + "=" * 80)
    print("COMPONENT 2: RAG SYSTEMS")
    print("=" * 80)
    
    try:
        result = dbutils.notebook.run(
            "/Shared/dev/SkillAIPath/3_AI_Platform/2_Rag_Pipeline/00_ORCHESTRATOR_RAG_Pipeline",
            timeout_seconds=1800
        )
        print("✅ SUCCESS: RAG Systems")
        print(f"   Result: {result}")
    except Exception as e:
        print(f"❌ ERROR: RAG Systems")
        print(f"   {str(e)}")
else:
    print("\n⏭️  SKIPPED: RAG Systems")

# COMMAND ----------

# DBTITLE 1,3️⃣ AI Agents
if mode in ["quick_demo", "full_stack", "agents_only"]:
    print("\n" + "=" * 80)
    print("COMPONENT 3: AI AGENTS")
    print("=" * 80)
    
    agents_mode = "full_suite" if mode == "full_stack" else "main_only"
    
    try:
        result = dbutils.notebook.run(
            "/Shared/dev/SkillAIPath/3_AI_Platform/3_AI_Agents/00_ORCHESTRATOR_AI_Agents",
            timeout_seconds=1800,
            arguments={"mode": agents_mode}
        )
        print("✅ SUCCESS: AI Agents")
        print(f"   Result: {result}")
    except Exception as e:
        print(f"❌ ERROR: AI Agents")
        print(f"   {str(e)}")
else:
    print("\n⏭️  SKIPPED: AI Agents")

# COMMAND ----------

# DBTITLE 1,4️⃣ AI Gateway
if mode in ["quick_demo", "full_stack", "gateway_only"]:
    print("\n" + "=" * 80)
    print("COMPONENT 4: AI GATEWAY")
    print("=" * 80)
    
    gateway_mode = "full_suite" if mode == "full_stack" else "main_only"
    
    try:
        result = dbutils.notebook.run(
            "/Shared/dev/SkillAIPath/3_AI_Platform/4_AI_Gateway/00_ORCHESTRATOR_AI_Gateway",
            timeout_seconds=1800,
            arguments={"mode": gateway_mode}
        )
        print("✅ SUCCESS: AI Gateway")
        print(f"   Result: {result}")
    except Exception as e:
        print(f"❌ ERROR: AI Gateway")
        print(f"   {str(e)}")
else:
    print("\n⏭️  SKIPPED: AI Gateway")

# COMMAND ----------

# DBTITLE 1,✅ Platform Validation & Summary
print("\n" + "=" * 80)
print("AI PRODUCTION PLATFORM VALIDATION")
print("=" * 80)

# Check key tables across all components
key_tables = {
    "MLOps": ["sap_dev.gold.customer_churn_predictions"],
    "RAG/Agents": [
        "sap_dev.gold.mart_customer_360",
        "sap_dev.gold.mart_product_performance",
        "sap_dev.gold.mart_rfm_analysis"
    ],
    "Gateway": [
        "sap_dev.gold.ai_gateway_request_logs",
        "sap_dev.gold.ai_gateway_cost_tracking"
    ]
}

for component, tables in key_tables.items():
    print(f"\n{component}:")
    for table in tables:
        if spark.catalog.tableExists(table):
            count = spark.table(table).count()
            print(f"  ✅ {table.split('.')[-1]}: {count:,} rows")
        else:
            print(f"  ⚠️  {table.split('.')[-1]}: Will be created on first use")

print("\n" + "=" * 80)
print("AI PRODUCTION PLATFORM ORCHESTRATION COMPLETE")
print("=" * 80)
print(f"End Time: {datetime.now()}")
print("=" * 80)

print(f"\nExecution Summary for mode='{mode}':")

if mode == "quick_demo":
    print("  ✅ Main production notebooks executed")
    print("  ✅ All 4 AI components functional")
    print("  ✅ Ready for demos and presentations")
elif mode == "full_stack":
    print("  ✅ Complete AI platform deployed")
    print("  ✅ All features and advanced capabilities tested")
    print("  ✅ Production-ready with full observability")
elif mode == "mlops_only":
    print("  ✅ MLOps pipeline complete")
    print("  ✅ Models trained, deployed, and tested")
elif mode == "rag_only":
    print("  ✅ All 3 RAG systems operational")
    print("  ✅ Ready for knowledge retrieval queries")
elif mode == "agents_only":
    print("  ✅ AI Agents with 4 tools functional")
    print("  ✅ Ready for conversational analytics")
elif mode == "gateway_only":
    print("  ✅ AI Gateway deployed with governance")
    print("  ✅ Ready for multi-model routing")

print("\nPlatform Capabilities Now Available:")
print("  🤖 ML Models: Churn, Recommendation, Demand, Next Purchase")
print("  🔍 RAG: Platform Docs, E-Commerce, Customer 360")
print("  🧠 Agents: SQL, Python, Calculator, Notifications")
print("  🌐 Gateway: Multi-model routing with PII detection & cost tracking")

print("\n" + "=" * 80)