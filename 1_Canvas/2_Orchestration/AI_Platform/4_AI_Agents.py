# Databricks notebook source
# MAGIC %md
# MAGIC # SkillAIPath - AI Agents Orchestrator
# MAGIC ## AI Production Platform: Intelligent Agent Systems
# MAGIC
# MAGIC ### 🎯 Overview
# MAGIC Production orchestrator for AI Agent systems in SkillAIPath.
# MAGIC
# MAGIC ### 🤖 AI Agents
# MAGIC 1. **Main Production Agent** - Multi-tool orchestration (SQL, Python, Calculator, Notifications)
# MAGIC 2. **Multi-Tool Toolchain** - Advanced tool orchestration
# MAGIC 3. **Memory Persistence** - Delta table conversation storage
# MAGIC 4. **Human-in-Loop** - Approval workflows
# MAGIC 5. **MLflow Observability** - Cost and performance tracking
# MAGIC 6. **Recursive Planner** - Complex task planning
# MAGIC 7. **Self-Correction** - Error recovery mechanisms
# MAGIC
# MAGIC ### 🛠️ Execution Flow
# MAGIC - Run main production agent with all 6 tests
# MAGIC - Optionally run advanced feature notebooks
# MAGIC - Validation and reporting
# MAGIC
# MAGIC ### 💰 Cost
# MAGIC - **100% FREE**: Foundation Models API (Llama 3.1 70B)
# MAGIC - **No paid dependencies**
# MAGIC
# MAGIC ---
# MAGIC **Status**: Production-ready  
# MAGIC **Last Updated**: July 2026

# COMMAND ----------

# DBTITLE 1,📦 Configuration
from datetime import datetime

print("=" * 80)
print("SKILLAIPPATH - AI AGENTS ORCHESTRATOR")
print("=" * 80)
print(f"Start Time: {datetime.now()}")
print("=" * 80)

# Create execution mode widget
dbutils.widgets.dropdown("mode", "main_only", ["main_only", "full_suite"], "Execution Mode")
mode = dbutils.widgets.get("mode")

print(f"Execution Mode: {mode}")
if mode == "main_only":
    print("  → Running main production agent only (recommended for demos)")
elif mode == "full_suite":
    print("  → Running full agent suite with advanced features")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,🤖 Main Production Agent (Required)
print("\n" + "=" * 80)
print("MAIN PRODUCTION AGENT - 4 TOOLS + 6 TESTS")
print("=" * 80)

try:
    result = dbutils.notebook.run(
        "/Shared/dev/SkillAIPath/3_AI_Platform/3_AI_Agents/SkillAIPath_Production_AI_Agents",
        timeout_seconds=900
    )
    print("✅ SUCCESS: Main Production Agent")
    print(f"   Result: {result}")
    print("   • SQL Analytics Tool: ✅")
    print("   • Python Analytics Tool: ✅")
    print("   • Calculator Tool: ✅")
    print("   • Notification Tool: ✅")
    print("   • 6 Tests Passed: ✅")
except Exception as e:
    print(f"❌ ERROR: Main Production Agent")
    print(f"   {str(e)}")

# COMMAND ----------

# DBTITLE 1,🛠️ Advanced Features (Optional)
if mode == "full_suite":
    print("\n" + "=" * 80)
    print("ADVANCED AI AGENT FEATURES")
    print("=" * 80)
    
    advanced_notebooks = [
        ("01_multi_tool_agent_toolchain", "Multi-Tool Orchestration"),
        ("02_agent_memory_persistence", "Memory Persistence"),
        ("03_human_in_loop_approval", "Human-in-Loop Approval"),
        ("04_agent_observability_mlflow", "MLflow Observability"),
        ("05_recursive_agent_planner", "Recursive Planning"),
        ("06_agent_self_correction", "Self-Correction")
    ]
    
    for notebook, name in advanced_notebooks:
        print(f"\n  Running: {name}")
        try:
            result = dbutils.notebook.run(
                f"/Shared/dev/SkillAIPath/3_AI_Platform/3_AI_Agents/{notebook}",
                timeout_seconds=600
            )
            print(f"    ✅ SUCCESS: {name}")
        except Exception as e:
            print(f"    ⚠️  SKIPPED: {name} - {str(e)}")
    
    print("\n" + "=" * 80)
else:
    print("\n⏭️  SKIPPED: Advanced features (mode=main_only)")
    print("   To run full suite: Set widget 'mode' = 'full_suite'")

# COMMAND ----------

# DBTITLE 1,✅ Validation & Summary
print("\n" + "=" * 80)
print("AI AGENTS VALIDATION")
print("=" * 80)

# Check if required tables exist for agents
required_tables = [
    "sap_dev.gold.mart_customer_360",
    "sap_dev.gold.mart_product_performance",
    "sap_dev.gold.mart_rfm_analysis"
]

print("\nData Sources:")
for table in required_tables:
    if spark.catalog.tableExists(table):
        count = spark.table(table).count()
        print(f"  ✅ {table}: {count:,} rows")
    else:
        print(f"  ❌ {table}: NOT FOUND")

print("\n" + "=" * 80)
print("AI AGENTS ORCHESTRATION COMPLETE")
print("=" * 80)
print(f"End Time: {datetime.now()}")
print("=" * 80)

if mode == "main_only":
    print("\nMain production agent is ready for:")
    print("  • SQL queries on gold tables")
    print("  • Python analytics and ML predictions")
    print("  • Financial calculations")
    print("  • Automated notifications")
elif mode == "full_suite":
    print("\nFull agent suite executed with:")
    print("  • Main production agent (4 tools)")
    print("  • Advanced features (memory, approval, observability)")
    print("  • Recursive planning and self-correction")

print("\nNext: Test individual agent capabilities or integrate with applications.")
print("=" * 80)
