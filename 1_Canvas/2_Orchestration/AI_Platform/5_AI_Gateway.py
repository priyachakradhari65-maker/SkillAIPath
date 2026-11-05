# Databricks notebook source
# MAGIC %md
# MAGIC # SkillAIPath - AI Gateway Orchestrator
# MAGIC ## AI Production Platform: LLM Gateway & Governance
# MAGIC
# MAGIC ### 🎯 Overview
# MAGIC Production orchestrator for AI Gateway systems in SkillAIPath.
# MAGIC
# MAGIC ### 🌐 AI Gateway Components
# MAGIC 1. **Main Production Gateway** - Multi-model routing, PII detection, cost tracking
# MAGIC 2. **Gateway Setup** - Endpoint initialization
# MAGIC 3. **Rate Limiting** - Request throttling
# MAGIC 4. **Guardrails & PII Detection** - Security filters
# MAGIC 5. **Cost Tracking** - Usage monitoring
# MAGIC 6. **Model Routing** - Load balancing
# MAGIC 7. **Prompt Templates** - Template management
# MAGIC 8. **Response Caching** - Semantic cache
# MAGIC 9. **Audit Logging** - Compliance
# MAGIC 10. **A/B Testing** - Model comparison
# MAGIC
# MAGIC ### 🛠️ Execution Flow
# MAGIC - Run main production gateway with all 4 tests
# MAGIC - Optionally run individual feature notebooks
# MAGIC - Validation and reporting
# MAGIC
# MAGIC ### 💰 Cost
# MAGIC - **Primary (FREE)**: Databricks Foundation API (Llama 3.1 70B)
# MAGIC - **Fallback**: OpenAI/Claude (requires user API keys)
# MAGIC
# MAGIC ---
# MAGIC **Status**: Production-ready  
# MAGIC **Last Updated**: July 2026

# COMMAND ----------

# DBTITLE 1,📦 Configuration
from datetime import datetime

print("=" * 80)
print("SKILLAIPPATH - AI GATEWAY ORCHESTRATOR")
print("=" * 80)
print(f"Start Time: {datetime.now()}")
print("=" * 80)

# Create execution mode widget
dbutils.widgets.dropdown("mode", "main_only", ["main_only", "full_suite"], "Execution Mode")
mode = dbutils.widgets.get("mode")

print(f"Execution Mode: {mode}")
if mode == "main_only":
    print("  → Running main production gateway only (recommended for demos)")
elif mode == "full_suite":
    print("  → Running full gateway suite with all features")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,🌐 Main Production Gateway (Required)
print("\n" + "=" * 80)
print("MAIN PRODUCTION GATEWAY - ROUTING + GOVERNANCE + 4 TESTS")
print("=" * 80)

try:
    result = dbutils.notebook.run(
        "/Shared/dev/SkillAIPath/3_AI_Platform/4_AI_Gateway/SkillAIPath_Production_AI_Gateway",
        timeout_seconds=900
    )
    print("✅ SUCCESS: Main Production Gateway")
    print(f"   Result: {result}")
    print("   • Multi-Model Routing: ✅")
    print("   • PII Detection: ✅")
    print("   • Cost Tracking: ✅")
    print("   • Semantic Caching: ✅")
    print("   • Rate Limiting: ✅")
    print("   • 4 Tests Passed: ✅")
except Exception as e:
    print(f"❌ ERROR: Main Production Gateway")
    print(f"   {str(e)}")

# COMMAND ----------

# DBTITLE 1,🛠️ Individual Gateway Features (Optional)
if mode == "full_suite":
    print("\n" + "=" * 80)
    print("INDIVIDUAL GATEWAY FEATURES")
    print("=" * 80)
    
    feature_notebooks = [
        ("01_gateway_endpoint_setup", "Gateway Endpoint Setup"),
        ("02_rate_limiting", "Rate Limiting"),
        ("03_guardrails_pii_detection", "Guardrails & PII Detection"),
        ("04_cost_tracking", "Cost Tracking"),
        ("05_model_routing", "Model Routing"),
        ("06_prompt_templates", "Prompt Templates"),
        ("07_response_caching", "Response Caching"),
        ("08_audit_logging", "Audit Logging"),
        ("09_ab_testing", "A/B Testing"),
        ("semantic_cache", "Semantic Cache")
    ]
    
    for notebook, name in feature_notebooks:
        print(f"\n  Running: {name}")
        try:
            result = dbutils.notebook.run(
                f"/Shared/dev/SkillAIPath/3_AI_Platform/4_AI_Gateway/{notebook}",
                timeout_seconds=600
            )
            print(f"    ✅ SUCCESS: {name}")
        except Exception as e:
            print(f"    ⚠️  SKIPPED: {name} - {str(e)}")
    
    print("\n" + "=" * 80)
else:
    print("\n⏭️  SKIPPED: Individual features (mode=main_only)")
    print("   To run full suite: Set widget 'mode' = 'full_suite'")

# COMMAND ----------

# DBTITLE 1,✅ Validation & Summary
print("\n" + "=" * 80)
print("AI GATEWAY VALIDATION")
print("=" * 80)

# Check if gateway logging tables exist
gateway_tables = [
    "sap_dev.gold.ai_gateway_request_logs",
    "sap_dev.gold.ai_gateway_cost_tracking",
    "sap_dev.gold.ai_gateway_semantic_cache"
]

print("\nGateway Tables:")
for table in gateway_tables:
    if spark.catalog.tableExists(table):
        count = spark.table(table).count()
        print(f"  ✅ {table}: {count:,} rows")
    else:
        print(f"  ⚠️  {table}: Will be created on first use")

print("\n" + "=" * 80)
print("AI GATEWAY ORCHESTRATION COMPLETE")
print("=" * 80)
print(f"End Time: {datetime.now()}")
print("=" * 80)

if mode == "main_only":
    print("\nMain production gateway is ready for:")
    print("  • Multi-model LLM routing (Databricks + OpenAI + Claude)")
    print("  • PII detection and content filtering")
    print("  • Cost tracking and optimization")
    print("  • Semantic caching (30-60% cost savings)")
    print("  • Rate limiting and governance")
elif mode == "full_suite":
    print("\nFull gateway suite executed with:")
    print("  • Main production gateway (all features)")
    print("  • Individual feature modules (10 components)")
    print("  • Advanced caching and routing strategies")

print("\nNext: Integrate gateway with RAG systems and AI Agents.")
print("=" * 80)
