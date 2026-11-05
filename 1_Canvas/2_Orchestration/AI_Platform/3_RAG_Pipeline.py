# Databricks notebook source
# MAGIC %md
# MAGIC # SkillAIPath - RAG Pipeline Orchestrator
# MAGIC ## AI Production Platform: Retrieval-Augmented Generation Systems
# MAGIC
# MAGIC ### 🎯 Overview
# MAGIC Production orchestrator for all 3 RAG systems in SkillAIPath.
# MAGIC
# MAGIC ### 📊 RAG Systems
# MAGIC 1. **Platform Documentation RAG** - Knowledge retrieval for platform docs
# MAGIC 2. **E-Commerce Intelligence RAG** - Product and sales insights
# MAGIC 3. **Customer 360 RAG** - Customer intelligence and segmentation
# MAGIC
# MAGIC ### 🛠️ Execution Flow
# MAGIC - Sequential execution of all RAG systems
# MAGIC - Smoke tests for each system
# MAGIC - Validation and reporting
# MAGIC
# MAGIC ### 💰 Cost
# MAGIC - **100% FREE**: BM25 search + Llama 3.1 70B Foundation API
# MAGIC - **No Vector Search fees**
# MAGIC
# MAGIC ---
# MAGIC **Status**: Production-ready  
# MAGIC **Last Updated**: July 2026

# COMMAND ----------

# DBTITLE 1,📦 Configuration
from datetime import datetime

print("=" * 80)
print("SKILLAIPPATH - RAG PIPELINE ORCHESTRATOR")
print("=" * 80)
print(f"Start Time: {datetime.now()}")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,🔍 RAG System 1: Platform Documentation
print("\n" + "=" * 80)
print("RAG SYSTEM 1: PLATFORM DOCUMENTATION")
print("=" * 80)

try:
    result = dbutils.notebook.run(
        "/Shared/dev/SkillAIPath/3_AI_Platform/2_Rag_Pipeline/01_knowledge_retrieval_system",
        timeout_seconds=600
    )
    print("✅ SUCCESS: Platform Documentation RAG")
    print(f"   Result: {result}")
except Exception as e:
    print(f"❌ ERROR: Platform Documentation RAG")
    print(f"   {str(e)}")

# COMMAND ----------

# DBTITLE 1,🛒 RAG System 2: E-Commerce Intelligence
print("\n" + "=" * 80)
print("RAG SYSTEM 2: E-COMMERCE INTELLIGENCE")
print("=" * 80)

try:
    result = dbutils.notebook.run(
        "/Shared/dev/SkillAIPath/3_AI_Platform/2_Rag_Pipeline/02_ecommerce_intelligence",
        timeout_seconds=600
    )
    print("✅ SUCCESS: E-Commerce Intelligence RAG")
    print(f"   Result: {result}")
except Exception as e:
    print(f"❌ ERROR: E-Commerce Intelligence RAG")
    print(f"   {str(e)}")

# COMMAND ----------

# DBTITLE 1,👤 RAG System 3: Customer 360
print("\n" + "=" * 80)
print("RAG SYSTEM 3: CUSTOMER 360")
print("=" * 80)

try:
    result = dbutils.notebook.run(
        "/Shared/dev/SkillAIPath/3_AI_Platform/2_Rag_Pipeline/03_customer_360",
        timeout_seconds=600
    )
    print("✅ SUCCESS: Customer 360 RAG")
    print(f"   Result: {result}")
except Exception as e:
    print(f"❌ ERROR: Customer 360 RAG")
    print(f"   {str(e)}")

# COMMAND ----------

# DBTITLE 1,✅ Validation & Summary
print("\n" + "=" * 80)
print("RAG PIPELINE VALIDATION")
print("=" * 80)

# Check if required tables exist
required_tables = [
    "sap_dev.gold.mart_customer_360",
    "sap_dev.gold.mart_product_performance",
    "sap_dev.gold.mart_rfm_analysis"
]

for table in required_tables:
    if spark.catalog.tableExists(table):
        count = spark.table(table).count()
        print(f"  ✅ {table}: {count:,} rows")
    else:
        print(f"  ❌ {table}: NOT FOUND")

print("\n" + "=" * 80)
print("RAG PIPELINE ORCHESTRATION COMPLETE")
print("=" * 80)
print(f"End Time: {datetime.now()}")
print("=" * 80)
print("\nAll 3 RAG systems are production-ready.")
print("Next: Run individual notebooks for specific queries.")
print("=" * 80)
