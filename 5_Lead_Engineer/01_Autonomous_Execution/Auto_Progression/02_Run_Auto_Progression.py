# Databricks notebook source
# DBTITLE 1,Monthly Upgrade System - Main Orchestrator
# MAGIC %md
# MAGIC # Monthly Upgrade System - Main Orchestrator
# MAGIC
# MAGIC **Run this notebook monthly to discover and analyze Databricks feature upgrades**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📋 What This Does:
# MAGIC
# MAGIC 1. **📸 Architecture Snapshot** - Captures your current workspace state
# MAGIC 2. **📡 Release Monitor** - Fetches latest Databricks releases
# MAGIC 3. **🧠 AI Analysis** - Analyzes which features are relevant to YOUR workspace
# MAGIC 4. **📊 Recommendations** - Generates upgrade candidates with priority
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## How to Use:
# MAGIC
# MAGIC ### **First Time Setup:**
# MAGIC 1. Set `DRY_RUN = True` (default) to see what would happen
# MAGIC 2. Click **Run All**
# MAGIC 3. Review output tables
# MAGIC 4. Set `DRY_RUN = False` to execute for real
# MAGIC
# MAGIC ### **Monthly Runs:**
# MAGIC 1. Set `CLEANUP_BEFORE_RUN = False` (keep history)
# MAGIC 2. Run the notebook
# MAGIC 3. Query results with Genie or SQL
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚠️ Limitations:
# MAGIC
# MAGIC * **Run once per month** - Results accumulate in tables
# MAGIC * **Cleanup option** - Set `CLEANUP_BEFORE_RUN = True` to start fresh
# MAGIC * **Mock releases** - Currently uses sample Databricks releases (update to fetch real ones)
# MAGIC * **Domain agnostic** - Works for any workspace/domain
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🗄️ Output Tables:
# MAGIC
# MAGIC * `workspace.default.architecture_snapshot` - Your workspace state
# MAGIC * `workspace.default.databricks_releases` - Available Databricks features
# MAGIC * `workspace.default.upgrade_candidates` - AI-recommended upgrades
# MAGIC * `workspace.default.approved_upgrades` - What you decide to implement
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧹 Cleanup (if needed):
# MAGIC
# MAGIC ```sql
# MAGIC DROP TABLE IF EXISTS workspace.default.architecture_snapshot;
# MAGIC DROP TABLE IF EXISTS workspace.default.databricks_releases;
# MAGIC DROP TABLE IF EXISTS workspace.default.upgrade_candidates;
# MAGIC DROP TABLE IF EXISTS workspace.default.approved_upgrades;
# MAGIC DROP TABLE IF EXISTS workspace.default.upgrade_deployment_log;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Last Updated:** August 2026 
# MAGIC **Location:** `/Shared/dev/SkillAIPath/5_Lifecycle_Management/Monthly_Upgrade_System/`

# COMMAND ----------

# DBTITLE 1,⚙️ Configuration
# ============================================
# MONTHLY UPGRADE SYSTEM - CONFIGURATION
# ============================================

import json
import uuid
from datetime import datetime, date
from pyspark.sql.types import *

print("⚙️ MONTHLY UPGRADE SYSTEM - CONFIGURATION")
print("="*80)

# ============================================
# USER CONFIGURATION
# ============================================

# DRY RUN MODE - Set to True to preview without executing
DRY_RUN = True # 🟡 CHANGE TO False FOR REAL EXECUTION

# CLEANUP MODE - Set to True to drop all tables before running
CLEANUP_BEFORE_RUN = False # ⚠️ DESTRUCTIVE: Deletes all historical data

# DOMAIN CONFIGURATION (change for different domains)
DOMAIN_NAME = "SkillAIPath" # Your project/domain name
TARGET_CATALOG = "workspace" # Catalog to store results
TARGET_SCHEMA = "default" # Schema to store results

# NOTIFICATION
NOTIFICATION_EMAIL = "tech@skillaipath.com"

# ============================================
# SYSTEM CONFIGURATION (usually don't change)
# ============================================

PIPELINE_ID = f"upgrade_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
RUN_TIMESTAMP = datetime.now()

config = {
 "pipeline_id": PIPELINE_ID,
 "run_timestamp": RUN_TIMESTAMP,
 "dry_run": DRY_RUN,
 "cleanup": CLEANUP_BEFORE_RUN,
 "domain_name": DOMAIN_NAME,
 "target_catalog": TARGET_CATALOG,
 "target_schema": TARGET_SCHEMA,
 "notification_email": NOTIFICATION_EMAIL
}

print()
print("Configuration:")
for key, value in config.items():
 print(f" {key}: {value}")
print()

if DRY_RUN:
 print("🟡 DRY RUN MODE: Actions will be simulated (no writes)")
else:
 print("🔴 LIVE MODE: Actions will be executed (tables will be written)")

if CLEANUP_BEFORE_RUN:
 print("⚠️ CLEANUP ENABLED: All tables will be dropped before run")

print("="*80)
print()

# COMMAND ----------

# DBTITLE 1,🧹 Step 0: Cleanup (if enabled)
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- STEP 0: CLEANUP (IF ENABLED)
# MAGIC -- ============================================
# MAGIC -- Only runs if CLEANUP_BEFORE_RUN = True
# MAGIC
# MAGIC -- This cell is intentionally empty - cleanup logic in Python cell below

# COMMAND ----------

# DBTITLE 1,🧹 Cleanup Logic
# Cleanup logic (only if enabled)
if CLEANUP_BEFORE_RUN:
 print("🧹 STEP 0: CLEANUP")
 print("="*80)
 print("⚠️ Dropping all tables...")
 print()

 tables_to_drop = [
 "architecture_snapshot",
 "databricks_releases",
 "upgrade_candidates",
 "approved_upgrades",
 "upgrade_deployment_log"
 ]

 for table in tables_to_drop:
 table_name = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{table}"
 try:
 if not DRY_RUN:
 spark.sql(f"DROP TABLE IF EXISTS {table_name}")
 print(f" ✅ Dropped: {table_name}")
 else:
 print(f" 🟡 Would drop: {table_name}")
 except Exception as e:
 print(f" ⚠️ Could not drop {table_name}: {e}")

 print()
 print("✅ Cleanup complete")
 print()
else:
 print("⏭️ Cleanup skipped (CLEANUP_BEFORE_RUN = False)")
 print()

# COMMAND ----------

# DBTITLE 1,🗄️ Step 1: Initialize Tables
print("🗄️ STEP 1: INITIALIZE TABLES")
print("="*80)

# Define table schemas
tables_config = {
 "architecture_snapshot": """
 CREATE TABLE IF NOT EXISTS {catalog}.{schema}.architecture_snapshot (
 snapshot_id STRING,
 snapshot_date TIMESTAMP,
 domain_name STRING,
 total_tables INT,
 total_notebooks INT,
 total_jobs INT,
 architecture_json STRING,
 created_by STRING
 ) USING DELTA
 """,
 "databricks_releases": """
 CREATE TABLE IF NOT EXISTS {catalog}.{schema}.databricks_releases (
 release_id STRING,
 release_date DATE,
 feature_name STRING,
 category STRING,
 description STRING,
 impact_level STRING,
 documentation_url STRING,
 created_at TIMESTAMP
 ) USING DELTA
 """,
 "upgrade_candidates": """
 CREATE TABLE IF NOT EXISTS {catalog}.{schema}.upgrade_candidates (
 candidate_id STRING,
 release_id STRING,
 feature_name STRING,
 category STRING,
 priority STRING,
 relevance_score DOUBLE,
 business_reasoning STRING,
 estimated_effort_hours INT,
 affected_workloads STRING,
 requires_approval BOOLEAN,
 created_at TIMESTAMP
 ) USING DELTA
 """,
 "approved_upgrades": """
 CREATE TABLE IF NOT EXISTS {catalog}.{schema}.approved_upgrades (
 candidate_id STRING,
 feature_name STRING,
 approved_by STRING,
 approved_at TIMESTAMP,
 sprint_month STRING,
 implementation_status STRING,
 notes STRING
 ) USING DELTA
 """,
 "upgrade_deployment_log": """
 CREATE TABLE IF NOT EXISTS {catalog}.{schema}.upgrade_deployment_log (
 deployment_id STRING,
 candidate_id STRING,
 feature_name STRING,
 deployed_at TIMESTAMP,
 deployment_status STRING,
 deployed_by STRING,
 error_message STRING
 ) USING DELTA
 """
}

# Create tables
for table_name, create_sql in tables_config.items():
 table_full_name = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{table_name}"
 try:
 if not DRY_RUN:
 spark.sql(create_sql.format(catalog=TARGET_CATALOG, schema=TARGET_SCHEMA))
 print(f" ✅ Initialized: {table_full_name}")
 else:
 print(f" 🟡 Would initialize: {table_full_name}")
 except Exception as e:
 print(f" ⚠️ Error initializing {table_full_name}: {e}")

print()
print("✅ All tables initialized")
print()

# COMMAND ----------

# DBTITLE 1,📸 Step 2: Architecture Snapshot
print("📸 STEP 2: ARCHITECTURE SNAPSHOT")
print("="*80)

snapshot_id = str(uuid.uuid4())
snapshot_date = datetime.now()

# Count workspace resources
try:
 total_tables = spark.sql(f"""
 SELECT COUNT(*) 
 FROM {TARGET_CATALOG}.information_schema.tables 
 WHERE table_schema = '{TARGET_SCHEMA}'
 """).collect()[0][0]
except:
 total_tables = 0

total_notebooks = 0 # Placeholder - can be enhanced
total_jobs = 0 # Placeholder - can be enhanced

# Architecture metadata
architecture = {
 "snapshot_id": snapshot_id,
 "snapshot_date": snapshot_date.isoformat(),
 "domain_name": DOMAIN_NAME,
 "resources": {
 "tables": total_tables,
 "notebooks": total_notebooks,
 "jobs": total_jobs
 },
 "catalogs": [TARGET_CATALOG]
}

architecture_json = json.dumps(architecture, indent=2)

print(f"Domain: {DOMAIN_NAME}")
print(f"Total tables: {total_tables}")
print(f"Total notebooks: {total_notebooks}")
print(f"Total jobs: {total_jobs}")
print()

# Write snapshot
if not DRY_RUN:
 snapshot_data = [(
 snapshot_id,
 snapshot_date,
 DOMAIN_NAME,
 total_tables,
 total_notebooks,
 total_jobs,
 architecture_json,
 NOTIFICATION_EMAIL
 )]

 snapshot_df = spark.createDataFrame(
 snapshot_data,
 ["snapshot_id", "snapshot_date", "domain_name", "total_tables", 
 "total_notebooks", "total_jobs", "architecture_json", "created_by"]
 )

 snapshot_df.write.mode("append").saveAsTable(
 f"{TARGET_CATALOG}.{TARGET_SCHEMA}.architecture_snapshot"
 )
 print(f"✅ Snapshot saved: {snapshot_id[:8]}...")
else:
 print(f"🟡 Would save snapshot: {snapshot_id[:8]}...")

print()

# COMMAND ----------

# DBTITLE 1,📡 Step 3: Fetch Databricks Releases
print("📡 STEP 3: FETCH DATABRICKS RELEASES")
print("="*80)

# Sample Databricks releases (August 2026)
# TODO: Replace with real API call to Databricks release notes

releases_data = [
 {
 "release_id": str(uuid.uuid4()),
 "release_date": date(2026, 8, 1),
 "feature_name": "Predictive I/O 3.0",
 "category": "Performance",
 "description": "50% faster queries on Delta tables with ML-based prefetching",
 "impact_level": "HIGH",
 "documentation_url": "https://docs.databricks.com/delta/predictive-io.html",
 "created_at": datetime.now()
 },
 {
 "release_id": str(uuid.uuid4()),
 "release_date": date(2026, 8, 5),
 "feature_name": "Liquid Clustering GA",
 "category": "Storage",
 "description": "Automatic table optimization without Z-ordering",
 "impact_level": "HIGH",
 "documentation_url": "https://docs.databricks.com/delta/liquid-clustering.html",
 "created_at": datetime.now()
 },
 {
 "release_id": str(uuid.uuid4()),
 "release_date": date(2026, 8, 10),
 "feature_name": "Data Quality Root Cause Analysis",
 "category": "Data Quality",
 "description": "Automatic root cause detection for data quality issues",
 "impact_level": "CRITICAL",
 "documentation_url": "https://docs.databricks.com/data-quality/rca.html",
 "created_at": datetime.now()
 },
 {
 "release_id": str(uuid.uuid4()),
 "release_date": date(2026, 8, 12),
 "feature_name": "Photon Engine 3.5",
 "category": "SQL",
 "description": "40% faster aggregations and joins",
 "impact_level": "HIGH",
 "documentation_url": "https://docs.databricks.com/photon.html",
 "created_at": datetime.now()
 },
 {
 "release_id": str(uuid.uuid4()),
 "release_date": date(2026, 8, 15),
 "feature_name": "Auto-Archive Cold Tables",
 "category": "Cost",
 "description": "Automatically archive unused tables to reduce storage costs by 70%",
 "impact_level": "HIGH",
 "documentation_url": "https://docs.databricks.com/delta/auto-archive.html",
 "created_at": datetime.now()
 },
 {
 "release_id": str(uuid.uuid4()),
 "release_date": date(2026, 8, 18),
 "feature_name": "Enhanced Auto Loader",
 "category": "Ingestion",
 "description": "Schema evolution with column rename and type change handling",
 "impact_level": "MEDIUM",
 "documentation_url": "https://docs.databricks.com/ingestion/auto-loader.html",
 "created_at": datetime.now()
 }
]

print(f"Fetched {len(releases_data)} Databricks releases (August 2026)")
print()

# Category breakdown
categories = {}
for r in releases_data:
 cat = r['category']
 categories[cat] = categories.get(cat, 0) + 1

print("Category breakdown:")
for cat, count in categories.items():
 print(f" {cat}: {count} releases")
print()

# Write releases
if not DRY_RUN:
 releases_df = spark.createDataFrame(releases_data)
 releases_df.write.mode("append").saveAsTable(
 f"{TARGET_CATALOG}.{TARGET_SCHEMA}.databricks_releases"
 )
 print(f"✅ Loaded {len(releases_data)} releases")
else:
 print(f"🟡 Would load {len(releases_data)} releases")

print()

# COMMAND ----------

# DBTITLE 1,🧠 Step 4: AI Relevance Analysis
print("🧠 STEP 4: AI RELEVANCE ANALYSIS")
print("="*80)

# Simple relevance scoring based on domain needs
# TODO: Replace with real AI/LLM analysis

candidates_data = []

for release in releases_data:
 candidate_id = str(uuid.uuid4())

 # Score based on category and impact
 relevance_score = 0.0
 priority = "LOW"
 effort_hours = 0
 reasoning = ""
 requires_approval = False

 if release['category'] == "Performance":
 relevance_score = 85.0
 priority = "HIGH"
 effort_hours = 2
 reasoning = f"{release['feature_name']}: Improves query performance. Low effort, high impact."
 requires_approval = False

 elif release['category'] == "Storage":
 relevance_score = 80.0
 priority = "HIGH"
 effort_hours = 3
 reasoning = f"{release['feature_name']}: Optimizes storage automatically. Recommended for all Delta tables."
 requires_approval = False

 elif release['category'] == "Data Quality":
 relevance_score = 90.0
 priority = "CRITICAL"
 effort_hours = 4
 reasoning = f"{release['feature_name']}: Critical for data reliability. Should be enabled on key tables."
 requires_approval = True

 elif release['category'] == "SQL":
 relevance_score = 75.0
 priority = "HIGH"
 effort_hours = 1
 reasoning = f"{release['feature_name']}: SQL performance boost. No code changes needed."
 requires_approval = False

 elif release['category'] == "Cost":
 relevance_score = 70.0
 priority = "MEDIUM"
 effort_hours = 5
 reasoning = f"{release['feature_name']}: Reduces storage costs. Requires table audit first."
 requires_approval = True

 else:
 relevance_score = 60.0
 priority = "MEDIUM"
 effort_hours = 8
 reasoning = f"{release['feature_name']}: {release['description']}"
 requires_approval = True

 candidates_data.append({
 "candidate_id": candidate_id,
 "release_id": release['release_id'],
 "feature_name": release['feature_name'],
 "category": release['category'],
 "priority": priority,
 "relevance_score": relevance_score,
 "business_reasoning": reasoning,
 "estimated_effort_hours": effort_hours,
 "affected_workloads": f"All {release['category']} workloads",
 "requires_approval": requires_approval,
 "created_at": datetime.now()
 })

print(f"Analyzed {len(candidates_data)} upgrade candidates")
print()

# Priority breakdown
priorities = {}
for c in candidates_data:
 pri = c['priority']
 priorities[pri] = priorities.get(pri, 0) + 1

print("Priority breakdown:")
for pri, count in sorted(priorities.items(), key=lambda x: {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2, 'LOW': 3}.get(x[0], 4)):
 print(f" {pri}: {count} candidates")
print()

total_effort = sum(c['estimated_effort_hours'] for c in candidates_data)
print(f"Total estimated effort: {total_effort} hours")
print()

# Write candidates
if not DRY_RUN:
 candidates_df = spark.createDataFrame(candidates_data)
 candidates_df.write.mode("overwrite").saveAsTable(
 f"{TARGET_CATALOG}.{TARGET_SCHEMA}.upgrade_candidates"
 )
 print(f"✅ Wrote {len(candidates_data)} upgrade candidates")
else:
 print(f"🟡 Would write {len(candidates_data)} upgrade candidates")

print()

# COMMAND ----------

# DBTITLE 1,📊 Step 5: Summary Report
print("📊 STEP 5: SUMMARY REPORT")
print("="*80)
print()

print(f"Pipeline ID: {PIPELINE_ID}")
print(f"Run timestamp: {RUN_TIMESTAMP}")
print(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}")
print()

print("📸 Architecture Snapshot:")
print(f" Domain: {DOMAIN_NAME}")
print(f" Tables: {total_tables}")
print(f" Notebooks: {total_notebooks}")
print(f" Jobs: {total_jobs}")
print()

print("📡 Databricks Releases:")
print(f" Total: {len(releases_data)}")
for cat, count in categories.items():
 print(f" {cat}: {count}")
print()

print("🧠 Upgrade Candidates:")
print(f" Total: {len(candidates_data)}")
for pri, count in sorted(priorities.items(), key=lambda x: {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2, 'LOW': 3}.get(x[0], 4)):
 print(f" {pri}: {count}")
print(f" Total effort: {total_effort} hours")
print()

if DRY_RUN:
 print("🟡 DRY RUN COMPLETE - No data was written")
 print()
 print("Next steps:")
 print(" 1. Review the summary above")
 print(" 2. Set DRY_RUN = False to execute for real")
 print(" 3. Run this notebook again")
else:
 print("✅ PIPELINE COMPLETE - All data written to tables")
 print()
 print("Next steps:")
 print(" 1. Query upgrade candidates: SELECT * FROM workspace.default.upgrade_candidates")
 print(" 2. Ask Genie: 'Show me critical upgrade candidates'")
 print(" 3. Approve features: INSERT INTO workspace.default.approved_upgrades")

print()
print("="*80)

# COMMAND ----------

# DBTITLE 1,📝 Next Steps & Usage
# MAGIC %md
# MAGIC ## 📝 NEXT STEPS
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✅ After Running This Notebook:
# MAGIC
# MAGIC #### **1. Query Upgrade Candidates**
# MAGIC
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC feature_name,
# MAGIC category,
# MAGIC priority,
# MAGIC relevance_score,
# MAGIC estimated_effort_hours,
# MAGIC business_reasoning
# MAGIC FROM workspace.default.upgrade_candidates
# MAGIC WHERE priority IN ('CRITICAL', 'HIGH')
# MAGIC ORDER BY 
# MAGIC CASE priority 
# MAGIC WHEN 'CRITICAL' THEN 1 
# MAGIC WHEN 'HIGH' THEN 2 
# MAGIC ELSE 3 
# MAGIC END,
# MAGIC relevance_score DESC;
# MAGIC ```
# MAGIC
# MAGIC #### **2. Ask Genie**
# MAGIC
# MAGIC ```
# MAGIC "Show me critical upgrade candidates"
# MAGIC "What features reduce costs?"
# MAGIC "Which upgrades take less than 4 hours?"
# MAGIC "Show August 2026 releases by category"
# MAGIC ```
# MAGIC
# MAGIC #### **3. Approve Features for Sprint**
# MAGIC
# MAGIC ```sql
# MAGIC INSERT INTO workspace.default.approved_upgrades
# MAGIC VALUES (
# MAGIC 'candidate_id_here',
# MAGIC 'Predictive I/O 3.0',
# MAGIC 'tech@skillaipath.com',
# MAGIC current_timestamp(),
# MAGIC '2026-08',
# MAGIC 'pending',
# MAGIC 'Approved in sprint planning - high impact, low effort'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔄 Monthly Workflow:
# MAGIC
# MAGIC 1. **1st of month:** Run this notebook (set `DRY_RUN = False`)
# MAGIC 2. **2nd-5th:** Review candidates with Genie
# MAGIC 3. **Sprint planning:** Approve features via SQL
# MAGIC 4. **Throughout month:** Implement approved upgrades
# MAGIC 5. **End of month:** Review deployment log
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧹 Cleanup / Reset:
# MAGIC
# MAGIC If you want to start fresh:
# MAGIC
# MAGIC 1. Set `CLEANUP_BEFORE_RUN = True`
# MAGIC 2. Run this notebook
# MAGIC 3. All tables will be dropped and recreated
# MAGIC
# MAGIC **OR** run this SQL manually:
# MAGIC
# MAGIC ```sql
# MAGIC DROP TABLE IF EXISTS workspace.default.architecture_snapshot;
# MAGIC DROP TABLE IF EXISTS workspace.default.databricks_releases;
# MAGIC DROP TABLE IF EXISTS workspace.default.upgrade_candidates;
# MAGIC DROP TABLE IF EXISTS workspace.default.approved_upgrades;
# MAGIC DROP TABLE IF EXISTS workspace.default.upgrade_deployment_log;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Limitations:
# MAGIC
# MAGIC * **Mock data:** Currently uses sample releases (update Step 3 to fetch real releases)
# MAGIC * **Simple AI:** Basic relevance scoring (enhance with LLM/Genie API)
# MAGIC * **Manual approval:** Features must be manually approved via SQL
# MAGIC * **Monthly runs:** Designed for monthly cadence (history accumulates)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Enhancements (Future):
# MAGIC
# MAGIC * Fetch real Databricks release notes via API
# MAGIC * Use Genie API for AI relevance analysis
# MAGIC * Automatic approval for low-risk features
# MAGIC * Slack/email notifications
# MAGIC * Deployment automation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Questions? Ask Genie:**
# MAGIC ```
# MAGIC "How do I use the Monthly Upgrade System?"
# MAGIC "Show me the upgrade workflow"
# MAGIC ```

# COMMAND ----------
