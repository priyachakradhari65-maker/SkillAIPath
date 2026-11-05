# Databricks notebook source
# DBTITLE 1,SkillAIPath - Demo & Session Calendar
# MAGIC %md
# MAGIC # SkillAIPath: Demo & Session Calendar
# MAGIC ## Team-Based Progress Tracker (No Deadlines)
# MAGIC
# MAGIC **Purpose:** Track readiness checkpoints, schedule demos when teams complete work
# MAGIC
# MAGIC **Model:** 9 pods working in parallel at natural pace
# MAGIC **Progress:** Biweekly sync points (not deadlines)
# MAGIC **Demos:** Scheduled when work is READY, not by calendar
# MAGIC
# MAGIC **Last Updated:** 2026-08-05
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Team Readiness Dashboard
# MAGIC
# MAGIC ### Legend
# MAGIC - ✅ **COMPLETE** - Module finished and validated
# MAGIC - 🚧 **IN PROGRESS** - Currently working
# MAGIC - 🟡 **READY TO START** - No blockers, can begin
# MAGIC - ⏸️ **BLOCKED** - Waiting on dependency
# MAGIC - 📅 **DEMO SCHEDULED** - Session planned
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Team Progress Status
# MAGIC
# MAGIC ### Pod 6: Data Ingestion Squad
# MAGIC | Component | Status | Output | Blocks | Notes |
# MAGIC |-----------|--------|--------|--------|-------|
# MAGIC | Sample Data Generation | 🚧 IN PROGRESS | Bronze layer data | MED-002 | 30-day dataset |
# MAGIC | Auto Loader Setup | 🟡 READY | Incremental ingestion | - | Docs ready |
# MAGIC | Volume Uploads | 🟡 READY | CSV/JSON/Parquet patterns | - | Template ready |
# MAGIC | REST API Ingestion | 🟡 READY | External API data | - | Can start |
# MAGIC | Marketplace Integration | 🟡 READY | Community datasets | - | Can start |
# MAGIC
# MAGIC ### Pod 1+2: Medallion Architecture Squad
# MAGIC | Component | Status | Output | Blocks | Notes |
# MAGIC |-----------|--------|--------|--------|-------|
# MAGIC | Config & Environment | ✅ COMPLETE | DEV/UAT/PROD detection | - | StartHere Cell 2 done |
# MAGIC | Bronze Layer | 🚧 IN PROGRESS | 3 tables with data | MED-004, ML-001 | Ready for silver |
# MAGIC | Silver Dimensions (9) | ⏸️ BLOCKED | Clean dimensions | MED-005 | Waiting on bronze |
# MAGIC | Silver Facts (3) | ⏸️ BLOCKED | Sales, Purchases, Inventory | MED-008 | Waiting on dims |
# MAGIC | Gold Marts (3) | ⏸️ BLOCKED | Customer 360, Product Perf, RFM | ML-001, RAG-001 | Waiting on facts |
# MAGIC | Data Quality Suite | 🟡 READY | Validation framework | - | Can start after dims |
# MAGIC
# MAGIC ### Pod 5: DBT Transformation Squad
# MAGIC | Component | Status | Output | Blocks | Notes |
# MAGIC |-----------|--------|--------|--------|-------|
# MAGIC | DBT Project Setup | ⏸️ BLOCKED | DBT initialized | DBT-002 | Waiting for stable SQL |
# MAGIC | Convert to DBT Models | ⏸️ BLOCKED | Tested DBT models | DBT-003 | Waiting for silver/gold |
# MAGIC | Testing Framework | ⏸️ BLOCKED | Schema + data tests | - | Waiting for models |
# MAGIC | Multi-Env Profiles | 🟡 READY | DEV/UAT/PROD configs | - | Can configure now |
# MAGIC
# MAGIC ### Pod 4: Streaming & CDC Squad
# MAGIC | Component | Status | Output | Blocks | Notes |
# MAGIC |-----------|--------|--------|--------|-------|
# MAGIC | Streaming Sales | ⏸️ BLOCKED | Real-time fact_sales | STR-007 | Waiting for batch sales proven |
# MAGIC | Customer CDC | ⏸️ BLOCKED | SCD tracking | - | Waiting for batch dims |
# MAGIC | Streaming Inventory | ⏸️ BLOCKED | Real-time inventory | - | Waiting for batch inventory |
# MAGIC | Late-Arriving Data | 🟡 READY | Watermark patterns | - | Docs ready |
# MAGIC | Stream Monitoring | 🟡 READY | Lag metrics dashboard | - | Can build now |
# MAGIC
# MAGIC ### Pod 5: ModelTrainers (Pod 5)
# MAGIC | Component | Status | Output | Blocks | Notes |
# MAGIC |-----------|--------|--------|--------|-------|
# MAGIC | Feature Store | ⏸️ BLOCKED | UC feature tables | ML-002 | Waiting for gold layer |
# MAGIC | Churn Model Training | ⏸️ BLOCKED | Trained churn model | ML-005 | Waiting for features |
# MAGIC | Demand Forecast Model | ⏸️ BLOCKED | Demand model | - | Waiting for features |
# MAGIC | Churn Endpoint | ⏸️ BLOCKED | REST API | - | Waiting for model |
# MAGIC | Model Monitoring | 🟡 READY | Drift dashboard | - | Can build framework |
# MAGIC
# MAGIC ### Team 5B: RAG & AI Agents Squad
# MAGIC | Component | Status | Output | Blocks | Notes |
# MAGIC |-----------|--------|--------|--------|-------|
# MAGIC | Vector Search Index | ⏸️ BLOCKED | Embeddings infra | RAG-002 | Waiting for gold data |
# MAGIC | Product Embeddings | ⏸️ BLOCKED | Product search | RAG-004 | Waiting for index |
# MAGIC | RAG Pipeline | ⏸️ BLOCKED | Retrieval chain | AGT-001 | Waiting for embeddings |
# MAGIC | AI Gateway | 🟡 READY | Multi-model routing | - | Can configure now |
# MAGIC | SQL StoryBuilders/ImpactMakers Agent | ⏸️ BLOCKED | Query agent | - | Waiting for gold layer |
# MAGIC | Churn Prevention Agent | ⏸️ BLOCKED | Retention bot | - | Waiting for churn model |
# MAGIC | Agent Framework | 🟡 READY | Function calling base | - | Can build now |
# MAGIC
# MAGIC ### Team 6: Business Intelligence Squad
# MAGIC | Component | Status | Output | Blocks | Notes |
# MAGIC |-----------|--------|--------|--------|-------|
# MAGIC | Pipeline Monitoring Dashboard | 🚧 IN PROGRESS | Health metrics | - | Can work on now |
# MAGIC | Executive Dashboard | ⏸️ BLOCKED | KPI dashboard | - | Waiting for gold marts |
# MAGIC | Sales Dashboard | ⏸️ BLOCKED | Sales analytics | - | Waiting for gold |
# MAGIC | Operational Dashboard | ⏸️ BLOCKED | Inventory/ops | - | Waiting for gold |
# MAGIC | Intelligence Hub App | ⏸️ BLOCKED | RAG analytics app | - | Waiting for RAG |
# MAGIC | Agents Dashboard App | ⏸️ BLOCKED | Agent monitoring | - | Waiting for agents |
# MAGIC | Genie Space | ⏸️ BLOCKED | Conversational BI | - | Waiting for gold |
# MAGIC | Cost Tracking | 🟡 READY | Cost dashboard | - | Can build now |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Next Biweekly Sync Checkpoint
# MAGIC
# MAGIC **Who Can Present:**
# MAGIC - Pod 6: Sample data generation progress
# MAGIC - Pod 1+2: Bronze → Silver transformation
# MAGIC - Team 6: Pipeline monitoring dashboard
# MAGIC
# MAGIC **Coordination Decisions:**
# MAGIC - When will silver dimensions be complete? (Unblocks Pod 5 DBT)
# MAGIC - When will gold layer be ready? (Unblocks Teams 5A, 5B)
# MAGIC - Any blockers or help needed?
# MAGIC
# MAGIC **Demo Opportunities:**
# MAGIC - If bronze layer complete: Demo "Data Ingestion Patterns"
# MAGIC - If pipeline monitoring ready: Demo "Platform Observability"
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Import Libraries & Setup
from datetime import datetime, timedelta
import pandas as pd
from pyspark.sql.functions import col, lit, current_timestamp

# Get current date
today = datetime.now()
print(f"Calendar Updated: {today.strftime('%Y-%m-%d %H:%M')}")
print(f"Target: 100% Platform Live by {(today + timedelta(days=42)).strftime('%Y-%m-%d')}")

# COMMAND ----------

# DBTITLE 1,Demo Schedule
# MAGIC %md
# MAGIC ## 📅 Demo & Session Calendar (Readiness-Based)
# MAGIC
# MAGIC **Model:** Demos scheduled when teams complete milestones, NOT by calendar date
# MAGIC
# MAGIC ### Completed Demos
# MAGIC
# MAGIC | Demo ID | Completed | Duration | Topic | Team | Components Shown | Recording | LinkedIn Post |
# MAGIC |---------|-----------|----------|-------|------|------------------|-----------|---------------|
# MAGIC | DEMO-000 | 2026-08-05 | - | Foundation Setup | Medallion | Config auto-detection, catalog provisioning | - | - |
# MAGIC
# MAGIC ### Ready to Schedule (Team Has Completed Work)
# MAGIC
# MAGIC | Demo ID | Team | Topic | Components | Status | Schedule When |
# MAGIC |---------|------|-------|------------|--------|---------------|
# MAGIC | DEMO-001 | Pod 6 | Data Ingestion Patterns | Sample data, Auto Loader, volumes | 🟡 NEARLY READY | After ING-001 complete |
# MAGIC | DEMO-002 | Team 6 | Pipeline Monitoring | Health dashboard, alerts | 🟡 NEARLY READY | After BI-001 complete |
# MAGIC
# MAGIC ### Future Demos (Waiting on Dependencies)
# MAGIC
# MAGIC | Demo ID | Team | Topic | Components | Blocked By | Can Schedule After |
# MAGIC |---------|------|-------|------------|------------|--------------------|
# MAGIC | DEMO-003 | Pod 1+2 | Medallion Architecture Live | Bronze→Silver→Gold, 3 marts | MED-010 | Gold layer complete |
# MAGIC | DEMO-004 | Pod 5 | DBT in Production | DBT models, tests, docs | DBT-005 | DBT fully implemented |
# MAGIC | DEMO-005 | Pod 4 | Real-Time Streaming | CDC, streaming tables | STR-007 | Streaming pipelines live |
# MAGIC | DEMO-006 | Pod 5 | ModelTrainers End-to-End | Feature Store, serving, monitoring | ML-009 | All models deployed |
# MAGIC | DEMO-007 | Team 5B | AI Agents Live | 4 agents operational, observability | AGT-006 | All agents deployed |
# MAGIC | DEMO-008 | Team 6 | Full BI Stack | Dashboards, Apps, Genie | AGT-006 | All apps deployed |
# MAGIC | DEMO-009 | All Teams | Platform Sales Demo | Complete end-to-end | All above | Full platform live |
# MAGIC
# MAGIC ### Customer/Partner Sessions
# MAGIC
# MAGIC | Session ID | Company | Topic | Requested Components | Assigned Team | Status | Notes |
# MAGIC |------------|---------|-------|---------------------|---------------|--------|-------|
# MAGIC | CUST-001 | TBD | - | - | - | ⏸️ Pending | Waiting for first customer request |
# MAGIC
# MAGIC ### Biweekly Sync Sessions
# MAGIC
# MAGIC | Sync | Date | Teams Presenting | Topics | Decisions Made | Recording |
# MAGIC |------|------|------------------|--------|----------------|----------|
# MAGIC | Sync-01 | TBD | All | Initial kickoff, team assignments | Team structure finalized | - |
# MAGIC | Sync-02 | TBD | 1, 2, 6 | Progress updates, blockers | - | - |
# MAGIC | Sync-03 | TBD | - | - | - | - |
# MAGIC
# MAGIC ### Demo Prep Checklist (When Scheduling)
# MAGIC
# MAGIC **Pre-Demo (After scheduling, before demo day):**
# MAGIC - [ ] Validate all components working
# MAGIC - [ ] Prepare demo script (3-5 key talking points)
# MAGIC - [ ] Create backup data snapshot
# MAGIC - [ ] Test queries/features to show
# MAGIC - [ ] Notify attendees
# MAGIC
# MAGIC **During Demo:**
# MAGIC - [ ] Record session (30-45 min)
# MAGIC - [ ] Show live features (not slides)
# MAGIC - [ ] Capture Q&A
# MAGIC
# MAGIC **Post-Demo:**
# MAGIC - [ ] Extract 2-3 LinkedIn clips (60-90 sec each)
# MAGIC - [ ] Write LinkedIn post about demo
# MAGIC - [ ] Update brain pack with learnings
# MAGIC - [ ] Add to portfolio showcase
# MAGIC - [ ] Schedule next demo
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Query Live Deployment Status
# ============================================================================
# LIVE DEPLOYMENT STATUS QUERY
# ============================================================================
# This cell queries actual workspace state to show what's REALLY deployed

print("\n" + "="*80)
print(" LIVE DEPLOYMENT STATUS CHECK")
print("="*80)

# Check catalogs
print("\n📦 CATALOGS")
try:
 catalogs_df = spark.sql("SHOW CATALOGS")
 catalogs = [row.catalog for row in catalogs_df.collect()]

 sap_catalogs = [c for c in catalogs if c.startswith('sap_')]
 if sap_catalogs:
 print(f"✅ SkillAIPath Catalogs: {', '.join(sap_catalogs)}")

 # Check schemas in each catalog
 for cat in sap_catalogs:
 schemas_df = spark.sql(f"SHOW SCHEMAS IN {cat}")
 schemas = [row.databaseName for row in schemas_df.collect()]
 print(f" {cat}: {', '.join(schemas)}")

 # Check table counts
 for schema in schemas:
 try:
 tables_df = spark.sql(f"SHOW TABLES IN {cat}.{schema}")
 table_count = tables_df.count()
 print(f" {schema}: {table_count} tables")
 except:
 print(f" {schema}: 0 tables")
 else:
 print("⚠️ No sap_* catalogs found - run StartHere to provision")
except Exception as e:
 print(f"⚠️ Cannot query catalogs: {str(e)[:100]}")

# Check model serving endpoints
print("\n🤖 MODEL SERVING ENDPOINTS")
try:
 from databricks.sdk import WorkspaceClient
 w = WorkspaceClient()
 endpoints = list(w.serving_endpoints.list())

 if endpoints:
 print(f"✅ {len(endpoints)} endpoints found")
 for ep in endpoints:
 print(f" {ep.name}: {ep.state.config_update}")
 else:
 print("⚠️ No model serving endpoints deployed")
except Exception as e:
 print(f"⚠️ Cannot query endpoints: {str(e)[:100]}")

# Check Databricks Apps
print("\n📱 DATABRICKS APPS")
try:
 apps = list(w.apps.list())

 if apps:
 print(f"✅ {len(apps)} apps found")
 for app in apps:
 print(f" {app.name}: {app.status}")
 else:
 print("⚠️ No Databricks Apps deployed")
except Exception as e:
 print(f"⚠️ Cannot query apps: {str(e)[:100]}")

# Check jobs
print("\n⚙️ SCHEDULED JOBS")
try:
 jobs = list(w.jobs.list())
 sap_jobs = [j for j in jobs if 'skillaipath' in j.settings.name.lower() or 'sap_' in j.settings.name.lower()]

 if sap_jobs:
 print(f"✅ {len(sap_jobs)} SkillAIPath jobs found")
 for job in sap_jobs:
 print(f" {job.settings.name}: {job.settings.schedule}")
 else:
 print("⚠️ No SkillAIPath jobs found")
except Exception as e:
 print(f"⚠️ Cannot query jobs: {str(e)[:100]}")

print("\n" + "="*80)
print(" STATUS CHECK COMPLETE")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Next Actions
# MAGIC %md
# MAGIC ## Next Actions (No Deadlines - Natural Pace)
# MAGIC
# MAGIC ### Immediate (Can Start Now)
# MAGIC
# MAGIC **Pod 6 (Ingestion):**
# MAGIC - [ ] Complete sample data generation (ING-001)
# MAGIC - [ ] Bronze layer will then have data for Pod 1+2
# MAGIC
# MAGIC **Pod 1+2 (Medallion):**
# MAGIC - [ ] Run StartHere Cells 9-12 (complete silver dimensions + facts + gold marts)
# MAGIC - [ ] This unblocks Teams 3, 4, 5A, 5B
# MAGIC
# MAGIC **Team 6 (BI):**
# MAGIC - [ ] Complete pipeline monitoring dashboard (BI-001)
# MAGIC - [ ] Ready to demo when done
# MAGIC
# MAGIC **Cross-Team:**
# MAGIC - [ ] Setup Git workflow (PROD-001)
# MAGIC - [ ] Start office hours cadence (SUPP-001)
# MAGIC
# MAGIC ### Ready at Next Biweekly Sync
# MAGIC
# MAGIC **To Announce:**
# MAGIC - Pod 6: Data generation status
# MAGIC - Pod 1+2: Silver/gold completion (if done)
# MAGIC - Team 6: Monitoring dashboard (if done)
# MAGIC
# MAGIC **To Coordinate:**
# MAGIC - When can DBT team start? (needs stable SQL)
# MAGIC - When can ModelTrainers start? (needs gold layer)
# MAGIC - Any blockers?
# MAGIC
# MAGIC **Demo Opportunities:**
# MAGIC - If work complete: Schedule demo sessions
# MAGIC - If not complete: Set goals for next sync
# MAGIC
# MAGIC ### Update This Calendar After Each Milestone
# MAGIC
# MAGIC **When a team completes work:**
# MAGIC 1. Run the status check cell (Cell 4) to get current state
# MAGIC 2. Update the "Team Progress Status" tables above
# MAGIC 3. Move status from 🚧/⏸️ → ✅
# MAGIC 4. Identify newly unblocked teams
# MAGIC 5. Schedule demo if milestone warrants
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Session Recording Workflow
# MAGIC
# MAGIC ### Two Recording Fronts
# MAGIC
# MAGIC **Front 1: Foundation Track (Record as Pod 1+2 Completes)**
# MAGIC - Day 1-2: Data Ingestion (ING tasks)
# MAGIC - Day 3: Medallion Architecture (MED-001 through MED-010)
# MAGIC - Day 4: StoryBuilders/ImpactMakers Layer (gold marts)
# MAGIC - Day 5: Production Readiness (monitoring, quality)
# MAGIC
# MAGIC **Front 2: Advanced Track (Record Now as You Implement)**
# MAGIC - AI Agents & Apps (AGT + BI-008, BI-009)
# MAGIC - Advanced Dashboards (BI-004 through BI-007)
# MAGIC - Flash Multi-Ingestion (ING advanced + STR)
# MAGIC - ModelTrainers Production (ML-005 through ML-009)
# MAGIC
# MAGIC ### Post-Session Checklist
# MAGIC 1. [ ] Update team status in this calendar
# MAGIC 2. [ ] Upload recording
# MAGIC 3. [ ] Extract 2-3 LinkedIn clips (60-90 sec)
# MAGIC 4. [ ] Write LinkedIn post
# MAGIC 5. [ ] Update brain pack with patterns/learnings
# MAGIC 6. [ ] Add to portfolio showcase
# MAGIC 7. [ ] Identify next demo opportunity
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎓 Business Operating Model
# MAGIC
# MAGIC **What Founder Does (Human Work):**
# MAGIC - Record demos as teams complete work
# MAGIC - Run biweekly sync sessions
# MAGIC - Create LinkedIn content from sessions
# MAGIC - Customer/partner conversations
# MAGIC - Sales demos (when platform ready)
# MAGIC
# MAGIC **What Teams Do (500 Students):**
# MAGIC - 6 functional teams building different modules
# MAGIC - Natural pace with biweekly sync points
# MAGIC - Portfolio-based assessment (not time-based)
# MAGIC - Peer learning within teams
# MAGIC
# MAGIC **What AI + Delegation Handles:**
# MAGIC - Development sprint coordination
# MAGIC - Platform monitoring (24/7)
# MAGIC - Marketing automation
# MAGIC - Support operations
# MAGIC - Accounts & hiring
# MAGIC
# MAGIC **Target Operating Model:**
# MAGIC Founder only does: Content + Demos + Sales Conversations
# MAGIC Everything else: Automated or delegated
# MAGIC
# MAGIC ---

# COMMAND ----------
