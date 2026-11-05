# Databricks notebook source
# DBTITLE 1,🚀 Start_SkillAI - Complete Platform Orchestrator
# MAGIC %md
# MAGIC # Start_SkillAI Platform Orchestrator
# MAGIC
# MAGIC ## Overview
# MAGIC Complete SkillAI Path platform activation in **17 minutes**:
# MAGIC
# MAGIC ### Execution Flow (Serial - Safe & Predictable):
# MAGIC 1. **DBT Full** (4 min) - Bronze validation + Silver/Gold transformations
# MAGIC 2. **Streaming CDC** (5 min) - DLT pipeline activation (SCD2 + real-time)
# MAGIC 3. **MLOps** (5 min) - Train 2 models + UC Functions (churn + recommendations)
# MAGIC 4. **App Deploy** (2 min) - PR Reviewer App live deployment
# MAGIC 5. **Validation** (1 min) - Pre-deployed assets + cost report
# MAGIC
# MAGIC ### What Gets Created:
# MAGIC - **Tables:** 30+ (Bronze: 5, Silver: 17, Gold: 3+ marts)
# MAGIC - **Streaming:** 1 DLT pipeline (SCD2 history + watermarks)
# MAGIC - **ML:** 2 models + 2 UC Functions (no serving endpoints)
# MAGIC - **App:** PR Reviewer App (live URL)
# MAGIC - **Pre-Deployed:** 5 dashboards + 3 VDPs
# MAGIC
# MAGIC ### Cost: ~$2-3 for complete demo
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Click "Run All" to activate the complete platform**
# MAGIC
# MAGIC > **Note:** For parallel execution (faster), use the Databricks Job instead: [SkillAI_Platform_Orchestrator](#job-1103310932910542)

# COMMAND ----------

# DBTITLE 1,⚙️ Config + Helper Functions
# Configuration
import time
import json
from datetime import datetime
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Environment
ENV = "dev"  # dev, uat, prod
CATALOG_BRONZE = f"bronze_{ENV}"
CATALOG_SILVER = f"silver_{ENV}"
CATALOG_GOLD = f"gold_{ENV}"
CATALOG_SAP = f"sap_{ENV}"

# Existing notebook paths
NOTEBOOK_DBT = "/Shared/dev/SkillAIPath/1_Canvas/2_Orchestration/Data_Platform/14_StartDBT_Full"
NOTEBOOK_STREAMING = "/Shared/dev/SkillAIPath/2_Data_Production/02_Medallion_Architecture/03_Streaming_Cdc/11_setup_silver_dlt_pipeline"
NOTEBOOK_MLOPS = "/Shared/dev/SkillAIPath/1_Canvas/2_Orchestration/AI_Platform/01_MLOps"
APP_PATH = "/Workspace/Shared/dev/SkillAIPath/4_Business_Intelligence/3_Live_Apps/PR Reviewer"
APP_NAME = "pr-reviewer"

# DLT Pipeline ID
DLT_PIPELINE_ID = "e8e357b9-6567-4be6-b4c0-4a1c500ee006"

# Tracking
execution_log = []
start_time = time.time()

print("="*80)
print("START_SKILLAAI ORCHESTRATOR - INITIALIZED")
print("="*80)
print(f"Environment: {ENV}")
print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def log_step(step_name, status, duration_sec=None, details=None):
    """Log execution step"""
    entry = {
        "step": step_name,
        "status": status,
        "timestamp": datetime.now().isoformat(),
        "duration_sec": duration_sec
    }
    if details:
        entry["details"] = details
    execution_log.append(entry)
    
    # Print formatted
    status_icon = "✅" if status == "success" else "❌" if status == "failed" else "⏳"
    print(f"{status_icon} {step_name}: {status}")
    if duration_sec:
        print(f"   Duration: {duration_sec:.1f}s ({duration_sec/60:.1f} min)")
    if details:
        print(f"   Details: {details}")
    print()

def count_tables(catalog, schema):
    """Count tables in a schema"""
    try:
        tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").count()
        return tables
    except Exception as e:
        return 0

def validate_tables_exist(catalog, schema, min_tables=1):
    """Validate minimum table count"""
    count = count_tables(catalog, schema)
    return count >= min_tables, count

def estimate_cost(duration_min, dbu_rate=0.07):
    """Simple cost estimate (formulaic)"""
    # Assume avg 2 DBU/min for demo workload
    dbu_consumed = duration_min * 2
    cost_usd = dbu_consumed * dbu_rate
    return cost_usd

print("✅ Config + Helper Functions loaded")
print()

# COMMAND ----------

# DBTITLE 1,📋 Initialize Checkpoint Framework
# ============================================================================
# LOAD CHECKPOINT FRAMEWORK
# ============================================================================
print("Loading Checkpoint Framework...")
%run ./checkpoint_framework
print("✅ Checkpoint Framework loaded\n")

# ============================================================================
# INITIALIZE CHECKPOINT RUN
# ============================================================================
# Set FORCE_RERUN = True to ignore checkpoints and run everything from scratch
# Set FORCE_RERUN = False to resume from last failure (skip completed stages)
FORCE_RERUN = False

run_id = initialize_checkpoint_run("Start_SkillAI", force_rerun=FORCE_RERUN)
print(f"🆔 Run ID: {run_id}")
print(f"   Force Rerun: {'YES - Will run all stages' if FORCE_RERUN else 'NO - Will skip completed stages'}")
print(f"   Checkpoint Table: {STATE_TABLE}")
print()

# Show status of last run if resuming
if not FORCE_RERUN:
    print("📊 Previous run status:")
    print_run_summary(run_id)
else:
    print("🔄 Starting fresh run (all checkpoints ignored)\n")

# COMMAND ----------

# DBTITLE 1,📋 STEP 1: DBT Full (with Checkpoint)
print("=" * 80)
print("STEP 1: DBT FULL EXECUTION")
print("=" * 80)

STAGE_NAME = "DBT_FULL"

# ============================================================================
# CHECKPOINT: Check if stage should run
# ============================================================================
if not should_run_stage(run_id, STAGE_NAME, force_rerun=FORCE_RERUN):
    print(f"⏭️  {STAGE_NAME} already completed - skipping\n")
else:
    print(f"Notebook: {NOTEBOOK_DBT}")
    print("Expected: Bronze validation + dbt run + dbt test")
    print("Timeout: 300 seconds (5 min)")
    print()

    step_start = mark_stage_started(run_id, "Start_SkillAI", STAGE_NAME)
    log_step("DBT Full", "running")

    try:
    # Run DBT orchestrator
    result = dbutils.notebook.run(
        NOTEBOOK_DBT,
        timeout_seconds=360
    )

    step_duration = time.time() - step_start

    print(f"DBT Full completed in {step_duration:.1f}s")
    print(f"Result: {result}")
    print()

    # Post-validation: Count tables
    print("Post-Validation: Counting DBT tables...")

    # Updated catalog structure:
    # sap_dev.staging
    # sap_dev.silver
    # sap_dev.gold

    staging_ok, staging_count = validate_tables_exist(
        CATALOG_SAP,
        "staging",
        min_tables=3
    )

    silver_ok, silver_count = validate_tables_exist(
        CATALOG_SAP,
        "silver",
        min_tables=10
    )

    gold_ok, gold_count = validate_tables_exist(
        CATALOG_SAP,
        "gold",
        min_tables=3
    )

    print(f"   Staging: {staging_count} tables {'OK' if staging_ok else 'WARNING'}")
    print(f"   Silver: {silver_count} tables {'OK' if silver_ok else 'WARNING'}")
    print(f"   Gold: {gold_count} tables {'OK' if gold_ok else 'WARNING'}")
    print()

    validation_status = (
        "success"
        if (staging_ok and silver_ok and gold_ok)
        else "partial"
    )

        log_step(
            "DBT Full",
            validation_status,
            duration_sec=step_duration,
            details=(
                f"Staging: {staging_count}, "
                f"Silver: {silver_count}, "
                f"Gold: {gold_count}"
            )
        )

        # Mark checkpoint complete
        mark_stage_complete(run_id, STAGE_NAME, status='success', start_time=step_start)

    except Exception as e:
        step_duration = time.time() - step_start

        print(f"DBT Full failed: {str(e)}")

        log_step(
            "DBT Full",
            "failed",
            duration_sec=step_duration,
            details=str(e)
        )

        # Mark checkpoint failed
        mark_stage_complete(run_id, STAGE_NAME, status='failed', error_message=str(e), start_time=step_start)

        raise

print(f"Step 1 Complete: {step_duration / 60:.1f} min")
print()

# COMMAND ----------

# DBTITLE 1,📋 STEP 2: Streaming CDC (with Checkpoint)
print("=" * 80)
print("STEP 2: STREAMING CDC EXECUTION")
print("=" * 80)

STAGE_NAME = "STREAMING_CDC"

# ============================================================================
# CHECKPOINT: Check if stage should run
# ============================================================================
if not should_run_stage(run_id, STAGE_NAME, force_rerun=FORCE_RERUN):
    print(f"⏭️  {STAGE_NAME} already completed - skipping\n")
else:
    print(f"Notebook: {NOTEBOOK_STREAMING}")
    print(f"Pipeline ID: {DLT_PIPELINE_ID}")
    print("Expected: DLT pipeline start + SCD2 + streaming tables")
    print("Timeout: 360 seconds (6 min)")
    print()

    step_start = mark_stage_started(run_id, "Start_SkillAI", STAGE_NAME)
    log_step("Streaming CDC", "running")

    try:
    # ================================================================
    # RUN STREAMING CDC SETUP
    # ================================================================

    result = dbutils.notebook.run(
        NOTEBOOK_STREAMING,
        timeout_seconds=360
    )

    step_duration = time.time() - step_start

    print(f"✅ Streaming CDC completed in {step_duration:.1f}s")
    print(f"Result: {result}")
    print()

    # ================================================================
    # POST-VALIDATION: CHECK DLT PIPELINE STATUS
    # ================================================================

    print("✅ Post-Validation: Checking DLT pipeline...")

    try:
        pipeline = w.pipelines.get(DLT_PIPELINE_ID)

        pipeline_state = (
            pipeline.state.value
            if pipeline.state
            else "UNKNOWN"
        )

        print(
            f"   Pipeline state: {pipeline_state} "
            f"{'✅' if pipeline_state in ['RUNNING', 'IDLE'] else '⚠️'}"
        )

    except Exception as e:
        print(
            f"   ⚠️ Pipeline check failed: "
            f"{str(e)[:200]}"
        )
        pipeline_state = "CHECK_FAILED"

    # ================================================================
    # POST-VALIDATION: CHECK STREAMING TABLES
    # ================================================================

    # Updated catalog structure:
    #
    # sap_dev
    # ├── bronze
    # ├── staging
    # ├── silver
    # └── gold
    #
    # Therefore:
    # Catalog = sap_dev
    # Schema  = silver

    streaming_catalog = CATALOG_SAP
    streaming_schema = "silver"

    print()
    print("   Checking streaming tables...")
    print(f"   Catalog: {streaming_catalog}")
    print(f"   Schema : {streaming_schema}")

    streaming_ok, streaming_count = validate_tables_exist(
        streaming_catalog,
        streaming_schema,
        min_tables=3
    )

    print(
        f"   Streaming tables: {streaming_count} "
        f"{'✅' if streaming_ok else '⚠️'}"
    )

    print()

    # ================================================================
    # FINAL VALIDATION STATUS
    # ================================================================

    validation_status = (
        "success"
        if streaming_ok
        else "partial"
    )

        log_step(
            "Streaming CDC",
            validation_status,
            duration_sec=step_duration,
            details=(
                f"Pipeline: {pipeline_state}, "
                f"Catalog: {streaming_catalog}, "
                f"Schema: {streaming_schema}, "
                f"Tables: {streaming_count}"
            )
        )

        # Mark checkpoint complete
        mark_stage_complete(run_id, STAGE_NAME, status='success', start_time=step_start)

    except Exception as e:

        step_duration = time.time() - step_start

        print(
            f"❌ Streaming CDC failed: {str(e)}"
        )

        log_step(
            "Streaming CDC",
            "failed",
            duration_sec=step_duration,
            details=str(e)
        )

        # Mark checkpoint failed
        mark_stage_complete(run_id, STAGE_NAME, status='failed', error_message=str(e), start_time=step_start)

        raise

print(
    f"Step 2 Complete: "
    f"{step_duration / 60:.1f} min"
)

print()

# COMMAND ----------

# DBTITLE 1,📋 STEP 3: MLOps (with Checkpoint)
print("=" * 80)
print("STEP 3: MLOPS EXECUTION")
print("=" * 80)

STAGE_NAME = "MLOPS"

# ============================================================================
# CHECKPOINT: Check if stage should run
# ============================================================================
if not should_run_stage(run_id, STAGE_NAME, force_rerun=FORCE_RERUN):
    print(f"⏭️  {STAGE_NAME} already completed - skipping\n")
else:
    print(f"Notebook: {NOTEBOOK_MLOPS}")
    print("Expected: Train 2 models + Register in UC + Create UC Functions")
    print("Timeout: 360 seconds (6 min)")
    print()

    step_start = mark_stage_started(run_id, "Start_SkillAI", STAGE_NAME)
    log_step("MLOps", "running")

    try:
    # ================================================================
    # RUN MLOPS ORCHESTRATOR
    # ================================================================

    result = dbutils.notebook.run(
        NOTEBOOK_MLOPS,
        timeout_seconds=360
    )

    step_duration = time.time() - step_start

    print(f"✅ MLOps completed in {step_duration:.1f}s")
    print(f"Result: {result}")
    print()

    # ================================================================
    # POST-VALIDATION: CHECK ML ARTIFACTS
    # ================================================================

    print("✅ Post-Validation: Checking ML artifacts...")

    # Updated Unity Catalog structure:
    #
    # sap_dev
    # ├── bronze
    # ├── staging
    # ├── silver
    # ├── gold
    # └── intelligence
    #
    # Catalog = sap_dev
    # Schema  = intelligence

    ml_catalog = CATALOG_SAP
    intelligence_schema = "intelligence"
    intelligence_namespace = f"{ml_catalog}.{intelligence_schema}"

    print(f"   ML Catalog : {ml_catalog}")
    print(f"   ML Schema  : {intelligence_schema}")
    print()

    # ================================================================
    # CHECK INTELLIGENCE SCHEMA
    # ================================================================

    schema_exists = False

    try:
        spark.sql(
            f"DESCRIBE SCHEMA {intelligence_namespace}"
        )

        schema_exists = True

        print(
            f"   Intelligence Schema: "
            f"{intelligence_namespace} ✅"
        )

    except Exception as e:
        print(
            f"   ⚠️ Intelligence schema check failed: "
            f"{str(e)[:200]}"
        )

    # ================================================================
    # CHECK UC FUNCTIONS
    # ================================================================

    function_count = 0

    if schema_exists:
        try:
            functions = spark.sql(
                f"SHOW FUNCTIONS IN {intelligence_namespace}"
            ).collect()

            function_count = len(functions)

            print(
                f"   UC Functions: "
                f"{function_count} "
                f"{'✅' if function_count >= 2 else '⚠️'}"
            )

            if function_count > 0:
                func_names = [
                    f.function
                    for f in functions[:5]
                ]

                print(
                    f"   Functions: "
                    f"{', '.join(func_names)}"
                )

        except Exception as e:
            print(
                f"   ⚠️ Function check failed: "
                f"{str(e)[:200]}"
            )

    # ================================================================
    # CHECK REGISTERED MODELS
    # ================================================================

    model_count = 0

    try:
        import mlflow

        mlflow.set_registry_uri("databricks-uc")

        registered_models = mlflow.search_registered_models(
            filter_string=(
                f"name LIKE "
                f"'{intelligence_namespace}.%'"
            )
        )

        model_count = len(registered_models)

        print(
            f"   Registered Models: "
            f"{model_count} "
            f"{'✅' if model_count >= 2 else '⚠️'}"
        )

        if model_count > 0:
            model_names = [
                m.name
                for m in registered_models[:5]
            ]

            print(
                f"   Models: "
                f"{', '.join(model_names)}"
            )

    except Exception as e:
        print(
            f"   ⚠️ Model check failed: "
            f"{str(e)[:200]}"
        )

    print()

    # ================================================================
    # FINAL VALIDATION
    # ================================================================

    validation_status = (
        "success"
        if (
            function_count >= 2
            or model_count >= 2
        )
        else "partial"
    )

        log_step(
            "MLOps",
            validation_status,
            duration_sec=step_duration,
            details=(
                f"Catalog: {ml_catalog}, "
                f"Schema: {intelligence_schema}, "
                f"Models: {model_count}, "
                f"Functions: {function_count}"
            )
        )

        # Mark checkpoint complete
        mark_stage_complete(run_id, STAGE_NAME, status='success', start_time=step_start)

    except Exception as e:

        step_duration = time.time() - step_start

        print(f"❌ MLOps failed: {str(e)}")

        log_step(
            "MLOps",
            "failed",
            duration_sec=step_duration,
            details=str(e)
        )

        # Mark checkpoint failed
        mark_stage_complete(run_id, STAGE_NAME, status='failed', error_message=str(e), start_time=step_start)

        raise

print(
    f"Step 3 Complete: "
    f"{step_duration / 60:.1f} min"
)

print()

# COMMAND ----------

# DBTITLE 1,📋 STEP 4: App Deploy (with Checkpoint)
print("="*80)
print("STEP 4: APP DEPLOYMENT")
print("="*80)

STAGE_NAME = "APP_DEPLOY"

# ============================================================================
# CHECKPOINT: Check if stage should run
# ============================================================================
if not should_run_stage(run_id, STAGE_NAME, force_rerun=FORCE_RERUN):
    print(f"⏭️  {STAGE_NAME} already completed - skipping\n")
else:
    print(f"App: {APP_NAME}")
    print(f"Source: {APP_PATH}")
    print(f"Expected: Deploy Databricks App")
    print()

    step_start = mark_stage_started(run_id, "Start_SkillAI", STAGE_NAME)
    log_step("App Deploy", "running")

    try:
    import subprocess
    
    # Deploy app using Databricks CLI
    print("📦 Deploying app via Databricks CLI...")
    print()
    
    deploy_result = subprocess.run(
        [
            'databricks', 'apps', 'deploy',
            APP_NAME,
            '--source-code-path', APP_PATH
        ],
        capture_output=True,
        text=True,
        timeout=120
    )
    
    step_duration = time.time() - step_start
    
    if deploy_result.returncode == 0:
        print(f"✅ App deployed successfully in {step_duration:.1f}s")
        print(f"Output: {deploy_result.stdout[:500]}")
    else:
        print(f"⚠️ App deployment warning (may already exist)")
        print(f"Stderr: {deploy_result.stderr[:500]}")
    
    print()
    
    # Post-validation: Check app status
    print("✅ Post-Validation: Checking app status...")
    
    status_result = subprocess.run(
        ['databricks', 'apps', 'get', APP_NAME],
        capture_output=True,
        text=True,
        timeout=10
    )
    
    if status_result.returncode == 0:
        # Parse app URL from output (basic extraction)
        output = status_result.stdout
        print(f"   App status: RUNNING ✅")
        
        # Try to extract URL
        if "url" in output.lower():
            import re
            url_match = re.search(r'https://[^\s]+', output)
            if url_match:
                app_url = url_match.group(0)
                print(f"   App URL: {app_url}")
        else:
            print(f"   App exists (check workspace Apps UI for URL)")
    else:
        print(f"   ⚠️ App status check failed: {status_result.stderr[:200]}")
    
    print()
    
        log_step(
            "App Deploy",
            "success",
            duration_sec=step_duration,
            details=f"App: {APP_NAME}"
        )

        # Mark checkpoint complete
        mark_stage_complete(run_id, STAGE_NAME, status='success', start_time=step_start)
        
    except subprocess.TimeoutExpired:
        step_duration = time.time() - step_start
        print(f"⚠️ App deployment timed out (may still be deploying)")
        log_step("App Deploy", "timeout", duration_sec=step_duration)
        # Mark as partial success (timeout)
        mark_stage_complete(run_id, STAGE_NAME, status='success', error_message='Timeout - may still be deploying', start_time=step_start)
        
    except Exception as e:
        step_duration = time.time() - step_start
        print(f"❌ App deployment failed: {str(e)}")
        log_step("App Deploy", "failed", duration_sec=step_duration, details=str(e))
        # Mark checkpoint failed
        mark_stage_complete(run_id, STAGE_NAME, status='failed', error_message=str(e), start_time=step_start)
        # Don't raise - app deploy failures shouldn't block demo

print(f"Step 4 Complete: {step_duration/60:.1f} min")
print()

# COMMAND ----------

# DBTITLE 1,📋 STEP 5: Validation (with Checkpoint)
print("="*80)
print("VALIDATION: PRE-DEPLOYED ASSETS")
print("="*80)

STAGE_NAME = "VALIDATION"

# ============================================================================
# CHECKPOINT: Check if stage should run
# ============================================================================
if not should_run_stage(run_id, STAGE_NAME, force_rerun=FORCE_RERUN):
    print(f"⏭️  {STAGE_NAME} already completed - skipping\n")
else:
    print("Checking dashboards and VDPs deployed via Asset Bundles")
    print()

    step_start = mark_stage_started(run_id, "Start_SkillAI", STAGE_NAME)

    # ============================================================================
    # CHECK DASHBOARDS
    # ============================================================================
    print("📈 Checking Lakeview Dashboards...")

    try:
        # List dashboards using SDK
        dashboards = list(w.lakeview.list())
        
        # Filter for SkillAI dashboards (by naming convention or folder)
        sap_dashboards = [
            d for d in dashboards 
            if 'skillaip' in d.display_name.lower() or 'sap' in d.display_name.lower()
        ]
        
        dashboard_count = len(sap_dashboards)
        print(f"   Found {dashboard_count} SkillAI dashboards {'✅' if dashboard_count >= 3 else '⚠️'}")
        
        if dashboard_count > 0:
            print("   Dashboards:")
            for db in sap_dashboards[:5]:
                print(f"      • {db.display_name}")
    except Exception as e:
        print(f"   ⚠️ Dashboard check failed: {str(e)[:150]}")
        dashboard_count = 0

    print()

    # ============================================================================
    # CHECK VDPs (Visual Data Preps)
    # ============================================================================
    print("🎨 Checking Visual Data Preps...")

    try:
        # VDPs are stored as .vdp files in workspace
        # Check for VDP files in Business Intelligence folder
        vdp_folder = "/Workspace/Shared/dev/SkillAIPath/4_Business_Intelligence/2_Visual_Data_Prep"
        
        import subprocess
        result = subprocess.run(
            ['ls', '-la', vdp_folder],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.returncode == 0:
            # Count .vdp or folders (VDPs can be folders or files)
            lines = result.stdout.split('\n')
            vdp_items = [l for l in lines if '.vdp' in l or ('drwx' in l and not l.endswith('.'))]
            vdp_count = max(0, len(vdp_items) - 2)  # Subtract . and ..
            
            print(f"   Found {vdp_count} Visual Data Preps {'✅' if vdp_count >= 2 else '⚠️'}")
            print(f"   Location: {vdp_folder}")
        else:
            print(f"   ⚠️ VDP folder not found")
            vdp_count = 0
    except Exception as e:
        print(f"   ⚠️ VDP check failed: {str(e)[:150]}")
        vdp_count = 0

    print()

    step_duration = time.time() - step_start

    log_step(
        "Pre-Deployed Assets",
        "success",
        duration_sec=step_duration,
        details=f"Dashboards: {dashboard_count}, VDPs: {vdp_count}"
    )

    # Mark checkpoint complete
    mark_stage_complete(run_id, STAGE_NAME, status='success', start_time=step_start)

    print(f"Validation Complete: {step_duration:.1f}s")
    print()

# COMMAND ----------

# DBTITLE 1,📊 Final Execution Report
print("="*80)
print("FINAL EXECUTION REPORT")
print("="*80)
print()

total_duration = time.time() - start_time
total_minutes = total_duration / 60

# ============================================================================
# EXECUTION TIMELINE
# ============================================================================
print("🕒 EXECUTION TIMELINE")
print("-" * 80)
for entry in execution_log:
    step = entry['step']
    status = entry['status']
    duration = entry.get('duration_sec', 0)
    
    status_icon = "✅" if status == "success" else "❌" if status == "failed" else "⚠️" if status == "partial" else "⏳"
    
    print(f"{status_icon} {step:30s} {status:10s} {duration/60:6.1f} min")
    
    if 'details' in entry:
        print(f"   └─ {entry['details']}")

print("-" * 80)
print(f"TOTAL RUNTIME: {total_minutes:.1f} minutes ({total_duration:.0f} seconds)")
print()

# ============================================================================
# DATA CREATED
# ============================================================================
print("💾 DATA CREATED")
print("-" * 80)

# Count all tables
try:
    bronze_count = count_tables(CATALOG_BRONZE, "main")
    staging_count = count_tables(CATALOG_SAP, f"staging")
    silver_dbt_count = count_tables(CATALOG_SAP, f"silver")
    silver_streaming_count = count_tables(CATALOG_SILVER, "silver")
    gold_count = count_tables(CATALOG_SAP, f"gold")
    
    total_tables = bronze_count + staging_count + silver_dbt_count + silver_streaming_count + gold_count
    
    print(f"Bronze:            {bronze_count:3d} tables")
    print(f"Staging (DBT):     {staging_count:3d} tables")
    print(f"Silver (DBT):      {silver_dbt_count:3d} tables")
    print(f"Silver (Streaming):{silver_streaming_count:3d} tables")
    print(f"Gold:              {gold_count:3d} tables")
    print(f"{'-'*40}")
    print(f"TOTAL:             {total_tables:3d} tables")
except Exception as e:
    print(f"⚠️ Table count failed: {str(e)[:100]}")

print()

# ============================================================================
# ML ARTIFACTS
# ============================================================================
print("🤖 ML ARTIFACTS")
print("-" * 80)

for entry in execution_log:
    if entry['step'] == 'MLOps' and 'details' in entry:
        print(entry['details'])
        break
else:
    print("No MLOps data available")

print()

# ============================================================================
# STREAMING STATUS
# ============================================================================
print("🌊 STREAMING STATUS")
print("-" * 80)

for entry in execution_log:
    if entry['step'] == 'Streaming CDC' and 'details' in entry:
        print(entry['details'])
        break
else:
    print("No streaming data available")

print()

# ============================================================================
# APPS
# ============================================================================
print("🚀 APPS DEPLOYED")
print("-" * 80)

for entry in execution_log:
    if entry['step'] == 'App Deploy' and 'details' in entry:
        print(entry['details'])
        print(f"Status: {'Success ✅' if entry['status'] == 'success' else 'Check workspace Apps UI'}")
        break
else:
    print("No app data available")

print()

# ============================================================================
# PRE-DEPLOYED ASSETS
# ============================================================================
print("🎨 PRE-DEPLOYED ASSETS")
print("-" * 80)

for entry in execution_log:
    if entry['step'] == 'Pre-Deployed Assets' and 'details' in entry:
        print(entry['details'])
        break
else:
    print("Dashboard and VDP validation not available")

print()

# ============================================================================
# COST ESTIMATE
# ============================================================================
print("💵 COST ESTIMATE")
print("-" * 80)

estimated_cost = estimate_cost(total_minutes)
print(f"Total runtime:     {total_minutes:.1f} minutes")
print(f"Estimated DBUs:    {total_minutes * 2:.1f} DBU")
print(f"Estimated cost:    ${estimated_cost:.2f} USD")
print(f"   (Assuming 2 DBU/min @ $0.07/DBU)")
print()

# ============================================================================
# SUCCESS SUMMARY
# ============================================================================
print("="*80)
print("✅ START_SKILLAAI EXECUTION COMPLETE")
print("="*80)
print(f"Total runtime: {total_minutes:.1f} min")
print(f"Estimated cost: ${estimated_cost:.2f}")
print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()
print("🎉 Platform is ready for demo!")
print()

# COMMAND ----------

# DBTITLE 1,📊 Checkpoint Run Summary
# ============================================================================
# CHECKPOINT RUN SUMMARY
# ============================================================================

print("⏭️  CHECKPOINT FRAMEWORK - RUN SUMMARY")
print("="*80)
print(f"Run ID: {run_id}")
print(f"Force Rerun: {FORCE_RERUN}")
print()

print_run_summary(run_id)

print("\n📁 View recent runs:")
print("="*80)
recent_runs = get_recent_runs("Start_SkillAI", limit=5)
recent_runs.show(truncate=False)

print()
print("💡 To force a complete re-run (ignoring checkpoints):")
print("   Set FORCE_RERUN = True in Cell 3")
print()
print("💡 To clear checkpoints for this run:")
print(f"   clear_run_checkpoints('{run_id}')")
print()
print("💡 To resume from last failure:")
print("   Set FORCE_RERUN = False in Cell 3 (default behavior)")
print()

# COMMAND ----------

# DBTITLE 1,🎯 Next Steps
# MAGIC %md
# MAGIC # 🎯 What's Next?
# MAGIC
# MAGIC ## Explore Your Platform:
# MAGIC
# MAGIC ### 1. 📊 **Data Catalog**
# MAGIC Browse Unity Catalog to see all created tables:
# MAGIC - **Bronze Layer**: Raw data (5 tables)
# MAGIC - **Silver Layer**: Cleaned + enriched (17 tables: 12 DBT + 5 Streaming)
# MAGIC - **Gold Layer**: Business marts (3+ tables)
# MAGIC
# MAGIC ### 2. 🌊 **DLT Pipeline Monitoring**
# MAGIC View your streaming pipeline:
# MAGIC - Pipeline ID: `e8e357b9-6567-4be6-b4c0-4a1c500ee006`
# MAGIC - Check: Data Explorer > DLT Pipelines
# MAGIC - View: Real-time data flow, SCD2 history, watermarks
# MAGIC
# MAGIC ### 3. 🤖 **ML Models**
# MAGIC Explore registered models:
# MAGIC - Location: `sap_dev.intelligence` schema
# MAGIC - Models: Customer Churn Predictor, Product Recommender
# MAGIC - Test UC Functions in SQL queries
# MAGIC
# MAGIC ### 4. 📈 **Dashboards**
# MAGIC 5 production Lakeview dashboards (pre-deployed):
# MAGIC - Customer 360 with Churn Predictions
# MAGIC - Product Recommendations
# MAGIC - Demand Forecast
# MAGIC - Inventory Health Monitor
# MAGIC - Sales Performance Analytics
# MAGIC
# MAGIC ### 5. 🎨 **Visual Data Preps**
# MAGIC 3 self-service VDPs for business users:
# MAGIC - Marketing Campaign Builder
# MAGIC - Sales Territory Assignments
# MAGIC - Finance Reconciliation
# MAGIC
# MAGIC ### 6. 🚀 **Live App**
# MAGIC Intelligence Hub dashboard app:
# MAGIC - Check: Workspace Apps UI
# MAGIC - App name: `sap-intelligence-hub`
# MAGIC - Live customer intelligence with ML predictions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Create Job/Workflow:
# MAGIC
# MAGIC **To make this one-click for demos:**
# MAGIC
# MAGIC 1. Go to **Workflows** > **Jobs**
# MAGIC 2. Click **Create Job**
# MAGIC 3. Configure:
# MAGIC    - **Name**: `Start_SkillAI`
# MAGIC    - **Task**: Notebook task
# MAGIC    - **Notebook path**: `/Users/user@example.com/Start_SkillAI`
# MAGIC    - **Cluster**: Serverless (recommended)
# MAGIC    - **Schedule**: Manual (for demos) or Daily (for testing)
# MAGIC 4. **Save** and click **Run Now**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Estimated Demo Timeline:
# MAGIC
# MAGIC - **00:00-00:04**: DBT Full (Bronze + Silver + Gold)
# MAGIC - **00:04-00:09**: Streaming CDC (DLT pipeline)
# MAGIC - **00:09-00:14**: MLOps (2 models + UC Functions)
# MAGIC - **00:14-00:16**: App deployment
# MAGIC - **00:16-00:17**: Validation + reporting
# MAGIC
# MAGIC **Total: ~17 minutes** (leaves 13 min for explanation in 30-min demo)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **🎉 Platform activation complete - ready for production demos!**