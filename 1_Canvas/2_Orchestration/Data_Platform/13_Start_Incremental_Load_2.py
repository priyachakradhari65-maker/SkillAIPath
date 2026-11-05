# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Incremental Load Pipeline Job Definition
# MAGIC %md
# MAGIC # Incremental Load Pipeline - Databricks Job Definition
# MAGIC
# MAGIC ## Overview
# MAGIC Creates a Databricks Job for the **Incremental Load Pipeline** with all tasks, dependencies, and parallel execution.
# MAGIC
# MAGIC ## Pipeline Structure
# MAGIC
# MAGIC ### Stage 1: Data Generation (Incremental)
# MAGIC - **Task:** Generate incremental data for today
# MAGIC - **Mode:** incremental
# MAGIC - **Tables:** stg_sales_incremental, stg_purchase_incremental, customer_changes_raw
# MAGIC
# MAGIC ### Stage 2: Dimensions (9 tasks - parallel)
# MAGIC - dim_date, dim_product_category, dim_warehouse, dim_retailer
# MAGIC - dim_agent, dim_payment_mode, dim_product, dim_wholesaler
# MAGIC - **dim_customer (CDC SCD2)** - processes customer changes
# MAGIC
# MAGIC ### Stage 3: Facts (3 tasks - parallel)
# MAGIC - **fact_sales (CDC)** - MERGE operations
# MAGIC - **fact_purchases** - APPEND
# MAGIC - **fact_inventory** - APPEND
# MAGIC - Depends on: ALL dimensions
# MAGIC
# MAGIC ### Stage 4: Validation Framework
# MAGIC - Incremental test suite
# MAGIC - Validates CDC operations, SCD2 integrity, watermarks
# MAGIC - Depends on: ALL facts
# MAGIC
# MAGIC ### Stage 5: Gold Layer (4 tasks - parallel)
# MAGIC - mart_customer_360, mart_rfm_analysis
# MAGIC - mart_product_performance, mart_inventory_health
# MAGIC - Depends on: Validation
# MAGIC
# MAGIC ### Stage 6: Data Quality Monitoring
# MAGIC - Comprehensive quality tests (incremental mode)
# MAGIC - Depends on: ALL gold marts
# MAGIC
# MAGIC ### Stage 7: Archive & Cleanup
# MAGIC - Archives incremental staging → history
# MAGIC - Truncates incremental staging tables
# MAGIC - Depends on: Quality Monitoring
# MAGIC
# MAGIC ## Execution
# MAGIC Run all cells to:
# MAGIC 1. Create/update "Incremental Load Pipeline" job
# MAGIC 2. Trigger the pipeline execution
# MAGIC 3. Get monitoring link
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Install Databricks SDK
# MAGIC %pip install --quiet databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Initialize Databricks SDK
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, compute
import os

# Initialize Databricks SDK
w = WorkspaceClient()

# Detect environment - check dev and main only
environment = None
for env in ["dev", "main"]:
    try:
        test_path = f"/Workspace/Shared/{env}/SkillAIPath"
        dbutils.fs.ls(test_path)
        environment = env
        print(f"✅ Detected {env.upper()} environment (found {test_path})")
        break
    except:
        continue

if not environment:
    environment = "dev"  # Default to dev if none found
    print(f"⚠️  Defaulting to DEV environment (no paths detected)")

print(f"Environment detected: {environment}")
print(f"Workspace: {w.config.host}")
print(f"User: {w.current_user.me().user_name}")

# COMMAND ----------

# DBTITLE 1,Define Job Configuration
# Base path for all notebooks
base_path = f"/Workspace/Shared/{environment}/SkillAIPath/2_Data_Platform_Foundation/02_Medallion_Architecture"

print("Defining Incremental Load Pipeline job configuration...\n")
print("⚠️  VERIFYING: All notebooks configured for INCREMENTAL mode\n")

# =============================================================================
# STAGE 1: DATA GENERATOR (INCREMENTAL MODE)
# =============================================================================
task_data_generator = jobs.Task(
    task_key="data_generator_incremental",
    description="Generate today's incremental data (sales, purchases, customer changes)",
    notebook_task=jobs.NotebookTask(
        notebook_path=f"/Workspace/Shared/{environment}/SkillAIPath/1_Canvas/1_Setup/2_data_generator",
        base_parameters={"mode": "incremental"}  # ✅ INCREMENTAL MODE
    ),
    timeout_seconds=600,
    max_retries=2
)

print("✅ Stage 1: Data Generator - mode='incremental'")

# =============================================================================
# STAGE 2: DIMENSIONS (9 tasks - parallel)
# =============================================================================
dimension_notebooks = [
    ("dim_date", f"{base_path}/01_Dimensions/01_dim_date"),
    ("dim_product_category", f"{base_path}/01_Dimensions/02_dim_product_category"),
    ("dim_warehouse", f"{base_path}/01_Dimensions/03_dim_warehouse_nb"),
    ("dim_retailer", f"{base_path}/01_Dimensions/04_dim_retailer"),
    ("dim_agent", f"{base_path}/01_Dimensions/05_dim_agent"),
    ("dim_payment_mode", f"{base_path}/01_Dimensions/06_dim_payment_mode"),
    ("dim_product", f"{base_path}/01_Dimensions/07_dim_product_nb"),
    ("dim_wholesaler", f"{base_path}/01_Dimensions/13_dim_wholesaler"),
]

dimension_tasks = []
for dim_name, dim_path in dimension_notebooks:
    task = jobs.Task(
        task_key=dim_name,
        description=f"Load {dim_name} dimension (incremental MERGE)",
        notebook_task=jobs.NotebookTask(
            notebook_path=dim_path,
            base_parameters={}  # Dimensions auto-detect incremental via staging tables
        ),
        depends_on=[jobs.TaskDependency(task_key="data_generator_incremental")],
        timeout_seconds=600,
        max_retries=2
    )
    dimension_tasks.append(task)

# Customer Dimension (CDC SCD Type 2)
task_dim_customer_cdc = jobs.Task(
    task_key="dim_customer_cdc",
    description="Load dim_customer using CDC SCD Type 2 pattern",
    notebook_task=jobs.NotebookTask(
        notebook_path=f"{base_path}/03_Streaming_Cdc/06_customer_scd2_cdc",  # ✅ CDC notebook
        base_parameters={}
    ),
    depends_on=[jobs.TaskDependency(task_key="data_generator_incremental")],
    timeout_seconds=900,
    max_retries=2
)
dimension_tasks.append(task_dim_customer_cdc)

print("✅ Stage 2: 9 Dimensions - incremental MERGE + Customer CDC SCD2")

# =============================================================================
# STAGE 3: FACT TABLES (3 tasks - parallel, depend on ALL dimensions)
# =============================================================================
all_dim_dependencies = [jobs.TaskDependency(task_key=task.task_key) for task in dimension_tasks]

# Fact Sales (CDC) - CRITICAL: Using CDC notebook
task_fact_sales_cdc = jobs.Task(
    task_key="fact_sales_cdc",
    description="Load fact_sales using CDC MERGE pattern",
    notebook_task=jobs.NotebookTask(
        notebook_path=f"{base_path}/03_Streaming_Cdc/07_sales_fact_cdc",  # ✅ CDC MERGE notebook
        base_parameters={}
    ),
    depends_on=all_dim_dependencies,
    timeout_seconds=1200,
    max_retries=2
)

# Fact Purchases - Incremental APPEND
task_fact_purchases = jobs.Task(
    task_key="fact_purchases",
    description="Load fact_purchases (incremental APPEND)",
    notebook_task=jobs.NotebookTask(
        notebook_path=f"{base_path}/02_Facts/02_fact_purchases",
        base_parameters={}
    ),
    depends_on=all_dim_dependencies,
    timeout_seconds=1200,
    max_retries=2
)

# Fact Inventory - Incremental APPEND
task_fact_inventory = jobs.Task(
    task_key="fact_inventory",
    description="Load fact_inventory (incremental APPEND)",
    notebook_task=jobs.NotebookTask(
        notebook_path=f"{base_path}/02_Facts/03_fact_inventory",
        base_parameters={}
    ),
    depends_on=all_dim_dependencies,
    timeout_seconds=1200,
    max_retries=2
)

fact_tasks = [task_fact_sales_cdc, task_fact_purchases, task_fact_inventory]

print("✅ Stage 3: 3 Facts - Sales CDC MERGE + Purchases/Inventory APPEND")

# =============================================================================
# STAGE 4: VALIDATION FRAMEWORK (depends on ALL facts)
# =============================================================================
all_fact_dependencies = [jobs.TaskDependency(task_key=task.task_key) for task in fact_tasks]

task_validation = jobs.Task(
    task_key="incremental_validation",
    description="Incremental validation test suite (CDC, SCD2, watermarks)",
    notebook_task=jobs.NotebookTask(
        notebook_path=f"/Workspace/Shared/{environment}/SkillAIPath/2_Data_Platform_Foundation/04_Quality_Operations/2_Validation_Framework/3_incremental_test_suite",  # ✅ Incremental test suite
        base_parameters={}
    ),
    depends_on=all_fact_dependencies,
    timeout_seconds=600,
    max_retries=2
)

print("✅ Stage 4: Validation - 3_incremental_test_suite (not full load tests)")

# =============================================================================
# STAGE 5: GOLD LAYER (4 tasks - parallel, depend on validation)
# =============================================================================
validation_dependency = [jobs.TaskDependency(task_key="incremental_validation")]

gold_notebooks = [
    ("mart_customer_360", f"{base_path}/04_Gold/01_mart_customer_360"),
    ("mart_rfm_analysis", f"{base_path}/04_Gold/02_mart_rfm_analysis"),
    ("mart_product_performance", f"{base_path}/04_Gold/03_mart_product_performance"),
    ("mart_inventory_health", f"{base_path}/04_Gold/04_mart_inventory_health"),
]

gold_tasks = []
for gold_name, gold_path in gold_notebooks:
    task = jobs.Task(
        task_key=gold_name,
        description=f"Incremental refresh of {gold_name} business mart",
        notebook_task=jobs.NotebookTask(
            notebook_path=gold_path,
            base_parameters={}  # Gold marts auto-detect incremental from Silver
        ),
        depends_on=validation_dependency,
        timeout_seconds=900,
        max_retries=2
    )
    gold_tasks.append(task)

print("✅ Stage 5: 4 Gold Marts - incremental refresh")

# =============================================================================
# STAGE 6: DATA QUALITY MONITORING (depends on ALL gold marts)
# =============================================================================
all_gold_dependencies = [jobs.TaskDependency(task_key=task.task_key) for task in gold_tasks]

task_dq_monitoring = jobs.Task(
    task_key="data_quality_monitoring",
    description="Comprehensive data quality validation (incremental mode)",
    notebook_task=jobs.NotebookTask(
        notebook_path=f"/Workspace/Shared/{environment}/SkillAIPath/2_Data_Platform_Foundation/04_Quality_Operations/2_Validation_Framework/2_monitoring_quality_tests",
        base_parameters={"load_type": "incremental"}  # ✅ INCREMENTAL MODE
    ),
    depends_on=all_gold_dependencies,
    timeout_seconds=900,
    max_retries=2
)

print("✅ Stage 6: Quality Monitoring - load_type='incremental' (not 'full')")

# =============================================================================
# STAGE 7: ARCHIVE & CLEANUP (depends on quality monitoring)
# =============================================================================
task_archive_cleanup = jobs.Task(
    task_key="archive_cleanup",
    description="Archive incremental staging to history and cleanup",
    notebook_task=jobs.NotebookTask(
        notebook_path=f"/Workspace/Shared/{environment}/SkillAIPath/1_Canvas/1_Setup/5_archive_and_cleanup",
        base_parameters={
            "environment": environment,
            "retention_days": "0",
            "validate_downstream": "true",
            "dry_run": "false"
        }
    ),
    depends_on=[jobs.TaskDependency(task_key="data_quality_monitoring")],
    timeout_seconds=600,
    max_retries=2
)

print("✅ Stage 7: Archive & Cleanup - staging → history")

print("\n" + "="*80)
print("🎯 INCREMENTAL MODE VERIFICATION SUMMARY:")
print("="*80)
print(f"✅ Stage 1: Data Generator - mode='incremental'")
print(f"✅ Stage 2: Customer Dimension - CDC SCD2 (06_customer_scd2_cdc)")
print(f"✅ Stage 3: Sales Fact - CDC MERGE (07_sales_fact_cdc)")
print(f"✅ Stage 4: Validation - 3_incremental_test_suite (NOT full)")
print(f"✅ Stage 6: Quality Monitoring - load_type='incremental' (NOT 'full')")
print(f"✅ Stage 7: Archive & Cleanup - removes staging after processing")
print("\n🚫 NO FULL LOAD COMPONENTS INCLUDED")
print("="*80)

print(f"\n📊 Job configuration defined:")
print(f"   - Stage 1: Data Generator (1 task)")
print(f"   - Stage 2: Dimensions ({len(dimension_tasks)} tasks - parallel)")
print(f"   - Stage 3: Facts ({len(fact_tasks)} tasks - parallel)")
print(f"   - Stage 4: Validation (1 task)")
print(f"   - Stage 5: Gold Layer ({len(gold_tasks)} tasks - parallel)")
print(f"   - Stage 6: Quality Monitoring (1 task)")
print(f"   - Stage 7: Archive & Cleanup (1 task)")
print(f"   Total: {1 + len(dimension_tasks) + len(fact_tasks) + 1 + len(gold_tasks) + 1 + 1} tasks")

# COMMAND ----------

# DBTITLE 1,Create or Update Job
# Combine all tasks
all_tasks = [task_data_generator] + dimension_tasks + fact_tasks + [task_validation] + gold_tasks + [task_dq_monitoring, task_archive_cleanup]

# Email for notifications (update this to your email)
email_notifications = jobs.JobEmailNotifications(
    on_failure=["your-email@example.com"],
    on_success=["your-email@example.com"]
)

print("\n🔨 Creating or updating Incremental Load Pipeline job...\n")

try:
    # Check if job already exists
    existing_jobs = list(w.jobs.list(name="Incremental Load Pipeline", limit=1))
    
    if existing_jobs:
        # Job exists - delete and recreate (simpler than update)
        job_id = existing_jobs[0].job_id
        print(f"⚠️  Job already exists (ID: {job_id}). Deleting...")
        w.jobs.delete(job_id=job_id)
        print("✅ Old job deleted.\n")
    
    # Create the job with serverless compute
    created_job = w.jobs.create(
        name="Incremental Load Pipeline",
        tasks=all_tasks,
        email_notifications=email_notifications,
        max_concurrent_runs=1,
        timeout_seconds=0,  # No timeout at job level (tasks have individual timeouts)
        description=f"""Incremental Load Data Pipeline for sap_{environment}
        
Automated end-to-end incremental data pipeline with parallel execution on Serverless Compute:
- Stage 1: Data Generator (incremental mode)
- Stage 2: Silver Layer - 9 Dimensions (parallel) including Customer CDC SCD2
- Stage 3: Silver Layer - 3 Facts (parallel) with CDC MERGE operations
- Stage 4: Validation Framework (incremental test suite)
- Stage 5: Gold Layer - 4 Business Marts (parallel)
- Stage 6: Data Quality Monitoring (incremental mode)
- Stage 7: Archive & Cleanup (staging → history)

All tasks are configured for INCREMENTAL LOAD mode with CDC support.
Running on Databricks Serverless Compute for optimal performance and cost.

Created by: IncrementalLoad_Pipeline_Job notebook
        """
    )
    
    job_id = created_job.job_id
    
    print("✅ SUCCESS! Job created successfully.\n")
    print("="*80)
    print(f"Job ID: {job_id}")
    print(f"Job Name: Incremental Load Pipeline")
    print(f"Environment: {environment}")
    print(f"Total Tasks: {len(all_tasks)}")
    print("="*80)
    
except Exception as e:
    print(f"\n❌ ERROR: Failed to create job\n")
    print(str(e))
    raise

# COMMAND ----------

# DBTITLE 1,Trigger Run & Display Monitoring
# Trigger the pipeline run
print("\n🚀 Starting Incremental Load Pipeline execution...\n")

try:
    # Get the job we just created
    jobs_list = w.jobs.list(name="Incremental Load Pipeline", limit=1)
    job_id = str(list(jobs_list)[0].job_id)
    
    # Trigger the run
    run_response = w.jobs.run_now(job_id=int(job_id))
    run_id = str(run_response.run_id)
    
    # Get workspace URL
    context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    workspace_url = context.apiUrl().get()
    
    # Construct URLs
    job_run_url = f"{workspace_url}/#job/{job_id}/run/{run_id}"
    job_url = f"{workspace_url}/#job/{job_id}"
    
    print("\n" + "="*80)
    print("🎉 INCREMENTAL LOAD PIPELINE RUNNING!")
    print("="*80)
    print(f"\n📊 Job Details:")
    print(f"   Job Name: Incremental Load Pipeline")
    print(f"   Job ID: {job_id}")
    print(f"   Run ID: {run_id}")
    print(f"   Environment: {environment}")
    print(f"   Catalog: sap_{environment}")
    print(f"\n🚀 Monitor Running Pipeline:")
    print(f"   {job_run_url}")
    print(f"\n⚙️  Pipeline Structure:")
    print(f"   ├─ Stage 1: Data Generator (incremental mode)")
    print(f"   ├─ Stage 2: Dimensions - 9 tasks (parallel) + Customer CDC")
    print(f"   ├─ Stage 3: Facts - 3 tasks (parallel) with CDC MERGE")
    print(f"   ├─ Stage 4: Validation Framework (incremental tests)")
    print(f"   ├─ Stage 5: Gold - 4 business marts (parallel)")
    print(f"   ├─ Stage 6: Quality Monitoring (incremental mode)")
    print(f"   └─ Stage 7: Archive & Cleanup")
    print(f"\n💡 What's Happening:")
    print(f"   ✅ Job created/updated successfully")
    print(f"   ✅ Pipeline run triggered (ONCE)")
    print(f"   🔄 All {len(all_tasks)} tasks executing in sequential layers")
    print(f"   📊 Click the 'Running Pipeline' URL above to monitor progress")
    print(f"\n⚠️  Important Notes:")
    print(f"   • All tasks configured for INCREMENTAL mode")
    print(f"   • Data generator creates today's incremental data")
    print(f"   • CDC SCD2 for Customer dimension")
    print(f"   • CDC MERGE for Sales fact")
    print(f"   • Each task has retry logic (max 2 retries)")
    print("="*80)
    
    # Display clickable HTML link
    displayHTML(f"""
    <div style="padding: 20px; background: linear-gradient(135deg, #3b82f6 0%, #1d4ed8 100%); 
                border-radius: 10px; margin: 20px 0; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
        <h2 style="color: white; margin: 0 0 15px 0; font-size: 24px;">
            ✅ Incremental Load Pipeline Running!
        </h2>
        <div style="background: rgba(255,255,255,0.2); padding: 15px; border-radius: 5px; margin-bottom: 15px;">
            <p style="color: white; margin: 5px 0; font-size: 16px;">
                <strong>Job ID:</strong> {job_id}
            </p>
            <p style="color: white; margin: 5px 0; font-size: 16px;">
                <strong>Run ID:</strong> {run_id}
            </p>
            <p style="color: white; margin: 5px 0; font-size: 16px;">
                <strong>Status:</strong> <span style="color: #90EE90;">● Running</span>
            </p>
            <p style="color: white; margin: 5px 0; font-size: 14px; margin-top: 10px;">
                {len(all_tasks)} tasks executing in layers:<br>
                Data Gen → 9 Dims (CDC) → 3 Facts (CDC) → Validation → 4 Gold → Quality → Cleanup
            </p>
        </div>
        <a href="{job_run_url}" target="_blank" 
           style="display: inline-block; background: white; color: #1d4ed8; padding: 12px 30px; 
                  border-radius: 5px; text-decoration: none; font-weight: bold; font-size: 16px;
                  box-shadow: 0 2px 4px rgba(0,0,0,0.2); transition: transform 0.2s;"
           onmouseover="this.style.transform='scale(1.05)'" 
           onmouseout="this.style.transform='scale(1)'">
            📊 Monitor Pipeline Execution
        </a>
    </div>
    """)
    
except Exception as e:
    print(f"\n❌ ERROR: Failed to trigger pipeline run\n")
    print(str(e))
    raise

# COMMAND ----------

