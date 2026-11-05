# Databricks notebook source
# DBTITLE 1,Silver CDC Pipeline Setup
# MAGIC %md
# MAGIC # 🚀 Silver DLT Pipeline - Automated Setup
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC
# MAGIC This notebook **programmatically creates** the Lakeflow Spark Declarative Pipeline (DLT) for Silver layer processing.
# MAGIC
# MAGIC **Features:**
# MAGIC - ✅ Environment-aware (auto-detects dev/uat/prod)
# MAGIC - ✅ No hardcoded values - uses Config
# MAGIC - ✅ Idempotent - safe to re-run
# MAGIC - ✅ Creates pipeline with correct catalog/schema settings
# MAGIC - ✅ No CLI or DABs required - just run this notebook!
# MAGIC
# MAGIC **What You Get:**
# MAGIC - Pipeline Name: `Silver DLT Pipeline (dev/uat/prod)`
# MAGIC - Source: Bronze tables (stg_sales, stg_purchase, stg_stock_status)
# MAGIC - Target: Silver schema with SCD Type 2 tracking
# MAGIC - Mode: Triggered streaming (manual or scheduled)
# MAGIC
# MAGIC **Instructions:**
# MAGIC 1. Run all cells in order
# MAGIC 2. Pipeline will be created automatically
# MAGIC 3. Click the generated URL to view/run the pipeline

# COMMAND ----------

# DBTITLE 1,Install and Import Databricks SDK
# Check if Databricks SDK is already installed
try:
    from databricks.sdk import WorkspaceClient
    print("✓ Databricks SDK already installed")
except ImportError:
    print("○ Databricks SDK not found - installing...")
    %pip install --quiet databricks-sdk
    dbutils.library.restartPython()
    print("✓ Installed - notebook will restart. Please re-run remaining cells.")

# COMMAND ----------

# DBTITLE 1,Load Configuration
# MAGIC %run /Shared/dev/SkillAIPath/1_Canvas/1_Setup/1_Config

# COMMAND ----------

# DBTITLE 1,Initialize Workspace Client
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary

# Initialize workspace client
w = WorkspaceClient()

print("✓ Databricks SDK initialized")
print(f"✓ Connected to workspace: {w.config.host}")

# COMMAND ----------

# DBTITLE 1,Check if Pipeline Already Exists
# Pipeline configuration - fully portable paths (uses Config.WORKSPACE_ROOT)
pipeline_name = f"Bronze-Silver DLT Pipeline ({Config.ENVIRONMENT})"
# Bronze layer: Auto Loader from streaming test volume
dlt_bronze_notebook_path = f"{Config.WORKSPACE_ROOT}/2_Data_Production/02_Medallion_Architecture/03_Streaming_Cdc/12_enterprise_dlt_bronze_pipeline"
# Silver layer: CDC/SCD Type 2 from bronze tables
dlt_silver_notebook_path = f"{Config.WORKSPACE_ROOT}/2_Data_Production/02_Medallion_Architecture/03_Streaming_Cdc/13_enterprise_dlt_silver_cdc_pipeline"

print(f"Checking for existing pipeline: {pipeline_name}")
print("="*60)

# List all pipelines and check if ours exists
existing_pipeline = None
for pipeline in w.pipelines.list_pipelines():
    if pipeline.name == pipeline_name:
        existing_pipeline = pipeline
        print(f"✓ Found existing pipeline: {pipeline.pipeline_id}")
        break

if not existing_pipeline:
    print("○ Pipeline does not exist yet - will create new one")
else:
    print(f"○ Pipeline exists - will update configuration")

# COMMAND ----------

# DBTITLE 1,Create or Update Pipeline
# Pipeline configuration settings
pipeline_config = {
    "name": pipeline_name,
    "catalog": Config.CATALOG,
    "target": Config.SCHEMA_SILVER,
    "channel": "CURRENT",
    "serverless": True,
    "continuous": False,
    "development": True,
    "libraries": [
        PipelineLibrary(
            notebook=NotebookLibrary(path=dlt_bronze_notebook_path)
        ),
        PipelineLibrary(
            notebook=NotebookLibrary(path=dlt_silver_notebook_path)
        )
    ],
    "configuration": {
        "pipelines.applyChangesPreviewEnabled": "true"
    }
}

print("\nPipeline Configuration:")
print("="*60)
for key, value in pipeline_config.items():
    if key != "libraries":
        print(f"  {key}: {value}")
print(f"  bronze_notebook: {dlt_bronze_notebook_path}")
print(f"  silver_notebook: {dlt_silver_notebook_path}")
print("="*60)

if existing_pipeline:
    # Update existing pipeline
    print("\n🔄 Pipeline already exists - updating configuration...")
    updated = w.pipelines.update(
        pipeline_id=existing_pipeline.pipeline_id,
        **pipeline_config
    )
    pipeline_id = existing_pipeline.pipeline_id
    print(f"✓ Pipeline updated successfully!")
else:
    # Try to create new pipeline
    try:
        print("\n🆕 Creating new pipeline...")
        created = w.pipelines.create(**pipeline_config)
        pipeline_id = created.pipeline_id
        print(f"✓ Pipeline created successfully!")
    except Exception as e:
        if "already used" in str(e) or "ResourceConflict" in str(type(e).__name__):
            # Pipeline exists but wasn't detected in Cell 6 - find it now
            print("\n⚠️  Pipeline already exists (created in a previous run)")
            print("🔍 Finding existing pipeline...")
            for pipeline in w.pipelines.list_pipelines():
                if pipeline.name == pipeline_name:
                    pipeline_id = pipeline.pipeline_id
                    print(f"✓ Found pipeline: {pipeline_id}")
                    print("💡 Tip: Re-run Cell 6 before Cell 7 to update the pipeline")
                    break
        else:
            raise

print(f"\nPipeline ID: {pipeline_id}")

# COMMAND ----------

# DBTITLE 1,Verify and Print Pipeline URL
# Get pipeline details
pipeline = w.pipelines.get(pipeline_id=pipeline_id)

print("\n" + "="*60)
print("✅ PIPELINE SETUP COMPLETE")
print("="*60)
print(f"Name: {pipeline.name}")
print(f"Pipeline ID: {pipeline.pipeline_id}")
print(f"Catalog: {pipeline.spec.catalog}")
print(f"Target Schema: {pipeline.spec.target}")
print(f"State: {pipeline.state}")
print("="*60)

# Display clickable link that opens in the same tab
workspace_url = w.config.host
pipeline_url = f"{workspace_url}/pipelines/{pipeline_id}"

print(f"\n🔗 Pipeline URL (for reference):")
print(pipeline_url)

# Create HTML link that opens in same tab (not a new tab)
displayHTML(f"""
<div style="padding: 20px; background-color: #f0f7ff; border: 2px solid #0066cc; border-radius: 8px; margin: 10px 0;">
    <h3 style="margin-top: 0; color: #0066cc;">🚀 Open Pipeline</h3>
    <p style="margin: 10px 0;">Click the button below to open the pipeline (opens in same tab):</p>
    <a href="{pipeline_url}" target="_self" style="
        display: inline-block;
        padding: 12px 24px;
        background-color: #0066cc;
        color: white;
        text-decoration: none;
        border-radius: 4px;
        font-weight: bold;
        font-size: 14px;
    ">📊 Open Silver DLT Pipeline</a>
    <p style="margin: 15px 0 5px 0; font-size: 12px; color: #666;">
        Or copy URL: <code style="background: #fff; padding: 2px 6px; border-radius: 3px;">{pipeline_url}</code>
    </p>
</div>
""")

# COMMAND ----------

# DBTITLE 1,Ensure Checkpoint Volume Exists
# Ensure required UC volumes exist for DLT pipeline checkpoints
try:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {Config.CATALOG}.{Config.SCHEMA_GOLD}.checkpoints")
    print(f"✓ Checkpoint volume ensured: {Config.CATALOG}.{Config.SCHEMA_GOLD}.checkpoints")
except Exception as e:
    print(f"⚠️  Could not create checkpoint volume: {str(e)[:100]}")

# COMMAND ----------

# DBTITLE 1,Next Steps
# MAGIC %md
# MAGIC  
# MAGIC ## ✅ Next Steps
# MAGIC
# MAGIC ### 1. Open the Pipeline
# MAGIC Click the URL above to open the pipeline in the Databricks UI
# MAGIC
# MAGIC ### 2. Review Configuration
# MAGIC - Check the pipeline settings
# MAGIC - Verify source and target tables
# MAGIC - Review the notebook library
# MAGIC
# MAGIC ### 3. Run the Pipeline
# MAGIC **Option A: Manual Run**
# MAGIC - Click "Start" in the pipeline UI
# MAGIC - Monitor the progress in real-time
# MAGIC
# MAGIC **Option B: Schedule it**
# MAGIC - Go to the pipeline settings
# MAGIC - Add a schedule (e.g., daily at midnight)
# MAGIC - Enable notifications
# MAGIC
# MAGIC ### 4. Monitor Results
# MAGIC After the pipeline runs:
# MAGIC - Check Silver schema tables: `sap_dev.silver.*`
# MAGIC - Verify SCD Type 2 history in `silver_customers`
# MAGIC - Review data quality metrics in the UI
# MAGIC
# MAGIC ### 5. Troubleshooting
# MAGIC If the pipeline fails:
# MAGIC - Check the event logs in the pipeline UI
# MAGIC - Verify bronze source tables exist and have data
# MAGIC - Review notebook cell errors
# MAGIC - Re-run this setup notebook to update configuration
