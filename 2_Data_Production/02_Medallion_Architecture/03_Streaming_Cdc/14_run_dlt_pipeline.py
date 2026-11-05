# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,DLT Pipeline Runner
# MAGIC %md
# MAGIC # 🚀 Run DLT Pipeline
# MAGIC
# MAGIC Simple notebook that triggers the Bronze-Silver DLT Pipeline.
# MAGIC
# MAGIC **What it does:**
# MAGIC - Finds pipeline by name: `Bronze-Silver DLT Pipeline ({environment})`
# MAGIC - Triggers an update
# MAGIC - Waits for completion
# MAGIC - Reports success/failure
# MAGIC
# MAGIC **Usage:**
# MAGIC - Called from Jobs 06 & 22 as a task
# MAGIC - Receives `environment` parameter
# MAGIC - Idempotent - safe to run multiple times

# COMMAND ----------

# DBTITLE 1,Get Parameters
import re

def notebook_path() -> str:
    """Return this notebook's workspace path."""
    # 1) Spark conf (available in job runs)
    try:
        return spark.conf.get("spark.databricks.notebook.path")
    except Exception:
        pass
    # 2) Notebook context (interactive runs)
    try:
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    except Exception:
        return ""

def detect_environment_from_path() -> str:
    """Detect environment from this notebook's workspace path: /Shared/<environment>/SkillAIPath/..."""
    match = re.match(r"^/Shared/([^/]+)/SkillAIPath/", notebook_path())
    return match.group(1) if match else "main"

# Default comes from path detection; an explicitly passed job parameter still takes precedence
dbutils.widgets.text("environment", detect_environment_from_path())
environment = dbutils.widgets.get("environment")
VALID_ENVS = {"dev", "staging", "main"}
if environment not in VALID_ENVS:
    raise ValueError(f"Invalid environment '{environment}'. Must be one of: {VALID_ENVS}")
print(f"Environment: {environment} (from path: {notebook_path()})")


# COMMAND ----------

# DBTITLE 1,Find Pipeline
from databricks.sdk import WorkspaceClient
import time

# Initialize client
w = WorkspaceClient()

# Find pipeline by name
pipeline_name = f"Bronze-Silver DLT Pipeline ({environment})"
print(f"Looking for pipeline: {pipeline_name}")

pipeline_id = None
for pipeline in w.pipelines.list_pipelines():
    if pipeline.name == pipeline_name:
        pipeline_id = pipeline.pipeline_id
        print(f"✓ Found pipeline: {pipeline_id}")
        break

if not pipeline_id:
    raise Exception(f"Pipeline not found: {pipeline_name}. Run 11_setup_silver_dlt_pipeline first.")

# COMMAND ----------

# DBTITLE 1,Trigger Pipeline
# Trigger pipeline update
print(f"Starting pipeline update...")

# An active update cannot be started twice (ResourceConflict) - reuse it if one is running
ACTIVE_STATES = {"CREATED", "WAITING_FOR_RESOURCES", "INITIALIZING", "RESTARTING", "RUNNING", "STOPPING", "SETTING_UP_TABLES"}
update_id = None
for u in w.pipelines.get(pipeline_id=pipeline_id).latest_updates or []:
    if u.state and u.state.value in ACTIVE_STATES:
        update_id = u.update_id
        print(f"ℹ Update already in progress, reusing: {update_id}")
        break

if not update_id:
    update = w.pipelines.start_update(pipeline_id=pipeline_id)
    update_id = update.update_id
    print(f"✓ Update started: {update_id}")

# COMMAND ----------

# DBTITLE 1,Wait for Completion
# Wait for completion
print("Waiting for pipeline to complete...")
max_wait = 1800  # 30 minutes
start_time = time.time()

def get_update_errors(pid, uid):
    """Fetch the errors recorded against an update so failures are actionable."""
    try:
        ev = w.api_client.do("GET", f"/api/2.0/pipelines/{pid}/events?max_results=100&order=DESC")
        msgs = []
        for e in ev.get("events", []):
            if (e.get("origin") or {}).get("update_id") != uid and e.get("update_id") != uid:
                continue
            for ex in (e.get("error") or {}).get("exceptions", []):
                msgs.append(f"{ex.get('class_name', '')}: {ex.get('message', '')}")
        return msgs
    except Exception:
        return []

while True:
    update_info = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id)
    state = update_info.update.state.value if update_info.update.state else "UNKNOWN"
    
    if state in ["COMPLETED", "FAILED", "CANCELED"]:
        print(f"\nPipeline finished: {state}")
        if state == "COMPLETED":
            print("✅ SUCCESS")
        else:
            error_msgs = get_update_errors(pipeline_id, update_id)
            for m in error_msgs[:3]:
                print(f"❌ {m}")
            detail = " | ".join(m[:500] for m in error_msgs[:3]) or "Check pipeline logs for details"
            raise Exception(f"Pipeline {state}: {detail}")
        break
    
    elapsed = time.time() - start_time
    if elapsed > max_wait:
        raise Exception(f"Pipeline timeout after {max_wait}s")
    
    print(f"Status: {state} (elapsed: {int(elapsed)}s)")
    time.sleep(30)

# COMMAND ----------

