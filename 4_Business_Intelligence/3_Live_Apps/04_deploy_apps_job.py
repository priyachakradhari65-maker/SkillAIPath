# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Business Intelligence Apps Deployment Job
# MAGIC
# MAGIC Automated deployment of all 3 Business Intelligence Live Apps using Databricks SDK

# COMMAND ----------

import time
import json
import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import (
    App, AppDeployment, AppDeploymentMode,
    ComputeState, AppDeploymentState
)

# Get WAREHOUSE_ID from job parameter (via dbutils widget) or environment variable
# If not provided, apps will auto-detect the first available warehouse
try:
    WAREHOUSE_ID = dbutils.widgets.get("WAREHOUSE_ID")
except:
    WAREHOUSE_ID = os.getenv("WAREHOUSE_ID", "")

# Create WorkspaceClient - auto-authenticates in Databricks notebook runtime
w = WorkspaceClient()

print(f"🔧 Warehouse Configuration: {WAREHOUSE_ID if WAREHOUSE_ID else 'Auto-detect'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## App Configuration

# COMMAND ----------

# DBTITLE 1,App Configuration
apps_config = [
    {
        "name": "bi-center",
        "display_name": "Business Intelligence Center",
        "source_path": "/Workspace/Shared/dev/SkillAIPath/4_Business_Intelligence/3_Live_Apps/01_BI_Center",
        "description": "Bidirectional data collaboration - read AI data, write business overrides, submit feedback"
    },
    {
        "name": "team-intelligence-hub",
        "display_name": "Team Intelligence Hub",
        "source_path": "/Workspace/Shared/dev/SkillAIPath/4_Business_Intelligence/3_Live_Apps/02_Team_Intelligence_Hub",
        "description": "Live execution platform - code executor, AI chat, job monitor, team activity"
    },
    {
        "name": "skillai-execution-support",
        "display_name": "SkillAI Execution Support Agent",
        "source_path": "/Workspace/Shared/dev/SkillAIPath/4_Business_Intelligence/3_Live_Apps/03_SkillAI_Execution_Support",
        "description": "Autonomous AI assistant with persistent memory, user context, and RAG knowledge base"
    }
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy Apps Using Databricks SDK

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, as_completed

# Results tracking
deployment_results = []

def state_str(state):
    """Convert state enum or string to string for comparison."""
    if state is None:
        return "UNKNOWN"
    if hasattr(state, 'value'):
        return state.value
    return str(state)

def get_deployment_state(app):
    """Get the deployment state from pending or active deployment."""
    if app.pending_deployment and app.pending_deployment.status:
        return state_str(app.pending_deployment.status.state), app.pending_deployment.status.message or ""
    if app.active_deployment and app.active_deployment.status:
        return state_str(app.active_deployment.status.state), app.active_deployment.status.message or ""
    return "UNKNOWN", ""

print("=" * 80)
print("🚀 DEPLOYING BUSINESS INTELLIGENCE APPS")
print("=" * 80)

def deploy_app(app_config):
    app_name = app_config["name"]
    display_name = app_config["display_name"]
    source_path = app_config["source_path"]

    result = {
        "name": app_name,
        "display_name": display_name,
        "status": "FAILED",
        "message": "",
        "url": ""
    }

    try:
        print(f"\n{'='*60}")
        print(f"📦 Processing: {display_name}")
        print(f"   Name: {app_name}")
        print(f"   Source: {source_path}")

        # Step 1: Check if app exists
        app_exists = False
        try:
            app = w.apps.get(name=app_name)
            app_exists = True
            current_state = state_str(app.compute_status.state) if app.compute_status else "UNKNOWN"
            print(f"   ✅ App exists (Status: {current_state})")
        except Exception as e:
            err_str = str(e)
            err_lower = err_str.lower()
            if any(kw in err_lower for kw in ["notfound", "404", "resource_not_found", "does not exist", "is deleted"]):
                app_exists = False
                print(f"   ➕ App does not exist, will create new")
            else:
                raise

        if not app_exists:
            # Step 2: Create new app
            print(f"   ➕ Creating new app...")
            w.apps.create(app=App(
                name=app_name,
                description=app_config["description"]
            ))
            print(f"   ✅ App created")
            time.sleep(2)

        # Step 3: Ensure app is RUNNING (deploy requires RUNNING state)
        app = w.apps.get(name=app_name)
        current_state = state_str(app.compute_status.state) if app.compute_status else "UNKNOWN"

        if current_state != "ACTIVE":
            if current_state != "STARTING":
                print(f"   ▶️  Starting app (current state: {current_state})...")
                w.apps.start(name=app_name)
            else:
                print(f"   ⏳ App is already STARTING, waiting for ACTIVE...")
            for i in range(60):  # 300 seconds max
                time.sleep(2)
                app = w.apps.get(name=app_name)
                state = state_str(app.compute_status.state) if app.compute_status else "UNKNOWN"
                if state == "ACTIVE":
                    print(f"   ✅ App is running")
                    break
                elif state in ("ERROR", "FAILED"):
                    msg = app.compute_status.message if app.compute_status else ""
                    print(f"   ❌ Could not start app (State: {state})")
                    if msg:
                        print(f"      {msg}")
                    result["message"] = f"Could not start app: {state} - {msg}"
                    break
            else:
                print(f"   ❌ Startup timed out (300s)")
                result["message"] = "Startup timed out"
                return result
        else:
            print(f"   ✅ App already running")

        # Step 3b: Wait for any existing deployment to complete
        app = w.apps.get(name=app_name)
        dep_state, _ = get_deployment_state(app)
        if dep_state not in ("SUCCEEDED", "FAILED", "UNKNOWN"):
            print(f"   ⏳ Waiting for existing deployment ({dep_state})...")
            for i in range(36):
                time.sleep(2)
                app = w.apps.get(name=app_name)
                dep_state, _ = get_deployment_state(app)
                if dep_state in ("SUCCEEDED", "FAILED", "UNKNOWN"):
                    print(f"   ✅ Existing deployment finished ({dep_state})")
                    break
            else:
                print(f"   ⚠️ Existing deployment still in progress, deploying anyway")

        # Step 4: Deploy source code (app must be RUNNING)
        print(f"   🔄 Deploying source code (SNAPSHOT mode)...")
        w.apps.deploy(
            app_name=app_name,
            app_deployment=AppDeployment(
                source_code_path=source_path,
                mode=AppDeploymentMode.SNAPSHOT
            )
        )
        print(f"   ✅ Deploy command sent")

        # Step 5: Wait for deployment to complete (max 180 seconds, poll every 5s)
        print(f"   ⏳ Waiting for deployment...")
        deploy_success = False
        for i in range(36):
            time.sleep(2)
            app = w.apps.get(name=app_name)
            dep_state, dep_msg = get_deployment_state(app)
            if dep_state == "SUCCEEDED":
                print(f"   ✅ Deployment succeeded")
                deploy_success = True
                break
            elif dep_state == "FAILED":
                print(f"   ❌ Deployment failed: {dep_msg}")
                result["message"] = f"Deployment failed: {dep_msg}"
                break
        else:
            print(f"   ❌ Deployment timed out (180s)")
            result["message"] = "Deployment timed out"

        if not deploy_success:
            return result

        # Step 6: Verify app is ACTIVE (skip wait if already active)
        app = w.apps.get(name=app_name)
        state = state_str(app.compute_status.state) if app.compute_status else "UNKNOWN"

        if state == "ACTIVE":
            url = app.url or "N/A"
            print(f"   ✅ App is LIVE!")
            print(f"   🌐 URL: {url}")
            result["status"] = "SUCCESS"
            result["message"] = "Deployed and running"
            result["url"] = url
        else:
            if state != "STARTING":
                print(f"   ▶️  Starting app (current state: {state})...")
                w.apps.start(name=app_name)
            print(f"   ⏳ Waiting for app to be ACTIVE...")
            for i in range(150):  # 300 seconds max (poll every 2s)
                time.sleep(2)
                app = w.apps.get(name=app_name)
                state = state_str(app.compute_status.state) if app.compute_status else "UNKNOWN"
                if state == "ACTIVE":
                    url = app.url or "N/A"
                    print(f"   ✅ App is LIVE!")
                    print(f"   🌐 URL: {url}")
                    result["status"] = "SUCCESS"
                    result["message"] = "Deployed and running"
                    result["url"] = url
                    break
                elif state in ("ERROR", "FAILED"):
                    msg = app.compute_status.message if app.compute_status else ""
                    print(f"   ❌ Startup failed (State: {state})")
                    if msg:
                        print(f"      {msg}")
                    result["message"] = f"Startup failed: {state} - {msg}"
                    break
            else:
                print(f"   ❌ App did not become active (300s)")
                result["message"] = "App did not become active"

    except Exception as e:
        print(f"   ❌ Error: {str(e)}")
        result["message"] = f"Exception: {str(e)}"

    return result

# Deploy all apps in parallel
with ThreadPoolExecutor(max_workers=len(apps_config)) as executor:
    futures = {executor.submit(deploy_app, cfg): cfg for cfg in apps_config}
    for future in as_completed(futures):
        try:
            result = future.result()
        except Exception as e:
            cfg = futures[future]
            result = {
                "name": cfg["name"],
                "display_name": cfg["display_name"],
                "status": "FAILED",
                "message": f"Exception: {str(e)}",
                "url": ""
            }
        deployment_results.append(result)

print(f"\n{'='*80}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Status

# COMMAND ----------

print("=" * 80)
print("📊 FINAL STATUS")
print("=" * 80)

success_count = 0
failure_count = 0

for r in deployment_results:
    icon = "🟢" if r["status"] == "SUCCESS" else "🔴"
    print(f"{icon} {r['display_name']}")
    print(f"   Name: {r['name']}")
    print(f"   Status: {r['status']}")
    if r["url"]:
        print(f"   URL: {r['url']}")
    if r["message"]:
        print(f"   Message: {r['message']}")
    print()

    if r["status"] == "SUCCESS":
        success_count += 1
    else:
        failure_count += 1

print("=" * 80)
print(f"Total: {success_count} succeeded, {failure_count} failed")
print("=" * 80)

if failure_count > 0:
    print("❌ DEPLOYMENT FAILED")
    raise Exception(f"Deployment failed for {failure_count} app(s)")
else:
    print("✅ ALL APPS DEPLOYED SUCCESSFULLY")