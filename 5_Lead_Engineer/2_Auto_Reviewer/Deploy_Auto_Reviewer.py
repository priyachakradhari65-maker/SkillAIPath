# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Auto Reviewer Apps Deployment
# MAGIC %md
# MAGIC # PR Auto Reviewer - Single App Deployment
# MAGIC
# MAGIC Deploys the Growth Accelerator PR Auto Reviewer as a **single Databricks App**.
# MAGIC
# MAGIC **App Name:** `pr-review-webhook` (existing app - will be updated)
# MAGIC
# MAGIC **Source:** `/Workspace/Shared/dev/SkillAIPath/5_Lead_Engineer/2_Auto_Reviewer`
# MAGIC
# MAGIC **Entrypoint:** `app.py.OLD_MONOLITHIC_DO_NOT_USE` (the complete standalone application)
# MAGIC
# MAGIC ## Features
# MAGIC * Complete Gradio UI for code review
# MAGIC * GitHub integration (PR fetching and comment posting)
# MAGIC * AI-powered code review with RAG context
# MAGIC * Context learning and storage in Unity Catalog
# MAGIC * All-in-one deployment (no service dependencies)
# MAGIC
# MAGIC ## Deployment Flow
# MAGIC 1. Verify the existing `pr-review-webhook` app exists
# MAGIC 2. Deploy/update the app from `02_Auto_Reviewer` folder
# MAGIC 3. Wait for deployment to complete
# MAGIC 4. Wait until app reaches RUNNING state
# MAGIC 5. Print the live app URL

# COMMAND ----------

# DBTITLE 1,Import Libraries
import subprocess
import time
import json
import os

# COMMAND ----------

# DBTITLE 1,App Configuration
# Single App Configuration
app_name = "pr-review-webhook"
app_display_name = "PR Auto Reviewer"
app_source_path = "/Workspace/Shared/dev/SkillAIPath/5_Lead_Engineer/2_Auto_Reviewer"
app_description = "AI-powered PR code reviewer with GitHub integration and RAG context"

print("="*80)
print("📦 PR AUTO REVIEWER - DEPLOYMENT CONFIGURATION")
print("="*80)
print(f"App Name:      {app_name}")
print(f"Display Name:  {app_display_name}")
print(f"Source Folder: {app_source_path}")
print(f"Description:   {app_description}")
print(f"Entrypoint:    app.py.OLD_MONOLITHIC_DO_NOT_USE")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Helper Functions
def run_cli_command(command):
    """Run databricks CLI command and return output"""
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=300
        )
        return result.returncode == 0, result.stdout, result.stderr
    except Exception as e:
        return False, "", str(e)

# COMMAND ----------

# DBTITLE 1,Deploy Application
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import App, AppDeployment
import time

print("="*80)
print("🚀 DEPLOYING PR AUTO REVIEWER")
print("="*80)
print()

# Initialize SDK client
w = WorkspaceClient()

# Step 1: Check if app exists, create if missing
print(f"🔍 Checking if app '{app_name}' exists...")
app_exists = False
try:
    app = w.apps.get(name=app_name)
    print(f"✅ App exists: {app_name}")
    status = app.compute_status.state if app.compute_status else "UNKNOWN"
    print(f"   Current status: {status}")
    if app.url:
        print(f"   Current URL: {app.url}")
    app_exists = True
except Exception as e:
    if "does not exist" in str(e).lower() or "not found" in str(e).lower():
        print(f"ℹ️  App does not exist, will create it")
    else:
        print(f"⚠️  Error checking app: {str(e)}")
        print("   Will attempt to create app")

# Step 2: Create app if it doesn't exist
if not app_exists:
    print(f"\n📦 Creating app '{app_name}'...")
    try:
        w.apps.create(App(name=app_name, description=app_description)).result()
        print(f"✅ App created: {app_name}")
    except Exception as e:
        error_msg = str(e)
        if "quota" in error_msg.lower() or "maximum" in error_msg.lower():
            print("\n❌ Workspace app quota reached")
            print("\n📋 Existing apps:")
            try:
                for existing_app in w.apps.list():
                    print(f"   • {existing_app.name}")
            except:
                pass
            print("\n💡 Delete an unused app or contact your workspace admin")
        raise Exception(f"Failed to create app: {error_msg}")

# Step 3: Deploy source code
print(f"\n📦 Deploying from {app_source_path}...")
print(f"   Target app: {app_name}")
try:
    deployment = w.apps.deploy(
        app_name,
        AppDeployment(source_code_path=app_source_path)
    ).result()
    print("✅ Deployment initiated")
    print(f"   Deployment ID: {deployment.deployment_id}")
except Exception as e:
    raise Exception(f"Deployment failed: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Wait for Deployment
print("\n⏳ Waiting for deployment to complete...")
print("   This may take 2-3 minutes")
print()

max_wait = 180  # 3 minutes
start_time = time.time()

while (time.time() - start_time) < max_wait:
    try:
        app = w.apps.get(name=app_name)
        
        if app.active_deployment:
            deploy_status = app.active_deployment.deployment_state
            print(f"   Deployment status: {deploy_status}")
            
            if deploy_status in ["SUCCEEDED", "ACTIVE"]:
                print("\n✅ Deployment completed successfully")
                break
            elif deploy_status in ["FAILED", "CANCELLED"]:
                print(f"\n❌ Deployment {deploy_status}")
                raise Exception(f"Deployment {deploy_status}")
        
        time.sleep(10)
    except Exception as e:
        if "deployment" not in str(e).lower():
            print(f"   Error checking status: {str(e)}")
        time.sleep(10)
else:
    print("\n⚠️  Deployment timeout (3 minutes)")
    print("   App may still be deploying - check status manually")

# COMMAND ----------

# DBTITLE 1,Wait for App to Start
from databricks.sdk.service.apps import ComputeState

print("\n⏳ Waiting for app to reach ACTIVE state...")
print()

max_wait = 120  # 2 minutes
start_time = time.time()
app_url = None

while (time.time() - start_time) < max_wait:
    try:
        app = w.apps.get(name=app_name)
        
        if app.compute_status:
            status = app.compute_status.state
            print(f"   App status: {status}")
            
            if status == ComputeState.ACTIVE:
                app_url = app.url
                print("\n✅ App is ACTIVE (running)")
                break
            elif status in [ComputeState.ERROR, ComputeState.STOPPED]:
                print(f"\n❌ App failed to start: {status}")
                if app.compute_status.message:
                    print(f"   Error: {app.compute_status.message}")
                raise Exception(f"App failed with status: {status}")
        
        time.sleep(10)
    except Exception as e:
        if "app failed" in str(e).lower():
            raise
        time.sleep(10)
else:
    print("\n⚠️  Timeout waiting for ACTIVE state")
    print("   App may still be starting - check status manually")

# COMMAND ----------

# DBTITLE 1,Final Status Report
# Final Status Report
from databricks.sdk.service.apps import ComputeState

print("\n" + "="*40)
print("PR REVIEWER APP DEPLOYMENT")
print("="*40)
print()

try:
    app = w.apps.get(name=app_name)
    status = app.compute_status.state if app.compute_status else None
    app_url = app.url if app.url else "<URL not available yet>"
    
    print(f"App: {app_name}")
    print(f"Source: 02_Auto_Reviewer")
    print()
    
    if status == ComputeState.ACTIVE:
        print("Deployment: SUCCESS")
        print(f"Status: ACTIVE (running)")
        print(f"URL: {app_url}")
    elif status in [ComputeState.STARTING, ComputeState.UPDATING]:
        print("Deployment: IN PROGRESS")
        print(f"Status: {status.value if status else 'UNKNOWN'}")
        print(f"URL: {app_url}")
        print()
        print("The app is deployed but not fully active yet.")
        print("Wait a few more minutes and check the status.")
    else:
        print("Deployment: FAILED")
        print(f"Status: {status.value if status else 'UNKNOWN'}")
        print(f"URL: {app_url}")
        print()
        raise Exception(f"App did not reach ACTIVE state: {status}")
except Exception as e:
    print(f"Error: {str(e)}")
    raise

print()
print("="*40)
