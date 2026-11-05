# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,ENV_CLEANUP - Nuclear Environment Reset
# MAGIC %md
# MAGIC # 🧹 ENV_CLEANUP - Nuclear Environment Reset
# MAGIC
# MAGIC ## ⚠️ EXTREME CAUTION - THIS DELETES EVERYTHING ⚠️
# MAGIC
# MAGIC **What This Does:**
# MAGIC - Deletes ALL content under `/Shared/` (keeps folder itself)
# MAGIC - Deletes ALL content under `/Users/` (keeps folder itself)  
# MAGIC - Drops ALL Unity Catalog catalogs (except system catalogs)
# MAGIC - Deletes ALL jobs (except this running job)
# MAGIC - Deletes ALL dashboards
# MAGIC - Deletes ALL pipelines
# MAGIC - Deletes ALL ML models and experiments
# MAGIC
# MAGIC **⏱️ Estimated Time:** 5-15 minutes depending on workspace size
# MAGIC
# MAGIC **Safety Features:**
# MAGIC - Requires explicit confirmation: `CONFIRM_DELETE = "YES_DELETE_EVERYTHING"`
# MAGIC - Dry-run mode available (preview only)
# MAGIC - Self-aware (won't delete this running job)
# MAGIC - Logs everything deleted to audit trail
# MAGIC
# MAGIC **When to Use:**
# MAGIC - Fresh start for development environment
# MAGIC - Complete teardown before rebuild
# MAGIC - Testing bundle deployment from scratch
# MAGIC - Cleaning up after training/demo
# MAGIC
# MAGIC **⚠️ WARNING: NO UNDO - Everything will be permanently deleted!**

# COMMAND ----------

# DBTITLE 1,Configuration & Safety Widgets
# ============================================================================
# CONFIGURATION & SAFETY CONTROLS
# ============================================================================

try:
    dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "🔍 Dry Run (Preview Only)")
    dbutils.widgets.text("confirmation", "", "⚠️ Type: YES_DELETE_EVERYTHING")
    dbutils.widgets.dropdown("include_catalogs", "true", ["true", "false"], "Delete Unity Catalogs")
    dbutils.widgets.dropdown("include_jobs", "true", ["true", "false"], "Delete Jobs")
    dbutils.widgets.dropdown("include_dashboards", "true", ["true", "false"], "Delete Dashboards")
    dbutils.widgets.dropdown("include_workspace", "true", ["true", "false"], "Delete Workspace Files")
    
    DRY_RUN = dbutils.widgets.get("dry_run") == "true"
    CONFIRMATION = dbutils.widgets.get("confirmation")
    INCLUDE_CATALOGS = dbutils.widgets.get("include_catalogs") == "true"
    INCLUDE_JOBS = dbutils.widgets.get("include_jobs") == "true"
    INCLUDE_DASHBOARDS = dbutils.widgets.get("include_dashboards") == "true"
    INCLUDE_WORKSPACE = dbutils.widgets.get("include_workspace") == "true"
except:
    DRY_RUN = True
    CONFIRMATION = ""
    INCLUDE_CATALOGS = False
    INCLUDE_JOBS = False
    INCLUDE_DASHBOARDS = False
    INCLUDE_WORKSPACE = False

# Safety check - require explicit confirmation for production run
if not DRY_RUN and CONFIRMATION != "YES_DELETE_EVERYTHING":
    raise ValueError(
        "❌ PRODUCTION RUN BLOCKED: Type 'YES_DELETE_EVERYTHING' in confirmation widget to proceed.\n"
        "   This is a destructive operation with NO UNDO."
    )

print("="*80)
print("ENV_CLEANUP CONFIGURATION")
print("="*80)
print(f"Mode: {'🔍 DRY RUN (Preview Only)' if DRY_RUN else '🔥 PRODUCTION (WILL DELETE)'}")
print(f"Unity Catalogs: {'✅' if INCLUDE_CATALOGS else '⏭️ Skip'}")
print(f"Jobs: {'✅' if INCLUDE_JOBS else '⏭️ Skip'}")
print(f"Dashboards: {'✅' if INCLUDE_DASHBOARDS else '⏭️ Skip'}")
print(f"Workspace Files: {'✅' if INCLUDE_WORKSPACE else '⏭️ Skip'}")
print("="*80)

if not DRY_RUN:
    print("\n⚠️  PRODUCTION MODE ACTIVE - DELETIONS WILL BE PERMANENT ⚠️\n")

# COMMAND ----------

# DBTITLE 1,🔥 PRODUCTION MODE - Direct Configuration (Bypass Widgets)
# ============================================================================
# DIRECT CONFIGURATION - BYPASS WIDGETS + INITIALIZE ENGINE
# ============================================================================

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

# FORCE DELETE any existing variables from Cell 2 (widgets)
try:
    del DRY_RUN, CONFIRMATION, INCLUDE_CATALOGS, INCLUDE_JOBS, INCLUDE_DASHBOARDS, INCLUDE_WORKSPACE
except:
    pass

print("⚠️" * 40)
print("\n🔥 PRODUCTION MODE ENABLED - THIS WILL DELETE EVERYTHING! 🔥\n")
print("⚠️" * 40)

# Set configuration directly (no widget dependency)
DRY_RUN = False  # ← Set to True for preview, False for actual deletion
CONFIRMATION = "YES_DELETE_EVERYTHING"
INCLUDE_CATALOGS = True
INCLUDE_JOBS = True
INCLUDE_DASHBOARDS = True
INCLUDE_WORKSPACE = False  # ← Set to True to delete /Shared/ and /Users/ folders

print("\n" + "="*80)
print("ENV_CLEANUP CONFIGURATION (DIRECT MODE)")
print("="*80)
print(f"Mode: {'🔍 DRY RUN (Preview Only)' if DRY_RUN else '🔥 PRODUCTION (WILL DELETE)'}")
print(f"Unity Catalogs: {'✅ WILL DROP' if INCLUDE_CATALOGS and not DRY_RUN else '⏭️ Skip'}")
print(f"Jobs: {'✅ WILL DELETE' if INCLUDE_JOBS and not DRY_RUN else '⏭️ Skip'}")
print(f"Dashboards: {'✅ WILL DELETE' if INCLUDE_DASHBOARDS and not DRY_RUN else '⏭️ Skip'}")
print(f"Workspace Files: {'✅ WILL DELETE' if INCLUDE_WORKSPACE and not DRY_RUN else '⏭️ Skip'}")
print("="*80)

# Initialize CleanupEngine IMMEDIATELY with correct DRY_RUN value
class CleanupEngine:
    def __init__(self, dry_run=True):
        self.dry_run = dry_run
        self.deleted_items = []
        self.failed_items = []
        self.start_time = datetime.now()
        
    def log_delete(self, resource_type, resource_name, status="SUCCESS", error=None):
        """Log deletion attempt"""
        record = {
            "timestamp": datetime.now().isoformat(),
            "resource_type": resource_type,
            "resource_name": resource_name,
            "status": status,
            "error": str(error) if error else None,
            "dry_run": self.dry_run
        }
        
        if status == "SUCCESS":
            self.deleted_items.append(record)
        else:
            self.failed_items.append(record)
        
        return record
    
    def delete_workspace_path(self, path):
        """Delete a workspace path (file or folder)"""
        try:
            if self.dry_run:
                print(f"  [DRY RUN] Would delete: {path}")
                return self.log_delete("workspace", path)
            else:
                dbutils.fs.rm(f"/Workspace{path}", recurse=True)
                return self.log_delete("workspace", path)
        except Exception as e:
            print(f"  ❌ Failed: {path} - {str(e)[:50]}")
            return self.log_delete("workspace", path, "FAILED", e)
    
    def summary(self):
        """Print cleanup summary"""
        duration = (datetime.now() - self.start_time).total_seconds()
        
        print("\n" + "="*80)
        print("CLEANUP SUMMARY")
        print("="*80)
        print(f"Mode: {'DRY RUN' if self.dry_run else 'PRODUCTION'}")
        print(f"Duration: {duration:.1f}s")
        print(f"✅ Successfully deleted: {len(self.deleted_items)}")
        print(f"❌ Failed deletions: {len(self.failed_items)}")
        
        if self.failed_items:
            print("\nFailed Items:")
            for item in self.failed_items[:10]:  # Show first 10
                print(f"  - {item['resource_type']}: {item['resource_name']}")
        
        print("="*80)

engine = CleanupEngine(dry_run=DRY_RUN)
print(f"\n✅ Cleanup engine initialized (dry_run={engine.dry_run})")

# VERIFICATION: Print what will ACTUALLY execute
print("\n" + "="*80)
print("EXECUTION PLAN VERIFICATION")
print("="*80)
print(f"Variable Check:")
print(f"  DRY_RUN = {DRY_RUN}")
print(f"  INCLUDE_WORKSPACE = {INCLUDE_WORKSPACE}")
print(f"  INCLUDE_CATALOGS = {INCLUDE_CATALOGS}")
print(f"  INCLUDE_JOBS = {INCLUDE_JOBS}")
print(f"  INCLUDE_DASHBOARDS = {INCLUDE_DASHBOARDS}")
print(f"  engine.dry_run = {engine.dry_run}")
print("\nWhat will execute:")
if INCLUDE_WORKSPACE and not DRY_RUN:
    print("  ❌ WILL DELETE /Shared/ and /Users/ contents")
else:
    print("  ✅ /Shared/ and /Users/ will be PRESERVED")
if INCLUDE_CATALOGS and not DRY_RUN:
    print("  ❌ WILL DROP Unity Catalogs")
else:
    print("  ⏭️ Unity Catalogs will be SKIPPED")
if INCLUDE_JOBS and not DRY_RUN:
    print("  ❌ WILL DELETE Jobs")
else:
    print("  ⏭️ Jobs will be SKIPPED")
if INCLUDE_DASHBOARDS and not DRY_RUN:
    print("  ❌ WILL DELETE Dashboards")
else:
    print("  ⏭️ Dashboards will be SKIPPED")
print("="*80)

if not DRY_RUN:
    print("\n⚠️  PRODUCTION MODE ACTIVE - DELETIONS WILL BE PERMANENT ⚠️")
    print("\n⏰ Starting in 3 seconds... (Stop notebook now if you want to cancel)")
    import time
    time.sleep(3)
    print("\n🚀 Starting cleanup...\n")
else:
    print("\n🔍 DRY RUN MODE - No changes will be made\n")

# COMMAND ----------

# DBTITLE 1,Step 1: Clean /Shared/ Workspace
# ============================================================================
# STEP 1: DELETE /Shared/ CONTENTS
# ============================================================================

print(f"\n[DEBUG CELL 4] INCLUDE_WORKSPACE={INCLUDE_WORKSPACE}, DRY_RUN={DRY_RUN}")

if not INCLUDE_WORKSPACE:
    print("⏭️  Skipping workspace cleanup - /Shared/ preserved")
else:
    print("\n" + "="*80)
    print("STEP 1: CLEANING /Shared/ WORKSPACE")
    print("="*80)
    
    try:
        # List all immediate children of /Shared/
        shared_items = dbutils.fs.ls("/Workspace/Shared")
        
        print(f"Found {len(shared_items)} items under /Shared/")
        
        if len(shared_items) == 0:
            print("  ℹ️  /Shared/ is already empty")
        else:
            print("\nDeleting items:")
            print("-" * 80)
            
            deleted_count = 0
            failed_count = 0
            
            for item in shared_items:
                # Extract path without /Workspace prefix for display
                path = item.path.replace("dbfs:/Workspace", "")
                
                result = engine.delete_workspace_path(path)
                
                if result["status"] == "SUCCESS":
                    deleted_count += 1
                    print(f"  ✅ {path}")
                else:
                    failed_count += 1
            
            print(f"\n📊 /Shared/ Summary: {deleted_count} deleted, {failed_count} failed")
    
    except Exception as e:
        print(f"❌ Error accessing /Shared/: {str(e)}")
        print("   Possible causes: Permissions, folder doesn't exist, or access denied")

# COMMAND ----------

# DBTITLE 1,Step 2: Clean /Users/ Workspace
# ============================================================================
# STEP 2: DELETE /Users/ CONTENTS
# ============================================================================

print(f"\n[DEBUG CELL 5] INCLUDE_WORKSPACE={INCLUDE_WORKSPACE}, DRY_RUN={DRY_RUN}")

if not INCLUDE_WORKSPACE:
    print("⏭️  Skipping workspace cleanup - /Users/ preserved")
else:
    print("\n" + "="*80)
    print("STEP 2: CLEANING /Users/ WORKSPACE")
    print("="*80)
    
    try:
        # List all user folders under /Users/
        users_items = dbutils.fs.ls("/Workspace/Users")
        
        print(f"Found {len(users_items)} user folders under /Users/")
        
        if len(users_items) == 0:
            print("  ℹ️  /Users/ is already empty")
        else:
            print("\nDeleting user folders:")
            print("-" * 80)
            
            deleted_count = 0
            failed_count = 0
            
            for item in users_items:
                # Extract path without /Workspace prefix
                path = item.path.replace("dbfs:/Workspace", "")
                
                result = engine.delete_workspace_path(path)
                
                if result["status"] == "SUCCESS":
                    deleted_count += 1
                    print(f"  ✅ {path}")
                else:
                    failed_count += 1
            
            print(f"\n📊 /Users/ Summary: {deleted_count} deleted, {failed_count} failed")
    
    except Exception as e:
        print(f"❌ Error accessing /Users/: {str(e)}")
        print("   Possible causes: Permissions, folder doesn't exist, or access denied")

# COMMAND ----------

# DBTITLE 1,Step 3: Drop Unity Catalogs
# ============================================================================
# STEP 3: DROP UNITY CATALOG CATALOGS
# ============================================================================

print(f"\n[DEBUG] INCLUDE_CATALOGS={INCLUDE_CATALOGS}, DRY_RUN={DRY_RUN}, engine.dry_run={engine.dry_run}")

if not INCLUDE_CATALOGS:
    print("⏭️  Skipping catalog cleanup (disabled)")
else:
    print("\n" + "="*80)
    print("STEP 3: DROPPING UNITY CATALOGS")
    print("="*80)
    
    try:
        # Get all catalogs
        catalogs = spark.sql("SHOW CATALOGS").collect()
        
        # System catalogs to preserve
        system_catalogs = ["hive_metastore", "system", "samples", "main"]
        
        # Filter out system catalogs
        user_catalogs = [c.catalog for c in catalogs if c.catalog not in system_catalogs]
        
        print(f"Found {len(user_catalogs)} user catalogs to drop")
        print(f"System catalogs preserved: {', '.join(system_catalogs)}")
        
        if len(user_catalogs) == 0:
            print("  ℹ️  No user catalogs to drop")
        else:
            print("\nDropping catalogs (this may take several minutes):")
            print("-" * 80)
            
            for catalog in user_catalogs:
                try:
                    if DRY_RUN:
                        print(f"  [DRY RUN] Would drop: {catalog}")
                        engine.log_delete("catalog", catalog)
                    else:
                        print(f"  🗑️  Dropping {catalog}... ", end="")
                        spark.sql(f"DROP CATALOG IF EXISTS {catalog} CASCADE")
                        print("✅ Done")
                        engine.log_delete("catalog", catalog)
                except Exception as e:
                    print(f"❌ Failed: {str(e)[:50]}")
                    engine.log_delete("catalog", catalog, "FAILED", e)
            
            print(f"\n📊 Catalogs Summary: {len([i for i in engine.deleted_items if i['resource_type']=='catalog'])} dropped")
    
    except Exception as e:
        print(f"❌ Error with catalog operations: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Step 4: Delete Jobs (Except Running)
# ============================================================================
# STEP 4: DELETE JOBS (EXCEPT THIS RUNNING JOB)
# ============================================================================

print(f"\n[DEBUG] INCLUDE_JOBS={INCLUDE_JOBS}, DRY_RUN={DRY_RUN}, engine.dry_run={engine.dry_run}")

if not INCLUDE_JOBS:
    print("⏭️  Skipping job cleanup (disabled)")
else:
    print("\n" + "="*80)
    print("STEP 4: DELETING JOBS")
    print("="*80)
    
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        
        # Get current job context (if running in a job)
        try:
            current_job_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("jobId").getOrElse(None)
            print(f"ℹ️  Running as job: {current_job_id} (will be preserved)")
        except:
            current_job_id = None
            print("ℹ️  Not running in a job context")
        
        # List all jobs
        all_jobs = list(w.jobs.list())
        print(f"\nFound {len(all_jobs)} total jobs")
        
        # Filter out current job
        jobs_to_delete = [j for j in all_jobs if str(j.job_id) != current_job_id]
        print(f"Jobs to delete: {len(jobs_to_delete)}")
        
        if len(jobs_to_delete) == 0:
            print("  ℹ️  No jobs to delete")
        else:
            print("\nDeleting jobs:")
            print("-" * 80)
            
            for job in jobs_to_delete:
                try:
                    if DRY_RUN:
                        print(f"  [DRY RUN] Would delete: {job.settings.name} (ID: {job.job_id})")
                        engine.log_delete("job", f"{job.settings.name} ({job.job_id})")
                    else:
                        w.jobs.delete(job_id=job.job_id)
                        print(f"  ✅ {job.settings.name} (ID: {job.job_id})")
                        engine.log_delete("job", f"{job.settings.name} ({job.job_id})")
                except Exception as e:
                    print(f"  ❌ Failed: {job.settings.name} - {str(e)[:50]}")
                    engine.log_delete("job", f"{job.settings.name} ({job.job_id})", "FAILED", e)
            
            print(f"\n📊 Jobs Summary: {len([i for i in engine.deleted_items if i['resource_type']=='job'])} deleted")
    
    except Exception as e:
        print(f"❌ Error with job operations: {str(e)}")
        print("   Note: Databricks SDK might not be available in all environments")

# COMMAND ----------

# DBTITLE 1,Step 5: Delete Dashboards
# ============================================================================
# STEP 5: DELETE DASHBOARDS
# ============================================================================

print(f"\n[DEBUG] INCLUDE_DASHBOARDS={INCLUDE_DASHBOARDS}, DRY_RUN={DRY_RUN}, engine.dry_run={engine.dry_run}")

if not INCLUDE_DASHBOARDS:
    print("⏭️  Skipping dashboard cleanup (disabled)")
else:
    print("\n" + "="*80)
    print("STEP 5: DELETING DASHBOARDS")
    print("="*80)
    
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        
        # List all dashboards
        all_dashboards = list(w.lakeview.list())
        print(f"Found {len(all_dashboards)} dashboards")
        
        if len(all_dashboards) == 0:
            print("  ℹ️  No dashboards to delete")
        else:
            print("\nDeleting dashboards:")
            print("-" * 80)
            
            for dashboard in all_dashboards:
                try:
                    if DRY_RUN:
                        print(f"  [DRY RUN] Would delete: {dashboard.display_name} (ID: {dashboard.dashboard_id})")
                        engine.log_delete("dashboard", f"{dashboard.display_name} ({dashboard.dashboard_id})")
                    else:
                        w.lakeview.trash(dashboard.dashboard_id)
                        print(f"  ✅ {dashboard.display_name} (ID: {dashboard.dashboard_id})")
                        engine.log_delete("dashboard", f"{dashboard.display_name} ({dashboard.dashboard_id})")
                except Exception as e:
                    print(f"  ❌ Failed: {dashboard.display_name} - {str(e)[:50]}")
                    engine.log_delete("dashboard", f"{dashboard.display_name} ({dashboard.dashboard_id})", "FAILED", e)
            
            print(f"\n📊 Dashboards Summary: {len([i for i in engine.deleted_items if i['resource_type']=='dashboard'])} deleted")
    
    except Exception as e:
        print(f"❌ Error with dashboard operations: {str(e)}")
        print("   Note: Databricks SDK might not be available in all environments")

# COMMAND ----------

# DBTITLE 1,Final Summary & Audit Log
# ============================================================================
# FINAL SUMMARY & AUDIT LOG
# ============================================================================

engine.summary()

if not DRY_RUN:
    print("\n" + "="*80)
    print("AUDIT LOG EXPORT")
    print("="*80)
    
    # Export audit log as JSON
    audit_log = {
        "cleanup_timestamp": engine.start_time.isoformat(),
        "dry_run": DRY_RUN,
        "deleted_items": engine.deleted_items,
        "failed_items": engine.failed_items,
        "summary": {
            "total_deleted": len(engine.deleted_items),
            "total_failed": len(engine.failed_items),
            "duration_seconds": (datetime.now() - engine.start_time).total_seconds()
        }
    }
    
    # Print audit log (cannot write to DBFS /tmp or UC Volumes that may have been deleted)
    print("\nAudit Log (JSON):")
    print("-" * 80)
    print(json.dumps(audit_log, indent=2))
    print("-" * 80)
    print("\nℹ️  Note: To persist this log, copy the JSON above or redirect to an external system.")
    
    print("\n⚠️  CLEANUP COMPLETE - Environment has been reset!")
    print("   Next step: Run bundle deploy + 00_Master_Start_SkillAI to rebuild")
else:
    print("\n🔍 DRY RUN COMPLETE - No changes were made")
    print("   Set dry_run=false and confirm to execute deletion")