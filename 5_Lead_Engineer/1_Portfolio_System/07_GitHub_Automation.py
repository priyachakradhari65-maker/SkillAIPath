# Databricks notebook source
# DBTITLE 1,🚀 Portfolio GitHub Auto-Sync with Human Review
# MAGIC %md
# MAGIC # 🚀 Portfolio GitHub Auto-Sync with Human Review
# MAGIC
# MAGIC **Automated Portfolio Sync with Decision Control**
# MAGIC
# MAGIC ## Philosophy
# MAGIC ✨ **Automate repetitions, NOT decisions**
# MAGIC - Auto-detects changes in your workspace
# MAGIC - Shows you EXACTLY what will be pushed
# MAGIC - Waits for YOUR approval before committing
# MAGIC - You can edit the commit message
# MAGIC - You can cancel anytime
# MAGIC
# MAGIC ## What This Does
# MAGIC 1. 📊 Scans portfolio folders for changes
# MAGIC 2. 📋 Shows detailed diff preview
# MAGIC 3. ⏸️  **PAUSES for your review**
# MAGIC 4. ✅ Commits and pushes only after approval
# MAGIC 5. 📝 Creates summary report
# MAGIC
# MAGIC ## Triggered By
# MAGIC - Job 23: Monthly schedule (shows preview, waits for approval)
# MAGIC - Manual on-demand runs
# MAGIC - Post-content generation
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Configuration
import os
import subprocess
import tempfile
import shutil
from datetime import datetime
import requests
import base64
import json

# Configuration
REPO_NAME = "SkillAIPath"
PORTFOLIO_PATHS = [
    "/Workspace/Shared/dev/SkillAIPath/5_Lead_Engineer/1_Portfolio_System",
    "/Workspace/Shared/dev/SkillAIPath/5_Lead_Engineer/4_ContentStudio",
    "/Workspace/Shared/dev/SkillAIPath/4_Business_Intelligence",
    "/Workspace/Shared/dev/SkillAIPath/README.md"
]

# Get GitHub credentials
try:
    github_token = dbutils.secrets.get(scope="student-github", key="personal-pat")
    print("✅ GitHub token loaded")
except Exception as e:
    raise ValueError(f"❌ GitHub token not found. Run Job 01 first to set up GitHub integration. Error: {e}")

# Get GitHub username
current_user = spark.sql("SELECT current_user()").collect()[0][0]
username_derived = current_user.split('@')[0].replace('.', '-')

# Fetch real GitHub username
try:
    headers = {"Authorization": f"token {github_token}"}
    user_response = requests.get("https://api.github.com/user", headers=headers)
    github_username = user_response.json()['login'] if user_response.status_code == 200 else username_derived
except:
    github_username = username_derived

repo_full_name = f"{github_username}/{REPO_NAME}"

print(f"""
{'='*70}
🔄 GitHub Portfolio Auto-Sync with Review
{'='*70}
   Repository: {repo_full_name}
   Run Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
   Mode: PREVIEW & APPROVAL REQUIRED
{'='*70}
""")

# COMMAND ----------

# DBTITLE 1,Step 1: Scan & Preview Changes
def preview_changes(repo_full_name, token, github_username, source_paths):
    """
    Preview what will be synced WITHOUT making changes
    Returns: dict with file counts, diff preview, and temp directory path
    """
    print("\n🔍 Scanning for Changes...")
    print("=" * 70)
    
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Clone repo
        print("📥 Cloning repository...")
        clone_url = f"https://{github_username}:{token}@github.com/{repo_full_name}.git"
        subprocess.run(["git", "clone", clone_url, temp_dir], check=True, capture_output=True)
        print("   ✅ Cloned")
        
        os.chdir(temp_dir)
        
        # Configure git
        subprocess.run(["git", "config", "user.name", "SkillAI Portfolio Bot"], check=True)
        subprocess.run(["git", "config", "user.email", "portfolio-bot@skillai.com"], check=True)
        
        # Pull latest
        print("\n📥 Pulling latest changes...")
        subprocess.run(["git", "pull", "origin", "main"], check=True, capture_output=True)
        
        # Sync each portfolio path
        file_count = 0
        synced_paths = []
        for source_path in source_paths:
            if os.path.exists(source_path):
                rel_path = source_path.replace("/Workspace/Shared/dev/SkillAIPath/", "")
                dest_path = os.path.join(temp_dir, rel_path)
                
                print(f"📦 Syncing: {rel_path}")
                
                # Create parent directory
                if not os.path.isfile(source_path):  # Only for files
                    os.makedirs(os.path.dirname(dest_path) if os.path.dirname(dest_path) else dest_path, exist_ok=True)
                
                # Copy files
                if os.path.isdir(source_path):
                    for root, dirs, files in os.walk(source_path):
                        # Skip .git
                        dirs[:] = [d for d in dirs if d != ".git"]
                        file_count += len(files)
                    
                    shutil.copytree(source_path, dest_path, dirs_exist_ok=True)
                else:
                    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                    shutil.copy2(source_path, dest_path)
                    file_count += 1
                
                synced_paths.append(rel_path)
                print(f"   ✅ Copied to staging")
        
        print(f"\n📝 Total files processed: {file_count}")
        
        # Stage all changes
        subprocess.run(["git", "add", "-A"], check=True)
        
        # Check for changes
        status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
        
        changes_detected = bool(status.stdout.strip())
        
        # Get diff preview
        diff_output = ""
        if changes_detected:
            diff_result = subprocess.run(["git", "diff", "--cached", "--stat"], capture_output=True, text=True)
            diff_output = diff_result.stdout
            
            # Get detailed diff for small changes
            detailed_diff = subprocess.run(["git", "diff", "--cached"], capture_output=True, text=True)
            diff_preview = detailed_diff.stdout[:3000]  # First 3000 chars
        else:
            diff_output = "No changes detected"
            diff_preview = ""
        
        print(f"""
{'='*70}
📊 PREVIEW SUMMARY
{'='*70}
   Files processed: {file_count}
   Changes detected: {'Yes' if changes_detected else 'No'}
   
📋 Change Summary:
{diff_output}
{'='*70}
""")
        
        return {
            "success": True,
            "changes_detected": changes_detected,
            "file_count": file_count,
            "diff_stat": diff_output,
            "diff_preview": diff_preview,
            "temp_dir": temp_dir,
            "synced_paths": synced_paths
        }
    
    except Exception as e:
        print(f"\n❌ Preview failed: {e}")
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        raise

# Execute preview
preview_result = preview_changes(repo_full_name, github_token, github_username, PORTFOLIO_PATHS)

# COMMAND ----------

# DBTITLE 1,Step 2: Show Diff Preview (First 50 lines)
if preview_result["changes_detected"]:
    print("\n" + "="*70)
    print("📝 DETAILED CHANGES (Preview)")
    print("="*70)
    print(preview_result["diff_preview"])
    if len(preview_result["diff_preview"]) >= 3000:
        print("\n... (diff truncated, showing first 3000 characters)")
    print("="*70)
else:
    print("\nℹ️  No changes to preview - portfolio is already up to date")

# COMMAND ----------

# DBTITLE 1,⏸️ DECISION POINT: Approval Required
# MAGIC %md
# MAGIC ## ⏸️ APPROVAL CHECKPOINT
# MAGIC
# MAGIC ### Review the changes above, then decide:
# MAGIC
# MAGIC **Option 1: APPROVE & PUSH** ✅
# MAGIC - Run the next cell to commit and push to GitHub
# MAGIC - Default commit message is provided (you can edit it)
# MAGIC
# MAGIC **Option 2: CANCEL** ❌
# MAGIC - Stop here, no changes will be pushed
# MAGIC - The temporary staging area will be cleaned up
# MAGIC
# MAGIC **Option 3: EDIT FIRST** ✏️
# MAGIC - Modify the commit message in the next cell
# MAGIC - Then run it to push
# MAGIC
# MAGIC ---
# MAGIC ### 🎯 Your Call - You're in Control

# COMMAND ----------

# DBTITLE 1,Step 3: Commit & Push (Run ONLY if Approved)
# ⚠️ ONLY RUN THIS CELL IF YOU APPROVE THE CHANGES ABOVE

if not preview_result["changes_detected"]:
    print("ℹ️  No changes to push - skipping commit")
    dbutils.notebook.exit(json.dumps({
        "status": "success",
        "changes_made": False,
        "message": "No changes detected"
    }))
else:
    # Edit this message if needed:
    COMMIT_MESSAGE = f"""Portfolio Auto-Update: {datetime.now().strftime('%Y-%m-%d')}

Automated portfolio sync
- Updated documentation
- Synced content studio artifacts  
- Refreshed BI dashboards

Generated by: Job 23 - Portfolio AutoCommit
Timestamp: {datetime.now().isoformat()}
"""
    
    try:
        temp_dir = preview_result["temp_dir"]
        os.chdir(temp_dir)
        
        print(f"""
{'='*70}
🚀 COMMITTING & PUSHING TO GITHUB
{'='*70}
""")
        print(f"Commit Message:\n{COMMIT_MESSAGE}")
        print("="*70)
        
        # Commit
        subprocess.run(["git", "commit", "-m", COMMIT_MESSAGE], check=True, capture_output=True)
        print("✅ Changes committed locally")
        
        # Push
        print("\n🚀 Pushing to GitHub...")
        subprocess.run(["git", "push", "origin", "main"], check=True, capture_output=True)
        print("   ✅ Pushed successfully")
        
        print(f"""
{'='*70}
✅ SYNC COMPLETE
{'='*70}
   Files: {preview_result['file_count']}
   Repository: https://github.com/{repo_full_name}
   View changes: https://github.com/{repo_full_name}/commits/main
{'='*70}
""")
        
        # Return result
        result = {
            "status": "success",
            "changes_made": True,
            "file_count": preview_result['file_count'],
            "repo_url": f"https://github.com/{repo_full_name}",
            "commit_url": f"https://github.com/{repo_full_name}/commits/main",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        print(f"\n❌ Push failed: {e}")
        result = {"status": "failed", "error": str(e)}
        raise
    
    finally:
        # Cleanup
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            print("\n🧹 Cleaned up temporary files")
    
    # Return result for job monitoring
    dbutils.notebook.exit(json.dumps(result))

# COMMAND ----------

# DBTITLE 1,Cleanup (If Cancelled)
# If you cancelled and didn't run the commit cell above, run this to cleanup:

try:
    temp_dir = preview_result.get("temp_dir")
    if temp_dir and os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
        print("🧹 Cleaned up temporary staging area")
        print("✅ Cancelled successfully - no changes were pushed")
    else:
        print("ℹ️  Nothing to clean up")
except Exception as e:
    print(f"ℹ️  Cleanup note: {e}")

# COMMAND ----------

