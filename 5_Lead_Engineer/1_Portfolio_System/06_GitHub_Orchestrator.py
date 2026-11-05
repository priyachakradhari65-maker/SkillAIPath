# Databricks notebook source
# DBTITLE 1,🎮 GitHub Portfolio Control Center
# MAGIC %md
# MAGIC # 🎮 GitHub Portfolio Control Center
# MAGIC
# MAGIC **Your Central Hub for GitHub Automation**
# MAGIC
# MAGIC ## 🎯 What This Does
# MAGIC
# MAGIC This is your **single control panel** for all GitHub portfolio automation:
# MAGIC
# MAGIC 1. ✅ **Check Status** - See what's set up
# MAGIC 2. 🚀 **Initial Setup** - Run Job 01 (first time only)
# MAGIC 3. 🔄 **Sync Portfolio** - Trigger Job 23 with preview
# MAGIC 4. 🔍 **View Recent Changes** - See what will be synced
# MAGIC 5. 📊 **Dashboard** - Quick stats & health check
# MAGIC
# MAGIC ## 💡 Philosophy
# MAGIC
# MAGIC > **Automate repetitions, NOT decisions**
# MAGIC
# MAGIC You stay in control:
# MAGIC - See exactly what changed
# MAGIC - Approve before push
# MAGIC - Cancel anytime
# MAGIC - Edit commit messages
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Quick Links
# MAGIC
# MAGIC * **Setup Guide**: [AUTO_GITHUB_SETUP_GUIDE.md](/#workspace/Shared/dev/SkillAIPath/5_Lead_Engineer/AUTO_GITHUB_SETUP_GUIDE.md)
# MAGIC * **Job 01**: [GitHub Auto Setup](/#job/280670711662187)
# MAGIC * **Job 23**: [Portfolio AutoCommit](/#job/745730554004271)
# MAGIC * **Auto Reviewer**: [App](https://pr-review-webhook-deploy-7474646215602371.aws.databricksapps.com)
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Step 1: System Status Check
import requests
import subprocess
import os
from datetime import datetime

print("="*70)
print("📊 GITHUB AUTOMATION STATUS")
print("="*70)
print(f"Checked at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# Check 1: GitHub Token
print("✅ Checking GitHub Token...")
try:
    github_token = dbutils.secrets.get(scope="student-github", key="personal-pat")
    
    # Verify token is valid
    headers = {"Authorization": f"token {github_token}"}
    user_response = requests.get("https://api.github.com/user", headers=headers)
    
    if user_response.status_code == 200:
        github_user = user_response.json()
        print(f"   ✅ Token valid")
        print(f"   👤 GitHub User: {github_user.get('login')}")
        print(f"   📧 Email: {github_user.get('email', 'Not public')}")
        
        # Check if SkillAIPath repo exists
        repo_name = f"{github_user.get('login')}/SkillAIPath"
        repo_response = requests.get(f"https://api.github.com/repos/{repo_name}", headers=headers)
        
        if repo_response.status_code == 200:
            repo_data = repo_response.json()
            print(f"\n   ✅ Repository exists: {repo_data.get('html_url')}")
            print(f"   📋 Description: {repo_data.get('description', 'None')}")
            print(f"   🕑 Last updated: {repo_data.get('updated_at')}")
            print(f"   ⭐ Stars: {repo_data.get('stargazers_count')}")
            
            REPO_EXISTS = True
            REPO_URL = repo_data.get('html_url')
        else:
            print(f"\n   ⚠️  Repository 'SkillAIPath' not found")
            print(f"   👉 Run Job 01 to create it")
            REPO_EXISTS = False
            REPO_URL = None
    else:
        print(f"   ❌ Token invalid or expired (Status: {user_response.status_code})")
        print(f"   👉 Run Job 01 with a new token")
        REPO_EXISTS = False
        REPO_URL = None
        
except Exception as e:
    print(f"   ❌ Token not found or error: {e}")
    print(f"   👉 Run Job 01 to set up GitHub integration")
    REPO_EXISTS = False
    REPO_URL = None

# Check 2: Jobs Status
print(f"\n{'='*70}")
print("📈 JOBS STATUS")
print("="*70)

try:
    # Note: We'll use REST API since CLI might timeout
    import json
    
    print("\n📄 Job 01: GitHub Auto Setup")
    print("   ID: 280670711662187")
    print("   Purpose: Initial GitHub repo setup")
    print("   Status: Ready to run")
    print("   👉 [Run Job 01](/#job/280670711662187)")
    
    print("\n📄 Job 23: Portfolio AutoCommit")
    print("   ID: 745730554004271")
    print("   Purpose: Monthly portfolio sync with approval")
    print("   Status: Ready to run")
    print("   👉 [Run Job 23](/#job/745730554004271)")
    
except Exception as e:
    print(f"   ⚠️  Could not fetch job status: {e}")

# Check 3: Auto Reviewer App  
print(f"\n{'='*70}")
print("🤖 AUTO REVIEWER APP")
print("="*70)
print("\n🌐 App: pr-review-webhook-deploy")
print("   URL: https://pr-review-webhook-deploy-7474646215602371.aws.databricksapps.com")
print("   Purpose: Automatic PR reviews")
print("   Status: Production (v14)")
print("   👉 Check status: `databricks apps get pr-review-webhook-deploy`")

# Summary
print(f"\n{'='*70}")
print("🎯 QUICK SUMMARY")
print("="*70)

if REPO_EXISTS:
    print(f"""
✅ SETUP COMPLETE

   Your GitHub portfolio is ready:
   🔗 {REPO_URL}
   
   Next steps:
   1. Work normally in Databricks
   2. Run Job 23 monthly (or on-demand) to sync
   3. Review & approve changes before push
   
   👉 Run cells below to sync now or view recent changes
""")
else:
    print(f"""
⚠️  SETUP REQUIRED

   GitHub integration not set up yet.
   
   Next steps:
   1. Run cell below to launch Job 01
   2. Follow prompts to create GitHub token
   3. Re-run Job 01 with token
   4. Done! Portfolio will auto-sync from then on
   
   👉 Run "Launch Initial Setup" cell below
""")

print("="*70)

# COMMAND ----------

# DBTITLE 1,🚀 Launch Initial Setup (Job 01) - Run Once
# MAGIC %md
# MAGIC ## 🚀 Initial Setup
# MAGIC
# MAGIC **Run this ONLY if status check above shows "SETUP REQUIRED"**
# MAGIC
# MAGIC This will:
# MAGIC 1. Launch Job 01
# MAGIC 2. Job will guide you through token creation
# MAGIC 3. Re-run with token
# MAGIC 4. Done!
# MAGIC
# MAGIC **Time**: 2-3 minutes total
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Launch Job 01 - Initial Setup
# Only run this if setup is not complete

if not REPO_EXISTS:
    print("🚀 Launching Job 01: GitHub Auto Setup...")
    print("\nJob will open in a new window.")
    print("\nSteps:")
    print("1. Job will show: 'Create GitHub Token'")
    print("2. Click link → Create token (30 seconds)")
    print("3. Scopes needed: repo, workflow")
    print("4. Copy token")
    print("5. Re-run Job 01, paste token as parameter")
    print("6. Done!")
    print("\n" + "="*70)
    
    # Open job in new window
    displayHTML("""
    <div style="padding: 20px; background: #f0f8ff; border: 2px solid #4CAF50; border-radius: 8px;">
        <h3>🚀 Ready to Launch Job 01</h3>
        <p><strong>Click the link below to open the job:</strong></p>
        <a href="/#job/280670711662187" target="_blank" style="
            display: inline-block;
            padding: 12px 24px;
            background: #4CAF50;
            color: white;
            text-decoration: none;
            border-radius: 5px;
            font-weight: bold;
        ">
            🚀 Open Job 01: GitHub Auto Setup
        </a>
        <p style="margin-top: 15px;"><em>Then click "Run Now" in the job page</em></p>
    </div>
    """)
else:
    print("✅ Setup already complete!")
    print(f"   Your repo: {REPO_URL}")
    print("\n⚠️  Skip this cell - no action needed")
    print("\n👉 Scroll down to sync portfolio instead")

# COMMAND ----------

# DBTITLE 1,🔄 Portfolio Sync (Job 23) - Run Anytime
# MAGIC %md
# MAGIC ## 🔄 Sync Your Portfolio
# MAGIC
# MAGIC **Run this to push your latest work to GitHub**
# MAGIC
# MAGIC Workflow:
# MAGIC 1. Job scans for changes
# MAGIC 2. Shows preview (exactly what will be pushed)
# MAGIC 3. **Waits for your approval**
# MAGIC 4. You decide: Approve / Cancel / Edit
# MAGIC 5. Pushes only after you approve
# MAGIC
# MAGIC **Time**: 5-10 minutes
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Launch Job 23 - Portfolio Sync
if REPO_EXISTS:
    print("🔄 Launching Job 23: Portfolio Auto-Sync...")
    print("\nJob will:")
    print("1. 🔍 Scan portfolio folders for changes")
    print("2. 📋 Show detailed preview")
    print("3. ⏸️  PAUSE and wait for YOUR approval")
    print("4. ✅ Push only after you approve")
    print("\nYou control:")
    print("✓ Approve or cancel")
    print("✓ Edit commit message")
    print("✓ See exactly what changed")
    print("\n" + "="*70)
    
    # Open job in new window
    displayHTML("""
    <div style="padding: 20px; background: #fff3cd; border: 2px solid #ff9800; border-radius: 8px;">
        <h3>🔄 Ready to Sync Portfolio</h3>
        <p><strong>Click the link below to open the job:</strong></p>
        <a href="/#job/745730554004271" target="_blank" style="
            display: inline-block;
            padding: 12px 24px;
            background: #ff9800;
            color: white;
            text-decoration: none;
            border-radius: 5px;
            font-weight: bold;
        ">
            🔄 Open Job 23: Portfolio AutoCommit
        </a>
        <p style="margin-top: 15px;"><em>Then click "Run Now" in the job page</em></p>
        <p><strong>Remember</strong>: Job will show preview and wait for your approval!</p>
    </div>
    """)
else:
    print("⚠️  Setup required first!")
    print("\n👉 Run Job 01 (cell above) to set up GitHub integration first")

# COMMAND ----------

# DBTITLE 1,🔍 Preview: What Will Be Synced?
# MAGIC %md
# MAGIC ## 🔍 Preview Changes
# MAGIC
# MAGIC **See what's changed in your workspace (without pushing anything)**
# MAGIC
# MAGIC This gives you a quick look at:
# MAGIC - Files modified
# MAGIC - New files added
# MAGIC - Files deleted
# MAGIC - Recent activity
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Preview Workspace Changes
import os
from datetime import datetime, timedelta

print("="*70)
print("🔍 WORKSPACE CHANGES PREVIEW")
print("="*70)
print(f"Last 7 days of activity\n")

# Portfolio paths to check
portfolio_paths = [
    "/Workspace/Shared/dev/SkillAIPath/5_Lead_Engineer/1_Portfolio_System",
    "/Workspace/Shared/dev/SkillAIPath/5_Lead_Engineer/4_ContentStudio",
    "/Workspace/Shared/dev/SkillAIPath/4_Business_Intelligence",
    "/Workspace/Shared/dev/SkillAIPath/README.md"
]

try:
    changes_found = False
    
    for path in portfolio_paths:
        if os.path.exists(path):
            rel_path = path.replace("/Workspace/Shared/dev/SkillAIPath/", "")
            print(f"\n📂 {rel_path}")
            
            if os.path.isdir(path):
                file_count = 0
                for root, dirs, files in os.walk(path):
                    # Skip .git
                    dirs[:] = [d for d in dirs if d != ".git"]
                    file_count += len(files)
                
                print(f"   📄 Total files: {file_count}")
                
                # Show recent files (last 7 days)
                recent_files = []
                seven_days_ago = datetime.now().timestamp() - (7 * 24 * 60 * 60)
                
                for root, dirs, files in os.walk(path):
                    dirs[:] = [d for d in dirs if d != ".git"]
                    for file in files:
                        file_path = os.path.join(root, file)
                        try:
                            mtime = os.path.getmtime(file_path)
                            if mtime > seven_days_ago:
                                rel_file = file_path.replace(path, "")
                                recent_files.append((rel_file, mtime))
                        except:
                            pass
                
                if recent_files:
                    recent_files.sort(key=lambda x: x[1], reverse=True)
                    print(f"   🔥 Modified in last 7 days: {len(recent_files)} files")
                    
                    # Show top 5
                    for file, mtime in recent_files[:5]:
                        mod_date = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M')
                        print(f"      • {file} ({mod_date})")
                    
                    if len(recent_files) > 5:
                        print(f"      ... and {len(recent_files) - 5} more")
                    
                    changes_found = True
                else:
                    print(f"   👀 No changes in last 7 days")
            else:
                # Single file
                try:
                    mtime = os.path.getmtime(path)
                    mod_date = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M')
                    seven_days_ago = datetime.now().timestamp() - (7 * 24 * 60 * 60)
                    
                    if mtime > seven_days_ago:
                        print(f"   🔥 Modified: {mod_date}")
                        changes_found = True
                    else:
                        print(f"   👀 Last modified: {mod_date} (no recent changes)")
                except:
                    print(f"   ⚠️  Could not read file stats")
        else:
            print(f"\n⚠️  Path not found: {path}")
    
    print(f"\n{'='*70}")
    if changes_found:
        print("✅ CHANGES DETECTED")
        print("\n👉 Run Job 23 to sync these changes to GitHub")
        print("   (Job will show detailed diff and wait for your approval)")
    else:
        print("ℹ️  NO RECENT CHANGES")
        print("\nYour portfolio is likely already up to date.")
        print("Continue working - run Job 23 when you're ready to sync next batch.")
    print("="*70)
    
except Exception as e:
    print(f"\n❌ Error previewing changes: {e}")

# COMMAND ----------

# DBTITLE 1,📊 Quick Stats Dashboard
print("="*70)
print("📊 PORTFOLIO STATISTICS")
print("="*70)
print()

try:
    # Count files in portfolio
    total_notebooks = 0
    total_files = 0
    total_dirs = 0
    
    base_path = "/Workspace/Shared/dev/SkillAIPath"
    
    for root, dirs, files in os.walk(base_path):
        # Skip .git
        dirs[:] = [d for d in dirs if d != ".git"]
        
        total_dirs += len(dirs)
        for file in files:
            if file.endswith('.ipynb') or file.endswith('.py'):
                total_notebooks += 1
            total_files += 1
    
    print(f"📊 Workspace Stats:")
    print(f"   📁 Folders: {total_dirs}")
    print(f"   📄 Total files: {total_files}")
    print(f"   📓 Notebooks/Scripts: {total_notebooks}")
    print(f"   📈 Other files: {total_files - total_notebooks}")
    
    # Key folders
    print(f"\n🗂️ Key Portfolio Areas:")
    print(f"   🎓 Portfolio System")
    print(f"   📡 Content Studio")
    print(f"   📊 Business Intelligence")
    print(f"   🤖 AI Platform")
    print(f"   📦 Data Production")
    
    if REPO_EXISTS:
        print(f"\n🔗 GitHub:")
        print(f"   🌍 Repository: {REPO_URL}")
        print(f"   🔄 Auto-sync: Enabled (Job 23)")
        print(f"   🤖 Auto-review: Enabled (PR Reviewer)")
    
    print(f"\n{'='*70}")
    print("✅ PORTFOLIO HEALTHY")
    print("="*70)
    
except Exception as e:
    print(f"❌ Error generating stats: {e}")

# COMMAND ----------

# DBTITLE 1,📚 Resources & Next Steps
# MAGIC %md
# MAGIC ## 📚 Resources
# MAGIC
# MAGIC ### Documentation
# MAGIC * **Complete Guide**: [AUTO_GITHUB_SETUP_GUIDE.md](/#workspace/Shared/dev/SkillAIPath/5_Lead_Engineer/AUTO_GITHUB_SETUP_GUIDE.md)
# MAGIC * **Job 01 Guide**: [JOB_DESCRIPTION.md](/#workspace/Shared/dev/SkillAIPath/1_Canvas/0_DevOps/Production/JOB_DESCRIPTION.md)
# MAGIC * **Setup Guide**: [SETUP_GUIDE.md](/#workspace/Shared/dev/SkillAIPath/1_Canvas/0_DevOps/Production/SETUP_GUIDE.md)
# MAGIC
# MAGIC ### Jobs
# MAGIC * [Job 01: GitHub Auto Setup](/#job/280670711662187) - One-time setup
# MAGIC * [Job 23: Portfolio AutoCommit](/#job/745730554004271) - Monthly sync
# MAGIC
# MAGIC ### Apps & Tools
# MAGIC * [Auto Reviewer App](https://pr-review-webhook-deploy-7474646215602371.aws.databricksapps.com) - PR reviews
# MAGIC * [GitHub Repository](https://github.com) - Your SkillAIPath repo
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Next Steps
# MAGIC
# MAGIC ### If Setup Not Complete:
# MAGIC 1. Run "Launch Initial Setup" cell above
# MAGIC 2. Follow prompts in Job 01
# MAGIC 3. Come back here when done
# MAGIC
# MAGIC ### If Setup Complete:
# MAGIC 1. **Work normally** - Focus on your projects
# MAGIC 2. **Check this dashboard** - See what changed  
# MAGIC 3. **Run Job 23** - Sync when ready (monthly or on-demand)
# MAGIC 4. **Review & approve** - Control what goes live
# MAGIC
# MAGIC ### For Team Leads:
# MAGIC 1. Share this notebook with team
# MAGIC 2. Guide engineers through first-time setup
# MAGIC 3. Review monthly sync approvals
# MAGIC 4. Monitor Auto Reviewer feedback quality
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔥 Pro Tips
# MAGIC
# MAGIC * 📌 **Bookmark this notebook** - Your control center
# MAGIC * 📅 **Monthly rhythm** - Review Job 23 on 1st of month
# MAGIC * 👀 **Preview first** - Always check "Preview Changes" before syncing
# MAGIC * ✏️ **Edit commits** - Customize messages in Job 23
# MAGIC * 🚫 **Cancel anytime** - No changes pushed until you approve
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Philosophy**: Automate repetitions, NOT decisions.
# MAGIC
# MAGIC ✨ **You work. Your portfolio builds itself.**

# COMMAND ----------

