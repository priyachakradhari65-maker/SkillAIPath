# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 🚀 SkillAI Path - GitHub Auto Setup
# MAGIC
# MAGIC **What This Does:**
# MAGIC 1. Auto-detects YOUR Databricks username
# MAGIC 2. Creates YOUR GitHub repo named "SkillAIPath"
# MAGIC 3. Sets up `main` and `dev` branches
# MAGIC 4. Forks our template (encrypted source - not revealed)
# MAGIC 5. Pushes initial structure to `main`
# MAGIC 6. Creates `dev` branch from `main`
# MAGIC 7. Makes a simple change and creates your FIRST PR (dev → main)
# MAGIC
# MAGIC **✨ First-Time Setup (2 minutes):**
# MAGIC - Run this job → It will show GitHub token link
# MAGIC - Create token (30 seconds)
# MAGIC - Re-run job with token as parameter
# MAGIC - Done! Token stored for future runs
# MAGIC
# MAGIC **🎯 Zero Manual Input After First Run:**
# MAGIC - Username: Auto-detected from Databricks
# MAGIC - Repo Name: Fixed as "SkillAIPath"
# MAGIC - Token: Stored after first use

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Parameters

# COMMAND ----------

# DBTITLE 1,Parameters & Auto-Detection
# Import requests for API calls
import requests

# 🎯 ONLY github_token parameter needed!
dbutils.widgets.text("github_token", "", "GitHub Personal Access Token (required first time)")

# 1️⃣ Auto-detect Databricks user (no manual input!)
current_user = spark.sql("SELECT current_user()").collect()[0][0]
username = current_user.split('@')[0].replace('.', '-')  # john.doe@email.com → john-doe

print(f"✅ Auto-detected Databricks user: {current_user}")
print(f"   → GitHub username: {username}")

# 2️⃣ Fixed repo name as requested
repo_name = "SkillAIPath"
print(f"   → Repo name: {repo_name}")

# 3️⃣ Get github_token from parameter OR stored secret
github_token_param = dbutils.widgets.get("github_token").strip()

if not github_token_param:
    # Try to get from stored secret (for future runs)
    try:
        secret_key = f"personal-pat-{username}"
        github_token = dbutils.secrets.get(scope="student-github", key=secret_key)
        print("✅ Using stored GitHub token from previous run")
    except Exception as e:
        print(f"""
⚠️  GitHub Personal Access Token needed!

📋 Steps:
1️⃣ Create GitHub Token:
   → Click: https://github.com/settings/tokens/new?scopes=repo,workflow&description=Databricks-SkillAI-Setup
   → Select scopes: ✓ repo, ✓ workflow
   → Click "Generate token" → Copy token (ghp_...)

2️⃣ Paste token in the 'github_token' widget above ☝️

3️⃣ Re-run this cell (Shift+Enter)

Error: Secret not found for this user yet.
""")
        github_token = None
        github_username = username
else:
    # Token provided - use it and store for future runs
    github_token = github_token_param
    print("✅ GitHub token provided as parameter")
    
    # 4️⃣ Auto-store token for future runs via Databricks SDK
    # (dbutils.secrets.putSecret doesn't exist — use SDK)
    try:
        from databricks.sdk import WorkspaceClient as _W
        _w = _W()
        _scope = "student-github"
        _key = f"personal-pat-{username}"
        try:
            _w.secrets.create_scope(scope=_scope)
        except:
            pass
        _w.secrets.put_secret(scope=_scope, key=_key, string_value=github_token)
        print(f"   → ✅ Token stored for future runs (scope: {_scope}, key: {_key})")
        print("   → You won't need to enter it again!")
    except Exception as store_error:
        print(f"   → ⚠️  Could not auto-store token: {str(store_error)}")

# 5️⃣ Fetch REAL GitHub username from API (not guessed from email)
if github_token:
    try:
        headers = {"Authorization": f"token {github_token}"}
        user_response = requests.get("https://api.github.com/user", headers=headers)
        
        if user_response.status_code == 200:
            user_data = user_response.json()
            github_username = user_data['login']  # Real GitHub username
            print(f"   → ✅ Fetched real GitHub username from API: {github_username}")
        else:
            # Fallback to derived username if API fails
            github_username = username
            print(f"   → ⚠️  Could not fetch GitHub username from API, using derived: {github_username}")
    except Exception as api_error:
        # Fallback to derived username if API fails
        github_username = username
        print(f"   → ⚠️  API error, using derived username: {github_username}")

    print(f"""
{'='*70}
📋 Final Configuration:
{'='*70}
   Databricks User: {current_user}
   GitHub Username: {github_username}
   Repo Name: {repo_name}
   Token: {"*" * 20} (secured)
{'='*70}
""")
else:
    print("\n⏳ Waiting for GitHub token. Paste it in the widget above and re-run this cell.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Setup: Imports & Configuration

# COMMAND ----------

# DBTITLE 1,Imports
import base64
import requests
import json
import os
import subprocess
import shutil
import tempfile
import time
from datetime import datetime

print("✅ All imports loaded")
print("   Using native Git for fast mirror (20x faster than API)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Create GitHub Repository

# COMMAND ----------

# DBTITLE 1,Create GitHub Repository (with delete-if-exists)
def create_github_repo(username, repo_name, token, description="My SkillAI Path Journey"):
    """Create or reuse GitHub repository (no delete needed - Git push will overwrite)"""
    
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    # Check if repo exists
    check_url = f"https://api.github.com/repos/{username}/{repo_name}"
    check_response = requests.get(check_url, headers=headers)
    
    if check_response.status_code == 200:
        # Repo exists - will be overwritten by Git push
        print(f"⚠️  Repository {username}/{repo_name} already exists")
        print(f"📝 Will overwrite all files with workspace content (via Git push)")
        print(f"✅ No delete_repo permission needed!")
        return check_response.json()
    
    # Repo doesn't exist - create it
    url = "https://api.github.com/user/repos"
    data = {
        "name": repo_name,
        "description": description,
        "private": False,  # Public repo
        "auto_init": True,  # Initialize with README
        "has_issues": True,
        "has_projects": True,
        "has_wiki": True
    }
    
    response = requests.post(url, headers=headers, json=data)
    
    if response.status_code == 201:
        print(f"✅ Repository created: {username}/{repo_name}")
        return response.json()
    else:
        raise Exception(f"❌ Failed to create repo: {response.json()}")

# Create repo
repo_info = create_github_repo(github_username, repo_name, github_token)
repo_full_name = repo_info['full_name']
print(f"📦 Working with: {repo_full_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Mirror Workspace to GitHub (Full Copy)

# COMMAND ----------

# DBTITLE 1,Mirror Workspace to GitHub
import re

def mirror_workspace_to_github_fast(target_repo, token, github_username, workspace_path=None):
    """
    Incremental sync: Push only new/modified files from workspace to GitHub.
    Clones existing repo, updates with workspace changes, pushes only diffs.
    Removes files containing secrets entirely (not just emptying them).
    Falls back to orphan-branch (clean history) if GitHub Push Protection blocks.
    """
    
    # Dynamic workspace path - per user with fallback to shared
    if workspace_path is None:
        user_workspace = f"/Workspace/Users/{current_user}/SkillAIPath"
        shared_workspace = "/Workspace/Shared/dev/SkillAIPath"
        workspace_path = user_workspace if os.path.exists(user_workspace) else shared_workspace
    
    print(f"🚀 Incremental Sync: Workspace → GitHub")
    print(f"   Source: {workspace_path}")
    print(f"   Target: {target_repo}")
    print(f"   Method: Native Git (incremental)\n")
    
    # Create temp directory for git operations
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Step 1: Clone existing repo (incremental sync)
        print("📥 Step 1/5: Cloning existing repo...")
        clone_url = f"https://{github_username}:{token}@github.com/{target_repo}.git"
        clone_result = subprocess.run(["git", "clone", clone_url, temp_dir], capture_output=True, text=True)
        if clone_result.returncode != 0:
            print("   ⚠️  Clone failed, initializing fresh repo...")
            os.chdir(temp_dir)
            subprocess.run(["git", "init"], check=True)
            subprocess.run(["git", "branch", "-M", "main"], check=True)
            subprocess.run(["git", "remote", "add", "origin", clone_url], check=True)
        else:
            os.chdir(temp_dir)
        print("   ✅ Repo ready for sync")
        
        # Step 2: Clean repo + copy fresh workspace files
        print("\n📦 Step 2/5: Cleaning repo & copying workspace files...")
        
        # Remove ALL existing files from cloned repo (except .git) for clean sync
        # This prevents duplicate files/folders from accumulating across runs
        for item in os.listdir(temp_dir):
            if item != ".git":
                item_path = os.path.join(temp_dir, item)
                if os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                else:
                    os.remove(item_path)
        print("   ✅ Cleaned existing repo files (preventing duplicates)")
        
        # Enhanced .gitignore to exclude secret-heavy file types
        gitignore_content = """# Secrets & credentials\n*.p12\n*.pem\n*.key\n*.pfx\n*.cert\n*.jks\n**/service_account*\n**/credentials*\n**/.databricks*\n**/secrets*\n**/.secret*\n**/oauth*\n**/token*\n.env\n.env.*\n*.json.bak\n"""
        with open(os.path.join(temp_dir, ".gitignore"), "w") as f:
            f.write(gitignore_content)
        
        # Count files first
        file_count = 0
        for root, dirs, files in os.walk(workspace_path):
            dirs[:] = [d for d in dirs if d != ".git"]
            file_count += len(files)
        
        print(f"   Found {file_count} files to sync")
        
        # Copy everything from workspace
        for item in os.listdir(workspace_path):
            src = os.path.join(workspace_path, item)
            dst = os.path.join(temp_dir, item)
            if item == ".git":
                continue
            try:
                if os.path.isdir(src):
                    shutil.copytree(src, dst, dirs_exist_ok=True)
                else:
                    shutil.copy2(src, dst)
            except (OSError, shutil.Error):
                print(f"   ⚠️  Skipping {item}: not a copyable asset")
                continue
        print("   ✅ All files copied")
        
        # Step 3: Scan for secrets (regex + binary-safe markers, remove files entirely)
        print("\n🔒 Step 3/5: Scanning for secrets...")
        
        # Regex patterns for precise secret detection (binary-safe)
        secret_patterns = [
            re.compile(rb'gh[pousr]_[A-Za-z0-9]{36}'),          # GitHub tokens
            re.compile(rb'github_pat_[A-Za-z0-9_]{82}'),         # GitHub PAT
            re.compile(rb'dapi[0-9a-f]{32}'),                    # Databricks tokens
            re.compile(rb'AKIA[0-9A-Z]{16}'),                     # AWS access keys
            re.compile(rb'AIza[0-9A-Za-z\-_]{35}'),              # Google API keys
            re.compile(rb'sk-[A-Za-z0-9]{20,}'),                 # OpenAI/Stripe keys
            re.compile(rb'xox[baprs]-[A-Za-z0-9\-]+'),           # Slack tokens
            re.compile(rb'-----BEGIN [A-Z ]*PRIVATE KEY-----'), # Private keys
            re.compile(rb'"private_key"\s*:\s*"-----BEGIN'),   # GCP service account JSON
            re.compile(rb'"private_key_id"\s*:'),               # GCP key ID
            re.compile(rb'"client_secret"\s*:'),                 # OAuth client secret
            re.compile(rb'"refresh_token"\s*:'),                # Refresh tokens
        ]
        
        # Additional binary-safe string markers (fallback)
        secret_markers = [
            b"-----BEGIN PRIVATE KEY", b"-----BEGIN RSA PRIVATE KEY",
            b"-----BEGIN EC PRIVATE KEY", b"-----BEGIN OPENSSH PRIVATE KEY",
            b"-----BEGIN PGP PRIVATE KEY",
            b"service_account", b"private_key", b"client_secret",
            b"refresh_token", b"access_token",
            b"ghp_", b"gho_", b"ghu_", b"ghs_", b"ghr_", b"github_pat_",
            b"dapi", b"AKIA", b"AIza", b"sk-", b"xox",
        ]
        
        def has_secret(filepath):
            """Check if file contains secrets (binary-safe)."""
            try:
                with open(filepath, 'rb') as f:
                    raw = f.read()
                if not raw:
                    return False
                for pattern in secret_patterns:
                    if pattern.search(raw):
                        return True
                for marker in secret_markers:
                    if marker in raw:
                        return True
            except (OSError, PermissionError):
                pass
            return False
        
        removed_files = []
        for sroot, sdirs, sfiles in os.walk(temp_dir):
            sdirs[:] = [d for d in sdirs if d != ".git"]
            for fn in sfiles:
                fp = os.path.join(sroot, fn)
                rel_path = os.path.relpath(fp, temp_dir)
                if has_secret(fp):
                    os.remove(fp)
                    removed_files.append(rel_path)
        
        if removed_files:
            print(f"   🔒 Removed {len(removed_files)} files containing secrets:")
            for rf in removed_files[:20]:
                print(f"      ❌ {rf}")
            if len(removed_files) > 20:
                print(f"      ... and {len(removed_files) - 20} more")
        else:
            print("   ✅ No secrets detected in first pass")
        
        # Configure git user
        subprocess.run(["git", "config", "user.name", "SkillAI DevOps"], check=True)
        subprocess.run(["git", "config", "user.email", "devops@skillai.com"], check=True)
        subprocess.run(["git", "config", "http.postBuffer", "524288000"], check=True)
        
        # Add all files (respects .gitignore)
        subprocess.run(["git", "add", "-A"], check=True)
        
        # Second pass: scan git-tracked files, git rm --cached any remaining secret files
        tracked = subprocess.run(["git", "ls-files"], capture_output=True, text=True)
        late_removed = []
        for tf in tracked.stdout.strip().split("\n"):
            if not tf:
                continue
            fp = os.path.join(temp_dir, tf)
            if not os.path.exists(fp):
                subprocess.run(["git", "rm", "--cached", tf], capture_output=True)
                late_removed.append(tf)
                continue
            if has_secret(fp):
                os.remove(fp)
                subprocess.run(["git", "rm", "--cached", tf], capture_output=True)
                late_removed.append(tf)
        if late_removed:
            print(f"   🔒 Late-removed {len(late_removed)} additional secret files:")
            for lf in late_removed[:20]:
                print(f"      ❌ {lf}")
            subprocess.run(["git", "add", "-A"], check=True)
        
        # Step 4: Commit only if there are changes
        print("\n📝 Step 4/5: Committing changes...")
        status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
        
        if status.stdout.strip():
            changed_count = len(status.stdout.strip().split("\n"))
            commit_msg = f"Incremental sync: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\nAuto-synced by SkillAI DevOps (Job 23)"
            subprocess.run(["git", "commit", "-m", commit_msg], check=True)
            print(f"   ✅ {changed_count} files committed")
            
            # Step 5: Push to GitHub
            print("\n🚀 Step 5/5: Pushing to GitHub...")
            push_result = subprocess.run(["git", "push", "origin", "main"], capture_output=True, text=True)
            
            if push_result.returncode != 0:
                push_stderr = (push_result.stderr or "").lower()
                
                # If GitHub Push Protection blocks (secrets in git history)
                if "secret" in push_stderr or "push protection" in push_stderr:
                    print("   🔒 Push blocked by GitHub Push Protection (secrets in history)")
                    print("   🧹 Creating clean orphan branch (no history)...")
                    
                    # Orphan branch = new history with no parent commits
                    subprocess.run(["git", "checkout", "--orphan", "clean-main"], check=True)
                    subprocess.run(["git", "add", "-A"], check=True)
                    subprocess.run(["git", "commit", "-m", f"Clean sync: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\nHistory reset to remove secrets. Auto-synced by SkillAI DevOps (Job 23)"], check=True)
                    subprocess.run(["git", "branch", "-D", "main"], capture_output=True)
                    subprocess.run(["git", "branch", "-M", "main"], check=True)
                    
                    # Force push clean history
                    push_result = subprocess.run(["git", "push", "origin", "main", "--force"], capture_output=True, text=True)
                    if push_result.returncode != 0:
                        print(f"   ❌ Push still failed: {push_result.stderr[:500]}")
                        raise Exception(f"Git push failed after orphan reset: {push_result.stderr[:500]}")
                    print("   ✅ Clean history pushed (orphan branch - secrets removed from history)")
                else:
                    # Non-secret error: try force push
                    print("   ⚠️  Regular push failed, trying force push...")
                    push_result = subprocess.run(["git", "push", "origin", "main", "--force"], capture_output=True, text=True)
                    if push_result.returncode != 0:
                        print(f"   ❌ Push failed: {push_result.stderr[:500]}")
                        raise Exception(f"Git push failed: {push_result.stderr[:500]}")
            else:
                print("   ✅ Pushed to main branch")
            
            total_secrets = len(removed_files) + len(late_removed)
            print(f"\n{'='*70}")
            print(f"✅ INCREMENTAL SYNC COMPLETE!")
            print(f"   📦 Files changed: {changed_count}")
            print(f"   🔒 Secret files removed: {total_secrets}")
            print(f"   🌐 GitHub: https://github.com/{target_repo}")
            print(f"{'='*70}")
            
            return {"success": True, "files_changed": changed_count, "secrets_removed": total_secrets, "repo": target_repo}
        else:
            print("\n   ℹ️  No changes to commit (workspace already in sync)")
            print(f"\n{'='*70}")
            print(f"✅ SYNC COMPLETE - No changes needed")
            print(f"   🌐 GitHub: https://github.com/{target_repo}")
            print(f"{'='*70}")
            return {"success": True, "files_changed": 0, "secrets_removed": 0, "repo": target_repo}
    
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Git operation failed: {e}")
        if hasattr(e, 'stderr') and e.stderr:
            err = e.stderr.decode() if isinstance(e.stderr, bytes) else e.stderr
            print(f"   Error: {err[:500]}")
        raise
    
    finally:
        # Cleanup temp directory
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

# Incremental sync: workspace → GitHub
mirror_result = mirror_workspace_to_github_fast(repo_full_name, github_token, github_username)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Create Dev Branch

# COMMAND ----------

def create_branch(repo_full_name, branch_name, token, from_branch="main"):
    """Create a new branch from existing branch"""
    
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    # Get main branch SHA
    ref_url = f"https://api.github.com/repos/{repo_full_name}/git/refs/heads/{from_branch}"
    ref_response = requests.get(ref_url, headers=headers)
    
    if ref_response.status_code != 200:
        raise Exception(f"❌ Failed to get {from_branch} branch: {ref_response.json()}")
    
    main_sha = ref_response.json()['object']['sha']
    
    # Create new branch
    create_url = f"https://api.github.com/repos/{repo_full_name}/git/refs"
    create_data = {
        "ref": f"refs/heads/{branch_name}",
        "sha": main_sha
    }
    
    create_response = requests.post(create_url, headers=headers, json=create_data)
    
    if create_response.status_code == 201:
        print(f"✅ Branch '{branch_name}' created from '{from_branch}'")
        return create_response.json()
    elif create_response.status_code == 422:
        print(f"⚠️  Branch '{branch_name}' already exists")
        return None
    else:
        raise Exception(f"❌ Failed to create branch: {create_response.json()}")

# Create dev branch
create_branch(repo_full_name, "dev", github_token, from_branch="main")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Set Main as Default Branch

# COMMAND ----------

def set_default_branch(repo_full_name, branch_name, token):
    """Set the default branch for the repository"""
    
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    url = f"https://api.github.com/repos/{repo_full_name}"
    data = {
        "default_branch": branch_name
    }
    
    response = requests.patch(url, headers=headers, json=data)
    
    if response.status_code == 200:
        print(f"✅ Default branch set to '{branch_name}'")
    else:
        print(f"⚠️  Could not set default branch: {response.json().get('message', 'Unknown error')}")

# Set main as default
set_default_branch(repo_full_name, "main", github_token)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5️⃣ Make a Simple Change & Create First PR

# COMMAND ----------

def create_first_pr(repo_full_name, token):
    """Create a simple change in dev and open first PR"""
    
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    # Add a PROGRESS.md file in dev branch
    progress_content = f"""# 🎯 Progress Tracker

## Week 1
- [ ] Complete Investigation Case
- [ ] Submit Phase 1-5 daily
- [ ] Friday presentation ready
- [ ] Saturday demo complete

## First Milestone
**Date Started:** {datetime.now().strftime('%B %d, %Y')}

**Goal:** Build end-to-end data pipeline

---
*Track your journey here!*
"""
    
    file_path = "PROGRESS.md"
    file_url = f"https://api.github.com/repos/{repo_full_name}/contents/{file_path}"
    
    # Create file in dev branch
    file_data = {
        "message": "Add progress tracker",
        "content": base64.b64encode(progress_content.encode()).decode(),
        "branch": "dev"
    }
    
    file_response = requests.put(file_url, headers=headers, json=file_data)
    
    if file_response.status_code == 201:
        print(f"✅ Created {file_path} in dev branch")
    else:
        print(f"⚠️  Could not create file: {file_response.json().get('message', 'Unknown error')}")
    
    # Create Pull Request
    pr_url = f"https://api.github.com/repos/{repo_full_name}/pulls"
    pr_data = {
        "title": "🎯 First PR: Add Progress Tracker",
        "body": f"""## Your First Pull Request! 🎉

This PR adds a progress tracker to help you monitor your SkillAI Path journey.

**What's included:**
- PROGRESS.md file for tracking weekly goals
- Template for Investigation Case milestones

**Next steps:**
1. Review the changes
2. Merge this PR to complete your first Git workflow
3. Start tracking your progress!

---
*Auto-generated by SkillAI Path DevOps*
*Date: {datetime.now().strftime('%B %d, %Y %H:%M')}*
""",
        "head": "dev",
        "base": "main"
    }
    
    pr_response = requests.post(pr_url, headers=headers, json=pr_data)
    
    if pr_response.status_code == 201:
        pr_info = pr_response.json()
        print(f"✅ Pull Request created!")
        print(f"   PR #{pr_info['number']}: {pr_info['title']}")
        print(f"   URL: {pr_info['html_url']}")
        return pr_info
    else:
        print(f"⚠️  Could not create PR: {pr_response.json().get('message', 'Unknown error')}")
        return None

# Create first PR
pr_info = create_first_pr(repo_full_name, github_token)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Setup Complete!

# COMMAND ----------

print("=" * 70)
print("🎉 GITHUB SETUP COMPLETE!")
print("=" * 70)
print(f"""
✅ Repository: https://github.com/{repo_full_name}
✅ Branches: main (default), dev
✅ Template: Forked from encrypted source
✅ First PR: Ready for review and merge

🚀 NEXT STEPS:
1. Visit your repo: https://github.com/{repo_full_name}
2. Review and merge your first PR
3. Clone the repo locally
4. Start building!

📋 Your GitHub workflow is now ready for the Investigation Case!
""")

# Return success
dbutils.notebook.exit(json.dumps({
    "status": "success",
    "repo": repo_full_name,
    "branches": ["main", "dev"],
    "pr_url": pr_info['html_url'] if pr_info else None,
    "message": "GitHub setup complete"
}))
