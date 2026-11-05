# Databricks notebook source
# DBTITLE 1,SkillAI Platform - Automated Setup
# MAGIC %md
# MAGIC # SkillAI Platform - Initial Setup
# MAGIC
# MAGIC This notebook automates the `setup.sh` logic **inside Databricks** - no local terminal needed.
# MAGIC
# MAGIC **What it does:**
# MAGIC 1. Replaces `accounts-config.yml` with the clean template (resets for audience)
# MAGIC 2. Removes any leftover backup file so internal accounts stay hidden
# MAGIC 3. Prints next-steps instructions
# MAGIC
# MAGIC New users: just click **Run All** - that's it!

# COMMAND ----------

# DBTITLE 1,Run Setup
import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat

w = WorkspaceClient()

# This notebook lives at /Shared/dev/SkillAIPath/1_Canvas/0_DevOps/
# so the repo root is two directories up.
REPO = "/Shared/dev/SkillAIPath"

CONFIG    = f"{REPO}/accounts-config.yml"
BACKUP    = f"{REPO}/accounts-config.yml.backup"
TEMPLATE = f"{REPO}/accounts-config.template.yml"

print(f"Repo path: {REPO}")
print()

# Step 1: Replace accounts-config.yml with clean template
# No backup kept - internal account details must not be visible to audience
try:
    template_content = w.workspace.download(TEMPLATE).read()
    w.workspace.upload(CONFIG, template_content, overwrite=True, format=ImportFormat.AUTO)
    print("Replaced accounts-config.yml with clean template")
except Exception as e:
    w.workspace.upload(CONFIG, b"accounts: []", overwrite=True, format=ImportFormat.AUTO)
    print(f"Template not found ({e}) - created empty accounts-config.yml")

# Step 2: Remove any leftover backup file so internal accounts stay hidden
from databricks.sdk.errors import NotFound
try:
    w.workspace.delete(BACKUP)
    print("Removed accounts-config.yml.backup (internal data erased)")
except NotFound:
    pass  # no backup to remove - clean
except Exception:
    pass  # ignore - backup may not exist

print()
print("=" * 60)
print()
print("Next Steps:")
print("  1. Open accounts-config.yml and add your workspace details:")
print("       accounts:")
print("         - name: my_workspace")
print("           host: https://your-workspace.cloud.databricks.com")
print("           warehouse_id: your-warehouse-id")
print()
print("  2. Add your Databricks token as a GitHub Secret:")
print("       Repo Settings -> Secrets -> Actions -> New secret")
print("       Name:  DATABRICKS_TOKEN_MY_WORKSPACE")
print("       Value: your Databricks PAT token")
print()
print("  3. Deploy the bundle:")
print("       Option A (GitHub Actions): Actions tab -> Deploy Bundles -> Run")
print("       Option B (Local CLI):      databricks bundle deploy --target dev")
print()
print("Setup complete!")