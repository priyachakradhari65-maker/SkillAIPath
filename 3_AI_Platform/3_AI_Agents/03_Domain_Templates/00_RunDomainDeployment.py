# Databricks notebook source
# DBTITLE 1,🎯 INTERACTIVE MODE: Custom Domain Deployment
# MAGIC %md
# MAGIC # 🤖 DOMAIN TEMPLATES - INTERACTIVE DEPLOYMENT
# MAGIC
# MAGIC **Deploy production-grade data platforms via Genie API**
# MAGIC
# MAGIC This notebook orchestrates complete domain deployment:
# MAGIC * 🎯 Custom deployment requirements in natural language
# MAGIC * 💬 Interactive discussion with Genie
# MAGIC * 🚀 Auto-deployment from CSV/Excel files
# MAGIC * 📊 Bronze → Silver → Gold layer generation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Usage Modes:
# MAGIC
# MAGIC ### 1. **Automatic Mode** (Default)
# MAGIC * Upload CSVs to UC Volume
# MAGIC * Auto-detects schema and relationships
# MAGIC * Deploys complete medallion architecture
# MAGIC
# MAGIC ### 2. **Interactive Mode** (Genie)
# MAGIC * Custom requirements via Genie prompts
# MAGIC * AI-driven domain recommendations
# MAGIC * Pattern matching from SkillAIPath
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Integration:
# MAGIC * **Genie Space:** [Domain Templates Assistant](#genie-01f18d238c78141796f0c9fc39484e2b)
# MAGIC * **Pattern Source:** SkillAIPath (1000+ production patterns)
# MAGIC * **Output:** Unity Catalog tables + notebooks

# COMMAND ----------

# DBTITLE 1,Genie API Integration (Interactive Mode)
# ============================================
# GENIE API INTEGRATION FOR CUSTOM DEPLOYMENT
# ============================================
# Adapted from: Monthly_Upgrade_System/auto_upgrade/00_RUN_AUTO_UPGRADE

import requests
import json
import time

def ask_genie_for_deployment(custom_requirements: str, space_id: str = "01f18d238c78141796f0c9fc39484e2b", timeout: int = 120):
    """
    Use Genie API to analyze custom deployment requirements.
    
    Args:
        custom_requirements: Natural language description (e.g., "Deploy healthcare domain with patient tracking")
        space_id: Domain Templates Assistant Genie Space ID
        timeout: Max wait time for Genie response
    
    Returns:
        dict: Genie recommendations including catalog, schema, tables, relationships
    """
    print(f"💬 Asking Genie: {custom_requirements[:100]}...")
    
    # Get Databricks context
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    host = ctx.browserHostName().get()
    token = ctx.apiToken().get()
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Start conversation with Genie
    create_url = f"https://{host}/api/2.0/genie/spaces/{space_id}/start-conversation"
    create_response = requests.post(create_url, headers=headers, json={})
    
    if create_response.status_code != 200:
        print(f"❌ Failed to start Genie conversation: {create_response.text}")
        return None
    
    conversation_id = create_response.json()["conversation_id"]
    message_id = create_response.json()["message_id"]
    
    # Send deployment requirements
    message_url = f"https://{host}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages"
    message_response = requests.post(
        message_url,
        headers=headers,
        json={"content": custom_requirements}
    )
    
    if message_response.status_code != 200:
        print(f"❌ Failed to send message: {message_response.text}")
        return None
    
    # Poll for response
    start_time = time.time()
    while (time.time() - start_time) < timeout:
        status_url = f"https://{host}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}"
        status_response = requests.get(status_url, headers=headers)
        
        if status_response.status_code == 200:
            data = status_response.json()
            if data.get("status") == "COMPLETED":
                print("✅ Genie response received!")
                return {
                    "recommendations": data.get("attachments", []),
                    "sql_queries": data.get("query", ""),
                    "conversation_id": conversation_id,
                    "full_response": data
                }
        
        time.sleep(2)
    
    print("⏱️ Genie response timeout")
    return None

# COMMAND ----------

# DBTITLE 1,Interactive Mode: Usage Examples
# MAGIC %md
# MAGIC ## 📚 HOW TO USE INTERACTIVE MODE
# MAGIC
# MAGIC ### Example 1: Healthcare Domain
# MAGIC ```python
# MAGIC CONFIG["interactive_mode"] = True
# MAGIC CONFIG["custom_requirements"] = """
# MAGIC Deploy a healthcare analytics platform with:
# MAGIC - Patient demographics and visit tracking
# MAGIC - Appointment scheduling
# MAGIC - Billing and insurance
# MAGIC - SCD2 for patient history
# MAGIC """
# MAGIC ```
# MAGIC
# MAGIC ### Example 2: E-commerce
# MAGIC ```python
# MAGIC CONFIG["interactive_mode"] = True
# MAGIC CONFIG["custom_requirements"] = """
# MAGIC Build an e-commerce platform tracking:
# MAGIC - Customers and orders
# MAGIC - Product catalog with inventory
# MAGIC - Shopping cart sessions
# MAGIC - Payment transactions
# MAGIC """
# MAGIC ```
# MAGIC
# MAGIC ### Example 3: Financial Services
# MAGIC ```python
# MAGIC CONFIG["interactive_mode"] = True
# MAGIC CONFIG["custom_requirements"] = """
# MAGIC Create a financial data warehouse with:
# MAGIC - Account transactions
# MAGIC - Customer KYC data
# MAGIC - Loan applications and approvals
# MAGIC - Risk scoring models
# MAGIC """
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,MASTER - Domain Deployment Orchestrator
# MAGIC %md
# MAGIC # 🚀 DOMAIN TEMPLATES ORCHESTRATOR - PRODUCTION SYSTEM
# MAGIC **Autonomous Domain Deployment System**
# MAGIC
# MAGIC This notebook orchestrates complete domain deployment:
# MAGIC
# MAGIC ## Pipeline Flow:
# MAGIC 1. **Configuration** → Set domain name, catalog, upload location
# MAGIC 2. **Genie API** (if interactive) → Get AI recommendations
# MAGIC 3. **Pattern Extraction** → Scan SkillAIPath for matching patterns
# MAGIC 4. **Autonomous Deployment** → Bronze → Silver → Gold
# MAGIC 5. **Summary** → Tables created, notebooks generated, Genie Space link
# MAGIC
# MAGIC ## Frequency:
# MAGIC * **On-demand:** Run manually with custom requirements
# MAGIC * **Scheduled:** Monthly auto-deployment from UC Volume
# MAGIC
# MAGIC ## Integration:
# MAGIC * **Source Data:** UC Volume `/Volumes/main/domain_uploads/<domain>/`
# MAGIC * **Pattern Library:** SkillAIPath (1000+ production patterns)
# MAGIC * **Output Catalog:** Unity Catalog `<domain>_analytics`

# COMMAND ----------

# DBTITLE 1,Pipeline Configuration
# Domain Deployment Pipeline Configuration
from datetime import datetime
import time
import uuid

print("🔧 DOMAIN DEPLOYMENT CONFIGURATION")
print("="*80)

# Configuration
CONFIG = {
    "domain_name": "learning_platform_v2",  # Change this for your domain
    "catalog": "learning_platform_v2",
    "domain_type": "education",  # education, healthcare, ecommerce, finance, etc.
    
    # Deployment settings
    "auto_deploy_enabled": True,
    "dry_run": False,  # False = Deploy to Unity Catalog
    
    # Data source
    "upload_location": "/Volumes/main/domain_uploads/learning_platform_v2/",  # UC Volume or workspace path
    
    # 🆕 NEW: Interactive Mode (Custom Requirements via Genie)
    "interactive_mode": False,  # Set to True to use Genie API
    "custom_requirements": """
    Deploy a learning management system with engineer tracking,
    course enrollment, and assessment scores.
    """,
    
    # Genie Space
    "genie_space_id": "01f18d238c78141796f0c9fc39484e2b",  # Domain Templates Assistant
    
    # Pattern source
    "skillaipath_root": "/Workspace/Users/user@example.com/SkillAIPath",
    
    # Output
    "output_path": "/Workspace/Users/user@example.com/Domain_Templates/output",
    
    # Notification
    "notification_email": "user@example.com"
}

print(f"Domain: {CONFIG['domain_name']}")
print(f"Catalog: {CONFIG['catalog']}")
print(f"Interactive Mode: {'✅ ENABLED' if CONFIG['interactive_mode'] else '❌ DISABLED'}")
print(f"Dry Run: {'⚠️  YES (no actual deployment)' if CONFIG['dry_run'] else '✅ NO (will deploy to UC)'}")
print("="*80)

# Generate pipeline ID
pipeline_id = str(uuid.uuid4())
start_time = time.time()

print(f"\n🚀 Pipeline ID: {pipeline_id}")
print(f"⏰ Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# COMMAND ----------

# DBTITLE 1,Step 1 - Check Genie Interactive Mode
# Step 1: Check for Interactive Mode
print("\n💬 STEP 1: INTERACTIVE MODE CHECK")
print("="*80)

genie_recommendations = None

if CONFIG.get("interactive_mode", False):
    print("✅ Interactive Mode: ENABLED")
    print()
    
    custom_req = CONFIG.get("custom_requirements", "")
    if custom_req:
        print(f"📝 Custom Requirements:")
        print(f"   {custom_req}")
        print()
        
        # Call Genie API
        print("🤖 Calling Genie API...")
        genie_recommendations = ask_genie_for_deployment(
            custom_req,
            space_id=CONFIG["genie_space_id"]
        )
        
        if genie_recommendations:
            print("\n✅ Genie Recommendations Received:")
            print(json.dumps(genie_recommendations, indent=2)[:500] + "...")
            
            # TODO: Merge Genie recommendations with CONFIG
            # For now, keep existing CONFIG and let deployer use it
        else:
            print("⚠️ No Genie recommendations - continuing with default config")
    else:
        print("⚠️ Interactive mode enabled but no custom_requirements provided")
else:
    print("❌ Interactive Mode: DISABLED (using default config)")

print("="*80)

# COMMAND ----------

# DBTITLE 1,Step 2 - Execute Autonomous Deployer
# Step 2: Execute Complete Autonomous Deployer
print("\n⚙️ STEP 2: EXECUTE AUTONOMOUS DEPLOYER")
print("="*80)
print()

import sys
sys.path.insert(0, '/Workspace/Users/user@example.com/Domain_Templates')

# Write temp config file
import yaml
from pathlib import Path

temp_config_path = f"/tmp/domain_config_{pipeline_id}.yaml"

config_for_deployer = {
    "domain": {
        "name": CONFIG["domain_name"],
        "catalog": CONFIG["catalog"],
        "type": CONFIG["domain_type"]
    },
    "upload_location": CONFIG["upload_location"],
    "skillaipath_root": CONFIG["skillaipath_root"],
    "output_path": CONFIG["output_path"],
    "enable_dbt": False  # Can enable later
}

with open(temp_config_path, 'w') as f:
    yaml.dump(config_for_deployer, f)

print(f"✅ Config written to: {temp_config_path}")
print()

if not CONFIG["dry_run"]:
    print("🚀 DEPLOYING TO UNITY CATALOG...")
    print()
    
    from complete_autonomous_deployer import CompleteAutonomousDeployer
    
    # Initialize deployer
    deployer = CompleteAutonomousDeployer(temp_config_path)
    
    # Execute deployment
    deployer.deploy_complete_domain()
    
    print()
    print("✅ DEPLOYMENT COMPLETE!")
else:
    print("⚠️ DRY RUN MODE - Skipping actual deployment")
    print("   Set CONFIG['dry_run'] = False to deploy")

print("="*80)

# COMMAND ----------

# DBTITLE 1,Step 3 - Pipeline Summary
# Pipeline Summary
print("\n📊 PIPELINE SUMMARY")
print("="*80)
print()

end_time = time.time()
duration = end_time - start_time

print(f"Pipeline ID: {pipeline_id}")
print(f"Duration: {duration:.2f} seconds")
print(f"Domain: {CONFIG['domain_name']}")
print(f"Catalog: {CONFIG['catalog']}")
print()

if not CONFIG["dry_run"]:
    print("✅ DEPLOYMENT SUCCESSFUL")
    print()
    print("📁 Generated Assets:")
    print(f"   • Bronze tables: Check {CONFIG['catalog']}.bronze")
    print(f"   • Silver tables: Check {CONFIG['catalog']}.silver")
    print(f"   • Gold tables: Check {CONFIG['catalog']}.gold")
    print(f"   • Python files: {CONFIG['output_path']}/{CONFIG['domain_name']}")
    print()
    print("🔗 Next Steps:")
    print(f"   1. Query tables in Unity Catalog: {CONFIG['catalog']}")
    print(f"   2. Ask questions in Genie Space: https://your-workspace.cloud.databricks.com/genie/spaces/{CONFIG['genie_space_id']}")
    print(f"   3. Schedule this notebook as a monthly job")
else:
    print("⚠️ DRY RUN - No deployment executed")

print()
print("="*80)

# COMMAND ----------

