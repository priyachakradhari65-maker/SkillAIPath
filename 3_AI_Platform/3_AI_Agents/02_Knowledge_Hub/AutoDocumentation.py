# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Incremental Auto-Documentation System
# MAGIC
# MAGIC **Purpose:** Updates knowledge base on PRODUCTION DEPLOYMENTS (not daily)
# MAGIC
# MAGIC **Trigger:** Manual or post-deployment hook (weekly/monthly)
# MAGIC **Detection:** Watermark-based (last successful run timestamp)
# MAGIC **Strategy:** MERGE to Unity Catalog (preserves unchanged entries)
# MAGIC
# MAGIC **Updates:**
# MAGIC - Google Sheets → Portfolio tasks
# MAGIC - Modified Code Files (since last deployment) → Re-chunked documentation
# MAGIC - MERGE to Unity Catalog (preserves unchanged entries)
# MAGIC
# MAGIC **Location:** `/Workspace/Shared/dev/SkillAIPath/3_AI_Platform/3_AI_Agents/Knowledge_Hub`

# COMMAND ----------

# DBTITLE 1,Configuration
import os
import json
import shutil
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# Configuration
CATALOG = "sap_dev"
SCHEMA = "gold"
KNOWLEDGE_HUB = "/Workspace/Shared/dev/SkillAIPath/3_AI_Platform/3_AI_Agents/02_Knowledge_Hub"
BASE_PATH = "/Workspace/Shared/dev/SkillAIPath"
WATERMARK_FILE = f"{KNOWLEDGE_HUB}/.last_run_watermark"

print(f"✅ Deployment-Based Auto-Documentation Started: {datetime.now()}")
print(f"📂 Knowledge Hub: {KNOWLEDGE_HUB}")
print(f"📂 Base Path: {BASE_PATH}")
print(f"🔖 Watermark File: {WATERMARK_FILE}")

# COMMAND ----------

# DBTITLE 1,Create Table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS sap_dev.gold.knowledge_base (
# MAGIC   id STRING,
# MAGIC   title STRING,
# MAGIC   content STRING,
# MAGIC   source_path STRING,
# MAGIC   category STRING,
# MAGIC   tags STRING,
# MAGIC   last_updated TIMESTAMP,
# MAGIC   created_at TIMESTAMP,
# MAGIC   deployment_version STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Central knowledge base for AI training - watermark-based deployment updates';

# COMMAND ----------

# DBTITLE 1,Watermark Detection
# STEP 1: Load Watermark (last successful run timestamp)
print("\n🔖 WATERMARK-BASED CHANGE DETECTION")
print("="*70)

# Load last successful run timestamp
if os.path.exists(WATERMARK_FILE):
    with open(WATERMARK_FILE, 'r') as f:
        watermark_data = json.load(f)
        last_run_timestamp = watermark_data.get('last_run_timestamp', 0)
        last_run_date = datetime.fromtimestamp(last_run_timestamp)
        deployment_version = watermark_data.get('deployment_version', 'unknown')
    
    print(f"\n📅 Last successful run: {last_run_date}")
    print(f"🏷️  Last deployment: {deployment_version}")
else:
    # First run - use 30 days ago as baseline
    last_run_timestamp = (datetime.now() - timedelta(days=30)).timestamp()
    last_run_date = datetime.fromtimestamp(last_run_timestamp)
    deployment_version = "initial"
    print(f"\n🆕 First run detected")
    print(f"📅 Baseline: {last_run_date} (30 days ago)")

# STEP 2: Detect Changed Files Since Last Run (watermark-based)
print(f"\n🔍 Scanning for files modified since {last_run_date}")
print("="*70)

FILE_EXTENSIONS = [".py", ".sql", ".md", ".ipynb"]
SKIP_DIRS = ["__pycache__", ".git", ".github", "tmp", "temp", ".backup"]

modified_files = []
current_time = datetime.now().timestamp()

for root, dirs, files in os.walk(BASE_PATH):
    dirs[:] = [d for d in dirs if d not in SKIP_DIRS and not d.startswith('.')]
    
    for file in files:
        if any(file.endswith(ext) for ext in FILE_EXTENSIONS):
            file_path = os.path.join(root, file)
            try:
                mod_time = os.path.getmtime(file_path)
                
                # Compare against watermark (not fixed 24h)
                if mod_time > last_run_timestamp:
                    days_ago = (current_time - mod_time) / 86400
                    modified_files.append({
                        "path": file_path,
                        "name": file,
                        "modified_days_ago": round(days_ago, 1),
                        "modified_timestamp": mod_time
                    })
            except:
                pass

print(f"\n✅ Found {len(modified_files)} files modified since last deployment")

if modified_files:
    print("\n📄 Recently modified files:")
    for f in sorted(modified_files, key=lambda x: x['modified_timestamp'], reverse=True)[:15]:
        print(f"   {f['name']} ({f['modified_days_ago']}d ago)")
    if len(modified_files) > 15:
        print(f"   ... and {len(modified_files) - 15} more")
else:
    print("\nℹ️ No code changes since last run - will refresh Google Sheets only")

# COMMAND ----------

# DBTITLE 1,Google Sheets
# STEP 3: Google Sheets - Growth Accelerator Portfolio
print("\n📊 Loading Google Sheets data...")

GOOGLE_SHEETS_ID = "2PACX-1vTtNHyz8rzgRHpVZRRBBMK81P-wcUpIRh0QDKCOESViIma4zu0iE6yAFWe_smBqdzshKUfbAGjCLjVU"
sheets_csv_url = f"https://docs.google.com/spreadsheets/d/e/{GOOGLE_SHEETS_ID}/pub?output=csv"

try:
    sheets_df = pd.read_csv(sheets_csv_url)
    print(f"✅ Loaded {len(sheets_df)} tasks from Google Sheets")
    
    sheets_records = []
    for idx, row in sheets_df.iterrows():
        task_id = str(row.get('Task_ID', f'task_{idx}'))
        content = f"""Task: {row.get('Task', 'N/A')}
Team: {row.get('Team', 'N/A')}
Module: {row.get('Module', 'N/A')}
Objective: {row.get('Objective', 'N/A')}
Priority: {row.get('Priority', 'N/A')}
File Path: {row.get('File_Path', 'N/A')}
Reviewer: {row.get('Reviewer', 'N/A')}""".strip()
        
        sheets_records.append({
            "id": f"gsheet_{task_id}",
            "title": f"{row.get('Task', 'Task')}",
            "content": content,
            "source_path": "Google Sheets - Growth Accelerator",
            "category": "portfolio",
            "tags": f"{row.get('Team', '')},{row.get('Module', '')},{row.get('Priority', '')}".lower(),
            "last_updated": datetime.now(),
            "created_at": datetime.now(),
            "deployment_version": deployment_version
        })
    
    print(f"   Converted {len(sheets_records)} portfolio tasks")
except Exception as e:
    print(f"⚠️ Error loading Google Sheets: {e}")
    sheets_records = []

# COMMAND ----------

# DBTITLE 1,Re-chunk Modified Files
# STEP 4: Re-chunk Modified Files ONLY (watermark-based)
print("\n📚 Processing Modified Code Files...")

kb_records = []

if modified_files:
    for file_info in modified_files:
        try:
            file_path = file_info['path']
            file_name = file_info['name']
            
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Chunk: 500 lines per chunk
            lines = content.split('\n')
            chunk_size = 500
            
            for i in range(0, len(lines), chunk_size):
                chunk_lines = lines[i:i+chunk_size]
                chunk_text = '\n'.join(chunk_lines)[:2000]  # Max 2000 chars
                
                if chunk_text.strip():
                    chunk_id = f"kb_{file_name.replace('.', '_')}_{i//chunk_size}"
                    
                    kb_records.append({
                        "id": chunk_id,
                        "title": f"{file_name} (Chunk {i//chunk_size + 1})",
                        "content": chunk_text,
                        "source_path": file_path.replace('/Workspace', ''),
                        "category": file_name.split('.')[-1],
                        "tags": f"{file_name.split('.')[-1]},modified",
                        "last_updated": datetime.now(),
                        "created_at": datetime.now(),
                        "deployment_version": deployment_version
                    })
        except Exception as e:
            print(f"   ⚠️ Failed to process {file_name}: {e}")
    
    print(f"✅ Re-chunked {len(modified_files)} files → {len(kb_records)} chunks")
else:
    print("ℹ️ No modified files - skipping re-chunking")

# COMMAND ----------

# DBTITLE 1,Backup
# STEP 5: Backup Before Update (Safety)
print("\n📦 Creating backup...")

KB_JSON_PATH = f"{KNOWLEDGE_HUB}/knowledge_base.json"
BACKUP_PATH = f"{KNOWLEDGE_HUB}/knowledge_base.json.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

try:
    if os.path.exists(KB_JSON_PATH):
        shutil.copy2(KB_JSON_PATH, BACKUP_PATH)
        print(f"✅ Backup created: {os.path.basename(BACKUP_PATH)}")
    else:
        print("ℹ️ No existing knowledge_base.json (first run)")
except Exception as e:
    print(f"⚠️ Backup failed: {e}")

# COMMAND ----------

# DBTITLE 1,Combine Sources
# STEP 6: Combine All Sources
print("\n📦 Combining training sources...")

all_records = []
all_records.extend(sheets_records)
all_records.extend(kb_records)

print(f"   Google Sheets: {len(sheets_records)} entries")
print(f"   Modified Files: {len(kb_records)} chunks")
print(f"\n✅ Total records to merge: {len(all_records)}")

if all_records:
    combined_df = spark.createDataFrame(all_records)
else:
    print("⚠️ No data to update")
    combined_df = None

# COMMAND ----------

# DBTITLE 1,MERGE and Update Watermark
# STEP 7: MERGE to Unity Catalog (INCREMENTAL - preserves unchanged)
if combined_df and combined_df.count() > 0:
    # Deduplicate source data - keep latest record per ID
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, col, desc
    
    window_spec = Window.partitionBy("id").orderBy(desc("last_updated"))
    deduplicated_df = combined_df.withColumn("row_num", row_number().over(window_spec)) \
                                   .filter(col("row_num") == 1) \
                                   .drop("row_num")
    
    print(f"\nDeduplication: {combined_df.count()} → {deduplicated_df.count()} records")
    
    deduplicated_df.createOrReplaceTempView("new_kb_data")
    
    merge_sql = f"""
    MERGE INTO {CATALOG}.{SCHEMA}.knowledge_base AS target
    USING new_kb_data AS source
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
    
    spark.sql(merge_sql)
    
    total_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.knowledge_base").collect()[0]['cnt']
    
    print("\n✅ INCREMENTAL MERGE COMPLETE")
    print(f"   Records merged: {deduplicated_df.count()}")
    print(f"   Total KB size: {total_count}")
    print(f"   Strategy: MERGE (preserved unchanged entries)")
    
    print("\n📊 Recently updated:")
    spark.sql(f"""
        SELECT id, title, category, last_updated 
        FROM {CATALOG}.{SCHEMA}.knowledge_base 
        ORDER BY last_updated DESC 
        LIMIT 5
    """).show(truncate=False)
    
    # Update watermark (only on successful merge)
    new_watermark = {
        "last_run_timestamp": current_time,
        "last_run_date": datetime.now().isoformat(),
        "deployment_version": deployment_version,
        "files_processed": len(modified_files),
        "records_merged": combined_df.count()
    }
    
    with open(WATERMARK_FILE, 'w') as f:
        json.dump(new_watermark, f, indent=2)
    
    print(f"\n🔖 Watermark updated: {datetime.now()}")
else:
    print("\nℹ️ No new data to merge - watermark unchanged")

# COMMAND ----------

# DBTITLE 1,Summary Report
# STEP 8: Summary Report
total_kb_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.knowledge_base").collect()[0]['cnt']

summary = {
    "timestamp": str(datetime.now()),
    "mode": "WATERMARK_BASED_INCREMENTAL",
    "trigger": "POST_DEPLOYMENT",
    "records_merged": combined_df.count() if combined_df else 0,
    "total_kb_size": total_kb_count,
    "change_detection": {
        "watermark_date": str(last_run_date),
        "files_modified_since_last_run": len(modified_files),
        "chunks_updated": len(kb_records)
    },
    "sources": {
        "google_sheets": len(sheets_records),
        "code_chunks": len(kb_records)
    },
    "status": "success",
    "deployment_version": deployment_version
}

print("\n" + "="*70)
print("✅ DEPLOYMENT-BASED AUTO-DOCUMENTATION COMPLETE")
print("="*70)
print(json.dumps(summary, indent=2))
print("\n🔄 Strategy:")
print("  ✅ Changed since last deployment: UPDATED")
print("  ✅ New: INSERTED")
print("  ✅ Unchanged: PRESERVED")
print("\n📊 Feeds:")
print("  - Team Operations Center")
print("  - PR Reviewer")
print("  - All apps via sap_dev.gold.knowledge_base")
print("\n🚀 Next Run:")
print("  Trigger: Manual or post-deployment hook")
print("  Detection: Files modified since this run")
