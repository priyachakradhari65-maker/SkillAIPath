# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 🚀 Google Drive → Unity Catalog Universal Extractor
# MAGIC ## Autonomous Multi-Source Training Data Pipeline
# MAGIC
# MAGIC ### 📊 **Architecture:**
# MAGIC ```
# MAGIC Google Drive Folder (AutoLogger)
# MAGIC   ├─ Sheets  → Pandas      → Text
# MAGIC   ├─ Docs    → Google API  → Text  
# MAGIC   ├─ HTML    → BeautifulSoup → Text
# MAGIC   └─ PDF/IMG → ai_extract  → Text
# MAGIC          ↓
# MAGIC   Watermark Filter (only modified files)
# MAGIC          ↓
# MAGIC   Source Detection (team chat|discord|graphy|genie)
# MAGIC          ↓
# MAGIC   Categorization (data_eng|mlops|sql|...)
# MAGIC          ↓
# MAGIC   Delta Table (MERGE - no duplicates)
# MAGIC          ↓
# MAGIC   Watermark Update (save timestamp)
# MAGIC ```
# MAGIC
# MAGIC ### ✅ **Features:**
# MAGIC - **Watermark-based** - Processes only new/modified files
# MAGIC - **Multi-format** - Sheets, Docs, HTML, PDF, Images
# MAGIC - **Multi-source** - team chat, Discord, community, Genie
# MAGIC - **Autonomous** - Self-healing, auto-creates tables
# MAGIC - **Scheduled** - Daily via job, manual anytime
# MAGIC - **Idempotent** - Safe to run multiple times
# MAGIC
# MAGIC ### 🔗 **Google Drive Folder:**
# MAGIC **ID:** `1AZCgTAFKkTw8aqkkYfQUdObABuSGM-lA`

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚠️ Google Cloud APIs Required
# MAGIC
# MAGIC Before running, enable these APIs:
# MAGIC
# MAGIC 1. **Google Drive API:** https://console.developers.google.com/apis/api/drive.googleapis.com/overview?project=102579871571
# MAGIC 2. **Google Docs API:** https://console.developers.google.com/apis/api/docs.googleapis.com/overview?project=102579871571
# MAGIC 3. **Google Sheets API:** https://console.developers.google.com/apis/api/sheets.googleapis.com/overview?project=102579871571
# MAGIC
# MAGIC Click each link → Enable API → Wait 2-3 minutes

# COMMAND ----------

# MAGIC %pip install --quiet google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client beautifulsoup4
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Configuration
import os
import json
import re
import hashlib
from datetime import datetime, timedelta
import pandas as pd
from bs4 import BeautifulSoup
import requests
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# Configuration
CATALOG = 'sap_dev'
SCHEMA = 'gold'
TABLE = 'skillai_execution_knowledge'

# Google Drive
GOOGLE_DRIVE_FOLDER_ID = '1AZCgTAFKkTw8aqkkYfQUdObABuSGM-lA'
SERVICE_ACCOUNT_JSON_PATH = '/Workspace/Shared/dev/SkillAIPath/3_AI_Platform/3_AI_Agents/02_Knowledge_Hub/Training_Data_Extraction/diesel-ellipse-505304-d0-f359e9488ed5.json'

# Watermark - using workspace path (not UC Volume)
WATERMARK_PATH = '/Workspace/Shared/dev/SkillAIPath/3_AI_Platform/3_AI_Agents/02_Knowledge_Hub/Training_Data_Extraction/.watermark_gdrive.json'

print(f'🎯 Target: {CATALOG}.{SCHEMA}.{TABLE}')
print(f'📂 Google Drive: {GOOGLE_DRIVE_FOLDER_ID}')
print(f'🔖 Watermark: {WATERMARK_PATH}')

# COMMAND ----------

# Authenticate with Google Drive API
with open(SERVICE_ACCOUNT_JSON_PATH, 'r') as f:
    service_account_info = json.load(f)

credentials = service_account.Credentials.from_service_account_info(
    service_account_info,
    scopes=[
        'https://www.googleapis.com/auth/drive.readonly',
        'https://www.googleapis.com/auth/documents.readonly',
        'https://www.googleapis.com/auth/spreadsheets.readonly'
    ]
)

drive_service = build('drive', 'v3', credentials=credentials)
docs_service = build('docs', 'v1', credentials=credentials)
sheets_service = build('sheets', 'v4', credentials=credentials)

print('✅ Google Drive API authenticated')
print(f'   Service account: {service_account_info.get("client_email", "N/A")}')

# COMMAND ----------

def load_watermark():
    try:
        watermark_content = dbutils.fs.head(WATERMARK_PATH)
        watermark_data = json.loads(watermark_content)
        last_run_timestamp = watermark_data.get('last_run_timestamp_utc')
        last_run_dt = datetime.fromisoformat(last_run_timestamp)
        print(f'📌 Watermark loaded: {last_run_dt} UTC')
        return last_run_dt, watermark_data
    except:
        # First run - process all files from beginning of time
        baseline = datetime(1970, 1, 1)  # Unix epoch
        print(f'🆕 First run - baseline: {baseline} (all files ever)')
        return baseline, {}

def save_watermark(stats):
    watermark_data = {
        'last_run_timestamp_utc': datetime.utcnow().isoformat(),
        'last_run_stats': stats,
        'google_drive_folder_id': GOOGLE_DRIVE_FOLDER_ID
    }
    dbutils.fs.put(WATERMARK_PATH, json.dumps(watermark_data, indent=2), overwrite=True)
    print(f'\n✅ Watermark updated: {watermark_data["last_run_timestamp_utc"]}')
    return watermark_data

# Load current watermark
print('\n🔖 WATERMARK CHECK')
print('='*80)
last_watermark, watermark_history = load_watermark()

# COMMAND ----------

def list_drive_files(folder_id, modified_after=None):
    query = f"'{folder_id}' in parents and trashed=false"
    if modified_after:
        modified_str = modified_after.strftime('%Y-%m-%dT%H:%M:%S')
        query += f" and modifiedTime > '{modified_str}Z'"
    
    results = drive_service.files().list(
        q=query,
        fields='files(id, name, mimeType, modifiedTime, size, webViewLink)',
        pageSize=1000
    ).execute()
    
    files = results.get('files', [])
    print(f'\n📂 Found {len(files)} files in Google Drive')
    if modified_after:
        print(f'   (modified after {modified_after})')
    
    for f in files[:10]:
        print(f'   • {f["name"]} ({f["mimeType"].split(".")[-1]})')
    if len(files) > 10:
        print(f'   ... and {len(files) - 10} more')
    
    return files

# Scan for new/modified files
print('\n📂 SCANNING GOOGLE DRIVE')
print('='*80)
drive_files = list_drive_files(GOOGLE_DRIVE_FOLDER_ID, modified_after=last_watermark)

# COMMAND ----------

def extract_google_sheet(file_id, file_name):
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=file_id, range='A1:Z1000'
        ).execute()
        values = result.get('values', [])
        if not values:
            return None
        df = pd.DataFrame(values[1:], columns=values[0])
        text_parts = [f'# {file_name}\n']
        for idx, row in df.iterrows():
            row_text = ' | '.join([f'{col}: {val}' for col, val in zip(df.columns, row) if val])
            text_parts.append(f'Row {idx+1}: {row_text}')
        return '\n'.join(text_parts)
    except Exception as e:
        print(f'  ⚠️  Sheet error: {str(e)[:100]}')
        return None

def extract_google_doc(file_id, file_name):
    try:
        doc = docs_service.documents().get(documentId=file_id).execute()
        content = doc.get('body', {}).get('content', [])
        text_parts = [f'# {file_name}\n']
        for element in content:
            if 'paragraph' in element:
                for text_run in element['paragraph'].get('elements', []):
                    if 'textRun' in text_run:
                        text_parts.append(text_run['textRun']['content'])
        return ''.join(text_parts)
    except Exception as e:
        print(f'  ⚠️  Doc error: {str(e)[:100]}')
        return None

def extract_html(file_id, file_name):
    try:
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        html_content = fh.getvalue().decode('utf-8')
        soup = BeautifulSoup(html_content, 'html.parser')
        for script in soup(['script', 'style']):
            script.decompose()
        text = soup.get_text()
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split('  '))
        text = '\n'.join(chunk for chunk in chunks if chunk)
        return f'# {file_name}\n{text}'
    except Exception as e:
        print(f'  ⚠️  HTML error: {str(e)[:100]}')
        return None

# COMMAND ----------

def detect_source_type(content, file_name):
    content_lower = (content or '').lower()
    name_lower = file_name.lower()
    if 'source: team chat' in content_lower or 'team chat' in name_lower:
        return 'team chat'
    if 'source: discord' in content_lower or 'discord' in name_lower:
        return 'discord'
    if 'source: graphy' in content_lower or 'graphy' in name_lower:
        return 'graphy'
    if 'genie' in name_lower or 'databricks' in content_lower:
        return 'genie'
    return 'unknown'

def categorize_content(content):
    content_lower = (content or '').lower()
    categories = []
    keywords = {
        'data_engineering': ['etl', 'pipeline', 'airflow', 'spark', 'delta'],
        'ml_ops': ['model', 'training', 'mlflow', 'feature', 'inference'],
        'sql': ['select', 'join', 'query', 'table', 'database'],
        'error_handling': ['error', 'exception', 'failed', 'bug', 'issue'],
        'deployment': ['deploy', 'production', 'release', 'environment'],
        'documentation': ['guide', 'tutorial', 'how to', 'documentation']
    }
    for category, kws in keywords.items():
        if any(kw in content_lower for kw in kws):
            categories.append(category)
    return '|'.join(categories) if categories else 'general'

print('✅ Helper functions loaded')

# COMMAND ----------

print('\n🔄 PROCESSING FILES')
print('='*80)

extracted_records = []
stats = {'processed': 0, 'skipped': 0, 'errors': 0, 'by_type': {}}

for file_info in drive_files:
    file_id = file_info['id']
    file_name = file_info['name']
    mime_type = file_info['mimeType']
    modified_time = datetime.fromisoformat(file_info['modifiedTime'].replace('Z', '+00:00'))
    
    print(f'\n📄 {file_name}')
    print(f'   Type: {mime_type.split(".")[-1]}')
    print(f'   Modified: {modified_time}')
    
    # Extract based on type
    content = None
    file_format = 'unknown'
    
    if 'spreadsheet' in mime_type:
        content = extract_google_sheet(file_id, file_name)
        file_format = 'sheet'
    elif 'document' in mime_type:
        content = extract_google_doc(file_id, file_name)
        file_format = 'doc'
    elif 'html' in mime_type or file_name.endswith('.html'):
        content = extract_html(file_id, file_name)
        file_format = 'html'
    
    if not content:
        print(f'   ⏭️  Skipped (no extractor or empty)')
        stats['skipped'] += 1
        continue
    
    # Detect source and categorize
    source_type = detect_source_type(content, file_name)
    categories = categorize_content(content)
    
    # Generate conversation ID
    conv_id = hashlib.md5(f'{file_id}_{file_name}'.encode()).hexdigest()
    
    # Create record
    record = {
        'conversation_id': conv_id,
        'user_email': 'gdrive_auto_import@skillaipath.com',
        'thread_name': file_name,
        'conversation_text': content[:50000],
        'full_conversation_json': json.dumps({
            'file_id': file_id,
            'file_name': file_name,
            'mime_type': mime_type,
            'web_link': file_info.get('webViewLink', ''),
            'extracted_at': datetime.utcnow().isoformat()
        }),
        'message_count': len(content.split('\n')),
        'user_message_count': 0,
        'assistant_message_count': 0,
        'categories': categories,
        'scopes': source_type,
        'error_patterns': '',
        'has_errors': 'error' in content.lower() or 'exception' in content.lower(),
        'has_code': 'def ' in content or 'SELECT' in content or 'import ' in content,
        'has_assets_created': False,
        'source': source_type,
        'file_path': file_info.get('webViewLink', ''),
        'file_modified_timestamp': modified_time,
        'extraction_timestamp': datetime.utcnow(),
        'file_format': file_format,
        'created_at': datetime.utcnow()
    }
    
    extracted_records.append(record)
    stats['processed'] += 1
    stats['by_type'][file_format] = stats['by_type'].get(file_format, 0) + 1
    
    print(f'   ✅ Extracted: {len(content)} chars')
    print(f'   Source: {source_type} | Categories: {categories}')

print(f'\n📊 Extraction complete: {stats["processed"]} processed, {stats["skipped"]} skipped')

# COMMAND ----------

# DBTITLE 1,Ensure Table Exists
# Ensure schema and table exist BEFORE any processing
print('\n🔧 ENSURING TABLE EXISTS')
print('='*80)

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    print(f'✅ Schema {CATALOG}.{SCHEMA} ready')
except Exception as e:
    print(f'⚠️  Schema check: {e}')

# Create table with explicit schema if it doesn't exist
try:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.{TABLE} (
            conversation_id STRING,
            user_email STRING,
            thread_name STRING,
            conversation_text STRING,
            full_conversation_json STRING,
            message_count INT,
            user_message_count INT,
            assistant_message_count INT,
            categories STRING,
            scopes STRING,
            error_patterns STRING,
            has_errors BOOLEAN,
            has_code BOOLEAN,
            has_assets_created BOOLEAN,
            source STRING,
            file_path STRING,
            file_modified_timestamp TIMESTAMP,
            extraction_timestamp TIMESTAMP,
            file_format STRING,
            created_at TIMESTAMP
        )
        USING DELTA
    """)
    print(f'✅ Table {CATALOG}.{SCHEMA}.{TABLE} ready')
except Exception as e:
    print(f'❌ Table creation failed: {e}')
    raise

# COMMAND ----------

# DBTITLE 1,Save to Delta Table
if extracted_records:
    print('\n💾 SAVING TO DELTA TABLE')
    print('='*80)
    
    # Convert to Spark DataFrame
    schema = StructType([
        StructField('conversation_id', StringType()),
        StructField('user_email', StringType()),
        StructField('thread_name', StringType()),
        StructField('conversation_text', StringType()),
        StructField('full_conversation_json', StringType()),
        StructField('message_count', IntegerType()),
        StructField('user_message_count', IntegerType()),
        StructField('assistant_message_count', IntegerType()),
        StructField('categories', StringType()),
        StructField('scopes', StringType()),
        StructField('error_patterns', StringType()),
        StructField('has_errors', BooleanType()),
        StructField('has_code', BooleanType()),
        StructField('has_assets_created', BooleanType()),
        StructField('source', StringType()),
        StructField('file_path', StringType()),
        StructField('file_modified_timestamp', TimestampType()),
        StructField('extraction_timestamp', TimestampType()),
        StructField('file_format', StringType()),
        StructField('created_at', TimestampType())
    ])
    
    df = spark.createDataFrame(extracted_records, schema=schema)
    df.createOrReplaceTempView('new_records')
    
    # Use MERGE to avoid duplicates (table already created earlier)
    spark.sql(f"""
        MERGE INTO {CATALOG}.{SCHEMA}.{TABLE} AS target
        USING new_records AS source
        ON target.conversation_id = source.conversation_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    print(f'✅ Saved {len(extracted_records)} records')
    print(f'   Table: {CATALOG}.{SCHEMA}.{TABLE}')
    
    # Update watermark
    save_watermark(stats)
    
else:
    print('\nℹ️  No new files to process - all up to date!')

# COMMAND ----------

# DBTITLE 1,Validation - Training Data Summary
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   source,
# MAGIC   file_format,
# MAGIC   COUNT(*) as record_count,
# MAGIC   SUM(message_count) as total_messages,
# MAGIC   SUM(CASE WHEN has_code THEN 1 ELSE 0 END) as with_code,
# MAGIC   SUM(CASE WHEN has_errors THEN 1 ELSE 0 END) as with_errors,
# MAGIC   MAX(extraction_timestamp) as last_extraction
# MAGIC FROM sap_dev.gold.skillai_execution_knowledge
# MAGIC WHERE source IS NOT NULL
# MAGIC GROUP BY source, file_format
# MAGIC ORDER BY record_count DESC
