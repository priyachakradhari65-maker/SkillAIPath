# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 Verify Genie Code Extraction Processing
# MAGIC
# MAGIC ## Purpose
# MAGIC Validate data quality and completeness of extracted Genie Code conversations
# MAGIC
# MAGIC ## Checks Performed
# MAGIC 1. ✅ Extraction completeness (thread coverage)
# MAGIC 2. ✅ Data quality (null values, malformed records)
# MAGIC 3. ✅ Content validation (message counts, timestamps)
# MAGIC 4. ✅ Gap detection (missing threads, failed extractions)
# MAGIC 5. ✅ Summary statistics and trends

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Configuration

# COMMAND ----------

CATALOG = "sap_dev"
SCHEMA = "content_studio"

print(f"Verifying tables in: {CATALOG}.{SCHEMA}")
print(f"  - raw_genie_chats")
print(f"  - filtered_chats_* (if exist)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Check 1: Table Existence & Record Counts

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check if extraction table exists and get row count
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_records,
# MAGIC   COUNT(DISTINCT conversation_id) as unique_conversations,
# MAGIC   COUNT(DISTINCT user_email) as unique_users,
# MAGIC   MIN(created_at) as first_extraction,
# MAGIC   MAX(created_at) as last_extraction
# MAGIC FROM sap_dev.content_studio.raw_genie_chats

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Check 2: Data Quality - Null Values

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for null or empty values in critical fields
# MAGIC SELECT 
# MAGIC   SUM(CASE WHEN conversation_id IS NULL THEN 1 ELSE 0 END) as null_conversation_ids,
# MAGIC   SUM(CASE WHEN user_email IS NULL THEN 1 ELSE 0 END) as null_user_emails,
# MAGIC   0 as null_thread_names,
# MAGIC   SUM(CASE WHEN full_conversation_json IS NULL THEN 1 ELSE 0 END) as null_conversation_content,
# MAGIC   SUM(CASE WHEN message_count = 0 THEN 1 ELSE 0 END) as zero_message_conversations,
# MAGIC   COUNT(*) as total_records
# MAGIC FROM sap_dev.content_studio.raw_genie_chats

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Check 3: Content Statistics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze conversation content
# MAGIC SELECT 
# MAGIC   ROUND(AVG(message_count), 1) as avg_messages_per_conversation,
# MAGIC   MIN(message_count) as min_messages,
# MAGIC   MAX(message_count) as max_messages,
# MAGIC   ROUND(AVG(user_message_count), 1) as avg_user_messages,
# MAGIC   ROUND(AVG(assistant_message_count), 1) as avg_assistant_messages,
# MAGIC   ROUND(AVG(tool_call_count), 1) as avg_tool_calls,
# MAGIC   SUM(CASE WHEN has_errors THEN 1 ELSE 0 END) as conversations_with_errors,
# MAGIC   SUM(CASE WHEN has_code THEN 1 ELSE 0 END) as conversations_with_code,
# MAGIC   SUM(CASE WHEN has_assets_created THEN 1 ELSE 0 END) as conversations_creating_assets
# MAGIC FROM sap_dev.content_studio.raw_genie_chats

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Check 4: Extraction Timeline

# COMMAND ----------

# MAGIC %sql
# MAGIC -- When were conversations extracted?
# MAGIC SELECT 
# MAGIC   extraction_date,
# MAGIC   COUNT(*) as conversations_extracted,
# MAGIC   COUNT(DISTINCT user_email) as unique_users
# MAGIC FROM sap_dev.content_studio.raw_genie_chats
# MAGIC GROUP BY extraction_date
# MAGIC ORDER BY extraction_date DESC
# MAGIC LIMIT 30

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Check 5: Gap Detection - Compare with System Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare extracted threads vs available threads in system table
# MAGIC WITH system_threads AS (
# MAGIC   SELECT DISTINCT event_id as thread_id
# MAGIC   FROM system.access.assistant_events
# MAGIC   WHERE workspace_id = YOUR_WORKSPACE_ID
# MAGIC     AND event_date >= DATE_SUB(CURRENT_DATE(), 30)
# MAGIC     AND event_id IS NOT NULL
# MAGIC ),
# MAGIC extracted_threads AS (
# MAGIC   SELECT DISTINCT conversation_id as thread_id
# MAGIC   FROM sap_dev.content_studio.raw_genie_chats
# MAGIC )
# MAGIC SELECT 
# MAGIC   (SELECT COUNT(*) FROM system_threads) as threads_available_in_system,
# MAGIC   (SELECT COUNT(*) FROM extracted_threads) as threads_extracted,
# MAGIC   (SELECT COUNT(*) FROM system_threads) - (SELECT COUNT(*) FROM extracted_threads) as threads_missing,
# MAGIC   ROUND(
# MAGIC     (SELECT COUNT(*) FROM extracted_threads) * 100.0 / 
# MAGIC     NULLIF((SELECT COUNT(*) FROM system_threads), 0), 
# MAGIC     1
# MAGIC   ) as extraction_coverage_percent

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Check 6: Recent Conversations (Last 7 Days)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show most recent extracted conversations
# MAGIC SELECT 
# MAGIC   conversation_id,
# MAGIC   'N/A' as thread_name,
# MAGIC   user_email,
# MAGIC   message_count,
# MAGIC   has_errors,
# MAGIC   has_code,
# MAGIC   has_assets_created,
# MAGIC   created_at as extraction_timestamp
# MAGIC FROM sap_dev.content_studio.raw_genie_chats
# MAGIC WHERE created_at >= DATE_SUB(CURRENT_TIMESTAMP(), 7)
# MAGIC ORDER BY created_at DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Check 7: Error Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Conversations marked with errors
# MAGIC SELECT 
# MAGIC   conversation_id,
# MAGIC   'N/A' as thread_name,
# MAGIC   message_count,
# MAGIC   created_at as extraction_timestamp
# MAGIC FROM sap_dev.content_studio.raw_genie_chats
# MAGIC WHERE has_errors = TRUE
# MAGIC ORDER BY created_at DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Check 8: Processing Status Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Distribution of processing statuses
# MAGIC SELECT 
# MAGIC   processing_status,
# MAGIC   COUNT(*) as count,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage
# MAGIC FROM sap_dev.content_studio.raw_genie_chats
# MAGIC GROUP BY processing_status
# MAGIC ORDER BY count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Summary Report

# COMMAND ----------

print("="*80)
print("🔍 GENIE CODE EXTRACTION VERIFICATION SUMMARY")
print("="*80)
print()

# Get key metrics
from pyspark.sql.functions import *

df = spark.table("sap_dev.content_studio.raw_genie_chats")

total_conversations = df.count()
unique_users = df.select("user_email").distinct().count()
avg_messages = df.agg(avg("message_count")).collect()[0][0]
with_errors = df.filter(col("has_errors") == True).count()
with_code = df.filter(col("has_code") == True).count()
with_assets = df.filter(col("has_assets_created") == True).count()

print(f"✅ Total Conversations Extracted: {total_conversations:,}")
print(f"✅ Unique Users: {unique_users}")
print(f"✅ Average Messages per Conversation: {avg_messages:.1f}")
print(f"⚠️  Conversations with Errors: {with_errors}")
print(f"💻 Conversations with Code: {with_code}")
print(f"🎯 Conversations Creating Assets: {with_assets}")
print()
print("="*80)

# Calculate health score
if total_conversations > 0:
    null_checks = df.filter(
        col("conversation_id").isNull() | 
        col("user_email").isNull() | 
        col("full_conversation_json").isNull()
    ).count()
    
    health_score = 100 - (null_checks * 100.0 / total_conversations)
    
    print(f"🏥 Data Health Score: {health_score:.1f}%")
    
    if health_score >= 95:
        print("   ✅ EXCELLENT - Data quality is very high")
    elif health_score >= 80:
        print("   ⚠️  GOOD - Minor data quality issues detected")
    else:
        print("   ❌ NEEDS ATTENTION - Significant data quality issues")
else:
    print("⚠️  No data extracted yet")

print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Verification Complete
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC 1. **If gaps detected**: Re-run 1_REAL_Chat_Extractor
# MAGIC 2. **If quality issues**: Check extraction function logic
# MAGIC 3. **Schedule regular runs**: Set up daily verification
# MAGIC 4. **Monitor trends**: Watch extraction coverage over time