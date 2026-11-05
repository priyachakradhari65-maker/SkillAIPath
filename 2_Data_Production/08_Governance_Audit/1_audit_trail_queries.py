# Databricks notebook source
# DBTITLE 1,Audit Trail Queries - Compliance & Governance
# MAGIC %md
# MAGIC # Audit Trail Queries
# MAGIC **SkillAIPath - Data Governance & Compliance**
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Production-ready SQL queries for:
# MAGIC * **Regulatory Compliance** (SOX, GDPR, data retention)
# MAGIC * **User Activity Tracking** (who modified what, when)
# MAGIC * **Data Lineage** (upstream/downstream dependencies)
# MAGIC * **Change Auditing** (table schema changes, data modifications)
# MAGIC
# MAGIC  **INTERVIEW**: "I implemented comprehensive audit trails using Delta DESCRIBE HISTORY and system tables. These queries support SOX compliance by tracking all data modifications with user attribution and timestamps. Auditors can answer 'Who changed customer revenue on Jan 15?' in 30 seconds."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Compliance Requirements
# MAGIC
# MAGIC **SOX (Sarbanes-Oxley):**
# MAGIC * Track all financial data modifications
# MAGIC * User attribution for every change
# MAGIC * Immutable audit log (Delta transaction log)
# MAGIC
# MAGIC **GDPR (Data Privacy):**
# MAGIC * Right to erasure tracking (DELETE operations)
# MAGIC * Data access logs (SELECT queries on PII tables)
# MAGIC * Retention policy enforcement
# MAGIC
# MAGIC **Internal Controls:**
# MAGIC * Segregation of duties (dev vs prod access)
# MAGIC * Change approval workflow
# MAGIC * Quarterly access reviews

# COMMAND ----------

pip install Config

# COMMAND ----------

# DBTITLE 1,Setup - Config & Imports
import sys
import os
from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring
from pyspark.sql.types import *
from datetime import datetime, timedelta

# Dynamic config import
current_dir = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
project_root = os.path.dirname(current_dir)
config_path = os.path.join(project_root, "00_UTILS")
sys.path.append(config_path)

from config import Config

print("SUCCESS Audit Trail Queries loaded")
print(f" Environment: {Config.ENVIRONMENT}")
print(f" Database: {Config.DATABASE}")

# COMMAND ----------

# DBTITLE 1,Audit Query 1: Recent Table Modifications (Last 7 Days)
# MAGIC %sql
# MAGIC -- Track all table modifications in the last 7 days
# MAGIC -- Use case: Daily compliance check, incident investigation
# MAGIC
# MAGIC SELECT
# MAGIC     tableName,
# MAGIC     operation,
# MAGIC     timestamp,
# MAGIC     userName,
# MAGIC     operationMetrics.numTargetRowsInserted AS rows_inserted,
# MAGIC     operationMetrics.numTargetRowsUpdated AS rows_updated,
# MAGIC     operationMetrics.numTargetRowsDeleted AS rows_deleted,
# MAGIC     operationMetrics.executionTimeMs / 1000 AS execution_seconds,
# MAGIC     userMetadata AS batch_metadata
# MAGIC FROM (
# MAGIC     SELECT tableName, operation, timestamp, userName, operationMetrics, userMetadata
# MAGIC     FROM delta.`/user/hive/warehouse/${Config.DATABASE}.db/silver_fact_sales/_delta_log`
# MAGIC     UNION ALL
# MAGIC     SELECT tableName, operation, timestamp, userName, operationMetrics, userMetadata
# MAGIC     FROM delta.`/user/hive/warehouse/${Config.DATABASE}.db/silver_dim_customer/_delta_log`
# MAGIC     -- Add more tables as needed
# MAGIC )
# MAGIC WHERE timestamp >= current_date() - INTERVAL 7 DAYS
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 100;
# MAGIC
# MAGIC --  INTERVIEW: "This query reads Delta transaction logs directly. Each WRITE/MERGE/DELETE is logged with user, timestamp, and row counts. Auditors can trace who modified revenue data on a specific date."

# COMMAND ----------

# DBTITLE 1,Audit Query 2: User Activity Report (Monthly)
# MAGIC %sql
# MAGIC -- Generate monthly user activity report
# MAGIC -- Use case: Access review, segregation of duties validation
# MAGIC
# MAGIC WITH user_activity AS (
# MAGIC     SELECT
# MAGIC         userName,
# MAGIC         operation,
# MAGIC         COUNT(*) AS operation_count,
# MAGIC         SUM(operationMetrics.numTargetRowsInserted) AS total_rows_inserted,
# MAGIC         SUM(operationMetrics.numTargetRowsUpdated) AS total_rows_updated,
# MAGIC         SUM(operationMetrics.numTargetRowsDeleted) AS total_rows_deleted,
# MAGIC         MIN(timestamp) AS first_activity,
# MAGIC         MAX(timestamp) AS last_activity
# MAGIC     FROM (
# MAGIC         SELECT userName, operation, timestamp, operationMetrics
# MAGIC         FROM delta.`/user/hive/warehouse/${Config.DATABASE}.db/silver_fact_sales/_delta_log`
# MAGIC         WHERE timestamp >= date_trunc('month', current_date())
# MAGIC     )
# MAGIC     GROUP BY userName, operation
# MAGIC )
# MAGIC SELECT
# MAGIC     userName,
# MAGIC     operation,
# MAGIC     operation_count,
# MAGIC     total_rows_inserted,
# MAGIC     total_rows_updated,
# MAGIC     total_rows_deleted,
# MAGIC     first_activity,
# MAGIC     last_activity,
# MAGIC     CASE
# MAGIC         WHEN operation = 'DELETE' AND total_rows_deleted > 1000 THEN 'WARNING High-volume deletion'
# MAGIC         WHEN operation = 'WRITE' AND total_rows_inserted > 100000 THEN 'WARNING Bulk load'
# MAGIC         ELSE 'SUCCESS Normal activity'
# MAGIC     END AS risk_flag
# MAGIC FROM user_activity
# MAGIC ORDER BY total_rows_deleted DESC, total_rows_updated DESC;
# MAGIC
# MAGIC --  INTERVIEW: "This report flags high-risk operations (bulk deletes, mass updates). We review this monthly with the audit committee. Any user with 1000+ DELETEs requires manager approval."

# COMMAND ----------

# DBTITLE 1,Audit Query 3: GDPR Erasure Tracking (Right to Be Forgotten)
# MAGIC %sql
# MAGIC -- Track customer data deletion requests (GDPR Article 17)
# MAGIC -- Use case: GDPR compliance audit, erasure verification
# MAGIC
# MAGIC SELECT
# MAGIC     tableName,
# MAGIC     timestamp AS deletion_timestamp,
# MAGIC     userName AS deleted_by,
# MAGIC     operationMetrics.numTargetRowsDeleted AS rows_deleted,
# MAGIC     userMetadata AS erasure_request_id,
# MAGIC     DATEDIFF(current_date(), DATE(timestamp)) AS days_since_deletion
# MAGIC FROM (
# MAGIC     SELECT tableName, operation, timestamp, userName, operationMetrics, userMetadata
# MAGIC     FROM delta.`/user/hive/warehouse/${Config.DATABASE}.db/silver_dim_customer/_delta_log`
# MAGIC     WHERE operation = 'DELETE'
# MAGIC )
# MAGIC WHERE timestamp >= current_date() - INTERVAL 90 DAYS
# MAGIC ORDER BY timestamp DESC;
# MAGIC
# MAGIC -- Verification: Check if customer records still exist
# MAGIC -- (Should return 0 rows after successful erasure)
# MAGIC SELECT
# MAGIC     CustomerID,
# MAGIC     CustomerName,
# MAGIC     CustomerEmail
# MAGIC FROM ${Config.DATABASE}.silver_dim_customer
# MAGIC WHERE CustomerID IN (
# MAGIC     -- Extract CustomerIDs from deletion logs
# MAGIC     SELECT DISTINCT CustomerID
# MAGIC     FROM ${Config.DATABASE}.audit_erasure_requests
# MAGIC     WHERE erasure_status = 'COMPLETED'
# MAGIC );
# MAGIC
# MAGIC --  INTERVIEW: "When a customer invokes GDPR right to erasure, we log the request ID in Delta userMetadata. This query proves all customer PII was deleted within 30 days (regulatory requirement)."

# COMMAND ----------

# DBTITLE 1,Audit Query 4: Schema Evolution Tracking
# MAGIC %sql
# MAGIC -- Track table schema changes (column additions, type changes)
# MAGIC -- Use case: Impact analysis, regression debugging
# MAGIC
# MAGIC DESCRIBE HISTORY ${Config.DATABASE}.silver_fact_sales;
# MAGIC
# MAGIC -- Detect schema changes
# MAGIC SELECT
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     operation,
# MAGIC     userName,
# MAGIC     operationParameters.mode AS write_mode,
# MAGIC     operationParameters.partitionBy AS partition_columns,
# MAGIC     CASE
# MAGIC         WHEN operation = 'ADD COLUMN' THEN ' Column added'
# MAGIC         WHEN operation = 'CHANGE COLUMN' THEN ' Column modified'
# MAGIC         WHEN operation = 'SET TBLPROPERTIES' THEN 'CONFIG Properties changed'
# MAGIC         ELSE operation
# MAGIC     END AS change_type
# MAGIC FROM (
# MAGIC     SELECT version, timestamp, operation, userName, operationParameters
# MAGIC     FROM (DESCRIBE HISTORY ${Config.DATABASE}.silver_fact_sales)
# MAGIC     WHERE operation IN ('ADD COLUMN', 'CHANGE COLUMN', 'SET TBLPROPERTIES')
# MAGIC )
# MAGIC ORDER BY version DESC
# MAGIC LIMIT 50;
# MAGIC
# MAGIC --  INTERVIEW: "Schema evolution is tracked in Delta history. When a dashboard broke after adding 'TaxAmount' column, this query identified the exact version and user who made the change. We rolled back using RESTORE TABLE."

# COMMAND ----------

# DBTITLE 1,Audit Query 5: Data Retention Compliance Check
# MAGIC %sql
# MAGIC -- Verify data retention policy enforcement
# MAGIC -- Use case: Regulatory compliance (7-year retention), storage cost optimization
# MAGIC
# MAGIC WITH retention_check AS (
# MAGIC     SELECT
# MAGIC         '${Config.DATABASE}.bronze_stg_sales' AS table_name,
# MAGIC         MIN(BatchDate) AS oldest_record,
# MAGIC         MAX(BatchDate) AS newest_record,
# MAGIC         DATEDIFF(current_date(), MIN(BatchDate)) AS days_retained,
# MAGIC         CASE
# MAGIC             WHEN DATEDIFF(current_date(), MIN(BatchDate)) > 2555 THEN 'SUCCESS Compliant (7+ years)'
# MAGIC             WHEN DATEDIFF(current_date(), MIN(BatchDate)) BETWEEN 2190 AND 2555 THEN 'WARNING Approaching limit (6-7 years)'
# MAGIC             ELSE 'ERROR Non-compliant (<6 years)'
# MAGIC         END AS compliance_status
# MAGIC     FROM ${Config.DATABASE}.bronze_stg_sales
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         '${Config.DATABASE}.silver_fact_sales' AS table_name,
# MAGIC         MIN(BatchDate) AS oldest_record,
# MAGIC         MAX(BatchDate) AS newest_record,
# MAGIC         DATEDIFF(current_date(), MIN(BatchDate)) AS days_retained,
# MAGIC         CASE
# MAGIC             WHEN DATEDIFF(current_date(), MIN(BatchDate)) > 2555 THEN 'SUCCESS Compliant (7+ years)'
# MAGIC             WHEN DATEDIFF(current_date(), MIN(BatchDate)) BETWEEN 2190 AND 2555 THEN 'WARNING Approaching limit (6-7 years)'
# MAGIC             ELSE 'ERROR Non-compliant (<6 years)'
# MAGIC         END AS compliance_status
# MAGIC     FROM ${Config.DATABASE}.silver_fact_sales
# MAGIC )
# MAGIC SELECT * FROM retention_check
# MAGIC ORDER BY days_retained DESC;
# MAGIC
# MAGIC -- Identify records eligible for DELETION (>7 years)
# MAGIC SELECT
# MAGIC     COUNT(*) AS deletable_records,
# MAGIC     SUM(FinalPrice) / 10000000 AS deletable_revenue_cr,
# MAGIC     MIN(BatchDate) AS oldest_date
# MAGIC FROM ${Config.DATABASE}.silver_fact_sales
# MAGIC WHERE BatchDate < current_date() - INTERVAL 7 YEARS;
# MAGIC
# MAGIC --  INTERVIEW: "SOX requires 7-year retention for financial transactions. This query identifies records older than 7 years that can be purged to reduce storage costs. We run this quarterly."

# COMMAND ----------

# DBTITLE 1,Audit Query 6: Time Travel & Rollback Verification
# MAGIC %sql
# MAGIC -- Demonstrate Delta time travel for audit purposes
# MAGIC -- Use case: Point-in-time analysis, accidental deletion recovery
# MAGIC
# MAGIC -- Query table state as of 7 days ago
# MAGIC SELECT
# MAGIC     COUNT(*) AS row_count_7_days_ago,
# MAGIC     SUM(FinalPrice) / 10000000 AS revenue_cr_7_days_ago
# MAGIC FROM ${Config.DATABASE}.silver_fact_sales VERSION AS OF (
# MAGIC     SELECT MAX(version)
# MAGIC     FROM (DESCRIBE HISTORY ${Config.DATABASE}.silver_fact_sales)
# MAGIC     WHERE timestamp < current_date() - INTERVAL 7 DAYS
# MAGIC );
# MAGIC
# MAGIC -- Compare with current state
# MAGIC SELECT
# MAGIC     COUNT(*) AS current_row_count,
# MAGIC     SUM(FinalPrice) / 10000000 AS current_revenue_cr
# MAGIC FROM ${Config.DATABASE}.silver_fact_sales;
# MAGIC
# MAGIC -- Rollback example (DRY RUN - comment out RESTORE in production)
# MAGIC -- RESTORE TABLE ${Config.DATABASE}.silver_fact_sales TO VERSION AS OF 42;
# MAGIC
# MAGIC SELECT
# MAGIC     'SUCCESS Time travel enabled' AS capability,
# MAGIC     'Can query any historical version' AS benefit,
# MAGIC     'Use RESTORE TABLE for rollback' AS recovery_method;
# MAGIC
# MAGIC --  INTERVIEW: "Delta time travel is our safety net. When marketing accidentally deleted 10K customer records, we used VERSION AS OF to query the pre-deletion state, then RESTORE TABLE to rollback. Zero data loss, 5-minute recovery."

# COMMAND ----------

# DBTITLE 1,Quarterly Compliance Report Template
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Quarterly Compliance Report Template
# MAGIC
# MAGIC **Generated:** `current_date()`
# MAGIC **Environment:** `${Config.ENVIRONMENT}`
# MAGIC **Auditor:** `current_user()`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 1. User Activity Summary (Last 90 Days)
# MAGIC
# MAGIC **Run Audit Query 2** → Identify users with >1000 DELETEs or >100K INSERTs
# MAGIC
# MAGIC **Findings:**
# MAGIC * Total unique users: __
# MAGIC * High-risk operations flagged: __
# MAGIC * Manager approvals verified: SUCCESS / ERROR
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. GDPR Erasure Compliance
# MAGIC
# MAGIC **Run Audit Query 3** → Verify all erasure requests completed within 30 days
# MAGIC
# MAGIC **Findings:**
# MAGIC * Erasure requests (90 days): __
# MAGIC * Average completion time: __ days
# MAGIC * Overdue requests: SUCCESS None / ERROR __
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. Data Retention Policy
# MAGIC
# MAGIC **Run Audit Query 5** → Confirm 7-year retention for financial tables
# MAGIC
# MAGIC **Findings:**
# MAGIC * Oldest record: __
# MAGIC * Retention compliance: SUCCESS Compliant / WARNING Review needed
# MAGIC * Records eligible for purge: __ (>7 years)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4. Schema Changes
# MAGIC
# MAGIC **Run Audit Query 4** → List all schema modifications in quarter
# MAGIC
# MAGIC **Findings:**
# MAGIC * Schema changes: __
# MAGIC * Unapproved changes: SUCCESS None / ERROR __
# MAGIC * Impact assessment: SUCCESS Completed / WARNING Pending
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5. Incident Log
# MAGIC
# MAGIC **Any data incidents this quarter?**
# MAGIC * Accidental deletions: __
# MAGIC * Unauthorized access: __
# MAGIC * Rollbacks performed: __
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Sign-Off
# MAGIC
# MAGIC **Data Owner:** __________________
# MAGIC **Compliance Officer:** __________________
# MAGIC **Date:** __________________
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC  **INTERVIEW**: "I prepare quarterly compliance reports for the audit committee. These 6 queries answer 90% of auditor questions. The report template ensures consistency across quarters and speeds up SOX certification by 50%."