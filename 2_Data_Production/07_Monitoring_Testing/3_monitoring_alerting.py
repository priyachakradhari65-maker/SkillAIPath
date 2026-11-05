# Databricks notebook source
# DBTITLE 1,Pipeline Monitoring & Alerting
# MAGIC %md
# MAGIC # Pipeline Monitoring & Alerting
# MAGIC **SkillAIPath Data Warehouse - Production Observability**
# MAGIC
# MAGIC ## THE PROBLEM: "How do we know when pipelines fail?"
# MAGIC
# MAGIC **Wednesday Morning - Production Incident**
# MAGIC
# MAGIC **Business Analyst:** "Why is yesterday's sales report empty? Our executive dashboard shows zero revenue."
# MAGIC
# MAGIC **Data Engineer:** "Let me check... The nightly pipeline failed at 3 AM. No one noticed until now."
# MAGIC
# MAGIC **Tech Lead:** "We need monitoring. Health checks after every pipeline run, alerts sent to Slack if anything fails."
# MAGIC
# MAGIC **Engineer:** "What should we monitor? Just failures, or data quality too?"
# MAGIC
# MAGIC **Lead:** "Both. Check row counts, null percentages, referential integrity. Alert if thresholds are breached. Build a pipeline health dashboard."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## THE SOLUTION: Automated Monitoring & Alerting
# MAGIC
# MAGIC **What It Is:**
# MAGIC * Automated health checks after pipeline execution
# MAGIC * Data quality metrics (row counts, nulls, duplicates)
# MAGIC * Performance metrics (execution time, cost, data volume)
# MAGIC * Alert notifications (email, Slack, PagerDuty)
# MAGIC * Monitoring dashboard (pipeline status, SLA compliance)
# MAGIC
# MAGIC **Key Concepts:**
# MAGIC * Health Check: Validation query run after pipeline step
# MAGIC * Alert Threshold: Numeric boundary triggering notification
# MAGIC * SLA (Service Level Agreement): Committed pipeline completion time
# MAGIC * Runbook: Step-by-step guide for responding to alerts
# MAGIC
# MAGIC **When to Use:**
# MAGIC * Production pipelines running unattended
# MAGIC * Critical business reports with daily deadlines
# MAGIC * Multi-step workflows (failure cascades)
# MAGIC * Compliance requirements (audit trail)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC * Understanding of SkillAIPath pipeline structure
# MAGIC * Databricks Jobs configured (Module 06_ORCHESTRATION)
# MAGIC * Unity Catalog access (read system tables)
# MAGIC * Slack webhook URL (for Slack alerts) or SMTP credentials (for email)

# COMMAND ----------

# DBTITLE 1,Config - Load Environment
# Load configuration from 1_Setup/1_config

%run ./config

print("=" * 70)
print("MONITORING CONFIGURATION")
print("=" * 70)
print()
print(f"Environment: {environment}")
print(f"Catalog: {catalog_name}")
print(f"Bronze Schema: {bronze_schema}")
print(f"Silver Schema: {silver_schema}")
print(f"Gold Schema: {gold_schema}")
print()
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Health Check - Row Count Validation
# Health Check 1: Validate row counts across layers

from pyspark.sql.functions import count, current_timestamp

print("=" * 70)
print("HEALTH CHECK: ROW COUNT VALIDATION")
print("=" * 70)
print()

# Define expected row count ranges (configure per table)
expected_counts = {
    f"{catalog_name}.{silver_schema}.dim_customer": {"min": 1000, "max": 50000},
    f"{catalog_name}.{silver_schema}.dim_product": {"min": 100, "max": 10000},
    f"{catalog_name}.{silver_schema}.dim_store": {"min": 10, "max": 100},
    f"{catalog_name}.{gold_schema}.mart_daily_sales": {"min": 100, "max": 100000},
    f"{catalog_name}.{gold_schema}.mart_product_performance": {"min": 50, "max": 10000}
}

health_check_results = []

for table_name, thresholds in expected_counts.items():
    try:
        # Get actual row count
        actual_count = spark.table(table_name).count()

        # Validate against thresholds
        status = "PASS"
        if actual_count < thresholds["min"]:
            status = "FAIL - Below minimum"
        elif actual_count > thresholds["max"]:
            status = "WARN - Above maximum"

        health_check_results.append({
            "table": table_name,
            "actual_count": actual_count,
            "min_expected": thresholds["min"],
            "max_expected": thresholds["max"],
            "status": status,
            "check_time": str(current_timestamp())
        })

        print(f"Table: {table_name}")
        print(f"  Actual: {actual_count:,} rows")
        print(f"  Expected: {thresholds['min']:,} - {thresholds['max']:,}")
        print(f"  Status: {status}")
        print()

    except Exception as e:
        health_check_results.append({
            "table": table_name,
            "actual_count": None,
            "min_expected": thresholds["min"],
            "max_expected": thresholds["max"],
            "status": f"ERROR - {str(e)}",
            "check_time": str(current_timestamp())
        })
        print(f"Table: {table_name}")
        print(f"  Status: ERROR - {e}")
        print()

# Summary
failed_checks = [r for r in health_check_results if "FAIL" in r["status"] or "ERROR" in r["status"]]
warning_checks = [r for r in health_check_results if "WARN" in r["status"]]

print("=" * 70)
print(f"Total Checks: {len(health_check_results)}")
print(f"Passed: {len([r for r in health_check_results if r['status'] == 'PASS'])}")
print(f"Warnings: {len(warning_checks)}")
print(f"Failed: {len(failed_checks)}")
print("=" * 70)

if failed_checks:
    print()
    print("ALERT: The following checks FAILED:")
    for check in failed_checks:
        print(f"  - {check['table']}: {check['status']}")

# COMMAND ----------

# DBTITLE 1,Health Check - Data Quality Metrics
# Health Check 2: Data quality metrics (nulls, duplicates)

from pyspark.sql.functions import col, count, sum as _sum, when

print("=" * 70)
print("HEALTH CHECK: DATA QUALITY METRICS")
print("=" * 70)
print()

# Define critical columns that should not have NULLs
critical_null_checks = [
    (f"{catalog_name}.{silver_schema}.dim_customer", "CustomerSK"),
    (f"{catalog_name}.{silver_schema}.dim_customer", "CustomerID"),
    (f"{catalog_name}.{silver_schema}.dim_product", "ProductSK"),
    (f"{catalog_name}.{silver_schema}.dim_product", "ProductID"),
    (f"{catalog_name}.{gold_schema}.mart_daily_sales", "SalesDate"),
    (f"{catalog_name}.{gold_schema}.mart_daily_sales", "TotalRevenue")
]

quality_results = []

for table_name, column_name in critical_null_checks:
    try:
        df = spark.table(table_name)
        total_rows = df.count()

        # Count NULLs in critical column
        null_count = df.filter(col(column_name).isNull()).count()
        null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0

        # Status
        status = "PASS"
        if null_count > 0:
            status = "FAIL - NULLs detected"

        quality_results.append({
            "table": table_name,
            "column": column_name,
            "null_count": null_count,
            "null_percentage": round(null_percentage, 2),
            "status": status
        })

        print(f"{table_name}.{column_name}")
        print(f"  NULLs: {null_count} ({null_percentage:.2f}%)")
        print(f"  Status: {status}")
        print()

    except Exception as e:
        quality_results.append({
            "table": table_name,
            "column": column_name,
            "null_count": None,
            "null_percentage": None,
            "status": f"ERROR - {str(e)}"
        })
        print(f"{table_name}.{column_name}")
        print(f"  Status: ERROR - {e}")
        print()

# Check for duplicates in dimension surrogate keys
print("=" * 70)
print("DUPLICATE KEY CHECKS")
print("=" * 70)
print()

duplicate_checks = [
    (f"{catalog_name}.{silver_schema}.dim_customer", "CustomerSK"),
    (f"{catalog_name}.{silver_schema}.dim_product", "ProductSK"),
    (f"{catalog_name}.{silver_schema}.dim_store", "StoreSK")
]

for table_name, key_column in duplicate_checks:
    try:
        df = spark.table(table_name)

        # Find duplicates
        duplicate_count = df.groupBy(key_column).count().filter(col("count") > 1).count()

        status = "PASS" if duplicate_count == 0 else f"FAIL - {duplicate_count} duplicate keys"

        print(f"{table_name}.{key_column}")
        print(f"  Duplicates: {duplicate_count}")
        print(f"  Status: {status}")
        print()

    except Exception as e:
        print(f"{table_name}.{key_column}")
        print(f"  Status: ERROR - {e}")
        print()

print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Health Check - Referential Integrity
# Health Check 3: Referential integrity (orphan records)

print("=" * 70)
print("HEALTH CHECK: REFERENTIAL INTEGRITY")
print("=" * 70)
print()

# Check if fact table foreign keys exist in dimension tables

# Example: fact_sales should reference existing customers
try:
    # Assuming fact_sales exists with CustomerSK foreign key
    fact_table = f"{catalog_name}.{silver_schema}.fact_sales"
    dim_table = f"{catalog_name}.{silver_schema}.dim_customer"

    fact_df = spark.table(fact_table).select("CustomerSK").distinct()
    dim_df = spark.table(dim_table).select("CustomerSK")

    # Left anti join to find orphans
    orphans = fact_df.join(dim_df, "CustomerSK", "left_anti")
    orphan_count = orphans.count()

    status = "PASS" if orphan_count == 0 else f"FAIL - {orphan_count} orphan records"

    print(f"fact_sales -> dim_customer (CustomerSK)")
    print(f"  Orphan records: {orphan_count}")
    print(f"  Status: {status}")
    print()

except Exception as e:
    print(f"fact_sales -> dim_customer (CustomerSK)")
    print(f"  Status: SKIPPED - {e}")
    print()

# Check ProductSK
try:
    fact_table = f"{catalog_name}.{silver_schema}.fact_sales"
    dim_table = f"{catalog_name}.{silver_schema}.dim_product"

    fact_df = spark.table(fact_table).select("ProductSK").distinct()
    dim_df = spark.table(dim_table).select("ProductSK")

    orphans = fact_df.join(dim_df, "ProductSK", "left_anti")
    orphan_count = orphans.count()

    status = "PASS" if orphan_count == 0 else f"FAIL - {orphan_count} orphan records"

    print(f"fact_sales -> dim_product (ProductSK)")
    print(f"  Orphan records: {orphan_count}")
    print(f"  Status: {status}")
    print()

except Exception as e:
    print(f"fact_sales -> dim_product (ProductSK)")
    print(f"  Status: SKIPPED - {e}")
    print()

print("=" * 70)
print("Referential integrity checks complete.")
print("If any orphans detected, investigate fact table load logic.")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Performance Metrics - Pipeline Execution Time
# MAGIC %sql
# MAGIC -- Performance Monitoring: Query execution history from system tables
# MAGIC
# MAGIC SELECT
# MAGIC   workflow_name,
# MAGIC   task_name,
# MAGIC   start_time,
# MAGIC   end_time,
# MAGIC   TIMESTAMPDIFF(MINUTE, start_time, end_time) AS duration_minutes,
# MAGIC   state,
# MAGIC   result_state
# MAGIC FROM system.lakeflow.task_runs
# MAGIC WHERE workflow_name LIKE '%sap%'
# MAGIC   AND start_time >= CURRENT_DATE() - INTERVAL 7 DAYS
# MAGIC ORDER BY start_time DESC
# MAGIC LIMIT 50;
# MAGIC
# MAGIC -- Note: This query requires access to system.lakeflow tables (Databricks Jobs)
# MAGIC -- If not available, skip this cell or use Databricks Jobs API via REST

# COMMAND ----------

# DBTITLE 1,Alert Function - Slack Notification
# Alert Function: Send notification to Slack

import requests
import json

def send_slack_alert(webhook_url, alert_title, alert_message, severity="ERROR"):
    """
    Send alert to Slack channel via webhook

    Args:
        webhook_url: Slack webhook URL (store in secrets)
        alert_title: Alert header (e.g., "Pipeline Failure")
        alert_message: Detailed message
        severity: ERROR, WARNING, INFO

    Returns:
        True if sent successfully, False otherwise
    """

    # Color coding
    colors = {
        "ERROR": "#FF0000",
        "WARNING": "#FFA500",
        "INFO": "#0000FF"
    }

    payload = {
        "attachments": [
            {
                "color": colors.get(severity, "#808080"),
                "title": alert_title,
                "text": alert_message,
                "fields": [
                    {"title": "Severity", "value": severity, "short": True},
                    {"title": "Environment", "value": environment, "short": True}
                ],
                "footer": "SkillAIPath Pipeline Monitor",
                "ts": int(spark.sql("SELECT unix_timestamp()").collect()[0][0])
            }
        ]
    }

    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        if response.status_code == 200:
            print(f"Slack alert sent: {alert_title}")
            return True
        else:
            print(f"Failed to send Slack alert: HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"Error sending Slack alert: {e}")
        return False

print("=" * 70)
print("SLACK ALERT FUNCTION LOADED")
print("=" * 70)
print()
print("Usage:")
print("  webhook_url = dbutils.secrets.get(scope='sap-prod', key='slack_webhook')")
print("  send_slack_alert(webhook_url, 'Pipeline Failure', 'dim_customer load failed', 'ERROR')")
print()
print("Setup:")
print("  1. Create Slack incoming webhook: https://api.slack.com/messaging/webhooks")
print("  2. Store webhook URL in Databricks Secrets")
print("  3. Call this function when health checks fail")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Alert Function - Email Notification
# Alert Function: Send email notification

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_email_alert(to_emails, subject, body, smtp_config):
    """
    Send email alert via SMTP

    Args:
        to_emails: List of recipient emails
        subject: Email subject line
        body: Email body (plain text or HTML)
        smtp_config: Dict with keys: host, port, username, password, from_email

    Returns:
        True if sent successfully, False otherwise
    """

    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = smtp_config['from_email']
        msg['To'] = ', '.join(to_emails)
        msg['Subject'] = subject

        msg.attach(MIMEText(body, 'plain'))

        # Connect to SMTP server
        server = smtplib.SMTP(smtp_config['host'], smtp_config['port'])
        server.starttls()
        server.login(smtp_config['username'], smtp_config['password'])

        # Send email
        server.send_message(msg)
        server.quit()

        print(f"Email alert sent to: {', '.join(to_emails)}")
        return True

    except Exception as e:
        print(f"Error sending email alert: {e}")
        return False

print("=" * 70)
print("EMAIL ALERT FUNCTION LOADED")
print("=" * 70)
print()
print("Usage:")
print("  smtp_config = {")
print("      'host': 'smtp.gmail.com',")
print("      'port': 587,")
print("      'username': dbutils.secrets.get('sap-prod', 'smtp_user'),")
print("      'password': dbutils.secrets.get('sap-prod', 'smtp_pass'),")
print("      'from_email': 'alerts@sap.com'")
print("  }")
print("  send_email_alert(['team@company.com'], 'Pipeline Alert', 'Details...', smtp_config)")
print()
print("Setup:")
print("  1. Configure SMTP credentials (Gmail, SendGrid, AWS SES)")
print("  2. Store credentials in Databricks Secrets")
print("  3. Call this function when health checks fail")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Monitoring Dashboard Query
# MAGIC %sql
# MAGIC -- Monitoring Dashboard: Pipeline health summary
# MAGIC
# MAGIC -- This query can be saved as a Databricks SQL query and added to a dashboard
# MAGIC
# MAGIC WITH latest_runs AS (
# MAGIC   SELECT
# MAGIC     'dim_customer' AS table_name,
# MAGIC     COUNT(*) AS row_count,
# MAGIC     MAX(UpdatedAt) AS last_updated
# MAGIC   FROM {Config.SCHEMA_SILVER}.dim_customer
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   SELECT
# MAGIC     'dim_product' AS table_name,
# MAGIC     COUNT(*) AS row_count,
# MAGIC     MAX(UpdatedAt) AS last_updated
# MAGIC   FROM {Config.SCHEMA_SILVER}.dim_product
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   SELECT
# MAGIC     'mart_daily_sales' AS table_name,
# MAGIC     COUNT(*) AS row_count,
# MAGIC     MAX(UpdatedAt) AS last_updated
# MAGIC   FROM {Config.SCHEMA_GOLD}.mart_daily_sales
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   table_name,
# MAGIC   row_count,
# MAGIC   last_updated,
# MAGIC   CASE
# MAGIC     WHEN last_updated >= CURRENT_TIMESTAMP() - INTERVAL 1 DAY THEN 'HEALTHY'
# MAGIC     WHEN last_updated >= CURRENT_TIMESTAMP() - INTERVAL 2 DAYS THEN 'STALE'
# MAGIC     ELSE 'CRITICAL'
# MAGIC   END AS health_status,
# MAGIC   TIMESTAMPDIFF(HOUR, last_updated, CURRENT_TIMESTAMP()) AS hours_since_update
# MAGIC FROM latest_runs
# MAGIC ORDER BY last_updated DESC;
# MAGIC
# MAGIC -- Add this query to a Databricks SQL Dashboard
# MAGIC -- Refresh every 15 minutes for real-time monitoring

# COMMAND ----------

# DBTITLE 1,SLA Monitoring - Pipeline Completion Times
# SLA Monitoring: Track pipeline execution against SLA targets

from datetime import datetime, timedelta

print("=" * 70)
print("SLA MONITORING - PIPELINE COMPLETION TIMES")
print("=" * 70)
print()

# Define SLA targets (configure per pipeline)
sla_targets = {
    "Daily Dimension Load": {
        "target_completion_time": "06:00",  # Must complete by 6 AM
        "max_duration_minutes": 30
    },
    "Daily Fact Load": {
        "target_completion_time": "07:00",  # Must complete by 7 AM
        "max_duration_minutes": 60
    },
    "Gold Mart Refresh": {
        "target_completion_time": "08:00",  # Must complete by 8 AM
        "max_duration_minutes": 45
    }
}

print("SLA Targets:")
print("-" * 70)
for pipeline, sla in sla_targets.items():
    print(f"{pipeline}:")
    print(f"  Target Completion: {sla['target_completion_time']}")
    print(f"  Max Duration: {sla['max_duration_minutes']} minutes")
    print()

print("=" * 70)
print()
print("SLA Monitoring Logic:")
print("-" * 70)
print("1. Track job start and end times (from Databricks Jobs API)")
print("2. Compare actual completion time vs SLA target")
print("3. Alert if completion time exceeds target OR duration exceeds max")
print("4. Calculate SLA breach percentage (monthly metric)")
print()
print("Implementation:")
print("- Query system.lakeflow.task_runs table for execution times")
print("- Store results in monitoring table for trend analysis")
print("- Trigger alerts via send_slack_alert() or send_email_alert()")
print("=" * 70)
print()
print("Example Alert Trigger:")
print("-" * 70)
print("if actual_completion_time > target_completion_time:")
print("    send_slack_alert(")
print("        webhook_url,")
print("        'SLA Breach: Daily Dimension Load',")
print("        f'Completed at {actual_time}, target was {target_time}',")
print("        'WARNING'")
print("    )")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Summary - Monitoring Best Practices
# Monitoring & Alerting Best Practices

print("=" * 70)
print("MONITORING & ALERTING - PRODUCTION CHECKLIST")
print("=" * 70)
print()

checklist = [
    ("Health checks run after every pipeline execution", "Automated validation"),
    ("Row count validation against expected ranges", "Detect missing/duplicate data"),
    ("NULL checks on critical columns (primary/foreign keys)", "Data quality enforcement"),
    ("Duplicate key detection in dimension tables", "Prevent surrogate key collisions"),
    ("Referential integrity checks (orphan records)", "Validate fact-dimension joins"),
    ("Performance metrics tracked (execution time, cost)", "Optimize slow pipelines"),
    ("SLA monitoring configured (completion time targets)", "Meet business deadlines"),
    ("Alert channels configured (Slack, email, PagerDuty)", "Immediate incident notification"),
    ("Monitoring dashboard created (pipeline health summary)", "Real-time observability"),
    ("Runbooks documented (response procedures for alerts)", "Standardized incident response")
]

for i, (item, detail) in enumerate(checklist, 1):
    print(f"{i:2}. {item}")
    print(f"    {detail}")
    print()

print("=" * 70)
print("ALERT SEVERITY LEVELS")
print("=" * 70)
print()
print("ERROR (Immediate Action Required):")
print("  - Pipeline failure (job crashed)")
print("  - Zero rows loaded (data source unavailable)")
print("  - Referential integrity breach (orphan records)")
print("  - SLA breach (completion time exceeded by >30 minutes)")
print()
print("WARNING (Review Within 1 Hour):")
print("  - Row count below minimum threshold")
print("  - Duplicate keys detected in dimension table")
print("  - Performance degradation (execution time >2x normal)")
print("  - Minor SLA breach (5-30 minutes late)")
print()
print("INFO (Informational):")
print("  - Pipeline completed successfully")
print("  - Row count within expected range")
print("  - All health checks passed")
print("=" * 70)
print()
print("Next Steps:")
print("  1. Configure Slack webhook and store in Databricks Secrets")
print("  2. Add health check notebook as final task in orchestration job")
print("  3. Create monitoring dashboard from provided SQL queries")
print("  4. Document runbooks for each alert type")
print("  5. Test alert delivery (send test notification to Slack)")
print("  6. Schedule phase-based SLA review meetings with SkillAIPath Developers")
print("=" * 70)
print()
print("Interview Talking Point:")
print("-" * 70)
print('"We monitor pipeline health via automated checks after every run.')
print(' Row counts, NULL percentages, and referential integrity are validated.')
print(' If thresholds are breached, alerts are sent to Slack immediately.')
print(' We track SLA compliance (pipelines must complete by 8 AM daily) and')
print(' maintain a monitoring dashboard showing real-time pipeline status.')
print(' This proactive monitoring reduces MTTR (mean time to resolution) from')
print(' hours to minutes."')
print("=" * 70)