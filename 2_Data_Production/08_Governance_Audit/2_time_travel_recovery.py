# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Delta Lake Time Travel & Recovery
# MAGIC **SkillAIPath Utilities - Version Control & Disaster Recovery**
# MAGIC
# MAGIC ## Purpose
# MAGIC Query historical versions of Delta tables for:
# MAGIC - Disaster recovery (accidental deletes/updates)
# MAGIC - Audit compliance (regulatory requirements)
# MAGIC - Data lineage investigation
# MAGIC - Performance troubleshooting
# MAGIC
# MAGIC ## Delta Time Travel Features
# MAGIC - VERSION AS OF: Query table at specific version number
# MAGIC - TIMESTAMP AS OF: Query table at specific datetime
# MAGIC - DESCRIBE HISTORY: View all table versions
# MAGIC - RESTORE TABLE: Rollback to previous version
# MAGIC
# MAGIC ## Retention
# MAGIC - Default: 30 days of version history
# MAGIC - Configurable via delta.logRetentionDuration

# COMMAND ----------

# Configuration
import sys
import os
from datetime import datetime, timedelta

current_dir = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
sys.path.append(current_dir)
from config import Config

print("="*70)
print("DELTA LAKE TIME TRAVEL & RECOVERY")
print("="*70)
print(f"Catalog: {Config.CATALOG}")
print(f"Environment: {Config.ENVIRONMENT}")
print()

# Widgets for interactive queries
dbutils.widgets.dropdown("table_layer", "silver", ["bronze", "silver", "gold"], "Layer")
dbutils.widgets.text("table_name", "fact_sales", "Table Name")
dbutils.widgets.text("version_number", "0", "Version (0=latest)")
dbutils.widgets.text("days_ago", "1", "Days Ago")

table_layer = dbutils.widgets.get("table_layer")
table_name = dbutils.widgets.get("table_name")
version = int(dbutils.widgets.get("version_number"))
days_ago = int(dbutils.widgets.get("days_ago"))

full_table = f"{Config.CATALOG}.{table_layer}.{table_name}"
print(f"Target Table: {full_table}")
print("="*70)

# COMMAND ----------

# 1. VIEW TABLE HISTORY
print()
print("TABLE VERSION HISTORY")
print("-"*70)
print()

try:
    history_df = spark.sql(f"DESCRIBE HISTORY {full_table}")

    # Show recent versions
    print(f"Showing last 10 versions of {full_table}:")
    print()

    display(history_df.select(
        "version",
        "timestamp",
        "operation",
        "operationMetrics.numOutputRows",
        "operationMetrics.numAddedFiles",
        "operationMetrics.numRemovedFiles"
    ).limit(10))

    latest_version = history_df.first()['version']
    oldest_version = history_df.orderBy("version").first()['version']

    print()
    print(f"Latest Version: {latest_version}")
    print(f"Oldest Available: {oldest_version}")
    print(f"Total Versions: {history_df.count()}")

except Exception as e:
    print(f"Error reading history: {str(e)}")
    print("Table may not exist or may not be a Delta table")

# COMMAND ----------

# 2. QUERY SPECIFIC VERSION
print()
print("QUERY BY VERSION NUMBER")
print("-"*70)
print()

if version > 0:
    try:
        version_df = spark.sql(f"""
        SELECT * FROM {full_table} VERSION AS OF {version}
        LIMIT 10
        """)

        print(f"Sample data from version {version}:")
        print()
        display(version_df)

        row_count = spark.sql(f"""
        SELECT COUNT(*) as row_count
        FROM {full_table} VERSION AS OF {version}
        """).first()['row_count']

        print()
        print(f"Total rows in version {version}: {row_count:,}")

    except Exception as e:
        print(f"Error: {str(e)}")
        print(f"Version {version} may not exist for this table")
else:
    print("Set version_number widget to query a specific version")
    print("Use the DESCRIBE HISTORY results above to find available versions")

# COMMAND ----------

# 3. QUERY BY TIMESTAMP
print()
print("QUERY BY TIMESTAMP")
print("-"*70)
print()

if days_ago > 0:
    target_date = datetime.now() - timedelta(days=days_ago)
    timestamp_str = target_date.strftime("%Y-%m-%d %H:%M:%S")

    try:
        timestamp_df = spark.sql(f"""
        SELECT * FROM {full_table}
        TIMESTAMP AS OF '{timestamp_str}'
        LIMIT 10
        """)

        print(f"Sample data as of {timestamp_str}:")
        print(f"({days_ago} days ago)")
        print()
        display(timestamp_df)

        row_count = spark.sql(f"""
        SELECT COUNT(*) as row_count
        FROM {full_table}
        TIMESTAMP AS OF '{timestamp_str}'
        """).first()['row_count']

        print()
        print(f"Total rows as of {timestamp_str}: {row_count:,}")

    except Exception as e:
        print(f"Error: {str(e)}")
        print(f"No version available for timestamp {timestamp_str}")
        print("Data may not have existed at that point or retention period expired")
else:
    print("Set days_ago widget to query data from past")

# COMMAND ----------

# 4. COMPARE VERSIONS
print()
print("VERSION COMPARISON")
print("-"*70)
print()

try:
    # Current count
    current_count = spark.table(full_table).count()

    # Count from X days ago
    if days_ago > 0:
        target_date = datetime.now() - timedelta(days=days_ago)
        timestamp_str = target_date.strftime("%Y-%m-%d %H:%M:%S")

        historical_count = spark.sql(f"""
        SELECT COUNT(*) as cnt
        FROM {full_table}
        TIMESTAMP AS OF '{timestamp_str}'
        """).first()['cnt']

        delta = current_count - historical_count
        pct_change = (delta / historical_count * 100) if historical_count > 0 else 0

        print(f"Current row count: {current_count:,}")
        print(f"Count {days_ago} days ago: {historical_count:,}")
        print(f"Change: {delta:+,} rows ({pct_change:+.2f}%)")
        print()

        if delta > 0:
            print(f"Table has GROWN by {delta:,} rows")
        elif delta < 0:
            print(f"Table has SHRUNK by {abs(delta):,} rows")
        else:
            print("Table size unchanged")
    else:
        print(f"Current row count: {current_count:,}")
        print("Set days_ago > 0 to compare with historical data")

except Exception as e:
    print(f"Error during comparison: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## RESTORE TABLE (Manual Execution)
# MAGIC
# MAGIC **CAUTION:** Use only for disaster recovery
# MAGIC
# MAGIC To restore table to previous version:
# MAGIC
# MAGIC ```sql
# MAGIC -- Restore to specific version
# MAGIC RESTORE TABLE {Config.SCHEMA_SILVER}.fact_sales TO VERSION AS OF 42
# MAGIC
# MAGIC -- Restore to timestamp
# MAGIC RESTORE TABLE {Config.SCHEMA_SILVER}.fact_sales TO TIMESTAMP AS OF '2026-06-01 12:00:00'
# MAGIC ```
# MAGIC
# MAGIC **Best Practice:**
# MAGIC 1. Query historical version first (cells above)
# MAGIC 2. Verify data integrity
# MAGIC 3. Create backup of current state
# MAGIC 4. Execute RESTORE
# MAGIC 5. Validate restored data
# MAGIC
# MAGIC **Integration with Reset Pipeline:**
# MAGIC
# MAGIC 06_ORCHESTRATION/00_reset_pipeline creates restore points before truncation.
# MAGIC Use this notebook to recover if needed after reset.

# COMMAND ----------

# 5. RECOVERY FROM RESET PIPELINE
print()
print("RESET PIPELINE RECOVERY")
print("-"*70)
print()

audit_table = f"{Config.CATALOG}.bronze.reset_audit"

try:
    # Check if reset audit table exists
    audit_df = spark.sql(f"""
    SELECT DISTINCT
        reset_id,
        MAX(reset_timestamp) as reset_time,
        COUNT(*) as tables_truncated
    FROM {audit_table}
    GROUP BY reset_id
    ORDER BY reset_time DESC
    LIMIT 5
    """)

    if audit_df.count() > 0:
        print("Recent reset operations:")
        print()
        display(audit_df)

        print()
        print("To recover from a reset:")
        print("1. Find the reset_timestamp from table above")
        print("2. Use TIMESTAMP AS OF query with timestamp BEFORE reset")
        print("3. Restore specific tables as needed")
    else:
        print("No reset operations found in audit table")

except Exception as e:
    print(f"Reset audit table not found: {str(e)}")
    print("Run 06_ORCHESTRATION/00_reset_pipeline to enable recovery tracking")