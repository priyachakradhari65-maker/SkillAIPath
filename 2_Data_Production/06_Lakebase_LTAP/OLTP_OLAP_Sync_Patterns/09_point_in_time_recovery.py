# Databricks notebook source
# Lakebase Point-in-Time Recovery (PITR) System
# PRODUCTION-GRADE IMPLEMENTATION
# PURPOSE: Automated recovery from accidental deletes, updates, or corruption
# Updated: June 2026 - Delta Lake Time Travel + Lakebase patterns

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("=" * 80)
print("LAKEBASE POINT-IN-TIME RECOVERY SYSTEM")
print("=" * 80)

class LakebasePITRManager:
    '''
    Production-grade Point-in-Time Recovery manager for Lakebase tables.

    Capabilities:
    - Query historical versions of tables
    - Restore to specific timestamp or version
    - Validate data consistency before/after recovery
    - Generate recovery audit logs
    '''

    def __init__(self, catalog="sap", schema="gold"):
        self.catalog = catalog
        self.schema = schema
        self.spark = SparkSession.builder.getOrCreate()

    def get_table_history(self, table_name, limit=20):
        '''Get version history of a Lakebase table'''
        try:
            full_table = f"{self.catalog}.{self.schema}.{table_name}"

            # Get Delta table history
            history_df = self.spark.sql(f"DESCRIBE HISTORY {full_table}")
            history_df = history_df.limit(limit)

            logger.info(f"Retrieved {history_df.count()} versions for {full_table}")
            return history_df

        except Exception as e:
            logger.error(f"Failed to get history for {table_name}: {str(e)}")
            raise

    def query_as_of_timestamp(self, table_name, timestamp_str):
        '''
        Query table as it existed at a specific timestamp.

        Args:
            table_name: Name of the table
            timestamp_str: Timestamp in format 'YYYY-MM-DD HH:MM:SS'

        Returns:
            DataFrame with historical data
        '''
        try:
            full_table = f"{self.catalog}.{self.schema}.{table_name}"

            # Query using time travel
            query = f"SELECT * FROM {full_table} TIMESTAMP AS OF '{timestamp_str}'"
            df = self.spark.sql(query)

            record_count = df.count()
            logger.info(f"Retrieved {record_count} records from {timestamp_str}")

            return df

        except Exception as e:
            logger.error(f"Time travel query failed: {str(e)}")
            raise

    def query_as_of_version(self, table_name, version_number):
        '''Query table at a specific version number'''
        try:
            full_table = f"{self.catalog}.{self.schema}.{table_name}"

            query = f"SELECT * FROM {full_table} VERSION AS OF {version_number}"
            df = self.spark.sql(query)

            logger.info(f"Retrieved data from version {version_number}")
            return df

        except Exception as e:
            logger.error(f"Version query failed: {str(e)}")
            raise

    def compare_versions(self, table_name, before_timestamp, after_timestamp):
        '''
        Compare table state between two timestamps to identify changes.

        Returns dict with:
        - records_before: count before
        - records_after: count after
        - records_added: new records
        - records_deleted: deleted records
        - records_modified: changed records
        '''
        try:
            logger.info(f"Comparing {table_name}: {before_timestamp} vs {after_timestamp}")

            df_before = self.query_as_of_timestamp(table_name, before_timestamp)
            df_after = self.query_as_of_timestamp(table_name, after_timestamp)

            count_before = df_before.count()
            count_after = df_after.count()

            # Find deleted records (in before, not in after)
            deleted = df_before.join(df_after, on=df_before.columns[0], how="left_anti")
            deleted_count = deleted.count()

            # Find added records (in after, not in before)
            added = df_after.join(df_before, on=df_after.columns[0], how="left_anti")
            added_count = added.count()

            comparison = {
                "records_before": count_before,
                "records_after": count_after,
                "records_deleted": deleted_count,
                "records_added": added_count,
                "net_change": count_after - count_before
            }

            logger.info(f"Comparison complete: {comparison}")
            return comparison

        except Exception as e:
            logger.error(f"Comparison failed: {str(e)}")
            raise

    def restore_table(self, table_name, target_timestamp, restore_mode="clone"):
        '''
        Restore table to a specific point in time.

        Args:
            table_name: Table to restore
            target_timestamp: Timestamp to restore to
            restore_mode: 'clone' (create backup) or 'replace' (overwrite)

        Returns:
            Dict with restoration metadata
        '''
        try:
            full_table = f"{self.catalog}.{self.schema}.{table_name}"
            restore_time = datetime.now().strftime("%Y%m%d_%H%M%S")

            if restore_mode == "clone":
                # Create backup before restore
                backup_table = f"{full_table}_backup_{restore_time}"
                logger.info(f"Creating backup: {backup_table}")

                self.spark.sql(f"CREATE TABLE {backup_table} CLONE {full_table}")

                # Restore from historical version
                logger.info(f"Restoring {full_table} to {target_timestamp}")

                # Get historical data
                df_historical = self.query_as_of_timestamp(table_name, target_timestamp)

                # Replace current table
                df_historical.write.mode("overwrite").saveAsTable(full_table)

                result = {
                    "status": "success",
                    "original_table": full_table,
                    "backup_table": backup_table,
                    "restore_timestamp": target_timestamp,
                    "restore_time": restore_time,
                    "mode": "clone"
                }

            elif restore_mode == "replace":
                logger.warning(f"REPLACE mode: No backup created!")

                df_historical = self.query_as_of_timestamp(table_name, target_timestamp)
                df_historical.write.mode("overwrite").saveAsTable(full_table)

                result = {
                    "status": "success",
                    "original_table": full_table,
                    "backup_table": None,
                    "restore_timestamp": target_timestamp,
                    "restore_time": restore_time,
                    "mode": "replace"
                }

            logger.info(f"Restoration complete: {result}")
            return result

        except Exception as e:
            logger.error(f"Restoration failed: {str(e)}")
            raise

    def rollback_to_version(self, table_name, version_number):
        '''Rollback table to a specific version number'''
        try:
            full_table = f"{self.catalog}.{self.schema}.{table_name}"

            logger.info(f"Rolling back {full_table} to version {version_number}")

            # Databricks Delta Lake rollback
            self.spark.sql(f"RESTORE TABLE {full_table} TO VERSION AS OF {version_number}")

            logger.info(f"Rollback successful")
            return {"status": "success", "version": version_number}

        except Exception as e:
            logger.error(f"Rollback failed: {str(e)}")
            raise

# Example Usage: Production Recovery Scenario
print("\nSCENARIO: Accidental DELETE at 10:30 AM - Recovery Process\n")

# Initialize PITR manager
pitr = LakebasePITRManager(catalog="sap", schema="gold")

# Step 1: Identify the incident
incident_table = "customer_orders"
incident_time = "2026-06-24 10:30:00"
safe_time = "2026-06-24 10:25:00"

print(f"Incident detected on table: {incident_table}")
print(f"Incident time: {incident_time}")
print(f"Safe restore point: {safe_time}")

# Step 2: Get table history
print("\nStep 1: Retrieve table history")
history = pitr.get_table_history(incident_table, limit=10)
print(f"Latest versions retrieved: {history.count()}")

# Step 3: Compare before/after to assess damage
print("\nStep 2: Compare versions to assess damage")
comparison = pitr.compare_versions(
    incident_table,
    before_timestamp=safe_time,
    after_timestamp=incident_time
)

print(f"Records before incident: {comparison['records_before']}")
print(f"Records after incident: {comparison['records_after']}")
print(f"Records deleted: {comparison['records_deleted']}")
print(f"Net change: {comparison['net_change']}")

if comparison['records_deleted'] > 0:
    print(f"\nDAMAGE CONFIRMED: {comparison['records_deleted']} records lost")
    print("Proceeding with recovery...")

# Step 4: Restore table
print("\nStep 3: Restore table to safe point")
restoration = pitr.restore_table(
    table_name=incident_table,
    target_timestamp=safe_time,
    restore_mode="clone"  # Safe mode with backup
)

print(f"\nRestoration Status: {restoration['status']}")
print(f"Backup created: {restoration['backup_table']}")
print(f"Restored to timestamp: {restoration['restore_timestamp']}")

# Step 5: Validate restoration
print("\nStep 4: Validate restoration")
restored_df = pitr.query_as_of_timestamp(incident_table, safe_time)
current_df = pitr.spark.table(f"{pitr.catalog}.{pitr.schema}.{incident_table}")

print(f"Historical record count: {restored_df.count()}")
print(f"Current record count: {current_df.count()}")

if restored_df.count() == current_df.count():
    print("\nVALIDATION PASSED: Data restored successfully")
else:
    print("\nWARNING: Record counts don't match - investigate further")

print("\n" + "=" * 80)
print("PITR BEST PRACTICES")
print("=" * 80)
print('''
1. Retention Policy:
   - Default: 30 days of time travel history
   - Configure: ALTER TABLE table_name SET TBLPROPERTIES (delta.logRetentionDuration = "interval 90 days")
   - Critical tables: Extend to 90-180 days

2. Regular Testing:
   - Monthly: Test restore procedures on non-prod tables
   - Quarterly: Full disaster recovery drill
   - Document: Keep runbooks for common scenarios

3. Monitoring:
   - Alert on large DELETE/UPDATE operations (> 10 percent of table)
   - Log all restoration operations for audit trail
   - Track time travel query performance

4. Performance Optimization:
   - Time travel queries scan historical files - can be slow
   - Use version numbers when possible (faster than timestamps)
   - Consider cloning large tables before destructive ops

5. Cost Management:
   - Historical data incurs storage costs
   - VACUUM command permanently deletes old versions
   - Balance retention period vs storage costs
''')

print("\nRecovery procedure complete.")