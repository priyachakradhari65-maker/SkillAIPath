# Databricks notebook source
# Lakebase Automated Compaction System
# PRODUCTION-GRADE IMPLEMENTATION
# PURPOSE: Optimize table performance via automated file compaction and optimization
# Updated: June 2026 - Delta Lake OPTIMIZE + Liquid Clustering patterns

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("=" * 80)
print("LAKEBASE AUTOMATED COMPACTION SYSTEM")
print("=" * 80)

class LakebaseCompactionManager:
    '''
    Production-grade automated compaction manager for Lakebase tables.

    Capabilities:
    - Analyze table file structure and fragmentation
    - Auto-trigger OPTIMIZE when needed
    - Z-ORDER by high-cardinality columns
    - Liquid Clustering for write-optimized tables
    - VACUUM old versions to reclaim storage
    - Track compaction history and performance gains
    '''

    def __init__(self, catalog="sap", schema="gold"):
        self.catalog = catalog
        self.schema = schema
        self.spark = SparkSession.builder.getOrCreate()

    def analyze_table_fragmentation(self, table_name):
        '''
        Analyze table file structure to determine if compaction is needed.

        Returns:
            Dict with fragmentation metrics
        '''
        try:
            full_table = f"{self.catalog}.{self.schema}.{table_name}"

            logger.info(f"Analyzing fragmentation: {full_table}")

            # Get table details
            table_details = self.spark.sql(f"DESCRIBE DETAIL {full_table}").first()

            num_files = table_details["numFiles"]
            size_in_bytes = table_details["sizeInBytes"]
            avg_file_size_mb = (size_in_bytes / num_files / 1024 / 1024) if num_files > 0 else 0

            # Get record count
            record_count = self.spark.table(full_table).count()

            # Determine if compaction is needed
            needs_compaction = False
            reason = []

            if num_files > 1000:
                needs_compaction = True
                reason.append(f"Too many files ({num_files} > 1000)")

            if avg_file_size_mb < 10:
                needs_compaction = True
                reason.append(f"Small file size ({avg_file_size_mb:.2f} MB < 10 MB)")

            if num_files > 0 and record_count / num_files < 1000:
                needs_compaction = True
                reason.append("Low records per file")

            fragmentation = {
                "table": table_name,
                "num_files": num_files,
                "total_size_mb": round(size_in_bytes / 1024 / 1024, 2),
                "avg_file_size_mb": round(avg_file_size_mb, 2),
                "record_count": record_count,
                "records_per_file": round(record_count / num_files) if num_files > 0 else 0,
                "needs_compaction": needs_compaction,
                "reasons": reason if needs_compaction else ["Table is well-optimized"]
            }

            logger.info(f"Fragmentation analysis: {fragmentation}")
            return fragmentation

        except Exception as e:
            logger.error(f"Fragmentation analysis failed: {str(e)}")
            raise

    def optimize_table(self, table_name, z_order_columns=None):
        '''
        Run OPTIMIZE on a table to compact small files.

        Args:
            table_name: Table to optimize
            z_order_columns: List of columns for Z-ORDER (improves query performance)

        Returns:
            Dict with optimization results
        '''
        try:
            full_table = f"{self.catalog}.{self.schema}.{table_name}"

            logger.info(f"Optimizing table: {full_table}")

            # Get metrics before optimization
            before_metrics = self.analyze_table_fragmentation(table_name)

            # Build OPTIMIZE command
            if z_order_columns:
                z_order_clause = ", ".join(z_order_columns)
                optimize_sql = f"OPTIMIZE {full_table} ZORDER BY ({z_order_clause})"
                logger.info(f"Z-ORDERing by: {z_order_clause}")
            else:
                optimize_sql = f"OPTIMIZE {full_table}"

            # Execute OPTIMIZE
            start_time = datetime.now()
            result = self.spark.sql(optimize_sql)
            end_time = datetime.now()
            duration_seconds = (end_time - start_time).total_seconds()

            # Get metrics after optimization
            after_metrics = self.analyze_table_fragmentation(table_name)

            # Calculate improvements
            files_removed = before_metrics["num_files"] - after_metrics["num_files"]
            size_reduction_mb = before_metrics["total_size_mb"] - after_metrics["total_size_mb"]

            optimization_result = {
                "table": table_name,
                "status": "completed",
                "duration_seconds": round(duration_seconds, 2),
                "before_files": before_metrics["num_files"],
                "after_files": after_metrics["num_files"],
                "files_removed": files_removed,
                "before_avg_file_size_mb": before_metrics["avg_file_size_mb"],
                "after_avg_file_size_mb": after_metrics["avg_file_size_mb"],
                "size_reduction_mb": round(size_reduction_mb, 2),
                "z_order_columns": z_order_columns,
                "timestamp": datetime.now().isoformat()
            }

            logger.info(f"Optimization complete: {optimization_result}")
            return optimization_result

        except Exception as e:
            logger.error(f"Optimization failed: {str(e)}")
            raise

    def vacuum_table(self, table_name, retention_hours=168):
        '''
        VACUUM table to permanently delete old file versions.

        Args:
            table_name: Table to vacuum
            retention_hours: Minimum age of files to delete (default: 168 hours = 7 days)

        WARNING: VACUUM is irreversible - time travel will be limited to retention period.

        Returns:
            Dict with vacuum results
        '''
        try:
            full_table = f"{self.catalog}.{self.schema}.{table_name}"

            logger.warning(f"VACUUM {full_table} (retention: {retention_hours} hours)")

            # Get storage before vacuum
            before_details = self.spark.sql(f"DESCRIBE DETAIL {full_table}").first()
            before_size_mb = before_details["sizeInBytes"] / 1024 / 1024

            # Execute VACUUM
            vacuum_sql = f"VACUUM {full_table} RETAIN {retention_hours} HOURS"
            self.spark.sql(vacuum_sql)

            # Get storage after vacuum
            after_details = self.spark.sql(f"DESCRIBE DETAIL {full_table}").first()
            after_size_mb = after_details["sizeInBytes"] / 1024 / 1024

            space_reclaimed_mb = before_size_mb - after_size_mb

            vacuum_result = {
                "table": table_name,
                "status": "completed",
                "retention_hours": retention_hours,
                "before_size_mb": round(before_size_mb, 2),
                "after_size_mb": round(after_size_mb, 2),
                "space_reclaimed_mb": round(space_reclaimed_mb, 2),
                "timestamp": datetime.now().isoformat()
            }

            logger.info(f"VACUUM complete: Reclaimed {space_reclaimed_mb:.2f} MB")
            return vacuum_result

        except Exception as e:
            logger.error(f"VACUUM failed: {str(e)}")
            raise

    def enable_liquid_clustering(self, table_name, cluster_columns):
        '''
        Enable Liquid Clustering for a table (write-optimized clustering).

        Args:
            table_name: Table to enable clustering on
            cluster_columns: List of columns to cluster by

        Liquid Clustering automatically organizes data during writes without
        explicit OPTIMIZE commands.
        '''
        try:
            full_table = f"{self.catalog}.{self.schema}.{table_name}"

            logger.info(f"Enabling Liquid Clustering on {full_table}")
            logger.info(f"Cluster columns: {cluster_columns}")

            # Alter table to add clustering
            cluster_clause = ", ".join(cluster_columns)
            cluster_sql = f"ALTER TABLE {full_table} CLUSTER BY ({cluster_clause})"

            self.spark.sql(cluster_sql)

            result = {
                "table": table_name,
                "status": "liquid_clustering_enabled",
                "cluster_columns": cluster_columns,
                "timestamp": datetime.now().isoformat()
            }

            logger.info(f"Liquid Clustering enabled: {result}")
            return result

        except Exception as e:
            logger.error(f"Failed to enable Liquid Clustering: {str(e)}")
            raise

    def run_scheduled_compaction(self, tables_config):
        '''
        Run scheduled compaction across multiple tables.

        Args:
            tables_config: List of dicts with table name and optimization config

        Example:
            tables_config = [
                {"table": "orders", "z_order": ["customer_id", "order_date"]},
                {"table": "payments", "z_order": ["transaction_id"]}
            ]

        Returns:
            List of optimization results
        '''
        try:
            logger.info(f"Running scheduled compaction on {len(tables_config)} tables")

            results = []

            for config in tables_config:
                table_name = config["table"]
                z_order_cols = config.get("z_order", None)

                # Analyze fragmentation
                fragmentation = self.analyze_table_fragmentation(table_name)

                if fragmentation["needs_compaction"]:
                    logger.info(f"{table_name} needs compaction: {fragmentation['reasons']}")

                    # Run optimization
                    opt_result = self.optimize_table(table_name, z_order_columns=z_order_cols)
                    results.append(opt_result)
                else:
                    logger.info(f"{table_name} is well-optimized - skipping")
                    results.append({
                        "table": table_name,
                        "status": "skipped",
                        "reason": "Already optimized"
                    })

            logger.info(f"Scheduled compaction complete: {len(results)} tables processed")
            return results

        except Exception as e:
            logger.error(f"Scheduled compaction failed: {str(e)}")
            raise

# Example Usage: Automated Nightly Compaction Job
print("\nSCENARIO: Nightly Compaction Job (2 AM UTC)\n")

# Initialize compaction manager
compactor = LakebaseCompactionManager(catalog="sap", schema="gold")

# Tables to optimize nightly
compaction_config = [
    {
        "table": "daily_sales",
        "z_order": ["sale_date", "store_id"]
    },
    {
        "table": "customer_orders",
        "z_order": ["customer_id", "order_date"]
    },
    {
        "table": "product_inventory",
        "z_order": ["product_id", "warehouse_id"]
    }
]

print(f"Nightly compaction job started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Tables in scope: {len(compaction_config)}")

# Step 1: Analyze all tables
print("\n" + "=" * 80)
print("STEP 1: Analyze Fragmentation")
print("=" * 80)

for config in compaction_config:
    table = config["table"]
    analysis = compactor.analyze_table_fragmentation(table)

    print(f"\n{table}:")
    print(f"  Files: {analysis['num_files']}")
    print(f"  Avg file size: {analysis['avg_file_size_mb']} MB")
    print(f"  Records per file: {analysis['records_per_file']}")
    print(f"  Needs compaction: {analysis['needs_compaction']}")
    if analysis['needs_compaction']:
        print(f"  Reasons: {', '.join(analysis['reasons'])}")

# Step 2: Run compaction on tables that need it
print("\n" + "=" * 80)
print("STEP 2: Run Compaction")
print("=" * 80)

results = compactor.run_scheduled_compaction(compaction_config)

for result in results:
    if result["status"] == "completed":
        print(f"\n{result['table']}: OPTIMIZED")
        print(f"  Files: {result['before_files']} -> {result['after_files']} ({result['files_removed']} removed)")
        print(f"  Avg size: {result['before_avg_file_size_mb']} -> {result['after_avg_file_size_mb']} MB")
        print(f"  Duration: {result['duration_seconds']}s")
    else:
        print(f"\n{result['table']}: {result['status'].upper()}")

# Step 3: Weekly VACUUM (Sunday)
print("\n" + "=" * 80)
print("STEP 3: Weekly VACUUM (Reclaim Storage)")
print("=" * 80)

day_of_week = datetime.now().weekday()
if day_of_week == 6:  # Sunday
    print("Today is Sunday - running VACUUM on all tables")

    for config in compaction_config:
        table = config["table"]
        vacuum_result = compactor.vacuum_table(table, retention_hours=168)
        print(f"\n{table}:")
        print(f"  Space reclaimed: {vacuum_result['space_reclaimed_mb']} MB")
else:
    print(f"Today is not Sunday - VACUUM skipped")

print("\n" + "=" * 80)
print("COMPACTION BEST PRACTICES")
print("=" * 80)
print('''
1. Compaction Triggers:
   - Files > 1000: Immediate compaction
   - Avg file size < 10 MB: Compact during low-traffic window
   - After bulk inserts/updates: Compact same day
   - Regular schedule: Nightly at 2 AM

2. Z-ORDER Strategy:
   - Use on high-cardinality columns (customer_id, product_id)
   - Use on columns in WHERE clauses
   - Limit to 3-4 columns max (diminishing returns)
   - Re-run monthly as data distribution changes

3. Liquid Clustering (June 2026):
   - New tables: Use Liquid Clustering instead of Z-ORDER
   - Write-optimized: No explicit OPTIMIZE needed
   - Better for streaming/frequent writes
   - Automatically adapts to data patterns

4. VACUUM Strategy:
   - Default: 7 days retention (168 hours)
   - Critical tables: 30 days retention (for compliance)
   - Test tables: 1 day retention (aggressive cleanup)
   - Run phase-based during low-traffic window
   - WARNING: VACUUM is irreversible!

5. Performance Impact:
   - OPTIMIZE during low-traffic hours (1-4 AM)
   - Monitor: Query latency before/after
   - Expect: 2-5x query speedup on optimized tables
   - Duration: 5-30 minutes per 100GB table

6. Cost Savings:
   - VACUUM can reclaim 20-40 percent storage
   - Smaller files = faster queries = lower compute cost
   - Monitor: Storage costs before/after VACUUM
''')

print(f"\nCompaction job completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")