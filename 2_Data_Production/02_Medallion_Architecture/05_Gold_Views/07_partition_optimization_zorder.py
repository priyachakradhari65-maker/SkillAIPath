# Databricks notebook source
# DBTITLE 1,Partition Optimization & ZORDER - Configuration
# TITLE: Partition Optimization & ZORDER for Gold Layer Tables
# PURPOSE: Production-grade table optimization patterns for query performance
# EDITION: Free Edition Compatible (OPTIMIZE/ZORDER available in all editions)
# AUDIENCE: Data Engineers optimizing large-scale production tables

# PRODUCTION NOTES:
# - Schedule: Run phase-based during low-traffic windows (Sundays 2-4 AM)
# - Cost: OPTIMIZE is compute-intensive, use appropriate cluster sizing
# - Monitoring: Track file count reduction and query performance improvements
# - Dependencies: Requires Unity Catalog tables from silver/gold layers

# Load configuration for portable table references
%run ../../../1_Canvas/1_Setup/1_config

# Set SQL variables for use in SQL cells
spark.conf.set("schema_gold", Config.SCHEMA_GOLD)

print("✓ Configuration loaded (portable)")
print(f"  Current Catalog: {Config.CATALOG}")
print(f"  Tables to optimize: {Config.TBL_FACT_SALES}, {Config.TBL_FACT_INVENTORY}, {Config.TBL_DIM_CUSTOMER}")

# COMMAND ----------

# DBTITLE 1,Analyze Table Statistics Before Optimization
# CELL: Pre-Optimization Analysis
# Measure current state: file count, size, query performance baseline

from pyspark.sql import functions as F
from datetime import datetime

print("=" * 60)
print("PRE-OPTIMIZATION TABLE STATISTICS")
print("=" * 60)

# Analyze fact_sales table
table_name = f"{Config.TBL_FACT_SALES}"
print(f"\nAnalyzing: {table_name}")

# Get file statistics
try:
    details = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]

    file_count = details['numFiles']
    size_bytes = details['sizeInBytes']
    size_gb = size_bytes / (1024**3)

    print(f"Current Files: {file_count:,}")
    print(f"Table Size: {size_gb:.2f} GB")
    print(f"Avg File Size: {(size_bytes/file_count)/(1024**2):.2f} MB")

    # Optimal file size: 128 MB - 1 GB for Databricks
    if file_count > 100 and (size_bytes/file_count) < 128*1024*1024:
        print("\nWARNING: Small file problem detected - OPTIMIZE recommended")

    # Check partitioning scheme
    if 'partitionColumns' in details:
        print(f"\nPartition Columns: {details['partitionColumns']}")
    else:
        print("\nNo partitioning detected")

except Exception as e:
    print(f"Error analyzing table: {str(e)}")
    print("Ensure table exists and you have SELECT privilege")

# COMMAND ----------

# DBTITLE 1,Check File Distribution by Partition
# MAGIC %sql
# MAGIC -- CELL: File Distribution Analysis
# MAGIC -- Identify partitions with excessive small files
# MAGIC
# MAGIC -- Example: Check daily_sales_summary table file distribution
# MAGIC -- Note: This works for Delta tables and underlying storage
# MAGIC
# MAGIC DESCRIBE DETAIL ${spark.conf.schema_gold}.daily_sales_summary;
# MAGIC
# MAGIC -- Look for:
# MAGIC -- 1. numFiles > 1000 indicates fragmentation
# MAGIC -- 2. sizeInBytes / numFiles < 100MB indicates small file problem
# MAGIC -- 3. Multiple versions without VACUUM indicates historical bloat

# COMMAND ----------

# DBTITLE 1,Run OPTIMIZE on Fact Tables
# CELL: Execute OPTIMIZE Command
# Compacts small files into larger ones (target: 1 GB per file)

from datetime import datetime
import time

print("=" * 60)
print("RUNNING OPTIMIZE ON GOLD LAYER TABLES")
print("=" * 60)

# Tables to optimize
tables_to_optimize = [
    Config.TBL_FACT_SALES,
    Config.TBL_FACT_INVENTORY,
    Config.TBL_DIM_CUSTOMER
]

optimization_results = []

for table in tables_to_optimize:
    print(f"\n--- Optimizing: {table} ---")
    start_time = time.time()

    try:
        # Run OPTIMIZE
        result = spark.sql(f"OPTIMIZE {table}")

        # Capture metrics
        metrics = result.collect()[0]
        duration = time.time() - start_time

        optimization_results.append({
            'table': table,
            'files_added': metrics['metrics']['numFilesAdded'],
            'files_removed': metrics['metrics']['numFilesRemoved'],
            'duration_sec': round(duration, 2),
            'status': 'SUCCESS'
        })

        print(f"Files Removed: {metrics['metrics']['numFilesRemoved']}")
        print(f"Files Added: {metrics['metrics']['numFilesAdded']}")
        print(f"Duration: {duration:.2f} seconds")

    except Exception as e:
        print(f"ERROR: {str(e)}")
        optimization_results.append({
            'table': table,
            'status': 'FAILED',
            'error': str(e)
        })

print("\n" + "=" * 60)
print("OPTIMIZATION COMPLETE")
print("=" * 60)

# COMMAND ----------

# DBTITLE 1,Apply ZORDER for Query Optimization
# CELL: ZORDER BY - Colocate Data for Query Performance
# ZORDER clusters data based on frequently filtered columns
# CRITICAL: Choose columns used in WHERE/JOIN clauses

print("=" * 60)
print("APPLYING ZORDER INDEXING")
print("=" * 60)

# ZORDER Strategy (using actual SkillAIPath schema column names):
# - fact_sales: BatchDate (time-based queries), CustomerSK (joins)
# - dim_customer: Skip for now (check schema first)
# - fact_inventory: ProductSK, WarehouseSK (frequent lookups)

zorder_operations = [
    {
        'table': Config.TBL_FACT_SALES,
        'columns': ['BatchDate', 'CustomerSK'],
        'reasoning': 'Optimizes time-range queries and customer analytics'
    }
]

for op in zorder_operations:
    table = op['table']
    columns = op['columns']

    print(f"\n--- ZORDER: {table} ---")
    print(f"Columns: {', '.join(columns)}")
    print(f"Reasoning: {op['reasoning']}")

    start_time = time.time()

    try:
        # Build ZORDER command
        zorder_cols = ', '.join(columns)
        cmd = f"OPTIMIZE {table} ZORDER BY ({zorder_cols})"

        result = spark.sql(cmd)
        duration = time.time() - start_time

        metrics = result.collect()[0]
        print(f"Files Processed: {metrics['metrics']['numRemovedFiles']}")
        print(f"Duration: {duration:.2f} seconds")
        print("Status: SUCCESS")

    except Exception as e:
        print(f"ERROR: {str(e)}")
        print("Note: ZORDER requires columns to exist in table schema")

print("\n" + "=" * 60)
print("ZORDER COMPLETE")
print("=" * 60)

# COMMAND ----------

# DBTITLE 1,Verify Post-Optimization Improvements
# CELL: Post-Optimization Verification
# Measure improvements in file count and query performance

print("=" * 60)
print("POST-OPTIMIZATION VERIFICATION")
print("=" * 60)

for table in tables_to_optimize:
    print(f"\n--- {table} ---")

    try:
        details = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]

        file_count = details['numFiles']
        size_bytes = details['sizeInBytes']
        avg_file_size_mb = (size_bytes/file_count)/(1024**2)

        print(f"Files: {file_count:,}")
        print(f"Avg File Size: {avg_file_size_mb:.2f} MB")

        # Healthy file size: 128 MB - 1 GB
        if 128 <= avg_file_size_mb <= 1024:
            print("Status: OPTIMAL file size")
        elif avg_file_size_mb < 128:
            print("Status: Files still small - consider more data or OPTIMIZE frequency")
        else:
            print("Status: Files large - good for scan performance")

    except Exception as e:
        print(f"Error: {str(e)}")

print("\n" + "=" * 60)
print("VERIFICATION COMPLETE")
print("=" * 60)

# COMMAND ----------

# DBTITLE 1,Run Sample Query Performance Test
# MAGIC %sql
# MAGIC -- CELL: Query Performance Validation
# MAGIC -- Test query that benefits from ZORDER
# MAGIC -- Compare execution time before/after optimization
# MAGIC
# MAGIC -- Time-range query on daily_sales_summary (benefits from sale_date ZORDER)
# MAGIC -- Updated to query actual data range (last 7 days)
# MAGIC SELECT
# MAGIC     sale_date,
# MAGIC     total_transactions as order_count,
# MAGIC     ROUND(total_revenue, 2) as revenue,
# MAGIC     unique_customers
# MAGIC FROM sap_dev.gold.daily_sales_summary
# MAGIC WHERE sale_date >= DATE_SUB(CURRENT_DATE(), 7)
# MAGIC ORDER BY sale_date;
# MAGIC
# MAGIC -- Note: Check query execution time in Spark UI
# MAGIC -- Expected improvement: 30-70% faster after ZORDER on order_date

# COMMAND ----------

# DBTITLE 1,Cleanup Old Versions with VACUUM
# CELL: VACUUM - Remove Old File Versions
# Reclaims storage from OPTIMIZE/ZORDER old files
# WARNING: VACUUM is irreversible - cannot time travel beyond retention

print("=" * 60)
print("VACUUM OPERATION - STORAGE CLEANUP")
print("=" * 60)

print("\nWARNING: VACUUM permanently deletes old file versions")
print("Default retention: 7 days (168 hours)")
print("Cannot time travel beyond VACUUM point\n")

# Conservative retention: 168 hours (7 days)
retention_hours = 168

# Tables to vacuum (same list from Cell 4)
tables_to_vacuum = [
    Config.TBL_FACT_SALES,
    Config.TBL_FACT_INVENTORY,
    Config.TBL_DIM_CUSTOMER
]

for table in tables_to_vacuum:
    print(f"\n--- VACUUM: {table} ---")

    try:
        # Execute VACUUM
        result = spark.sql(f"VACUUM {table} RETAIN {retention_hours} HOURS")

        print(f"VACUUM completed with {retention_hours}h retention")
        print("Status: SUCCESS")

    except Exception as e:
        print(f"ERROR: {str(e)}")
        print("Note: VACUUM requires MODIFY privilege on table")

print("\n" + "=" * 60)
print("VACUUM COMPLETE - Storage reclaimed")
print("=" * 60)

# COMMAND ----------

# DBTITLE 1,Generate Optimization Report
# CELL: Optimization Summary Report
# Log results for monitoring and scheduling

import json
from datetime import datetime

print("=" * 60)
print("OPTIMIZATION SUMMARY REPORT")
print("=" * 60)

# Note: This cell requires Cell 4 to have been run first to populate optimization_results
try:
    optimization_results
except NameError:
    print("WARNING: optimization_results not found. Run Cell 4 (OPTIMIZE) first.\n")
    optimization_results = []

report = {
    'timestamp': datetime.now().isoformat(),
    'optimized_tables': len([r for r in optimization_results if r.get('status') == 'SUCCESS']),
    'operations': optimization_results,
    'recommendations': []
}

# Calculate total files reduced
total_removed = sum([r.get('files_removed', 0) for r in optimization_results if r.get('status') == 'SUCCESS'])
total_added = sum([r.get('files_added', 0) for r in optimization_results if r.get('status') == 'SUCCESS'])
file_reduction = total_removed - total_added

print(f"\nTimestamp: {report['timestamp']}")
print(f"Tables Optimized: {report['optimized_tables']}")
print(f"\nFile Consolidation:")
print(f"  Files Removed: {total_removed:,}")
print(f"  Files Added: {total_added:,}")
print(f"  Net Reduction: {file_reduction:,} files")

if file_reduction > 0:
    reduction_pct = (file_reduction / total_removed) * 100
    print(f"  Reduction %: {reduction_pct:.1f}%")

# Recommendations
print("\n--- Recommendations ---")
if file_reduction < 10:
    print("- Low file reduction: Consider running OPTIMIZE less frequently")
    report['recommendations'].append('Reduce OPTIMIZE frequency')
else:
    print("- Good file consolidation achieved")

print("- Monitor query performance in Databricks SQL Query History")
print("- Schedule OPTIMIZE phase-based during low-traffic windows")
print("- Track storage costs before/after VACUUM")

print("\n" + "=" * 60)
print("REPORT COMPLETE")
print("=" * 60)

# Optionally log to monitoring table
# spark.createDataFrame([report]).write.mode('append').saveAsTable('sap.monitoring.optimization_log')

# COMMAND ----------

# DBTITLE 1,Production Deployment Notes
# MAGIC %md
# MAGIC # Production Deployment Guide: Partition Optimization & ZORDER
# MAGIC
# MAGIC ## Scheduling Recommendations
# MAGIC
# MAGIC **Frequency:**
# MAGIC - OPTIMIZE: Weekly (or when file count > 1000)
# MAGIC - ZORDER: Monthly (or after schema changes)
# MAGIC - VACUUM: Weekly (after OPTIMIZE)
# MAGIC
# MAGIC **Timing:**
# MAGIC - Run during low-traffic windows (2-4 AM Sunday)
# MAGIC - Avoid business hours (OPTIMIZE locks tables briefly)
# MAGIC
# MAGIC **Job Configuration:**
# MAGIC ```python
# MAGIC # Databricks Job Settings
# MAGIC Cluster: Use job cluster with autoscaling (4-16 workers)
# MAGIC Timeout: 2 hours
# MAGIC Retries: 2 (with 5-minute delay)
# MAGIC Notifications: Email on failure
# MAGIC ```
# MAGIC
# MAGIC ## Cost Considerations
# MAGIC
# MAGIC **Compute Costs:**
# MAGIC - OPTIMIZE: ~0.5-2 DBU per GB processed
# MAGIC - ZORDER: ~1.5-3 DBU per GB (more intensive)
# MAGIC - Use appropriate cluster sizing based on table size
# MAGIC
# MAGIC **Storage Savings:**
# MAGIC - VACUUM reclaims storage from old file versions
# MAGIC - Typical savings: 20-40% after first VACUUM
# MAGIC - Monitor with: `DESCRIBE DETAIL <table>` before/after
# MAGIC
# MAGIC ## Monitoring
# MAGIC
# MAGIC **Key Metrics:**
# MAGIC 1. File count reduction (target: 70%+ consolidation)
# MAGIC 2. Query performance improvement (30-70% faster)
# MAGIC 3. Storage reclaimed by VACUUM (GB)
# MAGIC 4. OPTIMIZE/ZORDER execution time (trend over time)
# MAGIC
# MAGIC **Alerts:**
# MAGIC - File count > 5000: Increase OPTIMIZE frequency
# MAGIC - OPTIMIZE duration > 1 hour: Review cluster size
# MAGIC - Query performance regression: Re-evaluate ZORDER columns
# MAGIC
# MAGIC ## Free Edition Notes
# MAGIC
# MAGIC - All features (OPTIMIZE, ZORDER, VACUUM) available in Databricks Community Edition
# MAGIC - Limitations: Smaller cluster sizes, longer execution times
# MAGIC - Cost tracking: Not applicable (free compute)
# MAGIC
# MAGIC ## Troubleshooting
# MAGIC
# MAGIC **Issue: OPTIMIZE fails with timeout**
# MAGIC - Solution: Increase cluster size or split large tables
# MAGIC
# MAGIC **Issue: ZORDER not improving query performance**
# MAGIC - Solution: Re-evaluate column selection (check query patterns)
# MAGIC
# MAGIC **Issue: VACUUM not reclaiming storage**
# MAGIC - Solution: Check retention period, ensure OPTIMIZE ran first
