# Databricks notebook source
# DBTITLE 1,Storage Cost Analysis: Delta Tables
# MAGIC %md
# MAGIC # Storage Cost Analysis: Delta Tables
# MAGIC
# MAGIC FREE EDITION COMPATIBLE
# MAGIC
# MAGIC Purpose: Monitor Delta Lake storage costs and identify optimization opportunities
# MAGIC
# MAGIC Key Metrics:
# MAGIC - Total storage size by catalog/schema/table
# MAGIC - Storage growth trends over time
# MAGIC - File count and average file size
# MAGIC - Old file versions consuming space
# MAGIC - Partitioning efficiency
# MAGIC
# MAGIC Data Sources:
# MAGIC - DESCRIBE DETAIL (table metadata)
# MAGIC - DESCRIBE HISTORY (version history)
# MAGIC - system.information_schema.tables
# MAGIC
# MAGIC Storage Costs:
# MAGIC - DBFS: $0.10-0.15 per GB/month
# MAGIC - S3: $0.023 per GB/month
# MAGIC - Target: < 100 GB for free tier ($2-3/month)

# COMMAND ----------

# DBTITLE 1,Config Import
# MAGIC %run /Workspace/Shared/SkillAIPath-June/00_UTILS/01_config

# COMMAND ----------

# DBTITLE 1,Storage Cost Parameters
# Storage cost parameters
STORAGE_COST_PER_GB_USD = 0.10  # DBFS cost per GB per month
USD_TO_INR = 83.0
MONTHLY_STORAGE_BUDGET_GB = 100  # Target storage limit

print(f"Storage Cost: ${STORAGE_COST_PER_GB_USD}/GB/month = Rs.{STORAGE_COST_PER_GB_USD * USD_TO_INR:.2f}/GB/month")
print(f"Monthly Budget: {MONTHLY_STORAGE_BUDGET_GB} GB = Rs.{MONTHLY_STORAGE_BUDGET_GB * STORAGE_COST_PER_GB_USD * USD_TO_INR:.2f}/month")

# COMMAND ----------

# DBTITLE 1,Overall Storage Summary
# MAGIC %sql
# MAGIC -- Total storage across all tables
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_tables,
# MAGIC     COUNT(DISTINCT table_catalog) AS catalogs,
# MAGIC     COUNT(DISTINCT table_schema) AS schemas,
# MAGIC     ROUND(SUM(table_size_bytes) / 1024 / 1024 / 1024, 2) AS total_size_gb,
# MAGIC     ROUND(SUM(table_size_bytes) / 1024 / 1024 / 1024 * 0.10 * 83, 2) AS estimated_monthly_cost_inr,
# MAGIC     100 AS budget_gb,
# MAGIC     ROUND(SUM(table_size_bytes) / 1024 / 1024 / 1024 / 100 * 100, 2) AS budget_used_pct,
# MAGIC     CASE
# MAGIC         WHEN SUM(table_size_bytes) / 1024 / 1024 / 1024 > 100 THEN 'OVER BUDGET'
# MAGIC         WHEN SUM(table_size_bytes) / 1024 / 1024 / 1024 > 80 THEN 'WARNING - Over 80 percent'
# MAGIC         ELSE 'HEALTHY'
# MAGIC     END AS storage_status
# MAGIC FROM system.information_schema.tables
# MAGIC WHERE table_type = 'MANAGED';

# COMMAND ----------

# DBTITLE 1,Storage by Catalog & Schema
# MAGIC %sql
# MAGIC -- Breakdown by catalog and schema
# MAGIC SELECT
# MAGIC     table_catalog,
# MAGIC     table_schema,
# MAGIC     COUNT(*) AS table_count,
# MAGIC     ROUND(SUM(table_size_bytes) / 1024 / 1024 / 1024, 2) AS total_size_gb,
# MAGIC     ROUND(SUM(table_size_bytes) / 1024 / 1024 / 1024 * 0.10 * 83, 2) AS monthly_cost_inr,
# MAGIC     ROUND(AVG(table_size_bytes) / 1024 / 1024, 2) AS avg_table_size_mb,
# MAGIC     ROUND(SUM(table_size_bytes) * 100.0 / SUM(SUM(table_size_bytes)) OVER (), 2) AS pct_of_total
# MAGIC FROM system.information_schema.tables
# MAGIC WHERE table_type = 'MANAGED'
# MAGIC GROUP BY table_catalog, table_schema
# MAGIC ORDER BY total_size_gb DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# DBTITLE 1,Top 30 Tables by Size
# MAGIC %sql
# MAGIC -- Largest tables consuming storage
# MAGIC SELECT
# MAGIC     table_catalog || '.' || table_schema || '.' || table_name AS full_table_name,
# MAGIC     ROUND(table_size_bytes / 1024 / 1024 / 1024, 3) AS size_gb,
# MAGIC     ROUND(table_size_bytes / 1024 / 1024 / 1024 * 0.10 * 83, 2) AS monthly_cost_inr,
# MAGIC     ROUND(table_size_bytes / 1024 / 1024, 2) AS size_mb,
# MAGIC     created AS created_date,
# MAGIC     last_altered AS last_modified
# MAGIC FROM system.information_schema.tables
# MAGIC WHERE table_type = 'MANAGED'
# MAGIC ORDER BY table_size_bytes DESC
# MAGIC LIMIT 30;

# COMMAND ----------

# DBTITLE 1,Detailed Table Analysis Function
def analyze_table_storage(table_name):
    """
    Analyze storage details for a specific table.
    Args:
        table_name: Fully qualified table name (catalog.schema.table)
    """
    try:
        # Get table details
        detail_df = spark.sql(f"DESCRIBE DETAIL {table_name}")
        detail = detail_df.collect()[0]

        # Get history
        history_df = spark.sql(f"DESCRIBE HISTORY {table_name}")
        versions = history_df.count()

        # Calculate metrics
        size_gb = detail['sizeInBytes'] / 1024 / 1024 / 1024
        num_files = detail['numFiles']
        avg_file_size_mb = (detail['sizeInBytes'] / num_files / 1024 / 1024) if num_files > 0 else 0

        print(f"\n--- Table: {table_name} ---")
        print(f"Total Size: {size_gb:.3f} GB")
        print(f"Monthly Cost: Rs.{size_gb * 0.10 * 83:.2f}")
        print(f"Number of Files: {num_files:,}")
        print(f"Average File Size: {avg_file_size_mb:.2f} MB")
        print(f"Version History: {versions} versions")
        print(f"Location: {detail['location']}")
        print(f"Format: {detail['format']}")

        # Optimization recommendations
        print(f"\n--- Recommendations ---")
        if num_files > 1000:
            print(f"WARNING: {num_files} files detected. Run OPTIMIZE to compact.")
        if avg_file_size_mb < 100:
            print(f"WARNING: Average file size {avg_file_size_mb:.2f} MB is small. Run OPTIMIZE.")
        if versions > 30:
            print(f"INFO: {versions} versions in history. Consider running VACUUM to remove old files.")
        if num_files < 100 and avg_file_size_mb > 500:
            print(f"GOOD: File size and count are well optimized.")

        return {
            'size_gb': size_gb,
            'num_files': num_files,
            'avg_file_size_mb': avg_file_size_mb,
            'versions': versions
        }

    except Exception as e:
        print(f"ERROR analyzing {table_name}: {str(e)}")
        return None

print("Function analyze_table_storage defined")

# COMMAND ----------

# DBTITLE 1,Analyze Top SkillAIPath Tables
# Analyze key SkillAIPath tables
sap_tables = [
    'sap_dev.bronze.sales',
    'sap_dev.silver.dim_product',
    'sap_dev.silver.dim_customer',
    'sap_dev.gold.mv_daily_sales'
]

for table in sap_tables:
    try:
        result = analyze_table_storage(table)
    except:
        print(f"Skipping {table} (may not exist yet)")

# COMMAND ----------

# DBTITLE 1,Storage Growth Trend (Historical)
# MAGIC %sql
# MAGIC -- Storage growth over time (requires historical tracking)
# MAGIC -- Note: Run this query daily and save results to a tracking table
# MAGIC -- CREATE TABLE sap.monitoring.storage_history AS ...
# MAGIC
# MAGIC SELECT
# MAGIC     CURRENT_DATE() AS snapshot_date,
# MAGIC     table_catalog,
# MAGIC     table_schema,
# MAGIC     COUNT(*) AS table_count,
# MAGIC     ROUND(SUM(table_size_bytes) / 1024 / 1024 / 1024, 2) AS total_size_gb,
# MAGIC     ROUND(SUM(table_size_bytes) / 1024 / 1024 / 1024 * 0.10 * 83, 2) AS monthly_cost_inr
# MAGIC FROM system.information_schema.tables
# MAGIC WHERE table_type = 'MANAGED'
# MAGIC GROUP BY table_catalog, table_schema
# MAGIC ORDER BY total_size_gb DESC;

# COMMAND ----------

# DBTITLE 1,Tables Needing OPTIMIZE
# MAGIC %sql
# MAGIC -- Identify tables with too many small files
# MAGIC -- This query requires querying DESCRIBE DETAIL for each table
# MAGIC -- Use the analyze_table_storage function above for detailed analysis
# MAGIC
# MAGIC SELECT
# MAGIC     table_catalog || '.' || table_schema || '.' || table_name AS full_table_name,
# MAGIC     ROUND(table_size_bytes / 1024 / 1024 / 1024, 3) AS size_gb,
# MAGIC     'Run DESCRIBE DETAIL to check file count' AS recommendation
# MAGIC FROM system.information_schema.tables
# MAGIC WHERE table_type = 'MANAGED'
# MAGIC   AND table_size_bytes > 100 * 1024 * 1024  -- Larger than 100 MB
# MAGIC ORDER BY table_size_bytes DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# DBTITLE 1,Storage Optimization Strategies
# MAGIC %md
# MAGIC ## Storage Cost Optimization
# MAGIC
# MAGIC File Compaction (OPTIMIZE):
# MAGIC - Target: 128-256 MB per file
# MAGIC - Run phase-based for active tables
# MAGIC - Savings: 20-40 percent storage reduction after first OPTIMIZE
# MAGIC - Command: OPTIMIZE tablename ZORDER BY (frequently_filtered_columns)
# MAGIC
# MAGIC Old Version Cleanup (VACUUM):
# MAGIC - Default retention: 7 days (168 hours)
# MAGIC - Run after OPTIMIZE completes
# MAGIC - Savings: 10-30 percent by removing old file versions
# MAGIC - Command: VACUUM tablename RETAIN 168 HOURS
# MAGIC - WARNING: Cannot time travel beyond retention period
# MAGIC
# MAGIC Partitioning Strategy:
# MAGIC - Partition by date for time-series data (sale_date, event_date)
# MAGIC - Avoid over-partitioning (< 1 GB per partition)
# MAGIC - Drop old partitions if data has expiration policy
# MAGIC - Command: ALTER TABLE tablename DROP PARTITION (year=2023)
# MAGIC
# MAGIC Data Lifecycle Management:
# MAGIC - Archive old data to cheaper storage (S3 Glacier)
# MAGIC - Implement retention policies (delete data > 2 years)
# MAGIC - Use external tables for cold data (query on-demand)
# MAGIC - Aggregate old data to reduce granularity
# MAGIC
# MAGIC Table Cleanup:
# MAGIC - Drop unused test tables
# MAGIC - Drop temporary tables from failed experiments
# MAGIC - Consolidate duplicate tables
# MAGIC - Document table ownership and purpose
# MAGIC
# MAGIC Expected Savings:
# MAGIC - OPTIMIZE + VACUUM: 30-50 percent storage reduction
# MAGIC - Partitioning + lifecycle: 20-40 percent reduction
# MAGIC - Table cleanup: 10-20 percent reduction
# MAGIC - Total potential savings: 50-70 percent
# MAGIC
# MAGIC Storage Budget:
# MAGIC - Free tier: 100 GB (Rs.830/month)
# MAGIC - Small deployment: 200-500 GB (Rs.1,660-4,150/month)
# MAGIC - Medium deployment: 500-1000 GB (Rs.4,150-8,300/month)
# MAGIC - Large deployment: 1-5 TB (Rs.8,300-41,500/month)

# COMMAND ----------

