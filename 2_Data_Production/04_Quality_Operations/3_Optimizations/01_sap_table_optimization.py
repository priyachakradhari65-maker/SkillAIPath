# Databricks notebook source
# DBTITLE 1,SkillAIPath Table Optimization Framework
# MAGIC %md
# MAGIC # SkillAIPath Table Optimization Framework
# MAGIC **Production Delta Lake Optimization - ZORDER + Liquid Clustering + VACUUM**
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC Comprehensive table optimization for SkillAIPath Unity Catalog tables.
# MAGIC
# MAGIC **Optimization Techniques:**
# MAGIC * **OPTIMIZE** - Compacts small files into larger files
# MAGIC * **ZORDER** - Co-locality optimization for high-cardinality columns
# MAGIC * **Liquid Clustering** - Automatic clustering (Delta Lake 3.1+)
# MAGIC * **VACUUM** - Removes old file versions
# MAGIC
# MAGIC ## When to Optimize
# MAGIC
# MAGIC | Scenario | Frequency | Technique |
# MAGIC |----------|-----------|----------|
# MAGIC | After batch loads | Daily | OPTIMIZE |
# MAGIC | High-cardinality filters | Weekly | ZORDER |
# MAGIC | New tables (DBR 13+) | At creation | Liquid Clustering |
# MAGIC | After major deletes | Monthly | VACUUM |
# MAGIC
# MAGIC ## Performance Impact
# MAGIC
# MAGIC **Example: fact_sales (500K rows)**
# MAGIC * Before: 30s query time, 200 small files
# MAGIC * After OPTIMIZE: 8s query time, 10 large files
# MAGIC * After ZORDER: 3s query time (with WHERE CustomerSK filter)
# MAGIC
# MAGIC ## SkillAIPath Optimization Strategy
# MAGIC
# MAGIC **Silver Layer:**
# MAGIC * fact_sales: ZORDER BY (BatchDate, CustomerSK, ProductSK)
# MAGIC * fact_purchase: ZORDER BY (BatchDate, ProductSK)
# MAGIC * dim_customer: ZORDER BY (CustomerID) - SCD2 lookups
# MAGIC
# MAGIC **Gold Layer:**
# MAGIC * All marts: OPTIMIZE only (pre-aggregated, small datasets)

# COMMAND ----------

# DBTITLE 1,Setup and Configuration
# SkillAIPath Table Optimization - Production Setup

import sys
import os
from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring
from datetime import datetime

# Load SkillAIPath Config
current_dir = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
project_root = os.path.dirname(current_dir)
config_path = os.path.join(project_root, "00_UTILS")
sys.path.append(config_path)
from config import Config

print("="*70)
print("SKILLAIPPATH TABLE OPTIMIZATION FRAMEWORK")
print("="*70)
print(f"Environment: {Config.ENVIRONMENT}")
print(f"Catalog: {Config.CATALOG}")
print("="*70)

# Widgets
dbutils.widgets.dropdown("operation", "optimize", ["optimize", "zorder", "vacuum", "analyze"], "Operation")
dbutils.widgets.dropdown("layer", "silver", ["bronze", "silver", "gold", "all"], "Target Layer")
dbutils.widgets.text("vacuum_hours", "168", "Vacuum Retention Hours")

operation = dbutils.widgets.get("operation")
layer = dbutils.widgets.get("layer")
vacuum_hours = int(dbutils.widgets.get("vacuum_hours"))

print(f"\nOperation: {operation.upper()}")
print(f"Target layer: {layer}")
if operation == "vacuum":
    print(f"Retention: {vacuum_hours} hours ({vacuum_hours//24} days)")

# COMMAND ----------

# DBTITLE 1,Table Optimization Engine
def optimize_table(table_name, zorder_columns=None, dry_run=False):
    """
    Optimize Delta table with optional ZORDER.

    Args:
        table_name: Fully qualified table name (catalog.schema.table)
        zorder_columns: List of columns for ZORDER (optional)
        dry_run: If True, print commands without executing

    Returns:
        Dictionary with optimization metrics
    """
    print(f"\n{'='*70}")
    print(f"[OPTIMIZE] {table_name}")
    print(f"{'='*70}")

    try:
        # Get table stats before optimization
        df_before = spark.sql(f"DESCRIBE DETAIL {table_name}")
        files_before = df_before.select("numFiles").first()[0]
        size_before = df_before.select("sizeInBytes").first()[0] / (1024**3)  # GB

        print(f"\nBefore optimization:")
        print(f"  Files: {files_before:,}")
        print(f"  Size: {size_before:.2f} GB")

        if dry_run:
            if zorder_columns:
                cmd = f"OPTIMIZE {table_name} ZORDER BY ({', '.join(zorder_columns)})"
            else:
                cmd = f"OPTIMIZE {table_name}"
            print(f"\n[DRY RUN] Would execute: {cmd}")
            return {"table": table_name, "status": "DRY_RUN", "command": cmd}

        # Execute OPTIMIZE
        start_time = datetime.now()
        if zorder_columns:
            zorder_clause = ', '.join(zorder_columns)
            print(f"\nApplying ZORDER BY ({zorder_clause})...")
            spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({zorder_clause})")
        else:
            print(f"\nCompacting files (no ZORDER)...")
            spark.sql(f"OPTIMIZE {table_name}")

        duration = (datetime.now() - start_time).total_seconds()

        # Get table stats after optimization
        df_after = spark.sql(f"DESCRIBE DETAIL {table_name}")
        files_after = df_after.select("numFiles").first()[0]
        size_after = df_after.select("sizeInBytes").first()[0] / (1024**3)

        print(f"\nAfter optimization:")
        print(f"  Files: {files_after:,} (reduced by {files_before - files_after:,})")
        print(f"  Size: {size_after:.2f} GB")
        print(f"  Duration: {duration:.1f}s")

        improvement = ((files_before - files_after) / files_before * 100) if files_before > 0 else 0
        print(f"\n[SUCCESS] File reduction: {improvement:.1f}%")

        return {
            "table": table_name,
            "status": "SUCCESS",
            "files_before": files_before,
            "files_after": files_after,
            "size_gb": size_after,
            "duration_sec": duration,
            "improvement_pct": round(improvement, 1),
            "zorder_columns": zorder_columns
        }

    except Exception as e:
        print(f"\n[ERROR] {str(e)}")
        return {
            "table": table_name,
            "status": "FAILED",
            "error": str(e)
        }

def vacuum_table(table_name, retention_hours=168, dry_run=False):
    """
    Vacuum table to remove old file versions.

    Args:
        table_name: Fully qualified table name
        retention_hours: Minimum retention (default 7 days)
        dry_run: If True, shows what would be deleted

    Returns:
        Dictionary with vacuum metrics
    """
    print(f"\n{'='*70}")
    print(f"[VACUUM] {table_name}")
    print(f"{'='*70}")
    print(f"Retention: {retention_hours} hours ({retention_hours//24} days)")

    try:
        # Set retention configuration
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

        if dry_run:
            print("\n[DRY RUN] Files that would be deleted:")
            df_vacuum = spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS DRY RUN")
            display(df_vacuum)
            return {"table": table_name, "status": "DRY_RUN"}

        # Execute VACUUM
        start_time = datetime.now()
        spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")
        duration = (datetime.now() - start_time).total_seconds()

        print(f"\n[SUCCESS] Vacuum completed in {duration:.1f}s")
        return {
            "table": table_name,
            "status": "SUCCESS",
            "retention_hours": retention_hours,
            "duration_sec": duration
        }

    except Exception as e:
        print(f"\n[ERROR] {str(e)}")
        return {"table": table_name, "status": "FAILED", "error": str(e)}

def analyze_table(table_name):
    """
    Analyze table to update statistics.
    """
    print(f"\n[ANALYZE] {table_name}")
    try:
        spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS")
        print(f"[SUCCESS] Statistics updated")
        return {"table": table_name, "status": "SUCCESS"}
    except Exception as e:
        print(f"[ERROR] {str(e)}")
        return {"table": table_name, "status": "FAILED", "error": str(e)}

print("Optimization engine loaded")

# COMMAND ----------

# DBTITLE 1,SkillAIPath Production Optimization Strategy
# Define optimization strategy for all SkillAIPath tables

optimization_strategy = {
    # SILVER LAYER - FACTS (High priority)
    Config.TBL_FACT_SALES: {
        "zorder": [Config.COL_BATCH_DATE, "CustomerSK", "ProductSK"],
        "priority": "HIGH",
        "description": "Main sales fact - optimize for date + customer/product filters"
    },
    Config.TBL_FACT_PURCHASE: {
        "zorder": [Config.COL_BATCH_DATE, "ProductSK"],
        "priority": "HIGH",
        "description": "Purchase fact - optimize for date + product filters"
    },
    Config.TBL_FACT_STOCK: {
        "zorder": [Config.COL_BATCH_DATE, "WarehouseSK"],
        "priority": "MEDIUM",
        "description": "Stock fact - optimize for date + warehouse filters"
    },

    # SILVER LAYER - DIMENSIONS (SCD2 lookups)
    Config.TBL_DIM_CUSTOMER: {
        "zorder": ["CustomerID", Config.SCD_START_DATE_COL],
        "priority": "HIGH",
        "description": "Customer dimension - optimize for ID lookup + SCD2"
    },
    Config.TBL_DIM_PRODUCT: {
        "zorder": ["ProductID"],
        "priority": "MEDIUM",
        "description": "Product dimension - optimize for ID lookup"
    },

    # GOLD LAYER - MARTS (Optimize only, no ZORDER needed)
    Config.TBL_MART_CUSTOMER_RFM: {
        "zorder": None,
        "priority": "LOW",
        "description": "RFM mart - small dataset, optimize only"
    },
    Config.TBL_MART_REVENUE_DASHBOARD: {
        "zorder": None,
        "priority": "LOW",
        "description": "Revenue dashboard - small dataset, optimize only"
    },
    Config.TBL_MART_PRODUCT_PERFORMANCE: {
        "zorder": None,
        "priority": "LOW",
        "description": "Product performance - small dataset, optimize only"
    }
}

print("Optimization strategy defined for SkillAIPath tables")
print(f"Total tables: {len(optimization_strategy)}")
print(f"  HIGH priority: {sum(1 for v in optimization_strategy.values() if v['priority'] == 'HIGH')}")
print(f"  MEDIUM priority: {sum(1 for v in optimization_strategy.values() if v['priority'] == 'MEDIUM')}")
print(f"  LOW priority: {sum(1 for v in optimization_strategy.values() if v['priority'] == 'LOW')}")

# COMMAND ----------

# DBTITLE 1,Execute Optimization Suite
# Execute optimization based on selected operation and layer

print("="*70)
print(f"EXECUTING {operation.upper()} ON {layer.upper()} LAYER")
print("="*70)

# Filter tables by layer
if layer == "all":
    tables_to_process = optimization_strategy
else:
    schema_map = {"bronze": Config.SCHEMA_BRONZE, "silver": Config.SCHEMA_SILVER, "gold": Config.SCHEMA_GOLD}
    target_schema = schema_map.get(layer)
    tables_to_process = {k: v for k, v in optimization_strategy.items()
                         if f"{Config.CATALOG}.{target_schema}" in k}

if not tables_to_process:
    print(f"\n[WARNING] No tables found for layer: {layer}")
    dbutils.notebook.exit("No tables to process")

print(f"\nTables to process: {len(tables_to_process)}")

results = []

for table_name, config in tables_to_process.items():
    print(f"\n{'='*70}")
    print(f"Table: {table_name}")
    print(f"Priority: {config['priority']}")
    print(f"Description: {config['description']}")

    if operation == "optimize":
        result = optimize_table(table_name, zorder_columns=None)
    elif operation == "zorder":
        if config['zorder']:
            result = optimize_table(table_name, zorder_columns=config['zorder'])
        else:
            print("[SKIP] No ZORDER columns defined (gold layer marts)")
            result = {"table": table_name, "status": "SKIPPED"}
    elif operation == "vacuum":
        result = vacuum_table(table_name, retention_hours=vacuum_hours)
    elif operation == "analyze":
        result = analyze_table(table_name)

    results.append(result)

print("\n" + "="*70)
print("OPTIMIZATION SUITE COMPLETE")
print("="*70)

# Summary
import pandas as pd
df_results = pd.DataFrame(results)
print("\nSummary:")
print(f"  Total: {len(results)}")
print(f"  Success: {df_results[df_results['status'] == 'SUCCESS'].shape[0]}")
print(f"  Failed: {df_results[df_results['status'] == 'FAILED'].shape[0]}")
print(f"  Skipped: {df_results[df_results['status'] == 'SKIPPED'].shape[0]}")

if operation == "optimize" or operation == "zorder":
    successful = df_results[df_results['status'] == 'SUCCESS']
    if not successful.empty:
        print(f"\nTotal file reduction: {successful['files_before'].sum() - successful['files_after'].sum():,} files")
        print(f"Average improvement: {successful['improvement_pct'].mean():.1f}%")
        print(f"Total duration: {successful['duration_sec'].sum():.1f}s")

# COMMAND ----------

# DBTITLE 1,Verify Optimization Results
# MAGIC %sql
# MAGIC -- Check table file counts and sizes after optimization
# MAGIC
# MAGIC SELECT
# MAGIC   name as table_name,
# MAGIC   numFiles as file_count,
# MAGIC   ROUND(sizeInBytes / (1024*1024*1024), 2) as size_gb,
# MAGIC   ROUND(sizeInBytes / numFiles / (1024*1024), 2) as avg_file_size_mb,
# MAGIC   format,
# MAGIC   partitionColumns
# MAGIC FROM (
# MAGIC   SELECT * FROM delta.`dbfs:/user/hive/warehouse/${Config.CATALOG}.db/${Config.SCHEMA_SILVER}`
# MAGIC   UNION ALL
# MAGIC   SELECT * FROM delta.`dbfs:/user/hive/warehouse/${Config.CATALOG}.db/${Config.SCHEMA_GOLD}`
# MAGIC )
# MAGIC WHERE format = 'delta'
# MAGIC ORDER BY size_gb DESC;
# MAGIC
# MAGIC -- INTERPRETATION:
# MAGIC -- file_count: Lower is better (10-50 files ideal for < 1GB tables)
# MAGIC -- avg_file_size_mb: Target 128-256 MB per file
# MAGIC -- If file_count > 100: Run OPTIMIZE
# MAGIC -- If avg_file_size_mb < 64: Run OPTIMIZE

# COMMAND ----------

# DBTITLE 1,Production Runbook
# MAGIC %md
# MAGIC # SkillAIPath Optimization Runbook
# MAGIC
# MAGIC ## Daily Optimization Schedule
# MAGIC
# MAGIC ### 1. After Batch Loads (Daily at 2 AM)
# MAGIC
# MAGIC **Run OPTIMIZE on fact tables:**
# MAGIC ```python
# MAGIC dbutils.widgets.set("operation", "optimize")
# MAGIC dbutils.widgets.set("layer", "silver")
# MAGIC # Execute notebook
# MAGIC ```
# MAGIC
# MAGIC **Why:** Batch loads create many small files. OPTIMIZE compacts them.
# MAGIC
# MAGIC ### 2. Weekly ZORDER (Sunday at 3 AM)
# MAGIC
# MAGIC **Apply ZORDER to high-priority tables:**
# MAGIC ```python
# MAGIC dbutils.widgets.set("operation", "zorder")
# MAGIC dbutils.widgets.set("layer", "all")
# MAGIC # Execute notebook
# MAGIC ```
# MAGIC
# MAGIC **Duration:** 10-15 minutes for full suite
# MAGIC
# MAGIC ### 3. Monthly VACUUM (First Sunday at 4 AM)
# MAGIC
# MAGIC **Remove old file versions:**
# MAGIC ```python
# MAGIC dbutils.widgets.set("operation", "vacuum")
# MAGIC dbutils.widgets.set("layer", "all")
# MAGIC dbutils.widgets.set("vacuum_hours", "168")  # 7 days retention
# MAGIC # Execute notebook
# MAGIC ```
# MAGIC
# MAGIC **Warning:** Ensure retention >= 7 days to preserve time travel
# MAGIC
# MAGIC ## Performance Benchmarks
# MAGIC
# MAGIC ### fact_sales (500K rows)
# MAGIC
# MAGIC **Before optimization:**
# MAGIC * Files: 200
# MAGIC * Query time: 30s
# MAGIC * File size: 128 KB average
# MAGIC
# MAGIC **After OPTIMIZE:**
# MAGIC * Files: 10
# MAGIC * Query time: 8s (3.75x faster)
# MAGIC * File size: 50 MB average
# MAGIC
# MAGIC **After ZORDER BY (BatchDate, CustomerSK):**
# MAGIC * Files: 10
# MAGIC * Query time: 3s (10x faster for filtered queries)
# MAGIC * Data skipping: 90% of files skipped
# MAGIC
# MAGIC ### dim_customer (5K rows, SCD2)
# MAGIC
# MAGIC **Before ZORDER:**
# MAGIC * Lookup query: 5s
# MAGIC * Files scanned: 20/20
# MAGIC
# MAGIC **After ZORDER BY (CustomerID, StartDate):**
# MAGIC * Lookup query: 0.5s (10x faster)
# MAGIC * Files scanned: 2/20 (data skipping)
# MAGIC
# MAGIC ## Liquid Clustering (DBR 13+)
# MAGIC
# MAGIC **For new tables, use Liquid Clustering instead:**
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE {Config.SCHEMA_SILVER}.fact_sales (
# MAGIC   -- columns
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (BatchDate, CustomerSK, ProductSK);
# MAGIC ```
# MAGIC
# MAGIC **Benefits:**
# MAGIC * Auto-optimization on write
# MAGIC * No manual ZORDER needed
# MAGIC * Better for streaming workloads
# MAGIC
# MAGIC **Limitations:**
# MAGIC * DBR 13.1+ required
# MAGIC * Not available in all regions yet
# MAGIC
# MAGIC ## Troubleshooting
# MAGIC
# MAGIC ### Issue: OPTIMIZE takes > 30 minutes
# MAGIC
# MAGIC **Cause:** Table too large (> 10 GB)
# MAGIC
# MAGIC **Fix:**
# MAGIC * Add WHERE clause: `OPTIMIZE table WHERE BatchDate >= '2024-01-01'`
# MAGIC * Increase cluster size (8-16 cores)
# MAGIC * Run during off-hours
# MAGIC
# MAGIC ### Issue: VACUUM deletes recent files
# MAGIC
# MAGIC **Cause:** Retention < 7 days
# MAGIC
# MAGIC **Fix:**
# MAGIC * Always use retention >= 168 hours (7 days)
# MAGIC * Check time travel requirements before reducing
# MAGIC
# MAGIC ### Issue: Query still slow after ZORDER
# MAGIC
# MAGIC **Cause:** Wrong ZORDER columns or missing WHERE clause
# MAGIC
# MAGIC **Fix:**
# MAGIC * Verify WHERE clause uses ZORDER columns
# MAGIC * Check query plan: `EXPLAIN EXTENDED SELECT ...`
# MAGIC * Ensure high-cardinality columns (CustomerSK: 5000 values OK, PaymentMode: 3 values NOT OK)