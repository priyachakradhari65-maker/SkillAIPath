# Databricks notebook source
# DBTITLE 1,SkillAIPath Performance Benchmark Framework
# MAGIC %md
# MAGIC # SkillAIPath Performance Benchmark Framework
# MAGIC **Production Pipeline Monitoring - Regression Detection & Optimization Validation**
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC Continuous performance monitoring for SkillAIPath data warehouse.
# MAGIC
# MAGIC **Purpose:**
# MAGIC * Establish performance baselines for critical queries
# MAGIC * Detect regressions (2x slowdown threshold)
# MAGIC * Validate optimization improvements
# MAGIC * Track historical trends
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC **Layers Monitored:**
# MAGIC * Bronze: Streaming ingestion rates
# MAGIC * Silver: CDC processing duration (SCD2 + fact MERGE)
# MAGIC * Gold: Aggregation query performance
# MAGIC
# MAGIC **Benchmark Storage:**
# MAGIC * Results table: `{Config.SCHEMA_GOLD}.benchmark_results`
# MAGIC * Retention: 90 days
# MAGIC * Partitioned by: benchmark_date
# MAGIC
# MAGIC ## Critical Queries Benchmarked
# MAGIC
# MAGIC 1. Daily sales aggregation by product
# MAGIC 2. Customer dimension SCD2 lookup
# MAGIC 3. Fact-dimension star schema join
# MAGIC 4. Gold mart aggregation (RFM, revenue dashboard)
# MAGIC 5. Streaming checkpoint lag

# COMMAND ----------

# DBTITLE 1,Setup and Configuration
# SkillAIPath Benchmark Framework - Production Setup

import sys
import os
import time
import json
from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring
from datetime import datetime
import pandas as pd

# Load SkillAIPath Config
current_dir = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
project_root = os.path.dirname(current_dir)
config_path = os.path.join(project_root, "00_UTILS")
sys.path.append(config_path)
from config import Config

print("="*70)
print("SKILLAIPPATH PERFORMANCE BENCHMARK FRAMEWORK")
print("="*70)
print(f"Environment: {Config.ENVIRONMENT}")
print(f"Catalog: {Config.CATALOG}")
print(f"Purpose: Baseline + regression detection")
print(f"Metrics: Duration, shuffle, rows, data freshness")
print("="*70)

# Widgets
dbutils.widgets.dropdown("mode", "baseline", ["baseline", "regression", "validation"], "Benchmark Mode")
dbutils.widgets.dropdown("layer", "all", ["all", "bronze", "silver", "gold"], "Pipeline Layer")
dbutils.widgets.text("alert_threshold", "2.0", "Alert Threshold (multiplier)")

mode = dbutils.widgets.get("mode")
layer_filter = dbutils.widgets.get("layer")
alert_threshold = float(dbutils.widgets.get("alert_threshold"))

print(f"\nMode: {mode}")
print(f"Layer: {layer_filter}")
print(f"Alert threshold: {alert_threshold}x baseline")

# Benchmark results table
benchmark_table = f"{Config.CATALOG}.{Config.SCHEMA_GOLD}.benchmark_results"

# COMMAND ----------

# DBTITLE 1,Benchmark Execution Engine
def run_benchmark(query_name, query_sql, layer="silver", baseline_duration=None):
    """
    Execute query and capture performance metrics.

    Args:
        query_name: Unique identifier for the query
        query_sql: SQL query to benchmark
        layer: Pipeline layer (bronze/silver/gold)
        baseline_duration: Expected duration in seconds

    Returns:
        Dictionary with performance metrics
    """
    print(f"\n[BENCHMARK] {query_name}")
    print("-" * 70)

    start_time = time.time()
    try:
        df = spark.sql(query_sql)
        result_count = df.count()  # Force execution
        end_time = time.time()
        duration = end_time - start_time

        metrics = {
            "query_name": query_name,
            "layer": layer,
            "duration_sec": round(duration, 2),
            "result_count": result_count,
            "timestamp": datetime.now().isoformat(),
            "status": "SUCCESS",
            "environment": Config.ENVIRONMENT
        }

        # Regression detection
        if baseline_duration:
            threshold = baseline_duration * alert_threshold
            if duration > threshold:
                metrics["alert"] = "REGRESSION"
                metrics["slowdown_factor"] = round(duration / baseline_duration, 2)
                print(f"\n[ALERT] REGRESSION DETECTED")
                print(f"  Baseline: {baseline_duration}s")
                print(f"  Current: {duration}s")
                print(f"  Slowdown: {metrics['slowdown_factor']}x")
            else:
                metrics["alert"] = "OK"
                print(f"[OK] Performance within threshold")

        print(f"Duration: {duration:.2f}s | Rows: {result_count:,}")
        return metrics

    except Exception as e:
        end_time = time.time()
        duration = end_time - start_time
        print(f"[ERROR] {str(e)}")
        return {
            "query_name": query_name,
            "layer": layer,
            "duration_sec": round(duration, 2),
            "result_count": 0,
            "timestamp": datetime.now().isoformat(),
            "status": "FAILED",
            "error_message": str(e),
            "environment": Config.ENVIRONMENT
        }

print("Benchmark engine loaded")

# COMMAND ----------

# DBTITLE 1,Production Query Suite Definition
# Critical SkillAIPath queries to benchmark

test_queries = {
    # SILVER LAYER - Fact table queries
    "silver_sales_daily_aggregate": {
        "description": "Daily sales aggregation by product",
        "layer": "silver",
        "sql": f"""
            SELECT
                ProductSK,
                COUNT(*) as transaction_count,
                ROUND(SUM(FinalPrice) / 100000, 2) as revenue_lakhs
            FROM {Config.TBL_FACT_SALES}
            WHERE {Config.COL_BATCH_DATE} >= CURRENT_DATE - INTERVAL 30 DAYS
            GROUP BY ProductSK
            ORDER BY revenue_lakhs DESC
            LIMIT 100
        """,
        "expected_rows": 100,
        "baseline_duration_sec": 4.5
    },

    "silver_customer_scd2_lookup": {
        "description": "Active customer dimension lookup (SCD Type 2)",
        "layer": "silver",
        "sql": f"""
            SELECT
                CustomerSK,
                CustomerID,
                CustomerName,
                City,
                {Config.SCD_START_DATE_COL},
                {Config.SCD_END_DATE_COL}
            FROM {Config.TBL_DIM_CUSTOMER}
            WHERE {Config.SCD_IS_CURRENT_COL} = TRUE
        """,
        "expected_rows": 5000,
        "baseline_duration_sec": 2.0
    },

    "silver_fact_dim_star_join": {
        "description": "Star schema join (fact + 3 dimensions)",
        "layer": "silver",
        "sql": f"""
            SELECT
                c.CustomerName,
                p.ProductName,
                pc.CategoryName,
                COUNT(*) as order_count,
                ROUND(SUM(f.FinalPrice) / 100000, 2) as revenue_lakhs
            FROM {Config.TBL_FACT_SALES} f
            INNER JOIN {Config.TBL_DIM_CUSTOMER} c ON f.CustomerSK = c.CustomerSK
            INNER JOIN {Config.TBL_DIM_PRODUCT} p ON f.ProductSK = p.ProductSK
            INNER JOIN {Config.TBL_DIM_CATEGORY} pc ON p.CategoryID = pc.CategoryID
            WHERE f.{Config.COL_BATCH_DATE} >= CURRENT_DATE - INTERVAL 7 DAYS
              AND c.{Config.SCD_IS_CURRENT_COL} = TRUE
            GROUP BY c.CustomerName, p.ProductName, pc.CategoryName
            LIMIT 500
        """,
        "expected_rows": 500,
        "baseline_duration_sec": 6.0
    },

    # GOLD LAYER - Aggregation queries
    "gold_revenue_dashboard": {
        "description": "Revenue dashboard aggregation",
        "layer": "gold",
        "sql": f"""
            SELECT
                CategoryName,
                SUM(TotalRevenue) / 10000000 as revenue_cr
            FROM {Config.TBL_MART_PRODUCT_PERFORMANCE}
            GROUP BY CategoryName
            ORDER BY revenue_cr DESC
        """,
        "expected_rows": 8,
        "baseline_duration_sec": 3.0
    },

    "gold_customer_rfm": {
        "description": "RFM segmentation query",
        "layer": "gold",
        "sql": f"""
            SELECT
                rfm_segment,
                COUNT(*) as customer_count,
                AVG(recency_days) as avg_recency,
                AVG(total_revenue) as avg_revenue
            FROM {Config.TBL_MART_CUSTOMER_RFM}
            GROUP BY rfm_segment
        """,
        "expected_rows": 8,
        "baseline_duration_sec": 2.5
    },

    # BRONZE LAYER - Data freshness check
    "bronze_data_freshness": {
        "description": "Check latest batch ingestion timestamp",
        "layer": "bronze",
        "sql": f"""
            SELECT
                MAX({Config.COL_BATCH_DATE}) as latest_batch,
                COUNT(*) as record_count,
                MAX({Config.COL_LOADED_AT}) as latest_load_time
            FROM {Config.TBL_STG_SALES}
        """,
        "expected_rows": 1,
        "baseline_duration_sec": 1.5
    }
}

print(f"Test suite loaded: {len(test_queries)} queries defined")
if layer_filter != "all":
    filtered = {k: v for k, v in test_queries.items() if v["layer"] == layer_filter}
    print(f"Filtered to layer '{layer_filter}': {len(filtered)} queries")
    test_queries = filtered

# COMMAND ----------

# DBTITLE 1,Execute Benchmark Suite
# Run all benchmark queries

print("="*70)
print("EXECUTING BENCHMARK SUITE")
print("="*70)

benchmark_results = []

for qid, info in test_queries.items():
    print(f"\n[{len(benchmark_results)+1}/{len(test_queries)}] {qid}")

    if mode == "baseline":
        metrics = run_benchmark(
            query_name=qid,
            query_sql=info["sql"],
            layer=info["layer"]
        )
    else:
        metrics = run_benchmark(
            query_name=qid,
            query_sql=info["sql"],
            layer=info["layer"],
            baseline_duration=info["baseline_duration_sec"]
        )

    metrics["description"] = info["description"]
    metrics["mode"] = mode
    metrics["expected_rows"] = info.get("expected_rows", 0)
    benchmark_results.append(metrics)

    time.sleep(0.5)  # Brief pause between queries

print("\n" + "="*70)
print("BENCHMARK SUITE COMPLETE")
print("="*70)

# Summary statistics
df_results = pd.DataFrame(benchmark_results)
print("\nSummary Statistics:")
print(f"  Total queries: {len(benchmark_results)}")
print(f"  Successful: {df_results[df_results['status'] == 'SUCCESS'].shape[0]}")
print(f"  Failed: {df_results[df_results['status'] == 'FAILED'].shape[0]}")
if 'alert' in df_results.columns:
    regressions = df_results[df_results['alert'] == 'REGRESSION'].shape[0]
    if regressions > 0:
        print(f"\n[ALERT] {regressions} regression(s) detected!")
print(f"  Total duration: {df_results['duration_sec'].sum():.2f}s")

print("\nTop 5 slowest queries:")
print(df_results.nlargest(5, 'duration_sec')[['query_name', 'duration_sec', 'result_count']])

# COMMAND ----------

# DBTITLE 1,Persist Results to Gold Layer
# Save benchmark results to Delta table

print("\n" + "="*70)
print("PERSISTING BENCHMARK RESULTS")
print("="*70)

df_benchmark = spark.createDataFrame(benchmark_results)
df_benchmark = df_benchmark \
    .withColumn("benchmark_run_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
    .withColumn("spark_version", lit(spark.version)) \
    .withColumn("benchmark_date", current_date())

try:
    # Create table if not exists
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {benchmark_table} (
            query_name STRING,
            layer STRING,
            description STRING,
            duration_sec DOUBLE,
            result_count BIGINT,
            expected_rows BIGINT,
            timestamp STRING,
            status STRING,
            mode STRING,
            environment STRING,
            alert STRING,
            slowdown_factor DOUBLE,
            error_message STRING,
            benchmark_run_id STRING,
            spark_version STRING,
            benchmark_date DATE
        )
        USING DELTA
        PARTITIONED BY (benchmark_date)
        LOCATION 'dbfs:/mnt/sap/gold/benchmark_results'
    """)

    df_benchmark.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("benchmark_date") \
        .saveAsTable(benchmark_table)

    print(f"[SUCCESS] Results saved to: {benchmark_table}")
    print(f"  Records written: {len(benchmark_results)}")
    print(f"  Run ID: {df_benchmark.select('benchmark_run_id').first()[0]}")
    print(f"  Partition: {df_benchmark.select('benchmark_date').first()[0]}")

except Exception as e:
    print(f"[ERROR] Failed to save results: {str(e)}")
    print("Results available in-memory only")

# COMMAND ----------

# DBTITLE 1,Performance Trend Analysis
# MAGIC %sql
# MAGIC -- Analyze performance trends over time
# MAGIC
# MAGIC SELECT
# MAGIC   query_name,
# MAGIC   layer,
# MAGIC   description,
# MAGIC   DATE(timestamp) as benchmark_date,
# MAGIC   MIN(duration_sec) as min_duration,
# MAGIC   AVG(duration_sec) as avg_duration,
# MAGIC   MAX(duration_sec) as max_duration,
# MAGIC   COUNT(*) as run_count
# MAGIC FROM ${benchmark_table}
# MAGIC WHERE timestamp >= CURRENT_DATE - INTERVAL 30 DAYS
# MAGIC   AND status = 'SUCCESS'
# MAGIC GROUP BY query_name, layer, description, DATE(timestamp)
# MAGIC ORDER BY layer, query_name, benchmark_date DESC
# MAGIC LIMIT 100;
# MAGIC
# MAGIC -- INTERPRETATION:
# MAGIC -- Look for max_duration increasing over time (performance degradation)
# MAGIC -- avg_duration 2x higher than min_duration indicates regression

# COMMAND ----------

# DBTITLE 1,Regression Detection Query
# MAGIC %sql
# MAGIC -- Detect queries that have regressed vs baseline
# MAGIC
# MAGIC WITH baseline AS (
# MAGIC   SELECT
# MAGIC     query_name,
# MAGIC     MIN(duration_sec) as baseline_duration,
# MAGIC     MAX(timestamp) as baseline_timestamp
# MAGIC   FROM ${benchmark_table}
# MAGIC   WHERE mode = 'baseline'
# MAGIC     AND status = 'SUCCESS'
# MAGIC   GROUP BY query_name
# MAGIC ),
# MAGIC latest AS (
# MAGIC   SELECT
# MAGIC     query_name,
# MAGIC     duration_sec as latest_duration,
# MAGIC     timestamp as latest_timestamp,
# MAGIC     layer,
# MAGIC     description
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       query_name,
# MAGIC       duration_sec,
# MAGIC       timestamp,
# MAGIC       layer,
# MAGIC       description,
# MAGIC       ROW_NUMBER() OVER (PARTITION BY query_name ORDER BY timestamp DESC) as rn
# MAGIC     FROM ${benchmark_table}
# MAGIC     WHERE status = 'SUCCESS'
# MAGIC   )
# MAGIC   WHERE rn = 1
# MAGIC )
# MAGIC SELECT
# MAGIC   l.layer,
# MAGIC   l.query_name,
# MAGIC   l.description,
# MAGIC   ROUND(b.baseline_duration, 2) as baseline_sec,
# MAGIC   ROUND(l.latest_duration, 2) as latest_sec,
# MAGIC   ROUND(l.latest_duration / b.baseline_duration, 2) as slowdown_factor,
# MAGIC   CASE
# MAGIC     WHEN l.latest_duration > b.baseline_duration * 2 THEN 'CRITICAL'
# MAGIC     WHEN l.latest_duration > b.baseline_duration * 1.5 THEN 'WARNING'
# MAGIC     ELSE 'OK'
# MAGIC   END as alert_level,
# MAGIC   DATE(b.baseline_timestamp) as baseline_date,
# MAGIC   DATE(l.latest_timestamp) as latest_date
# MAGIC FROM latest l
# MAGIC INNER JOIN baseline b ON l.query_name = b.query_name
# MAGIC WHERE l.latest_duration > b.baseline_duration * 1.2  -- 20% slower
# MAGIC ORDER BY slowdown_factor DESC;
# MAGIC
# MAGIC -- CRITICAL: 2x or slower
# MAGIC -- WARNING: 1.5x-2x slower
# MAGIC -- OK: Within 1.5x of baseline

# COMMAND ----------

# DBTITLE 1,Production Runbook
# MAGIC %md
# MAGIC # SkillAIPath Benchmark Framework - Production Runbook
# MAGIC
# MAGIC ## Daily Operations
# MAGIC
# MAGIC ### 1. Baseline Establishment (After Major Changes)
# MAGIC
# MAGIC **When to run:**
# MAGIC * After SCD Type 2 deployment
# MAGIC * After gold mart schema changes
# MAGIC * After cluster configuration updates
# MAGIC * Monthly refresh
# MAGIC
# MAGIC **Steps:**
# MAGIC ```python
# MAGIC # Set mode to baseline
# MAGIC dbutils.widgets.set("mode", "baseline")
# MAGIC
# MAGIC # Run all layers
# MAGIC dbutils.widgets.set("layer", "all")
# MAGIC
# MAGIC # Execute notebook
# MAGIC ```
# MAGIC
# MAGIC ### 2. Daily Regression Checks
# MAGIC
# MAGIC **Schedule:** Every day at 8 AM (after overnight batch completion)
# MAGIC
# MAGIC **Steps:**
# MAGIC ```python
# MAGIC dbutils.widgets.set("mode", "regression")
# MAGIC dbutils.widgets.set("layer", "all")
# MAGIC dbutils.widgets.set("alert_threshold", "2.0")
# MAGIC ```
# MAGIC
# MAGIC **Alert criteria:**
# MAGIC * CRITICAL: 2x slower than baseline
# MAGIC * WARNING: 1.5x-2x slower
# MAGIC * Trigger investigation if 3+ consecutive days in WARNING
# MAGIC
# MAGIC ### 3. Post-Optimization Validation
# MAGIC
# MAGIC **After applying optimizations:**
# MAGIC ```python
# MAGIC dbutils.widgets.set("mode", "validation")
# MAGIC dbutils.widgets.set("layer", "gold")  # or specific layer
# MAGIC ```
# MAGIC
# MAGIC ## Common Performance Issues
# MAGIC
# MAGIC ### Issue 1: Dimension Explosion (SCD Type 2)
# MAGIC
# MAGIC **Symptom:** Star schema joins 10x slower
# MAGIC
# MAGIC **Root cause:** Dimension grew from 50K to 2M rows (with history)
# MAGIC
# MAGIC **Fix:**
# MAGIC * Filter dimension by IsCurrent = TRUE in joins
# MAGIC * Add WHERE clause: `WHERE dim.IsCurrent = TRUE`
# MAGIC * Update baseline after fix
# MAGIC
# MAGIC ### Issue 2: Gold Aggregation Timeout
# MAGIC
# MAGIC **Symptom:** Gold mart queries exceed 30s
# MAGIC
# MAGIC **Root cause:** Silver fact table not partitioned/optimized
# MAGIC
# MAGIC **Fix:**
# MAGIC * OPTIMIZE fact_sales ZORDER BY (BatchDate, ProductSK)
# MAGIC * Add partition pruning: `WHERE BatchDate >= '2024-01-01'`
# MAGIC * Rebuild gold mart with incremental logic
# MAGIC
# MAGIC ### Issue 3: Bronze Ingestion Lag
# MAGIC
# MAGIC **Symptom:** Data freshness check shows 6+ hour lag
# MAGIC
# MAGIC **Root cause:** Streaming checkpoint not progressing
# MAGIC
# MAGIC **Fix:**
# MAGIC * Check streaming query status
# MAGIC * Verify Auto Loader configuration
# MAGIC * Review watermark tracking table
# MAGIC
# MAGIC ## Escalation Path
# MAGIC
# MAGIC 1. **CRITICAL alert** → Slack #data-engineering immediately
# MAGIC 2. **3+ consecutive WARNING** → Create Jira ticket
# MAGIC 3. **All queries 1.5x slower** → Investigate cluster/Spark config changes
# MAGIC
# MAGIC ## Maintenance
# MAGIC
# MAGIC **Weekly:**
# MAGIC * Review trend analysis query
# MAGIC * Archive benchmarks older than 90 days
# MAGIC
# MAGIC **Monthly:**
# MAGIC * Re-establish baselines
# MAGIC * Update expected_rows in query definitions
# MAGIC * Review and tune alert thresholds