# Databricks notebook source
# MAGIC %md
# MAGIC # Drift Detector
# MAGIC
# MAGIC Schema and data distribution drift monitoring

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# DBTITLE 1,Configuration and Widgets
dbutils.widgets.text("table_name", "{Config.SCHEMA_SILVER}.fact_sales", "Table to Monitor")
dbutils.widgets.text("days_to_compare", "7", "Days to Compare")

table_name = dbutils.widgets.get("table_name")
days_to_compare = int(dbutils.widgets.get("days_to_compare"))

# COMMAND ----------

# DBTITLE 1,Schema Drift Detection
from datetime import datetime, timedelta

# Get current schema
current_schema = spark.table(table_name).schema

# Get historical schema from Delta log (if available)
try:
    history_df = spark.sql(f"DESCRIBE HISTORY {table_name}")
    history_df = history_df.filter(f"timestamp < current_timestamp() - interval {days_to_compare} days")
    history_df = history_df.orderBy("version", ascending=False).limit(1)

    # Action inside try block to catch Spark execution errors
    row_count = history_df.count()

    if row_count > 0:
        old_version = history_df.select("version").collect()[0][0]
        old_df = spark.read.format("delta").option("versionAsOf", old_version).table(table_name)
        old_schema = old_df.schema

        # Compare schemas
        current_cols = set([f.name for f in current_schema.fields])
        old_cols = set([f.name for f in old_schema.fields])

        added_cols = current_cols - old_cols
        removed_cols = old_cols - current_cols

        print("SCHEMA DRIFT ANALYSIS")
        print("="*80)
        print(f"Comparing current schema with version {old_version} ({days_to_compare} days ago)")
        print(f"\nColumns added: {len(added_cols)}")
        if added_cols:
            for col in added_cols:
                print(f"  + {col}")

        print(f"\nColumns removed: {len(removed_cols)}")
        if removed_cols:
            for col in removed_cols:
                print(f"  - {col}")

        # Check for type changes
        common_cols = current_cols.intersection(old_cols)
        type_changes = []
        for col in common_cols:
            current_type = [f.dataType for f in current_schema.fields if f.name == col][0]
            old_type = [f.dataType for f in old_schema.fields if f.name == col][0]
            if str(current_type) != str(old_type):
                type_changes.append((col, str(old_type), str(current_type)))

        print(f"\nType changes: {len(type_changes)}")
        if type_changes:
            for col, old_t, new_t in type_changes:
                print(f"  {col}: {old_t} -> {new_t}")
    else:
        print(f"No historical versions found from {days_to_compare} days ago")
except Exception as e:
    print(f"Could not analyze schema drift: {e}")

# COMMAND ----------

# DBTITLE 1,Row Count Drift
from pyspark.sql.functions import col, count, to_date

df = spark.table(table_name)

# Find date column for partitioning
date_cols = [f.name for f in df.schema.fields if "date" in f.name.lower() or "timestamp" in f.name.lower()]

if date_cols:
    date_col = date_cols[0]
    print(f"\nROW COUNT DRIFT ANALYSIS (by {date_col})")
    print("="*80)

    # Get row counts by date for last N days
    df_filtered = df.withColumn("analysis_date", to_date(col(date_col)))
    row_counts = df_filtered.groupBy("analysis_date").agg(count("*").alias("row_count")).orderBy("analysis_date", ascending=False).limit(days_to_compare * 2)

    display(row_counts)

    # Calculate average and detect anomalies
    row_counts_list = row_counts.select("row_count").collect()
    if len(row_counts_list) > 1:
        avg_rows = sum([r[0] for r in row_counts_list]) / len(row_counts_list)
        latest_count = row_counts_list[0][0]
        deviation_pct = ((latest_count - avg_rows) / avg_rows * 100) if avg_rows > 0 else 0

        print(f"\nAverage daily row count: {avg_rows:,.0f}")
        print(f"Latest daily row count: {latest_count:,}")
        print(f"Deviation from average: {deviation_pct:+.2f}%")

        if abs(deviation_pct) > 20:
            print(f"\nWARNING: Row count anomaly detected (>{abs(deviation_pct):.0f}% deviation)")
else:
    print("\nNo date columns found for row count drift analysis")

# COMMAND ----------

# DBTITLE 1,Data Distribution Drift (Numeric Columns)
from pyspark.sql.functions import avg, stddev

numeric_cols = [f.name for f in df.schema.fields if str(f.dataType) in ["IntegerType", "LongType", "DoubleType", "FloatType", "DecimalType"]]

if numeric_cols:
    print("\nDATA DISTRIBUTION DRIFT (Numeric Columns)")
    print("="*80)

    # Compare statistics for numeric columns
    for col_name in numeric_cols[:5]:  # Limit to first 5 numeric columns
        stats = df.select(
            avg(col(col_name)).alias("avg_val"),
            stddev(col(col_name)).alias("stddev_val")
        ).collect()[0]

        print(f"\n{col_name}:")
        print(f"  Average: {stats['avg_val']:.2f}" if stats['avg_val'] else "  Average: NULL")
        print(f"  Std Dev: {stats['stddev_val']:.2f}" if stats['stddev_val'] else "  Std Dev: NULL")
        print("  - Compare with historical baseline to detect drift")

# COMMAND ----------

# DBTITLE 1,Recommendations
print("\nMONITORING RECOMMENDATIONS")
print("="*80)
print("- Run this notebook phase-based to track schema evolution")
print("- Set up alerts for schema changes in production")
print("- Store baseline statistics in control table for automated drift detection")
print("- Add rescue columns (_rescued_data) for unexpected schema changes")
print("- Document all intentional schema changes in change log")

# COMMAND ----------

# DBTITLE 1,Statistical Drift Tests
# Statistical Tests for Distribution Drift
if numeric_cols:
    print("\nSTATISTICAL DRIFT TESTS")
    print("="*80)

    from pyspark.sql.functions import kurtosis, skewness

    for col_name in numeric_cols[:5]:
        # Calculate distribution metrics
        dist_metrics = df.select(
            avg(col(col_name)).alias("mean"),
            stddev(col(col_name)).alias("stddev"),
            skewness(col(col_name)).alias("skewness"),
            kurtosis(col(col_name)).alias("kurtosis")
        ).collect()[0]

        print(f"\n{col_name}:")
        print(f"  Mean: {dist_metrics['mean']:.2f}" if dist_metrics['mean'] else "  Mean: NULL")
        print(f"  Std Dev: {dist_metrics['stddev']:.2f}" if dist_metrics['stddev'] else "  Std Dev: NULL")
        print(f"  Skewness: {dist_metrics['skewness']:.3f}" if dist_metrics['skewness'] else "  Skewness: NULL")
        print(f"  Kurtosis: {dist_metrics['kurtosis']:.3f}" if dist_metrics['kurtosis'] else "  Kurtosis: NULL")

        # Interpret skewness
        if dist_metrics['skewness']:
            if abs(dist_metrics['skewness']) < 0.5:
                print("  Distribution: Approximately symmetric")
            elif dist_metrics['skewness'] > 0:
                print("  Distribution: Right-skewed (tail on right)")
            else:
                print("  Distribution: Left-skewed (tail on left)")

        print("  Note: Store these metrics as baseline for future comparisons")

# COMMAND ----------

# DBTITLE 1,Baseline Comparison
# Baseline Comparison Framework
print("\nBASELINE COMPARISON FRAMEWORK")
print("="*80)

# Example baseline storage (in production, store in a control table)
baseline_example = {
    "table_name": table_name,
    "baseline_date": "2026-05-01",
    "row_count": 1095000,
    "column_count": len(df.columns),
    "avg_finalprice": 20864.25,
    "stddev_finalprice": 15234.50
}

print("To enable automated drift detection:")
print("")
print("1. CREATE BASELINE TABLE:")
print("   CREATE TABLE sap.monitoring.drift_baselines (")
print("     table_name STRING,")
print("     column_name STRING,")
print("     baseline_date DATE,")
print("     metric_name STRING,")
print("     metric_value DOUBLE,")
print("     recorded_at TIMESTAMP")
print("   )")
print("")
print("2. STORE CURRENT METRICS AS BASELINE:")
print("   INSERT INTO sap.monitoring.drift_baselines")
print("   VALUES ('{table_name}', 'finalprice', current_date(),")
print("           'avg', 20864.25, current_timestamp())")
print("")
print("3. COMPARE AGAINST BASELINE IN FUTURE RUNS:")
print("   - Calculate % deviation from baseline")
print("   - Alert if deviation > 20%")
print("   - Track drift trends over time")
print("")
print(f"Example Baseline (stored): {baseline_example}")

# COMMAND ----------

# DBTITLE 1,Alerting Thresholds
# Alerting Thresholds and Actions
print("\nALERTING FRAMEWORK")
print("="*80)

alerts = []

# Alert 1: Schema changes detected
try:
    if 'added_cols' in locals() and len(added_cols) > 0:
        alerts.append(("CRITICAL", "Schema Drift", f"{len(added_cols)} columns added"))
    if 'removed_cols' in locals() and len(removed_cols) > 0:
        alerts.append(("CRITICAL", "Schema Drift", f"{len(removed_cols)} columns removed"))
    if 'type_changes' in locals() and len(type_changes) > 0:
        alerts.append(("CRITICAL", "Schema Drift", f"{len(type_changes)} type changes"))
except:
    pass

# Alert 2: Row count anomalies
if 'deviation_pct' in locals() and abs(deviation_pct) > 20:
    alerts.append(("WARNING", "Row Count Anomaly", f"{deviation_pct:+.1f}% deviation from average"))

# Alert 3: Missing date partitions
if date_cols and 'row_counts_list' in locals():
    if len(row_counts_list) < days_to_compare:
        alerts.append(("WARNING", "Missing Data", f"Only {len(row_counts_list)} days found, expected {days_to_compare}"))

# Display alerts
if alerts:
    print("ALERTS DETECTED:")
    print("")
    for severity, category, message in alerts:
        icon = "!!!" if severity == "CRITICAL" else "!!"
        print(f"  {icon} [{severity}] {category}: {message}")

    print("")
    print("RECOMMENDED ACTIONS:")
    print("  1. Review drift analysis output above")
    print("  2. Check data pipeline logs for errors")
    print("  3. Validate source data quality")
    print("  4. Contact data engineering team if critical alerts present")
else:
    print("No alerts detected - all metrics within acceptable thresholds")
    print("")
    print("NEXT STEPS:")
    print("  - Store current metrics as baseline for future comparisons")
    print("  - Schedule this notebook to run phase-based")
    print("  - Set up email notifications for critical alerts")

# COMMAND ----------

# DBTITLE 1,Drift Trend Visualization
# Drift Trend Visualization
if date_cols and 'row_counts' in locals():
    print("\nDRIFT TREND VISUALIZATION")
    print("="*80)
    print("See displayed chart above for visual trend analysis")
    print("")
    print("Trend Analysis Tips:")
    print("  - Look for sudden spikes or drops")
    print("  - Check for missing dates (gaps in timeline)")
    print("  - Identify cyclical patterns (phase-based/monthly)")
    print("  - Compare weekday vs weekend patterns")
    print("")
    print("For Advanced Analysis:")
    print("  - Export drift metrics to BI tool (Tableau, PowerBI)")
    print("  - Create automated dashboards")
    print("  - Set up anomaly detection ML models")
    print("  - Track drift across multiple tables simultaneously")

# COMMAND ----------

# DBTITLE 1,Streaming Quality Gates - Real-Time Monitoring
# MAGIC %md
# MAGIC # Streaming Quality Gates Extension
# MAGIC
# MAGIC Real-time monitoring for streaming tables with automated alerting.
# MAGIC
# MAGIC Features:
# MAGIC - NULL rate monitoring (alert if > 5%)
# MAGIC - Out-of-range detection (negative prices, excessive quantities)
# MAGIC - Checkpoint health monitoring
# MAGIC - Schema drift tracking (streaming vs baseline)
# MAGIC
# MAGIC Run against streaming tables: streaming_customer_360, streaming_rfm_live, streaming_product_performance

# COMMAND ----------

# DBTITLE 1,Initialize Streaming Baseline Table
# Create baseline table for streaming quality comparisons
baseline_table = f"{Config.CATALOG}.{Config.SCHEMA_SILVER}.streaming_quality_baselines"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {baseline_table} (
        stream_name STRING,
        column_name STRING,
        metric_name STRING,
        metric_value DOUBLE,
        schema_json STRING,
        recorded_at TIMESTAMP,
        recorded_by STRING
    )
    USING DELTA
    PARTITIONED BY (stream_name)
""")

print(f"Baseline table ready: {baseline_table}")

# COMMAND ----------

# DBTITLE 1,Streaming NULL Rate Monitor
# Streaming-specific check: NULL rate monitoring
print("\n" + "="*70)
print("STREAMING NULL RATE MONITORING")
print("="*70)

null_threshold = 5.0
null_alerts = []

try:
    df = spark.table(table_name)
    total_rows = df.count()

    if total_rows > 0:
        print(f"Total rows: {total_rows:,}\n")

        for field in df.schema.fields:
            col_name = field.name
            null_count = df.filter(col(col_name).isNull()).count()
            null_rate = (null_count / total_rows) * 100

            if null_rate > null_threshold:
                null_alerts.append({"column": col_name, "rate": null_rate})
                print(f"ALERT: {col_name} - {null_rate:.2f}% NULL ({null_count:,} rows)")
            elif null_rate > 0:
                print(f"  {col_name} - {null_rate:.2f}% NULL")

        if not null_alerts:
            print("All columns within threshold")
    else:
        print("WARNING: Table is empty")

except Exception as e:
    print(f"NULL rate check error: {e}")

print(f"\nNull Alerts: {len(null_alerts)}")

# COMMAND ----------

# DBTITLE 1,Streaming Range Validation
# Out-of-range detection for streaming data
print("\n" + "="*70)
print("OUT-OF-RANGE VALUE DETECTION")
print("="*70)

range_alerts = []
numeric_cols = [f.name for f in df.schema.fields
                if str(f.dataType) in ["IntegerType", "LongType", "DoubleType", "FloatType"]]

range_rules = []
for col_name in numeric_cols:
    if "price" in col_name.lower() or "revenue" in col_name.lower():
        range_rules.append((col_name, "< 0", "Negative financial value"))
    if "quantity" in col_name.lower() or "count" in col_name.lower():
        range_rules.append((col_name, "> 10000", "Excessive quantity"))
        range_rules.append((col_name, "< 0", "Negative quantity"))

if range_rules:
    print(f"Checking {len(range_rules)} range rules...\n")

    for col_name, condition, desc in range_rules:
        violations = df.filter(f"{col_name} {condition}").count()
        if violations > 0:
            pct = (violations / total_rows) * 100
            range_alerts.append({"column": col_name, "rule": desc, "count": violations})
            print(f"ALERT: {col_name} - {desc} ({violations:,}, {pct:.2f}%)")

    if not range_alerts:
        print("All values in range")
else:
    print("No numeric columns with range rules")

print(f"\nRange Alerts: {len(range_alerts)}")

# COMMAND ----------

# DBTITLE 1,Streaming Checkpoint Health
# Checkpoint health monitoring for active streams
print("\n" + "="*70)
print("CHECKPOINT HEALTH MONITORING")
print("="*70)

checkpoint_alerts = []
active_streams = spark.streams.active

if len(active_streams) == 0:
    print("WARNING: No active streaming queries")
    checkpoint_alerts.append({"issue": "No active streams"})
else:
    print(f"Active streams: {len(active_streams)}\n")
    for query in active_streams:
        print(f"Query ID: {query.id}")
        print(f"Name: {query.name if query.name else 'unnamed'}")
        print(f"Status: {query.status}")

        if hasattr(query, 'lastProgress') and query.lastProgress:
            progress = query.lastProgress
            if 'numInputRows' in progress:
                print(f"  Input rows: {progress['numInputRows']:,}")
            if 'processedRowsPerSecond' in progress:
                print(f"  Throughput: {progress['processedRowsPerSecond']:.2f} rows/sec")
        print()

print(f"Checkpoint Alerts: {len(checkpoint_alerts)}")