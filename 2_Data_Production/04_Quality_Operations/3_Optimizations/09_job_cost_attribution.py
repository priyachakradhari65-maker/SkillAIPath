# Databricks notebook source
# Job-Level Cost Attribution & Chargeback
# FREE EDITION COMPATIBLE
# PURPOSE: Attribute compute costs to jobs/teams for chargeback
# June 2026

from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring

print("=" * 70)
print("JOB-LEVEL COST ATTRIBUTION & CHARGEBACK")
print("=" * 70)

# Job execution history with cost data (simulated)
job_costs = [
    ("job-etl-bronze", "Data Engineering", "Bronze ETL Pipeline", 45, 1200, 2.5, 600),
    ("job-ml-training", "ML Team", "Daily Model Training", 12, 3600, 15.0, 2700),
    ("job-reports", "Analytics", "Daily Reports", 180, 120, 0.25, 90),
    ("job-etl-silver", "Data Engineering", "Silver Transformations", 30, 800, 1.5, 360),
    ("job-ml-features", "ML Team", "Feature Engineering", 24, 1800, 7.5, 1350),
    ("job-dashboards", "Analytics", "Dashboard Refresh", 96, 180, 0.35, 126),
]

schema = "job_id STRING, team STRING, job_name STRING, executions INT, avg_duration_sec INT, avg_dbu DOUBLE, monthly_cost_usd INT"
df = spark.createDataFrame(job_costs, schema)

# Team-level aggregation
df_team_costs = df.groupBy("team").agg(
    sum("monthly_cost_usd").alias("total_monthly_cost"),
    sum("executions").alias("total_executions"),
    count("job_id").alias("job_count"),
    avg("avg_dbu").alias("avg_dbu_per_job"),
    (sum("monthly_cost_usd") / sum("executions")).alias("cost_per_execution")
)

print("\nCost Attribution by Team:")
display(df_team_costs.orderBy(col("total_monthly_cost").desc()))

# Job-level details
print("\nTop 5 Most Expensive Jobs:")
display(df.orderBy(col("monthly_cost_usd").desc()).limit(5))

# Chargeback summary
print("\nChargeback Summary:")
for row in df_team_costs.collect():
    print(f"{row['team']:20s} - ${row['total_monthly_cost']:,} ({row['job_count']} jobs, {row['total_executions']} runs)")

# Optimization recommendations
print("\nOptimization Recommendations:")
high_cost_jobs = df.filter(col("monthly_cost_usd") > 1000).collect()
for job in high_cost_jobs:
    print(f"- {job['job_name']}: ${job['monthly_cost_usd']}/month - Review for spot instances or schedule optimization")

print("\nChargeback Strategy:")
print("1. Allocate costs to cost centers monthly")
print("2. Set budget alerts at 80 percent threshold")
print("3. Review top 10 jobs quarterly for optimization")
print("4. Consider reserved instances for predictable workloads")