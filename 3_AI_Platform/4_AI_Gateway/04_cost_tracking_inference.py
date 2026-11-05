# Databricks notebook source
# AI Gateway Cost Tracking
# Track and analyze inference costs per model
# June 2026

from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring

print("AI GATEWAY COST TRACKING")

# Inference log analysis (from gateway inference table)
inference_data = [
    ("gpt-4o", 1250, 3500, 0.015),
    ("gpt-4o-mini", 800, 1200, 0.002),
    ("claude-3-sonnet", 1100, 2800, 0.012),
]

df = spark.createDataFrame(inference_data, ["model", "input_tokens", "output_tokens", "cost_usd"])
df = df.withColumn("total_tokens", col("input_tokens") + col("output_tokens"))

print("\nCost by Model:")
display(df.select("model", "total_tokens", "cost_usd"))

# Daily cost projection
daily_requests = 10000
df = df.withColumn("daily_cost_usd", col("cost_usd") * lit(daily_requests))

total_daily = df.agg(sum("daily_cost_usd")).collect()[0][0]
print(f"\nProjected daily cost: ${total_daily:,.2f}")
print(f"Monthly cost: ${total_daily * 30:,.2f}")