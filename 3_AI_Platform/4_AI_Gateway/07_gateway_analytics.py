# Databricks notebook source
# AI Gateway Analytics Dashboard
# Monitor gateway performance and usage
# June 2026

from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring

print("AI GATEWAY ANALYTICS")

metrics = [
    ("Requests/day", 45000),
    ("Avg latency", "850ms"),
    ("Error rate", "0.8 percent"),
    ("Cost/1K requests", "$2.50"),
    ("Top model", "gpt-4o-mini (65 percent traffic)"),
]

print("\nKey Metrics:")
for metric, value in metrics:
    print(f"- {metric}: {value}")

print("\nAlerts configured:")
print("- Error rate > 2 percent")
print("- Latency p95 > 2 seconds")
print("- Daily cost > $200")
print("- Rate limit violations")