# Databricks notebook source
# Predictive Cost Budgeting with Time Series Forecasting
# FREE EDITION COMPATIBLE
# PURPOSE: Forecast next month's compute costs using historical trends
# June 2026

from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring
from datetime import datetime, timedelta
import pandas as pd

print("=" * 70)
print("PREDICTIVE COST BUDGETING")
print("=" * 70)

# Historical monthly cost data (simulated - Premium uses system.billing.usage)
months = pd.date_range(start='2025-07-01', end='2026-06-01', freq='MS')
monthly_costs = [
    8500, 9200, 8800, 10500, 11200, 10800, 12500, 13200, 12800, 14500, 15200, 14800
]

cost_history = pd.DataFrame({
    'month': months,
    'total_cost_usd': monthly_costs
})

df = spark.createDataFrame(cost_history)

# Calculate trend metrics
df = df.withColumn("month_num", monotonically_increasing_id())
df = df.withColumn("month_over_month_growth_pct",
    ((col("total_cost_usd") - lag("total_cost_usd", 1).over(Window.orderBy("month"))) / 
     lag("total_cost_usd", 1).over(Window.orderBy("month")) * 100)
)

print("\nHistorical Cost Trend:")
display(df)

# Simple forecast: Average growth rate applied to last month
avg_growth = df.filter(col("month_over_month_growth_pct").isNotNull()).agg(
    avg("month_over_month_growth_pct")
).collect()[0][0]

last_month_cost = df.orderBy(col("month").desc()).limit(1).collect()[0]["total_cost_usd"]

forecast_next_month = last_month_cost * (1 + avg_growth / 100)
forecast_low = forecast_next_month * 0.90
forecast_high = forecast_next_month * 1.10

print("\n" + "=" * 70)
print("COST FORECAST: July 2026")
print("=" * 70)
print(f"Last month (June 2026): ${last_month_cost:,.2f}")
print(f"Average monthly growth rate: {avg_growth:.2f} percent")
print(f"\nForecast for July 2026:")
print(f"  Low estimate (90 percent confidence): ${forecast_low:,.2f}")
print(f"  Most likely: ${forecast_next_month:,.2f}")
print(f"  High estimate (110 percent confidence): ${forecast_high:,.2f}")

# Budget alert thresholds
budget_limit = 16000
buffer = budget_limit - forecast_next_month

print(f"\nBudget Analysis:")
print(f"Monthly budget limit: ${budget_limit:,.2f}")
print(f"Forecasted spend: ${forecast_next_month:,.2f}")
print(f"Remaining buffer: ${buffer:,.2f} ({buffer / budget_limit * 100:.1f} percent)")

if forecast_next_month > budget_limit:
    print("\nWARNING: Forecasted spend exceeds budget!")
    print("Recommended actions:")
    print("1. Review and terminate idle clusters")
    print("2. Enable auto-stop for SQL warehouses")
    print("3. Right-size over-provisioned jobs")
    print("4. Consider spot instances for non-critical workloads")
elif buffer < budget_limit * 0.10:
    print("\nCAUTION: Budget buffer below 10 percent")
    print("Monitor spend closely and prepare optimization plan")
else:
    print("\nStatus: Budget healthy with adequate buffer")

print("\nNext Steps:")
print("1. Review forecast accuracy at month-end")
print("2. Update budget allocations by team")
print("3. Set alerts at 80 percent and 95 percent of budget")