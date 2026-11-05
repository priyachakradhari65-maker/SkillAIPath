# Databricks notebook source
# Batch vs Real-Time ML Inference - Comprehensive Comparison
# FREE EDITION COMPATIBLE
# PURPOSE: Compare batch and real-time inference patterns with cost analysis and decision framework
# Updated: June 2026 - Latest serving patterns

from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring
from datetime import datetime, timedelta
import pandas as pd
import time

print("=" * 80)
print("BATCH VS REAL-TIME INFERENCE COMPARISON")
print("=" * 80)

# Comparison Framework
comparison_data = [
    {
        "dimension": "Latency",
        "batch": "Minutes to hours (scheduled runs)",
        "realtime": "Milliseconds to seconds per request",
        "winner": "Real-Time"
    },
    {
        "dimension": "Throughput",
        "batch": "Millions of records per run",
        "realtime": "Hundreds to thousands per second",
        "winner": "Batch"
    },
    {
        "dimension": "Cost per 1M predictions",
        "batch": "$5-15 (Spark cluster hourly rate)",
        "realtime": "$50-200 (endpoint uptime + inference)",
        "winner": "Batch"
    },
    {
        "dimension": "Infrastructure",
        "batch": "Ephemeral cluster (starts/stops)",
        "realtime": "Always-on endpoint (or scale-to-zero)",
        "winner": "Batch"
    },
    {
        "dimension": "Freshness",
        "batch": "Stale (last batch run)",
        "realtime": "Fresh (computed on-demand)",
        "winner": "Real-Time"
    },
    {
        "dimension": "Use Case Fit",
        "batch": "Nightly reports, bulk scoring",
        "realtime": "User-facing apps, real-time decisions",
        "winner": "Context-Dependent"
    },
    {
        "dimension": "Complexity",
        "batch": "Simple (Spark DataFrame operations)",
        "realtime": "Complex (API management, retries, auth)",
        "winner": "Batch"
    },
    {
        "dimension": "Scalability",
        "batch": "Horizontal scaling (add workers)",
        "realtime": "Vertical scaling (larger endpoint) + auto-scaling",
        "winner": "Batch"
    },
]

df_comparison = spark.createDataFrame(comparison_data)
display(df_comparison)

print("\n" + "=" * 80)
print("COST ANALYSIS: 1 Million Predictions")
print("=" * 80)

# Cost Scenario 1: Batch Inference
print("\nScenario 1: BATCH INFERENCE")
print("-" * 40)

batch_config = {
    "records": 1_000_000,
    "cluster_type": "i3.xlarge",
    "cluster_size": 8,
    "dbu_per_hour": 2.5,
    "dbu_cost_usd": 0.15,
    "runtime_hours": 0.5,
}

batch_cost = batch_config["dbu_per_hour"] * batch_config["dbu_cost_usd"] * batch_config["runtime_hours"] * batch_config["cluster_size"]
batch_cost_per_1k = (batch_cost / batch_config["records"]) * 1000

print(f"Records: {batch_config['records']:,}")
print(f"Cluster: {batch_config['cluster_size']}x {batch_config['cluster_type']} ({batch_config['dbu_per_hour']} DBU/hour each)")
print(f"Runtime: {batch_config['runtime_hours']} hours")
print(f"Total DBUs: {batch_config['dbu_per_hour'] * batch_config['runtime_hours'] * batch_config['cluster_size']}")
print(f"Total Cost: ${batch_cost:.2f}")
print(f"Cost per 1K predictions: ${batch_cost_per_1k:.4f}")

# Cost Scenario 2: Real-Time Inference
print("\nScenario 2: REAL-TIME INFERENCE (Always-On Endpoint)")
print("-" * 40)

realtime_config = {
    "records": 1_000_000,
    "endpoint_size": "Small",
    "dbu_per_hour": 0.50,
    "dbu_cost_usd": 0.22,
    "avg_latency_ms": 50,
    "daily_requests": 100_000,
    "uptime_hours_per_day": 24,
}

days_for_1m = realtime_config["records"] / realtime_config["daily_requests"]
realtime_cost = realtime_config["dbu_per_hour"] * realtime_config["dbu_cost_usd"] * realtime_config["uptime_hours_per_day"] * days_for_1m
realtime_cost_per_1k = (realtime_cost / realtime_config["records"]) * 1000

print(f"Records: {realtime_config['records']:,}")
print(f"Endpoint Size: {realtime_config['endpoint_size']} ({realtime_config['dbu_per_hour']} DBU/hour)")
print(f"Avg Latency: {realtime_config['avg_latency_ms']} ms")
print(f"Daily Requests: {realtime_config['daily_requests']:,}")
print(f"Days to reach 1M: {days_for_1m:.1f}")
print(f"Total Uptime: {realtime_config['uptime_hours_per_day'] * days_for_1m:.1f} hours")
print(f"Total DBUs: {realtime_config['dbu_per_hour'] * realtime_config['uptime_hours_per_day'] * days_for_1m:.2f}")
print(f"Total Cost: ${realtime_cost:.2f}")
print(f"Cost per 1K predictions: ${realtime_cost_per_1k:.4f}")

# Cost Scenario 3: Real-Time with Scale-to-Zero
print("\nScenario 3: REAL-TIME WITH SCALE-TO-ZERO")
print("-" * 40)

scale_to_zero_config = {
    "records": 1_000_000,
    "endpoint_size": "Small",
    "dbu_per_hour": 0.50,
    "dbu_cost_usd": 0.22,
    "avg_latency_ms": 50,
    "daily_requests": 100_000,
    "active_hours_per_day": 8,
}

days_for_1m_stz = scale_to_zero_config["records"] / scale_to_zero_config["daily_requests"]
stz_cost = scale_to_zero_config["dbu_per_hour"] * scale_to_zero_config["dbu_cost_usd"] * scale_to_zero_config["active_hours_per_day"] * days_for_1m_stz
stz_cost_per_1k = (stz_cost / scale_to_zero_config["records"]) * 1000
savings_pct = ((realtime_cost - stz_cost) / realtime_cost) * 100

print(f"Records: {scale_to_zero_config['records']:,}")
print(f"Endpoint Size: {scale_to_zero_config['endpoint_size']} (scale-to-zero enabled)")
print(f"Active Hours/Day: {scale_to_zero_config['active_hours_per_day']} (idle 16 hours)")
print(f"Days to reach 1M: {days_for_1m_stz:.1f}")
print(f"Total Active Time: {scale_to_zero_config['active_hours_per_day'] * days_for_1m_stz:.1f} hours")
print(f"Total DBUs: {scale_to_zero_config['dbu_per_hour'] * scale_to_zero_config['active_hours_per_day'] * days_for_1m_stz:.2f}")
print(f"Total Cost: ${stz_cost:.2f}")
print(f"Cost per 1K predictions: ${stz_cost_per_1k:.4f}")
print(f"Savings vs Always-On: ${realtime_cost - stz_cost:.2f} ({savings_pct:.1f}%)")

# Cost Summary
print("\n" + "=" * 80)
print("COST SUMMARY (1 Million Predictions)")
print("=" * 80)

cost_summary = [
    ("Batch Inference", batch_cost, batch_cost_per_1k),
    ("Real-Time (Always-On)", realtime_cost, realtime_cost_per_1k),
    ("Real-Time (Scale-to-Zero)", stz_cost, stz_cost_per_1k),
]

for method, total_cost, cost_per_1k in cost_summary:
    print(f"{method:30s} - Total: ${total_cost:8.2f}  |  Per 1K: ${cost_per_1k:.4f}")

print("\nKey Insight: Batch is 10-50x cheaper for large volumes")

# Decision Framework
print("\n" + "=" * 80)
print("DECISION FRAMEWORK: When to Use Each Pattern")
print("=" * 80)

decision_framework = """
+--------------------+------------------------+---------------------------+
|   Requirement      |   Use Batch            |   Use Real-Time           |
+--------------------+------------------------+---------------------------+
| Latency            | > 1 minute acceptable  | < 1 second required       |
| Volume             | > 1M predictions/day   | < 100K predictions/day    |
| Freshness          | Daily/hourly OK        | Must be current           |
| User-facing        | No (internal reports)  | Yes (web/mobile apps)     |
| Cost sensitivity   | High (optimize cost)   | Low (optimize latency)    |
| Data availability  | All data ready upfront | Request-driven            |
+--------------------+------------------------+---------------------------+

USE BATCH FOR:
- Nightly customer segmentation (RFM scores)
- Daily churn risk scoring for all customers
- Weekly product recommendations for email campaigns
- Monthly credit risk assessments
- Bulk lead scoring from CRM exports
- Quarterly forecasting for all SKUs

USE REAL-TIME FOR:
- Fraud detection during payment checkout
- Dynamic pricing on e-commerce pages
- Personalized content recommendations (homepage)
- Chatbot intent classification
- Real-time anomaly detection (IoT sensors)
- A/B test variant assignment

HYBRID APPROACH (Best of Both):
1. Batch: Pre-compute scores nightly, store in Delta table
2. Real-Time: Serve pre-computed scores via fast key-value lookup
3. Real-Time Fallback: Compute on-demand if record not in cache
4. Result: Sub-millisecond latency + batch economics

Example Hybrid Pattern:
- Batch job: Score 10M customers every night → $15/day
- Real-time endpoint: Serve cached scores from Delta → $5/day
- Total: $20/day vs $200/day for pure real-time
"""

print(decision_framework)

# Performance Benchmarks
print("\n" + "=" * 80)
print("PERFORMANCE BENCHMARKS (Typical Values)")
print("=" * 80)

benchmarks = [
    {
        "pattern": "Batch (10M records)",
        "throughput": "500K records/minute",
        "latency_p50": "N/A",
        "latency_p95": "N/A",
        "cost_per_1m": "$10"
    },
    {
        "pattern": "Real-Time (Small endpoint)",
        "throughput": "100 req/sec",
        "latency_p50": "50 ms",
        "latency_p95": "150 ms",
        "cost_per_1m": "$150"
    },
    {
        "pattern": "Real-Time (Medium endpoint)",
        "throughput": "500 req/sec",
        "latency_p50": "30 ms",
        "latency_p95": "100 ms",
        "cost_per_1m": "$300"
    },
    {
        "pattern": "Real-Time (Large endpoint)",
        "throughput": "2000 req/sec",
        "latency_p50": "20 ms",
        "latency_p95": "60 ms",
        "cost_per_1m": "$600"
    },
    {
        "pattern": "Hybrid (Batch + Cache)",
        "throughput": "10K req/sec (cached)",
        "latency_p50": "5 ms",
        "latency_p95": "20 ms",
        "cost_per_1m": "$15"
    },
]

df_benchmarks = spark.createDataFrame(benchmarks)
display(df_benchmarks)

# Implementation Code Samples
print("\n" + "=" * 80)
print("IMPLEMENTATION PATTERNS")
print("=" * 80)

print("""
1. BATCH INFERENCE PATTERN
--------------------------
from pyspark.sql.functions import col, lit, when, count, sum, avg, max, min, first, last, concat, concat_ws, trim, upper, lower, to_date, to_timestamp, date_format, year, month, day, row_number, rank, dense_rank, lag, lead, explode, array, struct, map_from_entries, coalesce, isnull, isnotnull, regexp_replace, substring
import mlflow

# Load model
model = mlflow.pyfunc.spark_udf(spark, model_uri="models:/churn_model/production")

# Load data
df_customers = spark.table("sap_dev.silver.dim_customer")

# Score all customers
df_scored = df_customers.withColumn(
    "churn_probability",
    model(struct(["recency", "frequency", "monetary"]))
)

# Save results
df_scored.write.mode("overwrite").saveAsTable("sap_dev.gold.customer_churn_scores")

# Schedule: Run daily at 2 AM via Databricks Job


2. REAL-TIME INFERENCE PATTERN
--------------------------------
import requests

# Call serving endpoint
url = "https://workspace.cloud.databricks.com/serving-endpoints/churn-model/invocations"
headers = {"Authorization": "Bearer <token>"}

payload = {
    "dataframe_records": [{"recency": 30, "frequency": 5, "monetary": 50000}]
}

response = requests.post(url, headers=headers, json=payload)
churn_prob = response.json()["predictions"][0]

if churn_prob > 0.7:
    trigger_retention_campaign(customer_id)


3. HYBRID PATTERN (Cache + Fallback)
--------------------------------------
def get_churn_score(customer_id):
    # Try cache first (Delta table from batch job)
    cached = spark.sql(f"""
        SELECT churn_probability 
        FROM sap_dev.gold.customer_churn_scores 
        WHERE customer_id = {customer_id}
          AND score_date = CURRENT_DATE()
    """).collect()
    
    if cached:
        return cached[0]["churn_probability"]
    
    # Fallback to real-time endpoint
    features = get_customer_features(customer_id)
    return call_realtime_endpoint(features)

# Best of both: Fast cache lookup (5ms) + on-demand fallback
""")

print("\n" + "=" * 80)
print("MIGRATION STRATEGY: Batch → Real-Time")
print("=" * 80)

print("""
Phase 1: Batch Only (Stage 1-2)
- Implement batch inference pipeline
- Schedule daily runs
- Serve results via batch tables
- Validate accuracy and cost

Phase 2: Hybrid (Stage 3-4)
- Deploy real-time endpoint
- Batch pre-computes 95% of predictions
- Real-time handles cache misses
- Monitor latency and error rates

Phase 3: Full Real-Time (Stage 5+)
- Migrate high-value workflows to real-time
- Keep batch for bulk/non-critical workloads
- Implement A/B test to compare patterns
- Optimize cost vs latency trade-off

Success Metrics:
- Latency: < 100ms p95
- Cost: < 2x batch baseline
- Availability: > 99.5%
- Error rate: < 0.1%
""")

print("\n" + "=" * 80)
print("RECOMMENDATIONS")
print("=" * 80)
print("""
1. Start with batch inference for all workloads
2. Migrate to real-time only when latency is critical
3. Use scale-to-zero for cost optimization
4. Implement hybrid pattern for best cost/latency balance
5. Monitor cost per 1K predictions phase-based
6. Right-size endpoints based on actual traffic
7. Cache frequently-requested predictions
8. Use batch for training data generation
""")

print("\nComparison complete.")