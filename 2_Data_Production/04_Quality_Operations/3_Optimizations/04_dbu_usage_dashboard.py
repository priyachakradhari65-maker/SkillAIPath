# Databricks notebook source
# DBTITLE 1,DBU Usage Dashboard - Configuration
# TITLE: DBU Usage Dashboard & Cost Monitoring
# PURPOSE: Track compute costs using system tables for budget control and optimization
# EDITION: Premium Edition Required (system.billing.usage table)
# AUDIENCE: FinOps, Data Engineering Leaders, Cost Analysts

# SYSTEM TABLES OVERVIEW:
# - system.billing.usage: DBU consumption by workspace, cluster, job, user
# - system.compute.clusters: Cluster configuration and runtime
# - system.workflow.job_runs: Job execution history with compute attribution

# PRODUCTION NOTES:
# - Schedule: Daily refresh at 7 AM (after billing data sync)
# - Alerting: Notify if daily DBU > threshold (e.g., 500 DBUs)
# - Retention: Keep 90 days of historical cost data
# - Access: Restrict to finance and leadership (contains cost data)

import sys
sys.path.append('/Workspace/Shared/SkillAIPath-June/00_UTILS')

%run /Workspace/Shared/SkillAIPath-June/00_UTILS/01_config

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta, date
import pandas as pd

print("DBU Usage Dashboard - SkillAIPath")
print(f"Execution Time: {datetime.now().isoformat()}")
print("\nFree Edition Note: system.billing.usage requires Premium/Enterprise Edition")
print("For Community Edition: Use cluster event logs or job run metrics instead")

# COMMAND ----------

# DBTITLE 1,Query DBU Usage from System Tables
# MAGIC %sql
# MAGIC -- CELL: Extract DBU Consumption Data
# MAGIC -- Query system.billing.usage for last 30 days
# MAGIC
# MAGIC -- Premium Edition Only: Replace with mock data for Free Edition
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dbu_usage AS
# MAGIC SELECT
# MAGIC     usage_date,
# MAGIC     workspace_id,
# MAGIC     cluster_id,
# MAGIC     sku_name,
# MAGIC     usage_unit,
# MAGIC     usage_quantity,
# MAGIC     billing_origin_product,
# MAGIC     usage_metadata
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
# MAGIC     AND usage_unit = 'DBU'
# MAGIC ORDER BY usage_date DESC;
# MAGIC
# MAGIC -- Check data availability
# MAGIC SELECT
# MAGIC     MIN(usage_date) AS earliest_date,
# MAGIC     MAX(usage_date) AS latest_date,
# MAGIC     COUNT(*) AS total_records,
# MAGIC     SUM(usage_quantity) AS total_dbus
# MAGIC FROM dbu_usage;
# MAGIC
# MAGIC -- Free Edition Workaround: Simulate usage data
# MAGIC -- CREATE OR REPLACE TEMPORARY VIEW dbu_usage AS
# MAGIC -- SELECT
# MAGIC --     CURRENT_DATE() - INTERVAL (id % 30) DAYS AS usage_date,
# MAGIC --     'cluster-001' AS cluster_id,
# MAGIC --     'ALL_PURPOSE_COMPUTE' AS sku_name,
# MAGIC --     'DBU' AS usage_unit,
# MAGIC --     CAST((RAND() * 100) AS DECIMAL(10,2)) AS usage_quantity
# MAGIC -- FROM RANGE(1000);

# COMMAND ----------

# DBTITLE 1,Calculate Daily DBU Costs
# CELL: Calculate Daily Cost Metrics
# Aggregate DBU usage by day, cluster type, and job

print("=" * 60)
print("DAILY DBU USAGE ANALYSIS")
print("=" * 60)

# DBU pricing (example - replace with actual contract rates)
DBU_PRICE_MAP = {
    'ALL_PURPOSE_COMPUTE': 0.55,  # $/DBU for interactive
    'JOBS_COMPUTE': 0.15,          # $/DBU for automated jobs
    'JOBS_LIGHT_COMPUTE': 0.10,   # $/DBU for lightweight jobs
    'SQL_COMPUTE': 0.22,           # $/DBU for SQL warehouses
    'DLT_COMPUTE': 0.30            # $/DBU for Delta Live Tables
}

try:
    # Load usage data
    dbu_df = spark.table('dbu_usage')

    # Add cost calculation
    daily_cost = dbu_df.withColumn(
        'dbu_price',
        F.when(F.col('sku_name').contains('ALL_PURPOSE'), F.lit(DBU_PRICE_MAP['ALL_PURPOSE_COMPUTE']))
         .when(F.col('sku_name').contains('JOBS'), F.lit(DBU_PRICE_MAP['JOBS_COMPUTE']))
         .when(F.col('sku_name').contains('SQL'), F.lit(DBU_PRICE_MAP['SQL_COMPUTE']))
         .when(F.col('sku_name').contains('DLT'), F.lit(DBU_PRICE_MAP['DLT_COMPUTE']))
         .otherwise(F.lit(0.20))  # Default price
    ).withColumn(
        'cost_usd',
        F.col('usage_quantity') * F.col('dbu_price')
    )

    # Daily aggregation
    daily_summary = daily_cost.groupBy('usage_date').agg(
        F.sum('usage_quantity').alias('total_dbus'),
        F.sum('cost_usd').alias('total_cost_usd'),
        F.countDistinct('cluster_id').alias('active_clusters')
    ).orderBy('usage_date')

    # Display recent trends
    print("\n--- Last 7 Days DBU Usage ---")
    recent_days = daily_summary.filter(
        F.col('usage_date') >= F.current_date() - 7
    ).toPandas()

    if not recent_days.empty:
        print(recent_days.to_string(index=False))

        # Calculate metrics
        total_dbus = recent_days['total_dbus'].sum()
        total_cost = recent_days['total_cost_usd'].sum()
        avg_daily_dbus = recent_days['total_dbus'].mean()
        avg_daily_cost = recent_days['total_cost_usd'].mean()

        print(f"\n--- 7-Day Summary ---")
        print(f"Total DBUs: {total_dbus:,.2f}")
        print(f"Total Cost: ${total_cost:,.2f}")
        print(f"Avg Daily DBUs: {avg_daily_dbus:,.2f}")
        print(f"Avg Daily Cost: ${avg_daily_cost:,.2f}")
    else:
        print("No usage data available for last 7 days")

except Exception as e:
    print(f"ERROR: {str(e)}")
    print("\nNote: system.billing.usage requires Premium Edition")
    print("For Free Edition: Use job run metrics or cluster event logs")

print("\n" + "=" * 60)

# COMMAND ----------

# DBTITLE 1,DBU Usage by Cluster Type
# MAGIC %sql
# MAGIC -- CELL: Break Down Usage by Compute Type
# MAGIC -- Identify interactive vs automated workload costs
# MAGIC
# MAGIC SELECT
# MAGIC     sku_name,
# MAGIC     COUNT(DISTINCT cluster_id) AS cluster_count,
# MAGIC     SUM(usage_quantity) AS total_dbus,
# MAGIC     ROUND(SUM(usage_quantity) / SUM(SUM(usage_quantity)) OVER() * 100, 2) AS percent_of_total,
# MAGIC     CASE
# MAGIC         WHEN sku_name LIKE '%ALL_PURPOSE%' THEN 0.55
# MAGIC         WHEN sku_name LIKE '%JOBS%' THEN 0.15
# MAGIC         WHEN sku_name LIKE '%SQL%' THEN 0.22
# MAGIC         WHEN sku_name LIKE '%DLT%' THEN 0.30
# MAGIC         ELSE 0.20
# MAGIC     END AS assumed_dbu_price,
# MAGIC     ROUND(SUM(usage_quantity) *
# MAGIC         CASE
# MAGIC             WHEN sku_name LIKE '%ALL_PURPOSE%' THEN 0.55
# MAGIC             WHEN sku_name LIKE '%JOBS%' THEN 0.15
# MAGIC             WHEN sku_name LIKE '%SQL%' THEN 0.22
# MAGIC             WHEN sku_name LIKE '%DLT%' THEN 0.30
# MAGIC             ELSE 0.20
# MAGIC         END, 2) AS estimated_cost_usd
# MAGIC FROM dbu_usage
# MAGIC GROUP BY sku_name
# MAGIC ORDER BY total_dbus DESC;
# MAGIC
# MAGIC -- Insights:
# MAGIC -- - All-purpose clusters cost 3-5x more than job clusters per DBU
# MAGIC -- - Consider migrating interactive workloads to jobs where possible
# MAGIC -- - SQL warehouses offer good balance for ad-hoc analytics

# COMMAND ----------

# DBTITLE 1,Top Cost-Driving Clusters
# CELL: Identify High-Cost Clusters
# Top 10 clusters by DBU consumption for optimization targeting

print("=" * 60)
print("TOP 10 COST-DRIVING CLUSTERS")
print("=" * 60)

try:
    dbu_df = spark.table('dbu_usage')

    # Aggregate by cluster
    cluster_costs = dbu_df.groupBy('cluster_id', 'sku_name').agg(
        F.sum('usage_quantity').alias('total_dbus'),
        F.countDistinct('usage_date').alias('active_days')
    ).withColumn(
        'dbu_price',
        F.when(F.col('sku_name').contains('ALL_PURPOSE'), F.lit(0.55))
         .when(F.col('sku_name').contains('JOBS'), F.lit(0.15))
         .otherwise(F.lit(0.20))
    ).withColumn(
        'estimated_cost',
        F.col('total_dbus') * F.col('dbu_price')
    ).withColumn(
        'avg_daily_dbus',
        F.col('total_dbus') / F.col('active_days')
    ).orderBy(F.col('total_dbus').desc()).limit(10)

    top_clusters_pdf = cluster_costs.toPandas()

    if not top_clusters_pdf.empty:
        print("\n", top_clusters_pdf.to_string(index=False))

        # Highlight optimization opportunities
        print("\n--- Optimization Recommendations ---")
        for idx, row in top_clusters_pdf.iterrows():
            if row['avg_daily_dbus'] > 100:
                print(f"\nCluster {row['cluster_id']}:")
                print(f"  High usage: {row['avg_daily_dbus']:.2f} DBUs/day")
                if 'ALL_PURPOSE' in row['sku_name']:
                    print(f"  Consider: Convert to job cluster (save ~$0.40 per DBU)")
                    potential_savings = row['total_dbus'] * (0.55 - 0.15)
                    print(f"  Potential 30-day savings: ${potential_savings:,.2f}")
    else:
        print("No cluster usage data available")

except Exception as e:
    print(f"ERROR: {str(e)}")

print("\n" + "=" * 60)

# COMMAND ----------

# DBTITLE 1,Cost Trend Visualization
# CELL: Visualize Cost Trends
# 30-day trend chart for budget tracking

import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

print("=" * 60)
print("COST TREND VISUALIZATION")
print("=" * 60)

try:
    # Get daily cost data
    dbu_df = spark.table('dbu_usage')

    daily_cost = dbu_df.withColumn(
        'dbu_price',
        F.when(F.col('sku_name').contains('ALL_PURPOSE'), F.lit(0.55))
         .when(F.col('sku_name').contains('JOBS'), F.lit(0.15))
         .when(F.col('sku_name').contains('SQL'), F.lit(0.22))
         .otherwise(F.lit(0.20))
    ).withColumn(
        'cost_usd',
        F.col('usage_quantity') * F.col('dbu_price')
    ).groupBy('usage_date').agg(
        F.sum('usage_quantity').alias('total_dbus'),
        F.sum('cost_usd').alias('total_cost')
    ).orderBy('usage_date').toPandas()

    if not daily_cost.empty and len(daily_cost) > 0:
        # Convert to datetime
        daily_cost['usage_date'] = pd.to_datetime(daily_cost['usage_date'])

        # Create figure
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))

        # Plot 1: Daily Cost
        ax1.plot(daily_cost['usage_date'], daily_cost['total_cost'],
                marker='o', linewidth=2, markersize=4, color='#1f77b4')
        ax1.set_title('Daily DBU Cost Trend (Last 30 Days)', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Date')
        ax1.set_ylabel('Cost (USD)', fontsize=12)
        ax1.grid(True, alpha=0.3)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))

        # Add average line
        avg_cost = daily_cost['total_cost'].mean()
        ax1.axhline(y=avg_cost, color='red', linestyle='--',
                   label=f'Average: ${avg_cost:.2f}/day')
        ax1.legend()

        # Plot 2: DBU Consumption
        ax2.bar(daily_cost['usage_date'], daily_cost['total_dbus'],
               color='#ff7f0e', alpha=0.7)
        ax2.set_title('Daily DBU Consumption', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Date')
        ax2.set_ylabel('DBUs', fontsize=12)
        ax2.grid(True, alpha=0.3, axis='y')
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))

        plt.tight_layout()
        plt.savefig('/tmp/dbu_cost_trend.png', dpi=100, bbox_inches='tight')

        print("\nChart saved to: /tmp/dbu_cost_trend.png")
        print(f"30-Day Total Cost: ${daily_cost['total_cost'].sum():,.2f}")
        print(f"30-Day Total DBUs: {daily_cost['total_dbus'].sum():,.2f}")
        print(f"Average Daily Cost: ${avg_cost:.2f}")

        # Display chart in notebook
        display(fig)
    else:
        print("No data available for visualization")

except Exception as e:
    print(f"ERROR: {str(e)}")
    print("Visualization requires billing data access")

print("\n" + "=" * 60)

# COMMAND ----------

# DBTITLE 1,Cost Alerting Logic
# CELL: Cost Alert Detection
# Trigger alerts when usage exceeds thresholds

print("=" * 60)
print("COST ALERTING & BUDGET MONITORING")
print("=" * 60)

# Define alert thresholds
DAILY_DBU_THRESHOLD = 500  # Alert if > 500 DBUs/day
DAILY_COST_THRESHOLD = 200  # Alert if > $200/day
WEEKLY_BUDGET = 1000  # Weekly budget: $1000

alerts = []

try:
    dbu_df = spark.table('dbu_usage')

    # Check today's usage
    today = date.today()
    today_usage = dbu_df.filter(
        F.col('usage_date') == today
    ).withColumn(
        'dbu_price',
        F.when(F.col('sku_name').contains('ALL_PURPOSE'), F.lit(0.55))
         .when(F.col('sku_name').contains('JOBS'), F.lit(0.15))
         .otherwise(F.lit(0.20))
    ).withColumn(
        'cost_usd',
        F.col('usage_quantity') * F.col('dbu_price')
    ).agg(
        F.sum('usage_quantity').alias('total_dbus'),
        F.sum('cost_usd').alias('total_cost')
    ).collect()

    if today_usage:
        today_dbus = today_usage[0]['total_dbus'] or 0
        today_cost = today_usage[0]['total_cost'] or 0

        print(f"\n--- Today's Usage ({today}) ---")
        print(f"DBUs: {today_dbus:,.2f}")
        print(f"Cost: ${today_cost:,.2f}")

        # Check thresholds
        if today_dbus > DAILY_DBU_THRESHOLD:
            alerts.append({
                'severity': 'WARNING',
                'message': f'Daily DBU usage ({today_dbus:.2f}) exceeded threshold ({DAILY_DBU_THRESHOLD})'
            })

        if today_cost > DAILY_COST_THRESHOLD:
            alerts.append({
                'severity': 'CRITICAL',
                'message': f'Daily cost (${today_cost:.2f}) exceeded budget (${DAILY_COST_THRESHOLD})'
            })

    # Check 7-day trend
    week_ago = today - timedelta(days=7)
    week_usage = dbu_df.filter(
        (F.col('usage_date') >= week_ago) & (F.col('usage_date') <= today)
    ).withColumn(
        'dbu_price',
        F.when(F.col('sku_name').contains('ALL_PURPOSE'), F.lit(0.55))
         .when(F.col('sku_name').contains('JOBS'), F.lit(0.15))
         .otherwise(F.lit(0.20))
    ).withColumn(
        'cost_usd',
        F.col('usage_quantity') * F.col('dbu_price')
    ).agg(
        F.sum('cost_usd').alias('total_cost')
    ).collect()

    if week_usage:
        week_cost = week_usage[0]['total_cost'] or 0

        print(f"\n--- Last 7 Days ---")
        print(f"Total Cost: ${week_cost:,.2f}")
        print(f"Weekly Budget: ${WEEKLY_BUDGET:,.2f}")
        print(f"Budget Remaining: ${WEEKLY_BUDGET - week_cost:,.2f}")

        if week_cost > WEEKLY_BUDGET:
            alerts.append({
                'severity': 'CRITICAL',
                'message': f'Weekly cost (${week_cost:.2f}) exceeded budget (${WEEKLY_BUDGET})'
            })
        elif week_cost > (WEEKLY_BUDGET * 0.8):
            alerts.append({
                'severity': 'WARNING',
                'message': f'Weekly cost (${week_cost:.2f}) at 80% of budget'
            })

    # Display alerts
    print("\n" + "=" * 60)
    if len(alerts) > 0:
        print("ALERTS TRIGGERED")
        print("=" * 60)
        for alert in alerts:
            print(f"\n[{alert['severity']}] {alert['message']}")

        # Production: Send notifications
        # Example: send_slack_alert(alerts)
        # Example: send_email_alert(alerts)
    else:
        print("NO ALERTS - Usage within budget")
        print("=" * 60)

except Exception as e:
    print(f"ERROR: {str(e)}")

print("\n" + "=" * 60)

# COMMAND ----------

# DBTITLE 1,Monthly Cost Projection
# CELL: Monthly Cost Forecast
# Project end-of-month cost based on current trends

from datetime import datetime
import calendar

print("=" * 60)
print("MONTHLY COST PROJECTION")
print("=" * 60)

try:
    dbu_df = spark.table('dbu_usage')

    # Get current month data
    current_month = datetime.now().month
    current_year = datetime.now().year
    current_day = datetime.now().day

    # Days in current month
    days_in_month = calendar.monthrange(current_year, current_month)[1]
    days_remaining = days_in_month - current_day

    # Calculate month-to-date cost
    mtd_cost = dbu_df.filter(
        (F.year('usage_date') == current_year) &
        (F.month('usage_date') == current_month)
    ).withColumn(
        'dbu_price',
        F.when(F.col('sku_name').contains('ALL_PURPOSE'), F.lit(0.55))
         .when(F.col('sku_name').contains('JOBS'), F.lit(0.15))
         .otherwise(F.lit(0.20))
    ).withColumn(
        'cost_usd',
        F.col('usage_quantity') * F.col('dbu_price')
    ).agg(
        F.sum('cost_usd').alias('total_cost'),
        F.sum('usage_quantity').alias('total_dbus')
    ).collect()

    if mtd_cost:
        mtd_total = mtd_cost[0]['total_cost'] or 0
        mtd_dbus = mtd_cost[0]['total_dbus'] or 0

        # Calculate daily average and project
        avg_daily_cost = mtd_total / current_day if current_day > 0 else 0
        projected_month_cost = mtd_total + (avg_daily_cost * days_remaining)

        print(f"\n--- {calendar.month_name[current_month]} {current_year} ---")
        print(f"Days Elapsed: {current_day} / {days_in_month}")
        print(f"\nMonth-to-Date:")
        print(f"  Cost: ${mtd_total:,.2f}")
        print(f"  DBUs: {mtd_dbus:,.2f}")
        print(f"  Avg Daily: ${avg_daily_cost:,.2f}")
        print(f"\nProjection:")
        print(f"  Projected Month-End Cost: ${projected_month_cost:,.2f}")
        print(f"  Days Remaining: {days_remaining}")
        print(f"  Estimated Remaining Cost: ${avg_daily_cost * days_remaining:,.2f}")

        # Budget comparison (example: $5000/month)
        MONTHLY_BUDGET = 5000
        budget_pct = (projected_month_cost / MONTHLY_BUDGET) * 100

        print(f"\n--- Budget Analysis ---")
        print(f"Monthly Budget: ${MONTHLY_BUDGET:,.2f}")
        print(f"Projected Usage: {budget_pct:.1f}% of budget")

        if budget_pct > 100:
            print(f"WARNING: Projected to exceed budget by ${projected_month_cost - MONTHLY_BUDGET:,.2f}")
        elif budget_pct > 80:
            print(f"CAUTION: Projected to use {budget_pct:.1f}% of budget")
        else:
            print(f"HEALTHY: Well within budget")
    else:
        print("No data available for current month")

except Exception as e:
    print(f"ERROR: {str(e)}")

print("\n" + "=" * 60)

# COMMAND ----------

# DBTITLE 1,Cost Optimization Recommendations
# CELL: Generate Optimization Recommendations
# Actionable cost savings opportunities

print("=" * 60)
print("COST OPTIMIZATION RECOMMENDATIONS")
print("=" * 60)

recommendations = []

try:
    dbu_df = spark.table('dbu_usage')

    # Recommendation 1: All-purpose cluster usage
    all_purpose_usage = dbu_df.filter(
        F.col('sku_name').contains('ALL_PURPOSE')
    ).agg(
        F.sum('usage_quantity').alias('total_dbus')
    ).collect()[0]['total_dbus'] or 0

    if all_purpose_usage > 1000:
        potential_savings = all_purpose_usage * (0.55 - 0.15)  # Convert to jobs compute
        recommendations.append({
            'priority': 'HIGH',
            'category': 'Cluster Type Optimization',
            'finding': f'{all_purpose_usage:.2f} DBUs on all-purpose clusters',
            'action': 'Migrate schedulable workloads to job clusters',
            'potential_savings': f'${potential_savings:,.2f}/month'
        })

    # Recommendation 2: Idle cluster detection
    # Note: Requires system.compute.clusters table
    recommendations.append({
        'priority': 'MEDIUM',
        'category': 'Idle Cluster Management',
        'finding': 'Check for clusters idle > 30 minutes',
        'action': 'Set auto-termination to 15 minutes for interactive clusters',
        'potential_savings': '10-20% reduction in all-purpose costs'
    })

    # Recommendation 3: Spot instances
    recommendations.append({
        'priority': 'MEDIUM',
        'category': 'Spot Instance Adoption',
        'finding': 'Job clusters may not be using spot instances',
        'action': 'Enable spot instances for fault-tolerant workloads',
        'potential_savings': '50-70% reduction on job compute costs'
    })

    # Recommendation 4: Photon engine
    recommendations.append({
        'priority': 'LOW',
        'category': 'Photon Engine',
        'finding': 'SQL-heavy workloads detected',
        'action': 'Enable Photon for 2-3x query speedup (offsets DBU increase)',
        'potential_savings': 'Faster queries = lower total DBU consumption'
    })

    # Display recommendations
    print("\n")
    for idx, rec in enumerate(recommendations, 1):
        print(f"{idx}. [{rec['priority']}] {rec['category']}")
        print(f"   Finding: {rec['finding']}")
        print(f"   Action: {rec['action']}")
        print(f"   Savings: {rec['potential_savings']}")
        print()

    print("=" * 60)
    print("NEXT STEPS")
    print("=" * 60)
    print("\n1. Review top cost-driving clusters (see cell above)")
    print("2. Audit cluster configurations for autoscaling/termination settings")
    print("3. Schedule cost review meeting with FinOps/Engineering")
    print("4. Implement automated cost alerting (Slack/Email)")
    print("5. Set up phase-based cost review dashboard")

except Exception as e:
    print(f"ERROR: {str(e)}")

print("\n" + "=" * 60)

# COMMAND ----------

# DBTITLE 1,Production Deployment Notes
# MAGIC %md
# MAGIC # DBU Usage Dashboard - Production Deployment Guide
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC **System Tables Access:**
# MAGIC - Requires Premium or Enterprise Edition
# MAGIC - User must have `SELECT` on `system.billing.usage`
# MAGIC - Enable System Tables in Admin Console (one-time setup)
# MAGIC
# MAGIC **Free Edition Workaround:**
# MAGIC ```python
# MAGIC # Use job run metrics and cluster event logs
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC w = WorkspaceClient()
# MAGIC
# MAGIC # Query job runs with runtime
# MAGIC job_runs = w.jobs.list_runs(limit=1000)
# MAGIC for run in job_runs:
# MAGIC     runtime_sec = (run.end_time - run.start_time) / 1000
# MAGIC     # Estimate DBUs based on cluster size and runtime
# MAGIC ```
# MAGIC
# MAGIC ## Scheduling
# MAGIC
# MAGIC **Recommended Schedule:**
# MAGIC ```
# MAGIC Frequency: Daily at 7 AM
# MAGIC Cluster: Small job cluster (2 workers)
# MAGIC Timeout: 10 minutes
# MAGIC Alerts: Email to finance-team@company.com on threshold breach
# MAGIC ```
# MAGIC
# MAGIC **Databricks Job Configuration:**
# MAGIC ```python
# MAGIC {
# MAGIC   "name": "Daily DBU Cost Monitoring",
# MAGIC   "tasks": [{
# MAGIC     "task_key": "dbu_dashboard",
# MAGIC     "notebook_task": {
# MAGIC       "notebook_path": "/Shared/SkillAIPath-June/14_COST_MONITORING/01_dbu_usage_dashboard",
# MAGIC       "source": "WORKSPACE"
# MAGIC     },
# MAGIC     "job_cluster_key": "cost_monitoring_cluster"
# MAGIC   }],
# MAGIC   "schedule": {
# MAGIC     "quartz_cron_expression": "0 0 7 * * ?",
# MAGIC     "timezone_id": "America/Los_Angeles"
# MAGIC   },
# MAGIC   "email_notifications": {
# MAGIC     "on_failure": ["data-eng@company.com"]
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ## Cost Thresholds
# MAGIC
# MAGIC **Customize for Your Organization:**
# MAGIC ```python
# MAGIC # Edit in Cell 6: Cost Alerting Logic
# MAGIC DAILY_DBU_THRESHOLD = 500    # Your daily DBU limit
# MAGIC DAILY_COST_THRESHOLD = 200   # Your daily $ limit
# MAGIC WEEKLY_BUDGET = 1000         # Your phase-based budget
# MAGIC MONTHLY_BUDGET = 5000        # Your monthly budget
# MAGIC
# MAGIC # DBU pricing (get from your contract)
# MAGIC DBU_PRICE_MAP = {
# MAGIC     'ALL_PURPOSE_COMPUTE': 0.55,
# MAGIC     'JOBS_COMPUTE': 0.15,
# MAGIC     'SQL_COMPUTE': 0.22
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ## Alert Integration
# MAGIC
# MAGIC **Slack Notifications:**
# MAGIC ```python
# MAGIC import requests
# MAGIC
# MAGIC def send_slack_alert(alerts, webhook_url):
# MAGIC     message = {
# MAGIC         "text": "DBU Cost Alert",
# MAGIC         "attachments": [{
# MAGIC             "color": "danger" if alert['severity'] == 'CRITICAL' else "warning",
# MAGIC             "text": alert['message']
# MAGIC         } for alert in alerts]
# MAGIC     }
# MAGIC     requests.post(webhook_url, json=message)
# MAGIC
# MAGIC # Call in Cell 6 after alert detection
# MAGIC if len(alerts) > 0:
# MAGIC     SLACK_WEBHOOK = dbutils.secrets.get('sap', 'slack_webhook')
# MAGIC     send_slack_alert(alerts, SLACK_WEBHOOK)
# MAGIC ```
# MAGIC
# MAGIC **Email Notifications:**
# MAGIC ```python
# MAGIC # Use Databricks email notification API
# MAGIC if len(alerts) > 0:
# MAGIC     dbutils.notebook.exit({
# MAGIC         "status": "ALERT",
# MAGIC         "message": json.dumps(alerts)
# MAGIC     })
# MAGIC # Configure job to email on non-zero exit
# MAGIC ```
# MAGIC
# MAGIC ## Dashboard Refresh
# MAGIC
# MAGIC **System Table Lag:**
# MAGIC - Billing data typically lags by 2-4 hours
# MAGIC - Schedule dashboard refresh for 7 AM (captures previous day completely)
# MAGIC - Real-time monitoring not possible with system tables
# MAGIC
# MAGIC ## Storage & Retention
# MAGIC
# MAGIC **Archive Cost Data:**
# MAGIC ```sql
# MAGIC -- Create historical cost table
# MAGIC CREATE TABLE sap.monitoring.dbu_cost_history (
# MAGIC     usage_date DATE,
# MAGIC     total_dbus DECIMAL(10,2),
# MAGIC     total_cost_usd DECIMAL(10,2),
# MAGIC     cluster_breakdown MAP<STRING, DECIMAL(10,2)>,
# MAGIC     recorded_at TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Append daily snapshots
# MAGIC INSERT INTO sap.monitoring.dbu_cost_history
# MAGIC SELECT ... FROM system.billing.usage ...
# MAGIC ```
# MAGIC
# MAGIC ## Cost Optimization Strategies
# MAGIC
# MAGIC 1. **Convert Interactive to Jobs**: Save $0.40 per DBU
# MAGIC 2. **Enable Spot Instances**: Save 50-70% on job clusters
# MAGIC 3. **Auto-Termination**: Set to 15 minutes for dev clusters
# MAGIC 4. **Photon Engine**: 2-3x faster = lower total DBU
# MAGIC 5. **Cluster Right-Sizing**: Use autoscaling, avoid over-provisioning
# MAGIC
# MAGIC ## Troubleshooting
# MAGIC
# MAGIC **Issue: system.billing.usage not found**
# MAGIC - Solution: Enable System Tables in Admin Console
# MAGIC - Solution: Use workspace admin account
# MAGIC
# MAGIC **Issue: Empty results**
# MAGIC - Solution: Check date filter (data may lag 2-4 hours)
# MAGIC - Solution: Verify workspace has compute activity
# MAGIC
# MAGIC **Issue: Incorrect cost calculations**
# MAGIC - Solution: Update DBU_PRICE_MAP with actual contract rates
# MAGIC - Solution: Check for promotion/discount pricing