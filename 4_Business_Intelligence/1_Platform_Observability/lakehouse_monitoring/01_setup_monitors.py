# LAKEHOUSE MONITORING SETUP
# Purpose: Create automated monitors on critical tables
# Usage: Run once per table, then monitors run on schedule

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorInferenceLog, MonitorTimeSeries

w = WorkspaceClient()

# Monitor Configuration
TABLE_NAME = "main.bronze.sales"
MONITOR_NAME = f"{TABLE_NAME}_quality_monitor"

# Define metrics to track
PROFILE_METRICS = [
    "null_count",
    "distinct_count",
    "min",
    "max",
    "mean",
    "stddev"
]

# Drift detection thresholds
DRIFT_THRESHOLDS = {
    "null_rate_change": 0.05,  # Alert if NULL rate changes >5%
    "volume_change": 0.25,      # Alert if row count changes >25%
    "schema_change": True       # Always alert on schema changes
}

def create_monitor():
    """Create Lakehouse Monitor on target table"""
    
    print(f"Creating monitor for {TABLE_NAME}...")
    
    # Create time series monitor
    monitor = w.quality_monitors.create(
        table_name=TABLE_NAME,
        assets_dir=f"/Workspace/Shared/monitors/{TABLE_NAME}",
        output_schema_name="main.monitoring_output",
        inference_log=MonitorInferenceLog(
            granularities=["5 minutes", "30 minutes", "1 hour", "1 day"],
            model_id_col="model_version",
            prediction_col="prediction",
            timestamp_col="timestamp",
            label_col="actual_value"
        )
    )
    
    print(f"Monitor created: {monitor.monitor_id}")
    print(f"Dashboard: {monitor.dashboard_id}")
    return monitor

def configure_alerts():
    """Set up alert notifications"""
    
    print("Configuring alerts...")
    
    # Slack webhook (replace with actual webhook)
    slack_webhook = "https://hooks.slack.com/services/YOUR/WEBHOOK/HERE"
    
    # Alert conditions
    alert_config = {
        "conditions": [
            {
                "metric": "null_rate",
                "threshold": 0.05,
                "comparison": "greater_than",
                "message": "NULL rate exceeded 5% threshold"
            },
            {
                "metric": "row_count_change",
                "threshold": 0.25,
                "comparison": "absolute_change_greater_than",
                "message": "Row count changed more than 25%"
            }
        ],
        "notification_channels": [
            {"type": "slack", "webhook": slack_webhook},
            {"type": "email", "recipients": ["data-team@company.com"]}
        ]
    }
    
    print("Alerts configured successfully")
    return alert_config

def run_monitor_refresh():
    """Manually trigger monitor refresh"""
    
    print(f"Refreshing monitor for {TABLE_NAME}...")
    
    # Trigger refresh
    refresh = w.quality_monitors.run_refresh(
        table_name=TABLE_NAME
    )
    
    print(f"Refresh triggered: {refresh.refresh_id}")
    return refresh

if __name__ == "__main__":
    # Step 1: Create monitor
    monitor = create_monitor()
    
    # Step 2: Configure alerts
    alerts = configure_alerts()
    
    # Step 3: Run initial refresh
    refresh = run_monitor_refresh()
    
    print("\nMonitor setup complete!")
    print(f"View metrics at: /sql/dashboards/{monitor.dashboard_id}")
