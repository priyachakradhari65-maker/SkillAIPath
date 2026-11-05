# Databricks notebook source
# A/B Testing with Traffic Split
# Deploy 2 model versions with traffic split for comparison
# June 2026

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

endpoint_config = {
    "name": "sap-churn-ab-test",
    "routes": [
        {"model_version": "v1", "traffic_percentage": 70},
        {"model_version": "v2_candidate", "traffic_percentage": 30},
    ]
}

print("A/B TEST DEPLOYMENT")
print(f"Model v1 (champion): 70 percent traffic")
print(f"Model v2 (challenger): 30 percent traffic")
print("\nMonitor for 7 days:")
print("- Compare accuracy, latency, error rates")
print("- Gradually shift traffic to better model")
print("- Full cutover if v2 shows 5 percent improvement")