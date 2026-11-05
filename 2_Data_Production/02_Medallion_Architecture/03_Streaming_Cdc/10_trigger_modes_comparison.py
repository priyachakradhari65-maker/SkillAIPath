# Databricks notebook source
# DBTITLE 1,Streaming Trigger Modes
# MAGIC %md
# MAGIC # Databricks Streaming Trigger Modes
# MAGIC
# MAGIC ## Three Options:
# MAGIC
# MAGIC ### 1. availableNow (Recommended)
# MAGIC * Processes all available data, then stops
# MAGIC * Cost-efficient (cluster auto-terminates)
# MAGIC * Use for: Batch-like streaming (hourly/daily)
# MAGIC
# MAGIC ### 2. processingTime='10 seconds'
# MAGIC * Continuous micro-batches every 10 seconds
# MAGIC * Always-on cluster
# MAGIC * Use for: Real-time dashboards
# MAGIC
# MAGIC ### 3. continuous='1 second'
# MAGIC * Ultra-low latency (<1s)
# MAGIC * Most expensive
# MAGIC * Use for: Fraud detection, alerts
# MAGIC
# MAGIC ## SkillAIPath Pattern: availableNow
# MAGIC
# MAGIC Run streaming jobs on schedule (Jobs), not 24/7

# COMMAND ----------

# DBTITLE 1,Cost Comparison
# MAGIC %md
# MAGIC ## Cost Analysis
# MAGIC
# MAGIC | Trigger Mode | Cluster Uptime | DBU Cost | Use Case |
# MAGIC |--------------|----------------|----------|----------|
# MAGIC | availableNow | 10 min/hour | $5/day | Daily ETL |
# MAGIC | processingTime='10s' | 24 hours | $120/day | Real-time dashboards |
# MAGIC | continuous | 24 hours | $200/day | Fraud detection |
# MAGIC
# MAGIC **SkillAIPath Choice:** availableNow scheduled hourly = $5/day vs $120/day (24x savings)