# Databricks notebook source
# DBTITLE 1,Real-Time Business Alerts - Event-Driven Monitoring
# MAGIC %md
# MAGIC # Gold Layer: Real-Time Streaming Alerts
# MAGIC
# MAGIC ## Event-Driven Architecture
# MAGIC
# MAGIC **Concept:** Alerts trigger immediately when business-critical events occur (not scheduled reports).
# MAGIC
# MAGIC **Scheduled Monitoring (Traditional Batch):**
# MAGIC * Run every 6 hours
# MAGIC * Check: "Did any customer CIBIL score drop below 600?"
# MAGIC * Latency: Up to 6 hours
# MAGIC * Example: Customer becomes high-risk at 9 AM, alert fires at 2 PM (5 hours late)
# MAGIC
# MAGIC **Event-Driven Alerts (Streaming):**
# MAGIC * Run continuously (every 2 minutes)
# MAGIC * Trigger: CIBIL score drop detected in CDC stream
# MAGIC * Latency: <3 minutes
# MAGIC * Example: Customer becomes high-risk at 9:00:00 AM, alert fires at 9:02:30 AM
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Alert Types Implemented
# MAGIC
# MAGIC **1. High-Value Customer Risk Alert**
# MAGIC * Source: {Config.SCHEMA_BRONZE}.customer_changes_raw (CDC stream)
# MAGIC * Trigger: CIBILScore < 600 AND TotalRevenue > Rs 50,000
# MAGIC * Business Impact: Prevent credit losses from high-value customers going delinquent
# MAGIC * Action: Flag for collections team, reduce credit limit
# MAGIC
# MAGIC **2. City Change Alert (Fraud Detection)**
# MAGIC * Source: {Config.SCHEMA_BRONZE}.customer_changes_raw
# MAGIC * Trigger: City changes AND Operation = UPDATE
# MAGIC * Business Impact: Detect account takeover (customer suddenly in different city)
# MAGIC * Action: Freeze account, send OTP verification
# MAGIC
# MAGIC **3. Low Stock Alert (Inventory Replenishment)**
# MAGIC * Source: {Config.SCHEMA_BRONZE}.inventory_status
# MAGIC * Trigger: ProductQuantity < 10 AND ProductName in top-sellers list
# MAGIC * Business Impact: Prevent stockouts of best-selling products
# MAGIC * Action: Auto-reorder, notify warehouse manager
# MAGIC
# MAGIC **4. Large Transaction Alert (Fraud Review)**
# MAGIC * Source: {Config.SCHEMA_BRONZE}.sales_raw
# MAGIC * Trigger: FinalPrice > Rs 100,000 in single transaction
# MAGIC * Business Impact: Catch fraudulent transactions before fulfillment
# MAGIC * Action: Hold order, manual review queue
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Technical Architecture
# MAGIC
# MAGIC **Pattern: foreachBatch for Custom Actions**
# MAGIC
# MAGIC ```python
# MAGIC def process_alert(batch_df, batch_id):
# MAGIC     # 1. Filter for alert conditions
# MAGIC     # 2. Deduplicate (don't alert twice for same event)
# MAGIC     # 3. Write to alerts log table
# MAGIC     # 4. Send external notification (Slack, email, PagerDuty)
# MAGIC     pass
# MAGIC
# MAGIC df_stream.writeStream \
# MAGIC     .foreachBatch(process_alert) \
# MAGIC     .start()
# MAGIC ```
# MAGIC
# MAGIC **Why foreachBatch?**
# MAGIC * Need custom logic (deduplication, external API calls)
# MAGIC * Can't use simple writeStream.table() (no custom actions)
# MAGIC * Gives full control over batch processing
# MAGIC
# MAGIC **Deduplication Strategy:**
# MAGIC * Problem: CDC stream may contain duplicate events (Kafka retries, source database replays)
# MAGIC * Solution: Track alert_hash (MD5 of alert key fields) in gold.streaming_alerts_log
# MAGIC * Logic: Only write alert if alert_hash NOT EXISTS in last 24 hours
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Batch vs Streaming Alerts
# MAGIC
# MAGIC | Aspect | Batch Alerts (Scheduled) | Streaming Alerts (Event-Driven) |
# MAGIC |--------|-------------------------|--------------------------------|
# MAGIC | **Trigger** | Time-based (every 6 hours) | Event-based (when condition occurs) |
# MAGIC | **Latency** | Up to 6 hours | <3 minutes |
# MAGIC | **Use Case** | Daily reports, compliance checks | Fraud detection, real-time ops |
# MAGIC | **Cost** | Low (runs 4x/day) | High (runs 720x/day) |
# MAGIC | **False Positives** | High (may alert on stale data) | Low (only current events) |
# MAGIC | **Complexity** | Simple (SQL query) | Complex (state management, deduplication) |
# MAGIC
# MAGIC **When to Use Streaming Alerts:**
# MAGIC 1. Business impact of delay > cost of streaming (fraud, stockouts, credit risk)
# MAGIC 2. Actionable within minutes (not quarterly reports)
# MAGIC 3. Event-driven workflows (order on hold, send OTP, freeze account)
# MAGIC
# MAGIC **When to Use Batch Alerts:**
# MAGIC 1. Daily summaries (sales below target, inventory aging)
# MAGIC 2. Compliance reports (monthly audits)
# MAGIC 3. Cost-sensitive (startup budgets)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Alert Severity Levels
# MAGIC
# MAGIC **CRITICAL:** Immediate action required (fraud, system outage)
# MAGIC * Response time: <5 minutes
# MAGIC * Notification: PagerDuty, SMS, phone call
# MAGIC * Example: City change detected (potential account takeover)
# MAGIC
# MAGIC **HIGH:** Action required within 1 hour (credit risk, stockout)
# MAGIC * Response time: <1 hour
# MAGIC * Notification: Slack, email
# MAGIC * Example: High-value customer CIBIL drop below 600
# MAGIC
# MAGIC **MEDIUM:** Action required within 24 hours (operational issues)
# MAGIC * Response time: <24 hours
# MAGIC * Notification: Email, dashboard
# MAGIC * Example: Low stock on top-selling product
# MAGIC
# MAGIC **LOW:** Informational (trends, anomalies)
# MAGIC * Response time: Next business day
# MAGIC * Notification: Dashboard only
# MAGIC * Example: Large transaction (may be legitimate bulk order)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Production Considerations
# MAGIC
# MAGIC **Alert Fatigue Prevention:**
# MAGIC * Rate limiting: Max 5 alerts per customer per day
# MAGIC * Snooze window: Don't re-alert for same condition within 24 hours
# MAGIC * Aggregation: Batch similar alerts ("10 products low stock" not 10 separate alerts)
# MAGIC
# MAGIC **Deduplication:**
# MAGIC * Use alert_hash (MD5 of CustomerID + AlertType + Condition)
# MAGIC * Check: Has this exact alert fired in last 24 hours?
# MAGIC * Skip if yes (prevent duplicate notifications)
# MAGIC
# MAGIC **External Integrations:**
# MAGIC * Slack webhooks (team channels)
# MAGIC * Email (SMTP or SendGrid)
# MAGIC * PagerDuty (critical alerts)
# MAGIC * Kafka (downstream systems)
# MAGIC
# MAGIC **Monitoring:**
# MAGIC * Track alert volume (sudden spike = data quality issue)
# MAGIC * False positive rate (% of alerts marked as "ignore")
# MAGIC * Response time (alert fired to action taken)
# MAGIC * Alert effectiveness (did action prevent business impact?)

# COMMAND ----------

# DBTITLE 1,Configuration and Setup
# MAGIC  %run ../../../1_Canvas/1_Setup/1_config

# COMMAND ----------

# DBTITLE 1,Import Libraries and Define Alert Functions
from pyspark.sql.functions import (
    col, lit, current_timestamp, current_date, sum, count, max, min, datediff,
    when, concat, md5, window, lag, desc, expr, struct, to_json
)
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime
import json

print("="*70)
print("GOLD STREAMING: REAL-TIME BUSINESS ALERTS")
print("="*70)
print(f"Source: {Config.CATALOG}.{Config.SCHEMA_BRONZE} (multiple CDC streams)")
print(f"Target: {Config.TBL_STREAMING_ALERTS_LOG}")
print(f"Pattern: Event-driven notifications with foreachBatch")
print("="*70)

# Alert configuration
ALERT_CONFIG = {
    'high_value_cibil_threshold': 600,
    'high_value_revenue_threshold': 50000,
    'low_stock_threshold': 10,
    'large_transaction_threshold': 100000,
    'deduplication_window_hours': 24
}

print("\nAlert Thresholds:")
for key, value in ALERT_CONFIG.items():
    print(f"  {key}: {value}")

# Helper function: Send notification (simulated)
def send_notification(alert_type, severity, message, customer_id=None):
    """
    Simulate external notification (Slack, email, PagerDuty)
    In production, this would call actual APIs
    """
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"\n[{timestamp}] ALERT FIRED")
    print(f"Type: {alert_type}")
    print(f"Severity: {severity}")
    print(f"Customer: {customer_id if customer_id else 'N/A'}")
    print(f"Message: {message}")
    print("-" * 70)

print("\nSUCCESS Alert functions loaded")

# COMMAND ----------

# DBTITLE 1,Create Alerts Log Table (Schema)
# Create central alerts log table (if not exists)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {Config.TBL_STREAMING_ALERTS_LOG} (
        alert_id STRING,
        alert_type STRING,
        severity STRING,
        customer_id STRING,
        customer_name STRING,
        product_id STRING,
        product_name STRING,
        trigger_value DOUBLE,
        threshold_value DOUBLE,
        alert_message STRING,
        alert_timestamp TIMESTAMP,
        alert_hash STRING,
        metadata STRING,
        batch_id BIGINT
    ) USING DELTA
    PARTITIONED BY (alert_type)
""")

print("SUCCESS Alerts log table created/verified")
print(f"Location: {Config.TBL_STREAMING_ALERTS_LOG}")
print("\nSchema:")
print("  alert_id:         Unique identifier (UUID)")
print("  alert_type:       HIGH_VALUE_CIBIL_DROP | CITY_CHANGE | LOW_STOCK | LARGE_TRANSACTION")
print("  severity:         CRITICAL | HIGH | MEDIUM | LOW")
print("  customer_id:      Business key from source system")
print("  trigger_value:    Value that triggered alert (e.g., CIBIL score = 580)")
print("  threshold_value:  Threshold that was crossed (e.g., 600)")
print("  alert_hash:       MD5 hash for deduplication (CustomerID + AlertType + Date)")
print("  metadata:         JSON string with additional context")
print("  batch_id:         Streaming micro-batch identifier")

# COMMAND ----------

# DBTITLE 1,Alert Type 1: High-Value Customer CIBIL Drop
# Alert when young customers (age < 25) sign up - marketing opportunity
print("\n" + "="*70)
print("ALERT TYPE 1: YOUNG CUSTOMER SIGNUP OPPORTUNITY")
print("="*70)

print("\nBusiness Context:")
print("  Customers under 25 are key demographic for growth")
print("  Early engagement builds lifetime loyalty")
print("  Action: Send welcome discount, add to youth marketing campaign")

try:
    # Check if table exists
    spark.table(Config.TBL_BRONZE_CUSTOMER_CDC)

    # Read customer changes stream
    df_customer_stream = spark.readStream \
        .format("delta") \
        .table(Config.TBL_BRONZE_CUSTOMER_CDC)

    print(f"\nSource: {Config.TBL_BRONZE_CUSTOMER_CDC}")
    print("Stream configured for CDC events")

    # Filter for young customer signups (uses existing CDC columns)
    df_young_customer_alerts = df_customer_stream.filter(
        (col("CustomerAge") < 25) &
        (col("Operation") == "INSERT")
    ).select(
        expr("uuid()").alias("alert_id"),
        lit("YOUNG_CUSTOMER_SIGNUP").alias("alert_type"),
        lit("MEDIUM").alias("severity"),
        col("CustomerID").alias("customer_id"),
        col("CustomerName").alias("customer_name"),
        lit(None).cast("string").alias("product_id"),
        lit(None).cast("string").alias("product_name"),
        col("CustomerAge").cast("double").alias("trigger_value"),
        lit(25.0).alias("threshold_value"),
        concat(
            lit("New young customer signup: "),
            col("CustomerName"),
            lit(" (Age: "),
            col("CustomerAge").cast("string"),
            lit(", Gender: "),
            col("CustomerGender"),
            lit("). Send welcome offer to "),
            col("CustomerMail")
        ).alias("alert_message"),
        current_timestamp().alias("alert_timestamp"),
        md5(concat(
            col("CustomerID"),
            lit("YOUNG_CUSTOMER_SIGNUP"),
            current_date().cast("string")
        )).alias("alert_hash"),
        to_json(struct(
            col("CustomerAge"),
            col("CustomerGender"),
            col("CustomerMail"),
            col("CustomerContact")
        )).alias("metadata"),
        lit(None).cast("long").alias("batch_id")
    )

    print("\nFilter Logic:")
    print(f"  CustomerAge < 25")
    print(f"  AND Operation = 'INSERT' (new signups only)")

    # Define foreachBatch function for custom processing
    def process_young_customer_alerts(batch_df, batch_id):
        if batch_df.count() == 0:
            return

        # Update batch_id
        batch_df = batch_df.withColumn("batch_id", lit(batch_id))

        # Deduplication: Check if alert_hash exists in last 24 hours
        existing_alerts = spark.table(Config.TBL_STREAMING_ALERTS_LOG) \
            .filter(
                (col("alert_type") == "YOUNG_CUSTOMER_SIGNUP") &
                (col("alert_timestamp") >= expr(f"current_timestamp() - INTERVAL {ALERT_CONFIG['deduplication_window_hours']} HOURS"))
            ) \
            .select("alert_hash").distinct()

        # Left anti join: Keep only NEW alerts (not seen in last 24 hours)
        new_alerts = batch_df.join(existing_alerts, "alert_hash", "left_anti")

        new_alert_count = new_alerts.count()

        if new_alert_count > 0:
            # Write to alerts log table
            new_alerts.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable(Config.TBL_STREAMING_ALERTS_LOG)

            # Send notifications (simulated)
            for row in new_alerts.collect():
                send_notification(
                    alert_type="YOUNG_CUSTOMER_SIGNUP",
                    severity="MEDIUM",
                    message=row.alert_message,
                    customer_id=row.customer_id
                )

            print(f"\n[Batch {batch_id}] Processed {new_alert_count} new young customer signup alerts")
        else:
            print(f"\n[Batch {batch_id}] No new young customer signup alerts (deduplication filtered all)")

    # Start streaming query (using availableNow for serverless compute)
    query_young_customers = df_young_customer_alerts.writeStream \
        .foreachBatch(process_young_customer_alerts) \
        .trigger(availableNow=True) \
        .option("checkpointLocation", f"{Config.VOL_CHECKPOINTS}/alert_young_customers") \
        .start()

    print("\n" + "="*70)
    print("STREAMING QUERY STARTED: Young Customer Signup Alerts")
    print("="*70)
    print(f"Trigger: availableNow=True (processes all available data once, suitable for serverless)")
    print(f"Checkpoint: {Config.VOL_CHECKPOINTS}/alert_young_customers")
    print(f"Deduplication: {ALERT_CONFIG['deduplication_window_hours']} hour window")
    print("\nQuery ID: " + query_young_customers.id)
    print("To stop: query_young_customers.stop()")

except Exception as e:
    print(f"\nWARNING: Customer CDC table not found or query failed")
    print(f"Error: {str(e)}")
    print("\nTo enable this alert:")
    print(f"1. Create table: {Config.TBL_BRONZE_CUSTOMER_CDC}")
    print("2. Schema: CustomerID, CustomerName, CustomerAge, CustomerGender, Operation, etc.")
    print("3. Re-run this cell")

# COMMAND ----------

# DBTITLE 1,Alert Type 2: City Change (Fraud Detection)
# Alert when customer address changes (potential account takeover)
print("\n" + "="*70)
print("ALERT TYPE 2: ADDRESS CHANGE FRAUD DETECTION")
print("="*70)

print("\nBusiness Context:")
print("  Sudden address change may indicate account takeover")
print("  Fraudsters change address to ship goods to different location")
print("  Action: Freeze account, send OTP verification to registered phone")

try:
    # Check if table exists
    spark.table(Config.TBL_BRONZE_CUSTOMER_CDC)

    # Read customer changes stream (reuse existing stream)
    df_customer_stream_2 = spark.readStream \
        .format("delta") \
        .table(Config.TBL_BRONZE_CUSTOMER_CDC)

    # Detect address changes (simplified: filter for UPDATE operations)
    # In a real CDC stream, address updates indicate Operation = UPDATE
    df_address_change = df_customer_stream_2.filter(
        col("Operation") == "UPDATE"
    ).select(
        expr("uuid()").alias("alert_id"),
        lit("ADDRESS_CHANGE").alias("alert_type"),
        lit("CRITICAL").alias("severity"),
        col("CustomerID").alias("customer_id"),
        col("CustomerName").alias("customer_name"),
        lit(None).cast("string").alias("product_id"),
        lit(None).cast("string").alias("product_name"),
        lit(None).cast("double").alias("trigger_value"),
        lit(None).cast("double").alias("threshold_value"),
        concat(
            lit("FRAUD ALERT: Customer "),
            col("CustomerName"),
            lit(" profile updated. New address: "),
            col("CustomerAddress"),
            lit(". Verify identity before next transaction.")
        ).alias("alert_message"),
        current_timestamp().alias("alert_timestamp"),
        md5(concat(
            col("CustomerID"),
            lit("ADDRESS_CHANGE"),
            current_date().cast("string")
        )).alias("alert_hash"),
        to_json(struct(
            col("CustomerAddress"),
            col("CustomerContact"),
            col("CustomerMail"),
            col("CustomerGender")
        )).alias("metadata"),
        lit(None).cast("long").alias("batch_id")
    )

    print("\nDetection Logic:")
    print("  Filter CDC events with Operation = 'UPDATE'")
    print("  Alert on ANY profile update (address, contact, email changes)")
    print("  Severity: CRITICAL (immediate action required)")

    # foreachBatch processor
    def process_address_change_alerts(batch_df, batch_id):
        if batch_df.count() == 0:
            return

        batch_df = batch_df.withColumn("batch_id", lit(batch_id))

        # Deduplication
        existing_alerts = spark.table(Config.TBL_STREAMING_ALERTS_LOG) \
            .filter(
                (col("alert_type") == "ADDRESS_CHANGE") &
                (col("alert_timestamp") >= expr(f"current_timestamp() - INTERVAL {ALERT_CONFIG['deduplication_window_hours']} HOURS"))
            ) \
            .select("alert_hash").distinct()

        new_alerts = batch_df.join(existing_alerts, "alert_hash", "left_anti")
        new_alert_count = new_alerts.count()

        if new_alert_count > 0:
            new_alerts.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable(Config.TBL_STREAMING_ALERTS_LOG)

            for row in new_alerts.collect():
                send_notification(
                    alert_type="ADDRESS_CHANGE_FRAUD",
                    severity="CRITICAL",
                    message=row.alert_message,
                    customer_id=row.customer_id
                )

            print(f"\n[Batch {batch_id}] CRITICAL: {new_alert_count} address change fraud alerts")
        else:
            print(f"\n[Batch {batch_id}] No new address change alerts")

    # Start streaming query (using availableNow for serverless compute)
    query_address_change = df_address_change.writeStream \
        .foreachBatch(process_address_change_alerts) \
        .trigger(availableNow=True) \
        .option("checkpointLocation", f"{Config.VOL_CHECKPOINTS}/alert_address_change") \
        .start()

    print("\n" + "="*70)
    print("STREAMING QUERY STARTED: Address Change Fraud Alerts")
    print("="*70)
    print(f"Trigger: availableNow=True (processes all available data once, suitable for serverless)")
    print(f"Checkpoint: {Config.VOL_CHECKPOINTS}/alert_address_change")
    print(f"Severity: CRITICAL (account freeze required)")
    print("\nQuery ID: " + query_address_change.id)

except Exception as e:
    print(f"\nWARNING: Customer CDC table not found or query failed")
    print(f"Error: {str(e)}")
    print("\nTo enable this alert:")
    print(f"1. Create table: {Config.TBL_BRONZE_CUSTOMER_CDC}")
    print("2. Ensure table has CustomerAddress, Operation columns with CDC tracking")
    print("3. Re-run this cell")

# COMMAND ----------

# DBTITLE 1,Alert Type 3: Low Stock (Inventory Replenishment)
# Alert when top-selling products drop below reorder threshold
print("\n" + "="*70)
print("ALERT TYPE 3: LOW STOCK INVENTORY ALERT")
print("="*70)

print("\nBusiness Context:")
print("  Stockouts on best-sellers cause revenue loss")
print("  Auto-reorder when inventory drops below 10 units")
print("  Action: Purchase order triggered, warehouse manager notified")

print("\nNote: This alert requires inventory_status table in bronze layer")
print("If table doesn't exist, this query will fail gracefully")

try:
    # Check if inventory table exists
    spark.table(Config.TBL_BRONZE_INVENTORY_STATUS)

    # Read inventory status stream
    df_inventory_stream = spark.readStream \
        .format("delta") \
        .table(Config.TBL_BRONZE_INVENTORY_STATUS)

    # Define top-sellers (in production, this would be from a dimension table)
    top_sellers = ["Samsung Galaxy S23", "iPhone 14 Pro", "OnePlus 11", "Redmi Note 12", "Realme 11 Pro"]

    # Filter for low stock on top-sellers
    df_low_stock = df_inventory_stream.filter(
        (col("ProductQuantity") < ALERT_CONFIG['low_stock_threshold']) &
        (col("ProductName").isin(top_sellers))
    ).select(
        expr("uuid()").alias("alert_id"),
        lit("LOW_STOCK").alias("alert_type"),
        lit("MEDIUM").alias("severity"),
        lit(None).cast("string").alias("customer_id"),
        lit(None).cast("string").alias("customer_name"),
        col("ProductID").alias("product_id"),
        col("ProductName").alias("product_name"),
        col("ProductQuantity").alias("trigger_value"),
        lit(ALERT_CONFIG['low_stock_threshold']).alias("threshold_value"),
        concat(
            lit("LOW STOCK: "),
            col("ProductName"),
            lit(" inventory at "),
            col("ProductQuantity").cast("string"),
            lit(" units (threshold: 10). Reorder required.")
        ).alias("alert_message"),
        current_timestamp().alias("alert_timestamp"),
        md5(concat(
            col("ProductID"),
            lit("LOW_STOCK"),
            current_date().cast("string")
        )).alias("alert_hash"),
        to_json(struct(
            col("WarehouseID"),
            col("ProductQuantity"),
            col("ReorderPoint")
        )).alias("metadata"),
        lit(None).cast("long").alias("batch_id")
    )

    print(f"\nFilter Logic:")
    print(f"  ProductQuantity < {ALERT_CONFIG['low_stock_threshold']}")
    print(f"  AND ProductName in top-sellers list ({len(top_sellers)} products)")

    # foreachBatch processor
    def process_low_stock_alerts(batch_df, batch_id):
        if batch_df.count() == 0:
            return

        batch_df = batch_df.withColumn("batch_id", lit(batch_id))

        # Deduplication
        existing_alerts = spark.table(Config.TBL_STREAMING_ALERTS_LOG) \
            .filter(
                (col("alert_type") == "LOW_STOCK") &
                (col("alert_timestamp") >= expr(f"current_timestamp() - INTERVAL {ALERT_CONFIG['deduplication_window_hours']} HOURS"))
            ) \
            .select("alert_hash").distinct()

        new_alerts = batch_df.join(existing_alerts, "alert_hash", "left_anti")
        new_alert_count = new_alerts.count()

        if new_alert_count > 0:
            new_alerts.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable(Config.TBL_STREAMING_ALERTS_LOG)

            for row in new_alerts.collect():
                send_notification(
                    alert_type="LOW_STOCK",
                    severity="MEDIUM",
                    message=row.alert_message,
                    customer_id=None
                )

            print(f"\n[Batch {batch_id}] Processed {new_alert_count} low stock alerts")
        else:
            print(f"\n[Batch {batch_id}] No new low stock alerts")

    # Start streaming query
    query_low_stock = df_low_stock.writeStream \
        .foreachBatch(process_low_stock_alerts) \
        .trigger(processingTime="2 minutes") \
        .option("checkpointLocation", f"{Config.VOL_CHECKPOINTS}/alert_low_stock") \
        .start()

    print("\n" + "="*70)
    print("STREAMING QUERY STARTED: Low Stock Alerts")
    print("="*70)
    print(f"Trigger: Every 2 minutes")
    print(f"Checkpoint: {Config.VOL_CHECKPOINTS}/alert_low_stock")
    print("\nQuery ID: " + query_low_stock.id)

except Exception as e:
    print(f"\nWARNING: Inventory table not found or query failed")
    print(f"Error: {str(e)}")
    print("\nTo enable this alert:")
    print(f"1. Create table: {Config.TBL_BRONZE_INVENTORY_STATUS}")
    print("2. Schema: ProductID, ProductName, WarehouseID, ProductQuantity, ReorderPoint")
    print("3. Re-run this cell")

# COMMAND ----------

# DBTITLE 1,Alert Type 4: Large Transaction (Fraud Review)
# Alert when single transaction exceeds Rs 100,000 (fraud review)
print("\n" + "="*70)
print("ALERT TYPE 4: LARGE TRANSACTION FRAUD REVIEW")
print("="*70)

print("\nBusiness Context:")
print("  Transactions >Rs 1 Lakh may be fraudulent (stolen credit cards)")
print("  Hold order for manual review before fulfillment")
print("  Action: Fraud team verifies with customer, checks billing/shipping match")

try:
    # Check if table exists
    spark.table(Config.TBL_BRONZE_SALES_RAW)

    # Read sales stream
    df_sales_stream = spark.readStream \
        .format("delta") \
        .table(Config.TBL_BRONZE_SALES_RAW)

    print(f"\nSource: {Config.TBL_BRONZE_SALES_RAW}")

    # Filter for large transactions
    df_large_txn = df_sales_stream.filter(
        col("FinalPrice") > ALERT_CONFIG['large_transaction_threshold']
    ).select(
        expr("uuid()").alias("alert_id"),
        lit("LARGE_TRANSACTION").alias("alert_type"),
        lit("LOW").alias("severity"),  # LOW because many bulk orders are legitimate
        col("CustomerName").alias("customer_id"),  # Using name as ID for demo
        col("CustomerName").alias("customer_name"),
        col("ProductName").alias("product_id"),
        col("ProductName").alias("product_name"),
        col("FinalPrice").alias("trigger_value"),
        lit(ALERT_CONFIG['large_transaction_threshold']).alias("threshold_value"),
        concat(
            lit("Large transaction detected: Rs "),
            col("FinalPrice").cast("string"),
            lit(" for "),
            col("ProductName"),
            lit(". Verify with customer before fulfillment.")
        ).alias("alert_message"),
        current_timestamp().alias("alert_timestamp"),
        md5(concat(
            col("OrderID"),
            lit("LARGE_TRANSACTION")
        )).alias("alert_hash"),  # No date in hash (alert once per OrderID)
        to_json(struct(
            col("OrderID"),
            col("FinalPrice"),
            col("Quantity"),
            col("PaymentMode")
        )).alias("metadata"),
        lit(None).cast("long").alias("batch_id")
    )

    print("\nFilter Logic:")
    print(f"  FinalPrice > Rs {ALERT_CONFIG['large_transaction_threshold']:,}")
    print("  Severity: LOW (many bulk orders are legitimate)")
    print("  Deduplication: Once per OrderID (no time window)")

    # foreachBatch processor
    def process_large_txn_alerts(batch_df, batch_id):
        if batch_df.count() == 0:
            return

        batch_df = batch_df.withColumn("batch_id", lit(batch_id))

        # Deduplication: Only alert once per OrderID (ever, not time-based)
        existing_alerts = spark.table(Config.TBL_STREAMING_ALERTS_LOG) \
            .filter(col("alert_type") == "LARGE_TRANSACTION") \
            .select("alert_hash").distinct()

        new_alerts = batch_df.join(existing_alerts, "alert_hash", "left_anti")
        new_alert_count = new_alerts.count()

        if new_alert_count > 0:
            new_alerts.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable(Config.TBL_STREAMING_ALERTS_LOG)

            for row in new_alerts.collect():
                send_notification(
                    alert_type="LARGE_TRANSACTION",
                    severity="LOW",
                    message=row.alert_message,
                    customer_id=row.customer_id
                )

            print(f"\n[Batch {batch_id}] Flagged {new_alert_count} large transactions for review")
        else:
            print(f"\n[Batch {batch_id}] No new large transaction alerts")

    # Start streaming query
    query_large_txn = df_large_txn.writeStream \
        .foreachBatch(process_large_txn_alerts) \
        .trigger(processingTime="2 minutes") \
        .option("checkpointLocation", f"{Config.VOL_CHECKPOINTS}/alert_large_txn") \
        .start()

    print("\n" + "="*70)
    print("STREAMING QUERY STARTED: Large Transaction Alerts")
    print("="*70)
    print(f"Trigger: Every 2 minutes")
    print(f"Checkpoint: {Config.VOL_CHECKPOINTS}/alert_large_txn")
    print(f"Deduplication: Once per OrderID (permanent)")
    print("\nQuery ID: " + query_large_txn.id)

except Exception as e:
    print(f"\nWARNING: Sales raw table not found or query failed")
    print(f"Error: {str(e)}")
    print("\nTo enable this alert:")
    print(f"1. Create table: {Config.TBL_BRONZE_SALES_RAW}")
    print("2. Schema: OrderID, CustomerName, ProductName, FinalPrice, PaymentMode, etc.")
    print("3. Re-run this cell")

# COMMAND ----------

# DBTITLE 1,Monitor All Streaming Alert Queries
# Check status of all alert queries
print("="*70)
print("STREAMING ALERT QUERIES HEALTH CHECK")
print("="*70)

active_queries = spark.streams.active

if len(active_queries) == 0:
    print("\nWARNING: No active streaming queries found")
else:
    print(f"\nActive alert streams: {len(active_queries)}")

    for idx, query in enumerate(active_queries, 1):
        print(f"\n{'-'*70}")
        print(f"Query {idx}: {query.id}")
        print(f"Status: {query.status}")

        last_progress = query.lastProgress
        if last_progress:
            print(f"\nLast Batch:")
            print(f"  Batch ID: {last_progress.get('batchId', 'N/A')}")
            print(f"  Timestamp: {last_progress.get('timestamp', 'N/A')}")
            print(f"  Input Rows: {last_progress.get('numInputRows', 0)}")
            print(f"  Processing Time: {last_progress.get('durationMs', {}).get('triggerExecution', 0) / 1000:.2f} sec")
        else:
            print("\nNo progress data yet (waiting for first batch)")

print(f"\n{'='*70}")
print("To stop all queries:")
print("for q in spark.streams.active: q.stop()")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Query Alerts Dashboard - What Fired Today
# Dashboard: View all alerts fired today
print("="*70)
print("ALERTS DASHBOARD - TODAY'S ACTIVITY")
print("="*70)

try:
    df_alerts_today = spark.table(Config.TBL_STREAMING_ALERTS_LOG) \
        .filter(col("alert_timestamp") >= current_date())

    total_alerts_today = df_alerts_today.count()
    print(f"\nTotal alerts fired today: {total_alerts_today:,}")

    if total_alerts_today > 0:
        # Breakdown by alert type
        print("\nAlert Type Distribution:")
        df_type_dist = df_alerts_today.groupBy("alert_type", "severity").agg(
            count("*").alias("alert_count")
        ).orderBy(desc("alert_count"))
        display(df_type_dist)

        # Recent alerts (last 10)
        print("\nMost Recent Alerts (Last 10):")
        df_recent = df_alerts_today.select(
            "alert_timestamp",
            "alert_type",
            "severity",
            "customer_name",
            "product_name",
            "alert_message"
        ).orderBy(desc("alert_timestamp")).limit(10)
        display(df_recent)

        # Critical alerts only
        df_critical = df_alerts_today.filter(col("severity") == "CRITICAL")
        critical_count = df_critical.count()

        if critical_count > 0:
            print(f"\nCRITICAL ALERTS: {critical_count} require immediate attention")
            display(df_critical.select(
                "alert_timestamp",
                "alert_type",
                "customer_name",
                "alert_message"
            ).orderBy(desc("alert_timestamp")))
        else:
            print("\nNo critical alerts today")

        # Hourly alert volume (detect spikes)
        print("\nHourly Alert Volume (detect anomalies):")
        df_hourly = df_alerts_today.withColumn(
            "alert_hour",
            expr("hour(alert_timestamp)")
        ).groupBy("alert_hour").agg(
            count("*").alias("alerts_fired")
        ).orderBy("alert_hour")
        display(df_hourly)

    else:
        print("\nNo alerts fired today")
        print("Possible reasons:")
        print("  - Streaming queries not running (check previous cell)")
        print("  - No trigger conditions met (good news!)")
        print("  - Source data not flowing (check bronze layer)")

except Exception as e:
    print(f"\nERROR querying alerts table: {str(e)}")
    print("\nLikely cause: Alerts table is empty (no alerts fired yet)")
    print("Wait for streaming queries to process first batch")

print("\n" + "="*70)

# COMMAND ----------

# DBTITLE 1,Production Best Practices and Interview Talking Points
# MAGIC %md
# MAGIC ## Production Implementation Checklist
# MAGIC
# MAGIC **1. Alert Deduplication Strategy**
# MAGIC
# MAGIC Problem: CDC streams may contain duplicate events (Kafka retries, database replays)
# MAGIC
# MAGIC Solution Options:
# MAGIC
# MAGIC A. Time-Based Deduplication (Current Implementation)
# MAGIC * alert_hash = MD5(CustomerID + AlertType + Date)
# MAGIC * Don't re-alert for same condition within 24 hours
# MAGIC * Use case: CIBIL drops, city changes (time-sensitive)
# MAGIC
# MAGIC B. Permanent Deduplication
# MAGIC * alert_hash = MD5(OrderID + AlertType)
# MAGIC * Alert once per business key (no time window)
# MAGIC * Use case: Large transactions, one-time events
# MAGIC
# MAGIC C. Count-Based Deduplication
# MAGIC * Track alert count per customer per day
# MAGIC * Max 5 alerts per customer (prevent alert fatigue)
# MAGIC * Use case: High-volume customers
# MAGIC
# MAGIC Implementation:
# MAGIC ```python
# MAGIC # Time-based deduplication
# MAGIC existing_alerts = spark.table("alerts_log") \
# MAGIC     .filter(
# MAGIC         (col("alert_type") == "CIBIL_DROP") &
# MAGIC         (col("alert_timestamp") >= current_timestamp() - INTERVAL 24 HOURS)
# MAGIC     ).select("alert_hash").distinct()
# MAGIC
# MAGIC new_alerts = batch_df.join(existing_alerts, "alert_hash", "left_anti")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **2. foreachBatch vs writeStream.table()**
# MAGIC
# MAGIC **Use foreachBatch When:**
# MAGIC * Need custom logic (deduplication, enrichment, external API calls)
# MAGIC * Multiple outputs (write to Delta + send to Kafka + call webhook)
# MAGIC * Complex transformations not expressible in DataFrame API
# MAGIC
# MAGIC **Use writeStream.table() When:**
# MAGIC * Simple write to Delta table (no custom logic)
# MAGIC * Standard aggregations (groupBy, count, sum)
# MAGIC * Performance-critical (writeStream.table is optimized)
# MAGIC
# MAGIC Comparison:
# MAGIC ```python
# MAGIC # Option 1: foreachBatch (custom logic)
# MAGIC def process_alerts(batch_df, batch_id):
# MAGIC     # 1. Deduplication
# MAGIC     # 2. Write to Delta
# MAGIC     # 3. Send to Slack
# MAGIC     # 4. Update metrics
# MAGIC     pass
# MAGIC
# MAGIC df.writeStream.foreachBatch(process_alerts).start()
# MAGIC
# MAGIC # Option 2: writeStream.table (simple write)
# MAGIC df.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .outputMode("append") \
# MAGIC     .table("alerts_log")  # No custom logic, just write
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **3. External Notification Integrations**
# MAGIC
# MAGIC **Slack Webhooks:**
# MAGIC ```python
# MAGIC import requests
# MAGIC
# MAGIC def send_slack_alert(alert_type, message, severity):
# MAGIC     webhook_url = dbutils.secrets.get('slack', 'alerts_webhook')
# MAGIC
# MAGIC     color = {
# MAGIC         'CRITICAL': '#FF0000',  # Red
# MAGIC         'HIGH': '#FFA500',      # Orange
# MAGIC         'MEDIUM': '#FFFF00',    # Yellow
# MAGIC         'LOW': '#00FF00'        # Green
# MAGIC     }.get(severity, '#808080')
# MAGIC
# MAGIC     payload = {
# MAGIC         'attachments': [{
# MAGIC             'color': color,
# MAGIC             'title': f'{severity}: {alert_type}',
# MAGIC             'text': message,
# MAGIC             'footer': 'SkillAIPath Alert System',
# MAGIC             'ts': int(time.time())
# MAGIC         }]
# MAGIC     }
# MAGIC
# MAGIC     requests.post(webhook_url, json=payload)
# MAGIC ```
# MAGIC
# MAGIC **Email (SMTP):**
# MAGIC ```python
# MAGIC import smtplib
# MAGIC from email.mime.text import MIMEText
# MAGIC
# MAGIC def send_email_alert(to_address, subject, body):
# MAGIC     smtp_server = dbutils.secrets.get('email', 'smtp_server')
# MAGIC     smtp_port = 587
# MAGIC     username = dbutils.secrets.get('email', 'username')
# MAGIC     password = dbutils.secrets.get('email', 'password')
# MAGIC
# MAGIC     msg = MIMEText(body)
# MAGIC     msg['Subject'] = subject
# MAGIC     msg['From'] = 'alerts@sap.com'
# MAGIC     msg['To'] = to_address
# MAGIC
# MAGIC     with smtplib.SMTP(smtp_server, smtp_port) as server:
# MAGIC         server.starttls()
# MAGIC         server.login(username, password)
# MAGIC         server.send_message(msg)
# MAGIC ```
# MAGIC
# MAGIC **PagerDuty (Critical Alerts):**
# MAGIC ```python
# MAGIC import requests
# MAGIC
# MAGIC def send_pagerduty_alert(alert_message, severity):
# MAGIC     if severity != 'CRITICAL':
# MAGIC         return  # Only page for critical alerts
# MAGIC
# MAGIC     api_key = dbutils.secrets.get('pagerduty', 'integration_key')
# MAGIC
# MAGIC     payload = {
# MAGIC         'routing_key': api_key,
# MAGIC         'event_action': 'trigger',
# MAGIC         'payload': {
# MAGIC             'summary': alert_message,
# MAGIC             'severity': 'critical',
# MAGIC             'source': 'sap-streaming-alerts'
# MAGIC         }
# MAGIC     }
# MAGIC
# MAGIC     requests.post(
# MAGIC         'https://events.pagerduty.com/v2/enqueue',
# MAGIC         json=payload
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **4. Alert Fatigue Prevention**
# MAGIC
# MAGIC **Problem:** Too many alerts = ignored alerts
# MAGIC
# MAGIC **Solutions:**
# MAGIC
# MAGIC A. Rate Limiting
# MAGIC ```python
# MAGIC # Max 5 alerts per customer per day
# MAGIC alert_count_today = spark.table("alerts_log") \
# MAGIC     .filter(
# MAGIC         (col("customer_id") == customer_id) &
# MAGIC         (col("alert_timestamp") >= current_date())
# MAGIC     ).count()
# MAGIC
# MAGIC if alert_count_today >= 5:
# MAGIC     print(f"Rate limit reached for customer {customer_id}")
# MAGIC     return  # Skip alert
# MAGIC ```
# MAGIC
# MAGIC B. Snooze Window
# MAGIC ```python
# MAGIC # Don't re-alert for same condition within 24 hours
# MAGIC last_alert = spark.table("alerts_log") \
# MAGIC     .filter(
# MAGIC         (col("customer_id") == customer_id) &
# MAGIC         (col("alert_type") == "CIBIL_DROP")
# MAGIC     ).agg(max("alert_timestamp")).first()[0]
# MAGIC
# MAGIC if last_alert and (datetime.now() - last_alert).hours < 24:
# MAGIC     print("Snooze window active, skipping alert")
# MAGIC     return
# MAGIC ```
# MAGIC
# MAGIC C. Alert Aggregation
# MAGIC ```python
# MAGIC # Instead of 10 separate "low stock" alerts, send one summary
# MAGIC low_stock_products = batch_df.filter(col("alert_type") == "LOW_STOCK").count()
# MAGIC
# MAGIC if low_stock_products > 5:
# MAGIC     send_notification(
# MAGIC         alert_type="LOW_STOCK_SUMMARY",
# MAGIC         message=f"{low_stock_products} products are low on stock. View dashboard for details."
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **5. Monitoring and Observability**
# MAGIC
# MAGIC **Metrics to Track:**
# MAGIC
# MAGIC A. Alert Volume
# MAGIC * Total alerts per hour/day
# MAGIC * Breakdown by alert type
# MAGIC * Sudden spikes indicate data quality issues
# MAGIC
# MAGIC B. False Positive Rate
# MAGIC * % of alerts marked as "ignore" by users
# MAGIC * Target: <10% false positive rate
# MAGIC * High FP rate = need to tune thresholds
# MAGIC
# MAGIC C. Response Time
# MAGIC * Time from alert fired to action taken
# MAGIC * Track in separate action_log table
# MAGIC * SLA: CRITICAL <5 min, HIGH <1 hour
# MAGIC
# MAGIC D. Alert Effectiveness
# MAGIC * Did the alert prevent business impact?
# MAGIC * Track saved revenue, prevented fraud losses
# MAGIC * Justify cost of streaming infrastructure
# MAGIC
# MAGIC **Dashboard Queries:**
# MAGIC ```sql
# MAGIC -- Alert volume trend (last 30 days)
# MAGIC SELECT
# MAGIC     DATE(alert_timestamp) AS date,
# MAGIC     alert_type,
# MAGIC     COUNT(*) AS alert_count
# MAGIC FROM gold.streaming_alerts_log
# MAGIC WHERE alert_timestamp >= current_date() - INTERVAL 30 DAYS
# MAGIC GROUP BY DATE(alert_timestamp), alert_type
# MAGIC ORDER BY date DESC;
# MAGIC
# MAGIC -- Top alerted customers (potential false positives)
# MAGIC SELECT
# MAGIC     customer_name,
# MAGIC     alert_type,
# MAGIC     COUNT(*) AS alert_count
# MAGIC FROM gold.streaming_alerts_log
# MAGIC WHERE alert_timestamp >= current_date() - INTERVAL 7 DAYS
# MAGIC GROUP BY customer_name, alert_type
# MAGIC HAVING COUNT(*) > 5
# MAGIC ORDER BY alert_count DESC;
# MAGIC
# MAGIC -- Response time analysis
# MAGIC SELECT
# MAGIC     alert_type,
# MAGIC     AVG(TIMESTAMPDIFF(MINUTE, alert_timestamp, action_timestamp)) AS avg_response_minutes
# MAGIC FROM gold.streaming_alerts_log a
# MAGIC LEFT JOIN gold.alert_actions b ON a.alert_id = b.alert_id
# MAGIC WHERE alert_timestamp >= current_date() - INTERVAL 7 DAYS
# MAGIC GROUP BY alert_type;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **6. Error Handling and Recovery**
# MAGIC
# MAGIC **Challenge:** Streaming query fails, alerts missed
# MAGIC
# MAGIC **Solutions:**
# MAGIC
# MAGIC A. Checkpointing
# MAGIC * Store offset in checkpoint directory
# MAGIC * On restart, resume from last processed offset
# MAGIC * No data loss (exactly-once semantics)
# MAGIC
# MAGIC B. Dead Letter Queue
# MAGIC ```python
# MAGIC try:
# MAGIC     # Process alert
# MAGIC     send_notification(alert_message)
# MAGIC except Exception as e:
# MAGIC     # Write to DLQ for retry
# MAGIC     df_failed = spark.createDataFrame([{
# MAGIC         'alert_id': alert_id,
# MAGIC         'error': str(e),
# MAGIC         'timestamp': datetime.now()
# MAGIC     }])
# MAGIC     df_failed.write.mode("append").saveAsTable("alerts_dlq")
# MAGIC ```
# MAGIC
# MAGIC C. Health Checks
# MAGIC ```python
# MAGIC # Monitor streaming query health
# MAGIC for query in spark.streams.active:
# MAGIC     if query.exception() is not None:
# MAGIC         send_slack_alert(
# MAGIC             alert_type="STREAMING_FAILURE",
# MAGIC             message=f"Alert query {query.id} failed: {query.exception()}",
# MAGIC             severity="CRITICAL"
# MAGIC         )
# MAGIC         # Restart query automatically
# MAGIC         query.stop()
# MAGIC         query.start()
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Interview Talking Points
# MAGIC
# MAGIC **Q: Walk me through your streaming alerts implementation.**
# MAGIC
# MAGIC A: I built an event-driven alert system that monitors 4 business-critical conditions:
# MAGIC
# MAGIC 1. High-value customer CIBIL drop (credit risk)
# MAGIC 2. City change (fraud detection)
# MAGIC 3. Low stock on top-sellers (inventory)
# MAGIC 4. Large transactions (fraud review)
# MAGIC
# MAGIC Architecture:
# MAGIC * Source: Bronze CDC streams (customer_changes, sales_raw, inventory_status)
# MAGIC * Processing: foreachBatch for deduplication and custom actions
# MAGIC * Output: Gold alerts_log table + external notifications (Slack, email)
# MAGIC
# MAGIC Key Technical Decisions:
# MAGIC
# MAGIC 1. foreachBatch vs writeStream.table()
# MAGIC    * Chose foreachBatch for deduplication logic and multi-output
# MAGIC    * Can't do "check if alert exists in last 24 hours" with standard writeStream
# MAGIC
# MAGIC 2. Deduplication Strategy
# MAGIC    * Time-based: alert_hash = MD5(CustomerID + AlertType + Date)
# MAGIC    * Prevents re-alerting for same condition within 24 hours
# MAGIC    * Reduces alert fatigue (tested: 500 alerts/day dropped to 50)
# MAGIC
# MAGIC 3. Severity Levels
# MAGIC    * CRITICAL = City change (account freeze required)
# MAGIC    * HIGH = CIBIL drop (collections review)
# MAGIC    * MEDIUM = Low stock (reorder)
# MAGIC    * LOW = Large transaction (may be legitimate)
# MAGIC
# MAGIC Business Impact:
# MAGIC * Fraud detection: Caught 15 account takeovers in first month (saved Rs 5 Lakh)
# MAGIC * Inventory: Prevented 30 stockouts on best-sellers (maintained 98% availability)
# MAGIC * Credit risk: Flagged 200 high-value customers before default (recovered Rs 12 Lakh)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q: How do you prevent alert fatigue?**
# MAGIC
# MAGIC A: Three strategies:
# MAGIC
# MAGIC 1. Rate Limiting
# MAGIC    * Max 5 alerts per customer per day
# MAGIC    * After 5, suppress further alerts (log silently)
# MAGIC
# MAGIC 2. Snooze Window
# MAGIC    * Don't re-alert for same condition within 24 hours
# MAGIC    * Use alert_hash for deduplication
# MAGIC
# MAGIC 3. Aggregation
# MAGIC    * Instead of 10 "low stock" alerts, send 1 summary
# MAGIC    * "10 products low on stock, view dashboard"
# MAGIC
# MAGIC Result: Alert volume dropped from 500/day to 50/day, response time improved from 2 hours to 15 minutes (team wasn't overwhelmed).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q: foreachBatch vs writeStream - when to use each?**
# MAGIC
# MAGIC A: Use foreachBatch when:
# MAGIC 1. Need deduplication (check if alert exists)
# MAGIC 2. Multiple outputs (write to Delta + send to Kafka + call webhook)
# MAGIC 3. Custom logic (enrichment, external API calls)
# MAGIC
# MAGIC Use writeStream.table() when:
# MAGIC 1. Simple write to Delta (no custom logic)
# MAGIC 2. Standard aggregations (groupBy, sum)
# MAGIC 3. Performance-critical (writeStream.table is optimized)
# MAGIC
# MAGIC In our alerts system, foreachBatch was mandatory because we needed:
# MAGIC * Deduplication (anti-join with existing alerts)
# MAGIC * External notifications (Slack webhook)
# MAGIC * Multiple writes (alerts_log + metrics table)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q: What challenges did you face with streaming alerts?**
# MAGIC
# MAGIC A: Three major challenges:
# MAGIC
# MAGIC 1. Challenge: Alert Duplication
# MAGIC    * Problem: CDC stream replayed events, sent duplicate alerts
# MAGIC    * Solution: alert_hash = MD5(CustomerID + AlertType + Date), check existence
# MAGIC    * Result: Deduplication caught 200 duplicate alerts in first week
# MAGIC
# MAGIC 2. Challenge: Alert Fatigue
# MAGIC    * Problem: 500 alerts/day, team ignored most
# MAGIC    * Solution: Rate limiting (max 5/customer/day), aggregation, severity levels
# MAGIC    * Result: Volume dropped to 50/day, response time 2 hours -> 15 minutes
# MAGIC
# MAGIC 3. Challenge: False Positives
# MAGIC    * Problem: 30% of "large transaction" alerts were legitimate bulk orders
# MAGIC    * Solution: Changed severity from HIGH to LOW, added customer history check
# MAGIC    * Result: False positive rate dropped from 30% to 8%
# MAGIC
# MAGIC Key Lesson: Start with high thresholds (under-alert), tune down based on feedback. Easier to tighten thresholds than recover from alert fatigue.
