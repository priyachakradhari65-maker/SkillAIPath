# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Pipeline Overview
# MAGIC %md
# MAGIC # Supply Chain Intelligence Agent - Daily Orchestrator
# MAGIC
# MAGIC ## Overview
# MAGIC This pipeline orchestrates the daily supply chain analytics workflow in 4 steps:
# MAGIC
# MAGIC 1. **Feature Engineering** - Calculate sales velocity and inventory metrics
# MAGIC 2. **Generate Predictions** - Create stockout predictions and reorder recommendations
# MAGIC 3. **Create Alerts** - Generate actionable alerts for stockouts, excess inventory, and dead stock
# MAGIC 4. **Calculate Metrics** - Compute daily KPIs and performance metrics

# COMMAND ----------

# DBTITLE 1,Import Libraries and Configuration
import sys
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import traceback

# Configuration
CATALOG = "sap_dev"
GOLD_SCHEMA = "gold"

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

print("\n" + "="*70)
print("SUPPLY CHAIN DAILY ORCHESTRATOR")
print("="*70)
print(f"\n📊 Configuration:")
print(f"  Catalog: {CATALOG}")
print(f"  Schema: {GOLD_SCHEMA}")
print(f"  Run Date: {date.today()}")
print(f"  Timestamp: {datetime.now()}")

# COMMAND ----------

# DBTITLE 1,Setup Logging and Audit Functions
# Pipeline execution tracking
pipeline_start_time = datetime.now()
pipeline_run_id = f"pipeline_{pipeline_start_time.strftime('%Y%m%d_%H%M%S')}"
errors = []
warnings = []

def log_event(event_type, message, severity="INFO"):
    """Log events with console output and error tracking"""
    icon = {
        "INFO": "ℹ️",
        "WARNING": "⚠️",
        "ERROR": "❌",
        "SUCCESS": "✅"
    }.get(severity, "ℹ️")
    
    print(f"{icon} [{event_type}] {message}")
    
    if severity == "ERROR":
        errors.append(message)

print(f"\n🆔 Pipeline Run ID: {pipeline_run_id}")
print(f"⏰ Started at: {pipeline_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
log_event("Pipeline", "Pipeline initialization complete", "SUCCESS")

# COMMAND ----------

# DBTITLE 1,Step 1: Run Feature Engineering
print("\n" + "="*70)
print("STEP 1: FEATURE ENGINEERING")
print("="*70)

step_start_time = datetime.now()

try:
    log_event("Pipeline_Run", "Starting feature engineering...", "INFO")
    
    # Run feature engineering SQL directly to create temp view in this session
    spark.sql(f"USE CATALOG {CATALOG}")
    spark.sql("USE SCHEMA silver")
    
    # Step 1: Sales velocity
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW sales_velocity AS
        SELECT
          fs.ProductSK,
          fs.WarehouseSK,
          CURRENT_DATE() AS analysis_date,
          SUM(CASE WHEN fs.BatchDate >= DATE_SUB(CURRENT_DATE(), 7) THEN fs.ProductQuantity ELSE 0 END) AS sales_last_7d,
          SUM(CASE WHEN fs.BatchDate >= DATE_SUB(CURRENT_DATE(), 14) THEN fs.ProductQuantity ELSE 0 END) AS sales_last_14d,
          SUM(CASE WHEN fs.BatchDate >= DATE_SUB(CURRENT_DATE(), 30) THEN fs.ProductQuantity ELSE 0 END) AS sales_last_30d,
          COALESCE(SUM(CASE WHEN fs.BatchDate >= DATE_SUB(CURRENT_DATE(), 7) THEN fs.ProductQuantity ELSE 0 END) / 7.0, 0) AS avg_daily_sales_7d,
          COALESCE(SUM(CASE WHEN fs.BatchDate >= DATE_SUB(CURRENT_DATE(), 14) THEN fs.ProductQuantity ELSE 0 END) / 14.0, 0) AS avg_daily_sales_14d,
          COALESCE(SUM(CASE WHEN fs.BatchDate >= DATE_SUB(CURRENT_DATE(), 30) THEN fs.ProductQuantity ELSE 0 END) / 30.0, 0) AS avg_daily_sales_30d,
          COALESCE(STDDEV(fs.ProductQuantity), 0) AS sales_volatility_30d,
          COUNT(DISTINCT fs.BatchDate) AS sales_days_30d
        FROM sap_dev.silver.fact_sales fs
        WHERE fs.BatchDate >= DATE_SUB(CURRENT_DATE(), 30)
        GROUP BY fs.ProductSK, fs.WarehouseSK
    """)
    
    # Step 2: Current inventory
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW current_inventory AS
        SELECT
          fi.ProductSK,
          fi.WarehouseSK,
          COALESCE(fi.StockQuantity, 0) AS current_stock,
          COALESCE(fi.StockQuantity, 0) AS available_stock,
          COALESCE(fi.StockValue, 0) AS stock_value,
          CASE 
            WHEN fi.StockQuantity = 0 THEN 'Out of Stock'
            WHEN fi.StockQuantity < 10 THEN 'Low Stock'
            ELSE 'In Stock'
          END AS stock_status
        FROM sap_dev.silver.fact_inventory fi
        WHERE fi.BatchDate = (SELECT MAX(BatchDate) FROM sap_dev.silver.fact_inventory)
    """)
    
    # Step 3: Purchase patterns
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW purchase_patterns AS
        SELECT
          fp.ProductSK,
          4 AS avg_lead_time_days,
          ROUND(30.0 / COUNT(DISTINCT DATE_TRUNC('WEEK', fp.PurchaseDate)), 0) AS order_frequency_days,
          AVG(fp.Quantity) AS avg_order_qty
        FROM sap_dev.silver.fact_purchase fp
        WHERE fp.PurchaseDate >= DATE_SUB(CURRENT_DATE(), 90)
        GROUP BY fp.ProductSK
    """)
    
    # Step 4: Enrich with dimensions
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW enriched_features AS
        SELECT
          COALESCE(sv.ProductSK, ci.ProductSK) AS product_sk,
          COALESCE(sv.WarehouseSK, ci.WarehouseSK) AS warehouse_sk,
          CURRENT_DATE() AS analysis_date,
          dp.ProductName AS product_name,
          'General' AS product_category,
          'Unknown' AS brand,
          'Standard' AS product_size,
          'INR' AS product_currency,
          dw.WarehouseName AS warehouse_name,
          dw.WareHouseCity AS warehouse_city,
          'India' AS warehouse_state,
          sv.sales_last_7d,
          sv.sales_last_14d,
          sv.sales_last_30d,
          sv.avg_daily_sales_7d,
          sv.avg_daily_sales_14d,
          sv.avg_daily_sales_30d,
          sv.sales_volatility_30d,
          sv.sales_days_30d,
          ci.current_stock,
          ci.available_stock,
          ci.stock_value,
          ci.stock_status,
          pp.avg_lead_time_days AS supplier_lead_time_days,
          pp.order_frequency_days,
          pp.avg_order_qty
        FROM current_inventory ci
        LEFT JOIN sales_velocity sv ON ci.ProductSK = sv.ProductSK AND ci.WarehouseSK = sv.WarehouseSK
        LEFT JOIN purchase_patterns pp ON ci.ProductSK = pp.ProductSK
        INNER JOIN sap_dev.silver.dim_product dp ON ci.ProductSK = dp.ProductSK
        INNER JOIN sap_dev.silver.dim_warehouse dw ON ci.WarehouseSK = dw.WarehouseSK
    """)
    
    # Step 5: Create final feature table with calculations
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW sales_velocity_features AS
        SELECT
          product_sk,
          warehouse_sk,
          analysis_date,
          product_name,
          product_category,
          brand,
          product_size,
          product_currency,
          warehouse_name,
          warehouse_city,
          warehouse_state,
          current_stock,
          available_stock,
          stock_value,
          stock_status,
          sales_last_7d AS sales_7d,
          sales_last_14d AS sales_14d,
          sales_last_30d AS sales_30d,
          avg_daily_sales_7d,
          avg_daily_sales_14d,
          avg_daily_sales_30d,
          sales_volatility_30d,
          sales_days_30d,
          CASE 
            WHEN avg_daily_sales_30d > 0 THEN ROUND(current_stock / avg_daily_sales_30d, 1)
            ELSE 999.0
          END AS days_until_stockout,
          CASE 
            WHEN avg_daily_sales_30d > 0 THEN DATE_ADD(CURRENT_DATE(), CAST(ROUND(current_stock / avg_daily_sales_30d, 0) AS INT))
            ELSE DATE_ADD(CURRENT_DATE(), 999)
          END AS predicted_stockout_date,
          CASE
            WHEN current_stock = 0 THEN 'Out of Stock'
            WHEN avg_daily_sales_30d = 0 THEN 'No Sales'
            WHEN (current_stock / avg_daily_sales_30d) <= 1 THEN 'Critical'
            WHEN (current_stock / avg_daily_sales_30d) <= 3 THEN 'High'
            WHEN (current_stock / avg_daily_sales_30d) <= 7 THEN 'Medium'
            WHEN (current_stock / avg_daily_sales_30d) <= 14 THEN 'Low'
            ELSE 'Healthy'
          END AS stockout_risk_level,
          CASE 
            WHEN avg_daily_sales_30d > 0 THEN current_stock / avg_daily_sales_30d
            ELSE 999.0
          END AS days_of_inventory,
          CASE 
            WHEN current_stock > 0 THEN (sales_last_30d / current_stock) * 12
            ELSE 0.0
          END AS inventory_turnover_annual,
          COALESCE(supplier_lead_time_days, 4) AS supplier_lead_time_days,
          ROUND(avg_daily_sales_30d * 1.5 * COALESCE(supplier_lead_time_days, 4), 0) AS calculated_safety_stock,
          ROUND(avg_daily_sales_30d * COALESCE(supplier_lead_time_days, 4) + (avg_daily_sales_30d * 1.5 * COALESCE(supplier_lead_time_days, 4)), 0) AS calculated_reorder_point,
          CASE 
            WHEN current_stock < ROUND(avg_daily_sales_30d * COALESCE(supplier_lead_time_days, 4) + (avg_daily_sales_30d * 1.5 * COALESCE(supplier_lead_time_days, 4)), 0)
            THEN GREATEST(ROUND(COALESCE(avg_order_qty, avg_daily_sales_30d * 14), 0), 100)
            ELSE 0
          END AS recommended_order_qty,
          CASE 
            WHEN current_stock < ROUND(avg_daily_sales_30d * COALESCE(supplier_lead_time_days, 4) + (avg_daily_sales_30d * 1.5 * COALESCE(supplier_lead_time_days, 4)), 0)
            THEN GREATEST(ROUND(COALESCE(avg_order_qty, avg_daily_sales_30d * 14), 0), 100) * avg_daily_sales_30d * 100
            ELSE 0
          END AS estimated_order_cost,
          CASE 
            WHEN avg_daily_sales_30d > 0 AND (current_stock / avg_daily_sales_30d) > 60 THEN TRUE
            ELSE FALSE
          END AS is_excess_inventory,
          CASE 
            WHEN sales_last_14d = 0 AND current_stock > 0 THEN TRUE
            ELSE FALSE
          END AS is_dead_stock
        FROM enriched_features
    """)
    
    feature_count = spark.table("sales_velocity_features").count()
    log_event("Pipeline_Run", f"Feature engineering completed. Generated {feature_count} feature records.", "SUCCESS")
    print(f"✅ Feature engineering completed: {feature_count} records")
    
    step_duration = (datetime.now() - step_start_time).total_seconds()
    print(f"⏱️ Duration: {step_duration:.2f} seconds")
    
except Exception as e:
    error_msg = f"Feature engineering failed: {str(e)}"
    log_event("Error", error_msg, "ERROR")
    print(f"❌ {error_msg}")
    print(traceback.format_exc())
    raise

# COMMAND ----------

# DBTITLE 1,Step 2: Insert Predictions into Gold Table
print("\n" + "="*70)
print("STEP 2: GENERATE AND INSERT PREDICTIONS")
print("="*70)

step_start_time = datetime.now()

try:
    log_event("Pipeline_Run", "Generating predictions...", "INFO")
    
    # Read features
    features_df = spark.table("sales_velocity_features")
    
    # Transform features into predictions format
    predictions_df = features_df.select(
        lit(date.today()).alias("prediction_date"),
        col("product_sk"),
        col("warehouse_sk"),
        
        # Current State
        col("current_stock"),
        col("stock_value"),
        col("stock_status"),
        
        # Sales Velocity
        col("sales_7d").alias("sales_last_7d"),
        col("sales_14d").alias("sales_last_14d"),
        col("sales_30d").alias("sales_last_30d"),
        col("avg_daily_sales_7d"),
        col("avg_daily_sales_14d"),
        col("avg_daily_sales_30d"),
        col("sales_volatility_30d").alias("sales_volatility"),
        
        # Stockout Predictions
        col("predicted_stockout_date"),
        col("days_until_stockout"),
        col("stockout_risk_level"),
        
        # Inventory Metrics
        col("days_of_inventory"),
        col("inventory_turnover_annual").alias("inventory_turnover_30d"),
        
        # Reorder Recommendations
        col("supplier_lead_time_days"),
        col("calculated_safety_stock").alias("safety_stock"),
        col("calculated_reorder_point").alias("reorder_point"),
        col("recommended_order_qty"),
        when(col("days_until_stockout") <= 7, 
             date_sub(current_date(), col("days_until_stockout").cast("int"))
        ).alias("recommended_reorder_date"),
        col("estimated_order_cost").alias("order_cost_estimate"),
        
        # Forecast placeholders (for future ML integration)
        lit(None).cast("int").alias("forecast_day1"),
        lit(None).cast("int").alias("forecast_day2"),
        lit(None).cast("int").alias("forecast_day3"),
        lit(None).cast("int").alias("forecast_day4"),
        lit(None).cast("int").alias("forecast_day5"),
        lit(None).cast("int").alias("forecast_day6"),
        lit(None).cast("int").alias("forecast_day7"),
        lit(None).cast("decimal(5,2)").alias("forecast_confidence"),
        
        # Classification Flags
        when(col("days_until_stockout") <= 1, True).otherwise(False).alias("is_critical_stockout"),
        when(col("days_until_stockout") <= 7, True).otherwise(False).alias("is_warning_stockout"),
        col("is_excess_inventory"),
        col("is_dead_stock"),
        
        # Metadata
        current_timestamp().alias("created_at"),
        current_timestamp().alias("updated_at")
    )
    
    # Delete today's predictions if they exist (idempotent)
    spark.sql(f"""
        DELETE FROM {CATALOG}.{GOLD_SCHEMA}.supply_chain_predictions
        WHERE prediction_date = CURRENT_DATE()
    """)
    
    # Insert new predictions
    predictions_df.write.mode("append").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.supply_chain_predictions")
    
    prediction_count = predictions_df.count()
    log_event("Prediction_Generated", f"Inserted {prediction_count} predictions for {date.today()}", "INFO")
    print(f"✅ Inserted {prediction_count} predictions")
    
    # Show summary statistics
    summary = predictions_df.groupBy("stockout_risk_level").count().orderBy("stockout_risk_level")
    print("\n📊 Predictions by Risk Level:")
    summary.show()
    
    step_duration = (datetime.now() - step_start_time).total_seconds()
    print(f"⏱️ Duration: {step_duration:.2f} seconds")
    
except Exception as e:
    error_msg = f"Prediction insertion failed: {str(e)}"
    log_event("Error", error_msg, "ERROR")
    print(f"❌ {error_msg}")
    print(traceback.format_exc())
    raise

# COMMAND ----------

# DBTITLE 1,Step 3: Generate and Insert Alerts
print("\n" + "="*70)
print("STEP 3: GENERATE AND INSERT ALERTS")
print("="*70)

step_start_time = datetime.now()

try:
    log_event("Pipeline_Run", "Generating alerts...", "INFO")
    
    # Read today's predictions
    predictions = spark.sql(f"""
        SELECT 
            p.*,
            pr.ProductName AS product_name,
        w.WareHouseName AS warehouse_name
        FROM {CATALOG}.{GOLD_SCHEMA}.supply_chain_predictions p
        INNER JOIN {CATALOG}.silver.dim_product pr ON p.product_sk = pr.ProductSK
        INNER JOIN {CATALOG}.silver.dim_warehouse w ON p.warehouse_sk = w.WarehouseSK
        WHERE p.prediction_date = CURRENT_DATE()
    """)
    
    # Generate Stockout Alerts (Critical and High only)
    stockout_alerts = predictions.filter(
        (col("stockout_risk_level").isin(["Critical", "High", "Out of Stock"])) &
        (col("current_stock") > 0)  # Only alert if there's still stock to reorder
    ).select(
        lit(date.today()).alias("alert_date"),
        lit("Stockout").alias("alert_type"),
        when(col("stockout_risk_level") == "Critical", "Critical")
            .when(col("stockout_risk_level") == "High", "High")
            .otherwise("Medium").alias("priority"),
        col("product_sk"),
        col("warehouse_sk"),
        col("product_name"),
        col("warehouse_name"),
        concat(lit("STOCKOUT RISK: "), col("stockout_risk_level"), lit(" - "), 
               col("days_until_stockout"), lit(" days")).alias("alert_title"),
        concat(
            col("product_name"), lit(" at "), col("warehouse_name"),
            lit(" will stock out in "), col("days_until_stockout"), lit(" day(s). "),
            lit("Current stock: "), col("current_stock"), lit(" units. "),
            lit("Avg daily sales: "), round(col("avg_daily_sales_30d"), 2), lit(" units.")
        ).alias("alert_message"),
        concat(
            lit("URGENT: Reorder "), col("recommended_order_qty"), lit(" units ASAP")
        ).alias("recommended_action"),
        col("current_stock"),
        col("avg_daily_sales_30d").alias("avg_daily_sales"),
        col("days_until_stockout"),
        lit(None).cast("decimal(10,2)").alias("days_of_inventory"),
        (col("recommended_order_qty") * col("avg_daily_sales_30d") * 100).alias("estimated_impact_value"),  # Lost revenue estimate
        lit(None).cast("int").alias("transfer_from_warehouse_sk"),
        lit(None).cast("int").alias("transfer_to_warehouse_sk"),
        lit(None).cast("int").alias("transfer_quantity"),
        lit(None).cast("decimal(15,2)").alias("transfer_cost_estimate"),
        lit(None).cast("decimal(15,2)").alias("transfer_net_benefit"),
        col("recommended_order_qty"),
        col("order_cost_estimate"),
        col("supplier_lead_time_days"),
        lit(True).alias("is_active"),
        lit(False).alias("is_acknowledged"),
        lit(None).cast("string").alias("acknowledged_by"),
        lit(None).cast("timestamp").alias("acknowledged_at"),
        lit(None).cast("string").alias("resolution_notes"),
        lit(None).cast("timestamp").alias("resolved_at"),
        current_timestamp().alias("created_at"),
        current_timestamp().alias("updated_at")
    )
    
    # Generate Excess Inventory Alerts
    excess_alerts = predictions.filter(
        col("is_excess_inventory") == True
    ).select(
        lit(date.today()).alias("alert_date"),
        lit("Excess_Inventory").alias("alert_type"),
        lit("Low").alias("priority"),
        col("product_sk"),
        col("warehouse_sk"),
        col("product_name"),
        col("warehouse_name"),
        concat(lit("EXCESS INVENTORY: "), round(col("days_of_inventory"), 1), lit(" days")).alias("alert_title"),
        concat(
            col("product_name"), lit(" at "), col("warehouse_name"),
            lit(" has "), round(col("days_of_inventory"), 1), lit(" days of inventory. "),
            lit("Current stock: "), col("current_stock"), lit(" units. "),
            lit("Value: ₹"), round(col("stock_value"), 2)
        ).alias("alert_message"),
        lit("Consider markdown, promotion, or transfer to high-demand warehouse").alias("recommended_action"),
        col("current_stock"),
        col("avg_daily_sales_30d").alias("avg_daily_sales"),
        lit(None).cast("int").alias("days_until_stockout"),
        col("days_of_inventory"),
        col("stock_value").alias("estimated_impact_value"),
        lit(None).cast("int").alias("transfer_from_warehouse_sk"),
        lit(None).cast("int").alias("transfer_to_warehouse_sk"),
        lit(None).cast("int").alias("transfer_quantity"),
        lit(None).cast("decimal(15,2)").alias("transfer_cost_estimate"),
        lit(None).cast("decimal(15,2)").alias("transfer_net_benefit"),
        lit(None).cast("int").alias("recommended_order_qty"),
        lit(None).cast("decimal(15,2)").alias("order_cost_estimate"),
        lit(None).cast("int").alias("supplier_lead_time_days"),
        lit(True).alias("is_active"),
        lit(False).alias("is_acknowledged"),
        lit(None).cast("string").alias("acknowledged_by"),
        lit(None).cast("timestamp").alias("acknowledged_at"),
        lit(None).cast("string").alias("resolution_notes"),
        lit(None).cast("timestamp").alias("resolved_at"),
        current_timestamp().alias("created_at"),
        current_timestamp().alias("updated_at")
    )
    
    # Generate Dead Stock Alerts
    dead_stock_alerts = predictions.filter(
        col("is_dead_stock") == True
    ).select(
        lit(date.today()).alias("alert_date"),
        lit("Excess_Inventory").alias("alert_type"),
        lit("Medium").alias("priority"),
        col("product_sk"),
        col("warehouse_sk"),
        col("product_name"),
        col("warehouse_name"),
        lit("DEAD STOCK: No sales in 14+ days").alias("alert_title"),
        concat(
            col("product_name"), lit(" at "), col("warehouse_name"),
            lit(" has zero sales for 14+ days. "),
            lit("Stock: "), col("current_stock"), lit(" units. "),
            lit("Value: ₹"), round(col("stock_value"), 2)
        ).alias("alert_message"),
        lit("Markdown, bundle, or liquidate immediately").alias("recommended_action"),
        col("current_stock"),
        col("avg_daily_sales_30d").alias("avg_daily_sales"),
        lit(None).cast("int").alias("days_until_stockout"),
        col("days_of_inventory"),
        col("stock_value").alias("estimated_impact_value"),
        lit(None).cast("int").alias("transfer_from_warehouse_sk"),
        lit(None).cast("int").alias("transfer_to_warehouse_sk"),
        lit(None).cast("int").alias("transfer_quantity"),
        lit(None).cast("decimal(15,2)").alias("transfer_cost_estimate"),
        lit(None).cast("decimal(15,2)").alias("transfer_net_benefit"),
        lit(None).cast("int").alias("recommended_order_qty"),
        lit(None).cast("decimal(15,2)").alias("order_cost_estimate"),
        lit(None).cast("int").alias("supplier_lead_time_days"),
        lit(True).alias("is_active"),
        lit(False).alias("is_acknowledged"),
        lit(None).cast("string").alias("acknowledged_by"),
        lit(None).cast("timestamp").alias("acknowledged_at"),
        lit(None).cast("string").alias("resolution_notes"),
        lit(None).cast("timestamp").alias("resolved_at"),
        current_timestamp().alias("created_at"),
        current_timestamp().alias("updated_at")
    )
    
    # Combine all alerts
    all_alerts = stockout_alerts.union(excess_alerts).union(dead_stock_alerts)
    
    # Delete today's alerts if they exist (idempotent)
    spark.sql(f"""
        DELETE FROM {CATALOG}.{GOLD_SCHEMA}.supply_chain_alerts
        WHERE alert_date = CURRENT_DATE()
    """)
    
    # Insert new alerts
    all_alerts.write.mode("append").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.supply_chain_alerts")
    
    alert_count = all_alerts.count()
    log_event("Alert_Created", f"Generated {alert_count} alerts for {date.today()}", "INFO")
    print(f"✅ Generated {alert_count} alerts")
    
    # Show summary
    alert_summary = all_alerts.groupBy("alert_type", "priority").count().orderBy("priority", "alert_type")
    print("\n📊 Alerts by Type and Priority:")
    alert_summary.show()
    
    step_duration = (datetime.now() - step_start_time).total_seconds()
    print(f"⏱️ Duration: {step_duration:.2f} seconds")
    
except Exception as e:
    error_msg = f"Alert generation failed: {str(e)}"
    log_event("Error", error_msg, "ERROR")
    print(f"❌ {error_msg}")
    print(traceback.format_exc())
    raise

# COMMAND ----------

# DBTITLE 1,Step 4: Calculate and Insert Metrics
print("\n" + "="*70)
print("STEP 4: CALCULATE AND INSERT METRICS")
print("="*70)

step_start_time = datetime.now()

try:
    log_event("Pipeline_Run", "Calculating metrics...", "INFO")
    
    # Calculate metrics from today's predictions and alerts
    metrics_sql = f"""
    WITH prediction_stats AS (
        SELECT
            CURRENT_DATE() as metric_date,
            COUNT(*) as total_predictions,
            COUNT(DISTINCT CONCAT(product_sk, '-', warehouse_sk)) as total_product_warehouse_combos
        FROM {CATALOG}.{GOLD_SCHEMA}.supply_chain_predictions
        WHERE prediction_date = CURRENT_DATE()
    ),
    alert_stats AS (
        SELECT
            COUNT(*) as total_alerts,
            SUM(CASE WHEN priority = 'Critical' THEN 1 ELSE 0 END) as critical_alerts,
            SUM(CASE WHEN priority = 'High' THEN 1 ELSE 0 END) as high_alerts,
            SUM(CASE WHEN priority = 'Medium' THEN 1 ELSE 0 END) as medium_alerts,
            SUM(CASE WHEN priority = 'Low' THEN 1 ELSE 0 END) as low_alerts,
            SUM(CASE WHEN alert_type = 'Stockout' THEN 1 ELSE 0 END) as stockout_alerts,
            SUM(CASE WHEN alert_type = 'Excess_Inventory' THEN 1 ELSE 0 END) as excess_inventory_alerts,
            SUM(CASE WHEN alert_type = 'Transfer_Opportunity' THEN 1 ELSE 0 END) as transfer_opportunity_alerts,
            SUM(CASE WHEN alert_type = 'Reorder_Needed' THEN 1 ELSE 0 END) as reorder_needed_alerts
        FROM {CATALOG}.{GOLD_SCHEMA}.supply_chain_alerts
        WHERE alert_date = CURRENT_DATE()
    ),
    inventory_health AS (
        SELECT
            SUM(CASE WHEN stockout_risk_level IN ('Critical', 'High', 'Medium') THEN 1 ELSE 0 END) as products_at_risk,
            SUM(CASE WHEN stockout_risk_level = 'Critical' THEN 1 ELSE 0 END) as products_critical_stockout,
            SUM(CASE WHEN is_excess_inventory THEN 1 ELSE 0 END) as products_excess_inventory,
            SUM(CASE WHEN is_dead_stock THEN 1 ELSE 0 END) as products_dead_stock,
            SUM(CASE WHEN stockout_risk_level = 'Healthy' THEN 1 ELSE 0 END) as products_healthy_stock,
            SUM(stock_value) as total_inventory_value,
            SUM(CASE WHEN is_excess_inventory THEN stock_value ELSE 0 END) as excess_inventory_value,
            SUM(CASE WHEN stockout_risk_level IN ('Critical', 'High') THEN stock_value ELSE 0 END) as at_risk_inventory_value,
            ROUND(AVG(days_of_inventory), 2) as avg_days_of_inventory,
            ROUND(AVG(inventory_turnover_30d), 2) as avg_inventory_turnover
        FROM {CATALOG}.{GOLD_SCHEMA}.supply_chain_predictions
        WHERE prediction_date = CURRENT_DATE()
    ),
    reorder_stats AS (
        SELECT
            SUM(CASE WHEN recommended_order_qty > 0 THEN 1 ELSE 0 END) as total_reorder_recommendations,
            0 as total_transfer_recommendations  -- Placeholder for future transfer logic
        FROM {CATALOG}.{GOLD_SCHEMA}.supply_chain_predictions
        WHERE prediction_date = CURRENT_DATE()
    )
    SELECT
        ps.metric_date,
        ps.total_predictions,
        ps.total_product_warehouse_combos,
        als.total_alerts,
        als.critical_alerts,
        als.high_alerts,
        als.medium_alerts,
        als.low_alerts,
        als.stockout_alerts,
        als.excess_inventory_alerts,
        als.transfer_opportunity_alerts,
        als.reorder_needed_alerts,
        ih.products_at_risk,
        ih.products_critical_stockout,
        ih.products_excess_inventory,
        ih.products_dead_stock,
        ih.products_healthy_stock,
        ih.total_inventory_value,
        ih.excess_inventory_value,
        ih.at_risk_inventory_value,
        NULL as estimated_prevented_stockouts,
        NULL as estimated_revenue_protected,
        NULL as estimated_cost_savings,
        NULL as estimated_capital_released,
        NULL as predictions_validated,
        NULL as predictions_accurate,
        NULL as predictions_inaccurate,
        NULL as prediction_accuracy_pct,
        NULL as stockout_prediction_accuracy,
        NULL as forecast_mape,
        ih.avg_days_of_inventory,
        ih.avg_inventory_turnover,
        rs.total_reorder_recommendations,
        rs.total_transfer_recommendations,
        NULL as total_alerts_acknowledged,
        NULL as total_alerts_resolved,
        NULL as avg_time_to_acknowledge_hrs,
        NULL as avg_time_to_resolve_hrs,
        {(datetime.now() - pipeline_start_time).total_seconds() / 60.0} as pipeline_run_duration_mins,
        TRUE as pipeline_success,
        NULL as pipeline_error_message,
        CURRENT_TIMESTAMP() as created_at
    FROM prediction_stats ps
    CROSS JOIN alert_stats als
    CROSS JOIN inventory_health ih
    CROSS JOIN reorder_stats rs
    """
    
    metrics_df = spark.sql(metrics_sql)
    
    # Delete today's metrics if they exist (idempotent)
    spark.sql(f"""
        DELETE FROM {CATALOG}.{GOLD_SCHEMA}.supply_chain_metrics
        WHERE metric_date = CURRENT_DATE()
    """)
    
    # Insert new metrics
    metrics_df.write.mode("append").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.supply_chain_metrics")
    
    log_event("Pipeline_Run", f"Metrics calculated and inserted for {date.today()}", "INFO")
    print(f"✅ Metrics calculated and inserted")
    
    # Display key metrics
    print("\n📊 Key Metrics Summary:")
    metrics_df.select(
        "total_predictions",
        "total_alerts",
        "critical_alerts",
        "products_at_risk",
        "products_excess_inventory",
        "total_inventory_value"
    ).show(vertical=True)
    
    step_duration = (datetime.now() - step_start_time).total_seconds()
    print(f"⏱️ Duration: {step_duration:.2f} seconds")
    
except Exception as e:
    error_msg = f"Metrics calculation failed: {str(e)}"
    log_event("Error", error_msg, "ERROR")
    print(f"❌ {error_msg}")
    print(traceback.format_exc())
    raise

# COMMAND ----------

# DBTITLE 1,Pipeline Summary and Completion
# Calculate total pipeline duration
pipeline_end_time = datetime.now()
pipeline_duration = (pipeline_end_time - pipeline_start_time).total_seconds()

print("\n" + "="*70)
print("PIPELINE EXECUTION SUMMARY")
print("="*70)

print(f"\n✅ Pipeline completed successfully!")
print(f"\n📊 Execution Details:")
print(f"  Run ID: {pipeline_run_id}")
print(f"  Start Time: {pipeline_start_time}")
print(f"  End Time: {pipeline_end_time}")
print(f"  Total Duration: {pipeline_duration / 60:.2f} minutes ({pipeline_duration:.1f} seconds)")

print(f"\n📋 Processing Summary:")
print(f"  Errors: {len(errors)}")
print(f"  Warnings: {len(warnings)}")

if errors:
    print("\n❌ Errors encountered:")
    for error in errors:
        print(f"  - {error}")

if warnings:
    print("\n⚠️ Warnings:")
    for warning in warnings:
        print(f"  - {warning}")

# Final audit log
if len(errors) == 0:
    log_event("Pipeline_Run", 
              f"Pipeline completed successfully in {pipeline_duration / 60:.2f} minutes", 
              "INFO")
    
    print("\n🎉 All supply chain tables updated successfully!")
    print(f"\n📊 Next Steps:")
    print(f"  1. Query alerts: SELECT * FROM {CATALOG}.{GOLD_SCHEMA}.supply_chain_alerts WHERE alert_date = CURRENT_DATE()")
    print(f"  2. View metrics: SELECT * FROM {CATALOG}.{GOLD_SCHEMA}.supply_chain_metrics WHERE metric_date = CURRENT_DATE()")
    print(f"  3. Deploy Agent Bricks endpoint with tool functions")
else:
    log_event("Pipeline_Run", 
              f"Pipeline completed with {len(errors)} error(s)", 
              "ERROR")
    print("\n❌ Pipeline completed with errors. Review logs above.")

print("\n" + "="*70)

# Return success status for job orchestration
dbutils.notebook.exit("SUCCESS" if len(errors) == 0 else "FAILED")