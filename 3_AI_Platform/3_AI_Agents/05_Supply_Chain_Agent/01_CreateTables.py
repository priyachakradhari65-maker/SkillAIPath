# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Supply Chain Agent - Table Creation
# MAGIC %md
# MAGIC # 🏗️ Supply Chain Intelligence Agent - Data Foundation
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook creates the gold layer tables for the Supply Chain Intelligence Agent:
# MAGIC
# MAGIC 1. **supply_chain_predictions** - Daily predictions for all product-warehouse combinations
# MAGIC 2. **supply_chain_alerts** - Actionable alerts (stockouts, dead stock, transfers)
# MAGIC 3. **supply_chain_metrics** - KPI tracking and accuracy metrics
# MAGIC 4. **supply_chain_audit_log** - Audit trail of agent decisions
# MAGIC
# MAGIC **Catalog:** `sap_dev`  
# MAGIC **Schema:** `gold`  
# MAGIC **Run Time:** ~2 minutes

# COMMAND ----------

# DBTITLE 1,Set catalog and schema
# MAGIC %sql
# MAGIC -- Set the target catalog and schema
# MAGIC USE CATALOG sap_dev;
# MAGIC USE SCHEMA gold;

# COMMAND ----------

# DBTITLE 1,Create supply_chain_predictions table
# MAGIC %sql
# MAGIC -- Table 1: supply_chain_predictions
# MAGIC -- Main predictions table with stock levels, sales velocity, stockout predictions, and reorder recommendations
# MAGIC
# MAGIC CREATE OR REPLACE TABLE sap_dev.gold.supply_chain_predictions (
# MAGIC   -- Keys
# MAGIC   prediction_id BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Unique prediction record ID',
# MAGIC   prediction_date DATE NOT NULL COMMENT 'Date when prediction was generated',
# MAGIC   product_sk INT NOT NULL COMMENT 'Product surrogate key from dim_product',
# MAGIC   warehouse_sk INT NOT NULL COMMENT 'Warehouse surrogate key from dim_warehouse',
# MAGIC   
# MAGIC   -- Current State
# MAGIC   current_stock INT NOT NULL COMMENT 'Current stock quantity at prediction time',
# MAGIC   stock_value DECIMAL(15,2) COMMENT 'Current stock value in rupees',
# MAGIC   stock_status STRING COMMENT 'Current status from fact_inventory (Low Stock, In Stock, etc.)',
# MAGIC   
# MAGIC   -- Sales Velocity Metrics
# MAGIC   sales_last_7d INT COMMENT 'Total units sold in last 7 days',
# MAGIC   sales_last_14d INT COMMENT 'Total units sold in last 14 days',
# MAGIC   sales_last_30d INT COMMENT 'Total units sold in last 30 days',
# MAGIC   avg_daily_sales_7d DECIMAL(10,2) COMMENT '7-day rolling average daily sales',
# MAGIC   avg_daily_sales_14d DECIMAL(10,2) COMMENT '14-day rolling average daily sales',
# MAGIC   avg_daily_sales_30d DECIMAL(10,2) COMMENT '30-day rolling average daily sales',
# MAGIC   sales_volatility DECIMAL(10,2) COMMENT 'Standard deviation of daily sales (demand variability)',
# MAGIC   
# MAGIC   -- Stockout Predictions
# MAGIC   predicted_stockout_date DATE COMMENT 'Predicted date when stock will run out',
# MAGIC   days_until_stockout INT COMMENT 'Number of days until predicted stockout',
# MAGIC   stockout_risk_level STRING COMMENT 'Risk level: Critical (<1 day), High (<3 days), Medium (<7 days), Low (>7 days)',
# MAGIC   
# MAGIC   -- Inventory Metrics
# MAGIC   days_of_inventory DECIMAL(10,2) COMMENT 'Days of inventory on hand (DOI = Stock / Avg Daily Sales)',
# MAGIC   inventory_turnover_30d DECIMAL(10,2) COMMENT '30-day inventory turnover rate',
# MAGIC   
# MAGIC   -- Reorder Recommendations
# MAGIC   supplier_lead_time_days INT COMMENT 'Average supplier lead time from fact_purchase history',
# MAGIC   safety_stock INT COMMENT 'Calculated safety stock quantity',
# MAGIC   reorder_point INT COMMENT 'Optimal reorder point (when to trigger replenishment)',
# MAGIC   recommended_order_qty INT COMMENT 'Recommended order quantity (units)',
# MAGIC   recommended_reorder_date DATE COMMENT 'Recommended date to place order',
# MAGIC   order_cost_estimate DECIMAL(15,2) COMMENT 'Estimated cost of recommended order',
# MAGIC   
# MAGIC   -- Demand Forecast (7-day ahead)
# MAGIC   forecast_day1 INT COMMENT 'Predicted sales for tomorrow',
# MAGIC   forecast_day2 INT COMMENT 'Predicted sales for day +2',
# MAGIC   forecast_day3 INT COMMENT 'Predicted sales for day +3',
# MAGIC   forecast_day4 INT COMMENT 'Predicted sales for day +4',
# MAGIC   forecast_day5 INT COMMENT 'Predicted sales for day +5',
# MAGIC   forecast_day6 INT COMMENT 'Predicted sales for day +6',
# MAGIC   forecast_day7 INT COMMENT 'Predicted sales for day +7',
# MAGIC   forecast_confidence DECIMAL(5,2) COMMENT 'Forecast confidence percentage (0-100)',
# MAGIC   
# MAGIC   -- Classification Flags
# MAGIC   is_critical_stockout BOOLEAN COMMENT 'Flag: stockout predicted within 1 day',
# MAGIC   is_warning_stockout BOOLEAN COMMENT 'Flag: stockout predicted within 7 days',
# MAGIC   is_excess_inventory BOOLEAN COMMENT 'Flag: days of inventory > 60 days',
# MAGIC   is_dead_stock BOOLEAN COMMENT 'Flag: zero sales in last 14 days',
# MAGIC   
# MAGIC   -- Metadata
# MAGIC   created_at TIMESTAMP COMMENT 'Record creation timestamp',
# MAGIC   updated_at TIMESTAMP COMMENT 'Record last update timestamp'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Daily supply chain predictions for all product-warehouse combinations. Core table for stockout prevention and inventory optimization.'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );
# MAGIC
# MAGIC SELECT '✅ Table 1: supply_chain_predictions created successfully' AS status;

# COMMAND ----------

# DBTITLE 1,Create supply_chain_alerts table
# MAGIC %sql
# MAGIC -- Table 2: supply_chain_alerts
# MAGIC -- Actionable alerts for stockouts, dead stock, transfers, and reorder recommendations
# MAGIC
# MAGIC CREATE OR REPLACE TABLE sap_dev.gold.supply_chain_alerts (
# MAGIC   -- Keys
# MAGIC   alert_id BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Unique alert ID',
# MAGIC   alert_date DATE NOT NULL COMMENT 'Date when alert was generated',
# MAGIC   alert_type STRING NOT NULL COMMENT 'Alert type: Stockout, Excess_Inventory, Transfer_Opportunity, Reorder_Needed',
# MAGIC   priority STRING NOT NULL COMMENT 'Priority level: Critical, High, Medium, Low',
# MAGIC   
# MAGIC   -- Asset References
# MAGIC   product_sk INT NOT NULL COMMENT 'Product surrogate key',
# MAGIC   warehouse_sk INT NOT NULL COMMENT 'Warehouse surrogate key',
# MAGIC   product_name STRING COMMENT 'Product name (denormalized for quick access)',
# MAGIC   warehouse_name STRING COMMENT 'Warehouse name (denormalized for quick access)',
# MAGIC   
# MAGIC   -- Alert Details
# MAGIC   alert_title STRING COMMENT 'Short alert title (e.g., "Critical Stockout in 0.1 days")',
# MAGIC   alert_message STRING COMMENT 'Detailed human-readable alert message',
# MAGIC   recommended_action STRING COMMENT 'Specific action recommendation (e.g., "URGENT REORDER 120 units")',
# MAGIC   
# MAGIC   -- Context & Impact
# MAGIC   current_stock INT COMMENT 'Current stock level',
# MAGIC   avg_daily_sales DECIMAL(10,2) COMMENT 'Average daily sales rate',
# MAGIC   days_until_stockout INT COMMENT 'Days until stockout (for stockout alerts)',
# MAGIC   days_of_inventory DECIMAL(10,2) COMMENT 'Days of inventory (for excess inventory alerts)',
# MAGIC   estimated_impact_value DECIMAL(15,2) COMMENT 'Estimated financial impact (revenue loss or cost savings)',
# MAGIC   
# MAGIC   -- Transfer-Specific (for Transfer_Opportunity alerts)
# MAGIC   transfer_from_warehouse_sk INT COMMENT 'Source warehouse for transfer recommendation',
# MAGIC   transfer_to_warehouse_sk INT COMMENT 'Destination warehouse for transfer recommendation',
# MAGIC   transfer_quantity INT COMMENT 'Recommended transfer quantity',
# MAGIC   transfer_cost_estimate DECIMAL(15,2) COMMENT 'Estimated cost of transfer',
# MAGIC   transfer_net_benefit DECIMAL(15,2) COMMENT 'Net benefit of transfer (impact - cost)',
# MAGIC   
# MAGIC   -- Reorder-Specific (for Reorder_Needed alerts)
# MAGIC   recommended_order_qty INT COMMENT 'Recommended reorder quantity',
# MAGIC   order_cost_estimate DECIMAL(15,2) COMMENT 'Estimated cost of reorder',
# MAGIC   supplier_lead_time_days INT COMMENT 'Supplier lead time',
# MAGIC   
# MAGIC   -- Alert State Management
# MAGIC   is_active BOOLEAN COMMENT 'Whether alert is still active/unresolved',
# MAGIC   is_acknowledged BOOLEAN COMMENT 'Whether someone has acknowledged this alert',
# MAGIC   acknowledged_by STRING COMMENT 'User email who acknowledged the alert',
# MAGIC   acknowledged_at TIMESTAMP COMMENT 'When alert was acknowledged',
# MAGIC   resolution_notes STRING COMMENT 'Notes about how alert was resolved',
# MAGIC   resolved_at TIMESTAMP COMMENT 'When alert was resolved/closed',
# MAGIC   
# MAGIC   -- Metadata
# MAGIC   created_at TIMESTAMP COMMENT 'Alert creation timestamp',
# MAGIC   updated_at TIMESTAMP COMMENT 'Alert last update timestamp'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Actionable supply chain alerts for immediate attention. Used by agent to surface critical issues.'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );
# MAGIC
# MAGIC SELECT '✅ Table 2: supply_chain_alerts created successfully' AS status;

# COMMAND ----------

# DBTITLE 1,Create supply_chain_metrics table
# MAGIC %sql
# MAGIC -- Table 3: supply_chain_metrics
# MAGIC -- KPI tracking and prediction accuracy metrics for agent performance monitoring
# MAGIC
# MAGIC CREATE OR REPLACE TABLE sap_dev.gold.supply_chain_metrics (
# MAGIC   -- Keys
# MAGIC   metric_id BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Unique metric record ID',
# MAGIC   metric_date DATE NOT NULL COMMENT 'Date of metrics snapshot',
# MAGIC   
# MAGIC   -- Prediction Metrics
# MAGIC   total_predictions INT COMMENT 'Total number of predictions generated',
# MAGIC   total_product_warehouse_combos INT COMMENT 'Total product-warehouse combinations analyzed',
# MAGIC   
# MAGIC   -- Alert Metrics
# MAGIC   total_alerts INT COMMENT 'Total alerts generated',
# MAGIC   critical_alerts INT COMMENT 'Number of critical priority alerts',
# MAGIC   high_alerts INT COMMENT 'Number of high priority alerts',
# MAGIC   medium_alerts INT COMMENT 'Number of medium priority alerts',
# MAGIC   low_alerts INT COMMENT 'Number of low priority alerts',
# MAGIC   
# MAGIC   -- Alert Type Breakdown
# MAGIC   stockout_alerts INT COMMENT 'Number of stockout alerts',
# MAGIC   excess_inventory_alerts INT COMMENT 'Number of excess inventory alerts',
# MAGIC   transfer_opportunity_alerts INT COMMENT 'Number of transfer opportunity alerts',
# MAGIC   reorder_needed_alerts INT COMMENT 'Number of reorder needed alerts',
# MAGIC   
# MAGIC   -- Inventory Health Metrics
# MAGIC   products_at_risk INT COMMENT 'Number of products at risk of stockout (<7 days)',
# MAGIC   products_critical_stockout INT COMMENT 'Number of products with critical stockout (<1 day)',
# MAGIC   products_excess_inventory INT COMMENT 'Number of products with excess inventory (>60 days DOI)',
# MAGIC   products_dead_stock INT COMMENT 'Number of products with zero sales (14+ days)',
# MAGIC   products_healthy_stock INT COMMENT 'Number of products with healthy stock levels',
# MAGIC   
# MAGIC   -- Inventory Value Metrics
# MAGIC   total_inventory_value DECIMAL(18,2) COMMENT 'Total value of inventory across all warehouses',
# MAGIC   excess_inventory_value DECIMAL(18,2) COMMENT 'Value tied up in excess inventory',
# MAGIC   at_risk_inventory_value DECIMAL(18,2) COMMENT 'Value of inventory at risk of stockout',
# MAGIC   
# MAGIC   -- Business Impact Metrics
# MAGIC   estimated_prevented_stockouts INT COMMENT 'Number of stockouts prevented by agent recommendations',
# MAGIC   estimated_revenue_protected DECIMAL(18,2) COMMENT 'Estimated revenue protected by preventing stockouts',
# MAGIC   estimated_cost_savings DECIMAL(18,2) COMMENT 'Estimated cost savings from transfer recommendations',
# MAGIC   estimated_capital_released DECIMAL(18,2) COMMENT 'Estimated capital released from excess inventory reduction',
# MAGIC   
# MAGIC   -- Accuracy Metrics (for validation)
# MAGIC   predictions_validated INT COMMENT 'Number of predictions validated against actuals',
# MAGIC   predictions_accurate INT COMMENT 'Number of accurate predictions',
# MAGIC   predictions_inaccurate INT COMMENT 'Number of inaccurate predictions',
# MAGIC   prediction_accuracy_pct DECIMAL(5,2) COMMENT 'Overall prediction accuracy percentage',
# MAGIC   stockout_prediction_accuracy DECIMAL(5,2) COMMENT 'Accuracy of stockout date predictions',
# MAGIC   forecast_mape DECIMAL(5,2) COMMENT 'Mean Absolute Percentage Error of demand forecasts',
# MAGIC   
# MAGIC   -- Operational Metrics
# MAGIC   avg_days_of_inventory DECIMAL(10,2) COMMENT 'Average days of inventory across all products',
# MAGIC   avg_inventory_turnover DECIMAL(10,2) COMMENT 'Average inventory turnover rate',
# MAGIC   total_reorder_recommendations INT COMMENT 'Total reorder recommendations generated',
# MAGIC   total_transfer_recommendations INT COMMENT 'Total transfer recommendations generated',
# MAGIC   
# MAGIC   -- Agent Performance
# MAGIC   total_alerts_acknowledged INT COMMENT 'Number of alerts acknowledged by users',
# MAGIC   total_alerts_resolved INT COMMENT 'Number of alerts resolved',
# MAGIC   avg_time_to_acknowledge_hrs DECIMAL(10,2) COMMENT 'Average hours to acknowledge alerts',
# MAGIC   avg_time_to_resolve_hrs DECIMAL(10,2) COMMENT 'Average hours to resolve alerts',
# MAGIC   
# MAGIC   -- Pipeline Execution Metrics
# MAGIC   pipeline_run_duration_mins DECIMAL(10,2) COMMENT 'Time taken to generate predictions (minutes)',
# MAGIC   pipeline_success BOOLEAN COMMENT 'Whether the pipeline ran successfully',
# MAGIC   pipeline_error_message STRING COMMENT 'Error message if pipeline failed',
# MAGIC   
# MAGIC   -- Metadata
# MAGIC   created_at TIMESTAMP COMMENT 'Record creation timestamp'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Daily KPIs and accuracy metrics for Supply Chain Intelligence Agent performance tracking.'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );
# MAGIC
# MAGIC SELECT '✅ Table 3: supply_chain_metrics created successfully' AS status;

# COMMAND ----------

# DBTITLE 1,Create supply_chain_audit_log table
# MAGIC %sql
# MAGIC -- Table 4: supply_chain_audit_log
# MAGIC -- Comprehensive audit trail of all agent decisions, recommendations, and actions
# MAGIC
# MAGIC CREATE OR REPLACE TABLE sap_dev.gold.supply_chain_audit_log (
# MAGIC   -- Keys
# MAGIC   audit_id BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Unique audit log entry ID',
# MAGIC   audit_timestamp TIMESTAMP COMMENT 'Exact timestamp of logged event',
# MAGIC   audit_date DATE COMMENT 'Date of audit event (for partitioning)',
# MAGIC   
# MAGIC   -- Event Classification
# MAGIC   event_type STRING NOT NULL COMMENT 'Type of event: Prediction_Generated, Alert_Created, Alert_Acknowledged, Alert_Resolved, Recommendation_Made, Action_Taken, Pipeline_Run, Error',
# MAGIC   event_category STRING COMMENT 'Category: Prediction, Alert, Recommendation, Action, System',
# MAGIC   severity STRING COMMENT 'Severity: Info, Warning, Error, Critical',
# MAGIC   
# MAGIC   -- Context
# MAGIC   product_sk INT COMMENT 'Related product surrogate key',
# MAGIC   warehouse_sk INT COMMENT 'Related warehouse surrogate key',
# MAGIC   prediction_id BIGINT COMMENT 'Related prediction record ID',
# MAGIC   alert_id BIGINT COMMENT 'Related alert record ID',
# MAGIC   
# MAGIC   -- Event Details
# MAGIC   event_description STRING COMMENT 'Human-readable description of what happened',
# MAGIC   event_data STRING COMMENT 'JSON payload with full event details (for detailed analysis)',
# MAGIC   
# MAGIC   -- Agent Decision Details
# MAGIC   decision_made STRING COMMENT 'What decision the agent made',
# MAGIC   decision_rationale STRING COMMENT 'Why the agent made this decision',
# MAGIC   input_parameters STRING COMMENT 'JSON of input parameters used for decision',
# MAGIC   output_results STRING COMMENT 'JSON of output results from decision',
# MAGIC   
# MAGIC   -- Recommendation Tracking
# MAGIC   recommendation_type STRING COMMENT 'Type: Reorder, Transfer, Markdown, Hold',
# MAGIC   recommendation_details STRING COMMENT 'Specific recommendation given',
# MAGIC   recommended_quantity INT COMMENT 'Quantity recommended (for reorders/transfers)',
# MAGIC   estimated_cost DECIMAL(15,2) COMMENT 'Estimated cost of recommendation',
# MAGIC   estimated_impact DECIMAL(15,2) COMMENT 'Estimated financial impact/benefit',
# MAGIC   
# MAGIC   -- Action Tracking
# MAGIC   action_taken STRING COMMENT 'What action was actually taken (if any)',
# MAGIC   action_taken_by STRING COMMENT 'User email who took the action',
# MAGIC   action_taken_at TIMESTAMP COMMENT 'When action was taken',
# MAGIC   action_outcome STRING COMMENT 'Result/outcome of the action',
# MAGIC   
# MAGIC   -- User Interaction
# MAGIC   user_email STRING COMMENT 'User who interacted with agent',
# MAGIC   user_query STRING COMMENT 'User question/query to agent',
# MAGIC   agent_response STRING COMMENT 'Agent response to user',
# MAGIC   
# MAGIC   -- System Context
# MAGIC   pipeline_run_id STRING COMMENT 'Pipeline execution ID (for tracing)',
# MAGIC   notebook_path STRING COMMENT 'Notebook path that generated this log',
# MAGIC   job_id STRING COMMENT 'Databricks job ID (if triggered by job)',
# MAGIC   run_id STRING COMMENT 'Databricks run ID (if triggered by job)',
# MAGIC   
# MAGIC   -- Error Tracking
# MAGIC   error_occurred BOOLEAN COMMENT 'Whether an error occurred',
# MAGIC   error_message STRING COMMENT 'Error message if error occurred',
# MAGIC   error_stack_trace STRING COMMENT 'Full stack trace for debugging',
# MAGIC   
# MAGIC   -- Performance Tracking
# MAGIC   execution_time_ms BIGINT COMMENT 'Execution time in milliseconds',
# MAGIC   rows_processed BIGINT COMMENT 'Number of rows/records processed',
# MAGIC   
# MAGIC   -- Metadata
# MAGIC   created_at TIMESTAMP COMMENT 'Log entry creation timestamp',
# MAGIC   log_version STRING COMMENT 'Log schema version'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (audit_date)
# MAGIC COMMENT 'Comprehensive audit trail for Supply Chain Intelligence Agent. Tracks all predictions, alerts, recommendations, actions, and user interactions.'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true',
# MAGIC   'delta.logRetentionDuration' = 'interval 90 days',
# MAGIC   'delta.deletedFileRetentionDuration' = 'interval 90 days'
# MAGIC );
# MAGIC
# MAGIC SELECT '✅ Table 4: supply_chain_audit_log created successfully' AS status;

# COMMAND ----------

# DBTITLE 1,Verify all tables created
# MAGIC %sql
# MAGIC -- Verification: Check all tables exist and show their properties
# MAGIC
# MAGIC SELECT 
# MAGIC   '📊 SUPPLY CHAIN AGENT TABLES VERIFICATION' AS section,
# MAGIC   '═══════════════════════════════════════════' AS separator
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   CONCAT('✅ ', table_name) AS table_status,
# MAGIC   CONCAT('Columns: ', CAST(COUNT(*) AS STRING)) AS column_count
# MAGIC FROM (
# MAGIC   SELECT 'supply_chain_predictions' AS table_name, column_name 
# MAGIC   FROM sap_dev.information_schema.columns 
# MAGIC   WHERE table_catalog = 'sap_dev' 
# MAGIC     AND table_schema = 'gold' 
# MAGIC     AND table_name = 'supply_chain_predictions'
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   SELECT 'supply_chain_alerts' AS table_name, column_name 
# MAGIC   FROM sap_dev.information_schema.columns 
# MAGIC   WHERE table_catalog = 'sap_dev' 
# MAGIC     AND table_schema = 'gold' 
# MAGIC     AND table_name = 'supply_chain_alerts'
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   SELECT 'supply_chain_metrics' AS table_name, column_name 
# MAGIC   FROM sap_dev.information_schema.columns 
# MAGIC   WHERE table_catalog = 'sap_dev' 
# MAGIC     AND table_schema = 'gold' 
# MAGIC     AND table_name = 'supply_chain_metrics'
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   SELECT 'supply_chain_audit_log' AS table_name, column_name 
# MAGIC   FROM sap_dev.information_schema.columns 
# MAGIC   WHERE table_catalog = 'sap_dev' 
# MAGIC     AND table_schema = 'gold' 
# MAGIC     AND table_name = 'supply_chain_audit_log'
# MAGIC )
# MAGIC GROUP BY table_name
# MAGIC ORDER BY section;
# MAGIC
# MAGIC -- Show detailed schema for all tables
# MAGIC SHOW TABLES IN sap_dev.gold LIKE 'supply_chain*';