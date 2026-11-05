# Databricks notebook source
# DBTITLE 1,Prediction Engine Header
# MAGIC %md
# MAGIC # 🔮 Prediction Engine
# MAGIC Generates daily predictions and writes to gold.supply_chain_predictions

# COMMAND ----------

# DBTITLE 1,Insert Predictions into Gold Table
# MAGIC %sql
# MAGIC -- Insert predictions into gold table
# MAGIC INSERT INTO sap_dev.gold.supply_chain_predictions
# MAGIC SELECT 
# MAGIC   CURRENT_DATE() as prediction_date,
# MAGIC   product_sk,
# MAGIC   warehouse_sk,
# MAGIC   current_stock,
# MAGIC   stock_value,
# MAGIC   sales_last_7d,
# MAGIC   avg_daily_sales_7d,
# MAGIC   avg_daily_sales_30d,
# MAGIC   sales_volatility_30d,
# MAGIC   predicted_stockout_date,
# MAGIC   days_until_stockout,
# MAGIC   days_of_inventory,
# MAGIC   recommended_order_qty,
# MAGIC   NULL as recommended_reorder_date,
# MAGIC   calculated_safety_stock,
# MAGIC   calculated_reorder_point,
# MAGIC   stockout_risk_level,
# MAGIC   1 as priority_score,
# MAGIC   NULL as forecast_7d_sales,
# MAGIC   NULL as forecast_confidence,
# MAGIC   CURRENT_TIMESTAMP() as calculated_at,
# MAGIC   'v1.0' as model_version
# MAGIC FROM sales_velocity_features;

# COMMAND ----------

