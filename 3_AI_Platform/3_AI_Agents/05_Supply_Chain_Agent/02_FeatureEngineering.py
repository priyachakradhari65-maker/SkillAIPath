# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Feature Engineering - Sales Velocity
# MAGIC %md
# MAGIC # 🛠️ Supply Chain Feature Engineering
# MAGIC
# MAGIC Calculates sales velocity metrics for all product-warehouse combinations:
# MAGIC - 7/14/30-day rolling averages
# MAGIC - Sales volatility (std dev)
# MAGIC - Days of inventory (DOI)
# MAGIC - Purchase patterns (lead time, order frequency)

# COMMAND ----------

# DBTITLE 1,Set catalog and schema
# MAGIC %sql
# MAGIC -- Set working context
# MAGIC USE CATALOG sap_dev;
# MAGIC USE SCHEMA silver;

# COMMAND ----------

# DBTITLE 1,Step 1: Calculate Sales Velocity Metrics
# MAGIC %sql
# MAGIC -- Step 1: Calculate sales velocity for all product-warehouse combinations
# MAGIC -- Rolling averages over 7, 14, and 30 days with sales volatility
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW sales_velocity AS
# MAGIC SELECT
# MAGIC   fs.ProductSK,
# MAGIC   fs.WarehouseSK,
# MAGIC   CURRENT_DATE() AS analysis_date,
# MAGIC   
# MAGIC   -- Sales totals by time period
# MAGIC   SUM(CASE WHEN fs.BatchDate >= DATE_SUB(CURRENT_DATE(), 7) THEN fs.ProductQuantity ELSE 0 END) AS sales_last_7d,
# MAGIC   SUM(CASE WHEN fs.BatchDate >= DATE_SUB(CURRENT_DATE(), 14) THEN fs.ProductQuantity ELSE 0 END) AS sales_last_14d,
# MAGIC   SUM(CASE WHEN fs.BatchDate >= DATE_SUB(CURRENT_DATE(), 30) THEN fs.ProductQuantity ELSE 0 END) AS sales_last_30d,
# MAGIC   
# MAGIC   -- Average daily sales (rolling averages)
# MAGIC   COALESCE(
# MAGIC     SUM(CASE WHEN fs.BatchDate >= DATE_SUB(CURRENT_DATE(), 7) THEN fs.ProductQuantity ELSE 0 END) / 7.0,
# MAGIC     0
# MAGIC   ) AS avg_daily_sales_7d,
# MAGIC   
# MAGIC   COALESCE(
# MAGIC     SUM(CASE WHEN fs.BatchDate >= DATE_SUB(CURRENT_DATE(), 14) THEN fs.ProductQuantity ELSE 0 END) / 14.0,
# MAGIC     0
# MAGIC   ) AS avg_daily_sales_14d,
# MAGIC   
# MAGIC   COALESCE(
# MAGIC     SUM(CASE WHEN fs.BatchDate >= DATE_SUB(CURRENT_DATE(), 30) THEN fs.ProductQuantity ELSE 0 END) / 30.0,
# MAGIC     0
# MAGIC   ) AS avg_daily_sales_30d,
# MAGIC   
# MAGIC   -- Sales volatility (standard deviation of daily sales)
# MAGIC   COALESCE(
# MAGIC     STDDEV(CASE WHEN fs.BatchDate >= DATE_SUB(CURRENT_DATE(), 30) THEN fs.ProductQuantity END),
# MAGIC     0
# MAGIC   ) AS sales_volatility_30d,
# MAGIC   
# MAGIC   -- Max and min daily sales (for safety stock calculation)
# MAGIC   COALESCE(
# MAGIC     MAX(CASE WHEN fs.BatchDate >= DATE_SUB(CURRENT_DATE(), 30) THEN fs.ProductQuantity END),
# MAGIC     0
# MAGIC   ) AS max_daily_sales_30d,
# MAGIC   
# MAGIC   COALESCE(
# MAGIC     MIN(CASE WHEN fs.BatchDate >= DATE_SUB(CURRENT_DATE(), 30) THEN fs.ProductQuantity END),
# MAGIC     0
# MAGIC   ) AS min_daily_sales_30d,
# MAGIC   
# MAGIC   -- Days with sales (to detect dead stock)
# MAGIC   COUNT(DISTINCT CASE WHEN fs.BatchDate >= DATE_SUB(CURRENT_DATE(), 7) AND fs.ProductQuantity > 0 THEN fs.BatchDate END) AS days_with_sales_7d,
# MAGIC   COUNT(DISTINCT CASE WHEN fs.BatchDate >= DATE_SUB(CURRENT_DATE(), 14) AND fs.ProductQuantity > 0 THEN fs.BatchDate END) AS days_with_sales_14d,
# MAGIC   COUNT(DISTINCT CASE WHEN fs.BatchDate >= DATE_SUB(CURRENT_DATE(), 30) AND fs.ProductQuantity > 0 THEN fs.BatchDate END) AS days_with_sales_30d,
# MAGIC   
# MAGIC   -- Revenue metrics
# MAGIC   COALESCE(
# MAGIC     SUM(CASE WHEN fs.BatchDate >= DATE_SUB(CURRENT_DATE(), 7) THEN fs.ProductQuantity * fs.sellingPrice ELSE 0 END),
# MAGIC     0
# MAGIC   ) AS revenue_last_7d,
# MAGIC   
# MAGIC   COALESCE(
# MAGIC     SUM(CASE WHEN fs.BatchDate >= DATE_SUB(CURRENT_DATE(), 30) THEN fs.ProductQuantity * fs.sellingPrice ELSE 0 END),
# MAGIC     0
# MAGIC   ) AS revenue_last_30d,
# MAGIC   
# MAGIC   -- Count of distinct orders
# MAGIC   COUNT(DISTINCT CASE WHEN fs.BatchDate >= DATE_SUB(CURRENT_DATE(), 30) THEN fs.OrderID END) AS order_count_30d
# MAGIC   
# MAGIC FROM sap_dev.silver.fact_sales fs
# MAGIC WHERE fs.BatchDate >= DATE_SUB(CURRENT_DATE(), 30)  -- Only last 30 days for performance
# MAGIC GROUP BY fs.ProductSK, fs.WarehouseSK
# MAGIC HAVING SUM(CASE WHEN fs.BatchDate >= DATE_SUB(CURRENT_DATE(), 30) THEN fs.ProductQuantity ELSE 0 END) > 0;  -- Only products with sales
# MAGIC
# MAGIC -- Quick verification
# MAGIC SELECT 
# MAGIC   '✅ Sales velocity calculated' AS status,
# MAGIC   COUNT(*) AS product_warehouse_combos,
# MAGIC   ROUND(AVG(avg_daily_sales_30d), 2) AS avg_velocity
# MAGIC FROM sales_velocity;

# COMMAND ----------

# DBTITLE 1,Step 2: Calculate Current Inventory Metrics
# MAGIC %sql
# MAGIC -- Step 2: Get current inventory levels and calculate Days of Inventory (DOI)
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW current_inventory AS
# MAGIC SELECT
# MAGIC   fi.ProductSK,
# MAGIC   fi.WarehouseSK,
# MAGIC   
# MAGIC   -- Current stock levels
# MAGIC   COALESCE(fi.StockQuantity, 0) AS current_stock,
# MAGIC   0 AS allocated_stock,  -- Not available in this schema
# MAGIC   COALESCE(fi.StockQuantity, 0) AS available_stock,
# MAGIC   
# MAGIC   -- Stock value
# MAGIC   COALESCE(fi.StockValue, 0) AS stock_value,
# MAGIC   CASE 
# MAGIC     WHEN fi.StockQuantity > 0 THEN ROUND(fi.StockValue / fi.StockQuantity, 2)
# MAGIC     ELSE 0
# MAGIC   END AS unit_cost,
# MAGIC   
# MAGIC   -- Stock status flags
# MAGIC   fi.StockStatus AS stock_status,
# MAGIC   CASE 
# MAGIC     WHEN COALESCE(fi.StockQuantity, 0) = 0 THEN TRUE 
# MAGIC     ELSE FALSE 
# MAGIC   END AS is_out_of_stock,
# MAGIC   
# MAGIC   CASE 
# MAGIC     WHEN fi.StockStatus = 'Low Stock' OR COALESCE(fi.StockQuantity, 0) <= 10 THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END AS is_low_stock,
# MAGIC   
# MAGIC   -- Last updated timestamp
# MAGIC   fi.LoadedAt AS last_updated,
# MAGIC   
# MAGIC   -- Reorder point placeholder (will be calculated later)
# MAGIC   0 AS existing_reorder_point
# MAGIC   
# MAGIC FROM sap_dev.silver.fact_inventory fi
# MAGIC WHERE fi.DateID = (
# MAGIC   SELECT MAX(DateID) 
# MAGIC   FROM sap_dev.silver.fact_inventory
# MAGIC );  -- Only most recent inventory snapshot
# MAGIC
# MAGIC -- Quick verification
# MAGIC SELECT 
# MAGIC   '✅ Current inventory calculated' AS status,
# MAGIC   COUNT(*) AS total_skus,
# MAGIC   SUM(CASE WHEN is_out_of_stock THEN 1 ELSE 0 END) AS out_of_stock_count,
# MAGIC   SUM(CASE WHEN is_low_stock THEN 1 ELSE 0 END) AS low_stock_count,
# MAGIC   ROUND(SUM(stock_value), 2) AS total_inventory_value
# MAGIC FROM current_inventory;

# COMMAND ----------

# DBTITLE 1,Step 3: Calculate Purchase Patterns & Lead Time
# MAGIC %sql
# MAGIC -- Step 3: Analyze purchase patterns to calculate supplier lead time and order frequency
# MAGIC -- Note: Delivery date not available in schema, so using defaults
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW purchase_patterns AS
# MAGIC SELECT
# MAGIC   fp.ProductSK,
# MAGIC   
# MAGIC   -- Lead time defaults (no delivery date in schema)
# MAGIC   4 AS avg_lead_time_days,
# MAGIC   3 AS min_lead_time_days,
# MAGIC   7 AS max_lead_time_days,
# MAGIC   
# MAGIC   -- Order frequency (days between orders)
# MAGIC   ROUND(
# MAGIC     30.0 / COUNT(DISTINCT fp.PurchaseID),
# MAGIC     1
# MAGIC   ) AS avg_days_between_orders,
# MAGIC   
# MAGIC   -- Order quantities
# MAGIC   ROUND(AVG(fp.Quantity), 0) AS avg_order_quantity,
# MAGIC   MIN(fp.Quantity) AS min_order_quantity,
# MAGIC   MAX(fp.Quantity) AS max_order_quantity,
# MAGIC   
# MAGIC   -- Unit costs
# MAGIC   ROUND(AVG(fp.UnitCost), 2) AS avg_unit_cost,
# MAGIC   ROUND(MIN(fp.UnitCost), 2) AS min_unit_cost,
# MAGIC   ROUND(MAX(fp.UnitCost), 2) AS max_unit_cost,
# MAGIC   
# MAGIC   -- Total purchases
# MAGIC   COUNT(DISTINCT fp.PurchaseID) AS purchase_count_30d,
# MAGIC   SUM(fp.Quantity) AS total_purchased_30d,
# MAGIC   
# MAGIC   -- Most recent purchase date
# MAGIC   MAX(fp.PurchaseDate) AS last_purchase_date,
# MAGIC   DATEDIFF(CURRENT_DATE(), MAX(fp.PurchaseDate)) AS days_since_last_purchase,
# MAGIC   
# MAGIC   -- Supplier reliability (assume 90% if no status field)
# MAGIC   90.0 AS supplier_reliability_pct
# MAGIC   
# MAGIC FROM sap_dev.silver.fact_purchase fp
# MAGIC WHERE fp.PurchaseDate >= DATE_SUB(CURRENT_DATE(), 90)  -- Last 90 days for stable patterns
# MAGIC GROUP BY fp.ProductSK
# MAGIC HAVING COUNT(DISTINCT fp.PurchaseID) >= 1;  -- At least 1 purchase
# MAGIC
# MAGIC -- Quick verification
# MAGIC SELECT 
# MAGIC   '✅ Purchase patterns calculated' AS status,
# MAGIC   COUNT(*) AS products_with_purchases,
# MAGIC   ROUND(AVG(avg_lead_time_days), 1) AS avg_lead_time,
# MAGIC   ROUND(AVG(avg_order_quantity), 0) AS avg_order_qty
# MAGIC FROM purchase_patterns;

# COMMAND ----------

# DBTITLE 1,Step 4: Join Product and Warehouse Dimensions
# MAGIC %sql
# MAGIC -- Step 4: Enrich with product and warehouse master data
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW enriched_features AS
# MAGIC SELECT
# MAGIC   -- Keys
# MAGIC   COALESCE(sv.ProductSK, ci.ProductSK) AS product_sk,
# MAGIC   COALESCE(sv.WarehouseSK, ci.WarehouseSK) AS warehouse_sk,
# MAGIC   CURRENT_DATE() AS analysis_date,
# MAGIC   
# MAGIC   -- Product details
# MAGIC   dp.ProductName AS product_name,
# MAGIC   'General' AS product_category,  -- Not in schema
# MAGIC   'Unknown' AS brand,  -- Not in schema
# MAGIC   'Standard' AS product_size,  -- Not in schema
# MAGIC   'Retail' AS product_line,  -- Not in schema
# MAGIC   
# MAGIC   -- Warehouse details
# MAGIC   dw.WareHouseName AS warehouse_name,
# MAGIC   'Unknown' AS warehouse_state,  -- Not in schema
# MAGIC   dw.WareHouseCity AS warehouse_city,
# MAGIC   'Central' AS warehouse_region,  -- Not in schema
# MAGIC   
# MAGIC   -- Current inventory
# MAGIC   COALESCE(ci.current_stock, 0) AS current_stock,
# MAGIC   COALESCE(ci.allocated_stock, 0) AS allocated_stock,
# MAGIC   COALESCE(ci.available_stock, 0) AS available_stock,
# MAGIC   COALESCE(ci.stock_value, 0) AS stock_value,
# MAGIC   COALESCE(ci.unit_cost, 0) AS unit_cost,
# MAGIC   ci.stock_status,
# MAGIC   ci.is_out_of_stock,
# MAGIC   ci.is_low_stock,
# MAGIC   ci.existing_reorder_point,
# MAGIC   
# MAGIC   -- Sales velocity
# MAGIC   COALESCE(sv.sales_last_7d, 0) AS sales_last_7d,
# MAGIC   COALESCE(sv.sales_last_14d, 0) AS sales_last_14d,
# MAGIC   COALESCE(sv.sales_last_30d, 0) AS sales_last_30d,
# MAGIC   COALESCE(sv.avg_daily_sales_7d, 0) AS avg_daily_sales_7d,
# MAGIC   COALESCE(sv.avg_daily_sales_14d, 0) AS avg_daily_sales_14d,
# MAGIC   COALESCE(sv.avg_daily_sales_30d, 0) AS avg_daily_sales_30d,
# MAGIC   COALESCE(sv.sales_volatility_30d, 0) AS sales_volatility_30d,
# MAGIC   COALESCE(sv.max_daily_sales_30d, 0) AS max_daily_sales_30d,
# MAGIC   COALESCE(sv.min_daily_sales_30d, 0) AS min_daily_sales_30d,
# MAGIC   COALESCE(sv.days_with_sales_7d, 0) AS days_with_sales_7d,
# MAGIC   COALESCE(sv.days_with_sales_14d, 0) AS days_with_sales_14d,
# MAGIC   COALESCE(sv.days_with_sales_30d, 0) AS days_with_sales_30d,
# MAGIC   COALESCE(sv.revenue_last_7d, 0) AS revenue_last_7d,
# MAGIC   COALESCE(sv.revenue_last_30d, 0) AS revenue_last_30d,
# MAGIC   COALESCE(sv.order_count_30d, 0) AS order_count_30d,
# MAGIC   
# MAGIC   -- Purchase patterns
# MAGIC   COALESCE(pp.avg_lead_time_days, 4) AS supplier_lead_time_days,  -- Default 4 days if no history
# MAGIC   COALESCE(pp.min_lead_time_days, 3) AS min_lead_time_days,
# MAGIC   COALESCE(pp.max_lead_time_days, 7) AS max_lead_time_days,
# MAGIC   COALESCE(pp.avg_days_between_orders, 15) AS avg_days_between_orders,
# MAGIC   COALESCE(pp.avg_order_quantity, 100) AS avg_order_quantity,
# MAGIC   COALESCE(pp.min_order_quantity, 50) AS min_order_quantity,
# MAGIC   COALESCE(pp.avg_unit_cost, ci.unit_cost, 0) AS avg_unit_cost,
# MAGIC   COALESCE(pp.purchase_count_30d, 0) AS purchase_count_30d,
# MAGIC   COALESCE(pp.total_purchased_30d, 0) AS total_purchased_30d,
# MAGIC   pp.last_purchase_date,
# MAGIC   COALESCE(pp.days_since_last_purchase, 999) AS days_since_last_purchase,
# MAGIC   COALESCE(pp.supplier_reliability_pct, 90.0) AS supplier_reliability_pct
# MAGIC   
# MAGIC FROM current_inventory ci
# MAGIC LEFT JOIN sales_velocity sv
# MAGIC   ON ci.ProductSK = sv.ProductSK
# MAGIC   AND ci.WarehouseSK = sv.WarehouseSK
# MAGIC LEFT JOIN purchase_patterns pp
# MAGIC   ON ci.ProductSK = pp.ProductSK
# MAGIC INNER JOIN sap_dev.silver.dim_product dp
# MAGIC   ON ci.ProductSK = dp.ProductSK
# MAGIC INNER JOIN sap_dev.silver.dim_warehouse dw
# MAGIC   ON ci.WarehouseSK = dw.WareHouseSK;
# MAGIC
# MAGIC -- Quick verification
# MAGIC SELECT 
# MAGIC   '✅ Features enriched with dimensions' AS status,
# MAGIC   COUNT(*) AS total_records,
# MAGIC   COUNT(DISTINCT product_sk) AS unique_products,
# MAGIC   COUNT(DISTINCT warehouse_sk) AS unique_warehouses
# MAGIC FROM enriched_features;

# COMMAND ----------

# DBTITLE 1,Step 5: Create Final Feature Table with Calculated Metrics
# MAGIC %sql
# MAGIC -- Step 5: Create final feature table with all calculated metrics
# MAGIC -- This is the main output used by the prediction engine
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW sales_velocity_features AS
# MAGIC SELECT
# MAGIC   -- Keys
# MAGIC   product_sk,
# MAGIC   warehouse_sk,
# MAGIC   analysis_date,
# MAGIC   
# MAGIC   -- Product & Warehouse Details
# MAGIC   product_name,
# MAGIC   product_category,
# MAGIC   brand,
# MAGIC   product_size,
# MAGIC   product_line,
# MAGIC   warehouse_name,
# MAGIC   warehouse_state,
# MAGIC   warehouse_city,
# MAGIC   warehouse_region,
# MAGIC   
# MAGIC   -- Current Inventory State
# MAGIC   current_stock,
# MAGIC   allocated_stock,
# MAGIC   available_stock,
# MAGIC   stock_value,
# MAGIC   unit_cost,
# MAGIC   stock_status,
# MAGIC   is_out_of_stock,
# MAGIC   is_low_stock,
# MAGIC   
# MAGIC   -- Sales Velocity
# MAGIC   sales_last_7d,
# MAGIC   sales_last_14d,
# MAGIC   sales_last_30d,
# MAGIC   avg_daily_sales_7d,
# MAGIC   avg_daily_sales_14d,
# MAGIC   avg_daily_sales_30d,
# MAGIC   
# MAGIC   -- Choose the best sales velocity metric (prioritize recent data)
# MAGIC   CASE
# MAGIC     WHEN days_with_sales_7d >= 4 THEN avg_daily_sales_7d  -- If good 7-day data
# MAGIC     WHEN days_with_sales_14d >= 7 THEN avg_daily_sales_14d  -- Else use 14-day
# MAGIC     ELSE avg_daily_sales_30d  -- Fallback to 30-day
# MAGIC   END AS best_avg_daily_sales,
# MAGIC   
# MAGIC   sales_volatility_30d,
# MAGIC   max_daily_sales_30d,
# MAGIC   min_daily_sales_30d,
# MAGIC   days_with_sales_7d,
# MAGIC   days_with_sales_14d,
# MAGIC   days_with_sales_30d,
# MAGIC   revenue_last_7d,
# MAGIC   revenue_last_30d,
# MAGIC   order_count_30d,
# MAGIC   
# MAGIC   -- Days of Inventory (DOI)
# MAGIC   CASE
# MAGIC     WHEN avg_daily_sales_30d > 0 THEN ROUND(current_stock / avg_daily_sales_30d, 2)
# MAGIC     ELSE 999.99  -- Infinite (no sales)
# MAGIC   END AS days_of_inventory,
# MAGIC   
# MAGIC   -- Inventory Turnover (30-day)
# MAGIC   CASE
# MAGIC     WHEN current_stock > 0 THEN ROUND((sales_last_30d / current_stock) * 365.0 / 30.0, 2)
# MAGIC     ELSE 0
# MAGIC   END AS inventory_turnover_annual,
# MAGIC   
# MAGIC   -- Purchase Patterns
# MAGIC   supplier_lead_time_days,
# MAGIC   min_lead_time_days,
# MAGIC   max_lead_time_days,
# MAGIC   avg_days_between_orders,
# MAGIC   avg_order_quantity,
# MAGIC   min_order_quantity,
# MAGIC   avg_unit_cost,
# MAGIC   purchase_count_30d,
# MAGIC   total_purchased_30d,
# MAGIC   last_purchase_date,
# MAGIC   days_since_last_purchase,
# MAGIC   supplier_reliability_pct,
# MAGIC   
# MAGIC   -- Calculated Reorder Metrics (formulas for prediction engine)
# MAGIC   -- Safety Stock = (Max Sales - Avg Sales) × Lead Time × 1.28 (90% service level)
# MAGIC   ROUND(
# MAGIC     (max_daily_sales_30d - avg_daily_sales_30d) * supplier_lead_time_days * 1.28,
# MAGIC     0
# MAGIC   ) AS calculated_safety_stock,
# MAGIC   
# MAGIC   -- Reorder Point = (Avg Daily Sales × Lead Time) + Safety Stock
# MAGIC   ROUND(
# MAGIC     (avg_daily_sales_30d * supplier_lead_time_days) + 
# MAGIC     ((max_daily_sales_30d - avg_daily_sales_30d) * supplier_lead_time_days * 1.28),
# MAGIC     0
# MAGIC   ) AS calculated_reorder_point,
# MAGIC   
# MAGIC   -- Days Until Stockout = Current Stock ÷ Avg Daily Sales
# MAGIC   CASE
# MAGIC     WHEN avg_daily_sales_30d > 0 THEN ROUND(current_stock / avg_daily_sales_30d, 1)
# MAGIC     ELSE 999.9  -- Infinite (no sales)
# MAGIC   END AS days_until_stockout,
# MAGIC   
# MAGIC   -- Predicted Stockout Date
# MAGIC   CASE
# MAGIC     WHEN avg_daily_sales_30d > 0 THEN DATE_ADD(CURRENT_DATE(), CAST(current_stock / avg_daily_sales_30d AS INT))
# MAGIC     ELSE NULL  -- No stockout expected
# MAGIC   END AS predicted_stockout_date,
# MAGIC   
# MAGIC   -- Risk Level Classification
# MAGIC   CASE
# MAGIC     WHEN current_stock = 0 THEN 'Out of Stock'
# MAGIC     WHEN avg_daily_sales_30d > 0 AND (current_stock / avg_daily_sales_30d) < 1 THEN 'Critical'  -- <1 day
# MAGIC     WHEN avg_daily_sales_30d > 0 AND (current_stock / avg_daily_sales_30d) < 3 THEN 'High'  -- <3 days
# MAGIC     WHEN avg_daily_sales_30d > 0 AND (current_stock / avg_daily_sales_30d) < 7 THEN 'Medium'  -- <7 days
# MAGIC     WHEN avg_daily_sales_30d > 0 AND (current_stock / avg_daily_sales_30d) < 14 THEN 'Low'  -- <14 days
# MAGIC     WHEN avg_daily_sales_30d > 0 AND (current_stock / avg_daily_sales_30d) >= 60 THEN 'Excess'  -- >60 days
# MAGIC     WHEN avg_daily_sales_30d = 0 AND days_with_sales_14d = 0 THEN 'Dead Stock'  -- No sales 14+ days
# MAGIC     ELSE 'Healthy'
# MAGIC   END AS stockout_risk_level,
# MAGIC   
# MAGIC   -- Dead Stock Flag
# MAGIC   CASE
# MAGIC     WHEN days_with_sales_14d = 0 AND current_stock > 0 THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END AS is_dead_stock,
# MAGIC   
# MAGIC   -- Excess Inventory Flag
# MAGIC   CASE
# MAGIC     WHEN avg_daily_sales_30d > 0 AND (current_stock / avg_daily_sales_30d) > 60 THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END AS is_excess_inventory,
# MAGIC   
# MAGIC   -- Recommended Order Quantity (Economic Order Quantity simplified)
# MAGIC   -- Order enough for 15 days at current velocity, rounded to nearest 50
# MAGIC   CASE
# MAGIC     WHEN avg_daily_sales_30d > 0 THEN ROUND(avg_daily_sales_30d * 15 / 50.0, 0) * 50
# MAGIC     ELSE min_order_quantity
# MAGIC   END AS recommended_order_qty,
# MAGIC   
# MAGIC   -- Estimated Order Cost
# MAGIC   CASE
# MAGIC     WHEN avg_daily_sales_30d > 0 THEN ROUND((avg_daily_sales_30d * 15) * avg_unit_cost, 2)
# MAGIC     ELSE ROUND(min_order_quantity * avg_unit_cost, 2)
# MAGIC   END AS estimated_order_cost
# MAGIC   
# MAGIC FROM enriched_features
# MAGIC WHERE product_name IS NOT NULL  -- Valid products only
# MAGIC   AND warehouse_name IS NOT NULL;  -- Valid warehouses only
# MAGIC
# MAGIC -- Quick verification
# MAGIC SELECT 
# MAGIC   '✅ Final sales_velocity_features created' AS status,
# MAGIC   COUNT(*) AS total_records,
# MAGIC   SUM(CASE WHEN stockout_risk_level = 'Critical' THEN 1 ELSE 0 END) AS critical_count,
# MAGIC   SUM(CASE WHEN stockout_risk_level = 'High' THEN 1 ELSE 0 END) AS high_count,
# MAGIC   SUM(CASE WHEN is_dead_stock THEN 1 ELSE 0 END) AS dead_stock_count,
# MAGIC   SUM(CASE WHEN is_excess_inventory THEN 1 ELSE 0 END) AS excess_inventory_count
# MAGIC FROM sales_velocity_features;

# COMMAND ----------

# DBTITLE 1,Sample Output - Preview Features
# MAGIC %sql
# MAGIC -- Preview the final features (top critical items)
# MAGIC SELECT
# MAGIC   product_name,
# MAGIC   warehouse_name,
# MAGIC   current_stock,
# MAGIC   avg_daily_sales_30d,
# MAGIC   days_until_stockout,
# MAGIC   predicted_stockout_date,
# MAGIC   stockout_risk_level,
# MAGIC   calculated_safety_stock,
# MAGIC   calculated_reorder_point,
# MAGIC   recommended_order_qty,
# MAGIC   is_dead_stock,
# MAGIC   is_excess_inventory
# MAGIC FROM sales_velocity_features
# MAGIC WHERE stockout_risk_level IN ('Critical', 'High', 'Out of Stock')
# MAGIC ORDER BY 
# MAGIC   CASE stockout_risk_level
# MAGIC     WHEN 'Out of Stock' THEN 1
# MAGIC     WHEN 'Critical' THEN 2
# MAGIC     WHEN 'High' THEN 3
# MAGIC   END,
# MAGIC   days_until_stockout
# MAGIC LIMIT 20;