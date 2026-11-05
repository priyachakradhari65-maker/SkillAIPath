# Databricks notebook source
# DBTITLE 1,Business Intelligence UC Functions for Genie
# MAGIC %md
# MAGIC # Business Intelligence - UC Functions for Genie
# MAGIC
# MAGIC ## Overview
# MAGIC Unity Catalog SQL functions attached to the Business Intelligence Genie Space as trusted assets.
# MAGIC
# MAGIC ## Functions
# MAGIC 1. **get_rfm_segment_summary()** - RFM segment distribution with counts and metrics
# MAGIC 2. **get_churn_risk_customers(risk_segment)** - Churn predictions filtered by risk level
# MAGIC 3. **get_top_products(metric, limit_count)** - Top products by revenue/quantity/orders
# MAGIC 4. **get_demand_forecast(product_name)** - Demand forecast for specific products
# MAGIC 5. **get_customer_360_view(customer_name)** - 360-degree customer profile with RFM + churn
# MAGIC 6. **get_revenue_summary()** - Revenue and business KPI summary
# MAGIC 7. **get_product_recommendations_for_customer(customer_id)** - Top product recommendations
# MAGIC 8. **get_inventory_health_summary()** - Inventory health by warehouse (stock status, health, turnover)
# MAGIC 9. **get_sales_profitability_summary(category)** - Revenue, cost, profit, margin (overall or by category)
# MAGIC 10. **get_category_performance()** - Category-level revenue, cost, profit, margin comparison
# MAGIC 11. **get_next_purchase_predictions(customer_id)** - Next purchase predictions for a customer
# MAGIC
# MAGIC **Schema:** sap_main.gold  
# MAGIC **Usage:** These functions are attached to the BI Genie Space as trusted SQL functions.

# COMMAND ----------

# DBTITLE 1,Function 1: get_rfm_segment_summary
# MAGIC %sql
# MAGIC -- 1. RFM Segment Summary
# MAGIC CREATE OR REPLACE FUNCTION sap_main.gold.get_rfm_segment_summary()
# MAGIC RETURNS TABLE(
# MAGIC   segment STRING,
# MAGIC   customer_count INT,
# MAGIC   avg_recency INT,
# MAGIC   avg_frequency DECIMAL(10,2),
# MAGIC   avg_monetary_lakhs DECIMAL(10,2)
# MAGIC )
# MAGIC RETURN
# MAGIC   SELECT 
# MAGIC     Segment as segment,
# MAGIC     COUNT(*) as customer_count,
# MAGIC     CAST(AVG(Recency) AS INT) as avg_recency,
# MAGIC     ROUND(AVG(Frequency), 2) as avg_frequency,
# MAGIC     ROUND(AVG(Monetary_Lakhs), 2) as avg_monetary_lakhs
# MAGIC   FROM sap_main.gold.mart_rfm_analysis
# MAGIC   GROUP BY Segment
# MAGIC   ORDER BY customer_count DESC;

# COMMAND ----------

# DBTITLE 1,Function 2: get_churn_risk_customers
# MAGIC %sql
# MAGIC -- 2. Churn Risk Customers by Risk Segment
# MAGIC CREATE OR REPLACE FUNCTION sap_main.gold.get_churn_risk_customers(risk_segment STRING DEFAULT NULL)
# MAGIC RETURNS TABLE(
# MAGIC   customer_id INT,
# MAGIC   recency_days INT,
# MAGIC   total_purchases BIGINT,
# MAGIC   lifetime_value_lakhs DECIMAL(10,9),
# MAGIC   churn_probability FLOAT,
# MAGIC   is_churned_predicted BIGINT,
# MAGIC   risk_segment STRING
# MAGIC )
# MAGIC RETURN
# MAGIC   SELECT 
# MAGIC     customer_id,
# MAGIC     recency_days,
# MAGIC     total_purchases,
# MAGIC     lifetime_value_lakhs,
# MAGIC     ROUND(churn_probability, 4) as churn_probability,
# MAGIC     is_churned_predicted,
# MAGIC     risk_segment
# MAGIC   FROM sap_main.gold.churn_predictions
# MAGIC   WHERE prediction_date = CURRENT_DATE()
# MAGIC     AND (get_churn_risk_customers.risk_segment IS NULL OR LOWER(risk_segment) = LOWER(get_churn_risk_customers.risk_segment))
# MAGIC   ORDER BY churn_probability DESC
# MAGIC   LIMIT 100;

# COMMAND ----------

# DBTITLE 1,Function 3: get_top_products
# MAGIC %sql
# MAGIC -- 3. Top Products by Metric (revenue, quantity, or orders)
# MAGIC CREATE OR REPLACE FUNCTION sap_main.gold.get_top_products(metric STRING DEFAULT 'revenue', limit_count INT DEFAULT 10)
# MAGIC RETURNS TABLE(
# MAGIC   product_name STRING,
# MAGIC   product_category STRING,
# MAGIC   total_orders BIGINT,
# MAGIC   total_quantity_sold BIGINT,
# MAGIC   total_revenue DECIMAL(21,2),
# MAGIC   avg_sale_value DECIMAL(11,2),
# MAGIC   performance_category STRING
# MAGIC )
# MAGIC RETURN
# MAGIC   WITH ranked AS (
# MAGIC     SELECT 
# MAGIC       ProductName,
# MAGIC       ProductCategory,
# MAGIC       TotalOrders,
# MAGIC       TotalQuantitySold,
# MAGIC       TotalRevenue,
# MAGIC       AvgSaleValue,
# MAGIC       PerformanceCategory,
# MAGIC       ROW_NUMBER() OVER (
# MAGIC         ORDER BY 
# MAGIC           CASE 
# MAGIC             WHEN LOWER(metric) = 'revenue' THEN CAST(TotalRevenue AS DOUBLE)
# MAGIC             WHEN LOWER(metric) = 'quantity' THEN CAST(TotalQuantitySold AS DOUBLE)
# MAGIC             WHEN LOWER(metric) = 'orders' THEN CAST(TotalOrders AS DOUBLE)
# MAGIC             ELSE CAST(TotalRevenue AS DOUBLE)
# MAGIC           END DESC
# MAGIC       ) as rn
# MAGIC     FROM sap_main.gold.mart_product_performance
# MAGIC   )
# MAGIC   SELECT 
# MAGIC     ProductName as product_name,
# MAGIC     ProductCategory as product_category,
# MAGIC     TotalOrders as total_orders,
# MAGIC     TotalQuantitySold as total_quantity_sold,
# MAGIC     TotalRevenue as total_revenue,
# MAGIC     AvgSaleValue as avg_sale_value,
# MAGIC     PerformanceCategory as performance_category
# MAGIC   FROM ranked
# MAGIC   WHERE rn <= limit_count;

# COMMAND ----------

# DBTITLE 1,Function 4: get_demand_forecast
# MAGIC %sql
# MAGIC -- 4. Demand Forecast for Products
# MAGIC CREATE OR REPLACE FUNCTION sap_main.gold.get_demand_forecast(product_name STRING DEFAULT NULL)
# MAGIC RETURNS TABLE(
# MAGIC   product_id BIGINT,
# MAGIC   product_name STRING,
# MAGIC   category STRING,
# MAGIC   forecast_date DATE,
# MAGIC   predicted_demand DOUBLE,
# MAGIC   lower_bound DOUBLE,
# MAGIC   upper_bound DOUBLE,
# MAGIC   model_type STRING,
# MAGIC   forecast_horizon_days BIGINT
# MAGIC )
# MAGIC RETURN
# MAGIC   SELECT 
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     category,
# MAGIC     forecast_date,
# MAGIC     ROUND(predicted_demand, 2) as predicted_demand,
# MAGIC     ROUND(lower_bound, 2) as lower_bound,
# MAGIC     ROUND(upper_bound, 2) as upper_bound,
# MAGIC     model_type,
# MAGIC     forecast_horizon_days
# MAGIC   FROM sap_main.gold.demand_forecast
# MAGIC   WHERE prediction_date = CURRENT_DATE()
# MAGIC     AND (get_demand_forecast.product_name IS NULL OR LOWER(product_name) LIKE LOWER(CONCAT('%', get_demand_forecast.product_name, '%')))
# MAGIC   ORDER BY forecast_date ASC, predicted_demand DESC
# MAGIC   LIMIT 50;

# COMMAND ----------

# DBTITLE 1,Function 5: get_customer_360_view
# MAGIC %sql
# MAGIC -- 5. Customer 360 View (joins mart_customer_360 + mart_rfm_analysis + churn_predictions)
# MAGIC CREATE OR REPLACE FUNCTION sap_main.gold.get_customer_360_view(customer_name STRING)
# MAGIC RETURNS TABLE(
# MAGIC   customer_key INT,
# MAGIC   customer_id INT,
# MAGIC   customer_name STRING,
# MAGIC   email STRING,
# MAGIC   total_purchases BIGINT,
# MAGIC   total_spend DECIMAL(21,2),
# MAGIC   avg_order_value DECIMAL(13,2),
# MAGIC   first_purchase_date DATE,
# MAGIC   last_purchase_date DATE,
# MAGIC   days_since_last_purchase INT,
# MAGIC   customer_segment STRING,
# MAGIC   activity_status STRING,
# MAGIC   rfm_segment STRING,
# MAGIC   rfm_score STRING,
# MAGIC   churn_probability FLOAT,
# MAGIC   churn_risk_segment STRING
# MAGIC )
# MAGIC RETURN
# MAGIC   SELECT 
# MAGIC     c.CustomerKey as customer_key,
# MAGIC     c.CustomerID as customer_id,
# MAGIC     c.CustomerName as customer_name,
# MAGIC     c.Email as email,
# MAGIC     c.TotalPurchases as total_purchases,
# MAGIC     c.TotalSpend as total_spend,
# MAGIC     c.AvgOrderValue as avg_order_value,
# MAGIC     c.FirstPurchaseDate as first_purchase_date,
# MAGIC     c.LastPurchaseDate as last_purchase_date,
# MAGIC     c.DaysSinceLastPurchase as days_since_last_purchase,
# MAGIC     c.CustomerSegment as customer_segment,
# MAGIC     c.ActivityStatus as activity_status,
# MAGIC     r.Segment as rfm_segment,
# MAGIC     r.RFM_Score as rfm_score,
# MAGIC     ROUND(ch.churn_probability, 4) as churn_probability,
# MAGIC     ch.risk_segment as churn_risk_segment
# MAGIC   FROM sap_main.gold.mart_customer_360 c
# MAGIC   LEFT JOIN sap_main.gold.mart_rfm_analysis r ON c.CustomerKey = r.CustomerSK
# MAGIC   LEFT JOIN sap_main.gold.churn_predictions ch ON c.CustomerKey = ch.customer_id AND ch.prediction_date = CURRENT_DATE()
# MAGIC   WHERE LOWER(c.CustomerName) LIKE LOWER(CONCAT('%', get_customer_360_view.customer_name, '%'))
# MAGIC   ORDER BY c.TotalSpend DESC
# MAGIC   LIMIT 20;

# COMMAND ----------

# DBTITLE 1,Function 6: get_revenue_summary
# MAGIC %sql
# MAGIC -- 6. Revenue and Business KPI Summary
# MAGIC CREATE OR REPLACE FUNCTION sap_main.gold.get_revenue_summary()
# MAGIC RETURNS TABLE(
# MAGIC   total_customers INT,
# MAGIC   active_customers INT,
# MAGIC   total_revenue DECIMAL(21,2),
# MAGIC   avg_customer_spend DECIMAL(21,2),
# MAGIC   avg_order_value DECIMAL(13,2),
# MAGIC   total_products INT,
# MAGIC   total_orders BIGINT,
# MAGIC   total_quantity_sold BIGINT,
# MAGIC   best_seller_count INT,
# MAGIC   popular_count INT,
# MAGIC   churned_customers INT,
# MAGIC   high_risk_customers INT
# MAGIC )
# MAGIC RETURN
# MAGIC   SELECT 
# MAGIC     (SELECT COUNT(*) FROM sap_main.gold.mart_customer_360) as total_customers,
# MAGIC     (SELECT COUNT(*) FROM sap_main.gold.mart_customer_360 WHERE ActivityStatus = 'Active') as active_customers,
# MAGIC     (SELECT COALESCE(SUM(TotalSpend), 0) FROM sap_main.gold.mart_customer_360) as total_revenue,
# MAGIC     (SELECT COALESCE(AVG(TotalSpend), 0) FROM sap_main.gold.mart_customer_360) as avg_customer_spend,
# MAGIC     (SELECT COALESCE(AVG(AvgOrderValue), 0) FROM sap_main.gold.mart_customer_360) as avg_order_value,
# MAGIC     (SELECT COUNT(*) FROM sap_main.gold.mart_product_performance) as total_products,
# MAGIC     (SELECT COALESCE(SUM(TotalOrders), 0) FROM sap_main.gold.mart_product_performance) as total_orders,
# MAGIC     (SELECT COALESCE(SUM(TotalQuantitySold), 0) FROM sap_main.gold.mart_product_performance) as total_quantity_sold,
# MAGIC     (SELECT COUNT(*) FROM sap_main.gold.mart_product_performance WHERE PerformanceCategory = 'Best Seller') as best_seller_count,
# MAGIC     (SELECT COUNT(*) FROM sap_main.gold.mart_product_performance WHERE PerformanceCategory = 'Popular') as popular_count,
# MAGIC     (SELECT COUNT(*) FROM sap_main.gold.churn_predictions WHERE prediction_date = CURRENT_DATE() AND is_churned_predicted = 1) as churned_customers,
# MAGIC     (SELECT COUNT(*) FROM sap_main.gold.churn_predictions WHERE prediction_date = CURRENT_DATE() AND risk_segment = 'High Risk') as high_risk_customers;

# COMMAND ----------

# DBTITLE 1,Function 7: get_product_recommendations_for_customer
# MAGIC %sql
# MAGIC -- 7. Product Recommendations for a Customer
# MAGIC CREATE OR REPLACE FUNCTION sap_main.gold.get_product_recommendations_for_customer(customer_id INT)
# MAGIC RETURNS TABLE(
# MAGIC   customer_id INT,
# MAGIC   recommended_product_name STRING,
# MAGIC   recommendation_score DOUBLE,
# MAGIC   recommendation_rank BIGINT
# MAGIC )
# MAGIC RETURN
# MAGIC   SELECT 
# MAGIC     customer_id,
# MAGIC     recommended_product_name,
# MAGIC     ROUND(recommendation_score, 4) as recommendation_score,
# MAGIC     recommendation_rank
# MAGIC   FROM sap_main.gold.product_recommendations
# MAGIC   WHERE prediction_date = CURRENT_DATE()
# MAGIC     AND customer_id = get_product_recommendations_for_customer.customer_id
# MAGIC   ORDER BY recommendation_rank ASC
# MAGIC   LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Function 8: get_inventory_health_summary
# MAGIC %sql
# MAGIC -- 8. Inventory Health Summary (warehouse-level + overall)
# MAGIC CREATE OR REPLACE FUNCTION sap_main.gold.get_inventory_health_summary()
# MAGIC RETURNS TABLE(
# MAGIC   warehouse_name STRING,
# MAGIC   total_products INT,
# MAGIC   in_stock_count INT,
# MAGIC   low_stock_count INT,
# MAGIC   out_of_stock_count INT,
# MAGIC   healthy_count INT,
# MAGIC   understock_count INT,
# MAGIC   avg_current_stock DECIMAL(15,2),
# MAGIC   avg_days_in_stock DECIMAL(10,2),
# MAGIC   avg_turnover_ratio DECIMAL(10,2),
# MAGIC   total_stock_value DECIMAL(18,2)
# MAGIC )
# MAGIC RETURN
# MAGIC   SELECT 
# MAGIC     COALESCE(WarehouseName, 'ALL') as warehouse_name,
# MAGIC     COUNT(*) as total_products,
# MAGIC     SUM(CASE WHEN StockStatus = 'In Stock' THEN 1 ELSE 0 END) as in_stock_count,
# MAGIC     SUM(CASE WHEN StockStatus = 'Low Stock' THEN 1 ELSE 0 END) as low_stock_count,
# MAGIC     SUM(CASE WHEN StockStatus = 'Out of Stock' THEN 1 ELSE 0 END) as out_of_stock_count,
# MAGIC     SUM(CASE WHEN HealthStatus = 'Healthy' THEN 1 ELSE 0 END) as healthy_count,
# MAGIC     SUM(CASE WHEN HealthStatus = 'Understock' THEN 1 ELSE 0 END) as understock_count,
# MAGIC     ROUND(AVG(CurrentStock), 2) as avg_current_stock,
# MAGIC     ROUND(AVG(DaysInStock), 2) as avg_days_in_stock,
# MAGIC     ROUND(AVG(TurnoverRatio), 2) as avg_turnover_ratio,
# MAGIC     ROUND(SUM(CurrentStockValue), 2) as total_stock_value
# MAGIC   FROM sap_main.gold.mart_inventory_health
# MAGIC   GROUP BY GROUPING SETS ((WarehouseName), ())
# MAGIC   ORDER BY 
# MAGIC     CASE WHEN COALESCE(WarehouseName, 'ALL') = 'ALL' THEN 0 ELSE 1 END,
# MAGIC     WarehouseName;

# COMMAND ----------

# DBTITLE 1,Function 9: get_sales_profitability_summary
# MAGIC %sql
# MAGIC -- 9. Sales Profitability Summary (overall or by category)
# MAGIC CREATE OR REPLACE FUNCTION sap_main.gold.get_sales_profitability_summary(category STRING DEFAULT NULL)
# MAGIC RETURNS TABLE(
# MAGIC   category STRING,
# MAGIC   total_orders BIGINT,
# MAGIC   total_quantity_sold BIGINT,
# MAGIC   total_revenue DECIMAL(21,2),
# MAGIC   total_cost DECIMAL(21,2),
# MAGIC   gross_profit DECIMAL(21,2),
# MAGIC   profit_margin_pct DECIMAL(10,2),
# MAGIC   total_discount DECIMAL(21,2)
# MAGIC )
# MAGIC RETURN
# MAGIC   SELECT 
# MAGIC     COALESCE(pc.ProductCategory, 'ALL') as category,
# MAGIC     COUNT(*) as total_orders,
# MAGIC     SUM(fs.ProductQuantity) as total_quantity_sold,
# MAGIC     ROUND(SUM(fs.ProductQuantity * fs.sellingPrice), 2) as total_revenue,
# MAGIC     ROUND(SUM(fs.ProductQuantity * fs.CostPrice), 2) as total_cost,
# MAGIC     ROUND(SUM(fs.ProductQuantity * (fs.sellingPrice - fs.CostPrice)), 2) as gross_profit,
# MAGIC     ROUND(
# MAGIC       SUM(fs.ProductQuantity * (fs.sellingPrice - fs.CostPrice)) / 
# MAGIC       NULLIF(SUM(fs.ProductQuantity * fs.sellingPrice), 0) * 100, 2
# MAGIC     ) as profit_margin_pct,
# MAGIC     ROUND(SUM(fs.discount), 2) as total_discount
# MAGIC   FROM sap_main.silver.fact_sales fs
# MAGIC   INNER JOIN sap_main.silver.dim_product_category pc ON fs.ProductCategorySK = pc.ProductCategorySK
# MAGIC   WHERE get_sales_profitability_summary.category IS NULL 
# MAGIC     OR LOWER(pc.ProductCategory) = LOWER(get_sales_profitability_summary.category)
# MAGIC   GROUP BY pc.ProductCategory
# MAGIC   ORDER BY total_revenue DESC;

# COMMAND ----------

# DBTITLE 1,Function 10: get_category_performance
# MAGIC %sql
# MAGIC -- 10. Category Performance (all categories for comparison)
# MAGIC CREATE OR REPLACE FUNCTION sap_main.gold.get_category_performance()
# MAGIC RETURNS TABLE(
# MAGIC   category STRING,
# MAGIC   total_orders BIGINT,
# MAGIC   total_quantity_sold BIGINT,
# MAGIC   total_revenue DECIMAL(21,2),
# MAGIC   total_cost DECIMAL(21,2),
# MAGIC   gross_profit DECIMAL(21,2),
# MAGIC   profit_margin_pct DECIMAL(10,2),
# MAGIC   avg_order_value DECIMAL(13,2)
# MAGIC )
# MAGIC RETURN
# MAGIC   SELECT 
# MAGIC     pc.ProductCategory as category,
# MAGIC     COUNT(*) as total_orders,
# MAGIC     SUM(fs.ProductQuantity) as total_quantity_sold,
# MAGIC     ROUND(SUM(fs.ProductQuantity * fs.sellingPrice), 2) as total_revenue,
# MAGIC     ROUND(SUM(fs.ProductQuantity * fs.CostPrice), 2) as total_cost,
# MAGIC     ROUND(SUM(fs.ProductQuantity * (fs.sellingPrice - fs.CostPrice)), 2) as gross_profit,
# MAGIC     ROUND(
# MAGIC       SUM(fs.ProductQuantity * (fs.sellingPrice - fs.CostPrice)) / 
# MAGIC       NULLIF(SUM(fs.ProductQuantity * fs.sellingPrice), 0) * 100, 2
# MAGIC     ) as profit_margin_pct,
# MAGIC     ROUND(SUM(fs.ProductQuantity * fs.sellingPrice) / NULLIF(COUNT(*), 0), 2) as avg_order_value
# MAGIC   FROM sap_main.silver.fact_sales fs
# MAGIC   INNER JOIN sap_main.silver.dim_product_category pc ON fs.ProductCategorySK = pc.ProductCategorySK
# MAGIC   GROUP BY pc.ProductCategory
# MAGIC   ORDER BY total_revenue DESC;

# COMMAND ----------

# DBTITLE 1,Function 11: get_next_purchase_predictions
# MAGIC %sql
# MAGIC -- 11. Next Purchase Predictions for a Customer
# MAGIC CREATE OR REPLACE FUNCTION sap_main.gold.get_next_purchase_predictions(customer_id INT)
# MAGIC RETURNS TABLE(
# MAGIC   customer_id INT,
# MAGIC   recommended_product_id INT,
# MAGIC   recommended_product_name STRING,
# MAGIC   recommendation_score DOUBLE,
# MAGIC   recommendation_rank BIGINT,
# MAGIC   prediction_date DATE
# MAGIC )
# MAGIC RETURN
# MAGIC   SELECT 
# MAGIC     customer_id,
# MAGIC     recommended_product_id,
# MAGIC     recommended_product_name,
# MAGIC     ROUND(recommendation_score, 6) as recommendation_score,
# MAGIC     recommendation_rank,
# MAGIC     prediction_date
# MAGIC   FROM sap_main.gold.next_purchase_predictions
# MAGIC   WHERE prediction_date = CURRENT_DATE()
# MAGIC     AND customer_id = get_next_purchase_predictions.customer_id
# MAGIC   ORDER BY recommendation_rank ASC
# MAGIC   LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Verify All 11 BI UC Functions
# Verify all 11 BI UC Functions exist and are callable
print("=" * 70)
print("BI UC FUNCTION VERIFICATION (via information_schema.routines)")
print("=" * 70)

target_functions = [
    'get_rfm_segment_summary',
    'get_churn_risk_customers',
    'get_top_products',
    'get_demand_forecast',
    'get_customer_360_view',
    'get_revenue_summary',
    'get_product_recommendations_for_customer',
    'get_inventory_health_summary',
    'get_sales_profitability_summary',
    'get_category_performance',
    'get_next_purchase_predictions'
]

# Check existence via information_schema
routines = spark.sql("""
    SELECT routine_name 
    FROM sap_main.information_schema.routines 
    WHERE routine_schema = 'gold' 
    ORDER BY routine_name
""").collect()
existing = set(r.routine_name for r in routines)

all_found = True
for func_name in target_functions:
    status = '✅' if func_name in existing else '❌'
    print(f"  {status} sap_main.gold.{func_name}")
    if func_name not in existing:
        all_found = False

# Test each function with safe invocations
print("\n" + "=" * 70)
print("FUNCTION INVOCATION TESTS")
print("=" * 70)

tests = [
    ("get_rfm_segment_summary", "SELECT COUNT(*) as cnt FROM sap_main.gold.get_rfm_segment_summary()"),
    ("get_churn_risk_customers", "SELECT COUNT(*) as cnt FROM sap_main.gold.get_churn_risk_customers('High Risk')"),
    ("get_top_products", "SELECT COUNT(*) as cnt FROM sap_main.gold.get_top_products('revenue', 10)"),
    ("get_demand_forecast", "SELECT COUNT(*) as cnt FROM sap_main.gold.get_demand_forecast('LG')"),
    ("get_customer_360_view", "SELECT COUNT(*) as cnt FROM sap_main.gold.get_customer_360_view('Aachal')"),
    ("get_revenue_summary", "SELECT COUNT(*) as cnt FROM sap_main.gold.get_revenue_summary()"),
    ("get_product_recommendations_for_customer", "SELECT COUNT(*) as cnt FROM sap_main.gold.get_product_recommendations_for_customer(1)"),
    ("get_inventory_health_summary", "SELECT COUNT(*) as cnt FROM sap_main.gold.get_inventory_health_summary()"),
    ("get_sales_profitability_summary", "SELECT COUNT(*) as cnt FROM sap_main.gold.get_sales_profitability_summary()"),
    ("get_sales_profitability_summary_filtered", "SELECT COUNT(*) as cnt FROM sap_main.gold.get_sales_profitability_summary('Electronics')"),
    ("get_category_performance", "SELECT COUNT(*) as cnt FROM sap_main.gold.get_category_performance()"),
    ("get_next_purchase_predictions", "SELECT COUNT(*) as cnt FROM sap_main.gold.get_next_purchase_predictions(1)"),
]

all_pass = True
for func_name, test_sql in tests:
    try:
        result = spark.sql(test_sql).collect()[0]['cnt']
        print(f"  ✅ {func_name}() -> {result} rows")
    except Exception as e:
        print(f"  ❌ {func_name}() -> Error: {str(e)[:120]}")
        all_pass = False

print("\n" + "=" * 70)
if all_found and all_pass:
    print("✅ ALL 11 BI UC FUNCTIONS VERIFIED AND CALLABLE")
elif all_found:
    print("⚠️  All 11 functions exist but some invocations failed")
else:
    print("❌ SOME FUNCTIONS MISSING - Check errors above")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,BI Agent Tools - Complete
# MAGIC %md
# MAGIC # ✅ Business Intelligence Agent Tools - Status
# MAGIC
# MAGIC ## UC Functions Created (sap_main.gold)
# MAGIC | # | Function | Parameters | Purpose |
# MAGIC |---|---------|-----------|---------|
# MAGIC | 1 | `get_rfm_segment_summary()` | none | RFM segment distribution |
# MAGIC | 2 | `get_churn_risk_customers(risk_segment)` | risk_segment (optional) | Churn predictions by risk |
# MAGIC | 3 | `get_top_products(metric, limit_count)` | metric, limit_count | Top products by metric |
# MAGIC | 4 | `get_demand_forecast(product_name)` | product_name (optional) | Demand forecast lookup |
# MAGIC | 5 | `get_customer_360_view(customer_name)` | customer_name | Customer 360 profile |
# MAGIC | 6 | `get_revenue_summary()` | none | Revenue KPI summary |
# MAGIC | 7 | `get_product_recommendations_for_customer(customer_id)` | customer_id | Product recommendations |
# MAGIC
# MAGIC ## Data Sources
# MAGIC * `sap_main.gold.mart_customer_360` - 6,373 customers
# MAGIC * `sap_main.gold.mart_rfm_analysis` - 6,373 RFM segments
# MAGIC * `sap_main.gold.mart_product_performance` - 78 products
# MAGIC * `sap_main.gold.mart_inventory_health` - 1,560 inventory records
# MAGIC * `sap_main.gold.churn_predictions` - 6,373 churn predictions
# MAGIC * `sap_main.gold.demand_forecast` - 150 forecast records
# MAGIC * `sap_main.gold.product_recommendations` - 63,730 recommendations
# MAGIC
# MAGIC **No Agent Bricks dependency.** All functions are pure SQL, callable from serverless compute.

# COMMAND ----------

