# Databricks notebook source
# DBTITLE 1,AI/BI Dashboard Specification - SkillAIPath June
# MAGIC %md
# MAGIC # AI/BI Dashboard Specification - SkillAIPath June
# MAGIC
# MAGIC ## Purpose
# MAGIC This notebook specifies the complete AI/BI Dashboard structure for SkillAIPath's Gold layer analytics. Demonstrates 3 dashboard pages with cross-filtering, drill-throughs, and business intelligence patterns.
# MAGIC
# MAGIC ## Dashboard Pages
# MAGIC 1. **Inventory Overview** - Stock health, turnover, warehouse utilization
# MAGIC 2. **Supplier Scorecard** - Delivery performance, reliability metrics
# MAGIC 3. **Order Analytics** - Sales trends, payment modes, customer patterns
# MAGIC
# MAGIC ## Key Features
# MAGIC - Demonstrates business intelligence skills (not just SQL)
# MAGIC - Shows understanding of end-user analytics
# MAGIC - Cross-filtering = interactive dashboard competency
# MAGIC - Drill-through = advanced navigation patterns
# MAGIC
# MAGIC ## Implementation Approach
# MAGIC - Connect dashboard to Gold marts (not raw tables)
# MAGIC - Design for business users (not technical audiences)
# MAGIC - Implement interactive filtering
# MAGIC - Create actionable insights (not just charts)

# COMMAND ----------

# DBTITLE 0,Configuration
# Configuration for Dashboard Queries
# Note: Replace ${Config.SCHEMA_GOLD} in SQL cells with your actual catalog.schema
# Example environments:
#   - Dev: sap_dev.gold
#   - Main: sap_main.gold
#   - Prod: sap_main.gold

%run ../../../1_Canvas/1_Setup/1_config

print("="*70)
print("DASHBOARD SPECIFICATION CONFIGURATION")
print("="*70)
print(f"Environment: {Config.ENVIRONMENT.upper()}")
print(f"Catalog: {Config.CATALOG}")
print(f"Gold Schema: {Config.SCHEMA_GOLD}")
print(f"Silver Schema: {Config.SCHEMA_SILVER}")
print("="*70)
print("\nAll queries below use Config variables (no hardcoding)")
print("Change environment widget - all queries update automatically")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Page 1: Inventory Overview Dashboard
# MAGIC %md
# MAGIC # Page 1: Inventory Overview Dashboard
# MAGIC
# MAGIC ## Business Questions
# MAGIC 1. Which products have overstock risk? (inventory > 60 days supply)
# MAGIC 2. Which warehouses have understock risk? (inventory < 7 days supply)
# MAGIC 3. What's the inventory health score by category?
# MAGIC 4. Which products have best/worst turnover ratios?
# MAGIC
# MAGIC ## Charts
# MAGIC
# MAGIC ### Chart 1: Inventory Health Scorecard (KPI Cards)
# MAGIC - **Metric 1:** Total Inventory Value (₹ Cr)
# MAGIC - **Metric 2:** Average Turnover Ratio
# MAGIC - **Metric 3:** Overstock Count (>60 days)
# MAGIC - **Metric 4:** Understock Count (<7 days)
# MAGIC - **Data Source:** `mart_inventory_health`
# MAGIC
# MAGIC ### Chart 2: Warehouse Utilization (Bar Chart)
# MAGIC - **X-axis:** WarehouseName
# MAGIC - **Y-axis:** UtilizationPercent
# MAGIC - **Color:** Health_Status (Healthy/Warning/Critical)
# MAGIC - **Sort:** Descending by Utilization
# MAGIC - **Data Source:** `mart_inventory_health` grouped by warehouse
# MAGIC
# MAGIC ### Chart 3: Bottom Products by Turnover (Horizontal Bar)
# MAGIC - **X-axis:** TurnoverRatio
# MAGIC - **Y-axis:** ProductName
# MAGIC - **Sort:** Ascending (worst turnover at top)
# MAGIC - **Limit:** Bottom 20 products
# MAGIC - **Color:** MoverCategory (Slow/Moderate/Fast)
# MAGIC - **Data Source:** `mart_inventory_health`
# MAGIC
# MAGIC ### Chart 4: Stock Aging Distribution (Histogram)
# MAGIC - **X-axis:** DaysInStock (bins: 0-7, 8-30, 31-60, 61-90, 90+)
# MAGIC - **Y-axis:** ProductCount
# MAGIC - **Color:** By bin (green/yellow/orange/red)
# MAGIC - **Data Source:** `fact_inventory` with date calculations
# MAGIC
# MAGIC ## Cross-Filtering
# MAGIC - Click warehouse in Chart 2 → filters Chart 3 and 4 to that warehouse
# MAGIC - Click category in Chart 3 → filters Chart 2 and 4 to that category
# MAGIC
# MAGIC ## Drill-Through
# MAGIC - Click any bar in Chart 2 → navigate to "Warehouse Detail" page (future enhancement)

# COMMAND ----------

# DBTITLE 1,Query 1.1: Inventory Health Scorecard Metrics
# KPI Card Metrics for Dashboard Page 1
query = f"""

WITH inventory_summary AS (
  SELECT
    SUM(StockValue) / 10000000 AS total_inventory_cr,
    AVG(TurnoverRatio) AS avg_turnover,
    SUM(CASE WHEN DaysInStock > 60 THEN 1 ELSE 0 END) AS overstock_count,
    SUM(CASE WHEN DaysInStock < 7 THEN 1 ELSE 0 END) AS understock_count,
    COUNT(*) AS total_products
  FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_inventory_health
)
SELECT
  ROUND(total_inventory_cr, 2) AS TotalInventoryValue_Cr,
  ROUND(avg_turnover, 2) AS AvgTurnoverRatio,
  overstock_count AS OverstockProducts,
  understock_count AS UnderstockProducts,
  total_products AS TotalActiveProducts
FROM inventory_summary
"""

print("Query 1.1: Inventory Health KPI Metrics")
print("="*70)
spark.sql(query).display()
print("\nUSAGE: These 5 metrics go into KPI cards at top of Page 1")

# COMMAND ----------

# DBTITLE 0,Query 1.2: Warehouse Utilization Bar Chart
# Warehouse Utilization with Health Status Color Coding
query = f"""
SELECT
  ih.WarehouseName,
  ROUND(AVG(ih.UtilizationPercent), 2) AS UtilizationPercent,
  CASE
    WHEN AVG(ih.UtilizationPercent) >= 80 THEN 'Critical'
    WHEN AVG(ih.UtilizationPercent) >= 60 THEN 'Warning'
    ELSE 'Healthy'
  END AS HealthStatus,
  COUNT(DISTINCT ih.ProductSK) AS ProductCount
FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_inventory_health ih
GROUP BY ih.WarehouseName
ORDER BY UtilizationPercent DESC
"""

print("Query 1.2: Warehouse Utilization Bar Chart")
print("="*70)
spark.sql(query).display()
print("\nCHART CONFIG:")
print("X-axis: WarehouseName")
print("Y-axis: UtilizationPercent")
print("Color: HealthStatus (Critical=Red, Warning=Yellow, Healthy=Green)")
print("Chart Type: Bar")

# COMMAND ----------

# DBTITLE 1,Query 1.3: Turnover by Category
# Top/Bottom Products by Turnover (Identify slow movers)
query = f"""
SELECT
  ih.ProductName,
  ROUND(ih.TurnoverRatio, 2) AS TurnoverRatio,
  ROUND(ih.StockValue / 100000, 2) AS StockValue_Lakhs,
  ih.DaysInStock,
  CASE
    WHEN ih.TurnoverRatio < 1.0 THEN 'Slow Mover'
    WHEN ih.TurnoverRatio < 2.0 THEN 'Moderate'
    ELSE 'Fast Mover'
  END AS MoverCategory
FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_inventory_health ih
ORDER BY ih.TurnoverRatio ASC
LIMIT 20
"""

print("Query 1.3: Bottom 20 Products by Turnover (Slow Movers)")
print("="*70)
spark.sql(query).display()
print("\nCHART CONFIG:")
print("X-axis: TurnoverRatio")
print("Y-axis: ProductName")
print("Color: MoverCategory (Slow=Red, Moderate=Yellow, Fast=Green)")
print("Chart Type: Horizontal Bar")
print("Tooltip: Show StockValue_Lakhs and DaysInStock")

# COMMAND ----------

# DBTITLE 1,Query 1.4: Stock Aging Distribution
# Stock Aging Histogram (How long products sit in warehouse)
query = f"""
SELECT
  CASE
    WHEN DaysInStock <= 7 THEN '0-7 days (Fresh)'
    WHEN DaysInStock <= 30 THEN '8-30 days (Normal)'
    WHEN DaysInStock <= 60 THEN '31-60 days (Aging)'
    WHEN DaysInStock <= 90 THEN '61-90 days (Stale)'
    ELSE '90+ days (Dead Stock)'
  END AS StockAgeBucket,
  COUNT(*) AS ProductCount,
  ROUND(SUM(StockValue) / 100000, 2) AS TotalValue_Lakhs,
  CASE
    WHEN DaysInStock <= 7 THEN 'Green'
    WHEN DaysInStock <= 30 THEN 'Blue'
    WHEN DaysInStock <= 60 THEN 'Yellow'
    WHEN DaysInStock <= 90 THEN 'Orange'
    ELSE 'Red'
  END AS ColorCode
FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_inventory_health
GROUP BY
  CASE
    WHEN DaysInStock <= 7 THEN '0-7 days (Fresh)'
    WHEN DaysInStock <= 30 THEN '8-30 days (Normal)'
    WHEN DaysInStock <= 60 THEN '31-60 days (Aging)'
    WHEN DaysInStock <= 90 THEN '61-90 days (Stale)'
    ELSE '90+ days (Dead Stock)'
  END,
  CASE
    WHEN DaysInStock <= 7 THEN 'Green'
    WHEN DaysInStock <= 30 THEN 'Blue'
    WHEN DaysInStock <= 60 THEN 'Yellow'
    WHEN DaysInStock <= 90 THEN 'Orange'
    ELSE 'Red'
  END
ORDER BY
  MIN(DaysInStock)
"""

print("Query 1.4: Stock Aging Distribution")
print("="*70)
spark.sql(query).display()
print("\nCHART CONFIG:")
print("X-axis: StockAgeBucket")
print("Y-axis: ProductCount")
print("Color: ColorCode")
print("Chart Type: Column/Bar")

# COMMAND ----------

# DBTITLE 1,Page 2: Supplier Scorecard Dashboard
# MAGIC %md
# MAGIC # Page 2: Purchase Analytics Dashboard
# MAGIC
# MAGIC ## Business Questions
# MAGIC 1. Which wholesalers do we purchase most from?
# MAGIC 2. What are monthly purchase cost trends?
# MAGIC 3. Which products do we purchase most frequently?
# MAGIC 4. How is purchase volume distributed across warehouses?
# MAGIC
# MAGIC ## Charts
# MAGIC
# MAGIC ### Chart 1: Purchase Volume by Wholesaler (Bar Chart)
# MAGIC - **X-axis:** WholesalerName
# MAGIC - **Y-axis:** TotalPurchaseAmount_Cr
# MAGIC - **Sort:** Descending by purchase amount
# MAGIC - **Additional Metrics:** Order count, average order value
# MAGIC - **Data Source:** `fact_purchase` + `dim_wholesaler`
# MAGIC
# MAGIC ### Chart 2: Monthly Purchase Cost Trend (Line Chart)
# MAGIC - **X-axis:** Month
# MAGIC - **Y-axis:** MonthlySpend_Cr
# MAGIC - **Metrics:** Total spend, order count, average unit cost
# MAGIC - **Trend Line:** 3-month moving average
# MAGIC - **Data Source:** `fact_purchase` with date aggregation
# MAGIC
# MAGIC ### Chart 3: Top Products Purchased (Horizontal Bar)
# MAGIC - **X-axis:** TotalQuantity
# MAGIC - **Y-axis:** ProductName
# MAGIC - **Additional Metrics:** Total cost, order frequency
# MAGIC - **Limit:** Top 15 products
# MAGIC - **Data Source:** `fact_purchase` + `dim_product`
# MAGIC
# MAGIC ### Chart 4: Warehouse Purchase Distribution (Treemap)
# MAGIC - **Size:** TotalPurchaseAmount
# MAGIC - **Color:** PurchaseVolume (units)
# MAGIC - **Label:** WarehouseName
# MAGIC - **Data Source:** `fact_purchase` + `dim_warehouse`
# MAGIC
# MAGIC ## Cross-Filtering
# MAGIC - Click wholesaler in Chart 1 → filters all other charts to that wholesaler
# MAGIC - Select time range in Chart 2 → filters Chart 1, 3, 4 to that period
# MAGIC - Click warehouse in Chart 4 → filters Chart 1, 2, 3 to that warehouse

# COMMAND ----------

# DBTITLE 1,Query 2.1: Supplier Performance Scorecard Table
# Purchase Volume by Wholesaler
query = f"""
SELECT
  w.WholesalerName,
  COUNT(DISTINCT fp.PurchaseID) AS TotalOrders,
  SUM(fp.Quantity) AS TotalQuantityPurchased,
  ROUND(SUM(fp.NetAmount) / 10000000, 2) AS TotalPurchaseAmount_Cr,
  ROUND(AVG(fp.NetAmount), 2) AS AvgOrderValue,
  ROUND(AVG(fp.UnitCost), 2) AS AvgUnitCost
FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_purchase fp
INNER JOIN {Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_wholesaler w
  ON fp.WholesalerSK = w.WholesalerSK
GROUP BY w.WholesalerName
ORDER BY TotalPurchaseAmount_Cr DESC
"""

print("Query 2.1: Purchase Volume by Wholesaler")
print("="*70)
spark.sql(query).display()
print("\nCHART CONFIG:")
print("Chart Type: Bar Chart")
print("X-axis: WholesalerName")
print("Y-axis: TotalPurchaseAmount_Cr")
print("Sort: Descending by purchase amount")
print("Tooltip: Show TotalOrders, TotalQuantityPurchased, AvgOrderValue")

# COMMAND ----------

# DBTITLE 1,Query 2.2: Revenue by Supplier Treemap
# Monthly Purchase Cost Trend with Moving Average
query = f"""
WITH monthly_purchases AS (
  SELECT
    DATE_TRUNC('month', fp.PurchaseDate) AS Month,
    COUNT(DISTINCT fp.PurchaseID) AS OrderCount,
    SUM(fp.Quantity) AS TotalQuantity,
    ROUND(SUM(fp.NetAmount) / 10000000, 2) AS MonthlySpend_Cr,
    ROUND(AVG(fp.UnitCost), 2) AS AvgUnitCost
  FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_purchase fp
  GROUP BY DATE_TRUNC('month', fp.PurchaseDate)
)
SELECT
  Month,
  MonthlySpend_Cr,
  OrderCount,
  TotalQuantity,
  AvgUnitCost,
  ROUND(AVG(MonthlySpend_Cr) OVER (
    ORDER BY Month
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ), 2) AS MovingAvg_3Months
FROM monthly_purchases
ORDER BY Month
"""

print("Query 2.2: Monthly Purchase Cost Trend")
print("="*70)
spark.sql(query).display()
print("\nCHART CONFIG:")
print("Chart Type: Line Chart")
print("X-axis: Month")
print("Y-axis (Primary): MonthlySpend_Cr (Line)")
print("Y-axis (Secondary): OrderCount (Line)")
print("Trend Line: MovingAvg_3Months (Dashed)")

# COMMAND ----------

# DBTITLE 1,Query 2.3: Delivery Performance Trend
# Top 15 Products Purchased (by Quantity)
query = f"""
SELECT
  p.ProductName,
  SUM(fp.Quantity) AS TotalQuantityPurchased,
  COUNT(DISTINCT fp.PurchaseID) AS OrderFrequency,
  ROUND(SUM(fp.NetAmount) / 10000000, 2) AS TotalCost_Cr,
  ROUND(AVG(fp.UnitCost), 2) AS AvgUnitCost,
  COUNT(DISTINCT fp.WholesalerSK) AS UniqueWholesalers
FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_purchase fp
INNER JOIN {Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_product p
  ON fp.ProductSK = p.ProductSK
GROUP BY p.ProductName
ORDER BY TotalQuantityPurchased DESC
LIMIT 15
"""

print("Query 2.3: Top 15 Products Purchased")
print("="*70)
spark.sql(query).display()
print("\nCHART CONFIG:")
print("Chart Type: Horizontal Bar")
print("X-axis: TotalQuantityPurchased")
print("Y-axis: ProductName")
print("Sort: Descending by quantity")
print("Tooltip: Show TotalCost_Cr, OrderFrequency, AvgUnitCost")

# COMMAND ----------

# DBTITLE 1,Query 2.4: Supplier Reliability vs Volume Scatter
# Warehouse Purchase Distribution
query = f"""
SELECT
  wh.WareHouseName,
  wh.WareHouseCity,
  COUNT(DISTINCT fp.PurchaseID) AS TotalOrders,
  SUM(fp.Quantity) AS TotalQuantityReceived,
  ROUND(SUM(fp.NetAmount) / 10000000, 2) AS TotalPurchaseAmount_Cr,
  COUNT(DISTINCT fp.ProductSK) AS UniqueProductsPurchased,
  COUNT(DISTINCT fp.WholesalerSK) AS UniqueWholesalers,
  ROUND(AVG(fp.UnitCost), 2) AS AvgUnitCost
FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_purchase fp
INNER JOIN {Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_warehouse wh
  ON fp.WarehouseSK = wh.WarehouseSK
GROUP BY wh.WareHouseName, wh.WareHouseCity
ORDER BY TotalPurchaseAmount_Cr DESC
"""

print("Query 2.4: Warehouse Purchase Distribution")
print("="*70)
spark.sql(query).display()
print("\nCHART CONFIG:")
print("Chart Type: Treemap")
print("Size: TotalPurchaseAmount_Cr")
print("Color: TotalQuantityReceived (gradient)")
print("Label: WareHouseName")
print("Tooltip: Show TotalOrders, UniqueProductsPurchased, UniqueWholesalers")

# COMMAND ----------

# DBTITLE 1,Page 3: Order Analytics Dashboard
# MAGIC %md
# MAGIC # Page 3: Order Analytics Dashboard
# MAGIC
# MAGIC ## Business Questions
# MAGIC 1. What are daily/monthly sales trends?
# MAGIC 2. Which payment modes are most popular?
# MAGIC 3. Which customer segments drive most revenue?
# MAGIC 4. What's the sales conversion funnel?
# MAGIC
# MAGIC ## Charts
# MAGIC
# MAGIC ### Chart 1: Sales Trend (Line + Area Chart)
# MAGIC - **X-axis:** Date
# MAGIC - **Y-axis 1:** DailySalesAmount_Cr (Area)
# MAGIC - **Y-axis 2:** OrderCount (Line)
# MAGIC - **Trend Line:** 7-day moving average
# MAGIC - **Data Source:** `fact_sales` daily aggregation
# MAGIC
# MAGIC ### Chart 2: Payment Mode Distribution (Donut Chart)
# MAGIC - **Slices:** PaymentMode
# MAGIC - **Size:** TotalSalesAmount
# MAGIC - **Label:** Percentage + Amount
# MAGIC - **Data Source:** `fact_sales` grouped by payment mode
# MAGIC
# MAGIC ### Chart 3: Top Products by Revenue (Bar Chart)
# MAGIC - **X-axis:** ProductName
# MAGIC - **Y-axis:** TotalRevenue_Cr
# MAGIC - **Color:** ProductCategory
# MAGIC - **Limit:** Top 15 products
# MAGIC - **Data Source:** `mart_product_performance`
# MAGIC
# MAGIC ### Chart 4: Customer Segment Revenue (Stacked Bar)
# MAGIC - **X-axis:** Month
# MAGIC - **Y-axis:** Revenue_Cr
# MAGIC - **Stacks:** CustomerType (New/Returning/VIP)
# MAGIC - **Data Source:** `mart_customer_360` + `fact_sales`
# MAGIC
# MAGIC ## Cross-Filtering
# MAGIC - Select date range in Chart 1 → filters all other charts
# MAGIC - Click payment mode in Chart 2 → filters Chart 1 and 3 to that payment mode
# MAGIC - Click product category in Chart 3 → filters Chart 1 and 2

# COMMAND ----------

# DBTITLE 1,Query 3.1: Daily Sales Trend with Moving Average
# Daily Sales Trend with 7-Day Moving Average
query = f"""
WITH daily_sales AS (
  SELECT
    d.Date AS Date,
    ROUND(SUM(fs.finalprice) / 10000000, 2) AS DailySales_Cr,
    COUNT(DISTINCT fs.OrderID) AS OrderCount
  FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_sales fs
  INNER JOIN {Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_date d ON fs.OrderDateID = d.DateID
  GROUP BY d.Date
)
SELECT
  Date,
  DailySales_Cr,
  OrderCount,
  ROUND(AVG(DailySales_Cr) OVER (
    ORDER BY Date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ), 2) AS MovingAvg_7Days
FROM daily_sales
ORDER BY Date
"""

print("Query 3.1: Daily Sales Trend with Moving Average")
print("="*70)
spark.sql(query).display()
print("\nCHART CONFIG:")
print("Chart Type: Combo (Area + Line)")
print("X-axis: Date")
print("Y-axis (Primary): DailySales_Cr (Area, filled)")
print("Y-axis (Secondary): OrderCount (Line)")
print("Trend: MovingAvg_7Days (Dashed line)")

# COMMAND ----------

# DBTITLE 1,Query 3.2: Payment Mode Distribution
# Payment Mode Revenue Distribution
query = f"""
SELECT
  pm.PaymentMode,
  COUNT(DISTINCT fs.OrderID) AS OrderCount,
  ROUND(SUM(fs.finalprice) / 10000000, 2) AS TotalRevenue_Cr,
  ROUND(SUM(fs.finalprice) * 100.0 / SUM(SUM(fs.finalprice)) OVER (), 2) AS RevenuePercent
FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_sales fs
INNER JOIN {Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_payment_mode pm
  ON fs.PaymentModeSK = pm.PaymentModeSK
GROUP BY pm.PaymentMode
ORDER BY TotalRevenue_Cr DESC
"""

print("Query 3.2: Payment Mode Distribution")
print("="*70)
spark.sql(query).display()
print("\nCHART CONFIG:")
print("Chart Type: Donut/Pie Chart")
print("Slices: PaymentMode")
print("Size: TotalRevenue_Cr")
print("Label: {PaymentMode}: {RevenuePercent}% (Rs {TotalRevenue_Cr} Cr)")

# COMMAND ----------

# DBTITLE 1,Query 3.3: Top Products by Revenue
# Top 15 Products by Revenue with Category Color
query = f"""
SELECT
  p.ProductName,
  ROUND(SUM(fs.finalprice) / 10000000, 2) AS TotalRevenue_Cr,
  COUNT(DISTINCT fs.OrderID) AS OrderCount,
  SUM(fs.ProductQuantity) AS TotalQuantitySold,
  ROUND(AVG(fs.finalprice), 2) AS AvgOrderValue
FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_sales fs
INNER JOIN {Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_product p
  ON fs.ProductSK = p.ProductSK
GROUP BY p.ProductName
ORDER BY TotalRevenue_Cr DESC
LIMIT 15
"""

print("Query 3.3: Top Products by Revenue")
print("="*70)
spark.sql(query).display()
print("\nCHART CONFIG:")
print("Chart Type: Horizontal Bar")
print("X-axis: TotalRevenue_Cr")
print("Y-axis: ProductName")
print("Tooltip: Show OrderCount, TotalQuantitySold, and AvgOrderValue")

# COMMAND ----------

# DBTITLE 1,Query 3.4: Customer Segment Revenue Over Time
# Monthly Revenue by Customer Segment (Stacked)
query = f"""
WITH customer_segments AS (
  SELECT
    CustomerSK,
    CASE
      WHEN total_orders = 1 THEN 'New Customer'
      WHEN total_orders BETWEEN 2 AND 5 THEN 'Returning Customer'
      WHEN total_orders > 5 THEN 'VIP Customer'
    END AS CustomerSegment
  FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_customer_360
),
monthly_segment_sales AS (
  SELECT
    DATE_TRUNC('month', d.Date) AS Month,
    cs.CustomerSegment,
    ROUND(SUM(fs.finalprice) / 10000000, 2) AS Revenue_Cr,
    COUNT(DISTINCT fs.CustomerSK) AS UniqueCustomers
  FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_sales fs
  INNER JOIN {Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_date d ON fs.OrderDateID = d.DateID
  INNER JOIN customer_segments cs ON fs.CustomerSK = cs.CustomerSK
  GROUP BY DATE_TRUNC('month', d.Date), cs.CustomerSegment
)
SELECT
  Month,
  CustomerSegment,
  Revenue_Cr,
  UniqueCustomers
FROM monthly_segment_sales
ORDER BY Month, CustomerSegment
"""

print("Query 3.4: Customer Segment Revenue Over Time")
print("="*70)
spark.sql(query).display()
print("\nCHART CONFIG:")
print("Chart Type: Stacked Bar")
print("X-axis: Month")
print("Y-axis: Revenue_Cr")
print("Stack: CustomerSegment (3 colors: New=Blue, Returning=Green, VIP=Gold)")
print("Tooltip: Show UniqueCustomers count")

# COMMAND ----------

# DBTITLE 1,Dashboard Implementation Guide
# MAGIC %md
# MAGIC # Dashboard Implementation Guide
# MAGIC
# MAGIC ## Step 1: Create Lakeview Dashboard
# MAGIC 1. Navigate to Workspace → Dashboards
# MAGIC 2. Click "Create Dashboard"
# MAGIC 3. Name: "SkillAIPath Executive Dashboard"
# MAGIC 4. Add 3 pages (tabs): Inventory, Suppliers, Orders
# MAGIC
# MAGIC ## Step 2: Add Queries
# MAGIC 1. For each query cell in this notebook:
# MAGIC    - Copy the SQL code
# MAGIC    - Create new query in dashboard
# MAGIC    - Name it (e.g., "Inventory_KPI_Metrics")
# MAGIC    - Run and save
# MAGIC
# MAGIC ## Step 3: Build Visualizations
# MAGIC 1. Page 1 (Inventory):
# MAGIC    - Add 4 KPI cards using Query 1.1
# MAGIC    - Add bar chart using Query 1.2
# MAGIC    - Add horizontal bar using Query 1.3
# MAGIC    - Add histogram using Query 1.4
# MAGIC
# MAGIC 2. Page 2 (Suppliers):
# MAGIC    - Add table using Query 2.1 (apply conditional formatting)
# MAGIC    - Add treemap using Query 2.2
# MAGIC    - Add line chart using Query 2.3
# MAGIC    - Add scatter plot using Query 2.4
# MAGIC
# MAGIC 3. Page 3 (Orders):
# MAGIC    - Add combo chart using Query 3.1
# MAGIC    - Add donut chart using Query 3.2
# MAGIC    - Add horizontal bar using Query 3.3
# MAGIC    - Add stacked bar using Query 3.4
# MAGIC
# MAGIC ## Step 4: Configure Cross-Filtering
# MAGIC 1. Click "Edit" mode
# MAGIC 2. Select chart → Settings → Enable "Use as filter"
# MAGIC 3. Test: Click warehouse in Page 1 → verify Page 1 charts update
# MAGIC
# MAGIC ## Step 5: Add Drill-Through (Optional)
# MAGIC 1. Select Chart 1.2 (Warehouse Utilization)
# MAGIC 2. Settings → Actions → Add "Go to Dashboard"
# MAGIC 3. Target: New page "Warehouse Detail" (create if needed)
# MAGIC 4. Pass parameter: WarehouseName
# MAGIC
# MAGIC ## Step 6: Styling
# MAGIC - Set color palette: Green (#27AE60) for positive, Red (#E74C3C) for negative
# MAGIC - Add dashboard logo/header
# MAGIC - Add text boxes for KPI explanations
# MAGIC - Set refresh schedule: Every 1 hour
# MAGIC
# MAGIC ## Interview Talking Points
# MAGIC **Q: Why Gold layer for dashboards?**
# MAGIC "Gold tables are pre-aggregated and business-ready. Querying Silver facts directly would be too slow for dashboard refresh and harder for business users to understand."
# MAGIC
# MAGIC **Q: Why cross-filtering?**
# MAGIC "Business users explore data interactively. Click warehouse → see that warehouse's products. This is self-service analytics."
# MAGIC
# MAGIC **Q: Why these specific metrics?**
# MAGIC "Each metric answers a business question. Inventory health = actionable (order more or discount). Supplier scorecard = vendor management decisions. Sales trend = demand forecasting."

# COMMAND ----------

# DBTITLE 1,🚀 Programmatic Query Creation
# 🚀 AUTOMATIC QUERY CREATION FOR DASHBOARD
# Run this cell to create all 12 SQL queries programmatically
# Then build the dashboard visually in Lakeview UI

import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

print("="*90)
print("🚀 PROGRAMMATIC QUERY CREATION FOR DASHBOARD")
print("="*90)
print(f"Environment: {Config.ENVIRONMENT.upper()}")
print(f"Catalog: {Config.CATALOG} | Gold: {Config.SCHEMA_GOLD} | Silver: {Config.SCHEMA_SILVER}")
print("="*90)

# Define all 12 queries
queries = [
    ("01_Inventory_KPI_Metrics", f"""
WITH inventory_summary AS (
  SELECT
    SUM(StockValue) / 10000000 AS total_inventory_cr,
    AVG(TurnoverRatio) AS avg_turnover,
    SUM(CASE WHEN DaysInStock > 60 THEN 1 ELSE 0 END) AS overstock_count,
    SUM(CASE WHEN DaysInStock < 7 THEN 1 ELSE 0 END) AS understock_count,
    COUNT(*) AS total_products
  FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_inventory_health
)
SELECT
  ROUND(total_inventory_cr, 2) AS TotalInventoryValue_Cr,
  ROUND(avg_turnover, 2) AS AvgTurnoverRatio,
  overstock_count AS OverstockProducts,
  understock_count AS UnderstockProducts,
  total_products AS TotalActiveProducts
FROM inventory_summary"""),
    
    ("02_Warehouse_Utilization", f"""
SELECT
  ih.WarehouseName,
  ROUND(AVG(ih.UtilizationPercent), 2) AS UtilizationPercent,
  CASE
    WHEN AVG(ih.UtilizationPercent) >= 80 THEN 'Critical'
    WHEN AVG(ih.UtilizationPercent) >= 60 THEN 'Warning'
    ELSE 'Healthy'
  END AS HealthStatus,
  COUNT(DISTINCT ih.ProductSK) AS ProductCount
FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_inventory_health ih
GROUP BY ih.WarehouseName
ORDER BY UtilizationPercent DESC"""),
    
    ("03_Bottom_Products_Turnover", f"""
SELECT
  ih.ProductName,
  ROUND(ih.TurnoverRatio, 2) AS TurnoverRatio,
  ROUND(ih.StockValue / 100000, 2) AS StockValue_Lakhs,
  ih.DaysInStock,
  CASE
    WHEN ih.TurnoverRatio < 1.0 THEN 'Slow Mover'
    WHEN ih.TurnoverRatio < 2.0 THEN 'Moderate'
    ELSE 'Fast Mover'
  END AS MoverCategory
FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_inventory_health ih
ORDER BY ih.TurnoverRatio ASC
LIMIT 20"""),
    
    ("04_Stock_Aging_Distribution", f"""
SELECT
  CASE
    WHEN DaysInStock <= 7 THEN '0-7 days (Fresh)'
    WHEN DaysInStock <= 30 THEN '8-30 days (Normal)'
    WHEN DaysInStock <= 60 THEN '31-60 days (Aging)'
    WHEN DaysInStock <= 90 THEN '61-90 days (Stale)'
    ELSE '90+ days (Dead Stock)'
  END AS StockAgeBucket,
  COUNT(*) AS ProductCount,
  ROUND(SUM(StockValue) / 100000, 2) AS TotalValue_Lakhs
FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_inventory_health
GROUP BY 1
ORDER BY MIN(DaysInStock)"""),
    
    ("05_Purchase_By_Wholesaler", f"""
SELECT
  w.WholesalerName,
  COUNT(DISTINCT fp.PurchaseID) AS TotalOrders,
  SUM(fp.Quantity) AS TotalQuantityPurchased,
  ROUND(SUM(fp.NetAmount) / 10000000, 2) AS TotalPurchaseAmount_Cr,
  ROUND(AVG(fp.NetAmount), 2) AS AvgOrderValue
FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_purchase fp
INNER JOIN {Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_wholesaler w ON fp.WholesalerSK = w.WholesalerSK
GROUP BY w.WholesalerName
ORDER BY TotalPurchaseAmount_Cr DESC"""),
    
    ("06_Monthly_Purchase_Trend", f"""
WITH monthly_purchases AS (
  SELECT
    DATE_TRUNC('month', fp.PurchaseDate) AS Month,
    COUNT(DISTINCT fp.PurchaseID) AS OrderCount,
    ROUND(SUM(fp.NetAmount) / 10000000, 2) AS MonthlySpend_Cr
  FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_purchase fp
  GROUP BY 1
)
SELECT
  Month,
  MonthlySpend_Cr,
  OrderCount,
  ROUND(AVG(MonthlySpend_Cr) OVER (ORDER BY Month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS MovingAvg_3M
FROM monthly_purchases
ORDER BY Month"""),
    
    ("07_Top_Products_Purchased", f"""
SELECT
  p.ProductName,
  SUM(fp.Quantity) AS TotalQuantityPurchased,
  COUNT(DISTINCT fp.PurchaseID) AS OrderFrequency,
  ROUND(SUM(fp.NetAmount) / 10000000, 2) AS TotalCost_Cr
FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_purchase fp
INNER JOIN {Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_product p ON fp.ProductSK = p.ProductSK
GROUP BY p.ProductName
ORDER BY TotalQuantityPurchased DESC
LIMIT 15"""),
    
    ("08_Warehouse_Purchase_Dist", f"""
SELECT
  wh.WareHouseName,
  COUNT(DISTINCT fp.PurchaseID) AS TotalOrders,
  ROUND(SUM(fp.NetAmount) / 10000000, 2) AS TotalPurchaseAmount_Cr,
  COUNT(DISTINCT fp.ProductSK) AS UniqueProductsPurchased
FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_purchase fp
INNER JOIN {Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_warehouse wh ON fp.WarehouseSK = wh.WarehouseSK
GROUP BY wh.WareHouseName
ORDER BY TotalPurchaseAmount_Cr DESC"""),
    
    ("09_Daily_Sales_Trend", f"""
WITH daily_sales AS (
  SELECT
    d.Date,
    ROUND(SUM(fs.finalprice) / 10000000, 2) AS DailySales_Cr,
    COUNT(DISTINCT fs.OrderID) AS OrderCount
  FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_sales fs
  INNER JOIN {Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_date d ON fs.OrderDateID = d.DateID
  GROUP BY d.Date
)
SELECT
  Date,
  DailySales_Cr,
  OrderCount,
  ROUND(AVG(DailySales_Cr) OVER (ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS MovingAvg_7D
FROM daily_sales
ORDER BY Date"""),
    
    ("10_Payment_Mode_Distribution", f"""
SELECT
  pm.PaymentMode,
  COUNT(DISTINCT fs.OrderID) AS OrderCount,
  ROUND(SUM(fs.finalprice) / 10000000, 2) AS TotalRevenue_Cr,
  ROUND(SUM(fs.finalprice) * 100.0 / SUM(SUM(fs.finalprice)) OVER (), 2) AS RevenuePercent
FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_sales fs
INNER JOIN {Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_payment_mode pm ON fs.PaymentModeSK = pm.PaymentModeSK
GROUP BY pm.PaymentMode
ORDER BY TotalRevenue_Cr DESC"""),
    
    ("11_Top_Products_Revenue", f"""
SELECT
  p.ProductName,
  ROUND(SUM(fs.finalprice) / 10000000, 2) AS TotalRevenue_Cr,
  COUNT(DISTINCT fs.OrderID) AS OrderCount,
  SUM(fs.ProductQuantity) AS TotalQuantitySold
FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_sales fs
INNER JOIN {Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_product p ON fs.ProductSK = p.ProductSK
GROUP BY p.ProductName
ORDER BY TotalRevenue_Cr DESC
LIMIT 15"""),
    
    ("12_Customer_Segment_Revenue", f"""
WITH customer_segments AS (
  SELECT
    CustomerSK,
    CASE
      WHEN total_orders = 1 THEN 'New Customer'
      WHEN total_orders BETWEEN 2 AND 5 THEN 'Returning Customer'
      WHEN total_orders > 5 THEN 'VIP Customer'
    END AS CustomerSegment
  FROM {Config.CATALOG}.{Config.SCHEMA_GOLD}.mart_customer_360
)
SELECT
  DATE_TRUNC('month', d.Date) AS Month,
  cs.CustomerSegment,
  ROUND(SUM(fs.finalprice) / 10000000, 2) AS Revenue_Cr,
  COUNT(DISTINCT fs.CustomerSK) AS UniqueCustomers
FROM {Config.CATALOG}.{Config.SCHEMA_SILVER}.fact_sales fs
INNER JOIN {Config.CATALOG}.{Config.SCHEMA_SILVER}.dim_date d ON fs.OrderDateID = d.DateID
INNER JOIN customer_segments cs ON fs.CustomerSK = cs.CustomerSK
GROUP BY 1, 2
ORDER BY Month, CustomerSegment""")
]

print(f"\n📊 Creating {len(queries)} SQL queries...\n")

created = {}
for i, (name, sql) in enumerate(queries, 1):
    try:
        print(f"  [{i:2d}/{len(queries)}] Creating: {name:35s} ", end="")
        q = w.queries.create(display_name=name, query_text=sql)
        created[name] = q.id
        print(f"✅ ID: {q.id}")
        time.sleep(0.3)
    except Exception as e:
        print(f"❌ Error: {str(e)[:50]}")

print(f"\n" + "="*90)
print(f"✅ SUCCESS! Created {len(created)}/{len(queries)} queries")
print("="*90)

print("\n📱 NEXT STEP: Build Dashboard in Lakeview UI")
print("-"*90)
print("1️⃣  Go to: SQL Editor → Dashboards → Create Dashboard")
print(f"2️⃣  Name: 'SkillAIPath Executive Dashboard ({Config.ENVIRONMENT.upper()})'")
print("3️⃣  Add 3 pages: Inventory Overview | Purchase Analytics | Order Analytics")
print("4️⃣  Drag queries from sidebar into each page and create visualizations")

print("\n📊 Page 1 - Inventory Overview:")
print("   • 01_Inventory_KPI_Metrics → 5 KPI Cards")
print("   • 02_Warehouse_Utilization → Bar Chart (X=WarehouseName, Y=UtilizationPercent, Color=HealthStatus)")
print("   • 03_Bottom_Products_Turnover → Horizontal Bar (X=TurnoverRatio, Y=ProductName)")
print("   • 04_Stock_Aging_Distribution → Column Chart (X=StockAgeBucket, Y=ProductCount)")

print("\n📊 Page 2 - Purchase Analytics:")
print("   • 05_Purchase_By_Wholesaler → Bar Chart (X=WholesalerName, Y=TotalPurchaseAmount_Cr)")
print("   • 06_Monthly_Purchase_Trend → Line Chart (X=Month, Y=MonthlySpend_Cr + MovingAvg_3M)")
print("   • 07_Top_Products_Purchased → Horizontal Bar (X=TotalQuantityPurchased, Y=ProductName)")
print("   • 08_Warehouse_Purchase_Dist → Table or Treemap")

print("\n📊 Page 3 - Order Analytics:")
print("   • 09_Daily_Sales_Trend → Combo Chart (Area=DailySales_Cr, Line=OrderCount)")
print("   • 10_Payment_Mode_Distribution → Donut Chart (Slices=PaymentMode, Size=TotalRevenue_Cr)")
print("   • 11_Top_Products_Revenue → Horizontal Bar (X=TotalRevenue_Cr, Y=ProductName)")
print("   • 12_Customer_Segment_Revenue → Stacked Bar (X=Month, Y=Revenue_Cr, Stack=CustomerSegment)")

print("\n⚡ Enable cross-filtering: Chart settings → 'Use as filter'")
print("🔄 Set refresh: Dashboard settings → Schedule → Every 1 hour")
print("="*90)
print("🚀 Dashboard queries ready! Build time: ~5-10 minutes in Lakeview UI")
print("="*90)