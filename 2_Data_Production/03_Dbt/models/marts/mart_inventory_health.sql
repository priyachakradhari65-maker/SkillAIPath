-- dbt Mart Model: Inventory Health
-- Real-time inventory metrics with turnover ratio and stock aging
-- Uses dbt intermediate models (single ownership: dbt owns silver layer)

{{
  config(
    materialized='table',
    file_format='delta',
    tags=['mart', 'gold', 'inventory']
  )
}}

WITH current_inventory AS (
  SELECT
    fi.ProductSK,
    fi.WarehouseSK,
    fi.StockQuantity,
    fi.StockValue,
    fi.StockStatus,
    fi.DateID,
    d.Date AS StockDate
  FROM {{ ref('fact_inventory') }} fi
  INNER JOIN {{ ref('dim_date') }} d ON fi.DateID = d.DateID
),

sales_velocity AS (
  SELECT
    fs.ProductSK,
    COUNT(DISTINCT fs.OrderID, fs.BatchDate) AS OrderCount, -- Phase P1: OrderID restarts daily — grain is (OrderID, BatchDate)
    SUM(fs.ProductQuantity) AS TotalSold,
    AVG(fs.ProductQuantity) AS AvgQuantityPerOrder
  FROM {{ ref('fact_sales') }} fs
  INNER JOIN {{ ref('dim_date') }} d ON fs.OrderDateID = d.DateID
  WHERE d.Date >= DATE_SUB(CURRENT_DATE(), 90)  -- Last 90 days
  GROUP BY fs.ProductSK
),

latest_inventory AS (
  SELECT
    ProductSK,
    WarehouseSK,
    StockQuantity,
    StockValue,
    StockStatus,
    StockDate,
    ROW_NUMBER() OVER (PARTITION BY ProductSK, WarehouseSK ORDER BY StockDate DESC) AS rn
  FROM current_inventory
)

SELECT
  li.ProductSK,
  p.ProductName,
  li.WarehouseSK,
  w.WareHouseName AS WarehouseName,
  li.StockQuantity AS CurrentStock,
  li.StockValue AS CurrentStockValue,
  li.StockStatus,
  DATEDIFF(CURRENT_DATE(), li.StockDate) AS DaysInStock,
  COALESCE(sv.TotalSold, 0) AS QuantitySoldLast90Days,
  COALESCE(sv.OrderCount, 0) AS OrderCountLast90Days,
  CASE
    WHEN DATEDIFF(CURRENT_DATE(), li.StockDate) > 0 
    THEN ROUND(COALESCE(sv.TotalSold, 0) * 1.0 / DATEDIFF(CURRENT_DATE(), li.StockDate), 2)
    ELSE 0
  END AS TurnoverRatio,
  CASE
    WHEN li.StockStatus = 'Low Stock' THEN 'Understock'
    WHEN DATEDIFF(CURRENT_DATE(), li.StockDate) > 60 THEN 'Overstock'
    ELSE 'Healthy'
  END AS HealthStatus
FROM latest_inventory li
INNER JOIN {{ ref('dim_product') }} p ON li.ProductSK = p.ProductSK
INNER JOIN {{ ref('dim_warehouse') }} w ON li.WarehouseSK = w.WarehouseSK
LEFT JOIN sales_velocity sv ON li.ProductSK = sv.ProductSK
WHERE li.rn = 1  -- Only latest inventory snapshot per product/warehouse
