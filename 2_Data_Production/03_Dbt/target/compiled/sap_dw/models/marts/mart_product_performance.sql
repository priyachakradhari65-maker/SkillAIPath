-- dbt Mart Model: Product Performance
-- Aggregates product sales, inventory turnover, and profitability
-- Uses dbt intermediate models (single ownership: dbt owns silver layer)



WITH product_sales AS (
  SELECT
    p.ProductSK,
    MAX(fs.ProductCategorySK) AS ProductCategorySK,
    COUNT(DISTINCT fs.OrderID, fs.BatchDate) AS TotalOrders, -- Phase P1: OrderID restarts daily — grain is (OrderID, BatchDate)
    SUM(fs.ProductQuantity) AS TotalQuantitySold,
    SUM(fs.finalprice) AS TotalRevenue,
    AVG(fs.finalprice) AS AvgSaleValue
  FROM `sap_dev`.`silver`.`fact_sales` fs
  INNER JOIN `sap_dev`.`silver`.`dim_product` p ON fs.ProductSK = p.ProductSK
  GROUP BY p.ProductSK
),

product_inventory AS (
  SELECT
    fi.ProductSK,
    AVG(fi.StockQuantity) AS AvgStockLevel
  FROM `sap_dev`.`silver`.`fact_inventory` fi
  GROUP BY fi.ProductSK
)

SELECT
  p.ProductSK,
  p.ProductID,
  p.ProductName,
  pc.ProductCategory,
  ps.TotalOrders,
  ps.TotalQuantitySold,
  ROUND(ps.TotalRevenue, 2) AS TotalRevenue,
  ROUND(ps.AvgSaleValue, 2) AS AvgSaleValue,
  ROUND(pi.AvgStockLevel, 0) AS AvgStockLevel,
  CASE
    WHEN ps.TotalOrders > 100 THEN 'Best Seller'
    WHEN ps.TotalOrders BETWEEN 50 AND 100 THEN 'Popular'
    WHEN ps.TotalOrders BETWEEN 10 AND 49 THEN 'Moderate'
    ELSE 'Slow Mover'
  END AS PerformanceCategory
FROM `sap_dev`.`silver`.`dim_product` p
LEFT JOIN product_sales ps ON p.ProductSK = ps.ProductSK
LEFT JOIN product_inventory pi ON p.ProductSK = pi.ProductSK
LEFT JOIN `sap_dev`.`silver`.`dim_product_category` pc ON ps.ProductCategorySK = pc.ProductCategorySK