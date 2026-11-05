-- dbt Intermediate Model: Product Performance Metrics
-- Aggregates product sales and inventory metrics



WITH product_sales AS (
  SELECT
    ProductName,
    COUNT(DISTINCT OrderID, BatchDate) AS total_orders, -- Phase P1: OrderID restarts daily — grain is (OrderID, BatchDate)
    SUM(ProductQuantity) AS total_quantity_sold,
    SUM(FinalPrice) AS total_revenue,
    AVG(SellingPrice) AS avg_selling_price,
    AVG(CostPrice) AS avg_cost_price,
    AVG(SellingPrice - CostPrice) AS avg_margin,
    MIN(OrderDate) AS first_sale_date,
    MAX(OrderDate) AS last_sale_date
  FROM `sap_dev`.`staging`.`vw_sales`
  GROUP BY ProductName
),

product_inventory AS (
  SELECT
    ProductName,
    AVG(CurrentStock) AS avg_stock_level,
    MAX(ReorderLevel) AS reorder_threshold
  FROM `sap_dev`.`staging`.`vw_stock`
  GROUP BY ProductName
)

SELECT
  s.ProductName,
  s.total_orders,
  s.total_quantity_sold,
  ROUND(s.total_revenue, 2) AS total_revenue,
  ROUND(s.avg_selling_price, 2) AS avg_selling_price,
  ROUND(s.avg_cost_price, 2) AS avg_cost_price,
  ROUND(s.avg_margin, 2) AS avg_margin,
  s.first_sale_date,
  s.last_sale_date,
  ROUND(COALESCE(i.avg_stock_level, 0), 0) AS avg_stock_level,
  i.reorder_threshold
FROM product_sales s
LEFT JOIN product_inventory i ON s.ProductName = i.ProductName