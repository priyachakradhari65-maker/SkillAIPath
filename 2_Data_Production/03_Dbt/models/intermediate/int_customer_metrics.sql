-- dbt Intermediate Model: Customer Purchase Metrics
-- Aggregates customer behavior for segmentation and analytics
-- Materialized as table for performance (frequently queried by marts)

{{
  config(
    materialized='table',
    tags=['intermediate', 'customer', 'metrics']
  )
}}

WITH customer_orders AS (
  SELECT
    CustomerName,
    COUNT(DISTINCT OrderID, BatchDate) AS order_count, -- Phase P1: OrderID restarts daily — grain is (OrderID, BatchDate)
    SUM(FinalPrice) AS total_spent,
    AVG(FinalPrice) AS avg_order_value,
    MIN(OrderDate) AS first_order_date,
    MAX(OrderDate) AS last_order_date,
    DATEDIFF(CURRENT_DATE(), MAX(OrderDate)) AS days_since_last_order,
    SUM(ProductQuantity) AS total_quantity_purchased
  FROM {{ ref('stg_sales') }}
  GROUP BY CustomerName
)

SELECT
  *,
  CASE 
    WHEN order_count >= 10 THEN 'VIP'
    WHEN order_count >= 5 THEN 'Regular'
    ELSE 'New'
  END AS customer_segment,
  CASE
    WHEN days_since_last_order <= 30 THEN 'Active'
    WHEN days_since_last_order <= 90 THEN 'At Risk'
    ELSE 'Churned'
  END AS activity_status
FROM customer_orders
