-- dbt Intermediate Model: Product Dimension
-- Creates product dimension with pricing information

{{
  config(
    materialized='table',
    tags=['intermediate', 'dimension', 'product']
  )
}}

WITH product_base AS (
  SELECT DISTINCT
    ProductName,
    MAX(SellingPrice) AS MRP
  FROM {{ source('bronze', 'sales') }}
  WHERE ProductName IS NOT NULL
  GROUP BY ProductName
)

SELECT
  ROW_NUMBER() OVER (ORDER BY ProductName) AS ProductSK,
  ROW_NUMBER() OVER (ORDER BY ProductName) AS ProductID,
  ProductName,
  CAST(MRP AS DOUBLE) AS MRP
FROM product_base
