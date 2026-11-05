
  
    
        create or replace table `sap_dev`.`silver`.`dim_product`
      
      
    using delta
  
      
      
      
      
      
      
      
      
      as
      -- dbt Intermediate Model: Product Dimension
-- Creates product dimension with pricing information



WITH product_base AS (
  SELECT DISTINCT
    ProductName,
    MAX(SellingPrice) AS MRP
  FROM `sap_dev`.`bronze`.`stg_sales`
  WHERE ProductName IS NOT NULL
  GROUP BY ProductName
)

SELECT
  ROW_NUMBER() OVER (ORDER BY ProductName) AS ProductSK,
  ROW_NUMBER() OVER (ORDER BY ProductName) AS ProductID,
  ProductName,
  CAST(MRP AS DOUBLE) AS MRP
FROM product_base
  