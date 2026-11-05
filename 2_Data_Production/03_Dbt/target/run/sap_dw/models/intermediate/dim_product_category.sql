
  
    
        create or replace table `sap_dev`.`silver`.`dim_product_category`
      
      
    using delta
  
      
      
      
      
      
      
      
      
      as
      -- dbt Intermediate Model: Product Category Dimension
-- Creates product category dimension



WITH unique_categories AS (
  SELECT DISTINCT
    ProductCategory
  FROM `sap_dev`.`bronze`.`stg_sales`
  WHERE ProductCategory IS NOT NULL
)

SELECT
  ROW_NUMBER() OVER (ORDER BY ProductCategory) AS ProductCategorySK,
  ROW_NUMBER() OVER (ORDER BY ProductCategory) AS ProductCategoryID,
  ProductCategory
FROM unique_categories
  