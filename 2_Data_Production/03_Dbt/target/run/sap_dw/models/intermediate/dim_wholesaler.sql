
  
    
        create or replace table `sap_dev`.`silver`.`dim_wholesaler`
      
      
    using delta
  
      
      
      
      
      
      
      
      
      as
      -- dbt Intermediate Model: Wholesaler Dimension
-- Creates wholesaler dimension from purchase transactions



WITH unique_wholesalers AS (
  SELECT DISTINCT
    WholeSalerName
  FROM `sap_dev`.`staging`.`vw_purchases`
  WHERE WholeSalerName IS NOT NULL
)

SELECT
  ROW_NUMBER() OVER (ORDER BY WholeSalerName) AS WholeSalerSK,
  ROW_NUMBER() OVER (ORDER BY WholeSalerName) AS WholeSalerID,
  WholeSalerName
FROM unique_wholesalers
  