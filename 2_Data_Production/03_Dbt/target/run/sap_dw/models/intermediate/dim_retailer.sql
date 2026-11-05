
  
    
        create or replace table `sap_dev`.`silver`.`dim_retailer`
      
      
    using delta
  
      
      
      
      
      
      
      
      
      as
      -- dbt Intermediate Model: Retailer Dimension
-- Creates retailer dimension from sales transactions



WITH unique_retailers AS (
  SELECT DISTINCT
    RetailerName,
    RetailerCity,
    RetailerContact,
    RetailerMail
  FROM `sap_dev`.`staging`.`vw_sales`
  WHERE RetailerName IS NOT NULL
)

SELECT
  ROW_NUMBER() OVER (ORDER BY RetailerName) AS RetailerSK,
  ROW_NUMBER() OVER (ORDER BY RetailerName) AS RetailerID,
  RetailerName,
  RetailerCity,
  RetailerContact,
  RetailerMail
FROM unique_retailers
  