
  
    
        create or replace table `sap_dev`.`silver`.`dim_payment_mode`
      
      
    using delta
  
      
      
      
      
      
      
      
      
      as
      -- dbt Intermediate Model: Payment Mode Dimension
-- Creates payment mode dimension from sales transactions



WITH unique_payment_modes AS (
  SELECT DISTINCT
    PaymentMode
  FROM `sap_dev`.`staging`.`vw_sales`
  WHERE PaymentMode IS NOT NULL
)

SELECT
  ROW_NUMBER() OVER (ORDER BY PaymentMode) AS PaymentModeSK,
  ROW_NUMBER() OVER (ORDER BY PaymentMode) AS PaymentModeID,
  PaymentMode
FROM unique_payment_modes
  