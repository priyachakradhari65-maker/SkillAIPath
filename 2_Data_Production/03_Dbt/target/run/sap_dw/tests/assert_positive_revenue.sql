
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Custom Test: All sales revenue must be positive

SELECT
  OrderID,
  FinalPrice
FROM `sap_dev`.`staging`.`vw_sales`
WHERE FinalPrice <= 0
  
  
      
    ) dbt_internal_test