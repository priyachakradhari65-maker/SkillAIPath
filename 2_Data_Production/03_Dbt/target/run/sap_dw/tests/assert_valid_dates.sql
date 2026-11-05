
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Custom Test: Delivery must be after order

SELECT
  OrderID,
  OrderDate,
  DeliveryDate
FROM `sap_dev`.`staging`.`vw_sales`
WHERE DeliveryDate < OrderDate
  
  
      
    ) dbt_internal_test