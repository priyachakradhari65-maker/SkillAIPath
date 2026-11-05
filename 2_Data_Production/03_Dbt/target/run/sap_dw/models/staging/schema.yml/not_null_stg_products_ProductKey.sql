
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ProductKey
from `sap_dev`.`staging`.`vw_products`
where ProductKey is null



  
  
      
    ) dbt_internal_test