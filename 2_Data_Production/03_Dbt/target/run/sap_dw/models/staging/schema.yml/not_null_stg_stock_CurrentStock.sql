
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CurrentStock
from `sap_dev`.`staging`.`vw_stock`
where CurrentStock is null



  
  
      
    ) dbt_internal_test