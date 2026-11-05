
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CustomerKey
from `sap_dev`.`gold`.`mart_customer_360`
where CustomerKey is null



  
  
      
    ) dbt_internal_test