
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CustomerName
from `sap_dev`.`staging`.`vw_customers`
where CustomerName is null



  
  
      
    ) dbt_internal_test