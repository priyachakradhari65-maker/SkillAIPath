
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CustomerName
from `sap_dev`.`silver`.`int_customer_metrics`
where CustomerName is null



  
  
      
    ) dbt_internal_test