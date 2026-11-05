
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select order_count
from `sap_dev`.`silver`.`int_customer_metrics`
where order_count is null



  
  
      
    ) dbt_internal_test