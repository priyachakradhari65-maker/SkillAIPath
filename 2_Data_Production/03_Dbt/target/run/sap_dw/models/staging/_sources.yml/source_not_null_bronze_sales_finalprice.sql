
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select finalprice
from `sap_dev`.`bronze`.`stg_sales`
where finalprice is null



  
  
      
    ) dbt_internal_test