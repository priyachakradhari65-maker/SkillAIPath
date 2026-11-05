
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ProductSK
from `sap_dev`.`silver`.`fact_sales`
where ProductSK is null



  
  
      
    ) dbt_internal_test