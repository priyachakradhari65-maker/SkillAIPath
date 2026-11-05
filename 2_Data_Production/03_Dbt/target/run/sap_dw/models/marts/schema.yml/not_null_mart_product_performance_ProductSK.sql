
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ProductSK
from `sap_dev`.`gold`.`mart_product_performance`
where ProductSK is null



  
  
      
    ) dbt_internal_test