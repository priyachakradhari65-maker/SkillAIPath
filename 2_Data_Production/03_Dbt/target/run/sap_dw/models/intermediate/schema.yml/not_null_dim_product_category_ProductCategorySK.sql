
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ProductCategorySK
from `sap_dev`.`silver`.`dim_product_category`
where ProductCategorySK is null



  
  
      
    ) dbt_internal_test