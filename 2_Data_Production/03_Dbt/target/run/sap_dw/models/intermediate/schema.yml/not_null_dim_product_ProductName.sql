
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ProductName
from `sap_dev`.`silver`.`dim_product`
where ProductName is null



  
  
      
    ) dbt_internal_test