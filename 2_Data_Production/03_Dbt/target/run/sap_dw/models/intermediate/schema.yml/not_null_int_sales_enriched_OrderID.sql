
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select OrderID
from `sap_dev`.`silver`.`int_sales_enriched`
where OrderID is null



  
  
      
    ) dbt_internal_test