
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select FinalPrice
from `sap_dev`.`staging`.`vw_sales`
where FinalPrice is null



  
  
      
    ) dbt_internal_test