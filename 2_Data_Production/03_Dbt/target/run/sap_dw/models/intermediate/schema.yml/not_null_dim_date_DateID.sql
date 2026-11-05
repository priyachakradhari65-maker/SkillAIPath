
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DateID
from `sap_dev`.`silver`.`dim_date`
where DateID is null



  
  
      
    ) dbt_internal_test