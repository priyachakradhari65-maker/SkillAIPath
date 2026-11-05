
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select TotalCost
from `sap_dev`.`staging`.`vw_purchases`
where TotalCost is null



  
  
      
    ) dbt_internal_test