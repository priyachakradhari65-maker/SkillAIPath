
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PurchaseID
from `sap_dev`.`staging`.`vw_purchases`
where PurchaseID is null



  
  
      
    ) dbt_internal_test