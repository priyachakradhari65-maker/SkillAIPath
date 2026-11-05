
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PurchaseID
from `sap_dev`.`bronze`.`stg_purchase`
where PurchaseID is null



  
  
      
    ) dbt_internal_test