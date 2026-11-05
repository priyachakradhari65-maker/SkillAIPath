
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        PurchaseID, BatchDate
    from `sap_dev`.`staging`.`vw_purchases`
    group by PurchaseID, BatchDate
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test