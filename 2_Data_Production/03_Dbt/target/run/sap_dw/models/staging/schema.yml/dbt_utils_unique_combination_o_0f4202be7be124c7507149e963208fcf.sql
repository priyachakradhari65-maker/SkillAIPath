
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        StockID, BatchDate
    from `sap_dev`.`staging`.`vw_stock`
    group by StockID, BatchDate
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test