
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        OrderID, LoadedAt
    from `sap_dev`.`silver`.`int_sales_enriched`
    group by OrderID, LoadedAt
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test