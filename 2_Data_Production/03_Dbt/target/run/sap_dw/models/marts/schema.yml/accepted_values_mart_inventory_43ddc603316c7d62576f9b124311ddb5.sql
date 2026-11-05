
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        HealthStatus as value_field,
        count(*) as n_records

    from `sap_dev`.`gold`.`mart_inventory_health`
    group by HealthStatus

)

select *
from all_values
where value_field not in (
    'Understock','Overstock','Healthy'
)



  
  
      
    ) dbt_internal_test