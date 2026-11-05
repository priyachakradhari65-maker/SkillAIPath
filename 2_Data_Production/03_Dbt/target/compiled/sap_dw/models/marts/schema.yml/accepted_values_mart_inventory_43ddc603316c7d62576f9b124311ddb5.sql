
    
    

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


