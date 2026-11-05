
    
    

with all_values as (

    select
        PerformanceCategory as value_field,
        count(*) as n_records

    from `sap_dev`.`gold`.`mart_product_performance`
    group by PerformanceCategory

)

select *
from all_values
where value_field not in (
    'Best Seller','Popular','Moderate','Slow Mover'
)


