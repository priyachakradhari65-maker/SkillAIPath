
    
    

with all_values as (

    select
        CustomerSegment as value_field,
        count(*) as n_records

    from `sap_dev`.`gold`.`mart_customer_360`
    group by CustomerSegment

)

select *
from all_values
where value_field not in (
    'New Customer','Returning Customer','VIP Customer'
)


