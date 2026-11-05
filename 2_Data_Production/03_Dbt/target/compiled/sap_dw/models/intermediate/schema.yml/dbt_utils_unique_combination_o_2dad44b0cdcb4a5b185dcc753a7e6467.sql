





with validation_errors as (

    select
        OrderID, LoadedAt
    from `sap_dev`.`silver`.`int_sales_enriched`
    group by OrderID, LoadedAt
    having count(*) > 1

)

select *
from validation_errors


