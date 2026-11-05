





with validation_errors as (

    select
        OrderID, BatchDate
    from `sap_dev`.`silver`.`int_sales_enriched`
    group by OrderID, BatchDate
    having count(*) > 1

)

select *
from validation_errors


