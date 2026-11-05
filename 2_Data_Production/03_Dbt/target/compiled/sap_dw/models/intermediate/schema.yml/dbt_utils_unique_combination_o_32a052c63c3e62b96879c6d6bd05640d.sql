





with validation_errors as (

    select
        OrderID, BatchDate
    from `sap_dev`.`silver`.`fact_sales`
    group by OrderID, BatchDate
    having count(*) > 1

)

select *
from validation_errors


