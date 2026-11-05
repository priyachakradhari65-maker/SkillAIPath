





with validation_errors as (

    select
        StockID, BatchDate
    from `sap_dev`.`staging`.`vw_stock`
    group by StockID, BatchDate
    having count(*) > 1

)

select *
from validation_errors


