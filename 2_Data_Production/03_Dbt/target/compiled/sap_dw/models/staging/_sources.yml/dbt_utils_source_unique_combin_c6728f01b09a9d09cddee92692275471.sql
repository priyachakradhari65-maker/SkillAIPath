





with validation_errors as (

    select
        StockID, BatchDate
    from `sap_dev`.`bronze`.`stg_stock_status`
    group by StockID, BatchDate
    having count(*) > 1

)

select *
from validation_errors


