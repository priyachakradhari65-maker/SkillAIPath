





with validation_errors as (

    select
        PurchaseID, BatchDate
    from `sap_dev`.`staging`.`vw_purchases`
    group by PurchaseID, BatchDate
    having count(*) > 1

)

select *
from validation_errors


