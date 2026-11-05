





with validation_errors as (

    select
        PurchaseID, BatchDate
    from `sap_dev`.`bronze`.`stg_purchase`
    group by PurchaseID, BatchDate
    having count(*) > 1

)

select *
from validation_errors


