
    
    

with all_values as (

    select
        M_Score as value_field,
        count(*) as n_records

    from `sap_dev`.`gold`.`mart_rfm_analysis`
    group by M_Score

)

select *
from all_values
where value_field not in (
    '1','2','3','4','5'
)


