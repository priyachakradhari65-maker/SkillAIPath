
    
    

with all_values as (

    select
        Segment as value_field,
        count(*) as n_records

    from `sap_dev`.`gold`.`mart_rfm_analysis`
    group by Segment

)

select *
from all_values
where value_field not in (
    'Champions','Loyal Customers','Recent Customers','At Risk','Cant Lose Them','Promising','Potential Loyalist','New Customers','Needs Attention','About to Sleep','Hibernating','Lost Customers','Others'
)


