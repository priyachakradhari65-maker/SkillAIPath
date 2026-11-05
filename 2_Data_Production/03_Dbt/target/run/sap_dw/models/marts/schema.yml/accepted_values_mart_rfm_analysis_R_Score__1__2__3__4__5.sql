
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        R_Score as value_field,
        count(*) as n_records

    from `sap_dev`.`gold`.`mart_rfm_analysis`
    group by R_Score

)

select *
from all_values
where value_field not in (
    '1','2','3','4','5'
)



  
  
      
    ) dbt_internal_test