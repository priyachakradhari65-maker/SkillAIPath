
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    DateID as unique_field,
    count(*) as n_records

from `sap_dev`.`silver`.`dim_date`
where DateID is not null
group by DateID
having count(*) > 1



  
  
      
    ) dbt_internal_test