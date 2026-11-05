
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    CustomerSK as unique_field,
    count(*) as n_records

from `sap_dev`.`silver`.`dim_customer`
where CustomerSK is not null
group by CustomerSK
having count(*) > 1



  
  
      
    ) dbt_internal_test