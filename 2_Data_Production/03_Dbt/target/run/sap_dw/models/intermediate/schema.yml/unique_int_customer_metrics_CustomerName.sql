
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    CustomerName as unique_field,
    count(*) as n_records

from `sap_dev`.`silver`.`int_customer_metrics`
where CustomerName is not null
group by CustomerName
having count(*) > 1



  
  
      
    ) dbt_internal_test