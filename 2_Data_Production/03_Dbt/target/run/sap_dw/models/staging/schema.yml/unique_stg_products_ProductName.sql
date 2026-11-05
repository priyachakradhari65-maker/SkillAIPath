
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    ProductName as unique_field,
    count(*) as n_records

from `sap_dev`.`staging`.`vw_products`
where ProductName is not null
group by ProductName
having count(*) > 1



  
  
      
    ) dbt_internal_test