
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    ProductSK as unique_field,
    count(*) as n_records

from `sap_dev`.`silver`.`dim_product`
where ProductSK is not null
group by ProductSK
having count(*) > 1



  
  
      
    ) dbt_internal_test