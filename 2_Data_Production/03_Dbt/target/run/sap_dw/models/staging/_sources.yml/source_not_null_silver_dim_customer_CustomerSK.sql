
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CustomerSK
from `sap_dev`.`silver`.`dim_customer`
where CustomerSK is null



  
  
      
    ) dbt_internal_test