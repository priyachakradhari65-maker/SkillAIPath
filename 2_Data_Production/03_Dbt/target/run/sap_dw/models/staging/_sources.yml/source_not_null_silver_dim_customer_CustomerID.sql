
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CustomerID
from `sap_dev`.`silver`.`dim_customer`
where CustomerID is null



  
  
      
    ) dbt_internal_test