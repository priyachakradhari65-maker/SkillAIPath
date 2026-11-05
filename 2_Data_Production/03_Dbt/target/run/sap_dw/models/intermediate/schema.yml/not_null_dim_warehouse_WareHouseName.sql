
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select WareHouseName
from `sap_dev`.`silver`.`dim_warehouse`
where WareHouseName is null



  
  
      
    ) dbt_internal_test