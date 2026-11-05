
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select WarehouseSK
from `sap_dev`.`silver`.`fact_inventory`
where WarehouseSK is null



  
  
      
    ) dbt_internal_test