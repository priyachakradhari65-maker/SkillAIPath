
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select WarehouseSK
from `sap_dev`.`gold`.`mart_inventory_health`
where WarehouseSK is null



  
  
      
    ) dbt_internal_test