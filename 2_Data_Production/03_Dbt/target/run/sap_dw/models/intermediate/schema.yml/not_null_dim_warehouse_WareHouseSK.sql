
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select WareHouseSK
from `sap_dev`.`silver`.`dim_warehouse`
where WareHouseSK is null



  
  
      
    ) dbt_internal_test