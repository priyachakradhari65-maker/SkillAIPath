
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select StockID
from `sap_dev`.`bronze`.`stg_stock_status`
where StockID is null



  
  
      
    ) dbt_internal_test