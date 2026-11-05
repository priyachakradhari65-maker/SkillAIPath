
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CustomerSK
from `sap_dev`.`gold`.`mart_rfm_analysis`
where CustomerSK is null



  
  
      
    ) dbt_internal_test