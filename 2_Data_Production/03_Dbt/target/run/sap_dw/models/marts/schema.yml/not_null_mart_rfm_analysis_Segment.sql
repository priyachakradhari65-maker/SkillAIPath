
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select Segment
from `sap_dev`.`gold`.`mart_rfm_analysis`
where Segment is null



  
  
      
    ) dbt_internal_test