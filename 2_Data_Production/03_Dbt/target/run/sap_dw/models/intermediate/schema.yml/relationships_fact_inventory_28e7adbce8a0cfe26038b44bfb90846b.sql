
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select WarehouseSK as from_field
    from `sap_dev`.`silver`.`fact_inventory`
    where WarehouseSK is not null
),

parent as (
    select WareHouseSK as to_field
    from `sap_dev`.`silver`.`dim_warehouse`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test