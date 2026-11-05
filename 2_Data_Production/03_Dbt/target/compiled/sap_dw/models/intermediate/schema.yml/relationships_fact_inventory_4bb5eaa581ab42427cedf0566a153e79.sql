
    
    

with child as (
    select ProductSK as from_field
    from `sap_dev`.`silver`.`fact_inventory`
    where ProductSK is not null
),

parent as (
    select ProductSK as to_field
    from `sap_dev`.`silver`.`dim_product`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


