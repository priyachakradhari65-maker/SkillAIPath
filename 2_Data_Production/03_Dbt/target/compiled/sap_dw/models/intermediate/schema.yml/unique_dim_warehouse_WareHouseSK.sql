
    
    

select
    WareHouseSK as unique_field,
    count(*) as n_records

from `sap_dev`.`silver`.`dim_warehouse`
where WareHouseSK is not null
group by WareHouseSK
having count(*) > 1


