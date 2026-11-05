
    
    

select
    ProductKey as unique_field,
    count(*) as n_records

from `sap_dev`.`staging`.`vw_products`
where ProductKey is not null
group by ProductKey
having count(*) > 1


