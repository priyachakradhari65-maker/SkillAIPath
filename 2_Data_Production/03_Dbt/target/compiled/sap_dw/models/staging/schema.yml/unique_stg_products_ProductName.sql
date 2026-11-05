
    
    

select
    ProductName as unique_field,
    count(*) as n_records

from `sap_dev`.`staging`.`vw_products`
where ProductName is not null
group by ProductName
having count(*) > 1


