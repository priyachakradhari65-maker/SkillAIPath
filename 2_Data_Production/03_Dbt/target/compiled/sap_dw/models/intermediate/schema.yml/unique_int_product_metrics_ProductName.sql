
    
    

select
    ProductName as unique_field,
    count(*) as n_records

from `sap_dev`.`silver`.`int_product_metrics`
where ProductName is not null
group by ProductName
having count(*) > 1


