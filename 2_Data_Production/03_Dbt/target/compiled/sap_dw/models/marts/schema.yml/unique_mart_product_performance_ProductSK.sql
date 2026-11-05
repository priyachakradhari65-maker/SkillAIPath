
    
    

select
    ProductSK as unique_field,
    count(*) as n_records

from `sap_dev`.`gold`.`mart_product_performance`
where ProductSK is not null
group by ProductSK
having count(*) > 1


