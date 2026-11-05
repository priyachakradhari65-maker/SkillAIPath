
    
    

select
    ProductCategorySK as unique_field,
    count(*) as n_records

from `sap_dev`.`silver`.`dim_product_category`
where ProductCategorySK is not null
group by ProductCategorySK
having count(*) > 1


