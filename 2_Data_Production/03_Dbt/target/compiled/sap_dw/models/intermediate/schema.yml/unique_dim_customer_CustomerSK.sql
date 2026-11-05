
    
    

select
    CustomerSK as unique_field,
    count(*) as n_records

from `sap_dev`.`silver`.`dim_customer`
where CustomerSK is not null
group by CustomerSK
having count(*) > 1


