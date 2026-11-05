
    
    

select
    CustomerName as unique_field,
    count(*) as n_records

from `sap_dev`.`staging`.`vw_customers`
where CustomerName is not null
group by CustomerName
having count(*) > 1


