
    
    

select
    CustomerKey as unique_field,
    count(*) as n_records

from `sap_dev`.`staging`.`vw_customers`
where CustomerKey is not null
group by CustomerKey
having count(*) > 1


