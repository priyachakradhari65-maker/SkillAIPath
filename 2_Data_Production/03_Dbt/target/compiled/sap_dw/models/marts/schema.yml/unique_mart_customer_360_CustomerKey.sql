
    
    

select
    CustomerKey as unique_field,
    count(*) as n_records

from `sap_dev`.`gold`.`mart_customer_360`
where CustomerKey is not null
group by CustomerKey
having count(*) > 1


