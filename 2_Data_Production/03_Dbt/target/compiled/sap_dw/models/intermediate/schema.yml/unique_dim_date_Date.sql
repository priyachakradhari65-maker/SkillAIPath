
    
    

select
    Date as unique_field,
    count(*) as n_records

from `sap_dev`.`silver`.`dim_date`
where Date is not null
group by Date
having count(*) > 1


