
    
    

select
    DateID as unique_field,
    count(*) as n_records

from `sap_dev`.`silver`.`dim_date`
where DateID is not null
group by DateID
having count(*) > 1


