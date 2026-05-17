
    
    

select
    id as unique_field,
    count(*) as n_records

from "xero_smoke"."public_bronze"."bronze_stripe_balance_transactions"
where id is not null
group by id
having count(*) > 1


