
    
    

select
    transaction_id as unique_field,
    count(*) as n_records

from "xero_smoke"."public_bronze"."bronze_xero_bank_transactions"
where transaction_id is not null
group by transaction_id
having count(*) > 1


