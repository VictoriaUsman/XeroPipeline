
    
    

with all_values as (

    select
        type as value_field,
        count(*) as n_records

    from "xero_smoke"."public_bronze"."bronze_xero_invoices"
    group by type

)

select *
from all_values
where value_field not in (
    'ACCREC','ACCPAY'
)


