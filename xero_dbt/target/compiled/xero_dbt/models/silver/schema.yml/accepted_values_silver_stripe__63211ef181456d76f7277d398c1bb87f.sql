
    
    

with all_values as (

    select
        match_status as value_field,
        count(*) as n_records

    from "xero_smoke"."public_silver"."silver_stripe_reconciliation"
    group by match_status

)

select *
from all_values
where value_field not in (
    'matched','unmatched'
)


