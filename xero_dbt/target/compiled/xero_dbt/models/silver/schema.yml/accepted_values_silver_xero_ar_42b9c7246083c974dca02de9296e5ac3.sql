
    
    

with all_values as (

    select
        aging_bucket as value_field,
        count(*) as n_records

    from "xero_smoke"."public_silver"."silver_xero_ar"
    group by aging_bucket

)

select *
from all_values
where value_field not in (
    'current','1-30','31-60','61-90','90+'
)


