
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

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



  
  
      
    ) dbt_internal_test