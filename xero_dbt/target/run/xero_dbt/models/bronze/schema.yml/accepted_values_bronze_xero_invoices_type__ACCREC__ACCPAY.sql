
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

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



  
  
      
    ) dbt_internal_test