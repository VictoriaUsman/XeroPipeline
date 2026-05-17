
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select payout_id
from "xero_smoke"."public_silver"."silver_stripe_reconciliation"
where payout_id is null



  
  
      
    ) dbt_internal_test