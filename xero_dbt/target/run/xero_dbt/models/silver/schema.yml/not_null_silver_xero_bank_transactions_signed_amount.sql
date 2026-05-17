
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select signed_amount
from "xero_smoke"."public_silver"."silver_xero_bank_transactions"
where signed_amount is null



  
  
      
    ) dbt_internal_test