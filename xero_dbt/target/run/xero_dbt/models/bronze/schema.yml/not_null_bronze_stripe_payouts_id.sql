
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select id
from "xero_smoke"."public_bronze"."bronze_stripe_payouts"
where id is null



  
  
      
    ) dbt_internal_test