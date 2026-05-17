
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    payout_id as unique_field,
    count(*) as n_records

from "xero_smoke"."public_silver"."silver_stripe_payouts"
where payout_id is not null
group by payout_id
having count(*) > 1



  
  
      
    ) dbt_internal_test