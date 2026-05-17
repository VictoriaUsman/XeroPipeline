
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select transaction_date
from "xero_smoke"."public_bronze"."bronze_xero_bank_transactions"
where transaction_date is null



  
  
      
    ) dbt_internal_test