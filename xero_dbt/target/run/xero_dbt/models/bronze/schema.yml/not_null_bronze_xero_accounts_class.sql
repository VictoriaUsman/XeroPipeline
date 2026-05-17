
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select class
from "xero_smoke"."public_bronze"."bronze_xero_accounts"
where class is null



  
  
      
    ) dbt_internal_test