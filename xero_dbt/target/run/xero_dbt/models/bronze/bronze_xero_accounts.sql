
  
    

  create  table "xero_smoke"."public_bronze"."bronze_xero_accounts__dbt_tmp"
  
  
    as
  
  (
    

SELECT
    account_id,
    code,
    name,
    type,
    class,
    status,
    description,
    tax_type,
    updated_at,
    loaded_at
FROM "xero_smoke"."public"."xero_accounts_raw"
WHERE account_id IS NOT NULL
  );
  