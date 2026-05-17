
  
    

  create  table "xero_smoke"."public_silver"."silver_xero_ap__dbt_tmp"
  
  
    as
  
  (
    

-- AP = ACCPAY invoices that are AUTHORISED or PAID
SELECT
    invoice_id,
    invoice_number,
    reference,
    invoice_date,
    due_date,
    CURRENT_DATE - due_date                          AS days_overdue,
    CASE
        WHEN due_date IS NULL OR CURRENT_DATE <= due_date THEN 'not_due'
        WHEN CURRENT_DATE - due_date BETWEEN 1 AND 30     THEN '1-30'
        WHEN CURRENT_DATE - due_date BETWEEN 31 AND 60    THEN '31-60'
        ELSE '60+'
    END                                              AS aging_bucket,
    status,
    contact_id,
    contact_name,
    sub_total,
    total_tax,
    total,
    amount_due,
    amount_paid,
    currency_code,
    updated_at
FROM "xero_smoke"."public_bronze"."bronze_xero_invoices"
WHERE type = 'ACCPAY'
  AND status IN ('AUTHORISED', 'PAID')
  );
  