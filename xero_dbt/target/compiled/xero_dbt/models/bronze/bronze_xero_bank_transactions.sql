

SELECT
    transaction_id,
    type,
    status,
    transaction_date,
    amount,
    bank_account_id,
    bank_account_name,
    reference,
    is_reconciled,
    currency_code,
    contact_id,
    contact_name,
    line_items,
    updated_at,
    loaded_at
FROM "xero_smoke"."public"."xero_bank_transactions_raw"
WHERE transaction_id IS NOT NULL
  AND status != 'DELETED'