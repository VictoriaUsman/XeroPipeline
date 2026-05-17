{{ config(materialized='table', schema='silver') }}

SELECT
    t.transaction_id,
    t.type,
    t.status,
    t.transaction_date,
    -- normalize: RECEIVE is cash in, SPEND is cash out
    CASE t.type
        WHEN 'RECEIVE' THEN  t.amount
        WHEN 'SPEND'   THEN -t.amount
        ELSE t.amount
    END                          AS signed_amount,
    t.amount                     AS gross_amount,
    t.bank_account_id,
    t.bank_account_name,
    t.reference,
    t.is_reconciled,
    t.currency_code,
    t.contact_id,
    t.contact_name,
    a.code                       AS account_code,
    a.name                       AS account_name,
    a.class                      AS account_class,
    t.line_items,
    t.updated_at
FROM {{ ref('bronze_xero_bank_transactions') }}  t
LEFT JOIN {{ ref('bronze_xero_accounts') }}      a
    ON t.bank_account_id = a.account_id
WHERE t.status = 'AUTHORISED'
