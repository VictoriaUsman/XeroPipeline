{{ config(materialized='view', schema='gold') }}

-- Weekly revenue breakdown from Xero bank transactions (INCOME class accounts)
-- and from invoices raised this week.
-- "WTD" = week starting Monday of the current week.
WITH week_start AS (
    SELECT DATE_TRUNC('week', CURRENT_DATE)::DATE AS wk
),

bank_income AS (
    SELECT
        t.bank_account_name  AS revenue_account,
        t.account_class,
        SUM(t.signed_amount) AS amount,
        COUNT(*)             AS transaction_count
    FROM {{ ref('silver_xero_bank_transactions') }} t
    CROSS JOIN week_start w
    WHERE t.account_class = 'INCOME'
      AND t.transaction_date >= w.wk
    GROUP BY 1, 2
),

invoice_revenue AS (
    SELECT
        'Invoiced Revenue'   AS revenue_account,
        'INCOME'             AS account_class,
        SUM(total)           AS amount,
        COUNT(*)             AS transaction_count
    FROM {{ ref('silver_xero_ar') }}
    CROSS JOIN week_start w
    WHERE invoice_date >= w.wk
)

SELECT
    revenue_account,
    account_class,
    amount,
    transaction_count,
    'bank_transaction' AS source,
    (SELECT wk FROM week_start) AS week_starting
FROM bank_income

UNION ALL

SELECT
    revenue_account,
    account_class,
    amount,
    transaction_count,
    'invoice',
    (SELECT wk FROM week_start)
FROM invoice_revenue
ORDER BY amount DESC
