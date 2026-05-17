{{ config(materialized='view', schema='gold') }}

-- WTD Stripe activity broken down by reporting category
WITH week_start AS (
    SELECT DATE_TRUNC('week', CURRENT_DATE)::DATE AS wk
)

SELECT
    bt.reporting_category,
    bt.currency,
    COUNT(*)            AS transaction_count,
    SUM(bt.amount)      AS gross_amount,
    SUM(bt.fee)         AS total_fees,
    SUM(bt.net)         AS net_amount,
    MIN(bt.created)     AS earliest,
    MAX(bt.created)     AS latest
FROM {{ ref('bronze_stripe_balance_transactions') }} bt
CROSS JOIN week_start w
WHERE bt.created::DATE >= w.wk
GROUP BY 1, 2
ORDER BY ABS(SUM(bt.net)) DESC
