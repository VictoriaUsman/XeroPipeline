

-- Current cash balance by bank account (from Xero bank transactions cumulative sum)
-- and pending Stripe float (payouts in_transit)
WITH xero_cash AS (
    SELECT
        bank_account_id,
        bank_account_name,
        currency_code,
        SUM(signed_amount)  AS balance
    FROM "xero_smoke"."public_silver"."silver_xero_bank_transactions"
    GROUP BY 1, 2, 3
),

stripe_float AS (
    SELECT
        currency,
        SUM(amount)         AS pending_amount
    FROM "xero_smoke"."public_bronze"."bronze_stripe_payouts"
    WHERE status = 'in_transit'
    GROUP BY 1
)

SELECT
    'xero'                  AS source,
    x.bank_account_name     AS account_name,
    x.currency_code         AS currency,
    x.balance,
    NULL::NUMERIC           AS stripe_pending,
    x.balance               AS total_cash
FROM xero_cash x

UNION ALL

SELECT
    'stripe_pending',
    'Stripe In-Transit',
    s.currency,
    NULL,
    s.pending_amount,
    s.pending_amount
FROM stripe_float s