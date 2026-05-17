

-- Matches Stripe payouts to Xero bank transactions.
-- Match logic: same currency, amount within $0.01, date within 3 days
-- (Stripe settlement delay is typically 2 business days).
WITH stripe AS (
    SELECT
        payout_id,
        net_amount      AS stripe_net,
        currency,
        arrival_date
    FROM "xero_smoke"."public_silver"."silver_stripe_payouts"
    WHERE status = 'paid'
),

xero AS (
    SELECT
        transaction_id,
        gross_amount    AS xero_amount,
        currency_code,
        transaction_date,
        is_reconciled
    FROM "xero_smoke"."public_silver"."silver_xero_bank_transactions"
    WHERE type = 'RECEIVE'
)

SELECT
    s.payout_id,
    x.transaction_id          AS xero_transaction_id,
    s.stripe_net,
    x.xero_amount,
    ABS(s.stripe_net - x.xero_amount)    AS amount_diff,
    s.arrival_date                        AS stripe_arrival_date,
    x.transaction_date                    AS xero_date,
    ABS(s.arrival_date - x.transaction_date) AS date_diff_days,
    x.is_reconciled,
    CASE
        WHEN x.transaction_id IS NOT NULL THEN 'matched'
        ELSE 'unmatched'
    END AS match_status
FROM stripe s
LEFT JOIN xero x
    ON  x.currency_code = s.currency
    AND ABS(x.xero_amount - s.stripe_net) < 0.01
    AND ABS(x.transaction_date - s.arrival_date) <= 3