

SELECT
    id,
    -- Stripe stores amounts in cents; convert to dollars
    amount       / 100.0 AS amount,
    fee          / 100.0 AS fee,
    net          / 100.0 AS net,
    available_on,
    created,
    currency,
    description,
    exchange_rate,
    reporting_category,
    source,
    status,
    type,
    fee_details,
    loaded_at
FROM "xero_smoke"."public"."stripe_balance_transactions_raw"
WHERE id IS NOT NULL