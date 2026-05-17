{{ config(materialized='table', schema='bronze') }}

SELECT
    id,
    amount       / 100.0 AS amount,
    arrival_date,
    created,
    currency,
    description,
    destination,
    failure_code,
    failure_message,
    method,
    source_type,
    status,
    type,
    loaded_at
FROM {{ source('raw', 'stripe_payouts_raw') }}
WHERE id IS NOT NULL
