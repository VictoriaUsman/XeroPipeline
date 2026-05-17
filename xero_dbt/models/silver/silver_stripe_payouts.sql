{{ config(materialized='table', schema='silver') }}

SELECT
    p.id                        AS payout_id,
    p.amount,
    p.currency,
    p.arrival_date::DATE        AS arrival_date,
    p.created::DATE             AS created_date,
    p.description,
    p.status,
    p.method,
    p.source_type,
    p.failure_code,
    p.failure_message,
    -- gross / fee / net from the matching balance transaction
    COALESCE(bt.amount, p.amount)  AS gross_amount,
    COALESCE(bt.fee,    0)         AS stripe_fee,
    COALESCE(bt.net,    p.amount)  AS net_amount
FROM {{ ref('bronze_stripe_payouts') }}            p
LEFT JOIN {{ ref('bronze_stripe_balance_transactions') }} bt
    ON bt.source = p.id
   AND bt.type   = 'payout'
