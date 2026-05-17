{{ config(materialized='view', schema='gold') }}

-- ─────────────────────────────────────────────────────────────────────────────
-- CEO / CFO Weekly Finance Dashboard
-- Query this view every Monday after the pipeline runs.
--
-- Sections:
--   1. cash         – bank balances + Stripe float
--   2. ar_summary   – total AR due, overdue breakdown
--   3. ap_summary   – total AP due
--   4. revenue_wtd  – gross revenue this week
--   5. stripe_wtd   – Stripe gross / fees / net this week
--   6. recon_status – % of bank transactions reconciled
-- ─────────────────────────────────────────────────────────────────────────────

WITH

-- 1. cash
cash AS (
    SELECT
        'cash'                          AS section,
        'Total Xero Cash'               AS metric,
        SUM(CASE WHEN source = 'xero'   THEN total_cash END)::TEXT AS value
    FROM {{ ref('gold_cash_position') }}
    UNION ALL
    SELECT 'cash', 'Stripe Float (In-Transit)',
        SUM(CASE WHEN source = 'stripe_pending' THEN total_cash END)::TEXT
    FROM {{ ref('gold_cash_position') }}
),

-- 2. AR
ar AS (
    SELECT
        'ar_summary'                    AS section,
        'Total AR Outstanding ($)'      AS metric,
        ROUND(SUM(total_outstanding), 2)::TEXT AS value
    FROM {{ ref('gold_ar_aging') }}
    UNION ALL
    SELECT 'ar_summary', 'AR Overdue 1-30 ($)',
        ROUND(SUM(total_outstanding) FILTER (WHERE aging_bucket = '1-30'), 2)::TEXT
    FROM {{ ref('gold_ar_aging') }}
    UNION ALL
    SELECT 'ar_summary', 'AR Overdue 31-60 ($)',
        ROUND(SUM(total_outstanding) FILTER (WHERE aging_bucket = '31-60'), 2)::TEXT
    FROM {{ ref('gold_ar_aging') }}
    UNION ALL
    SELECT 'ar_summary', 'AR Overdue 60+ ($)',
        ROUND(
            SUM(total_outstanding) FILTER (WHERE aging_bucket IN ('61-90', '90+')), 2
        )::TEXT
    FROM {{ ref('gold_ar_aging') }}
    UNION ALL
    SELECT 'ar_summary', 'Open AR Invoices (count)',
        SUM(invoice_count)::TEXT
    FROM {{ ref('gold_ar_aging') }}
),

-- 3. AP
ap AS (
    SELECT
        'ap_summary'                    AS section,
        'Total AP Outstanding ($)'      AS metric,
        ROUND(SUM(amount_due), 2)::TEXT AS value
    FROM {{ ref('silver_xero_ap') }}
    WHERE status = 'AUTHORISED' AND amount_due > 0
    UNION ALL
    SELECT 'ap_summary', 'AP Overdue Bills (count)',
        COUNT(*)::TEXT
    FROM {{ ref('silver_xero_ap') }}
    WHERE status = 'AUTHORISED' AND amount_due > 0 AND days_overdue > 0
),

-- 4. Revenue WTD (from invoiced AR raised this week)
revenue AS (
    SELECT
        'revenue_wtd'                   AS section,
        'Gross Revenue Invoiced WTD ($)' AS metric,
        ROUND(SUM(amount) FILTER (WHERE source = 'invoice'), 2)::TEXT AS value
    FROM {{ ref('gold_revenue_by_type') }}
    UNION ALL
    SELECT 'revenue_wtd', 'Cash Receipts WTD ($)',
        ROUND(SUM(amount) FILTER (WHERE source = 'bank_transaction'), 2)::TEXT
    FROM {{ ref('gold_revenue_by_type') }}
),

-- 5. Stripe WTD
stripe AS (
    SELECT
        'stripe_wtd'                    AS section,
        'Stripe Gross Charges WTD ($)'  AS metric,
        ROUND(SUM(gross_amount) FILTER (WHERE reporting_category = 'charge'), 2)::TEXT AS value
    FROM {{ ref('gold_stripe_movement') }}
    UNION ALL
    SELECT 'stripe_wtd', 'Stripe Fees WTD ($)',
        ROUND(ABS(SUM(total_fees)), 2)::TEXT
    FROM {{ ref('gold_stripe_movement') }}
    UNION ALL
    SELECT 'stripe_wtd', 'Stripe Refunds WTD ($)',
        ROUND(ABS(SUM(gross_amount)) FILTER (WHERE reporting_category = 'refund'), 2)::TEXT
    FROM {{ ref('gold_stripe_movement') }}
    UNION ALL
    SELECT 'stripe_wtd', 'Stripe Net WTD ($)',
        ROUND(SUM(net_amount), 2)::TEXT
    FROM {{ ref('gold_stripe_movement') }}
),

-- 6. Reconciliation status (all-time, last 90 days)
recon AS (
    SELECT
        'recon_status'                  AS section,
        'Bank Txns Reconciled % (90d)'  AS metric,
        ROUND(
            100.0 * SUM(is_reconciled::INT) / NULLIF(COUNT(*), 0), 1
        )::TEXT AS value
    FROM {{ ref('silver_xero_bank_transactions') }}
    WHERE transaction_date >= CURRENT_DATE - 90
    UNION ALL
    SELECT 'recon_status', 'Stripe Payouts Matched %',
        ROUND(
            100.0 * SUM(CASE WHEN match_status = 'matched' THEN 1 END) / NULLIF(COUNT(*), 0), 1
        )::TEXT
    FROM {{ ref('silver_stripe_reconciliation') }}
)

SELECT
    section,
    metric,
    COALESCE(value, '0')    AS value,
    CURRENT_DATE            AS report_date
FROM (
    SELECT * FROM cash
    UNION ALL SELECT * FROM ar
    UNION ALL SELECT * FROM ap
    UNION ALL SELECT * FROM revenue
    UNION ALL SELECT * FROM stripe
    UNION ALL SELECT * FROM recon
) combined
ORDER BY section, metric
