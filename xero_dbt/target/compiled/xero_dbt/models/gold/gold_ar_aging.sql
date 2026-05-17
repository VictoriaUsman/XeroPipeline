

SELECT
    aging_bucket,
    COUNT(*)                AS invoice_count,
    SUM(amount_due)         AS total_outstanding,
    SUM(total)              AS total_invoiced,
    ROUND(
        SUM(amount_due) / NULLIF(SUM(total), 0) * 100,
        1
    )                       AS pct_outstanding,
    MIN(due_date)           AS oldest_due_date,
    ARRAY_AGG(contact_name ORDER BY amount_due DESC)
        FILTER (WHERE amount_due > 0)
                            AS top_customers
FROM "xero_smoke"."public_silver"."silver_xero_ar"
WHERE status = 'AUTHORISED'
  AND amount_due > 0
GROUP BY aging_bucket
ORDER BY
    CASE aging_bucket
        WHEN 'current' THEN 1
        WHEN '1-30'    THEN 2
        WHEN '31-60'   THEN 3
        WHEN '61-90'   THEN 4
        WHEN '90+'     THEN 5
    END