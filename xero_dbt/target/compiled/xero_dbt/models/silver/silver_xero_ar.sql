

-- AR = ACCREC invoices that are AUTHORISED or PAID
SELECT
    invoice_id,
    invoice_number,
    reference,
    invoice_date,
    due_date,
    CURRENT_DATE - due_date                          AS days_overdue,
    CASE
        WHEN CURRENT_DATE <= due_date               THEN 'current'
        WHEN CURRENT_DATE - due_date BETWEEN 1 AND 30  THEN '1-30'
        WHEN CURRENT_DATE - due_date BETWEEN 31 AND 60 THEN '31-60'
        WHEN CURRENT_DATE - due_date BETWEEN 61 AND 90 THEN '61-90'
        ELSE '90+'
    END                                              AS aging_bucket,
    status,
    contact_id,
    contact_name,
    sub_total,
    total_tax,
    total,
    amount_due,
    amount_paid,
    currency_code,
    updated_at
FROM "xero_smoke"."public_bronze"."bronze_xero_invoices"
WHERE type = 'ACCREC'
  AND status IN ('AUTHORISED', 'PAID')