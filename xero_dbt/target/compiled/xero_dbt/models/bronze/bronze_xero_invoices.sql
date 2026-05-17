

SELECT
    invoice_id,
    type,
    status,
    invoice_date,
    due_date,
    invoice_number,
    reference,
    contact_id,
    contact_name,
    sub_total,
    total_tax,
    total,
    amount_due,
    amount_paid,
    currency_code,
    currency_rate,
    line_items,
    updated_at,
    loaded_at
FROM "xero_smoke"."public"."xero_invoices_raw"
WHERE invoice_id IS NOT NULL
  AND status NOT IN ('DELETED', 'VOIDED')