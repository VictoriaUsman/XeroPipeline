{{ config(materialized='table', schema='bronze') }}

SELECT
    account_id,
    code,
    name,
    type,
    class,
    status,
    description,
    tax_type,
    updated_at,
    loaded_at
FROM {{ source('raw', 'xero_accounts_raw') }}
WHERE account_id IS NOT NULL
