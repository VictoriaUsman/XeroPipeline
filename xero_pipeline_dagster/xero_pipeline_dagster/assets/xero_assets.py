import json
import os
from datetime import datetime, timedelta, timezone

import boto3
import psycopg2
from dagster import asset, OpExecutionContext, RetryPolicy, Backoff, Jitter

from ..resources.xero_client import XeroClient

_retry = RetryPolicy(max_retries=3, delay=10, backoff=Backoff.EXPONENTIAL, jitter=Jitter.PLUS_MINUS)

_BUCKET = os.environ.get("S3_BUCKET", "")
_REGION = os.environ.get("S3_REGION", "ap-southeast-2")
_PREFIX = os.environ.get("S3_PREFIX", "xero-pipeline")

# fetch last 8 days for incremental; accounts always full-refresh
_INCREMENTAL_DAYS = 8


def _rds_conn():
    return psycopg2.connect(
        host=os.environ["RDS_PG_HOST"],
        port=os.environ["RDS_PG_PORT"],
        dbname=os.environ["RDS_PG_DATABASE"],
        user=os.environ["RDS_PG_USER"],
        password=os.environ["RDS_PG_PASSWORD"],
    )


def _s3_client():
    return boto3.client("s3", region_name=_REGION)


def _modified_after() -> str:
    dt = datetime.now(timezone.utc) - timedelta(days=_INCREMENTAL_DAYS)
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


# ── extract ──────────────────────────────────────────────────────────────────

@asset(retry_policy=_retry)
def xero_accounts_to_s3(context: OpExecutionContext) -> None:
    records = XeroClient().get_accounts()
    context.log.info(f"Fetched {len(records)} Xero accounts")
    key = f"{_PREFIX}/xero/accounts/accounts.json"
    _s3_client().put_object(Bucket=_BUCKET, Key=key, Body=json.dumps(records).encode())
    context.log.info(f"Uploaded to s3://{_BUCKET}/{key}")


@asset(retry_policy=_retry)
def xero_bank_transactions_to_s3(context: OpExecutionContext) -> None:
    records = XeroClient().get_bank_transactions(modified_after=_modified_after())
    context.log.info(f"Fetched {len(records)} Xero bank transactions")
    key = f"{_PREFIX}/xero/bank_transactions/batch.json"
    _s3_client().put_object(Bucket=_BUCKET, Key=key, Body=json.dumps(records).encode())
    context.log.info(f"Uploaded to s3://{_BUCKET}/{key}")


@asset(retry_policy=_retry)
def xero_invoices_to_s3(context: OpExecutionContext) -> None:
    records = XeroClient().get_invoices(modified_after=_modified_after())
    context.log.info(f"Fetched {len(records)} Xero invoices")
    key = f"{_PREFIX}/xero/invoices/batch.json"
    _s3_client().put_object(Bucket=_BUCKET, Key=key, Body=json.dumps(records).encode())
    context.log.info(f"Uploaded to s3://{_BUCKET}/{key}")


# ── load ─────────────────────────────────────────────────────────────────────

@asset(deps=[xero_accounts_to_s3], retry_policy=_retry)
def rds_xero_accounts(context: OpExecutionContext) -> None:
    key = f"{_PREFIX}/xero/accounts/accounts.json"
    data = json.loads(_s3_client().get_object(Bucket=_BUCKET, Key=key)["Body"].read())
    context.log.info(f"Loading {len(data)} accounts into RDS")

    conn = _rds_conn()
    conn.autocommit = False
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS xero_accounts_raw (
                account_id   VARCHAR(64) PRIMARY KEY,
                code         VARCHAR(20),
                name         VARCHAR(255),
                type         VARCHAR(50),
                class        VARCHAR(20),
                status       VARCHAR(20),
                description  TEXT,
                tax_type     VARCHAR(50),
                updated_at   TIMESTAMPTZ,
                loaded_at    TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        for rec in data:
            cur.execute("""
                INSERT INTO xero_accounts_raw
                    (account_id, code, name, type, class, status, description, tax_type, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (account_id) DO UPDATE SET
                    code        = EXCLUDED.code,
                    name        = EXCLUDED.name,
                    type        = EXCLUDED.type,
                    class       = EXCLUDED.class,
                    status      = EXCLUDED.status,
                    description = EXCLUDED.description,
                    tax_type    = EXCLUDED.tax_type,
                    updated_at  = EXCLUDED.updated_at,
                    loaded_at   = NOW()
            """, (
                rec.get("AccountID"),
                rec.get("Code"),
                rec.get("Name"),
                rec.get("Type"),
                rec.get("Class"),
                rec.get("Status"),
                rec.get("Description"),
                rec.get("TaxType"),
                rec.get("UpdatedDateUTC"),
            ))
        conn.commit()
        context.log.info(f"Upserted {len(data)} accounts")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@asset(deps=[xero_bank_transactions_to_s3], retry_policy=_retry)
def rds_xero_bank_transactions(context: OpExecutionContext) -> None:
    key = f"{_PREFIX}/xero/bank_transactions/batch.json"
    data = json.loads(_s3_client().get_object(Bucket=_BUCKET, Key=key)["Body"].read())
    context.log.info(f"Loading {len(data)} bank transactions into RDS")

    conn = _rds_conn()
    conn.autocommit = False
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS xero_bank_transactions_raw (
                transaction_id       VARCHAR(64) PRIMARY KEY,
                type                 VARCHAR(20),
                status               VARCHAR(20),
                transaction_date     DATE,
                amount               NUMERIC(14, 2),
                bank_account_id      VARCHAR(64),
                bank_account_name    VARCHAR(255),
                reference            TEXT,
                is_reconciled        BOOLEAN,
                currency_code        VARCHAR(3),
                contact_id           VARCHAR(64),
                contact_name         VARCHAR(255),
                line_items           JSONB,
                updated_at           TIMESTAMPTZ,
                loaded_at            TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        for rec in data:
            bank = rec.get("BankAccount", {})
            contact = rec.get("Contact", {})
            cur.execute("""
                INSERT INTO xero_bank_transactions_raw
                    (transaction_id, type, status, transaction_date, amount,
                     bank_account_id, bank_account_name, reference, is_reconciled,
                     currency_code, contact_id, contact_name, line_items, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (transaction_id) DO UPDATE SET
                    status            = EXCLUDED.status,
                    transaction_date  = EXCLUDED.transaction_date,
                    amount            = EXCLUDED.amount,
                    bank_account_id   = EXCLUDED.bank_account_id,
                    bank_account_name = EXCLUDED.bank_account_name,
                    reference         = EXCLUDED.reference,
                    is_reconciled     = EXCLUDED.is_reconciled,
                    contact_id        = EXCLUDED.contact_id,
                    contact_name      = EXCLUDED.contact_name,
                    line_items        = EXCLUDED.line_items,
                    updated_at        = EXCLUDED.updated_at,
                    loaded_at         = NOW()
            """, (
                rec.get("BankTransactionID"),
                rec.get("Type"),
                rec.get("Status"),
                rec.get("DateString", "")[:10] or None,
                rec.get("Total"),
                bank.get("AccountID"),
                bank.get("Name"),
                rec.get("Reference"),
                rec.get("IsReconciled"),
                rec.get("CurrencyCode"),
                contact.get("ContactID"),
                contact.get("Name"),
                json.dumps(rec.get("LineItems", [])),
                rec.get("UpdatedDateUTC"),
            ))
        conn.commit()
        context.log.info(f"Upserted {len(data)} bank transactions")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@asset(deps=[xero_invoices_to_s3], retry_policy=_retry)
def rds_xero_invoices(context: OpExecutionContext) -> None:
    key = f"{_PREFIX}/xero/invoices/batch.json"
    data = json.loads(_s3_client().get_object(Bucket=_BUCKET, Key=key)["Body"].read())
    context.log.info(f"Loading {len(data)} invoices into RDS")

    conn = _rds_conn()
    conn.autocommit = False
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS xero_invoices_raw (
                invoice_id       VARCHAR(64) PRIMARY KEY,
                type             VARCHAR(10),
                status           VARCHAR(20),
                invoice_date     DATE,
                due_date         DATE,
                invoice_number   VARCHAR(50),
                reference        TEXT,
                contact_id       VARCHAR(64),
                contact_name     VARCHAR(255),
                sub_total        NUMERIC(14, 2),
                total_tax        NUMERIC(14, 2),
                total            NUMERIC(14, 2),
                amount_due       NUMERIC(14, 2),
                amount_paid      NUMERIC(14, 2),
                currency_code    VARCHAR(3),
                currency_rate    NUMERIC(14, 6),
                line_items       JSONB,
                updated_at       TIMESTAMPTZ,
                loaded_at        TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        for rec in data:
            contact = rec.get("Contact", {})
            cur.execute("""
                INSERT INTO xero_invoices_raw
                    (invoice_id, type, status, invoice_date, due_date, invoice_number,
                     reference, contact_id, contact_name, sub_total, total_tax, total,
                     amount_due, amount_paid, currency_code, currency_rate, line_items, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (invoice_id) DO UPDATE SET
                    status         = EXCLUDED.status,
                    invoice_date   = EXCLUDED.invoice_date,
                    due_date       = EXCLUDED.due_date,
                    contact_name   = EXCLUDED.contact_name,
                    sub_total      = EXCLUDED.sub_total,
                    total_tax      = EXCLUDED.total_tax,
                    total          = EXCLUDED.total,
                    amount_due     = EXCLUDED.amount_due,
                    amount_paid    = EXCLUDED.amount_paid,
                    currency_rate  = EXCLUDED.currency_rate,
                    line_items     = EXCLUDED.line_items,
                    updated_at     = EXCLUDED.updated_at,
                    loaded_at      = NOW()
            """, (
                rec.get("InvoiceID"),
                rec.get("Type"),
                rec.get("Status"),
                rec.get("DateString", "")[:10] or None,
                rec.get("DueDateString", "")[:10] or None,
                rec.get("InvoiceNumber"),
                rec.get("Reference"),
                contact.get("ContactID"),
                contact.get("Name"),
                rec.get("SubTotal"),
                rec.get("TotalTax"),
                rec.get("Total"),
                rec.get("AmountDue"),
                rec.get("AmountPaid"),
                rec.get("CurrencyCode"),
                rec.get("CurrencyRate"),
                json.dumps(rec.get("LineItems", [])),
                rec.get("UpdatedDateUTC"),
            ))
        conn.commit()
        context.log.info(f"Upserted {len(data)} invoices")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
