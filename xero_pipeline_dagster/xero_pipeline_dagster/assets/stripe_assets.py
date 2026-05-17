import json
import os
from datetime import datetime, timedelta, timezone

import boto3
import psycopg2
from dagster import asset, OpExecutionContext, RetryPolicy, Backoff, Jitter

from ..resources.stripe_client import StripeClient

_retry = RetryPolicy(max_retries=3, delay=10, backoff=Backoff.EXPONENTIAL, jitter=Jitter.PLUS_MINUS)

_BUCKET = os.environ.get("S3_BUCKET", "")
_REGION = os.environ.get("S3_REGION", "ap-southeast-2")
_PREFIX = os.environ.get("S3_PREFIX", "xero-pipeline")

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


def _since() -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=_INCREMENTAL_DAYS)


# ── extract ──────────────────────────────────────────────────────────────────

@asset(retry_policy=_retry)
def stripe_balance_transactions_to_s3(context: OpExecutionContext) -> None:
    records = StripeClient().get_balance_transactions(created_after=_since())
    context.log.info(f"Fetched {len(records)} Stripe balance transactions")
    payload = [dict(r) for r in records]
    key = f"{_PREFIX}/stripe/balance_transactions/batch.json"
    _s3_client().put_object(Bucket=_BUCKET, Key=key, Body=json.dumps(payload, default=str).encode())
    context.log.info(f"Uploaded to s3://{_BUCKET}/{key}")


@asset(retry_policy=_retry)
def stripe_payouts_to_s3(context: OpExecutionContext) -> None:
    records = StripeClient().get_payouts(created_after=_since())
    context.log.info(f"Fetched {len(records)} Stripe payouts")
    payload = [dict(r) for r in records]
    key = f"{_PREFIX}/stripe/payouts/batch.json"
    _s3_client().put_object(Bucket=_BUCKET, Key=key, Body=json.dumps(payload, default=str).encode())
    context.log.info(f"Uploaded to s3://{_BUCKET}/{key}")


# ── load ─────────────────────────────────────────────────────────────────────

@asset(deps=[stripe_balance_transactions_to_s3], retry_policy=_retry)
def rds_stripe_balance_transactions(context: OpExecutionContext) -> None:
    key = f"{_PREFIX}/stripe/balance_transactions/batch.json"
    data = json.loads(_s3_client().get_object(Bucket=_BUCKET, Key=key)["Body"].read())
    context.log.info(f"Loading {len(data)} Stripe balance transactions into RDS")

    conn = _rds_conn()
    conn.autocommit = False
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stripe_balance_transactions_raw (
                id                  VARCHAR(64) PRIMARY KEY,
                amount              INTEGER,
                available_on        TIMESTAMPTZ,
                created             TIMESTAMPTZ,
                currency            VARCHAR(3),
                description         TEXT,
                exchange_rate       NUMERIC(14, 6),
                fee                 INTEGER,
                net                 INTEGER,
                reporting_category  VARCHAR(50),
                source              VARCHAR(64),
                status              VARCHAR(20),
                type                VARCHAR(50),
                fee_details         JSONB,
                loaded_at           TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        for rec in data:
            cur.execute("""
                INSERT INTO stripe_balance_transactions_raw
                    (id, amount, available_on, created, currency, description,
                     exchange_rate, fee, net, reporting_category, source,
                     status, type, fee_details)
                VALUES (%s,%s,to_timestamp(%s),to_timestamp(%s),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO UPDATE SET
                    amount             = EXCLUDED.amount,
                    available_on       = EXCLUDED.available_on,
                    currency           = EXCLUDED.currency,
                    description        = EXCLUDED.description,
                    exchange_rate      = EXCLUDED.exchange_rate,
                    fee                = EXCLUDED.fee,
                    net                = EXCLUDED.net,
                    reporting_category = EXCLUDED.reporting_category,
                    source             = EXCLUDED.source,
                    status             = EXCLUDED.status,
                    type               = EXCLUDED.type,
                    fee_details        = EXCLUDED.fee_details,
                    loaded_at          = NOW()
            """, (
                rec.get("id"),
                rec.get("amount"),
                rec.get("available_on"),
                rec.get("created"),
                rec.get("currency"),
                rec.get("description"),
                rec.get("exchange_rate"),
                rec.get("fee"),
                rec.get("net"),
                rec.get("reporting_category"),
                rec.get("source"),
                rec.get("status"),
                rec.get("type"),
                json.dumps(rec.get("fee_details", [])),
            ))
        conn.commit()
        context.log.info(f"Upserted {len(data)} balance transactions")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@asset(deps=[stripe_payouts_to_s3], retry_policy=_retry)
def rds_stripe_payouts(context: OpExecutionContext) -> None:
    key = f"{_PREFIX}/stripe/payouts/batch.json"
    data = json.loads(_s3_client().get_object(Bucket=_BUCKET, Key=key)["Body"].read())
    context.log.info(f"Loading {len(data)} Stripe payouts into RDS")

    conn = _rds_conn()
    conn.autocommit = False
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stripe_payouts_raw (
                id              VARCHAR(64) PRIMARY KEY,
                amount          INTEGER,
                arrival_date    TIMESTAMPTZ,
                created         TIMESTAMPTZ,
                currency        VARCHAR(3),
                description     TEXT,
                destination     VARCHAR(64),
                failure_code    VARCHAR(50),
                failure_message TEXT,
                method          VARCHAR(20),
                source_type     VARCHAR(30),
                status          VARCHAR(20),
                type            VARCHAR(20),
                loaded_at       TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        for rec in data:
            cur.execute("""
                INSERT INTO stripe_payouts_raw
                    (id, amount, arrival_date, created, currency, description,
                     destination, failure_code, failure_message, method,
                     source_type, status, type)
                VALUES (%s,%s,to_timestamp(%s),to_timestamp(%s),%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO UPDATE SET
                    amount          = EXCLUDED.amount,
                    arrival_date    = EXCLUDED.arrival_date,
                    currency        = EXCLUDED.currency,
                    description     = EXCLUDED.description,
                    failure_code    = EXCLUDED.failure_code,
                    failure_message = EXCLUDED.failure_message,
                    status          = EXCLUDED.status,
                    loaded_at       = NOW()
            """, (
                rec.get("id"),
                rec.get("amount"),
                rec.get("arrival_date"),
                rec.get("created"),
                rec.get("currency"),
                rec.get("description"),
                rec.get("destination"),
                rec.get("failure_code"),
                rec.get("failure_message"),
                rec.get("method"),
                rec.get("source_type"),
                rec.get("status"),
                rec.get("type"),
            ))
        conn.commit()
        context.log.info(f"Upserted {len(data)} payouts")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
