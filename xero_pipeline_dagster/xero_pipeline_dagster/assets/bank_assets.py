"""
Manual bank statement CSV ingestion.

CSV format expected (comma-delimited, UTF-8 with header):
  date, description, debit, credit, balance, reference, account_name

Upload CSVs to S3 at:
  s3://<BUCKET>/<PREFIX>/bank/statements/<account_name>/YYYY-MM-DD.csv

Then trigger the `bank_statements_s3_to_rds` asset in Dagster.
"""
import csv
import io
import os

import boto3
import psycopg2
from dagster import asset, OpExecutionContext, RetryPolicy, Backoff, Jitter

_retry = RetryPolicy(max_retries=3, delay=10, backoff=Backoff.EXPONENTIAL, jitter=Jitter.PLUS_MINUS)

_BUCKET = os.environ.get("S3_BUCKET", "")
_REGION = os.environ.get("S3_REGION", "ap-southeast-2")
_PREFIX = os.environ.get("S3_PREFIX", "xero-pipeline")


def _rds_conn():
    return psycopg2.connect(
        host=os.environ["RDS_PG_HOST"],
        port=os.environ["RDS_PG_PORT"],
        dbname=os.environ["RDS_PG_DATABASE"],
        user=os.environ["RDS_PG_USER"],
        password=os.environ["RDS_PG_PASSWORD"],
    )


@asset(retry_policy=_retry)
def bank_statements_s3_to_rds(context: OpExecutionContext) -> None:
    """Scan s3://<BUCKET>/<PREFIX>/bank/statements/** and load all CSVs into RDS."""
    s3 = boto3.client("s3", region_name=_REGION)
    prefix = f"{_PREFIX}/bank/statements/"

    paginator = s3.get_paginator("list_objects_v2")
    csv_keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket=_BUCKET, Prefix=prefix)
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".csv")
    ]
    context.log.info(f"Found {len(csv_keys)} bank statement CSVs in S3")

    conn = _rds_conn()
    conn.autocommit = False
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bank_statements_raw (
                id              SERIAL PRIMARY KEY,
                statement_date  DATE NOT NULL,
                description     TEXT,
                debit           NUMERIC(14, 2),
                credit          NUMERIC(14, 2),
                balance         NUMERIC(14, 2),
                reference       TEXT,
                account_name    VARCHAR(255),
                source_file     VARCHAR(512),
                loaded_at       TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (statement_date, description, debit, credit, balance, account_name)
            )
        """)

        total = 0
        for key in csv_keys:
            obj = s3.get_object(Bucket=_BUCKET, Key=key)
            text = obj["Body"].read().decode("utf-8-sig")  # handle BOM from bank exports
            reader = csv.DictReader(io.StringIO(text))
            rows = list(reader)

            for row in rows:
                # normalise common header variants
                date_val = row.get("date") or row.get("Date") or row.get("DATE")
                desc = row.get("description") or row.get("Description") or row.get("DESCRIPTION")
                debit = row.get("debit") or row.get("Debit") or None
                credit = row.get("credit") or row.get("Credit") or None
                balance = row.get("balance") or row.get("Balance") or None
                reference = row.get("reference") or row.get("Reference") or None
                account = row.get("account_name") or row.get("Account") or key.split("/")[-2]

                cur.execute("""
                    INSERT INTO bank_statements_raw
                        (statement_date, description, debit, credit, balance, reference, account_name, source_file)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (statement_date, description, debit, credit, balance, account_name) DO NOTHING
                """, (date_val, desc, debit or None, credit or None, balance or None, reference, account, key))

            total += len(rows)
            context.log.info(f"Processed {len(rows)} rows from {key}")

        conn.commit()
        context.log.info(f"Loaded {total} bank statement rows total")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
