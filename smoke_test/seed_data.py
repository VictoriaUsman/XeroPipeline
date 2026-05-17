"""
Smoke test seed — populates all raw tables with realistic fake data.
Run after docker-compose smoke postgres is up.
"""
import json
import random
import uuid
from datetime import date, timedelta

import psycopg2

TODAY = date.today()


def d(offset: int) -> date:
    return TODAY + timedelta(days=offset)


def conn():
    return psycopg2.connect(
        host="localhost", port=5433,
        dbname="xero_smoke", user="xero", password="xero_smoke_pw",
    )


# ── helpers ──────────────────────────────────────────────────────────────────

def xid() -> str:
    return str(uuid.uuid4())


def sid() -> str:
    return "bt_" + uuid.uuid4().hex[:16]


def payout_id() -> str:
    return "po_" + uuid.uuid4().hex[:16]


# ── fixtures ─────────────────────────────────────────────────────────────────

ACCOUNTS = [
    (xid(), "1000", "Business Cheque Account",    "BANK",      "ASSET",     "ACTIVE"),
    (xid(), "1010", "Savings Account",             "BANK",      "ASSET",     "ACTIVE"),
    (xid(), "2000", "Accounts Payable",            "CREDITORS", "LIABILITY", "ACTIVE"),
    (xid(), "4000", "SaaS Subscription Revenue",   "REVENUE",   "INCOME",    "ACTIVE"),
    (xid(), "4010", "Professional Services",       "REVENUE",   "INCOME",    "ACTIVE"),
    (xid(), "4020", "Usage Revenue",               "REVENUE",   "INCOME",    "ACTIVE"),
    (xid(), "5000", "AWS / Hosting Costs",         "DIRECTCOSTS","EXPENSE",  "ACTIVE"),
    (xid(), "5010", "Contractor Costs",            "DIRECTCOSTS","EXPENSE",  "ACTIVE"),
    (xid(), "6000", "Salaries & Wages",            "EXPENSE",   "EXPENSE",   "ACTIVE"),
    (xid(), "6010", "Software Subscriptions",      "EXPENSE",   "EXPENSE",   "ACTIVE"),
]

# grab bank account IDs for transactions
BANK_ACC_ID   = ACCOUNTS[0][0]
BANK_ACC_NAME = ACCOUNTS[0][2]

CUSTOMERS = [
    ("Acme Corp",        xid()),
    ("Blue Sky SaaS",    xid()),
    ("Globex Industries",xid()),
    ("Initech Ltd",      xid()),
    ("Umbrella Co",      xid()),
]

VENDORS = [
    ("AWS",              xid()),
    ("Cloudflare",       xid()),
    ("Dev Studio Ltd",   xid()),
]


# ── seed functions ────────────────────────────────────────────────────────────

def seed_accounts(cur):
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
    for acc_id, code, name, type_, class_, status in ACCOUNTS:
        cur.execute("""
            INSERT INTO xero_accounts_raw (account_id, code, name, type, class, status, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT DO NOTHING
        """, (acc_id, code, name, type_, class_, status))
    print(f"  seeded {len(ACCOUNTS)} accounts")


def seed_bank_transactions(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS xero_bank_transactions_raw (
            transaction_id       VARCHAR(64) PRIMARY KEY,
            type                 VARCHAR(20),
            status               VARCHAR(20),
            transaction_date     DATE,
            amount               NUMERIC(14,2),
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
    rows = []

    # Cash receipts from customers (RECEIVE) — last 30 days
    for i, (name, cid) in enumerate(CUSTOMERS):
        for week_offset in [0, -7, -14]:
            amount = round(random.uniform(3000, 18000), 2)
            tx_date = d(week_offset - random.randint(0, 4))
            rows.append((
                xid(), "RECEIVE", "AUTHORISED", tx_date, amount,
                BANK_ACC_ID, BANK_ACC_NAME,
                f"INV-{1000 + i}",
                random.choice([True, True, False]),  # 67% reconciled
                "AUD", cid, name, json.dumps([]),
            ))

    # Expenses (SPEND) — last 30 days
    spend_items = [
        ("AWS", 2840.00), ("Cloudflare", 320.00),
        ("Dev Studio Ltd", 9500.00), ("Office Rent", 4200.00),
        ("Payroll - May", 28000.00), ("Software Licences", 640.00),
    ]
    for desc, amount in spend_items:
        tx_date = d(-random.randint(1, 25))
        rows.append((
            xid(), "SPEND", "AUTHORISED", tx_date, amount,
            BANK_ACC_ID, BANK_ACC_NAME, desc,
            True, "AUD", xid(), desc, json.dumps([]),
        ))

    cur.executemany("""
        INSERT INTO xero_bank_transactions_raw
            (transaction_id, type, status, transaction_date, amount,
             bank_account_id, bank_account_name, reference, is_reconciled,
             currency_code, contact_id, contact_name, line_items, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT DO NOTHING
    """, rows)
    print(f"  seeded {len(rows)} bank transactions")


def seed_invoices(cur):
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
            sub_total        NUMERIC(14,2),
            total_tax        NUMERIC(14,2),
            total            NUMERIC(14,2),
            amount_due       NUMERIC(14,2),
            amount_paid      NUMERIC(14,2),
            currency_code    VARCHAR(3),
            currency_rate    NUMERIC(14,6),
            line_items       JSONB,
            updated_at       TIMESTAMPTZ,
            loaded_at        TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    rows = []
    num = 1001

    # AR invoices — various aging buckets
    ar_scenarios = [
        # (invoice_date_offset, due_date_offset, status, amount_due_fraction)
        (  -5,  10, "AUTHORISED", 1.0),   # current
        ( -10,   5, "AUTHORISED", 1.0),   # current
        ( -20,  -5, "AUTHORISED", 1.0),   # 1-30 overdue
        ( -35, -15, "AUTHORISED", 1.0),   # 31-60 overdue
        ( -45, -20, "AUTHORISED", 0.5),   # 31-60 partially paid
        ( -70, -40, "AUTHORISED", 1.0),   # 61-90 overdue
        ( -95, -65, "AUTHORISED", 1.0),   # 90+ overdue
        ( -30,  -2, "PAID",       0.0),   # paid
        (  -3,  27, "AUTHORISED", 1.0),   # current (WTD — this week)
        (  -1,  29, "AUTHORISED", 1.0),   # current (WTD — this week)
    ]
    for inv_off, due_off, status, due_fraction in ar_scenarios:
        customer = random.choice(CUSTOMERS)
        total = round(random.uniform(2000, 15000), 2)
        tax = round(total * 0.1, 2)
        amount_due = round((total + tax) * due_fraction, 2)
        rows.append((
            xid(), "ACCREC", status,
            d(inv_off), d(due_off),
            f"INV-{num}", None,
            customer[1], customer[0],
            total, tax, round(total + tax, 2),
            amount_due, round((total + tax) - amount_due, 2),
            "AUD", 1.0, json.dumps([]),
        ))
        num += 1

    # AP bills
    ap_scenarios = [
        (-10,   5, "AUTHORISED", 1.0),   # not due
        (-20,  -3, "AUTHORISED", 1.0),   # 1-30 overdue
        (-40, -15, "AUTHORISED", 1.0),   # 31-60 overdue
        (-15,  15, "PAID",       0.0),   # paid
    ]
    for vendor in VENDORS:
        inv_off, due_off, status, due_fraction = random.choice(ap_scenarios)
        total = round(random.uniform(500, 12000), 2)
        tax = round(total * 0.1, 2)
        amount_due = round((total + tax) * due_fraction, 2)
        rows.append((
            xid(), "ACCPAY", status,
            d(inv_off), d(due_off),
            f"BILL-{num}", None,
            vendor[1], vendor[0],
            total, tax, round(total + tax, 2),
            amount_due, round((total + tax) - amount_due, 2),
            "AUD", 1.0, json.dumps([]),
        ))
        num += 1

    cur.executemany("""
        INSERT INTO xero_invoices_raw
            (invoice_id, type, status, invoice_date, due_date, invoice_number,
             reference, contact_id, contact_name, sub_total, total_tax, total,
             amount_due, amount_paid, currency_code, currency_rate, line_items, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT DO NOTHING
    """, rows)
    print(f"  seeded {len(rows)} invoices ({len(ar_scenarios)} AR, {len(VENDORS)} AP)")


def seed_stripe(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stripe_balance_transactions_raw (
            id                  VARCHAR(64) PRIMARY KEY,
            amount              INTEGER,
            available_on        TIMESTAMPTZ,
            created             TIMESTAMPTZ,
            currency            VARCHAR(3),
            description         TEXT,
            exchange_rate       NUMERIC(14,6),
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

    bt_rows = []
    payout_rows = []

    # Charges + fees this week
    charge_amounts = [4999, 9900, 2499, 14900, 7500, 3300, 5900]
    for amount_cents in charge_amounts:
        cid = sid()
        fee = int(amount_cents * 0.029 + 30)
        bt_rows.append((
            cid, amount_cents,
            f"NOW() - interval '{random.randint(0,5)} days'",
            f"NOW() - interval '{random.randint(0,5)} days'",
            "aud", "Charge", None,
            fee, amount_cents - fee,
            "charge", cid, "available", "charge",
            json.dumps([{"amount": fee, "currency": "aud", "type": "stripe_fee"}]),
        ))

    # Refund this week
    ref_amount = 2499
    ref_fee = int(ref_amount * 0.029 + 30)
    bt_rows.append((
        sid(), -ref_amount,
        "NOW() - interval '3 days'",
        "NOW() - interval '3 days'",
        "aud", "Refund", None,
        0, -ref_amount,
        "refund", sid(), "available", "refund",
        json.dumps([]),
    ))

    # Payouts (paid + one in_transit)
    payout_amounts = [
        (38200, "paid",       -4),
        (52700, "paid",       -11),
        (9800,  "in_transit",  2),
    ]
    for amount_cents, status, arrival_offset in payout_amounts:
        pid = payout_id()
        fee = 0
        payout_rows.append((
            pid, amount_cents,
            f"NOW() + interval '{arrival_offset} days'",
            f"NOW() - interval '2 days'",
            "aud", "STRIPE PAYOUT", "ba_test123",
            None, None, "standard", "card", status, "bank_account",
        ))
        # matching balance transaction
        bt_rows.append((
            sid(), -amount_cents,
            f"NOW() + interval '{arrival_offset} days'",
            f"NOW() - interval '2 days'",
            "aud", "STRIPE PAYOUT", None,
            0, -amount_cents,
            "payout", pid, "available", "payout",
            json.dumps([]),
        ))

    # Insert balance transactions using per-row timestamps
    for row in bt_rows:
        (id_, amount, avail_on, created, currency, desc, exch,
         fee, net, rep_cat, source, status, type_, fee_details) = row
        cur.execute(f"""
            INSERT INTO stripe_balance_transactions_raw
                (id, amount, available_on, created, currency, description,
                 exchange_rate, fee, net, reporting_category, source,
                 status, type, fee_details)
            VALUES (%s,%s,{avail_on},{created},%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
        """, (id_, amount, currency, desc, exch, fee, net, rep_cat, source, status, type_, fee_details))

    for row in payout_rows:
        (id_, amount, arrival, created, currency, desc, dest,
         fc, fm, method, source_type, status, type_) = row
        cur.execute(f"""
            INSERT INTO stripe_payouts_raw
                (id, amount, arrival_date, created, currency, description,
                 destination, failure_code, failure_message, method,
                 source_type, status, type)
            VALUES (%s,%s,{arrival},{created},%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
        """, (id_, amount, currency, desc, dest, fc, fm, method, source_type, status, type_))

    print(f"  seeded {len(bt_rows)} Stripe balance transactions")
    print(f"  seeded {len(payout_rows)} Stripe payouts")


def seed_bank_statements(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bank_statements_raw (
            id              SERIAL PRIMARY KEY,
            statement_date  DATE NOT NULL,
            description     TEXT,
            debit           NUMERIC(14,2),
            credit          NUMERIC(14,2),
            balance         NUMERIC(14,2),
            reference       TEXT,
            account_name    VARCHAR(255),
            source_file     VARCHAR(512),
            loaded_at       TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (statement_date, description, debit, credit, balance, account_name)
        )
    """)
    rows = [
        (d(-6),  "STRIPE PAYOUT",         None,   382.00, 142300.00, "po_001", "Business Cheque"),
        (d(-13), "STRIPE PAYOUT",         None,   527.00, 141918.00, "po_002", "Business Cheque"),
        (d(-5),  "AWS SYDNEY",            2840.00, None,  141391.00, "AWS",    "Business Cheque"),
        (d(-4),  "PAYROLL MAY 2026",     28000.00, None,  113391.00, "PAYROLL","Business Cheque"),
    ]
    cur.executemany("""
        INSERT INTO bank_statements_raw
            (statement_date, description, debit, credit, balance, reference, account_name, source_file)
        VALUES (%s,%s,%s,%s,%s,%s,%s,'smoke_test/seed')
        ON CONFLICT DO NOTHING
    """, rows)
    print(f"  seeded {len(rows)} bank statement rows")


def main():
    print("Connecting to smoke Postgres...")
    c = conn()
    c.autocommit = False
    try:
        cur = c.cursor()
        print("Seeding tables:")
        seed_accounts(cur)
        seed_bank_transactions(cur)
        seed_invoices(cur)
        seed_stripe(cur)
        seed_bank_statements(cur)
        c.commit()
        print("\nSeed complete.")
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()


if __name__ == "__main__":
    main()
