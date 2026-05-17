# Xero Finance Pipeline

A data pipeline that pulls from **Xero**, **Stripe**, and **bank statement CSVs**, lands everything in **PostgreSQL on RDS**, transforms it with **dbt**, and produces a weekly CEO/CFO finance dashboard view — orchestrated by **Dagster** on the same AWS stack as the car_sales pipeline.

---

## Architecture

```
┌─────────────────┐
│   Xero API      │  accounts, bank transactions, invoices (AR + AP)
│   Stripe API    │  balance transactions, payouts
│   Bank CSV      │  manual upload to S3
└────────┬────────┘
         │ Dagster assets (extract → S3 → RDS)
         ▼
┌─────────────────┐
│   S3 (JSON)     │  s3://<bucket>/xero-pipeline/
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Postgres (RDS) │  raw tables (upsert on each run)
└────────┬────────┘
         │ dbt build
         ▼
┌────────────────────────────────────────┐
│  bronze  │  silver  │  gold            │
│  typed   │  cleaned │  CEO dashboard   │
└────────────────────────────────────────┘
         │
         ▼
┌─────────────────┐
│  weekly_report  │  python script → terminal / email / Slack
└─────────────────┘
```

---

## Directory Structure

```
XERO/
├── .env.example                         # all required environment variables
├── requirements.txt
├── reporting/
│   └── weekly_report.py                 # queries gold dashboard, prints report
├── xero_pipeline_dagster/
│   ├── setup.py
│   ├── workspace.yaml
│   └── xero_pipeline_dagster/
│       ├── definitions.py               # jobs, schedules, dbt wiring
│       ├── project.py                   # DbtProject reference
│       ├── assets/
│       │   ├── xero_assets.py           # Xero extract + load assets
│       │   ├── stripe_assets.py         # Stripe extract + load assets
│       │   └── bank_assets.py           # bank CSV S3 scanner + loader
│       └── resources/
│           ├── xero_client.py           # Xero OAuth 2.0 client (token refresh)
│           └── stripe_client.py         # Stripe SDK wrapper
└── xero_dbt/
    ├── dbt_project.yml
    ├── profiles.yml                     # reads from env vars
    └── models/
        ├── sources.yml                  # declares raw Postgres tables as sources
        ├── bronze/                      # thin select, type casts, deleted filtered
        ├── silver/                      # signed amounts, aging buckets, reconciliation
        └── gold/                        # CEO dashboard views
```

---

## Data Sources

### Xero API (OAuth 2.0)

| Entity | Incremental window | Raw table |
|---|---|---|
| Accounts (chart of accounts) | full refresh | `xero_accounts_raw` |
| BankTransactions | last 8 days | `xero_bank_transactions_raw` |
| Invoices (AR + AP) | last 8 days | `xero_invoices_raw` |

### Stripe API

| Entity | Incremental window | Raw table |
|---|---|---|
| BalanceTransactions | last 8 days | `stripe_balance_transactions_raw` |
| Payouts | last 8 days | `stripe_payouts_raw` |

### Bank Statement CSVs (manual)

Upload CSVs to S3 at:
```
s3://<BUCKET>/xero-pipeline/bank/statements/<account_name>/YYYY-MM-DD.csv
```

Expected columns (case-insensitive):
```
date, description, debit, credit, balance, reference, account_name
```

The `bank_statements_s3_to_rds` asset scans the full prefix and inserts new rows (duplicate-safe). Trigger it manually from Dagster after each upload.

---

## dbt Layer

### Bronze
Reads from raw Postgres tables via `source()` references. Filters out `DELETED`/`VOIDED` records. Converts Stripe amounts from cents to dollars.

### Silver

| Model | What it adds |
|---|---|
| `silver_xero_bank_transactions` | `signed_amount` (positive = cash in, negative = out), joined to account class |
| `silver_xero_ar` | AR invoices with aging bucket (current / 1-30 / 31-60 / 61-90 / 90+) |
| `silver_xero_ap` | AP bills with aging bucket |
| `silver_stripe_payouts` | Payouts with gross/fee/net pulled from balance transactions |
| `silver_stripe_reconciliation` | Stripe payouts matched to Xero bank entries (amount ±$0.01, date ±3 days) |

### Gold

| Model | CEO use |
|---|---|
| `gold_cash_position` | Bank balances by account + Stripe in-transit float |
| `gold_ar_aging` | AR outstanding by aging bucket, top customers |
| `gold_revenue_by_type` | WTD revenue from invoices and bank receipts |
| `gold_stripe_movement` | WTD Stripe: gross charges, fees, refunds, net by category |
| `gold_ceo_weekly_dashboard` | **Single view, 6 sections** — query this every Monday |

#### Dashboard sections

```
section          metric
──────────────── ──────────────────────────────────────
cash             Total Xero Cash
                 Stripe Float (In-Transit)

ar_summary       Total AR Outstanding ($)
                 AR Overdue 1-30 ($)
                 AR Overdue 31-60 ($)
                 AR Overdue 60+ ($)
                 Open AR Invoices (count)

ap_summary       Total AP Outstanding ($)
                 AP Overdue Bills (count)

revenue_wtd      Gross Revenue Invoiced WTD ($)
                 Cash Receipts WTD ($)

stripe_wtd       Stripe Gross Charges WTD ($)
                 Stripe Fees WTD ($)
                 Stripe Refunds WTD ($)
                 Stripe Net WTD ($)

recon_status     Bank Txns Reconciled % (90d)
                 Stripe Payouts Matched %
```

---

## Schedules

| Schedule | Cron | Purpose |
|---|---|---|
| `xero_weekly_schedule` | `0 6 * * 1` | Monday 06:00 UTC — before CEO weekly review |
| `xero_monthly_close_schedule` | `0 1 2 * *` | 2nd of month 01:00 UTC — month-end close support pack |

---

## Setup

### 1. Prerequisites

- Python 3.11+
- PostgreSQL database (same RDS instance as car_sales, or a separate one)
- S3 bucket (same as car_sales)
- Xero app with OAuth 2.0 credentials ([create one here](https://developer.xero.com/app/manage))
- Stripe secret key

### 2. Environment variables

```bash
cp .env.example .env
# fill in all values
```

Key variables:

```bash
# Xero OAuth 2.0
XERO_CLIENT_ID=...
XERO_CLIENT_SECRET=...
XERO_TENANT_ID=...         # your Xero org ID
XERO_REFRESH_TOKEN=...     # see step 3

# Stripe
STRIPE_API_KEY=sk_live_...

# AWS + RDS (same as car_sales)
S3_BUCKET=...
RDS_PG_HOST=...
RDS_PG_DATABASE=xero_pipeline
RDS_PG_USER=...
RDS_PG_PASSWORD=...
```

### 3. Get your Xero refresh token (one-time)

Xero uses OAuth 2.0. You need to complete the browser authorization once to get a refresh token. The pipeline then rotates it automatically.

**Option A — Xero OAuth Playground (quickest):**
1. Go to [developer.xero.com/app/manage](https://developer.xero.com/app/manage), open your app
2. Add `http://localhost` as a redirect URI
3. Use the OAuth 2.0 Playground tab to authorize and copy the refresh token
4. Set `XERO_REFRESH_TOKEN=<token>` in your `.env`

**Option B — production:** Store the refresh token in AWS SSM Parameter Store:
```bash
aws ssm put-parameter \
  --name /xero-pipeline/refresh-token \
  --value '{"refresh_token": "<token>"}' \
  --type SecureString
```
Set `XERO_TOKEN_SSM_PATH=/xero-pipeline/refresh-token`. The pipeline reads SSM first, falls back to `XERO_REFRESH_TOKEN` env var, and always writes the new token back to SSM after each refresh.

**Finding your Tenant ID:**
```bash
# after you have a valid access token:
curl -H "Authorization: Bearer <access_token>" https://api.xero.com/connections
# returns array of tenants; copy the "tenantId" field
```

### 4. Install dependencies

```bash
cd XERO
pip install -r requirements.txt
cd xero_pipeline_dagster
pip install -e .
```

### 5. Compile dbt manifest

```bash
cd XERO/xero_dbt
dbt compile
```

### 6. Start Dagster

```bash
cd XERO/xero_pipeline_dagster
dagster dev
```

Open [http://localhost:3000](http://localhost:3000). You should see the `xero_full_pipeline` job with all assets and both schedules.

### 7. Run the weekly report

```bash
cd XERO
python reporting/weekly_report.py
```

Output:

```
==============================================================
               WEEKLY FINANCE DASHBOARD
                   Week of 2026-05-19
==============================================================

  CASH POSITION
  ----------------------------------------------------------
  Total Xero Cash                                  $142,300.00
  Stripe Float (In-Transit)                          $8,450.00

  ACCOUNTS RECEIVABLE (AR)
  ----------------------------------------------------------
  Total AR Outstanding ($)                          $67,200.00
  AR Overdue 1-30 ($)                               $12,400.00
  AR Overdue 31-60 ($)                               $4,100.00
  AR Overdue 60+ ($)                                 $1,800.00
  Open AR Invoices (count)                                  14
  ...
```

---

## Token Rotation

The Xero refresh token is single-use and rotates on every API call. The `XeroClient` handles this automatically:

1. Loads the current refresh token (SSM → local file → env var)
2. Exchanges it for a new access token + refresh token
3. Saves the new refresh token back to SSM (and `/tmp/xero_token.json` as a fallback)

If the pipeline fails mid-run before saving the new token, re-run it within 60 days using the last known token. After 60 days without a successful refresh, you must re-authorize via the browser flow.

---

## Adding More Xero Entities

The pattern is consistent across all assets. To add (for example) `Journals` for full GL visibility:

**1. Add to `XeroClient`** (`resources/xero_client.py`):
```python
def get_journals(self, modified_after=None):
    return self._paginate("Journals", "Journals", modified_after)
```

**2. Add Dagster assets** (`assets/xero_assets.py`):
```python
@asset(retry_policy=_retry)
def xero_journals_to_s3(context): ...

@asset(deps=[xero_journals_to_s3], retry_policy=_retry)
def rds_xero_journals(context): ...
```

**3. Add to `sources.yml`** and wire a bronze → silver → gold dbt model.

**4. Register in `definitions.py`**.

---

## AU/GST Notes

- Xero returns `TaxType` on each account and `TotalTax` on each invoice/transaction.
- BAS-relevant figures (GST collected, GST paid) can be derived from the silver layer by filtering on `tax_type = 'OUTPUT2'` (GST on income) and `tax_type = 'INPUT'` (GST on expenses).
- Tax compliance decisions stay with your AU accountant. This pipeline prepares the data substrate; the accountant reviews and lodges.

---

## Metabase Dashboard

Metabase is the visual dashboard layer. It connects directly to your RDS Postgres and reads the gold views — no code needed to build charts.

### Start Metabase

```bash
# on EC2 (or locally)
cd XERO
docker compose up -d
```

Open **http://\<EC2-public-ip\>:3001** (or http://localhost:3001 locally).

> Dagster runs on port 3000. Metabase runs on port 3001. The Terraform security group already has both open.

### First-time setup (5 minutes)

1. Create your admin account when prompted
2. Click **Add your data** → **PostgreSQL**
3. Fill in your RDS connection details:

| Field | Value |
|---|---|
| Display name | `Xero Pipeline` |
| Host | `$RDS_PG_HOST` |
| Port | `5432` |
| Database name | `$RDS_PG_DATABASE` |
| Username | `$RDS_PG_USER` |
| Password | `$RDS_PG_PASSWORD` |

4. Click **Save** — Metabase will scan your schemas and surface the gold views automatically

### Suggested charts to build

Once connected, create a new **Dashboard** called `Weekly Finance` and add these questions:

| Chart type | Table / view | X-axis | Y-axis / metric | Notes |
|---|---|---|---|---|
| **Number** | `gold.gold_cash_position` | — | `SUM(total_cash)` | Filter `source = xero` |
| **Number** | `gold.gold_cash_position` | — | `SUM(total_cash)` | Filter `source = stripe_pending` |
| **Bar** | `gold.gold_ar_aging` | `aging_bucket` | `total_outstanding` | Sort by bucket order |
| **Number** | `gold.gold_ar_aging` | — | `SUM(total_outstanding)` | All buckets combined |
| **Row** | `gold.gold_revenue_by_type` | `revenue_account` | `amount` | WTD revenue breakdown |
| **Table** | `gold.gold_stripe_movement` | — | all columns | WTD Stripe by category |
| **Number** | `gold.gold_ceo_weekly_dashboard` | — | `value` | Filter `metric = Bank Txns Reconciled % (90d)` |
| **Table** | `gold.gold_ceo_weekly_dashboard` | — | all columns | Full snapshot — useful as a summary card |

### Terraform

Port 3001 is already added to the EC2 security group in `terraform/ec2.tf`. Apply it:

```bash
cd terraform
terraform plan
terraform apply
```

Then SSH into EC2 and start Metabase:

```bash
ssh -i <key.pem> ec2-user@<EC2-public-ip>

# install Docker (Amazon Linux 2)
sudo yum update -y
sudo yum install -y docker
sudo service docker start
sudo usermod -aG docker ec2-user

# install docker compose plugin
sudo mkdir -p /usr/local/lib/docker/cli-plugins
sudo curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

# run Metabase
cd ~/xero-pipeline   # wherever you cloned the repo
docker compose up -d
```

### Keeping the dashboard current

The Dagster weekly schedule runs every Monday at 06:00 UTC and rebuilds all gold views. Metabase reads live from Postgres, so the dashboard reflects the latest data as soon as dbt finishes — no manual refresh needed.

To set Metabase to auto-refresh during the CEO's Monday review session:
- Open the dashboard → click the clock icon → set to **1 minute** auto-refresh

---

## What stays in Xero vs. what moves to the warehouse

| Stays in Xero | Moves to warehouse |
|---|---|
| Source of truth for transactions | Reporting aggregations |
| Invoice creation and approval | AR aging analysis |
| Bill entry and payment | Revenue classification |
| Bank reconciliation workflow | Stripe reconciliation matching |
| GST/BAS coding | CEO dashboard |
| Payroll (Xero Payroll) | Month-end close pack |

The warehouse is read-only relative to Xero. Nothing written to Postgres flows back to Xero.
