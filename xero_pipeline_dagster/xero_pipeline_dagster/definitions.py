from dagster import Definitions, ScheduleDefinition, define_asset_job
from dagster_dbt import DbtCliResource, dbt_assets

from .assets.xero_assets import (
    xero_accounts_to_s3,
    xero_bank_transactions_to_s3,
    xero_invoices_to_s3,
    rds_xero_accounts,
    rds_xero_bank_transactions,
    rds_xero_invoices,
)
from .assets.stripe_assets import (
    stripe_balance_transactions_to_s3,
    stripe_payouts_to_s3,
    rds_stripe_balance_transactions,
    rds_stripe_payouts,
)
from .assets.bank_assets import bank_statements_s3_to_rds
from .project import dbt_project

_raw_assets = [
    xero_accounts_to_s3,
    xero_bank_transactions_to_s3,
    xero_invoices_to_s3,
    rds_xero_accounts,
    rds_xero_bank_transactions,
    rds_xero_invoices,
    stripe_balance_transactions_to_s3,
    stripe_payouts_to_s3,
    rds_stripe_balance_transactions,
    rds_stripe_payouts,
    bank_statements_s3_to_rds,
]


@dbt_assets(manifest=dbt_project.manifest_path)
def xero_dbt_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


xero_pipeline_job = define_asset_job(
    name="xero_full_pipeline",
    selection=_raw_assets + [xero_dbt_assets],
)

# Monday 06:00 UTC — before the weekly CEO review
weekly_schedule = ScheduleDefinition(
    name="xero_weekly_schedule",
    job=xero_pipeline_job,
    cron_schedule="0 6 * * 1",
)

# 2nd of each month 01:00 UTC — month-end close support pack
monthly_close_schedule = ScheduleDefinition(
    name="xero_monthly_close_schedule",
    job=xero_pipeline_job,
    cron_schedule="0 1 2 * *",
)

defs = Definitions(
    assets=_raw_assets + [xero_dbt_assets],
    jobs=[xero_pipeline_job],
    schedules=[weekly_schedule, monthly_close_schedule],
    resources={
        "dbt": DbtCliResource(
            project_dir=dbt_project,
            profiles_dir=dbt_project.profiles_dir,
        ),
    },
)
