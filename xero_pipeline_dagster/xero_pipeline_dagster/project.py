from pathlib import Path

from dagster_dbt import DbtProject

XERO_DBT_PROJECT_DIR = Path(__file__).parent.parent.parent.parent / "xero_dbt"

dbt_project = DbtProject(
    project_dir=XERO_DBT_PROJECT_DIR,
    profiles_dir=XERO_DBT_PROJECT_DIR,
)
dbt_project.prepare_if_dev()
