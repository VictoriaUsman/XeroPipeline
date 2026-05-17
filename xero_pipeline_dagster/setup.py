from setuptools import find_packages, setup

setup(
    name="xero_pipeline_dagster",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "dagster-dbt",
        "dbt-postgres",
        "boto3",
        "psycopg2-binary",
        "requests",
        "stripe",
    ],
)
