"""Daily Airflow DAG: runs dbt build for the run date (ds) with a 14-day lookback and a 40-day drift check.

Only the dbt step of the proposed pipeline; extraction, loading and the FX sensor are not built.
"""
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.timetables.trigger import CronTriggerTimetable

PROJECT_DIR = "/app"

with DAG(
    dag_id="marketing_spend_daily",
    start_date=datetime(2025, 1, 1),
    # Explicit trigger timetable: the logical date (ds) is the run date, whatever create_cron_data_intervals says.
    schedule=CronTriggerTimetable("0 3 * * *", timezone="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10), "retry_exponential_backoff": True},
    tags=["marketing", "dbt"],
) as dag:
    dbt_build = BashOperator(
        task_id="dbt_build",
        # paths in profiles.yml and the models are relative to the project directory
        cwd=PROJECT_DIR,
        bash_command=(
            f"dbt build --project-dir {PROJECT_DIR} --profiles-dir {PROJECT_DIR} "
            "--vars '{run_date: \"{{ ds }}\", lookback_days: 14, drift_check_days: 40}'"
        ),
    )
