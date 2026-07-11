import subprocess
from datetime import datetime

from airflow.decorators import dag, task

from extraction.logger import setup_logger

logger = setup_logger()

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"


def run_dbt_command(args: list[str]):
    result = subprocess.run(
        ["dbt", *args],
        cwd=DBT_PROJECT_DIR,
        capture_output=True,
        text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt command failed: {' '.join(args)}")
    return result.stdout


@dag(
    dag_id="crypto_elt_backfill_pipeline",
    start_date=datetime(2026, 7, 1),
    schedule=None,
    catchup=False,
    tags=["crypto-elt", "backfill", "bigquery", "dbt"],
)
def crypto_elt_backfill_pipeline():

    @task
    def extract_task():
        from extraction.coingecko_extract import extract_backfill

        try:
            result = extract_backfill()
            logger.info(f"[Extract] Backfill extraction done: {result}")
            return result
        except Exception as e:
            logger.error(f"[Extract] Failed: {e}")
            raise

    @task
    def dbt_run_staging_task(extract_result: dict):
        try:
            run_dbt_command(["run", "--select", "staging"])
            logger.info("[dbt run] Staging models done")
        except Exception as e:
            logger.error(f"[dbt run staging] Failed: {e}")
            raise

    @task
    def dbt_run_marts_task():
        try:
            run_dbt_command(["run", "--select", "marts"])
            logger.info("[dbt run] Marts models done")
        except Exception as e:
            logger.error(f"[dbt run marts] Failed: {e}")
            raise

    @task
    def dbt_test_task():
        try:
            run_dbt_command(["test", "--select", "marts"])
            logger.info("[dbt test] Marts tests passed")
        except Exception as e:
            logger.error(f"[dbt test] Failed: {e}")
            raise

    extract_result = extract_task()
    staging_done = dbt_run_staging_task(extract_result)
    marts_done = dbt_run_marts_task()
    test_done = dbt_test_task()

    staging_done >> marts_done >> test_done


dag = crypto_elt_backfill_pipeline()
