from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_DIR = Path(__file__).resolve().parents[2]
PYTHON = PROJECT_DIR / "venv" / "bin" / "python"
DBT_DIR = PROJECT_DIR / "dbt" / "shipment_analytics" / "shipment_analytics"
DBT = PROJECT_DIR / "venv" / "bin" / "dbt"


default_args = {
    "owner": "akash",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="shipment_analytics_pipeline",
    default_args=default_args,
    description="Daily shipment analytics ETL pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["pyspark", "etl", "shipment", "dbt"],
) as dag:

    run_spark_etl = BashOperator(
        task_id="run_spark_etl",
        cwd=str(PROJECT_DIR),
        bash_command=(
            f'"{PYTHON}" src/spark_transform.py'
        ),
    )

    run_dbt = BashOperator(
        task_id="run_dbt",
        cwd=str(DBT_DIR),
        bash_command=(
            f'"{DBT}" run'
        ),
    )

    test_dbt = BashOperator(
        task_id="test_dbt",
        cwd=str(DBT_DIR),
        bash_command=(
            f'"{DBT}" test'
        ),
    )

    run_spark_etl >> run_dbt >> test_dbt
