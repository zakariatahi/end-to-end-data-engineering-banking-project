"""Refresh banking snapshots, then marts every UTC hour."""

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator


def run_dbt(command: str, selection: list[str], output_dir: str) -> None:
    # Airflow copies this function into the isolated environment.
    from pathlib import Path
    import subprocess
    import sys

    project = Path('/opt/airflow/banking_dbt')
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    dbt = Path(sys.executable).with_name('dbt')
    subprocess.run(
        [
            str(dbt),
            '--log-path', str(output / 'logs'),
            command,
            '--project-dir', str(project),
            '--profiles-dir', str(project),
            '--target-path', str(output / 'target'),
            '--select', *selection,
        ],
        cwd=output,
        check=True,
    )


with DAG(
    dag_id='dbt_snapshots_marts_hourly',
    start_date=datetime(2026, 9, 27, tzinfo=timezone.utc),
    schedule='0 * * * *',
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=50),
    is_paused_upon_creation=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'execution_timeout': timedelta(minutes=30),
    },
    tags=['banking', 'dbt'],
) as dag:
    operator_options = {
        'python_callable': run_dbt,
        'requirements': ['dbt-core==1.12.5', 'dbt-snowflake==1.12.1'],
        'system_site_packages': False,
        'venv_cache_path': '/opt/airflow/dbt_venvs',
        'expect_airflow': False,
        'do_xcom_push': False,
    }

    snapshots = PythonVirtualenvOperator(
        task_id='run_snapshots',
        op_kwargs={
            'command': 'snapshot',
            'selection': ['customers_snapshot', 'accounts_snapshot'],
            'output_dir': '/opt/airflow/dbt_runs/{{ ts_nodash }}/snapshots',
        },
        **operator_options,
    )

    marts = PythonVirtualenvOperator(
        task_id='run_marts',
        op_kwargs={
            'command': 'run',
            'selection': ['dim_customers', 'dim_accounts', 'fact_transactions'],
            'output_dir': '/opt/airflow/dbt_runs/{{ ts_nodash }}/marts',
        },
        **operator_options,
    )

    snapshots >> marts
