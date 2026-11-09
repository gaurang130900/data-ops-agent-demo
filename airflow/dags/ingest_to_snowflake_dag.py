# Airflow DAG: ingest_to_snowflake
#
# This file lives in the repo at:
#   data-ops-agent-demo/airflow/dags/ingest_to_snowflake_dag.py
#
# It must also be copied to the host machine's Airflow DAGs folder:
#   C:\Users\gaura\Documents\airflow-local\dags\ingest_to_snowflake_dag.py
#
# The Airflow Docker container maps that host directory to /opt/airflow/dags,
# so any changes here should be mirrored there for the scheduler to pick them up.

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow'
}

dag = DAG(
    dag_id='ingest_to_snowflake',
    default_args=default_args,
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

run_dbt = BashOperator(
    task_id='run_stg_customers',
    bash_command='cd /opt/airflow/dbt_project && python -m dbt.cli.main run --select stg_customers',
    dag=dag,
)
