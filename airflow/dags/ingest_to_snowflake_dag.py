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
#
# ---------------------------------------------------------------------------
# Choosing which dbt model to run (for testing the healing agent)
# ---------------------------------------------------------------------------
# The dbt model that this DAG runs is controlled by the Airflow Variable
# "dbt_target_model". Set it in the Airflow UI (Admin > Variables) or via CLI:
#
#   airflow variables set dbt_target_model scenario_1_schema_mismatch
#
# Valid values (dbt model names — the .sql filename without extension):
#   stg_customers                 (default, healthy)
#   fct_order_summary             (healthy 3-way join)
#   scenario_1_schema_mismatch    (invalid identifier error)
#   scenario_2_invalid_cast       (date/type cast error)
#   scenario_3_wrong_table        (non-existent source table)
#   scenario_4_syntax_error       (SQL syntax error)
#
# If the Variable is not set, it falls back to "stg_customers".

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow'
}

# Read the target dbt model from an Airflow Variable (with a safe default).
# default_var ensures DAG parsing never fails even if the Variable is absent.
TARGET_MODEL = Variable.get("dbt_target_model", default_var="stg_customers")

dag = DAG(
    dag_id='ingest_to_snowflake',
    default_args=default_args,
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

run_dbt = BashOperator(
    task_id='run_stg_customers',
    bash_command=(
        "cd /opt/airflow/dags/dbt_project && "
        "python -m dbt.cli.main run "
        f"--select {TARGET_MODEL} "
        "--profiles-dir /opt/airflow/dags/dbt_project"
    ),
    dag=dag,
)
