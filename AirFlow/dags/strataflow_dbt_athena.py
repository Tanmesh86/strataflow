from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

DBT_PROJECT = "/opt/strataflow/pulseflow_dbt"
DBT_PROFILES = "/opt/dbt-profiles"

DBT_OPTIONS = (
    f" --project-dir {DBT_PROJECT}"
    f" --profiles-dir {DBT_PROFILES}"
    f" --no-partial-parse"
)

with DAG(
    dag_id="strataflow_dbt_athena",
    description="Build and validate StrataFlow Athena Gold-layer models",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["strataflow", "dbt", "athena", "data-engineering"],
) as dag:

    validate_gold_sources = BashOperator(
        task_id="validate_gold_sources",
        bash_command=(
            f"cd {DBT_PROJECT} && "
            "dbt test" + DBT_OPTIONS +
            " --select source:strataflow_gold"
        ),
    )

    build_athena_models = BashOperator(
        task_id="build_athena_models",
        bash_command=(
            f"cd {DBT_PROJECT} && "
            "dbt run" + DBT_OPTIONS +
            " --select path:models/athena"
        ),
    )

    validate_athena_models = BashOperator(
        task_id="validate_athena_models",
        bash_command=(
            f"cd {DBT_PROJECT} && "
            "dbt test" + DBT_OPTIONS +
            " --select path:models/athena"
        ),
    )

    validate_gold_sources >> build_athena_models >> validate_athena_models
