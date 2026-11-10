from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="pulseflow_data_quality",
    description="StrataFlow data quality validation and AI incident intelligence",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["strataflow", "data-quality", "ai", "incident-intelligence"],
) as dag:

    run_data_quality = BashOperator(
        task_id="run_data_quality_checks",
        bash_command=(
            "cd /opt/pulseflow && "
            "python /opt/pulseflow/DataQuality/checks/run_dq_checks.py"
        ),
    )

    build_incident_context = BashOperator(
        task_id="build_incident_context",
        bash_command=(
            "cd /opt/pulseflow && "
            "python /opt/pulseflow/AI/investigator/build_incident_context.py"
        ),
    )

    run_failure_pattern_analysis = BashOperator(
        task_id="run_failure_pattern_analysis",
        bash_command=(
            "cd /opt/pulseflow && "
            "python /opt/pulseflow/AI/investigator/failure_pattern_analyzer.py"
        ),
    )

    run_incident_investigation = BashOperator(
        task_id="run_incident_investigation",
        bash_command=(
            "cd /opt/pulseflow && "
            "python /opt/pulseflow/AI/investigator/incident_investigator.py"
        ),
    )

    run_llm_incident_analysis = BashOperator(
        task_id="run_llm_incident_analysis",
        bash_command=(
            "cd /opt/pulseflow && "
            "python /opt/pulseflow/AI/rca/llm_incident_analyzer.py"
        ),
        env={
            "OLLAMA_HOST": "http://host.docker.internal:11434",
        },
        append_env=True,
    )

    run_evidence_reconciliation = BashOperator(
        task_id="run_evidence_reconciliation",
        bash_command=(
            "cd /opt/pulseflow && "
            "python /opt/pulseflow/AI/investigator/reconcile_ai_report.py"
        ),
    )

    (
        run_data_quality
        >> build_incident_context
        >> run_failure_pattern_analysis
        >> run_incident_investigation
        >> run_llm_incident_analysis
        >> run_evidence_reconciliation
    )