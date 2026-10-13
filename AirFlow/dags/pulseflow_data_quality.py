```python
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="pulseflow_data_quality",
    description="PulseFlow data quality validation and AI incident intelligence",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["pulseflow", "data-quality", "ai", "incident-intelligence"],
) as dag:

    # ---------------------------------------------------------
    # 1. DATA QUALITY
    # ---------------------------------------------------------
    run_data_quality = BashOperator(
        task_id="run_data_quality_checks",
        bash_command=(
            "cd /opt/pulseflow && "
            "python /opt/pulseflow/DataQuality/checks/run_dq_checks.py"
        ),
    )

    # ---------------------------------------------------------
    # 2. FAILURE PATTERN ANALYSIS
    # ---------------------------------------------------------
    run_failure_pattern_analysis = BashOperator(
        task_id="run_failure_pattern_analysis",
        bash_command=(
            "cd /opt/pulseflow && "
            "python /opt/pulseflow/AI/investigator/failure_pattern_analyzer.py"
        ),
    )

    # ---------------------------------------------------------
    # 3. INCIDENT CONTEXT / INVESTIGATION
    # ---------------------------------------------------------
    run_incident_investigation = BashOperator(
        task_id="run_incident_investigation",
        bash_command=(
            "cd /opt/pulseflow && "
            "python /opt/pulseflow/AI/investigator/incident_investigator.py"
        ),
    )

    # ---------------------------------------------------------
    # 4. LLM INCIDENT ANALYSIS
    # ---------------------------------------------------------
    run_llm_incident_analysis = BashOperator(
        task_id="run_llm_incident_analysis",
        bash_command=(
            "cd /opt/pulseflow && "
            "python /opt/pulseflow/AI/investigator/llm_incident_analyzer.py"
        ),
        env={
            "OLLAMA_HOST": "http://host.docker.internal:11434",
        },
    )

    # ---------------------------------------------------------
    # 5. EVIDENCE RECONCILIATION
    # ---------------------------------------------------------
    run_evidence_reconciliation = BashOperator(
        task_id="run_evidence_reconciliation",
        bash_command=(
            "cd /opt/pulseflow && "
            "python /opt/pulseflow/AI/investigator/reconcile_ai_report.py"
        ),
    )

    # ---------------------------------------------------------
    # PIPELINE DEPENDENCIES
    # ---------------------------------------------------------
    (
        run_data_quality
        >> run_failure_pattern_analysis
        >> run_incident_investigation
        >> run_llm_incident_analysis
        >> run_evidence_reconciliation
    )
```
