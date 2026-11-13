from typing import Any, TypedDict


class IncidentState(TypedDict, total=False):
    incident_context: dict[str, Any]
    incident_report: dict[str, Any]
    failure_patterns: dict[str, Any]

    deterministic_investigation: dict[str, Any]
    evidence: dict[str, Any]
    rag_context: str

    ai_analysis: dict[str, Any]
    ai_reconciliation: dict[str, Any]

    final_report: dict[str, Any]
    output_path: str
    error: str