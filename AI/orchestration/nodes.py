
import json
from datetime import datetime, timezone
from pathlib import Path

from AI.investigator.failure_pattern_analyzer import analyze_patterns
from AI.investigator.incident_investigator import investigate
from AI.rca import llm_incident_analyzer as rca


BASE_DIR = Path(__file__).resolve().parents[2]
REPORT_DIR = BASE_DIR / "AI" / "reports"

CONTEXT_FILE = REPORT_DIR / "incident_context.json"
OUTPUT_FILE = REPORT_DIR / "langgraph_incident_report.json"


def load_inputs(state):
    """Load the existing incident context without modifying it."""

    if not CONTEXT_FILE.exists():
        raise FileNotFoundError(
            f"Incident context not found: {CONTEXT_FILE}"
        )

    incident_context = rca.load_json(CONTEXT_FILE)

    if not isinstance(incident_context, dict):
        raise ValueError("Incident context must be a JSON object.")

    if not isinstance(incident_context.get("incidents"), list):
        raise ValueError("Incident context has no valid incidents list.")

    return {
        "incident_context": incident_context
    }


def analyze_incident(state):
    """Run deterministic failure-pattern analysis and investigation."""

    context = state["incident_context"]

    failure_patterns = analyze_patterns(context)
    deterministic_investigation = investigate(context)

    incident_report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pipeline": "StrataFlow",
        "investigation": deterministic_investigation
    }

    return {
        "failure_patterns": failure_patterns,
        "incident_report": incident_report,
        "deterministic_investigation": deterministic_investigation
    }


def build_evidence(state):
    """Build the compact evidence package used by the LLM."""

    evidence = rca.build_ai_evidence(
        state["incident_context"],
        state["incident_report"],
        state["failure_patterns"]
    )

    return {
        "evidence": evidence
    }


def retrieve_knowledge(state):
    """Retrieve relevant historical knowledge from the RAG store."""

    evidence = state["evidence"]

    retrieval_query = " ".join([
        str(evidence.get("pattern", "")),
        str(evidence.get("deterministic_root_cause", "")),
        str(evidence.get("deterministic_impact", "")),
        " ".join(evidence.get("categories", {}).keys()),
        " ".join(evidence.get("tables", {}).keys()),
        "investigate validation failures and DLQ recovery"
    ])

    rag_context = rca.retrieve_context(
        retrieval_query,
        top_k=3,
        max_per_source=2
    )

    return {
        "rag_context": rag_context
    }


def analyze_with_llm(state):
    """Generate, validate and reconcile the AI incident analysis."""

    evidence = state["evidence"]
    rag_context = state["rag_context"]

    prompt = rca.build_prompt(
        evidence,
        rag_context
    )

    raw_response = rca.call_ollama(prompt)

    ai_analysis = rca.extract_json(raw_response)
    ai_analysis = rca.validate_ai_analysis(ai_analysis)

    reconciliation = rca.reconcile_ai_analysis(
        ai_analysis,
        evidence
    )

    # Preserve the operational safety warning.
    deterministic_investigation = dict(
        state["deterministic_investigation"]
    )

    deterministic_investigation["recommendation"] = (
        "Review rejected records and correct the upstream data. "
        "Automated DLQ replay is not currently implemented. "
        "A controlled, validated and idempotent recovery workflow "
        "must be implemented before records can be replayed."
    )

    final_report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pipeline": "StrataFlow",
        "engine": {
            "type": "local_llm",
            "provider": "Ollama",
            "model": rca.MODEL,
            "endpoint": rca.OLLAMA_HOST
        },
        "orchestration": {
            "framework": "LangGraph",
            "nodes": [
                "load_inputs",
                "analyze_incident",
                "build_evidence",
                "retrieve_knowledge",
                "analyze_with_llm",
                "save_report"
            ]
        },
        "evidence": evidence,
        "retrieved_knowledge": rag_context,
        "deterministic_investigation": deterministic_investigation,
        "ai_analysis": ai_analysis,
        "ai_reconciliation": reconciliation
    }

    return {
        "ai_analysis": ai_analysis,
        "ai_reconciliation": reconciliation,
        "final_report": final_report
    }


def save_report(state):
    """Save the graph result to its own report file."""

    report = state["final_report"]

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    OUTPUT_FILE.write_text(
        json.dumps(report, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )

    return {
        "output_path": str(OUTPUT_FILE)
    }
