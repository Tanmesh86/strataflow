import json
from datetime import datetime, timezone
from pathlib import Path


# ============================================================
# PulseFlow AI Incident Intelligence
# Evidence Reconciliation Layer
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[2]

FAILURE_PATTERNS = (
    BASE_DIR
    / "AI"
    / "reports"
    / "failure_patterns.json"
)

AI_REPORT = (
    BASE_DIR
    / "AI"
    / "reports"
    / "ai_incident_report.json"
)

INCIDENT_REPORT = (
    BASE_DIR
    / "AI"
    / "reports"
    / "incident_report.json"
)

OUTPUT_FILE = (
    BASE_DIR
    / "AI"
    / "reports"
    / "final_incident_report.json"
)


# ============================================================
# JSON Utilities
# ============================================================

def load_json(path):
    if not path.exists():
        raise FileNotFoundError(
            f"Required file not found:\n{path}"
        )

    try:
        with open(
            path,
            "r",
            encoding="utf-8"
        ) as file:
            return json.load(file)

    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid JSON file:\n{path}\n\n{exc}"
        ) from exc


def save_json(path, data):
    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        path,
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(
            data,
            file,
            indent=2
        )


# ============================================================
# Reconciliation
# ============================================================

def reconcile():
    patterns = load_json(
        FAILURE_PATTERNS
    )

    ai_report = load_json(
        AI_REPORT
    )

    incident_report = load_json(
        INCIDENT_REPORT
    )

    ai = ai_report.get(
        "ai_analysis",
        {}
    )

    summary = patterns.get(
        "summary",
        {}
    )

    affected_tables = patterns.get(
        "affected_tables",
        {}
    )

    dominant_rule = summary.get(
        "dominant_rule",
        {}
    )

    pattern_type = summary.get(
        "pattern_type",
        "UNKNOWN"
    )

    total_records = summary.get(
        "total_rejected_records",
        0
    )

    # ========================================================
    # Deterministic Investigation
    # ========================================================

    deterministic_investigation = (
        incident_report.get(
            "investigation",
            incident_report
        )
    )

    deterministic_status = (
        deterministic_investigation.get(
            "status",
            "UNKNOWN"
        )
    )

    deterministic_severity = (
        deterministic_investigation.get(
            "severity",
            "UNKNOWN"
        )
    )

    deterministic_title = (
        deterministic_investigation.get(
            "title",
            ""
        )
    )

    # IMPORTANT:
    # This is the overall deterministic incident root cause.
    # It must NOT be replaced by the dominant validation rule.
    deterministic_root_cause = (
        deterministic_investigation.get(
            "root_cause",
            ""
        )
    )

    deterministic_impact = (
        deterministic_investigation.get(
            "impact",
            ""
        )
    )

    deterministic_recommendation = (
        deterministic_investigation.get(
            "recommendation",
            ""
        )
    )

    # ========================================================
    # Dominant Failure Rule
    # ========================================================

    dominant_failure_rule = dominant_rule.get(
        "rule"
    )

    dominant_failure_count = dominant_rule.get(
        "count",
        0
    )

    dominant_failure_percentage = dominant_rule.get(
        "percentage"
    )

    # ========================================================
    # Compare AI Output With Deterministic Evidence
    # ========================================================

    ai_affected_records = ai.get(
        "affected_records"
    )

    ai_scope = ai.get(
        "scope"
    )

    ai_incident_type = ai.get(
        "incident_type"
    )

    ai_probable_root_cause = ai.get(
        "probable_root_cause"
    )

    mismatches = []

    # --------------------------------------------------------
    # Record count mismatch
    # --------------------------------------------------------

    if ai_affected_records != total_records:
        mismatches.append({
            "field": "affected_records",
            "ai_value": ai_affected_records,
            "deterministic_value": total_records,
            "resolution": "deterministic_value_used"
        })

    # --------------------------------------------------------
    # Scope mismatch
    # --------------------------------------------------------

    deterministic_scope = (
        list(affected_tables.keys())
    )

    ai_scope_normalized = (
        str(ai_scope).strip().lower()
        if ai_scope is not None
        else ""
    )

    deterministic_scope_normalized = [
        str(table).strip().lower()
        for table in deterministic_scope
    ]

    scope_matches = (
        ai_scope_normalized in
        deterministic_scope_normalized
    )

    # If AI reports a single table while the deterministic
    # evidence shows multiple affected tables, the AI scope
    # is considered incorrect.
    if not scope_matches:
        mismatches.append({
            "field": "scope",
            "ai_value": ai_scope,
            "deterministic_value": (
                deterministic_scope
            ),
            "resolution": "deterministic_value_used"
        })

    # --------------------------------------------------------
    # Incident type mismatch
    # --------------------------------------------------------

    if ai_incident_type != pattern_type:
        mismatches.append({
            "field": "incident_type",
            "ai_value": ai_incident_type,
            "deterministic_value": pattern_type,
            "resolution": "deterministic_value_used"
        })

    # ========================================================
    # Build Trusted Facts
    # ========================================================

    trusted_facts = {
        "status": deterministic_status,
        "severity": deterministic_severity,
        "title": deterministic_title,

        # Overall incident classification
        "pattern_type": pattern_type,

        # Overall number of rejected records
        "total_rejected_records": total_records,

        # Tables affected by the incident
        "affected_tables": affected_tables,

        # Most frequent validation failure.
        # This is NOT the overall root cause.
        "dominant_rule": dominant_rule,

        # Overall deterministic incident root cause.
        "root_cause": deterministic_root_cause,

        "impact": deterministic_impact,

        "recommendation": deterministic_recommendation,

        "replay_required": total_records > 0
    }

    # ========================================================
    # Build Reconciled AI Analysis
    # ========================================================

    reconciled_ai = dict(ai)

    # --------------------------------------------------------
    # Deterministic facts always win.
    # --------------------------------------------------------

    reconciled_ai["incident_type"] = pattern_type

    reconciled_ai["scope"] = (
        deterministic_scope
    )

    reconciled_ai["affected_records"] = (
        total_records
    )

    reconciled_ai["replay_required"] = (
        total_records > 0
    )

    # --------------------------------------------------------
    # Semantic Correction
    #
    # The AI may have interpreted the dominant validation
    # rule as the overall root cause.
    #
    # That is incorrect when multiple validation rules caused
    # the rejected records.
    #
    # Therefore the reconciled report explicitly separates:
    #
    #   root_cause
    #       -> overall deterministic incident cause
    #
    #   dominant_failure_pattern
    #       -> most frequent individual validation failure
    # --------------------------------------------------------

    reconciled_ai["root_cause"] = (
        deterministic_root_cause
    )

    reconciled_ai["dominant_failure_pattern"] = {
        "rule": dominant_failure_rule,
        "count": dominant_failure_count,
        "percentage": dominant_failure_percentage
    }

    # Remove the misleading AI root-cause interpretation from
    # the trusted reconciled field.
    #
    # The original AI interpretation is still preserved below
    # inside ai_reasoning for complete auditability.
    reconciled_ai.pop(
        "probable_root_cause",
        None
    )

    # ========================================================
    # Preserve Original AI Reasoning
    # ========================================================

    reconciled_ai["ai_reasoning"] = {
        "incident_type": ai.get(
            "incident_type"
        ),

        # Preserve exactly what the AI originally claimed.
        # This is intentionally retained for auditability.
        "probable_root_cause": ai_probable_root_cause,

        "confidence": ai.get(
            "confidence"
        ),

        "scope": ai.get(
            "scope"
        ),

        "affected_records": ai.get(
            "affected_records"
        ),

        "impact": ai.get(
            "impact"
        ),

        "recommended_action": ai.get(
            "recommended_action"
        ),

        "replay_required": ai.get(
            "replay_required"
        )
    }

    # ========================================================
    # Reconciliation Metadata
    # ========================================================

    reconciled_ai["reconciliation"] = {
        "performed": True,

        "deterministic_source": (
            "failure_patterns.json"
        ),

        "mismatch_count": len(
            mismatches
        ),

        "mismatches": mismatches,

        "deterministic_facts_override_ai": True,

        # Explicit semantic clarification
        "root_cause_semantics": (
            "Overall incident root cause comes from "
            "deterministic investigation. The dominant rule "
            "represents only the most frequent individual "
            "validation failure and is not treated as the "
            "overall root cause."
        )
    }

    # ========================================================
    # Final Report
    # ========================================================

    final_report = {
        "generated_at": datetime.now(
            timezone.utc
        ).isoformat(),

        "pipeline": "PulseFlow",

        "report_type": (
            "Evidence-Reconciled AI Incident Report"
        ),

        "reconciliation_status": (
            "RECONCILED"
        ),

        "trusted_facts": trusted_facts,

        "ai_analysis": reconciled_ai,

        "deterministic_facts": {
            "pattern_type": pattern_type,

            "total_rejected_records": (
                total_records
            ),

            "affected_tables": (
                affected_tables
            ),

            "dominant_rule": (
                dominant_rule
            ),

            # Explicitly expose the overall root cause
            # separately from the dominant rule.
            "root_cause": (
                deterministic_root_cause
            )
        },

        "validation": {
            "ai_output_received": True,

            "deterministic_evidence_available": True,

            "mismatch_detected": (
                len(mismatches) > 0
            ),

            "mismatch_count": (
                len(mismatches)
            ),

            "trusted_report": True
        }
    }

    save_json(
        OUTPUT_FILE,
        final_report
    )

    return final_report


# ============================================================
# Main
# ============================================================

def main():
    print("=" * 60)
    print(
        "PulseFlow AI Evidence Reconciliation"
    )
    print("=" * 60)

    report = reconcile()

    trusted = report[
        "trusted_facts"
    ]

    validation = report[
        "validation"
    ]

    ai = report[
        "ai_analysis"
    ]

    print()

    print(
        "Reconciliation Status : "
        f"{report['reconciliation_status']}"
    )

    print(
        "Incident Type         : "
        f"{trusted['pattern_type']}"
    )

    print(
        "Severity              : "
        f"{trusted['severity']}"
    )

    # Overall incident root cause
    print(
        "Root Cause            : "
        f"{trusted['root_cause']}"
    )

    print(
        "Affected Records      : "
        f"{trusted['total_rejected_records']}"
    )

    print(
        "Affected Tables       : "
        f"{', '.join(trusted['affected_tables'].keys())}"
    )

    # Dominant rule is explicitly separated from root cause.
    dominant = trusted["dominant_rule"]

    print(
        "Dominant Rule         : "
        f"{dominant.get('rule')}"
    )

    print(
        "Dominant Rule Count   : "
        f"{dominant.get('count', 0)}"
    )

    if dominant.get("percentage") is not None:
        print(
            "Dominant Rule Share   : "
            f"{dominant.get('percentage')}%"
        )

    print(
        "Replay Required       : "
        f"{trusted['replay_required']}"
    )

    print()

    print(
        "AI Report Comparison"
    )

    print(
        "AI Records            : "
        f"{ai['ai_reasoning']['affected_records']}"
    )

    print(
        "Trusted Records       : "
        f"{trusted['total_rejected_records']}"
    )

    print(
        "Mismatches Detected   : "
        f"{validation['mismatch_count']}"
    )

    if validation["mismatch_detected"]:
        print()
        print(
            "AI output contained "
            "deterministic fact mismatches."
        )

        print(
            "Deterministic evidence "
            "was used for the final report."
        )

    print()

    print(
        "Final report          : "
        f"{OUTPUT_FILE}"
    )

    print("=" * 60)


if __name__ == "__main__":
    main()