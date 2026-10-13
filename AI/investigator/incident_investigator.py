import json
from pathlib import Path
from datetime import datetime, timezone


BASE_DIR = Path(__file__).resolve().parents[2]

CONTEXT_FILE = BASE_DIR / "AI" / "reports" / "incident_context.json"
REPORT_FILE = BASE_DIR / "AI" / "reports" / "incident_report.json"


def load_context():
    if not CONTEXT_FILE.exists():
        raise FileNotFoundError(
            f"Missing incident context: {CONTEXT_FILE}"
        )

    with CONTEXT_FILE.open("r", encoding="utf-8") as file:
        return json.load(file)


def investigate(context):
    dq = context["dq_summary"]
    dlq = context["dlq_summary"]
    incidents = context.get("incidents", [])

    # Healthy pipeline
    if dq["failed"] == 0 and dlq["record_count"] == 0:
        return {
            "status": "HEALTHY",
            "severity": "NONE",
            "title": "No active data quality incidents",
            "root_cause": None,
            "impact": (
                "No data quality failures or rejected records "
                "were detected."
            ),
            "evidence": {
                "dq_checks": dq["total_checks"],
                "dq_passed": dq["passed"],
                "dq_failed": dq["failed"],
                "dlq_records": dlq["record_count"],
            },
            "recommendation": (
                "Continue monitoring the pipeline. "
                "No remediation is required."
            ),
        }

    failed_checks = []
    dlq_records = []

    for incident in incidents:
        if incident["source"] == "data_quality":
            failed_checks.extend(
                incident.get("failed_checks", [])
            )

        if incident["source"] == "dead_letter_queue":
            dlq_records.extend(
                incident.get("records", [])
            )

    # Analyze DLQ failure patterns
    failure_counts = {}

    for record in dlq_records:
        rule = record.get("failed_rule", "unknown")

        failure_counts[rule] = (
            failure_counts.get(rule, 0) + 1
        )

    most_common_failure = None

    if failure_counts:
        most_common_failure = max(
            failure_counts,
            key=failure_counts.get
        )

    evidence = {
        "dq_checks": dq["total_checks"],
        "dq_passed": dq["passed"],
        "dq_failed": dq["failed"],
        "dlq_records": dlq["record_count"],
        "failed_rules": failure_counts,
    }

    # DQ failures have highest priority
    if failed_checks:
        severity = "HIGH"
        title = "Data quality validation failure"

        root_cause = (
            f"{len(failed_checks)} data quality rule(s) failed. "
            "The affected source data violates the configured "
            "data contract."
        )

        impact = (
            f"{dq['failed']} validation rule(s) failed and may "
            "prevent downstream processing."
        )

        recommendation = (
            "Investigate the upstream producer or source data "
            "responsible for the failed contract rules. Correct "
            "the invalid records and re-run validation before "
            "downstream processing."
        )

    # DLQ-only incident
    else:
        severity = "MEDIUM"
        title = "Invalid records rejected by data validation"

        root_cause = (
            f"{dlq['record_count']} record(s) failed validation "
            "and were captured in the Dead Letter Queue."
        )

        impact = (
            f"{dlq['record_count']} invalid record(s) were "
            "prevented from progressing through the validated "
            "pipeline."
        )

        recommendation = (
            "Review the rejected records and their validation "
            "reasons. Correct the upstream data, then replay "
            "the affected records after successful validation."
        )

    if most_common_failure:
        evidence["most_common_failure"] = {
            "rule": most_common_failure,
            "count": failure_counts[most_common_failure],
        }

    return {
        "status": "INCIDENT",
        "severity": severity,
        "title": title,
        "root_cause": root_cause,
        "impact": impact,
        "evidence": evidence,
        "recommendation": recommendation,
    }


def main():
    context = load_context()

    investigation = investigate(context)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pipeline": "PulseFlow",
        "investigation": investigation,
    }

    with REPORT_FILE.open("w", encoding="utf-8") as file:
        json.dump(report, file, indent=2)

    print("=" * 60)
    print("PulseFlow AI Incident Investigator")
    print("=" * 60)
    print(f"Status      : {investigation['status']}")
    print(f"Severity    : {investigation['severity']}")
    print(f"Title       : {investigation['title']}")
    print(f"Output      : {REPORT_FILE}")
    print("=" * 60)


if __name__ == "__main__":
    main()