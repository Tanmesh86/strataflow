import json
from datetime import datetime, timezone
from pathlib import Path


# ============================================================
# PulseFlow Unified Pipeline Health Monitor
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]

REPORT_DIR = BASE_DIR / "Monitoring" / "reports"

PIPELINE_REPORT = REPORT_DIR / "pipeline_health.json"

KAFKA_REPORT = (
    REPORT_DIR / "kafka_health.json"
)

INCIDENT_REPORT = (
    BASE_DIR
    / "AI"
    / "reports"
    / "incident_report.json"
)

AI_REPORT = (
    BASE_DIR
    / "AI"
    / "reports"
    / "final_incident_report.json"
)


# ============================================================
# JSON Loader
# ============================================================

def load_json(path):

    if not path.exists():
        return {}

    try:
        with open(
            path,
            "r",
            encoding="utf-8"
        ) as file:

            return json.load(file)

    except (
        json.JSONDecodeError,
        OSError
    ):

        return {}


# ============================================================
# Main
# ============================================================

def main():

    print("=" * 60)
    print("PulseFlow Pipeline Health Monitor")
    print("=" * 60)

    # ========================================================
    # Kafka Health
    # ========================================================

    kafka_report = load_json(
        KAFKA_REPORT
    )

    kafka = kafka_report.get(
        "kafka",
        {}
    )

    kafka_status = kafka.get(
        "status",
        "UNKNOWN"
    )

    broker_reachable = kafka.get(
        "broker_reachable",
        False
    )

    topic_count = kafka.get(
        "topic_count",
        0
    )

    application_topic_count = kafka.get(
        "application_topic_count",
        0
    )

    missing_topics = kafka.get(
        "missing_topics",
        []
    )

    # ========================================================
    # Deterministic Incident Evidence
    # ========================================================

    incident_report = load_json(
        INCIDENT_REPORT
    )

    investigation = incident_report.get(
        "investigation",
        {}
    )

    evidence = investigation.get(
        "evidence",
        {}
    )

    dq_checks = evidence.get(
        "dq_checks",
        0
    )

    dq_passed = evidence.get(
        "dq_passed",
        0
    )

    dq_failed = evidence.get(
        "dq_failed",
        0
    )

    dlq_records = evidence.get(
        "dlq_records",
        0
    )

    incident_status = investigation.get(
        "status",
        "NO_INCIDENT"
    )

    severity = investigation.get(
        "severity",
        "NONE"
    )

    root_cause = investigation.get(
        "root_cause"
    )

    failed_rules = evidence.get(
        "failed_rules",
        {}
    )

    most_common_failure = evidence.get(
        "most_common_failure",
        {}
    )

    # ========================================================
    # DQ Pass Rate
    # ========================================================

    if dq_checks > 0:

        dq_pass_rate = round(
            (
                dq_passed
                / dq_checks
            ) * 100,
            2
        )

    else:

        dq_pass_rate = 0.0

    # ========================================================
    # AI Evidence Reconciliation
    # ========================================================

    ai_report = load_json(
        AI_REPORT
    )

    # Top-level reconciliation status
    reconciliation_status = ai_report.get(
        "reconciliation_status",
        "UNKNOWN"
    )

    # AI analysis
    ai_analysis = ai_report.get(
        "ai_analysis",
        {}
    )

    ai_confidence = ai_analysis.get(
        "confidence"
    )

    # Reconciliation details
    reconciliation = ai_analysis.get(
        "reconciliation",
        {}
    )

    ai_mismatches = reconciliation.get(
        "mismatch_count",
        ai_report.get(
            "validation",
            {}
        ).get(
            "mismatch_count",
            0
        )
    )

    # ========================================================
    # Overall Health
    # ========================================================

    if kafka_status == "DOWN":

        overall_status = "CRITICAL"

    elif kafka_status == "DEGRADED":

        overall_status = "DEGRADED"

    elif incident_status == "INCIDENT":

        overall_status = "DEGRADED"

    elif dq_failed > 0:

        overall_status = "DEGRADED"

    elif reconciliation_status not in (
        "RECONCILED",
        "NOT_REQUIRED",
        "UNKNOWN"
    ):

        overall_status = "DEGRADED"

    else:

        overall_status = "HEALTHY"

    # ========================================================
    # Unified Monitoring Report
    # ========================================================

    report = {

        "generated_at":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "pipeline":
            "PulseFlow",

        "overall_status":
            overall_status,

        "pipeline_health": {

            "dq": {

                "checks":
                    dq_checks,

                "passed":
                    dq_passed,

                "failed":
                    dq_failed,

                "pass_rate":
                    dq_pass_rate
            },

            "incident": {

                "status":
                    incident_status,

                "severity":
                    severity,

                "dlq_records":
                    dlq_records,

                "root_cause":
                    root_cause
            },

            "ai": {

                "confidence":
                    ai_confidence
            },

            "reconciliation": {

                "status":
                    reconciliation_status,

                "mismatches_detected":
                    ai_mismatches
            }
        },

        "kafka": {

            "status":
                kafka_status,

            "broker_reachable":
                broker_reachable,

            "topic_count":
                topic_count,

            "application_topic_count":
                application_topic_count,

            "missing_topics":
                missing_topics
        },

        "evidence": {

            "failed_rules":
                failed_rules,

            "most_common_failure":
                most_common_failure
        },

        "sources": {

            "incident_report":
                str(INCIDENT_REPORT),

            "ai_report":
                str(AI_REPORT),

            "kafka_report":
                str(KAFKA_REPORT)
        }
    }

    # ========================================================
    # Write Report
    # ========================================================

    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        PIPELINE_REPORT,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            report,
            file,
            indent=2
        )

    # ========================================================
    # Console Output
    # ========================================================

    print()

    print(
        f"Overall Status        : "
        f"{overall_status}"
    )

    print()

    print("Pipeline")

    print(
        f"DQ Checks             : "
        f"{dq_checks}"
    )

    print(
        f"DQ Passed             : "
        f"{dq_passed}"
    )

    print(
        f"DQ Failed             : "
        f"{dq_failed}"
    )

    print(
        f"DQ Pass Rate          : "
        f"{dq_pass_rate}%"
    )

    print()

    print("Kafka")

    print(
        f"Kafka Status          : "
        f"{kafka_status}"
    )

    print(
        f"Broker Reachable      : "
        f"{broker_reachable}"
    )

    print(
        f"Total Topics          : "
        f"{topic_count}"
    )

    print(
        f"Application Topics    : "
        f"{application_topic_count}"
    )

    print(
        f"Missing Topics        : "
        f"{len(missing_topics)}"
    )

    print()

    print("Incidents")

    print(
        f"DLQ Records           : "
        f"{dlq_records}"
    )

    print(
        f"Incident              : "
        f"{incident_status}"
    )

    print(
        f"Severity              : "
        f"{severity}"
    )

    print()

    print("AI")

    print(
        f"AI Confidence         : "
        f"{ai_confidence}"
    )

    print(
        f"Reconciliation        : "
        f"{reconciliation_status}"
    )

    print(
        f"AI Mismatches         : "
        f"{ai_mismatches}"
    )

    print()

    print(
        f"Health report         : "
        f"{PIPELINE_REPORT}"
    )

    print("=" * 60)


if __name__ == "__main__":
    main()