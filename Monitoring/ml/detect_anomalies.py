
import json
from datetime import datetime, timezone
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[2]

HISTORY_FILE = (
    BASE_DIR
    / "Monitoring"
    / "reports"
    / "pipeline_observations.jsonl"
)

OUTPUT_FILE = (
    BASE_DIR
    / "Monitoring"
    / "reports"
    / "anomaly_report.json"
)


def load_observations():
    if not HISTORY_FILE.exists():
        raise FileNotFoundError(
            f"Observation history not found: {HISTORY_FILE}"
        )

    observations = []

    with HISTORY_FILE.open("r", encoding="utf-8-sig") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue

            try:
                observations.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Invalid JSON on history line {line_number}: {exc}"
                ) from exc

    if not observations:
        raise ValueError("No pipeline observations available.")

    return observations


def detect_anomalies(observation):
    findings = []

    # Data-quality failures.
    if observation.get("dq_failed", 0) > 0:
        findings.append({
            "code": "DQ_FAILURES",
            "severity": "HIGH",
            "message": (
                f"{observation['dq_failed']} data-quality checks failed."
            )
        })

    # DLQ records are an operational signal, not automatically
    # a statistical anomaly.
    if observation.get("dlq_records", 0) > 0:
        findings.append({
            "code": "DLQ_RECORDS_PRESENT",
            "severity": "MEDIUM",
            "message": (
                f"{observation['dlq_records']} rejected records "
                "are recorded in the latest observation. "
                "Verify correction and recovery status."
            )
        })

    # Kafka health.
    if observation.get("kafka_status") != "HEALTHY":
        findings.append({
            "code": "KAFKA_UNHEALTHY",
            "severity": "HIGH",
            "message": (
                f"Kafka status is {observation.get('kafka_status')}."
            )
        })

    # Missing application topics.
    if observation.get("missing_topics_count", 0) > 0:
        findings.append({
            "code": "MISSING_KAFKA_TOPICS",
            "severity": "HIGH",
            "message": (
                f"{observation['missing_topics_count']} Kafka topics "
                "are missing."
            )
        })

    # AI-to-evidence reconciliation.
    if observation.get("reconciliation_mismatches", 0) > 0:
        findings.append({
            "code": "RECONCILIATION_MISMATCH",
            "severity": "HIGH",
            "message": (
                f"{observation['reconciliation_mismatches']} "
                "AI reconciliation mismatches detected."
            )
        })

    # Overall pipeline status.
    if observation.get("overall_status") != "HEALTHY":
        findings.append({
            "code": "PIPELINE_NOT_HEALTHY",
            "severity": "MEDIUM",
            "message": (
                f"Overall pipeline status is "
                f"{observation.get('overall_status')}."
            )
        })

    return findings


def main():
    if OUTPUT_FILE.exists():
        raise FileExistsError(
            f"Output already exists. Refusing to overwrite: {OUTPUT_FILE}"
        )

    observations = load_observations()
    latest = observations[-1]

    findings = detect_anomalies(latest)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pipeline": "StrataFlow",
        "detector": {
            "type": "deterministic_rule_based",
            "version": "1.0"
        },
        "evaluation": {
            "observations_available": len(observations),
            "latest_observation_at": latest.get("generated_at"),
            "statistical_model_enabled": False,
            "reason": (
                "Insufficient historical variation for reliable "
                "statistical or machine-learning anomaly detection."
            )
        },
        "status": "ALERT" if findings else "HEALTHY",
        "finding_count": len(findings),
        "findings": findings,
        "latest_observation": latest
    }

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with OUTPUT_FILE.open("w", encoding="utf-8") as file:
        json.dump(report, file, indent=2)

    print("=" * 60)
    print("STRATAFLOW ANOMALY DETECTION")
    print("=" * 60)

    print(f"Observations : {len(observations)}")
    print(f"Status       : {report['status']}")
    print(f"Findings     : {len(findings)}")

    for finding in findings:
        print(
            f"[{finding['severity']}] "
            f"{finding['code']}: {finding['message']}"
        )

    print()
    print(f"Report       : {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
