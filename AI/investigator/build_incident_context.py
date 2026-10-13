import json
from pathlib import Path
from datetime import datetime, timezone


BASE_DIR = Path(__file__).resolve().parents[2]

DQ_REPORT = BASE_DIR / "DataQuality" / "checks" / "dq_report.json"
DLQ_FILE = BASE_DIR / "DataQuality" / "dlq" / "dlq_records.json"
OUTPUT_FILE = BASE_DIR / "AI" / "reports" / "incident_context.json"


def load_json(path):
    if not path.exists():
        return None

    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def main():
    dq_report = load_json(DQ_REPORT)
    dlq_records = load_json(DLQ_FILE)

    if dq_report is None:
        raise FileNotFoundError(f"Missing DQ report: {DQ_REPORT}")

    if dlq_records is None:
        raise FileNotFoundError(f"Missing DLQ file: {DLQ_FILE}")

    failed_checks = [
        check
        for check in dq_report.get("checks", [])
        if check.get("status") == "FAIL"
    ]

    incidents = []

    if dq_report.get("failed", 0) > 0:
        incidents.append({
            "source": "data_quality",
            "severity": "HIGH",
            "summary": (
                f"{dq_report['failed']} data quality checks failed "
                f"out of {dq_report['total_checks']}."
            ),
            "failed_checks": failed_checks,
        })

    if dlq_records:
        incidents.append({
            "source": "dead_letter_queue",
            "severity": "MEDIUM",
            "summary": (
                f"{len(dlq_records)} records were rejected "
                "and captured in the DLQ."
            ),
            "records": dlq_records,
        })

    context = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pipeline": "PulseFlow",
        "dq_summary": {
            "status": dq_report.get("status"),
            "total_checks": dq_report.get("total_checks"),
            "passed": dq_report.get("passed"),
            "failed": dq_report.get("failed"),
        },
        "dlq_summary": {
            "record_count": len(dlq_records),
        },
        "incidents": incidents,
    }

    with OUTPUT_FILE.open("w", encoding="utf-8") as file:
        json.dump(context, file, indent=2)

    print("=" * 60)
    print("PulseFlow Incident Context")
    print("=" * 60)
    print(f"DQ status       : {context['dq_summary']['status']}")
    print(f"DQ failures     : {context['dq_summary']['failed']}")
    print(f"DLQ records     : {context['dlq_summary']['record_count']}")
    print(f"Incidents found : {len(incidents)}")
    print(f"Output          : {OUTPUT_FILE}")
    print("=" * 60)


if __name__ == "__main__":
    main()