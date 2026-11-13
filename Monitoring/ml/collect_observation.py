import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
REPORT_FILE = BASE_DIR / "Monitoring" / "reports" / "pipeline_health.json"
HISTORY_FILE = BASE_DIR / "Monitoring" / "reports" / "pipeline_observations.jsonl"


def main():
    if not REPORT_FILE.exists():
        raise FileNotFoundError(f"Health report not found: {REPORT_FILE}")

    with REPORT_FILE.open("r", encoding="utf-8-sig") as file:
        report = json.load(file)

    health = report["pipeline_health"]
    dq = health["dq"]
    incident = health["incident"]
    ai = health["ai"]
    reconciliation = health["reconciliation"]
    kafka = report["kafka"]

    observation = {
        "generated_at": report["generated_at"],
        "overall_status": report["overall_status"],
        "kafka_status": kafka["status"],
        "incident_status": incident["status"],
        "dq_checks": dq["checks"],
        "dq_failed": dq["failed"],
        "dq_pass_rate": dq["pass_rate"],
        "dlq_records": incident["dlq_records"],
        "ai_confidence": ai["confidence"],
        "reconciliation_mismatches": reconciliation["mismatches_detected"],
        "kafka_topic_count": kafka["topic_count"],
        "application_topic_count": kafka["application_topic_count"],
        "missing_topics_count": len(kafka["missing_topics"])
    }

    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)

    # Avoid recording the same monitoring snapshot twice.
    if HISTORY_FILE.exists():
        with HISTORY_FILE.open("r", encoding="utf-8-sig") as file:
            for line in file:
                if line.strip():
                    existing = json.loads(line)
                    if existing.get("generated_at") == observation["generated_at"]:
                        print("This observation is already recorded.")
                        return

    with HISTORY_FILE.open("a", encoding="utf-8") as file:
        file.write(json.dumps(observation) + "\n")

    print("Pipeline observation recorded.")
    print(f"Timestamp : {observation['generated_at']}")
    print(f"Status    : {observation['overall_status']}")
    print(f"History   : {HISTORY_FILE}")


if __name__ == "__main__":
    main()
