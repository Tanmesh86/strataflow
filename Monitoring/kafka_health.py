import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path


# ============================================================
# PulseFlow Kafka Health Check
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]

OUTPUT_FILE = (
    BASE_DIR
    / "Monitoring"
    / "reports"
    / "kafka_health.json"
)

KAFKA_CONTAINER = "pulseflow-kafka"

BOOTSTRAP_SERVER = "localhost:9092"

EXPECTED_TOPICS = [
    "pulseflow.public.customers",
    "pulseflow.public.order_items",
    "pulseflow.public.orders",
    "pulseflow.public.payments",
    "pulseflow.public.products"
]


# ============================================================
# Command Runner
# ============================================================

def run_command(command):
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=30
        )

        return (
            result.returncode,
            result.stdout.strip(),
            result.stderr.strip()
        )

    except subprocess.TimeoutExpired:
        return (
            -1,
            "",
            "Command timed out"
        )


# ============================================================
# Kafka Health
# ============================================================

def check_kafka():
    topic_command = [
        "docker",
        "exec",
        KAFKA_CONTAINER,
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        BOOTSTRAP_SERVER,
        "--list"
    ]

    returncode, stdout, stderr = run_command(
        topic_command
    )

    if returncode != 0:
        return {
            "status": "DOWN",
            "broker_reachable": False,
            "topics": [],
            "expected_topics": EXPECTED_TOPICS,
            "missing_topics": EXPECTED_TOPICS,
            "topic_count": 0,
            "error": stderr
        }

    topics = [
        topic.strip()
        for topic in stdout.splitlines()
        if topic.strip()
    ]

    missing_topics = [
        topic
        for topic in EXPECTED_TOPICS
        if topic not in topics
    ]

    return {
        "status": (
            "HEALTHY"
            if not missing_topics
            else "DEGRADED"
        ),

        "broker_reachable": True,

        "topics": topics,

        "expected_topics": EXPECTED_TOPICS,

        "missing_topics": missing_topics,

        "topic_count": len(topics),

        "application_topic_count": len(
            EXPECTED_TOPICS
        )
    }


# ============================================================
# Main
# ============================================================

def main():
    print("=" * 60)
    print("PulseFlow Kafka Health Monitor")
    print("=" * 60)

    kafka = check_kafka()

    report = {
        "generated_at": datetime.now(
            timezone.utc
        ).isoformat(),

        "pipeline": "PulseFlow",

        "kafka": kafka
    }

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        OUTPUT_FILE,
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(
            report,
            file,
            indent=2
        )

    print()

    print(
        "Kafka Status          : "
        f"{kafka['status']}"
    )

    print(
        "Broker Reachable      : "
        f"{kafka['broker_reachable']}"
    )

    print(
        "Total Topics          : "
        f"{kafka['topic_count']}"
    )

    print(
        "Application Topics    : "
        f"{kafka['application_topic_count']}"
    )

    print(
        "Missing Topics        : "
        f"{len(kafka['missing_topics'])}"
    )

    print()

    print(
        "Kafka health report   : "
        f"{OUTPUT_FILE}"
    )

    print("=" * 60)


if __name__ == "__main__":
    main()