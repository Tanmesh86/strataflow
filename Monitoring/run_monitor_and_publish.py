import subprocess
import sys
from pathlib import Path

MONITOR_DIR = Path(__file__).resolve().parent
PYTHON = sys.executable

steps = [
    ("Kafka health check", "kafka_health.py"),
    ("Pipeline health report", "pipeline_health_monitor.py"),
    ("ML observation collection", "ml/collect_observation.py"),
    ("CloudWatch metrics publishing", "publish_cloudwatch_metrics.py"),
]

for label, script in steps:
    print(f"\n{'=' * 55}")
    print(f"Running: {label}")
    print("=" * 55)

    subprocess.run(
        [PYTHON, str(MONITOR_DIR / script)],
        check=True,
    )

print("\nStrataFlow monitoring and CloudWatch publishing completed.")
