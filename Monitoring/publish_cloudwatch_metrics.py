import json
from pathlib import Path
import boto3

REPORT = Path(__file__).parent / "reports" / "pipeline_health.json"
REGION = "ap-south-1"
NAMESPACE = "StrataFlow/Pipeline"

report = json.loads(REPORT.read_text(encoding="utf-8-sig"))
health = report["pipeline_health"]

status_values = {
    "HEALTHY": 1.0,
    "DEGRADED": 0.5,
    "CRITICAL": 0.0,
}

status = report.get("overall_status", "UNKNOWN").upper()
if status not in status_values:
    raise ValueError(f"Unsupported pipeline status: {status}")

metrics = [
    {"MetricName": "PipelineStatus", "Value": status_values[status], "Unit": "None"},
    {"MetricName": "DQPassRate", "Value": float(health["dq"]["pass_rate"]), "Unit": "Percent"},
    {"MetricName": "DLQRecords", "Value": float(health["incident"]["dlq_records"]), "Unit": "Count"},
    {"MetricName": "AIReconciliationMismatches", "Value": float(health["reconciliation"]["mismatches_detected"]), "Unit": "Count"},
]

for metric in metrics:
    metric["Dimensions"] = [{"Name": "Project", "Value": "StrataFlow"}]

cloudwatch = boto3.client("cloudwatch", region_name=REGION)
cloudwatch.put_metric_data(Namespace=NAMESPACE, MetricData=metrics)

print(f"Published {len(metrics)} metrics to {NAMESPACE}")
print(f"Region: {REGION}")
print(f"Pipeline status: {status}")
for metric in metrics:
    print(f'{metric["MetricName"]}: {metric["Value"]}')
