import json
import boto3

REGION = "ap-south-1"
DASHBOARD_NAME = "StrataFlow-Pipeline-Monitoring"
NAMESPACE = "StrataFlow/Pipeline"
DIMENSIONS = ["Project", "StrataFlow"]

def metric_widget(title, metric_name, stat="Average", y_min=0, y_max=None):
    metric = [
        NAMESPACE,
        metric_name,
        *DIMENSIONS,
        {"stat": stat, "period": 300}
    ]

    properties = {
        "title": title,
        "view": "timeSeries",
        "region": REGION,
        "period": 300,
        "stat": stat,
        "metrics": [metric],
        "yAxis": {"left": {"min": y_min}},
    }

    if y_max is not None:
        properties["yAxis"]["left"]["max"] = y_max

    return {
        "type": "metric",
        "width": 12,
        "height": 6,
        "properties": properties,
    }

dashboard = {
    "widgets": [
        {
            **metric_widget(
                "Pipeline Status (1=Healthy, 0.5=Degraded, 0=Critical)",
                "PipelineStatus",
                y_max=1
            ),
            "x": 0, "y": 0
        },
        {
            **metric_widget("Data Quality Pass Rate (%)", "DQPassRate", y_max=100),
            "x": 12, "y": 0
        },
        {
            **metric_widget("Dead Letter Queue Records", "DLQRecords"),
            "x": 0, "y": 6
        },
        {
            **metric_widget("AI Reconciliation Mismatches", "AIReconciliationMismatches"),
            "x": 12, "y": 6
        },
    ]
}

cloudwatch = boto3.client("cloudwatch", region_name=REGION)

response = cloudwatch.put_dashboard(
    DashboardName=DASHBOARD_NAME,
    DashboardBody=json.dumps(dashboard),
)

print("CloudWatch dashboard created successfully.")
print(f"Dashboard: {DASHBOARD_NAME}")
print(f"Region: {REGION}")
print(f"HTTP status: {response['ResponseMetadata']['HTTPStatusCode']}")
