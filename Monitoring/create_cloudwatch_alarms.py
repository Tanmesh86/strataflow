import boto3

REGION = "ap-south-1"
NAMESPACE = "StrataFlow/Pipeline"
DIMENSIONS = [{"Name": "Project", "Value": "StrataFlow"}]

cloudwatch = boto3.client("cloudwatch", region_name=REGION)

alarms = [
    {
        "AlarmName": "StrataFlow-Pipeline-Critical",
        "MetricName": "PipelineStatus",
        "ComparisonOperator": "LessThanThreshold",
        "Threshold": 0.5,
        "Statistic": "Minimum",
    },
    {
        "AlarmName": "StrataFlow-DLQ-High",
        "MetricName": "DLQRecords",
        "ComparisonOperator": "GreaterThanThreshold",
        "Threshold": 20,
        "Statistic": "Maximum",
    },
    {
        "AlarmName": "StrataFlow-AI-Reconciliation-Failure",
        "MetricName": "AIReconciliationMismatches",
        "ComparisonOperator": "GreaterThanThreshold",
        "Threshold": 0,
        "Statistic": "Maximum",
    },
]

for alarm in alarms:
    cloudwatch.put_metric_alarm(
        AlarmName=alarm["AlarmName"],
        AlarmDescription=f"StrataFlow monitoring alert for {alarm['MetricName']}",
        Namespace=NAMESPACE,
        MetricName=alarm["MetricName"],
        Dimensions=DIMENSIONS,
        Period=300,
        EvaluationPeriods=1,
        DatapointsToAlarm=1,
        Statistic=alarm["Statistic"],
        ComparisonOperator=alarm["ComparisonOperator"],
        Threshold=alarm["Threshold"],
        TreatMissingData="notBreaching",
    )
    print(f"Created/updated: {alarm['AlarmName']}")

print("\nAll 3 CloudWatch alarms configured.")
