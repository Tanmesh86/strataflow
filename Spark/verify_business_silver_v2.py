from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("StrataFlow-Silver-Verification")
    .master("local[*]")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.profile.ProfileCredentialsProvider")
    .config("spark.hadoop.fs.s3a.endpoint.region", "ap-south-1")
    .getOrCreate()
)

base = "s3a://pulseflow-623045222152/silver_v2"

checks = [
    ("orders", "orders_events", "order_id"),
    ("order_items", "order_items_events", "order_id"),
    ("payments", "payments_events", "order_id")
]

for table, folder, key in checks:
    print(f"\n========== SILVER: {table.upper()} | ORDER 6 ==========")

    df = spark.read.parquet(f"{base}/{folder}/")
    result = df.filter(col(key) == 6)

    print("Matching event count:", result.count())

    result.select(
        "after",
        "operation",
        "source.txId",
        "event_timestamp",
        "processed_at"
    ).show(truncate=False)

spark.stop()
