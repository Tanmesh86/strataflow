from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("VerifyBusinessBronzeV2")
    .master("local[*]")
    .config("spark.hadoop.fs.s3a.endpoint.region", "ap-south-1")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

base = "s3a://pulseflow-623045222152/bronze_v2"

tables = ["orders", "order_items", "payments"]

for table in tables:
    print(f"\n========== {table.upper()} | ORDER 6 ==========")

    df = spark.read.parquet(f"{base}/{table}/")

    df.filter(
        col("after.order_id") == 6
    ).select(
        "after",
        "operation",
        "event_timestamp",
        "source.txId"
    ).show(20, truncate=False)

spark.stop()
