from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("StrataFlow-Bronze-ReadOnly-Verification")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
    )
    .config("spark.hadoop.fs.s3a.endpoint.region", "ap-south-1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

path = (
    "s3a://pulseflow-623045222152/bronze_v2/products/"
    "part-00000-f8274f72-b0eb-4181-ae55-bb686656c592-c000.snappy.parquet"
)

df = spark.read.parquet(path)

print("\n===== PRODUCT ID 15 VERIFICATION =====")

result = df.filter(col("after.product_id") == 15)

result.select(
    "after.product_id",
    "after.name",
    "after.category",
    "after.price",
    "after.stock",
    "operation",
    "event_timestamp"
).show(truncate=False)

print("Matching rows:", result.count())

spark.stop()
