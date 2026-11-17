from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("Verify-StrataFlow-Bronze-V2")
    .master("local[*]")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
    )
    .config("spark.hadoop.fs.s3a.endpoint.region", "ap-south-1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

path = "s3a://pulseflow-623045222152/bronze_v2/products/"

df = spark.read.parquet(path)

print("\n===== BRONZE V2: TEST EVENT =====")

result = (
    df.filter(
        (col("after.product_id") == 3) &
        (col("after.stock") == 33)
    )
    .select(
        col("after.product_id").alias("product_id"),
        col("after.name").alias("name"),
        col("after.stock").alias("stock"),
        col("operation"),
        col("event_timestamp")
    )
)

result.show(truncate=False)

print("Matching event count:", result.count())

spark.stop()
