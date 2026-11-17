from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = (
    SparkSession.builder
    .appName("StrataFlow-Gold-Readiness")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
    )
    .config("spark.hadoop.fs.s3a.endpoint.region", "ap-south-1")
    .getOrCreate()
)

path = "s3a://pulseflow-623045222152/silver_v2/products_events/"
df = spark.read.parquet(path)

print("\n===== SILVER EVENT COUNT =====")
print(df.count())

print("\n===== DUPLICATE TIMESTAMPS PER PRODUCT =====")
duplicates = (
    df.groupBy("product_id", "event_timestamp")
      .agg(count("*").alias("event_count"))
      .filter(col("event_count") > 1)
)
duplicates.show(truncate=False)

print("\n===== DELETE EVENTS =====")
df.filter(col("operation") == "d").show(truncate=False)

print("\n===== LATEST EVENTS =====")
df.orderBy(col("event_timestamp").desc()).select(
    "product_id", "name", "operation", "price", "stock", "event_timestamp"
).show(20, truncate=False)

spark.stop()
