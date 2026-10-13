from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("CheckBronze")
    .master("local[*]")
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .config(
        "spark.hadoop.fs.s3a.endpoint.region",
        "ap-south-1"
    )
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
    )
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider.profile",
        "default"
    )
    .config(
        "spark.sql.parquet.mergeSchema",
        "false"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

BRONZE_PATH = "s3a://pulseflow-623045222152/bronze/products"

df = spark.read.parquet(BRONZE_PATH)

print("========================================")
print("AWS S3 BRONZE SCHEMA")
print("========================================")
df.printSchema()

print("========================================")
print("AWS S3 BRONZE COUNT")
print("========================================")
print(df.count())

print("========================================")
print("CDC EVENTS")
print("========================================")

df.select(
    col("after.product_id").alias("product_id"),
    col("after.name").alias("name"),
    col("after.stock").alias("stock"),
    col("operation"),
    col("event_timestamp")
).orderBy("event_timestamp").show(
    100,
    truncate=False
)

spark.stop()