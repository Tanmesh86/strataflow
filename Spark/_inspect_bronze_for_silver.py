from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("StrataFlow-Bronze-Inspection")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
    )
    .config("spark.hadoop.fs.s3a.endpoint.region", "ap-south-1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(
    "s3a://pulseflow-623045222152/bronze_v2/products/"
)

print("\n===== BRONZE SCHEMA =====")
df.printSchema()

print("\n===== CDC OPERATION COUNTS =====")
df.groupBy("operation").count().show()

print("\n===== SAMPLE CDC RECORDS =====")
df.select(
    "after.product_id",
    "after.name",
    "after.price",
    "after.stock",
    "after.created_at",
    "operation",
    "event_timestamp"
).show(20, truncate=False)

spark.stop()
