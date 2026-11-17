from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct

spark = (
    SparkSession.builder
    .appName("StrataFlow-Bronze-Event-Inspection")
    .master("local[*]")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
    )
    .config("spark.hadoop.fs.s3a.endpoint.region", "ap-south-1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(
    "s3a://pulseflow-623045222152/bronze_v2/products/"
)

print("\n===== ALL PRODUCT EVENTS =====")
df.select(
    col("after.product_id").alias("product_id"),
    col("after.name").alias("name"),
    col("after.category").alias("category"),
    col("after.price").alias("price"),
    col("after.stock").alias("stock"),
    col("operation"),
    col("event_timestamp")
).orderBy("event_timestamp").show(100, truncate=False)

print("\n===== EVENTS PER PRODUCT =====")
df.groupBy(
    col("after.product_id").alias("product_id")
).agg(
    count("*").alias("event_count"),
    countDistinct("operation").alias("distinct_operations")
).orderBy("product_id").show(100, truncate=False)

print("\n===== PRICE VALIDATION =====")
df.select(
    col("after.price").cast("decimal(12,2)").alias("price_decimal")
).agg(
    count("*").alias("total_events"),
    count("price_decimal").alias("valid_prices")
).show()

spark.stop()
