from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum

spark = (
    SparkSession.builder
    .appName("StrataFlow-Bronze-V2-Audit")
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

print("\n===== BRONZE V2 SCHEMA =====")
df.printSchema()

print("\n===== CDC OPERATION COUNTS =====")
df.groupBy("operation").count().orderBy("operation").show()

print("\n===== NULL BEFORE / AFTER COUNTS =====")
df.agg(
    count("*").alias("total_events"),
    spark_sum(col("before.product_id").isNull().cast("int")).alias("null_before"),
    spark_sum(col("after.product_id").isNull().cast("int")).alias("null_after")
).show()

print("\n===== PRODUCT 3 EVENT HISTORY =====")
df.filter(
    (col("before.product_id") == 3) |
    (col("after.product_id") == 3)
).select(
    col("before.product_id").alias("before_id"),
    col("before.stock").alias("before_stock"),
    col("after.product_id").alias("after_id"),
    col("after.stock").alias("after_stock"),
    col("operation"),
    col("event_timestamp")
).orderBy("event_timestamp").show(100, truncate=False)

print("\n===== TOTAL BRONZE EVENTS =====")
print(df.count())

spark.stop()
