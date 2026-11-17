from pyspark.sql import SparkSession
from pyspark.sql.functions import col

PATH = "s3a://pulseflow-623045222152/silver_v2/products_events/"

spark = (
    SparkSession.builder
    .appName("StrataFlow-Silver-V2-Readback-Verification")
    .master("local[*]")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
    )
    .config("spark.hadoop.fs.s3a.endpoint.region", "ap-south-1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

silver_df = spark.read.parquet(PATH)

print("\n===== SILVER V2 SCHEMA =====")
silver_df.printSchema()

print("\n===== TOTAL ROW COUNT =====")
print("Silver rows:", silver_df.count())

print("\n===== OPERATION COUNTS =====")
silver_df.groupBy("operation").count().orderBy("operation").show()

print("\n===== NULL CHECKS =====")
silver_df.select(
    col("product_id").isNull().cast("int").alias("null_product_id"),
    col("price").isNull().cast("int").alias("null_price"),
    col("stock").isNull().cast("int").alias("null_stock"),
    col("event_timestamp").isNull().cast("int").alias("null_event_timestamp")
).groupBy().sum().show()

print("\n===== PRODUCT 3 EVENT HISTORY =====")
silver_df.filter(
    col("product_id") == 3
).select(
    "product_id",
    "name",
    "price",
    "stock",
    "operation",
    "event_timestamp"
).orderBy("event_timestamp").show(truncate=False)

print("\n===== DISTINCT PRODUCT COUNT =====")
print("Distinct product IDs:", silver_df.select("product_id").distinct().count())

spark.stop()
