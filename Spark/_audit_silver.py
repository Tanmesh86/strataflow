from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum

spark = (
    SparkSession.builder
    .appName("StrataFlow-Silver-Audit")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
    )
    .config("spark.hadoop.fs.s3a.endpoint.region", "ap-south-1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

path = "s3a://pulseflow-623045222152/silver/products/"

df = spark.read.parquet(path)

print("\n===== SILVER SCHEMA =====")
df.printSchema()

print("\n===== TOTAL ROW COUNT =====")
print("Rows:", df.count())

print("\n===== SAMPLE RECORDS =====")
df.show(20, truncate=False)

print("\n===== NULL PRODUCT IDS =====")
print("Null IDs:", df.filter(col("product_id").isNull()).count())

print("\n===== DUPLICATE PRODUCT IDS =====")
duplicates = (
    df.groupBy("product_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
)
duplicates.show(20, truncate=False)

print("\n===== TEST PRODUCT ID 15 =====")
df.filter(col("product_id") == 15).show(truncate=False)

spark.stop()
