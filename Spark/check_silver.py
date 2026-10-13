from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CheckSilver")
    .master("local[*]")
    .getOrCreate()
)

df = spark.read.parquet(
    "/opt/pulseflow/data/silver/products"
)

print("========================================")
print("SILVER SCHEMA")
print("========================================")

df.printSchema()

print("========================================")
print("SILVER COUNT")
print("========================================")

print(df.count())

print("========================================")
print("SILVER DATA")
print("========================================")

df.show(
    truncate=False
)

spark.stop()