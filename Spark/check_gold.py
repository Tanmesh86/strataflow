from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CheckGold").master("local[*]").getOrCreate()

df = spark.read.parquet("/opt/pulseflow/data/gold/products")

print("========================================")
print("GOLD SCHEMA")
print("========================================")
df.printSchema()

print("========================================")
print("GOLD COUNT")
print("========================================")
print(df.count())

print("========================================")
print("GOLD DATA")
print("========================================")
df.orderBy("product_id").show(100, truncate=False)

spark.stop()
