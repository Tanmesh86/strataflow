from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CheckKafkaRaw")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "pulseflow-kafka:9092")
    .option("subscribe", "pulseflow.public.products")
    .option("startingOffsets", "earliest")
    .load()
)

print("========================================")
print("RAW KAFKA MESSAGE")
print("========================================")

df.selectExpr(
    "CAST(value AS STRING) AS value"
).show(20, truncate=False)

spark.stop()