from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType
)

BRONZE_PATH = "s3a://pulseflow-623045222152/bronze/products"
CHECKPOINT_PATH = "/opt/pulseflow/checkpoints/bronze/products"

spark = (
    SparkSession.builder
    .appName("PulseFlow-Debezium-Bronze")
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
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "pulseflow-kafka:9092")
    .option("subscribe", "pulseflow.public.products")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

raw_df = kafka_df.selectExpr(
    "CAST(value AS STRING) AS json_value"
)

product_schema = StructType([
    StructField("product_id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", StringType(), True),
    StructField("stock", IntegerType(), True),
    StructField("created_at", LongType(), True)
])

payload_schema = StructType([
    StructField("before", product_schema, True),
    StructField("after", product_schema, True),
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True),
    StructField("ts_us", LongType(), True),
    StructField("ts_ns", LongType(), True)
])

debezium_schema = StructType([
    StructField("payload", payload_schema, True)
])

parsed_df = raw_df.select(
    from_json(
        col("json_value"),
        debezium_schema
    ).alias("data")
)

cdc_df = parsed_df.select(
    col("data.payload.before").alias("before"),
    col("data.payload.after").alias("after"),
    col("data.payload.op").alias("operation"),
    col("data.payload.ts_ms").alias("event_timestamp")
)

query = (
    cdc_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", BRONZE_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .start()
)

print("PulseFlow Bronze streaming started")
print(f"Bronze S3 path: {BRONZE_PATH}")

query.awaitTermination()