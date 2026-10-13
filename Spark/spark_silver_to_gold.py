from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, unbase64, expr
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType
)

# --------------------------------------------------
# 1. Paths
# --------------------------------------------------

BRONZE_PATH = "/opt/pulseflow/data/bronze/products"
CHECKPOINT_PATH = "/opt/pulseflow/checkpoints/bronze/products"

# --------------------------------------------------
# 2. Spark
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("PulseFlow-Debezium-Bronze")
    .master("local[*]")
    .config("spark.hadoop.io.native.lib.available", "false")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# 3. Kafka
# --------------------------------------------------

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "pulseflow-kafka:9092")
    .option("subscribe", "pulseflow.public.products")
    .option("startingOffsets", "earliest")
    .load()
)

# --------------------------------------------------
# 4. Convert Kafka value to JSON string
# --------------------------------------------------

raw_df = kafka_df.selectExpr(
    "CAST(value AS STRING) AS json_value"
)

# --------------------------------------------------
# 5. Product schema
#
# IMPORTANT:
# price is Base64 encoded Kafka Connect Decimal.
# Therefore read it as STRING first.
# created_at is Debezium MicroTimestamp.
# --------------------------------------------------

product_schema = StructType([
    StructField("product_id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", StringType(), True),
    StructField("stock", IntegerType(), True),
    StructField("created_at", LongType(), True)
])

# --------------------------------------------------
# 6. Debezium PAYLOAD schema
# --------------------------------------------------

payload_schema = StructType([
    StructField("before", product_schema, True),
    StructField("after", product_schema, True),
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True),
    StructField("ts_us", LongType(), True),
    StructField("ts_ns", LongType(), True)
])

# --------------------------------------------------
# 7. Full Debezium envelope
#
# The actual structure is:
#
# {
#   "schema": {...},
#   "payload": {...}
# }
# --------------------------------------------------

debezium_schema = StructType([
    StructField("payload", payload_schema, True)
])

# --------------------------------------------------
# 8. Parse JSON
# --------------------------------------------------

parsed_df = raw_df.select(
    from_json(
        col("json_value"),
        debezium_schema
    ).alias("data")
)

# --------------------------------------------------
# 9. Extract payload
# --------------------------------------------------

cdc_df = parsed_df.select(
    col("data.payload.before").alias("before"),
    col("data.payload.after").alias("after"),
    col("data.payload.op").alias("operation"),
    col("data.payload.ts_ms").alias("event_timestamp")
)

# --------------------------------------------------
# 10. Write Bronze
# --------------------------------------------------

query = (
    cdc_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", BRONZE_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()