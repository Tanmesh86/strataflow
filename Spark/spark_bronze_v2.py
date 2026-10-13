from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType
)

spark = (
    SparkSession.builder
    .appName("PulseFlow-Bronze-V2")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Product schema inside Debezium payload
product_schema = StructType([
    StructField("product_id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", StringType(), True),
    StructField("stock", IntegerType(), True),
    StructField("created_at", LongType(), True)
])


# Debezium payload schema
debezium_schema = StructType([
    StructField("before", product_schema, True),
    StructField("after", product_schema, True),
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True)
])


# Kafka source
kafka_df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "pulseflow-kafka:9092"
    )
    .option(
        "subscribe",
        "pulseflow_v2.public.products"
    )
    .option(
        "startingOffsets",
        "earliest"
    )
    .load()
)


# Convert Kafka value from binary to JSON string
raw_df = kafka_df.selectExpr(
    "CAST(value AS STRING) AS json_value"
)


# Debezium JSON structure:
#
# {
#   "schema": {...},
#   "payload": {
#       "before": ...,
#       "after": ...,
#       "op": "r",
#       "ts_ms": ...
#   }
# }
#
# Therefore we parse the payload object.
parsed_df = (
    raw_df
    .select(
        from_json(
            col("json_value"),
            StructType([
                StructField("payload", debezium_schema, True)
            ])
        ).alias("data")
    )
)


# Extract Debezium payload fields
bronze_df = parsed_df.select(
    col("data.payload.before").alias("before"),
    col("data.payload.after").alias("after"),
    col("data.payload.op").alias("operation"),
    col("data.payload.ts_ms").alias("event_timestamp")
)


# Write Bronze data to S3
query = (
    bronze_df
    .writeStream
    .format("parquet")
    .outputMode("append")
    .option(
        "path",
        "s3a://pulseflow-623045222152/bronze_v2/products/"
    )
    .option(
        "checkpointLocation",
        "s3a://pulseflow-623045222152/checkpoints/bronze_v2/products/"
    )
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()