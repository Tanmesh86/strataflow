from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, IntegerType
)

BUCKET = "s3a://pulseflow-623045222152"
TOPIC_PREFIX = "pulseflow_v2.public"

spark = (
    SparkSession.builder
    .appName("StrataFlow-Bronze-Business-Tables-V2")
    .master("local[*]")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
    )
    .config("spark.hadoop.fs.s3a.endpoint.region", "ap-south-1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Common Debezium source metadata
source_schema = StructType([
    StructField("version", StringType(), True),
    StructField("connector", StringType(), True),
    StructField("name", StringType(), True),
    StructField("ts_ms", LongType(), True),
    StructField("snapshot", StringType(), True),
    StructField("db", StringType(), True),
    StructField("sequence", StringType(), True),
    StructField("ts_us", LongType(), True),
    StructField("ts_ns", LongType(), True),
    StructField("schema", StringType(), True),
    StructField("table", StringType(), True),
    StructField("txId", LongType(), True),
    StructField("lsn", LongType(), True),
    StructField("xmin", LongType(), True)
])

transaction_schema = StructType([
    StructField("id", StringType(), True),
    StructField("total_order", LongType(), True),
    StructField("data_collection_order", LongType(), True)
])

orders_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("status", StringType(), True),
    StructField("total_amount", StringType(), True),
    StructField("created_at", LongType(), True),
    StructField("updated_at", LongType(), True)
])

order_items_schema = StructType([
    StructField("order_item_id", LongType(), True),
    StructField("order_id", LongType(), True),
    StructField("product_id", LongType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", StringType(), True)
])

payments_schema = StructType([
    StructField("payment_id", LongType(), True),
    StructField("order_id", LongType(), True),
    StructField("amount", StringType(), True),
    StructField("status", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("created_at", LongType(), True)
])

def start_table_stream(table_name, row_schema):
    payload_schema = StructType([
        StructField("before", row_schema, True),
        StructField("after", row_schema, True),
        StructField("source", source_schema, True),
        StructField("transaction", transaction_schema, True),
        StructField("op", StringType(), True),
        StructField("ts_ms", LongType(), True),
        StructField("ts_us", LongType(), True),
        StructField("ts_ns", LongType(), True)
    ])

    envelope_schema = StructType([
        StructField("payload", payload_schema, True)
    ])

    topic = f"{TOPIC_PREFIX}.{table_name}"
    output_path = f"{BUCKET}/bronze_v2/{table_name}/"
    checkpoint_path = f"{BUCKET}/checkpoints/bronze_v2/{table_name}/"

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "pulseflow-kafka:9092")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    raw_df = kafka_df.selectExpr(
        "CAST(value AS STRING) AS json_value"
    )

    parsed_df = raw_df.select(
        from_json(
            col("json_value"),
            envelope_schema
        ).alias("data")
    )

    bronze_df = parsed_df.select(
        col("data.payload.before").alias("before"),
        col("data.payload.after").alias("after"),
        col("data.payload.op").alias("operation"),
        col("data.payload.ts_ms").alias("event_timestamp"),
        col("data.payload.ts_us").alias("event_timestamp_us"),
        col("data.payload.source").alias("source"),
        col("data.payload.transaction").alias("transaction")
    )

    query = (
        bronze_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print(f"Started Bronze stream: {table_name}")
    print(f"Kafka topic: {topic}")
    print(f"S3 path: {output_path}")
    print(f"Checkpoint: {checkpoint_path}")

    return query


queries = [
    start_table_stream("orders", orders_schema),
    start_table_stream("order_items", order_items_schema),
    start_table_stream("payments", payments_schema)
]

print("All three StrataFlow Bronze streams started.")

spark.streams.awaitAnyTermination()
