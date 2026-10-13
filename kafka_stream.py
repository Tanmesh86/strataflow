from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    expr
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType
)


# ============================================================
# 1. Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("PulseFlowKafkaConsumer")
    .master("local[*]")
    .config(
        "spark.sql.streaming.checkpointLocation",
        "D:/Data/PulseFlow/checkpoints"
    )
    .config(
        "spark.hadoop.fs.file.impl",
        "org.apache.hadoop.fs.RawLocalFileSystem"
    )
    .config(
        "spark.hadoop.fs.AbstractFileSystem.file.impl",
        "org.apache.hadoop.fs.local.LocalFs"
    )
    .config(
        "spark.hadoop.fs.file.impl.disable.cache",
        "true"
    )
    .config(
        "spark.hadoop.native.lib",
        "false"
    )
    .getOrCreate()
)


# Reduce Spark logging
spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. Kafka Configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

KAFKA_TOPIC = "pulseflow.public.orders"


# ============================================================
# 3. Read CDC Events from Kafka
# ============================================================

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)


# ============================================================
# 4. Convert Kafka Binary Data to String
# ============================================================

raw_df = (
    kafka_df
    .selectExpr(
        "CAST(key AS STRING) AS kafka_key",
        "CAST(value AS STRING) AS kafka_value",
        "topic",
        "partition",
        "offset",
        "timestamp AS kafka_timestamp"
    )
)


# ============================================================
# 5. Debezium Event Schema
# ============================================================

order_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("created_at", TimestampType(), True)
])


source_schema = StructType([
    StructField("version", StringType(), True),
    StructField("connector", StringType(), True),
    StructField("name", StringType(), True),
    StructField("ts_ms", StringType(), True),
    StructField("snapshot", StringType(), True),
    StructField("db", StringType(), True),
    StructField("sequence", StringType(), True),
    StructField("schema", StringType(), True),
    StructField("table", StringType(), True)
])


debezium_schema = StructType([
    StructField("before", order_schema, True),
    StructField("after", order_schema, True),
    StructField("source", source_schema, True),
    StructField("op", StringType(), True),
    StructField("ts_ms", StringType(), True)
])


# ============================================================
# 6. Parse Debezium JSON
# ============================================================

parsed_df = (
    raw_df
    .withColumn(
        "data",
        from_json(col("kafka_value"), debezium_schema)
    )
)


# ============================================================
# 7. Extract CDC Fields
# ============================================================

cdc_df = (
    parsed_df
    .select(
        col("kafka_key"),
        col("kafka_value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("kafka_timestamp"),

        col("data.before").alias("before"),
        col("data.after").alias("after"),
        col("data.source").alias("source"),
        col("data.op").alias("operation"),
        col("data.ts_ms").alias("debezium_timestamp")
    )
)


# ============================================================
# 8. Create Clean Streaming Data
# ============================================================

clean_df = (
    cdc_df
    .withColumn(
        "order_id",
        expr("COALESCE(after.id, before.id)")
    )
    .withColumn(
        "customer_id",
        col("after.customer_id")
    )
    .withColumn(
        "status",
        col("after.status")
    )
    .withColumn(
        "total_amount",
        col("after.total_amount")
    )
    .withColumn(
        "created_at",
        col("after.created_at")
    )
    .withColumn(
        "processed_at",
        current_timestamp()
    )
    .select(
        "order_id",
        "customer_id",
        "status",
        "total_amount",
        "created_at",
        "operation",
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
        "processed_at"
    )
)


# ============================================================
# 9. Console Output
# ============================================================

query = (
    clean_df
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", 20)
    .option(
        "checkpointLocation",
        "D:/Data/PulseFlow/checkpoints/kafka_console"
    )
    .start()
)


# ============================================================
# 10. Keep Streaming Query Alive
# ============================================================

query.awaitTermination()