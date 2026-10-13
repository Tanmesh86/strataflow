from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType,
    DecimalType
)

# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("PulseFlowDebeziumParse")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 2. Read CDC events from Kafka
# --------------------------------------------------

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "pulseflow.public.products")
    .option("startingOffsets", "latest")
    .load()
)


# --------------------------------------------------
# 3. Convert Kafka value from binary to String
# --------------------------------------------------

raw_df = kafka_df.selectExpr(
    "CAST(value AS STRING) AS json_value"
)


# --------------------------------------------------
# 4. Define actual products table schema
#
# PostgreSQL:
#
# product_id  bigint
# name        varchar
# category    varchar
# price       numeric
# stock       integer
# created_at  timestamp
# --------------------------------------------------

product_schema = StructType([
    StructField("product_id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DecimalType(12, 2), True),
    StructField("stock", IntegerType(), True),
    StructField("created_at", StringType(), True)
])


# --------------------------------------------------
# 5. Define Debezium event schema
# --------------------------------------------------

debezium_schema = StructType([
    StructField("before", product_schema, True),
    StructField("after", product_schema, True),
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True)
])


# --------------------------------------------------
# 6. Parse Debezium JSON
# --------------------------------------------------

parsed_df = raw_df.select(
    from_json(
        col("json_value"),
        debezium_schema
    ).alias("data")
)


# --------------------------------------------------
# 7. Extract CDC fields
# --------------------------------------------------

cdc_df = parsed_df.select(
    col("data.before").alias("before"),
    col("data.after").alias("after"),
    col("data.op").alias("operation"),
    col("data.ts_ms").alias("event_timestamp")
)


# --------------------------------------------------
# 8. Write parsed CDC events to console
# --------------------------------------------------

query = (
    cdc_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", 20)
    .start()
)


# --------------------------------------------------
# 9. Keep streaming query alive
# --------------------------------------------------

query.awaitTermination()