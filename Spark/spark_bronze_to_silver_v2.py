from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    concat_ws,
    current_timestamp,
    length,
    lit,
    timestamp_micros,
    timestamp_millis,
    when
)

BUCKET = "s3a://pulseflow-623045222152"

BRONZE_PATH = f"{BUCKET}/bronze_v2/products/"
SILVER_PATH = f"{BUCKET}/silver_v2/products_events/"
QUARANTINE_PATH = f"{BUCKET}/silver_v2/quarantine/products_events/"

spark = (
    SparkSession.builder
    .appName("StrataFlow-Bronze-V2-to-Silver-V2")
    .master("local[*]")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
    )
    .config("spark.hadoop.fs.s3a.endpoint.region", "ap-south-1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("===== READING BRONZE V2 =====")

bronze_df = spark.read.parquet(BRONZE_PATH)

# Keep the original CDC before/after structures.
# For deletes, the before image is the available record.
normalized_df = (
    bronze_df
    .withColumn(
        "product_id",
        coalesce(
            col("after.product_id"),
            col("before.product_id")
        )
    )
    .withColumn(
        "name",
        coalesce(col("after.name"), col("before.name"))
    )
    .withColumn(
        "category",
        coalesce(col("after.category"), col("before.category"))
    )
    .withColumn(
        "raw_price",
        coalesce(col("after.price"), col("before.price"))
    )
    .withColumn(
        "price",
        col("raw_price").cast("decimal(12,2)")
    )
    .withColumn(
        "stock",
        coalesce(col("after.stock"), col("before.stock"))
    )
    .withColumn(
        "created_at_micros",
        coalesce(
            col("after.created_at"),
            col("before.created_at")
        )
    )
    .withColumn(
        "created_at",
        timestamp_micros(col("created_at_micros"))
    )
    .withColumn(
        "event_timestamp",
        timestamp_millis(col("event_timestamp"))
    )
    .withColumn(
        "processed_at",
        current_timestamp()
    )
)

# Build explicit validation reasons.
validated_df = normalized_df.withColumn(
    "rejection_reason",
    concat_ws(
        "; ",
        when(col("product_id").isNull(), lit("MISSING_PRODUCT_ID")),
        when(
            col("operation").isNull() |
            (~col("operation").isin("c", "r", "u", "d")),
            lit("INVALID_CDC_OPERATION")
        ),
        when(col("event_timestamp").isNull(), lit("MISSING_EVENT_TIMESTAMP")),
        when(col("price").isNull(), lit("INVALID_OR_MISSING_PRICE")),
        when(col("stock").isNull(), lit("MISSING_STOCK"))
    )
)

silver_df = (
    validated_df
    .filter(length(col("rejection_reason")) == 0)
    .select(
        "product_id",
        "name",
        "category",
        "price",
        "raw_price",
        "stock",
        "created_at",
        "operation",
        "event_timestamp",
        "processed_at",
        "before",
        "after"
    )
)

quarantine_df = (
    validated_df
    .filter(length(col("rejection_reason")) > 0)
)

print("\n===== VALIDATION SUMMARY =====")

total_count = bronze_df.count()
valid_count = silver_df.count()
rejected_count = quarantine_df.count()

print(f"Bronze events: {total_count}")
print(f"Valid Silver events: {valid_count}")
print(f"Quarantined events: {rejected_count}")

print("\n===== SILVER OPERATION COUNTS =====")
silver_df.groupBy("operation").count().orderBy("operation").show()

print("\n===== QUARANTINE REASONS =====")
quarantine_df.groupBy("rejection_reason").count().show(truncate=False)

if valid_count == 0:
    raise RuntimeError("No valid records found. Silver write cancelled.")

print("\n===== WRITING SILVER V2 =====")

(
    silver_df.write
    .mode("errorifexists")
    .parquet(SILVER_PATH)
)

print("\n===== WRITING QUARANTINE =====")

(
    quarantine_df.write
    .mode("errorifexists")
    .parquet(QUARANTINE_PATH)
)

print("\nSilver V2 transformation completed.")
print(f"Silver path: {SILVER_PATH}")
print(f"Quarantine path: {QUARANTINE_PATH}")

spark.stop()
