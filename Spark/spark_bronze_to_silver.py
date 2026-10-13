from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_unixtime,
    current_timestamp
)

spark = (
    SparkSession.builder
    .appName("PulseFlow-Bronze-V2-to-Silver")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Read clean Bronze v2
bronze_df = spark.read.parquet(
    "s3a://pulseflow-623045222152/bronze_v2/products/"
)


# Transform CDC events into Silver
silver_df = (
    bronze_df
    .filter(col("operation").isin("c", "r", "u"))
    .select(
        col("after.product_id").alias("product_id"),
        col("after.name").alias("name"),
        col("after.category").alias("category"),
        col("after.price").cast("decimal(12,2)").alias("price"),
        col("after.stock").alias("stock"),

        from_unixtime(
            col("after.created_at") / 1000000
        ).cast("timestamp").alias("created_at"),

        col("operation").alias("cdc_operation"),

        from_unixtime(
            col("event_timestamp") / 1000
        ).cast("timestamp").alias("event_timestamp"),

        current_timestamp().alias("processed_at")
    )
)


# Write Silver layer to S3
(
    silver_df
    .write
    .mode("overwrite")
    .parquet(
        "s3a://pulseflow-623045222152/silver/products/"
    )
)


print("Silver layer created successfully.")

spark.stop()