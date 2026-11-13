from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, coalesce, concat_ws, current_timestamp,
    length, lit, timestamp_micros, timestamp_millis, when
)

BUCKET = "s3a://pulseflow-623045222152"

spark = (
    SparkSession.builder
    .appName("StrataFlow-Business-Bronze-to-Silver-V2")
    .master("local[*]")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
    )
    .config("spark.hadoop.fs.s3a.endpoint.region", "ap-south-1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


def transform_table(table, fields, rules):
    bronze_path = f"{BUCKET}/bronze_v2/{table}/"
    silver_path = f"{BUCKET}/silver_v2/{table}_events/"
    quarantine_path = f"{BUCKET}/silver_v2/quarantine/{table}_events/"

    print(f"\n========== PROCESSING {table.upper()} ==========")

    bronze = spark.read.parquet(bronze_path)
    normalized = bronze

    for name, expression in fields.items():
        normalized = normalized.withColumn(name, expression)

    normalized = (
        normalized
        .withColumn(
            "event_timestamp",
            timestamp_millis(col("event_timestamp"))
        )
        .withColumn("processed_at", current_timestamp())
    )

    rejection_conditions = [
        when(
            col("operation").isNull() |
            (~col("operation").isin("c", "r", "u", "d")),
            lit("INVALID_CDC_OPERATION")
        ),
        when(
            col("event_timestamp").isNull(),
            lit("MISSING_EVENT_TIMESTAMP")
        )
    ]

    rejection_conditions.extend(rules)

    validated = normalized.withColumn(
        "rejection_reason",
        concat_ws("; ", *rejection_conditions)
    )

    valid = validated.filter(length(col("rejection_reason")) == 0)
    rejected = validated.filter(length(col("rejection_reason")) > 0)

    total_count = bronze.count()
    valid_count = valid.count()
    rejected_count = rejected.count()

    print(f"Bronze events: {total_count}")
    print(f"Valid events: {valid_count}")
    print(f"Quarantined events: {rejected_count}")

    print("Quarantine reasons:")
    rejected.groupBy("rejection_reason").count().show(
        truncate=False
    )

    if total_count == 0:
        raise RuntimeError(f"No Bronze events found for {table}")

    if valid_count == 0:
        raise RuntimeError(
            f"No valid events for {table}; writes cancelled"
        )

    print(f"Writing Silver: {silver_path}")
    valid.write.mode("errorifexists").parquet(silver_path)

    if rejected_count > 0:
        print(f"Writing quarantine: {quarantine_path}")
        rejected.write.mode("errorifexists").parquet(quarantine_path)

    print(f"Completed {table}")


# ORDERS
transform_table(
    "orders",
    {
        "order_id": coalesce(
            col("after.order_id"), col("before.order_id")
        ),
        "customer_id": coalesce(
            col("after.customer_id"), col("before.customer_id")
        ),
        "status": coalesce(
            col("after.status"), col("before.status")
        ),
        "raw_total_amount": coalesce(
            col("after.total_amount"), col("before.total_amount")
        ),
        "total_amount": coalesce(
            col("after.total_amount"), col("before.total_amount")
        ).cast("decimal(12,2)"),
        "created_at": timestamp_micros(
            coalesce(
                col("after.created_at"), col("before.created_at")
            )
        ),
        "updated_at": timestamp_micros(
            coalesce(
                col("after.updated_at"), col("before.updated_at")
            )
        )
    },
    [
        when(col("order_id").isNull(), lit("MISSING_ORDER_ID")),
        when(col("customer_id").isNull(), lit("MISSING_CUSTOMER_ID")),
        when(
            col("status").isNull() |
            (~col("status").isin(
                "PENDING", "CONFIRMED", "SHIPPED",
                "DELIVERED", "CANCELLED"
            )),
            lit("INVALID_ORDER_STATUS")
        ),
        when(
            col("total_amount").isNull() |
            (col("total_amount") < 0),
            lit("INVALID_TOTAL_AMOUNT")
        ),
        when(col("created_at").isNull(), lit("MISSING_CREATED_AT")),
        when(col("updated_at").isNull(), lit("MISSING_UPDATED_AT"))
    ]
)


# ORDER ITEMS
transform_table(
    "order_items",
    {
        "order_item_id": coalesce(
            col("after.order_item_id"), col("before.order_item_id")
        ),
        "order_id": coalesce(
            col("after.order_id"), col("before.order_id")
        ),
        "product_id": coalesce(
            col("after.product_id"), col("before.product_id")
        ),
        "quantity": coalesce(
            col("after.quantity"), col("before.quantity")
        ),
        "raw_unit_price": coalesce(
            col("after.unit_price"), col("before.unit_price")
        ),
        "unit_price": coalesce(
            col("after.unit_price"), col("before.unit_price")
        ).cast("decimal(10,2)")
    },
    [
        when(
            col("order_item_id").isNull(),
            lit("MISSING_ORDER_ITEM_ID")
        ),
        when(col("order_id").isNull(), lit("MISSING_ORDER_ID")),
        when(col("product_id").isNull(), lit("MISSING_PRODUCT_ID")),
        when(
            col("quantity").isNull() | (col("quantity") <= 0),
            lit("INVALID_QUANTITY")
        ),
        when(
            col("unit_price").isNull() | (col("unit_price") < 0),
            lit("INVALID_UNIT_PRICE")
        )
    ]
)


# PAYMENTS
transform_table(
    "payments",
    {
        "payment_id": coalesce(
            col("after.payment_id"), col("before.payment_id")
        ),
        "order_id": coalesce(
            col("after.order_id"), col("before.order_id")
        ),
        "raw_amount": coalesce(
            col("after.amount"), col("before.amount")
        ),
        "amount": coalesce(
            col("after.amount"), col("before.amount")
        ).cast("decimal(12,2)"),
        "status": coalesce(
            col("after.status"), col("before.status")
        ),
        "payment_method": coalesce(
            col("after.payment_method"), col("before.payment_method")
        ),
        "created_at": timestamp_micros(
            coalesce(
                col("after.created_at"), col("before.created_at")
            )
        )
    },
    [
        when(col("payment_id").isNull(), lit("MISSING_PAYMENT_ID")),
        when(col("order_id").isNull(), lit("MISSING_ORDER_ID")),
        when(
            col("amount").isNull() | (col("amount") < 0),
            lit("INVALID_PAYMENT_AMOUNT")
        ),
        when(
            col("status").isNull() |
            (~col("status").isin(
                "PENDING", "SUCCESS", "FAILED", "REFUNDED"
            )),
            lit("INVALID_PAYMENT_STATUS")
        ),
        when(
            col("payment_method").isNull() |
            (length(col("payment_method")) == 0),
            lit("MISSING_PAYMENT_METHOD")
        ),
        when(col("created_at").isNull(), lit("MISSING_CREATED_AT"))
    ]
)


print("\nAll business-table Silver transformations completed.")
spark.stop()
