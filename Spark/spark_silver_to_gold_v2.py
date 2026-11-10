from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from decimal import Decimal


BUCKET = "s3a://pulseflow-623045222152"

SILVER_PATH = f"{BUCKET}/silver_v2/products_events/"
GOLD_CURRENT_PATH = f"{BUCKET}/gold_v2/products_current/"
GOLD_METRICS_PATH = f"{BUCKET}/gold_v2/category_inventory_metrics/"


spark = (
    SparkSession.builder
    .appName("StrataFlow-Silver-to-Gold-V2")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
    )
    .config("spark.hadoop.fs.s3a.endpoint.region", "ap-south-1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


try:
    print("\n===== STRATAFLOW GOLD V2 =====")

    # 1. Read validated Silver event history
    silver = spark.read.parquet(SILVER_PATH)

    silver_count = silver.count()
    print(f"Silver event count: {silver_count}")

    # 2. Select the latest event for each product.
    # event_timestamp is the business event ordering field.
    # operation is a deterministic secondary sort for timestamp ties.
    window = (
        Window
        .partitionBy("product_id")
        .orderBy(
            F.col("event_timestamp").desc(),
            F.col("processed_at").desc(),
            F.col("operation").desc()
        )
    )

    latest_events = (
        silver
        .withColumn("row_num", F.row_number().over(window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    latest_count = latest_events.count()
    print(f"Latest product events: {latest_count}")

    # 3. Exclude products whose latest operation is delete.
    current_products = (
        latest_events
        .filter(F.col("operation") != "d")
        .select(
            "product_id",
            "name",
            "category",
            "price",
            "stock",
            "created_at",
            "event_timestamp",
            "operation",
            "processed_at"
        )
        .withColumn(
            "inventory_value",
            F.round(
                F.col("price") * F.col("stock"),
                2
            ).cast("decimal(18,2)")
        )
        .withColumn("gold_processed_at", F.current_timestamp())
    )

    current_count = current_products.count()
    print(f"Current active products: {current_count}")

    # 4. Build category-level inventory metrics.
    category_metrics = (
        current_products
        .groupBy("category")
        .agg(
            F.countDistinct("product_id").alias("product_count"),
            F.sum("stock").alias("total_stock_units"),
            F.round(
                F.sum(F.col("price") * F.col("stock")),
                2
            ).cast("decimal(20,2)").alias("inventory_value"),
            F.round(
                F.avg("price"),
                2
            ).cast("decimal(12,2)").alias("avg_product_price")
        )
        .withColumn("gold_processed_at", F.current_timestamp())
        .orderBy("category")
    )

    print("\n===== CURRENT PRODUCTS PREVIEW =====")
    current_products.orderBy("product_id").show(50, truncate=False)

    print("\n===== CATEGORY INVENTORY METRICS =====")
    category_metrics.show(50, truncate=False)

    # 5. Validate the expected output before writing.
    if current_count == 0:
        raise ValueError("Gold current products is empty. Stopping.")

    if current_products.filter(
        F.col("product_id").isNull() |
        F.col("price").isNull() |
        F.col("stock").isNull()
    ).limit(1).count() > 0:
        raise ValueError("Gold current products contains null critical fields.")

    if current_products.select("product_id").distinct().count() != current_count:
        raise ValueError("Duplicate product IDs found in Gold current products.")

    # 6. Write without overwriting existing data.
    print("\n===== WRITING GOLD V2 =====")

    current_products.write \
        .mode("errorifexists") \
        .parquet(GOLD_CURRENT_PATH)

    category_metrics.write \
        .mode("errorifexists") \
        .parquet(GOLD_METRICS_PATH)

    print("\n===== GOLD V2 WRITE COMPLETED =====")
    print(f"Products path: {GOLD_CURRENT_PATH}")
    print(f"Metrics path:  {GOLD_METRICS_PATH}")

finally:
    spark.stop()
