from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = "s3a://pulseflow-623045222152/gold_v2"

spark = (
    SparkSession.builder
    .appName("StrataFlow-Gold-V2-Verification")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
    )
    .config("spark.hadoop.fs.s3a.endpoint.region", "ap-south-1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    products = spark.read.parquet(f"{BASE}/products_current/")
    metrics = spark.read.parquet(f"{BASE}/category_inventory_metrics/")

    product_count = products.count()
    category_count = metrics.count()

    print("\n===== GOLD PRODUCTS READ-BACK =====")
    print(f"Product rows: {product_count}")
    print(f"Distinct product IDs: {products.select('product_id').distinct().count()}")
    products.orderBy("product_id").show(50, truncate=False)

    print("\n===== GOLD CATEGORY METRICS READ-BACK =====")
    print(f"Category rows: {category_count}")
    metrics.orderBy("category").show(50, truncate=False)

    print("\n===== VALIDATION =====")
    print(
        "Null critical product fields:",
        products.filter(
            F.col("product_id").isNull()
            | F.col("price").isNull()
            | F.col("stock").isNull()
        ).count()
    )
    print(
        "Duplicate product IDs:",
        product_count - products.select("product_id").distinct().count()
    )
    print(
        "Null category names:",
        metrics.filter(F.col("category").isNull()).count()
    )

    print("\n===== EXPECTED COUNTS =====")
    print(f"Products expected=12, actual={product_count}")
    print(f"Categories expected=8, actual={category_count}")

    if product_count != 12 or category_count != 8:
        raise ValueError("Gold read-back count validation failed.")

    print("\nGOLD V2 READ-BACK VERIFICATION PASSED")

finally:
    spark.stop()
