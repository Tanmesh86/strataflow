from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count

spark = SparkSession.builder.appName("StrataFlow-Order-Reconciliation").getOrCreate()

base = "s3a://pulseflow-623045222152/silver_v2/"

orders = (
    spark.read.parquet(base + "orders_events/")
    .filter((col("order_id") == 6) & (col("operation") == "c"))
)

items = (
    spark.read.parquet(base + "order_items_events/")
    .filter((col("order_id") == 6) & (col("operation") == "c"))
)

payments = (
    spark.read.parquet(base + "payments_events/")
    .filter((col("order_id") == 6) & (col("operation") == "c"))
)

order_rows = orders.select("total_amount").collect()
item_result = items.agg(
    count("*").alias("item_count"),
    spark_sum(col("quantity") * col("unit_price")).alias("item_total")
).first()

payment_result = (
    payments.filter(col("status") == "SUCCESS")
    .agg(
        count("*").alias("payment_count"),
        spark_sum("amount").alias("paid_total")
    )
    .first()
)

print("\n========== ORDER 6 RECONCILIATION ==========")

order_count = len(order_rows)
order_total = order_rows[0]["total_amount"] if order_count == 1 else None
item_total = item_result["item_total"]
paid_total = payment_result["paid_total"]

checks = {
    "Exactly one order event": order_count == 1,
    "At least one order item": item_result["item_count"] > 0,
    "Item total matches order total": (
        order_total is not None and item_total is not None
        and order_total == item_total
    ),
    "Exactly one successful payment": payment_result["payment_count"] == 1,
    "Successful payment matches order total": (
        order_total is not None and paid_total is not None
        and order_total == paid_total
    )
}

print(f"Order count       : {order_count}")
print(f"Order total       : {order_total}")
print(f"Item count        : {item_result['item_count']}")
print(f"Calculated items  : {item_total}")
print(f"Successful payments: {payment_result['payment_count']}")
print(f"Paid total        : {paid_total}")

print("\nCHECK RESULTS")
for name, passed in checks.items():
    print(f"{'PASS' if passed else 'FAIL'} | {name}")

print(f"\nOVERALL: {'PASS' if all(checks.values()) else 'FAIL'}")

spark.stop()
