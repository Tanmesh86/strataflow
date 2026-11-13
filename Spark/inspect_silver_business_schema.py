from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("StrataFlow-Silver-Schema-Inspection").getOrCreate()

base = "s3a://pulseflow-623045222152/silver_v2/"

for table in ["orders", "order_items", "payments", "products"]:
    print(f"\n========== {table.upper()} ==========")
    df = spark.read.parquet(base + table + "_events/")
    df.printSchema()
    print("Event count:", df.count())

spark.stop()
