from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("StrataFlow-S3-ReadOnly-Check")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
    )
    .config("spark.hadoop.fs.s3a.endpoint.region", "ap-south-1")
    .getOrCreate()
)

try:
    path = "s3a://pulseflow-623045222152/bronze_v2/products/"

    print("\n===== STRATAFLOW S3 READ TEST =====")
    df = spark.read.parquet(path)

    print("S3_READ_OK")
    print("Schema:")
    df.printSchema()

    print("Sample existing Bronze records:")
    df.show(5, truncate=False)

finally:
    spark.stop()
