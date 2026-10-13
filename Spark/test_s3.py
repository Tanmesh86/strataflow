from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("PulseFlow-S3-Test")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint.region", "ap-south-1")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
    )
    .config("spark.hadoop.fs.s3a.aws.credentials.provider.profile", "default")
    .getOrCreate()
)

data = [
    ("PulseFlow", "AWS S3", "SUCCESS"),
    ("PulseFlow", "Spark", "CONNECTED")
]

df = spark.createDataFrame(
    data,
    ["project", "service", "status"]
)

df.write.mode("overwrite").parquet(
    "s3a://pulseflow-623045222152/test/spark/"
)

print("S3 WRITE SUCCESS")

spark.stop()
