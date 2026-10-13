from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("PulseFlowKafkaTest")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "pulseflow.public.products")
    .option("startingOffsets", "earliest")
    .load()
)

messages = df.selectExpr(
    "CAST(key AS STRING) AS key",
    "CAST(value AS STRING) AS value",
    "topic",
    "partition",
    "offset",
    "timestamp"
)

query = (
    messages.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("checkpointLocation", "./checkpoints/kafka-test")
    .start()
)

query.awaitTermination()