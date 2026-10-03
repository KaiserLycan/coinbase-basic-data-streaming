# Spark SQL and Session Documentation: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/index.html
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

# Kafka Integration Guide: https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
spark = (
        SparkSession.builder.appName("CoinbaseStream")
        # Downloads the required Kafka connector jar file automatically
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
        .getOrCreate()
    )

# Suppresses noisy informational logs, keeping only warnings and errors
spark.sparkContext.setLogLevel("WARN")

# Defines the expected JSON structure of the incoming data
payload_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("price", StringType(), True),
    StructField("best_bid", StringType(), True),
    StructField("best_ask", StringType(), True)
])

# Kafka Source Stream configuration: https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#creating-a-kafka-source-stream
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092") 
    .option("subscribe", "eth,btc")
    .option("startingOffsets", "latest")
    .load()
)

# Data transformation logic
parsed_df = (
    kafka_df
    .selectExpr("CAST(value as STRING)")
    .select(from_json(col("value"), payload_schema).alias("data"))
    .select("data.*")
)

# Output Sink Guide: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks
query = (
    parsed_df
    .writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start()
)

# Keeps the streaming application running indefinitely
query.awaitTermination()