from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

spark = (
        SparkSession.builder.appName("CoinbaseStream")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
        .getOrCreate()
    )
spark.sparkContext.setLogLevel("WARN")

payload_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("price", StringType(), True),
    StructField("best_bid", StringType(), True),
    StructField("best_ask", StringType(), True)
])

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092") 
    .option("subscribe", "eth,btc")
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = (
    kafka_df
    .selectExpr("CAST(value as STRING)")
    .select(from_json(col("value"), payload_schema).alias("data"))
    .select("data.*")
)

query = (
    parsed_df
    .writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()