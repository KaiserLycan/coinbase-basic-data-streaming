# Spark SQL and Session Documentation: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/index.html
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType
# DataStax Python driver, used to write each micro-batch into Cassandra: https://docs.datastax.com/en/developer/python-driver/latest/
from cassandra.cluster import Cluster
from datetime import timezone

# Kafka Integration Guide: https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
spark = (
        SparkSession.builder.appName("CoinbaseStream")
        # Downloads the required Kafka connector jar file automatically when run with `python coinbase-spark.py`
        # (must match the installed Spark 4.2.0 / Scala 2.13). With spark-submit, pass the same value to --packages instead.
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0")
        .getOrCreate()
    )

# Suppresses noisy informational logs, keeping only warnings and errors
spark.sparkContext.setLogLevel("WARN")

# Defines the expected JSON structure of the incoming data
payload_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("price", StringType(), True),
    StructField("best_bid", StringType(), True),
    StructField("best_ask", StringType(), True),
    StructField("volume_24_h", StringType(), True),
    StructField("price_percent_chg_24_h", StringType(), True),
    StructField("high_24_h", StringType(), True),
    StructField("low_24_h", StringType(), True)
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
    # Keep the source topic and offset so each row can be traced back to its Kafka stream
    .selectExpr("topic", "offset AS kafka_offset", "CAST(value as STRING)")
    .select("topic", "kafka_offset", from_json(col("value"), payload_schema).alias("data"))
    .select("topic", "kafka_offset", "data.*")
    # Convert the string fields into proper types that match the Cassandra table
    .select(
        "product_id",
        to_timestamp(col("event_time")).alias("event_time"),
        "kafka_offset",
        "topic",
        col("price").cast("double").alias("price"),
        col("best_bid").cast("double").alias("best_bid"),
        col("best_ask").cast("double").alias("best_ask"),
        col("volume_24_h").cast("double").alias("volume_24_h"),
        col("price_percent_chg_24_h").cast("double").alias("price_percent_chg_24_h"),
        col("high_24_h").cast("double").alias("high_24_h"),
        col("low_24_h").cast("double").alias("low_24_h")
    )
    # Drop malformed messages that cannot be stored (product_id and event_time make up the primary key)
    .filter(col("product_id").isNotNull() & col("event_time").isNotNull())
)

# Connect to Cassandra (keyspace and table are created by cassandra-schema.cql)
cassandra_session = Cluster(["127.0.0.1"], port=9042).connect("coinbase")
insert_statement = cassandra_session.prepare("""
    INSERT INTO ticker (product_id, event_time, kafka_offset, topic, price, best_bid, best_ask,
                        volume_24_h, price_percent_chg_24_h, high_24_h, low_24_h)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

# foreachBatch Guide: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#foreachbatch
def write_to_cassandra(batch_df, batch_id):
    rows = batch_df.collect()
    for row in rows:
        cassandra_session.execute(insert_statement, (
            # Spark returns local-time datetimes; convert to UTC so Cassandra stores the correct instant
            row.product_id, row.event_time.astimezone(timezone.utc), row.kafka_offset, row.topic, row.price, row.best_bid, row.best_ask,
            row.volume_24_h, row.price_percent_chg_24_h, row.high_24_h, row.low_24_h
        ))
    print(f"Batch {batch_id}: wrote {len(rows)} rows to Cassandra (coinbase.ticker)")

# Output Sink Guide: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks
# Sink 1: prints every micro-batch to the console as proof that Spark is processing the stream
console_query = (
    parsed_df
    .writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start()
)

# Sink 2: stores every micro-batch in Cassandra
cassandra_query = (
    parsed_df
    .writeStream
    .outputMode("append")
    .foreachBatch(write_to_cassandra)
    # Remembers which Kafka offsets were already written, so a restart continues where it left off
    .option("checkpointLocation", "checkpoints/cassandra")
    .start()
)

# Keeps the streaming application running indefinitely
spark.streams.awaitAnyTermination()
