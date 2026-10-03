# A wrapper library that abstracts the process of communicating with coinbase advance trader.
from coinbase_advanced_trader import EnhancedRESTClient
# KafkaProducer: Allows python to instantiate a producer, connect to a Kafka Server, and Stream/Send Data to a specifc topic.
from kafka import KafkaProducer
# JSON: A Standard library for manipulating/managing JSON within python.
import json

# List of prices we plan on tracking
watch_list = ["ETH-USDC"]
kafka_topic = "eth"

# Initiate a Coinbase REST Client
client = EnhancedRESTClient()

# Creates/Instantiate a Kafka Producer.
producer = KafkaProducer(
    # Specify the servers/brokers = where kafka is hosted, in our case (the default) localhost:9092.
    bootstrap_servers='localhost:9092',
    # encodes values sent to the producer from JSON to UTF-8. Without this the producer will not be able to send the data to the broker.
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# A function that formats the output of the data received.
def stream_price(update):
    payload = {
        "product_id": update.product_id,
        "price" : str(update.price),
        "best_bid" : str(update.best_bid),
        "best_ask" : str(update.best_ask)
    }

    # Stream/send the article information to "eth" topic.
    producer.send(topic=kafka_topic, value=payload)
    print(payload)



try:
    # Connects to a websocket and stream the data
    client.watch_ticker(
        watch_list,
        seconds=60*60*24, #Keeps the websocket open for 24 hours.
        callback=stream_price,
        print_prices=False
    )
except KeyboardInterrupt:
    print("Stopping stream...")
finally:
    # Makes sure all the messages are sent.
    producer.flush()
    # Closes/Kills the produer.
    producer.close()
    # Confirm that the entire process ran successfully.
    print("Done")

# Reference: https://github.com/rhettre/coinbase-advancedtrade-python