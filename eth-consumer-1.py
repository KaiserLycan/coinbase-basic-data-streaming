# KafkaConsumer: Allows python to connect to a Kafka Server and read/consume data from a specific topic.
from kafka import KafkaConsumer
# JSON: A Standard library for manipulating/managing JSON within python.
import json

kafka_topic = "eth"
# Each consumer has its own group, so every consumer independently receives every message in the topic.
group_id = "eth-consumer-group-1"

# Creates/Instantiate a Kafka Consumer.
consumer = KafkaConsumer(
    kafka_topic,
    # Specify the servers/brokers = where kafka is hosted, in our case (the default) localhost:9092.
    bootstrap_servers='localhost:9092',
    group_id=group_id,
    # Start from the newest messages when this group has no saved position yet.
    auto_offset_reset='latest',
    # Decodes the UTF-8 bytes sent by the producer back into a Python dictionary.
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print(f"[{group_id}] Listening to topic '{kafka_topic}'...")

try:
    # Blocks and yields each new message as it arrives in the topic.
    for message in consumer:
        data = message.value
        print(f"[{group_id}] partition={message.partition} offset={message.offset} "
              f"{data['product_id']} price={data['price']} bid={data['best_bid']} ask={data['best_ask']} "
              f"time={data.get('event_time')}")
except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    # Closes the consumer and commits its last read position.
    consumer.close()
    print("Done")

# Reference: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
