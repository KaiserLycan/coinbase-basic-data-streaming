# Coinbase Price Watcher

Real-time pipeline: **Coinbase WebSocket → 2 Kafka producers → topics `btc` / `eth` → 4 consumers + Spark Structured Streaming → Cassandra → Flask**.

Fields captured per ticker update: `product_id`, `event_time`, `price`, `best_bid`, `best_ask`, `volume_24_h`, `price_percent_chg_24_h`, `high_24_h`, `low_24_h`.

## Setup (once)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install coinbase-advancedtrade-python websockets websocket-client kafka-python cassandra-driver flask pyspark==4.2.0
```

Requires Docker, Java 17+ and Spark 4.2.0 (`spark-submit` on the PATH). No Coinbase API key is needed.

## Demo

`./demo.sh` resets the data and opens a tmux session (`tmux attach -t group15`) with every command pre-typed. See [docs/DEMO.md](docs/DEMO.md) for the checklist and video script.

## Run manually (one terminal per step, `source .venv/bin/activate` in each)

1. Start Kafka and Cassandra: `docker compose up -d kafka cassandra` (Cassandra needs ~1 minute to accept connections)
2. Create the topics:
   ```bash
   docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic btc
   docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic eth
   ```
3. Create the keyspace and table: `docker exec -i cassandra cqlsh < cassandra-schema.cql`
4. Consumers: `python btc-consumer-1.py`, `python btc-consumer-2.py`, `python eth-consumer-1.py`, `python eth-consumer-2.py`
5. Spark: `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 coinbase-spark.py`
   (Spark UI: http://localhost:4040 → **Structured Streaming** tab)
6. Producers: `python btc-usdc-producer.py` and `python eth-usdc-producer.py`
7. Flask: `cd flask-app && python app.py` → http://127.0.0.1:5000

## Verify

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
docker exec -it cassandra cqlsh
```

```sql
DESCRIBE TABLE coinbase.ticker;
SELECT * FROM coinbase.ticker WHERE product_id = 'BTC-USD' LIMIT 10;
SELECT * FROM coinbase.ticker WHERE product_id = 'ETH-USD' LIMIT 10;
SELECT COUNT(*) FROM coinbase.ticker;
```

Coinbase reports the USDC pairs as `BTC-USD` and `ETH-USD`, so those are the stored `product_id` values.

To start over with an empty table: `docker exec cassandra cqlsh -e "TRUNCATE coinbase.ticker;"` and delete the `checkpoints/` folder.
