-- Run with: docker exec -i cassandra cqlsh < cassandra-schema.cql

-- A keyspace is Cassandra's top-level container (like a database).
-- SimpleStrategy with 1 replica is enough for a single-node local setup.
CREATE KEYSPACE IF NOT EXISTS coinbase
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- One row per ticker update streamed by Spark from the "btc" and "eth" Kafka topics.
--   Partition key  (product_id)               : groups all rows of one coin together (BTC-USD, ETH-USD).
--   Clustering keys (event_time, kafka_offset): sort rows newest-first inside each partition;
--                                               kafka_offset keeps two updates captured in the same millisecond unique.
CREATE TABLE IF NOT EXISTS coinbase.ticker (
  product_id             text,
  event_time             timestamp,
  kafka_offset           bigint,
  topic                  text,
  price                  double,
  best_bid               double,
  best_ask               double,
  volume_24_h            double,
  price_percent_chg_24_h double,
  high_24_h              double,
  low_24_h               double,
  PRIMARY KEY ((product_id), event_time, kafka_offset)
) WITH CLUSTERING ORDER BY (event_time DESC, kafka_offset DESC);
