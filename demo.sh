#!/usr/bin/env bash
# Prepares a clean demo and opens a tmux session with every command pre-typed (press Enter to run each one live).
# Usage: ./demo.sh          then: tmux attach -t group15
set -euo pipefail
cd "$(dirname "$0")"
REPO="$(pwd)"
SESSION=group15

echo "[1/4] Starting Kafka and Cassandra..."
docker compose up -d kafka cassandra

echo "[2/4] Waiting for Cassandra to accept connections..."
until docker exec cassandra cqlsh -e "DESCRIBE KEYSPACES" >/dev/null 2>&1; do sleep 3; done

echo "[3/4] Creating topics and schema, clearing old rows..."
for topic in btc eth; do
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "$topic" >/dev/null
done
docker exec -i cassandra cqlsh < cassandra-schema.cql
docker exec cassandra cqlsh -e "TRUNCATE coinbase.ticker;"
rm -rf checkpoints

echo "[4/4] Building tmux session '$SESSION'..."
tmux kill-session -t "$SESSION" 2>/dev/null || true
tmux new-session -d -s "$SESSION" -n control -c "$REPO" -x 240 -y 60
tmux set -t "$SESSION" mouse on
tmux set -t "$SESSION" pane-border-status top
tmux set -t "$SESSION" pane-border-format " #{pane_title} "

# prep <pane> <title> <command>: activates the venv, then types the command without pressing Enter
prep() {
  tmux select-pane -t "$1" -T "$2"
  tmux send-keys -t "$1" "source .venv/bin/activate && clear" Enter
  [ -n "$3" ] && tmux send-keys -t "$1" "$3"
}

# Window 0: control
prep "$SESSION:control.0" "Control" "docker compose ps && docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"

# Window 1: live source (raw Coinbase WebSocket feed)
tmux new-window -t "$SESSION" -n source -c "$REPO"
prep "$SESSION:source.0" "Live source: Coinbase WebSocket" "python coinbase-stream.py"

# Window 2: Kafka (2 producers on top, 4 consumers below)
tmux new-window -t "$SESSION" -n kafka -c "$REPO"
tmux split-window -t "$SESSION:kafka.0" -v -l 60% -c "$REPO"
tmux split-window -t "$SESSION:kafka.0" -h -c "$REPO"
tmux split-window -t "$SESSION:kafka.2" -h -c "$REPO"
tmux split-window -t "$SESSION:kafka.2" -v -c "$REPO"
tmux split-window -t "$SESSION:kafka.4" -v -c "$REPO"
prep "$SESSION:kafka.0" "Producer 1 -> topic btc" "python btc-usdc-producer.py"
prep "$SESSION:kafka.1" "Producer 2 -> topic eth" "python eth-usdc-producer.py"
prep "$SESSION:kafka.2" "Consumer 1 (btc, group 1)" "python btc-consumer-1.py"
prep "$SESSION:kafka.3" "Consumer 2 (btc, group 2)" "python btc-consumer-2.py"
prep "$SESSION:kafka.4" "Consumer 3 (eth, group 1)" "python eth-consumer-1.py"
prep "$SESSION:kafka.5" "Consumer 4 (eth, group 2)" "python eth-consumer-2.py"

# Window 3: Spark Structured Streaming
tmux new-window -t "$SESSION" -n spark -c "$REPO"
prep "$SESSION:spark.0" "Spark Structured Streaming" "spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 coinbase-spark.py"

# Window 4: Cassandra (cqlsh)
tmux new-window -t "$SESSION" -n cassandra -c "$REPO"
prep "$SESSION:cassandra.0" "Cassandra (cqlsh)" "docker exec -it cassandra cqlsh"

# Window 5: Flask
tmux new-window -t "$SESSION" -n flask -c "$REPO"
prep "$SESSION:flask.0" "Flask web app" "cd flask-app && python app.py"

tmux select-window -t "$SESSION:control"
echo "Ready. Run: tmux attach -t $SESSION"
