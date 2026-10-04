# Demo Guide: Group 15

**Coinbase Price Watcher**: Coinbase → Kafka → Spark → Cassandra → Flask
**Members:** Joseph Rey Panti · Stiffler Yanic Yang
**Target length:** about 10 minutes · Presented by Stiffler alone (typing and speaking).

---

## 1. Checklist

### The day before
- [ ] Read the script out loud once while doing the steps, to get the timing right.
- [ ] Install OBS Studio: `sudo apt install obs-studio`. GNOME's built-in recorder doesn't record audio, so don't use it.
- [ ] OBS → Sources → **Screen Capture (PipeWire)** → choose the **external monitor**. Add **Mic**.
- [ ] Record 20 seconds as a test. Check that your voice is clear and the terminal text is readable.
- [ ] Ask Joseph for his contribution and learning segment (see Scene 7) before recording day.
- [ ] Do one full dry run of sections 2 and 3.

### Right before recording
- [ ] Docker Desktop is running, and the internet connection is stable.
- [ ] Close heavy apps. Turn on Do Not Disturb.
- [ ] Prepare 3 browser tabs on the external monitor:
  1. https://www.coinbase.com/price/bitcoin
  2. http://localhost:4040 (Spark UI; loads only after Spark starts)
  3. http://127.0.0.1:5000 (Flask; loads only after Flask starts)

---

## 2. Setup (off camera, about 5 minutes)

### 2.1 Start the services and reset the data
Open a terminal on the external monitor, maximize it, and make the font bigger (`Ctrl+Shift++` about 2–3 times).

```bash
cd ~/Documents/Mapua/DataSci2/FinalProj/coinbase-basic-data-streaming
```
```bash
docker compose up -d kafka cassandra
```
Wait about 1 minute for Cassandra, then check it answers. If you get a connection error, wait and run it again:
```bash
docker exec cassandra cqlsh -e "DESCRIBE KEYSPACES;"
```
Create the topics (if they already exist, this does nothing):
```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic btc
```
```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic eth
```
Create the keyspace and table:
```bash
docker exec -i cassandra cqlsh < cassandra-schema.cql
```
Empty the table and Spark's checkpoint so the demo starts from zero rows:
```bash
docker exec cassandra cqlsh -e "TRUNCATE coinbase.ticker;"
```
```bash
rm -rf checkpoints
```

### 2.2 Build the tmux layout by hand
Start tmux:
```bash
tmux new -s group15
```
Turn on the mouse (click panes, scroll) and pane titles. Press `Ctrl+b` then `:`, type each line, and press Enter:
```
set -g mouse on
set -g pane-border-status top
```

Create 6 windows. For each one, press `Ctrl+b c` to create it, then `Ctrl+b ,` to rename it:

| Window | Name | What runs there |
|---|---|---|
| 0 | `control` | Docker and topic checks (rename the first window) |
| 1 | `source` | `coinbase-stream.py` |
| 2 | `kafka` | 2 producers and 4 consumers (6 panes) |
| 3 | `spark` | `spark-submit` |
| 4 | `cassandra` | `cqlsh` |
| 5 | `flask` | `app.py` |

In the **kafka** window (`Ctrl+b 2`), make 6 panes:
1. Press `Ctrl+b %` once, then `Ctrl+b "` four times. This creates 6 panes.
2. Press `Ctrl+b Alt+5` to arrange them in an even grid (tiled layout).
3. Optional: name each pane. Click it, press `Ctrl+b :`, then type `select-pane -T "Producer 1 (btc)"`. Use these names:

| Pane | Title |
|---|---|
| Top left | Producer 1 (btc) |
| Top right | Producer 2 (eth) |
| Middle left | Consumer 1 (btc) |
| Middle right | Consumer 2 (btc) |
| Bottom left | Consumer 3 (eth) |
| Bottom right | Consumer 4 (eth) |

In **every pane of every window**, activate the Python environment and clear the screen:
```bash
source .venv/bin/activate && clear
```
Then go to the control window (`Ctrl+b 0`).

### tmux keys
| Key | Action |
|---|---|
| `Ctrl+b 0` … `Ctrl+b 5` | Jump to a window |
| Mouse click | Select a pane |
| `Ctrl+b z` | Zoom the current pane to full screen; press again to unzoom |
| Mouse wheel | Scroll a pane's output |
| `Ctrl+c` | Stop the program in the pane |

**Start the OBS recording now** and wait 3 seconds before speaking.

---

## 3. Demo: commands and script

🎙 = what you say · ▶ = what you do on screen. Tip: type the command first, then speak while it runs.

### Scene 0: Intro (≈0:30) · window `control`
▶ `Ctrl+b 0`, then type:
```bash
docker compose ps
```
```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```
🎙 "Hi, we are Group 15: Stiffler Yanic Yang and Joseph Rey Panti. Our project is the Coinbase Price Watcher, a real-time pipeline that captures live Bitcoin and Ethereum prices from Coinbase, streams them through Kafka, processes them with Spark Structured Streaming, stores them in Cassandra, and displays them on a Flask webpage. Kafka and Cassandra are running in Docker, on ports 9092 and 9042, and our two topics, `btc` and `eth`, are created. Let's start with the data source."

### Scene 1: Live source (≈1:00) · browser tab 1, then window `source`
▶ Show the Coinbase Bitcoin price page for about 5 seconds.

🎙 "Our live data source is Coinbase. Prices change several times per second. Instead of scraping the HTML page, we connect to Coinbase's public WebSocket ticker channel, which pushes every price change to us in real time and needs no API key."

▶ `Ctrl+b 1`, then type and let it run for about 10 seconds:
```bash
python coinbase-stream.py
```
🎙 "This script connects to the feed directly. Each update gives us the product ID, price, best bid and best ask. Our producers also take the 24-hour volume, percent change, high and low, and add a capture timestamp, so that's 9 fields per message."

▶ `Ctrl+c`.

### Scene 2: Producers (≈1:30) · window `kafka`, top row
▶ `Ctrl+b 2`. Click the top-left pane:
```bash
python btc-usdc-producer.py
```
▶ Click the top-right pane:
```bash
python eth-usdc-producer.py
```
🎙 "We built two separate producers. Producer 1 on the left captures only BTC and sends to the topic `btc`. Producer 2 on the right captures only ETH and sends to `eth`. Each one builds a JSON message from the update, encodes it to UTF-8, and sends it to Kafka continuously. Every printed line is one message being sent. We split the topics by coin so the two streams never mix."

### Scene 3: Four consumers (≈1:00) · window `kafka`, middle and bottom rows
▶ Click each pane and type its command:

Middle left:
```bash
python btc-consumer-1.py
```
Middle right:
```bash
python btc-consumer-2.py
```
Bottom left:
```bash
python eth-consumer-1.py
```
Bottom right:
```bash
python eth-consumer-2.py
```
🎙 "These are our four consumers: two on `btc` and two on `eth`. Each one is in its own consumer group, so each independently receives every message. Consumers 1 and 2 print the same offsets, and so do Consumers 3 and 4. This shows Kafka is receiving the data and the two topics stay separate: the BTC consumers only see BTC, and the ETH consumers only see ETH."

▶ Optional: click one consumer pane, press `Ctrl+b z` to zoom in, then `Ctrl+b z` again.

### Scene 4: Spark Structured Streaming (≈1:30) · window `spark`, then browser tab 2
▶ `Ctrl+b 3`, then type:
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 coinbase-spark.py
```
▶ Wait for the first `Batch:` tables (about 20–30 seconds). Start talking while it loads.

🎙 "Spark Structured Streaming subscribes to both topics. The `--packages` option loads Spark's Kafka connector. Spark reads the raw Kafka bytes, casts them to strings, and parses the JSON with a fixed schema into a streaming DataFrame. It converts the prices to numbers and the event time to a timestamp, and keeps the source topic and offset. Each table is one micro-batch, containing rows from both `btc` and `eth`."

▶ Switch to browser tab 2 (`localhost:4040`) → **Structured Streaming** tab → click the active query.

🎙 "This is the Spark dashboard. The query is active, and these graphs show the input rate, processing rate, and batch duration updating live. Spark also writes every micro-batch into Cassandra, which I'll show next."

### Scene 5: Cassandra (≈2:00) · window `cassandra`
▶ `Ctrl+b 3` and scroll up a little to show a line `Batch N: wrote X rows to Cassandra (coinbase.ticker)`.

🎙 "For storage, Spark uses `foreachBatch` to insert each micro-batch into Cassandra with the DataStax Python driver. You can see the 'wrote X rows to Cassandra' lines here. Now let's look at the database."

▶ `Ctrl+b 4`, then open cqlsh:
```bash
docker exec -it cassandra cqlsh
```
▶ Type each query and pause after each one:
```sql
DESCRIBE KEYSPACE coinbase;
```
🎙 "This is our keyspace `coinbase`. It uses SimpleStrategy with a replication factor of 1, which suits our single-node setup."

```sql
DESCRIBE TABLE coinbase.ticker;
```
🎙 "The `ticker` table has a column for every extracted field, plus the Kafka topic and offset. The partition key is `product_id`, so all Bitcoin rows are stored together and all Ethereum rows together. The clustering keys, `event_time` and `kafka_offset`, sort each coin's rows newest first."

```sql
SELECT product_id, event_time, topic, kafka_offset, price, best_bid, best_ask, price_percent_chg_24_h FROM coinbase.ticker WHERE product_id='BTC-USD' LIMIT 5;
```
```sql
SELECT product_id, event_time, topic, kafka_offset, price, best_bid, best_ask, price_percent_chg_24_h FROM coinbase.ticker WHERE product_id='ETH-USD' LIMIT 5;
```
🎙 "These are the newest stored records for each coin, with the same offsets the consumers are printing. Times in Cassandra are in UTC."

```sql
SELECT COUNT(*) FROM coinbase.ticker;
```
▶ Wait about 5 seconds, then press `↑` and `Enter` to run it again.

🎙 "The count keeps going up, so the streamed records are being saved continuously."

### Scene 6: Flask (≈1:30) · window `flask`, then browser tab 3
▶ `Ctrl+b 5`, then type:
```bash
cd flask-app
```
```bash
python app.py
```
▶ Switch to browser tab 3 (`http://127.0.0.1:5000`) and refresh.

🎙 "Finally, the Flask web app. It connects to Cassandra with the same driver, finds each coin with `SELECT DISTINCT product_id`, and reads the 100 newest rows for each. The page shows a summary card per coin, with the latest price, 24-hour change, high and low, and a scrollable table of the stored rows."

▶ Let it refresh 2–3 times, then scroll inside the BTC table.

🎙 "Every 3 seconds the page calls our `/api/ticker` endpoint and re-reads Cassandra. New rows are highlighted in yellow and the 'Last refreshed' time updates. The page shows Philippine time, so 14:38 here is 06:38 UTC in cqlsh. This completes the pipeline: from Coinbase, through Kafka and Spark, into Cassandra, and onto this webpage."

▶ Optional: `Ctrl+b 5` to show the `GET /api/ticker 200` request lines in Flask's log.

### Scene 7: Contributions and learning (≈1:30)
▶ Leave the Flask page on screen.

🎙 "This project was done by two members, each with an assigned part."

🎙 "**Joseph Rey Panti** chose Coinbase as the live source and built the capture side: the stream script, the two producers, the `btc` and `eth` topics, and the Spark Structured Streaming job that reads and parses both topics. He also documented the source, Kafka, and Spark sections."

🎙 "**I, Stiffler Yanic Yang**, built the storage and display side. I extended the producers to 9 fields, added the four consumers, designed the Cassandra keyspace and table, connected Spark's output to Cassandra with `foreachBatch`, built the Flask dashboard, and prepared the documentation, screenshots, and this demo."

🎙 *(Your learning, in your own words.)* Example: "I learned how Cassandra's partition and clustering keys decide which queries are possible, why each consumer group gets its own copy of the stream, and how important it is to match versions across Spark, Kafka, and Cassandra."

**Joseph's learning: pick one option**
- **Option A (recommended):** Joseph records a 30–60 second clip of himself (phone or webcam is fine) saying his contribution and what he learned. Add it at the end of the video, or insert it before your closing line.
- **Option B:** Joseph sends you 2–3 sentences, and you read them: 🎙 "Joseph's learning: …" Ask him for his own words; don't invent them.

🎙 "Thank you for watching. This was Group 15."

▶ Stop the OBS recording.

---

## 4. Files shown in the demo

| File | What it does |
|---|---|
| `coinbase-stream.py` | Connects to Coinbase's public WebSocket ticker channel and prints raw BTC and ETH updates. This is the live source on its own. |
| `btc-usdc-producer.py` | **Producer 1.** Captures BTC-USDC updates, builds a JSON message with 9 fields, and sends it to topic **`btc`**. |
| `eth-usdc-producer.py` | **Producer 2.** The same, for ETH-USDC, sending to topic **`eth`**. |
| `btc-consumer-1.py`, `btc-consumer-2.py` | **Consumers 1 and 2.** Read topic `btc`, each in its own consumer group. |
| `eth-consumer-1.py`, `eth-consumer-2.py` | **Consumers 3 and 4.** Read topic `eth`, each in its own consumer group. |
| `coinbase-spark.py` | **Spark Structured Streaming.** Reads both topics, parses the JSON into a typed DataFrame, prints each micro-batch, and writes it to Cassandra with `foreachBatch`. |
| `cassandra-schema.cql` | Creates the keyspace `coinbase` and the table `ticker`, partitioned by `product_id` and sorted newest first. |
| `flask-app/app.py` | Flask server. Reads the newest rows from Cassandra; `/` serves the page and `/api/ticker` returns JSON. |
| `flask-app/templates/index.html` | The dashboard: BTC and ETH cards with scrollable tables. Refreshes every 3 seconds and highlights new rows. |
| `docker-compose.yaml` | Runs Kafka (port 9092) and Cassandra (port 9042) in Docker. |

**The 9 fields:** `product_id`, `event_time`, `price`, `best_bid`, `best_ask`, `volume_24_h`, `price_percent_chg_24_h`, `high_24_h`, `low_24_h`. Spark adds `topic` and `kafka_offset`.

## 5. Workflow

```
Coinbase WebSocket (public ticker channel)
   ├─ Producer 1 ──► topic "btc" ──► Consumers 1 & 2, and Spark
   └─ Producer 2 ──► topic "eth" ──► Consumers 3 & 4, and Spark
Spark Structured Streaming ──► console output (proof)
                           └─► foreachBatch ──► Cassandra coinbase.ticker ──► Flask (localhost:5000)
```

---

## 6. After recording
1. Press `Ctrl+c` in every running pane. Type `exit` in cqlsh. Then run:
   ```bash
   tmux kill-session -t group15
   ```
   ```bash
   docker compose down
   ```
2. Upload to YouTube as **Unlisted**, not Private. Title: *Group 15 – Coinbase Price Watcher: Kafka, Spark, Cassandra, Flask*.
3. Open the link in a private browser window to confirm it plays without logging in.
4. Paste the link into the Google Doc next to the member names.

## 7. Quick fixes while recording
| Symptom | Fix |
|---|---|
| Spark: `Failed to find data source: kafka` | You left out `--packages ...`. Retype the full spark-submit command. |
| `ModuleNotFoundError` (kafka, cassandra, flask…) | The pane's venv isn't active. Run `source .venv/bin/activate`. |
| Spark: `No module named 'cassandra'` | Same cause: spark-submit used the system Python. Activate the venv, or start it as `PYSPARK_PYTHON=.venv/bin/python spark-submit --packages ...`. |
| Spark or Flask: `NoHostAvailable` | Cassandra isn't ready yet. Wait 30 seconds and run the command again. |
| A producer stops printing | The Coinbase connection dropped. Press `Ctrl+c`, then `↑` and `Enter`. |
| Flask page says "No rows in Cassandra yet" | Check that the producers and Spark are running. |
| `Address already in use` on port 5000 | An old Flask is still running. Run `pkill -f "python app.py"` and start again. |
