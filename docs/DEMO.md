# Demo Guide: Group 15

**Coinbase Price Watcher**: Coinbase → Kafka → Spark → Cassandra → Flask
**Members:** Joseph Rey Panti · Stiffler Yanic Yang
**Target length:** about 10 minutes

---

## 1. Checklist

### The day before
- [ ] Both members have read this guide and practised their own lines once.
- [ ] Install OBS Studio: `sudo apt install obs-studio`. GNOME's built-in recorder doesn't record audio, so don't use it.
- [ ] OBS → Sources → **Screen Capture (PipeWire)** → choose the **external monitor**. Add **Mic** and **Desktop Audio** (desktop audio captures Joseph's voice from Discord).
- [ ] Record 20 seconds as a test. Check that both voices are audible and the terminal text is readable at 1080p.
- [ ] Do a full dry run: `./demo.sh`, then go through every scene below.

### 30 minutes before
- [ ] Docker Desktop is running. The internet connection is stable (Coinbase is a live feed).
- [ ] Close heavy apps (extra browser tabs, IDEs). The pipeline needs about 4–5 GB of RAM.
- [ ] Turn on Do Not Disturb (GNOME: notification panel → Do Not Disturb).
- [ ] Join the Discord call. Keep the Discord window **off** the recorded monitor.
- [ ] Run `./demo.sh`. It starts Kafka and Cassandra, creates the topics and the table, **clears old rows**, and opens tmux with every command already typed in.
- [ ] Open a terminal on the external monitor, maximize it, make the font bigger (`Ctrl+Shift++` about 2–3 times), then run `tmux attach -t group15`.
- [ ] Prepare 3 browser tabs on the external monitor:
  1. https://www.coinbase.com/price/bitcoin (the live source website)
  2. http://localhost:4040 (Spark UI; it only loads after Spark starts)
  3. http://127.0.0.1:5000 (Flask; it only loads after Flask starts)

### 2 minutes before
- [ ] In tmux, press `Ctrl+b 0` to go to the **control** window.
- [ ] Start recording in OBS. Wait 3 seconds before speaking.

### tmux keys
| Key | Action |
|---|---|
| `Ctrl+b 0` … `Ctrl+b 5` | Jump to a window: 0 control · 1 source · 2 kafka · 3 spark · 4 cassandra · 5 flask |
| Mouse click | Select a pane (mouse mode is on) |
| `Ctrl+b z` | Zoom the current pane to full screen; press again to unzoom |
| Mouse wheel | Scroll back through a pane's output |
| `Enter` | Run the pre-typed command in the selected pane |
| `Ctrl+c` | Stop the program in the selected pane |

---

## 2. Files in the demo

| File | What it does |
|---|---|
| `coinbase-stream.py` | Connects to Coinbase's public WebSocket ticker channel and prints raw BTC and ETH updates. This is the live source with no Kafka involved. |
| `btc-usdc-producer.py` | **Producer 1.** Captures BTC-USDC ticker updates, builds a JSON message with 9 fields, and sends it to the Kafka topic **`btc`**. |
| `eth-usdc-producer.py` | **Producer 2.** The same, for ETH-USDC, sending to the topic **`eth`**. |
| `btc-consumer-1.py`, `btc-consumer-2.py` | **Consumers 1 and 2.** Read topic `btc`, each in its own consumer group, so each gets every message independently. |
| `eth-consumer-1.py`, `eth-consumer-2.py` | **Consumers 3 and 4.** The same, for topic `eth`. |
| `coinbase-spark.py` | **Spark Structured Streaming.** Reads both topics, parses the JSON into a typed streaming DataFrame, prints each micro-batch, and writes it to Cassandra with `foreachBatch`. |
| `cassandra-schema.cql` | Creates the keyspace `coinbase` and the table `ticker`. Rows are grouped by `product_id` and sorted newest first by `event_time` and `kafka_offset`. |
| `flask-app/app.py` | Flask server. Reads the newest rows from Cassandra; `/` serves the page and `/api/ticker` returns JSON. |
| `flask-app/templates/index.html` | The dashboard: a BTC and an ETH card, each with a summary and a scrollable table. It refreshes from Cassandra every 3 seconds and highlights new rows. |
| `docker-compose.yaml` | Runs Kafka on port 9092 and Cassandra on port 9042 in Docker. |
| `demo.sh` | Resets the data and builds the tmux session used in this demo. |

**The 9 fields:** `product_id`, `event_time`, `price`, `best_bid`, `best_ask`, `volume_24_h`, `price_percent_chg_24_h`, `high_24_h`, `low_24_h`. Spark adds `topic` and `kafka_offset` so every stored row can be traced back to its Kafka message.

---

## 3. Workflow

```
Coinbase WebSocket (ticker channel, public, no API key)
   ├─ Producer 1 ──► Kafka topic "btc" ──┬─► Consumer 1 (btc-consumer-group-1)
   │                                     ├─► Consumer 2 (btc-consumer-group-2)
   │                                     └─┐
   └─ Producer 2 ──► Kafka topic "eth" ──┬─┼► Consumer 3 (eth-consumer-group-1)
                                         ├─┼► Consumer 4 (eth-consumer-group-2)
                                         └─┴► Spark Structured Streaming
                                                 ├─ console sink (proof of processing)
                                                 └─ foreachBatch ──► Cassandra coinbase.ticker
                                                                        └─► Flask (localhost:5000, refreshes every 3 s)
```

**Run order** (this is also the video order): source → producers → consumers → Spark → Cassandra → Flask.
Consumers and Spark start from the *latest* offset, so they pick up messages from the moment they start.

---

## 4. Demo script

**[J]** = Joseph speaks · **[S]** = Stiffler speaks · ▶ = action on screen (Stiffler drives the keyboard; Joseph narrates over Discord)

### Scene 0: Intro (≈0:30) · window 0 *control*
▶ The tmux control window is on screen.

**[S]** "Hi, we are Group 15: Stiffler Yanic Yang and Joseph Rey Panti. Our project is the Coinbase Price Watcher, a real-time pipeline that captures live Bitcoin and Ethereum prices from Coinbase, streams them through Kafka, processes them with Spark Structured Streaming, stores them in Cassandra, and displays them on a Flask webpage."

▶ Press `Enter` (runs `docker compose ps` and lists the topics).

**[S]** "Kafka and Cassandra are running in Docker. Kafka is on port 9092 and Cassandra on port 9042, and our two topics, `btc` and `eth`, already exist. Joseph will start with the data source."

### Scene 1: Live source (≈1:00) · browser tab 1, then window 1 *source*
▶ Show the Coinbase Bitcoin price page for about 5 seconds.

**[J]** "Our live data source is Coinbase. Prices change several times per second. We don't scrape the HTML page. We connect to Coinbase's public WebSocket ticker channel, which pushes every price change to us in real time and needs no API key."

▶ `Ctrl+b 1` → `Enter` (runs `coinbase-stream.py`). Let it run for about 10 seconds.

**[J]** "This script connects to the feed directly. Each update gives us the product ID, price, best bid and best ask, plus 24-hour volume, percent change, high and low. With a capture timestamp, that's 9 fields per message."

▶ `Ctrl+c`.

### Scene 2: Producers and topics (≈1:30) · window 2 *kafka*
▶ `Ctrl+b 2`. Click the top-left pane → `Enter`. Click the top-right pane → `Enter`.

**[J]** "We built two separate producers. Producer 1 on the left captures only BTC and sends to the topic `btc`. Producer 2 on the right captures only ETH and sends to `eth`. Each producer turns the update into a JSON dictionary, serializes it to UTF-8, and sends it to Kafka continuously. Each printed line is one message being sent. We split the topics by coin so the two streams never mix."

### Scene 3: Four consumers (≈1:00) · window 2 *kafka*
▶ Click each of the 4 bottom panes and press `Enter`.

**[J]** "These are our four consumers: two on `btc` and two on `eth`. Each consumer is in its own consumer group, so each one independently receives every message. You can see that Consumer 1 and Consumer 2 print the same offsets, and so do Consumers 3 and 4. This proves Kafka is receiving the data and both topics work separately: BTC consumers only see BTC, and ETH consumers only see ETH."

▶ Optional: `Ctrl+b z` on one consumer pane to zoom in, then `Ctrl+b z` again.

### Scene 4: Spark Structured Streaming (≈1:30) · window 3 *spark*, then browser tab 2
▶ `Ctrl+b 3` → `Enter`. Wait until the first `Batch:` tables appear (about 20–30 seconds).

**[J]** "Spark Structured Streaming subscribes to both topics. It reads the raw Kafka bytes, casts them to strings, and parses the JSON with a fixed schema into a streaming DataFrame. It then converts the prices to numbers and the event time to a timestamp, and keeps the source topic and Kafka offset. Each table here is one micro-batch, with rows from both `btc` and `eth`."

▶ Switch to browser tab 2 (`localhost:4040`) → **Structured Streaming** tab → click the active query.

**[J]** "This is the Spark dashboard. The query is active, and these graphs show the input rate, processing rate, and batch duration updating live. Spark also writes every micro-batch to Cassandra, which Stiffler will show next."

### Scene 5: Cassandra (≈2:00) · window 4 *cassandra*
▶ Back in the terminal: `Ctrl+b 3` and scroll briefly to a `Batch N: wrote X rows to Cassandra` line. Then `Ctrl+b 4` → `Enter` to open cqlsh.

**[S]** "Spark uses `foreachBatch` to insert each micro-batch into Cassandra with the DataStax Python driver. You can see the 'wrote X rows to Cassandra' lines. Now let's check the database."

▶ Type these one at a time, pausing after each:
```sql
DESCRIBE KEYSPACE coinbase;
```
**[S]** "This is our keyspace `coinbase`, with SimpleStrategy and replication factor 1, which suits a single-node setup."

```sql
DESCRIBE TABLE coinbase.ticker;
```
**[S]** "The `ticker` table has a column for every extracted field, plus the Kafka topic and offset. The partition key is `product_id`, so all Bitcoin rows are stored together and all Ethereum rows together. The clustering keys `event_time` and `kafka_offset` sort each coin's rows newest first."

```sql
SELECT product_id, event_time, topic, kafka_offset, price, best_bid, best_ask, price_percent_chg_24_h FROM coinbase.ticker WHERE product_id='BTC-USD' LIMIT 5;
SELECT product_id, event_time, topic, kafka_offset, price, best_bid, best_ask, price_percent_chg_24_h FROM coinbase.ticker WHERE product_id='ETH-USD' LIMIT 5;
```
**[S]** "These are the newest stored records for each coin. The offsets match the ones the consumers are printing. Times here are in UTC."

```sql
SELECT COUNT(*) FROM coinbase.ticker;
```
▶ Wait about 5 seconds, then press `↑` and `Enter` to run it again.

**[S]** "The count keeps going up, so records are being saved continuously as Spark streams them in."

### Scene 6: Flask (≈1:30) · window 5 *flask*, then browser tab 3
▶ `Ctrl+b 5` → `Enter`. Switch to browser tab 3 and refresh.

**[S]** "Finally, the Flask web app. It connects to Cassandra with the same driver, finds each coin with `SELECT DISTINCT product_id`, and reads the 100 newest rows for each one. The page shows a summary card per coin with the latest price, 24-hour change, high and low, and a scrollable table of the stored rows."

▶ Let it refresh 2–3 times, then scroll inside the BTC table.

**[S]** "Every 3 seconds the page calls our `/api/ticker` endpoint and re-reads Cassandra. New rows are highlighted in yellow and the 'Last refreshed' time updates. Times here are shown in Philippine time, so 14:38 here is 06:38 UTC in cqlsh. This completes the full pipeline: from Coinbase, through Kafka and Spark, into Cassandra, and onto this webpage."

▶ Optional: switch back to the terminal (`Ctrl+b 5`) to show the `GET /api/ticker 200` lines.

### Scene 7: Contributions and learning (≈1:30)
▶ Leave the Flask page on screen.

**[J]** "My part of the project was choosing Coinbase as the live source and writing the stream script, the two producers, the `btc` and `eth` topics, and the Spark Structured Streaming job that reads and parses both topics. I also wrote the documentation for the source, Kafka, and Spark."
**[J]** *(Learning, in your own words.)* Example: "I learned how producers serialize data for Kafka, why topics keep streams separate, and how Spark turns Kafka's raw bytes into a typed streaming DataFrame using a schema."

**[S]** "My part was the storage and display side. I extended the producers to 9 fields, added the four consumers, designed the Cassandra keyspace and table, connected Spark's output to Cassandra with `foreachBatch`, built the Flask dashboard, and wrote the documentation, screenshots, and this demo."
**[S]** *(Learning, in your own words.)* Example: "I learned how Cassandra's partition and clustering keys shape the queries you can run, why each consumer group gets its own copy of the stream, and how to match connector versions across Spark, Kafka, and Cassandra."

**[S]** "Thank you for watching. This was Group 15."

▶ Stop the recording.

---

## 5. After recording
1. `Ctrl+c` in every running pane, then `tmux kill-session -t group15`. Optionally run `docker compose down`.
2. Upload to YouTube as **Unlisted**, not Private. Title: *Group 15 – Coinbase Price Watcher: Kafka, Spark, Cassandra, Flask*.
3. Open the link in a private browser window to confirm it plays without logging in.
4. Paste the link into the Google Doc next to the member names.

## 6. Quick fixes if something breaks while recording
| Symptom | Fix |
|---|---|
| Spark: `Failed to find data source: kafka` | Run it with `--packages ...` exactly as pre-typed. |
| Flask or Spark: `NoHostAvailable` | Cassandra isn't ready yet. Wait 30 seconds and run again. |
| A producer stops printing | The Coinbase connection dropped. Press `Ctrl+c`, then `↑` and `Enter`. |
| Flask: "No rows in Cassandra yet" | Check that the producers and Spark are running. |
| `Address already in use` on port 5000 | An old Flask is still running. Run `pkill -f "python app.py"` and start again. |
| Need a clean restart | `tmux kill-session -t group15 && ./demo.sh` |
