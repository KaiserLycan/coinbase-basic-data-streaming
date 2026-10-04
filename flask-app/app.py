# Flask: A lightweight web framework used to serve the dashboard page and a JSON endpoint.
from flask import Flask, jsonify, render_template
# DataStax Python driver, used to read the rows that Spark stored in Cassandra.
from cassandra.cluster import Cluster

# How many of the most recent rows to show per coin.
ROWS_PER_PRODUCT = 100

app = Flask(__name__)

# Connect to the local Cassandra node and the "coinbase" keyspace.
session = Cluster(["127.0.0.1"], port=9042).connect("coinbase")

# Prepared statements are parsed once by Cassandra and reused on every request.
# DISTINCT on the partition key lists each coin stored in the table (e.g. BTC-USD, ETH-USD).
products_query = session.prepare("SELECT DISTINCT product_id FROM ticker")
# Rows inside a partition are already sorted newest-first by the table's clustering order.
rows_query = session.prepare("""
    SELECT product_id, event_time, kafka_offset, topic, price, best_bid, best_ask,
           volume_24_h, price_percent_chg_24_h, high_24_h, low_24_h
    FROM ticker WHERE product_id = ? LIMIT ?
""")


def read_ticker_rows():
    """Returns {product_id: [row, ...]} with the newest rows first for every coin in Cassandra."""
    data = {}
    for product in sorted(row.product_id for row in session.execute(products_query)):
        data[product] = [
            {
                **row._asdict(),
                # Cassandra returns UTC times without a timezone; add "Z" so the browser shows local time.
                "event_time": row.event_time.isoformat(timespec="milliseconds") + "Z",
            }
            for row in session.execute(rows_query, (product, ROWS_PER_PRODUCT))
        ]
    return data


@app.route("/")
def index():
    # The page renders the current rows immediately, then refreshes itself from /api/ticker.
    return render_template("index.html", data=read_ticker_rows())


@app.route("/api/ticker")
def api_ticker():
    return jsonify(read_ticker_rows())


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)

# Reference: https://flask.palletsprojects.com/en/stable/quickstart/
