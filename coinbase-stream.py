# A wrapper library that abstracts the process of communicating with coinbase advance trader.
from coinbase_advanced_trader import EnhancedRESTClient

# List of prices we plan on tracking
watch_list = ["BTC-USDC", "ETH-USDC"]

# Initiate a Coinbase REST Client
client = EnhancedRESTClient()

# A function that formats the output of the data received.
def print_price(update):
    product_id = update.product_id
    price = update.price
    best_bid=update.best_bid
    best_ask=update.best_ask
    print(f"product_id:{product_id}")
    print(f"price:{price}")
    print(f"best_bid:{best_bid}")
    print(f"best_ask:{best_ask}")
    print("="*8)


try:
    # Connects to a websocket and stream the data
    client.watch_ticker(
        watch_list,
        seconds=60*60*24, #Keeps the websocket open for 24 hours.
        callback=print_price,
        print_prices=False
    )
except KeyboardInterrupt:
    print("Stopping stream...")

# Reference: https://github.com/rhettre/coinbase-advancedtrade-python