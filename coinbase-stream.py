# A wrapper library that abstracts the process of communicating with coinbase advance tradaer.
from coinbase_advanced_trader import EnhancedRESTClient

# List of prices we plan on tracking
watch_list = ["BTC-USDC", "ETH-USDC"]

client = EnhancedRESTClient()

def print_price(update):
    print(f"{update.product_id}: {update.price}")

try:
    client.watch_ticker(
        watch_list,
        seconds=60*60*24, #Keeps the websocket open for 24 hours.
        callback=print_price,
        print_prices=False
    )
except KeyboardInterrupt:
    print("Stopping stream...")