from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = os.getenv("COINBASE_API_KEY")
API_SECRET = os.getenv("COINBASE_API_SECRET")
