import asyncio
from alpaca.data.live.stock import StockDataStream

# 1. Initialize the client with your credentials
# Free tier users use the 'iex' feed by default
API_KEY = "PKWBAOKBV2OZT3D456LYQ3TRX4"
SECRET_KEY = "GQuRFGWYm6W6Z6cJMURVaCJPkhiXFh6gnE1UMGAJrGFu"

stream = StockDataStream(API_KEY, SECRET_KEY)

# 2. Define an async handler for incoming data
async def handle_trade(data):
    print(f"Trade: {data.symbol} | Price: ${data.price} | Size: {data.size}")

async def handle_quote(data):
    print(f"Quote: {data.symbol} | Bid: ${data.bid_price} | Ask: ${data.ask_price}")

# 3. Subscribe to specific symbols and data types
stream.subscribe_trades(handle_trade, "AAPL", "TSLA")
stream.subscribe_quotes(handle_quote, "IBM")

# 4. Run the stream
stream.run()


import requests

url = "https://paper-api.alpaca.markets/v2/account"

headers = {
    "accept": "application/json",
    "APCA-API-KEY-ID": "PKWBAOKBV2OZT3D456LYQ3TRX4",
    "APCA-API-SECRET-KEY": "GQuRFGWYm6W6Z6cJMURVaCJPkhiXFh6gnE1UMGAJrGFu"
}

response = requests.get(url, headers=headers)

print(response.text)