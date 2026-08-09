import asyncio
import json
import websockets

URL = "wss://stream.binance.us:9443/stream?streams=btcusdt@aggTrade"


async def main():
    async with websockets.connect(URL) as ws:
        for _ in range(5):
            msg = await ws.recv()
            print(json.dumps(json.loads(msg), indent=2))
            print("-" * 40)


asyncio.run(main())

# ===================================================================================
# -------------------------RESPONSE (DATA)-------------------------------------------
# {
#   "stream": "btcusdt@aggTrade",
#   "data": {
#     "e": "aggTrade",
#     "E": 1786292575002,
#     "s": "BTCUSDT",
#     "a": 29960159,
#     "p": "65231.11000000",
#     "q": "0.00153000",
#     "f": 31588879,
#     "l": 31588879,
#     "T": 1786292575001,
#     "m": false,
#     "M": true
#   }
# }
# ----------------------------------------
# {
#   "stream": "btcusdt@aggTrade",
#   "data": {
#     "e": "aggTrade",
#     "E": 1786292634798,
#     "s": "BTCUSDT",
#     "a": 29960160,
#     "p": "65225.03000000",
#     "q": "0.00048000",
#     "f": 31588880,
#     "l": 31588880,
#     "T": 1786292634798,
#     "m": false,
#     "M": true
#   }
# }
# ----------------------------------------
# {
#   "stream": "btcusdt@aggTrade",
#   "data": {
#     "e": "aggTrade",
#     "E": 1786292822588,
#     "s": "BTCUSDT",
#     "a": 29960161,
#     "p": "65191.61000000",
#     "q": "0.03834000",
#     "f": 31588881,
#     "l": 31588881,
#     "T": 1786292822588,
#     "m": false,
#     "M": true
#   }
# }
# ----------------------------------------