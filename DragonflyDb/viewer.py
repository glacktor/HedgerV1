# viewer.py
import asyncio
from DragonFlyConnector import DragonFlyConnector

async def print_order(exchange: str, order_id: str):
    connector = DragonFlyConnector(exchange)
    order = await connector.get_order(order_id)
    if order:
        print(f"[{exchange}] Order {order_id}: {order}")
    else:
        print(f"[{exchange}] Order {order_id} not found.")

async def print_orderbook(exchange: str, coin: str):
    connector = DragonFlyConnector(exchange)
    book = await connector.get_orderbook(coin)
    if book:
        print(f"[{exchange}] Orderbook for {coin}:\nBids: {book['bids']}\nAsks: {book['asks']}")
    else:
        print(f"[{exchange}] No orderbook data for {coin}.")

if __name__ == "__main__":
    import sys
    exchange = sys.argv[1]
    type_ = sys.argv[2]

    if type_ == "order":
        order_id = sys.argv[3]
        asyncio.run(print_order(exchange, order_id))
    elif type_ == "orderbook":
        coin = sys.argv[3]
        asyncio.run(print_orderbook(exchange, coin))
    else:
        print("Usage:")
        print("  python viewer.py binance order 123456")
        print("  python viewer.py okx orderbook BTCUSDT")
