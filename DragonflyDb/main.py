# main.py
import asyncio
from SubscriptionRouter import SubscriptionRouter
from config.config import CLIENTS


async def main():
    await asyncio.gather(*[client.connect_ws() for client in CLIENTS.values()])
    router = SubscriptionRouter()
    await asyncio.gather(
        router.subscribe("binance", "orderbook", symbol="btcusdt"),
        router.subscribe("bybit", "orderbook", symbol="BTCUSDT"),
        router.subscribe("okx", "orderbook", symbol="BTC-USDT"),
        router.subscribe("hyperliquid", "orderbook", symbol="BTC")
    )
#     router = SubscriptionRouter()
#     await router.init()
#     await asyncio.gather(
#         router.subscribe("okx", "orderbook", symbol="BTC-USDT"),
#         router.subscribe("hyperliquid", "orderbook", symbol="BTC")
#     )
#
if __name__ == "__main__":
    asyncio.run(main())
