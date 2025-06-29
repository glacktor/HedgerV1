import json
from CexWsClients.AsyncBinanceWSClient import AsyncBinanceWSClient
from CexWsClients.AsyncOKXWSClient import AsyncOKXWSClient
from CexWsClients.AsyncHyperliquidWSClient import AsyncHyperliquidWSClient
from CexWsClients.AsyncBybitWSClient import AsyncBybitWSClient
from config.config import CLIENTS

class SubscriptionRouter:
    def __init__(self):
        self.clients = CLIENTS

    async def subscribe(self, exchange: str, type: str, **kwargs):
        client = self.clients[exchange]
        if type == "orderbook":
            await client.subscribe_orderbook(kwargs["symbol"])
        elif type == "order":
            await client.subscribe_order(kwargs["order_id"])

    async def unsubscribe(self, exchange: str, type: str, **kwargs):
        client = self.clients[exchange]
        if type == "orderbook":
            await client.unsubscribe_orderbook(kwargs["symbol"])
        elif type == "order":
            await client.unsubscribe_order(kwargs["order_id"])
