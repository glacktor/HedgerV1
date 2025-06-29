import asyncio
import json

class AsyncHyperliquidInfoClient:
    def __init__(self, db):
        self._db = db

    async def get_orderbook(self, symbol: str) -> dict:
        normalized_symbol = symbol.upper()
        orderbook = await self._db.get_orderbook(normalized_symbol)
        if orderbook:
            return orderbook
        return {"bids": [], "asks": []}

    async def get_order_status(self, order_id: str) -> dict:
        key = f"hyperliquidOrders:{order_id}"
        order_json = await self._db.get(key)
        if order_json:
            return json.loads(order_json)
        return None

    async def get_all_orders(self) -> dict:
        pattern = "hyperliquidOrders:*"
        keys = await self._db.db.keys(pattern)

        orders = {}
        for key in keys:
            order_data = await self._db.db.get(key)
            if order_data:
                order_info = json.loads(order_data)
                order_id = order_info.get("orderId") or key.split(":")[-1]
                orders[order_id] = order_info

        return orders

    async def delete_order(self, order_id: str) -> bool:
        try:
            await self._db.delete_order(f"hyperliquidOrders:{order_id}")
            return True
        except Exception as e:
            print(f"Ошибка при удалении ордера {order_id}: {e}")
            return False