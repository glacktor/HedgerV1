import asyncio
import redis.asyncio as aioredis
import json


class DragonFlyConnector:
    def __init__(self, exchange: str):
        self.exchange = exchange.lower()
        self.db = aioredis.Redis(
            host='localhost',
            port=6379,
            password='strongpassword',
            decode_responses=True
        )

    def _order_key(self, order_id: str) -> str:
        return f"{self.exchange}Orders:{order_id}"

    def _orderbook_bids_key(self, coin: str) -> str:
        return f"ob:{self.exchange}:{coin}:bids"

    def _orderbook_asks_key(self, coin: str) -> str:
        return f"ob:{self.exchange}:{coin}:asks"

    async def get(self, key: str):
        return await self.db.get(key)

    # === ORDER ===
    async def save_order(self, order_id: str, fill_sz: float, price: float):
        value = {
            "orderId": order_id,
            "fillSz": fill_sz,
            "price": price
        }
        await self.db.set(self._order_key(order_id), json.dumps(value))

    async def delete_order(self, order_id: str):
        await self.db.delete(self._order_key(order_id))

    async def get_order(self, order_id: str):
        value = await self.db.get(self._order_key(order_id))
        return json.loads(value) if value else None

    async def update_order_fill(self, order_id: str, fill_sz: float, price: float):
        value = {
            "orderId": order_id,
            "fillSz": fill_sz,
            "price": price
        }
        await self.db.set(self._order_key(order_id), json.dumps(value))

    # === ORDERBOOK - HASH OPTIMIZED ===
    async def save_orderbook(self, coin: str, bids: list, asks: list):
        """Сохраняет ордербук в HASH структуре - фиксированные 10+10 записей"""
        bids_key = self._orderbook_bids_key(coin)
        asks_key = self._orderbook_asks_key(coin)

        pipe = self.db.pipeline()

        # Обновляем всегда ровно 10 уровней
        for i in range(10):
            if i < len(bids):
                # Формат: "price:quantity"
                pipe.hset(bids_key, str(i), f"{bids[i][0]}:{bids[i][1]}")
            else:
                # Если уровней меньше 10, ставим пустые значения
                pipe.hset(bids_key, str(i), "0:0")

            if i < len(asks):
                pipe.hset(asks_key, str(i), f"{asks[i][0]}:{asks[i][1]}")
            else:
                pipe.hset(asks_key, str(i), "0:0")

        await pipe.execute()

    async def get_orderbook(self, coin: str):
        """Получает ордербук из HASH структуры"""
        bids_key = self._orderbook_bids_key(coin)
        asks_key = self._orderbook_asks_key(coin)

        bids_raw, asks_raw = await asyncio.gather(
            self.db.hgetall(bids_key),
            self.db.hgetall(asks_key)
        )

        if not bids_raw or not asks_raw:
            return None

        # Парсим HASH обратно в массивы
        bids = []
        asks = []

        # Сортируем по индексу (0, 1, 2, ..., 9)
        for i in range(10):
            key = str(i)

            if key in bids_raw:
                price_qty = bids_raw[key].split(':')
                price, qty = float(price_qty[0]), float(price_qty[1])
                if price > 0 and qty > 0:  # Фильтруем пустые записи
                    bids.append([price, qty])

            if key in asks_raw:
                price_qty = asks_raw[key].split(':')
                price, qty = float(price_qty[0]), float(price_qty[1])
                if price > 0 and qty > 0:  # Фильтруем пустые записи
                    asks.append([price, qty])

        return {
            "bids": bids,
            "asks": asks
        }