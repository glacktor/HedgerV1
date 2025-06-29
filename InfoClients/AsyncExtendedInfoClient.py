import json


class AsyncExtendedInfoClient:
    def __init__(self, db):
        """
        Инициализирует клиент с подключением к базе данных.

        :param db: Экземпляр DragonflyConnector для взаимодействия с базой.
        """
        self._db = db

    async def get_orderbook(self, symbol: str) -> dict:
        """
        Получает ордербук для символа из базы данных.

        :param symbol: Торговый символ (например, 'BTCUSDT')
        :return: Словарь с ордербуком или пустой, если не найден
        """
        symbol = symbol.upper()
        orderbook = await self._db.get_orderbook(symbol)
        if orderbook:
            return orderbook
        return {"bids": [], "asks": []}

    async def get_order_status(self, order_id: str) -> dict:
        """
        Получает детали заполнения ордера по его ID.

        :param order_id: Уникальный идентификатор ордера.
        :return: Словарь с деталями заполнения или None, если ордер не найден.
        """
        order_data = await self._db.get_order(str(order_id))
        return order_data

    async def get_all_orders(self) -> dict:
        """
        Получает все активные ордера из базы данных.

        :return: Словарь с order_id в качестве ключей и данными ордеров в качестве значений.
        """
        pattern = f"{self._db.exchange}Orders:*"
        keys = await self._db.db.keys(pattern)

        orders = {}
        for key in keys:
            order_data = await self._db.db.get(key)
            if order_data:
                order_info = json.loads(order_data)
                order_id = order_info.get("orderId")
                orders[order_id] = order_info

        return orders

    async def delete_order(self, order_id: str) -> bool:
        """
        Удаляет ордер из базы данных.

        :param order_id: ID ордера для удаления.
        :return: True если ордер был удален, False если не найден.
        """
        try:
            await self._db.delete_order(str(order_id))
            return True
        except Exception as e:
            print(f"Ошибка при удалении ордера {order_id}: {e}")
            return False