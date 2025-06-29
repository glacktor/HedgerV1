import json
import asyncio
import websockets
from typing import Dict, Optional, Callable, Any, List
from hyperliquid.exchange import Exchange
from hyperliquid.utils.constants import MAINNET_API_URL
from hyperliquid.utils.signing import OrderType
from eth_account.signers.local import LocalAccount
from hyperliquid.utils.types import Cloid
import hmac
import hashlib
import time
import base64
import zmq
import sys
import os
from eth_account import Account
from eth_account.messages import encode_defunct

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from DragonflyDb.DragonFlyConnector import DragonFlyConnector
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AsyncHyperliquidWSClient:
    def __init__(self, wallet: LocalAccount, account_address: str, base_url=MAINNET_API_URL):
        self.wallet = wallet
        self.account_address = account_address
        self.exchange = Exchange(wallet=wallet, base_url=base_url, account_address=account_address)
        self._loop = asyncio.get_event_loop() if asyncio.get_event_loop().is_running() else None
        self.ws_url = "wss://api.hyperliquid.xyz/ws"
        self.private_key = None
        self.ws_connection = None
        self.subscription_handlers = {}
        self.running = False
        self.orderbook_tasks = {}
        self.listen_task = None
        self._orderbook_cache = {}
        self.db = DragonFlyConnector("hyperliquid")
        context = zmq.Context()
        self.zmq_socket = context.socket(zmq.PUSH)
        self.zmq_socket.connect("tcp://127.0.0.1:5555")
        self.running_orders = {}
        self._listener_started = False
        self._user_fills_subscribed = False
        self.ws_response_queue = asyncio.Queue()

    async def connect_ws(self):
        """Подключается к WebSocket и запускает слушатель"""
        if self.ws_connection is None:
            await self._connect_websocket()

        # Запускаем слушатель сообщений, если еще не запущен
        if not self._listener_started:
            self.listen_task = asyncio.create_task(self._listen_for_websocket_messages())
            self._listener_started = True
            logger.info("🎧 WebSocket слушатель запущен")

            # АВТОМАТИЧЕСКИ подписываемся на UserFills сразу при подключении
            await self._subscribe_to_user_fills()

    async def _subscribe_to_user_fills(self):
        """Автоматическая подписка на UserFills при подключении"""
        if not self._user_fills_subscribed:
            subscription_msg = {
                "method": "subscribe",
                "subscription": {
                    "type": "userFills",
                    "user": self.account_address
                }
            }
            await self.ws_connection.send(json.dumps(subscription_msg))
            self._user_fills_subscribed = True
            logger.info(f"📡 Автоподписка на userFills для {self.account_address}")

    async def subscribe_order(self, order_id: str):
        """Упрощенная подписка - UserFills уже активен"""
        if order_id in self.running_orders:
            return

        # UserFills уже подключен, просто добавляем в отслеживаемые
        self.running_orders[order_id] = True
        logger.info(f"🔔 Отслеживание ордера {order_id}")

    async def _listen_for_websocket_messages(self):
        """Основной слушатель WebSocket сообщений"""
        logger.info("🔊 Запуск WebSocket слушателя...")

        while not self._is_connection_closed():
            try:
                # Получаем сообщение с таймаутом
                message = await asyncio.wait_for(self.ws_connection.recv(), timeout=30)
                data = json.loads(message)

                logger.debug(f"📨 Получено WS сообщение: {data}")

                # Обрабатываем разные типы сообщений
                await self._handle_websocket_message(data)

            except asyncio.TimeoutError:
                # Отправляем ping для поддержания соединения
                try:
                    await self.ws_connection.send(json.dumps({"type": "ping"}))
                    logger.debug("📡 Ping отправлен")
                except Exception as e:
                    logger.error(f"❌ Ошибка отправки ping: {e}")
                    break

            except websockets.exceptions.ConnectionClosed:
                logger.warning("🔌 WebSocket соединение закрыто")
                break

            except json.JSONDecodeError as e:
                logger.error(f"❌ Ошибка парсинга JSON: {e}")
                continue

            except Exception as e:
                logger.error(f"❌ Ошибка в WebSocket слушателе: {e}")
                await asyncio.sleep(1)
                continue

        logger.info("🔇 WebSocket слушатель остановлен")

    async def _handle_websocket_message(self, data: Dict):
        """Обрабатывает входящие WebSocket сообщения"""
        try:
            # Ping/Pong
            if data.get("type") == "ping":
                await self.ws_connection.send(json.dumps({"type": "pong"}))
                return

            # Ответы на запросы (с ID)
            if "id" in data:
                await self.ws_response_queue.put(json.dumps(data))
                return

            # Подписки и уведомления
            channel = data.get("channel")

            if channel == "userFills":
                await self._process_fill_event(data)

            elif channel == "l2Book":
                coin = data["data"].get("coin")
                if coin:
                    levels = data["data"].get("levels", [[], []])
                    raw_bids, raw_asks = levels[0][:10], levels[1][:10]

                    # Нормализуем: [['price', 'size']] → [[float, float]]
                    bids = [[float(b["px"]), float(b["sz"])] for b in raw_bids]
                    asks = [[float(a["px"]), float(a["sz"])] for a in raw_asks]

                    await self.db.save_orderbook(coin, bids, asks)

                    self.zmq_socket.send_json({
                        "exchange": "hyperliquid",
                        "coin": coin,
                        "bids": bids,
                        "asks": asks
                    })

        except Exception as e:
            logger.error(f"❌ Ошибка обработки WebSocket сообщения: {e}")

    @classmethod
    def from_key(cls, private_key: str):
        from eth_account import Account
        account = Account.from_key(private_key)
        return cls(wallet=account, account_address=account.address)

    async def _get_asset_index(self, symbol: str) -> int:
        """Получает индекс актива по символу из мета-информации."""
        try:
            meta = await asyncio.get_running_loop().run_in_executor(None, self.exchange.info.meta)
            universe = meta.get("universe", [])
            for i, asset in enumerate(universe):
                if asset["name"] == symbol:
                    return i
            raise ValueError(f"Символ '{symbol}' не найден в мета-информации")
        except Exception as e:
            logger.error(f"Не удалось получить индекс актива для {symbol}: {e}")
            raise

    async def get_tick_size(self, symbol: str) -> str:
        """Получает минимальный шаг изменения цены (tick size) для символа"""
        try:
            # Получаем meta информацию
            meta = await asyncio.get_running_loop().run_in_executor(None, self.exchange.info.meta)
            universe = meta.get("universe", [])

            # Ищем asset по символу
            for asset in universe:
                if asset["name"] == symbol:
                    # Получаем текущую цену для определения точности
                    ob = await self.get_orderbook(symbol, depth=1)
                    if ob["bids"]:
                        current_price = ob["bids"][0][0]

                        # Определяем количество знаков после запятой
                        price_str = str(current_price)
                        if '.' in price_str:
                            decimal_places = len(price_str.split('.')[1])
                        else:
                            decimal_places = 0

                        # Tick size = 10^(-decimal_places)
                        tick_size = 10 ** (-decimal_places)

                        # Возвращаем как строку с нужной точностью
                        return f"{tick_size:.{decimal_places}f}"

            raise ValueError(f"Symbol {symbol} not found in meta")

        except Exception as e:
            logger.error(f"❌ Ошибка получения tick size для {symbol}: {e}")
            # Возвращаем дефолтное значение
            return "0.00001"

    async def get_symbol_info(self, symbol: str) -> Dict[str, int]:
        """
        quantityPrecision = szDecimals из meta().universe
        pricePrecision = кол-во знаков после запятой первого bid
        """
        # 1) лупаем meta
        meta = await asyncio.get_running_loop().run_in_executor(None, self.exchange.info.meta)
        universe = meta.get("universe", [])
        asset = next((a for a in universe if a["name"] == symbol), None)
        if not asset:
            raise ValueError(f"Symbol {symbol} not found in meta.universe")

        quantity_precision = asset["szDecimals"]

        # 2) лезем в orderbook за ценой
        ob = await self.get_orderbook(symbol)
        price = ob["bids"][0][0]  # or ob["asks"][0][0]
        dec_part = str(price).split(".")
        price_precision = len(dec_part[1]) if len(dec_part) > 1 else 0

        return {
            "quantityPrecision": quantity_precision,
            "pricePrecision": price_precision,
        }

    async def get_orderbook(self, symbol: str, depth: int = 10) -> Dict[str, List[List[float]]]:
        # получаем уровни: [[биды],[аски]]
        ob = await asyncio.get_running_loop().run_in_executor(None, self.exchange.info.l2_snapshot, symbol)
        levels = ob.get("levels", [])
        bids_lv = levels[0] if len(levels) >= 1 else []
        asks_lv = levels[1] if len(levels) >= 2 else []
        bids = [[float(l["px"]), float(l["sz"])] for l in bids_lv[:depth]]
        asks = [[float(l["px"]), float(l["sz"])] for l in asks_lv[:depth]]
        return {"bids": bids, "asks": asks}

    async def place_limit_order(self, symbol: str, side: str, price: float, qty: float):
        """Размещает лимитный ордер через REST API"""
        try:
            if side.lower() == 'long':
                side = 'buy'
            elif side.lower() == 'short':
                side = 'sell'

            is_buy = side.lower() == "buy"
            order_type: OrderType = {"limit": {"tif": "Gtc"}}

            # Отправляем ордер через REST
            data = await asyncio.get_running_loop().run_in_executor(
                None, self.exchange.order, symbol, is_buy, qty, price, order_type
            )

            logger.info(f"📤 Placed limit {side} {symbol} {price}@{qty}")
            logger.info(f"📋 Order response: {data}")

            # Извлекаем order ID из ответа
            statuses = data.get('response', {}).get('data', {}).get('statuses', [])
            order_id = None

            if statuses:
                first = statuses[0]
                if 'filled' in first and first['filled']:
                    order_id = str(first['filled'].get('oid'))
                    # Ордер уже исполнен
                    fill_sz = float(first['filled'].get('totalSz', qty))
                    avg_price = float(first['filled'].get('avgPx', price))

                    logger.info(f"✅ Ордер {order_id} сразу исполнен: {fill_sz}@{avg_price}")

                    # Сохраняем в БД как исполненный
                    await self.db.save_order(
                        order_id=order_id,
                        fill_sz=fill_sz,
                        price=avg_price
                    )

                    # Отправляем через ZMQ
                    self.zmq_socket.send_json({
                        "exchange": "hyperliquid",
                        "type": "fill",
                        "orderId": order_id,
                        "fillSz": fill_sz,
                        "price": avg_price,
                        "status": "FILLED"
                    })

                elif 'resting' in first:
                    order_id = str(first['resting'].get('oid'))
                    logger.info(f"⏳ Ордер {order_id} размещен и ожидает исполнения")

                    # УБИРАЕМ подписку - UserFills уже активен
                    # await self.subscribe_order(order_id)

                    # Просто добавляем в отслеживаемые
                    self.running_orders[order_id] = True

                    # Сохраняем в БД
                    await self.db.save_order(
                        order_id=order_id,
                        fill_sz=0.0,
                        price=price
                    )

                    # Отправляем через ZMQ
                    self.zmq_socket.send_json({
                        "exchange": "hyperliquid",
                        "type": "order",
                        "orderId": order_id,
                        "fillSz": 0.0,
                        "price": price,
                        "status": "NEW"
                    })

            return {
                "orderId": order_id,
                "symbol": symbol,
                "side": side,
                "price": price,
                "qty": qty,
                "status": "FILLED" if statuses and 'filled' in statuses[0] else "NEW"
            }

        except Exception as e:
            logger.error(f"❌ Ошибка размещения лимитного ордера: {e}")
            raise

    async def place_market_order(self, symbol: str, side: str, qty: float):
        """Размещает маркет ордер через REST API"""
        try:
            if side.lower() == 'long':
                side = 'buy'
            elif side.lower() == 'short':
                side = 'sell'

            is_buy = side.lower() == "buy"

            # Отправляем маркет ордер через REST
            data = await asyncio.get_running_loop().run_in_executor(
                None, self.exchange.market_open, symbol, is_buy, qty
            )

            logger.info(f"📤 Placed market {side} {symbol} {qty}")
            logger.info(f"📋 Market order response: {data}")

            order_id = str(data.get("oid", data.get("orderId", "")))

            if order_id:
                # Подписываемся на обновления ордера
                await self.subscribe_order(order_id)

                # Сохраняем в БД
                await self.db.save_order(
                    order_id=order_id,
                    fill_sz=qty,  # Маркет ордер обычно заполняется сразу
                    price=0.0  # Цена будет получена из fills
                )

                # Отправляем через ZMQ
                self.zmq_socket.send_json({
                    "exchange": "hyperliquid",
                    "type": "order",
                    "orderId": order_id,
                    "fillSz": qty,
                    "price": 0.0,
                    "status": "FILLED"
                })

            return {
                "orderId": order_id,
                "symbol": symbol,
                "side": side,
                "quantity": qty,
                "status": "FILLED"
            }

        except Exception as e:
            logger.error(f"❌ Ошибка размещения маркет ордера: {e}")
            raise
    async def place_fok_order(self, symbol: str, side: str, price: float, qty: float):
        """Fill-or-Kill ордер для Hyperliquid"""
        try:
            if side.lower() in ['long', 'buy']:
                is_buy = True
            else:
                is_buy = False

            order_type: OrderType = {"limit": {"tif": "Fok"}}

            data = await asyncio.get_running_loop().run_in_executor(
                None, self.exchange.order, symbol, is_buy, qty, price, order_type
            )

            statuses = data.get('response', {}).get('data', {}).get('statuses', [])

            if statuses:
                first = statuses[0]

                if 'filled' in first and first['filled']:
                    order_id = str(first['filled'].get('oid'))
                    fill_sz = float(first['filled'].get('totalSz', qty))
                    avg_price = float(first['filled'].get('avgPx', price))

                    return {
                        "success": True,
                        "orderId": order_id,
                        "status": "FILLED",
                        "filledQty": fill_sz,
                        "avgPrice": avg_price
                    }
                else:
                    return {
                        "success": False,
                        "status": "REJECTED",
                        "reason": "FOK_REJECTED"
                    }

            return {
                "success": False,
                "status": "UNKNOWN",
                "reason": "UNKNOWN_RESPONSE"
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }

    async def close_limit_order(self, symbol: str, side: str, price: float, qty: float):
        """Размещает закрывающий лимитный ордер (reduce_only) через REST API"""
        try:
            if side.lower() == 'long':
                side = 'buy'
            elif side.lower() == 'short':
                side = 'sell'

            is_buy = side.lower() == "buy"
            order_type: OrderType = {"limit": {"tif": "Gtc"}}

            # Отправляем закрывающий ордер через REST (reduce_only = True)
            data = await asyncio.get_running_loop().run_in_executor(
                None, self.exchange.order, symbol, is_buy, qty, price, order_type, True
            )

            logger.info(f"Placed close limit {side} {symbol} {price}@{qty}")

            # Извлекаем order ID из ответа
            statuses = data.get('response', {}).get('data', {}).get('statuses', [])
            order_id = None

            if statuses:
                first = statuses[0]
                if 'filled' in first and first['filled']:
                    order_id = str(first['filled'].get('oid'))
                    # Ордер уже исполнен
                    fill_sz = float(first['filled'].get('totalSz', qty))
                    avg_price = float(first['filled'].get('avgPx', price))

                    logger.info(f"✅ Закрывающий ордер {order_id} сразу исполнен: {fill_sz}@{avg_price}")

                    # Сохраняем в БД
                    await self.db.save_order(
                        order_id=order_id,
                        fill_sz=fill_sz,
                        price=avg_price
                    )

                    # Отправляем через ZMQ
                    self.zmq_socket.send_json({
                        "exchange": "hyperliquid",
                        "type": "fill",
                        "orderId": order_id,
                        "fillSz": fill_sz,
                        "price": avg_price,
                        "status": "FILLED",
                        "reduceOnly": True
                    })

                elif 'resting' in first:
                    order_id = str(first['resting'].get('oid'))
                    logger.info(f"⏳ Закрывающий ордер {order_id} размещен и ожидает исполнения")

                    # УБИРАЕМ подписку - UserFills уже активен
                    # await self.subscribe_order(order_id)

                    # Просто добавляем в отслеживаемые
                    self.running_orders[order_id] = True

                    # Сохраняем в БД
                    await self.db.save_order(
                        order_id=order_id,
                        fill_sz=0.0,
                        price=price
                    )

                    # Отправляем через ZMQ
                    self.zmq_socket.send_json({
                        "exchange": "hyperliquid",
                        "type": "order",
                        "orderId": order_id,
                        "fillSz": 0.0,
                        "price": price,
                        "status": "NEW",
                        "reduceOnly": True
                    })

            return {
                "orderId": order_id,
                "symbol": symbol,
                "side": side,
                "price": price,
                "quantity": qty,
                "status": "FILLED" if statuses and 'filled' in statuses[0] else "NEW",
                "reduceOnly": True
            }

        except Exception as e:
            logger.error(f"Ошибка размещения закрывающего лимитного ордера: {e}")
            raise

    async def subscribe_order(self, order_id: str):
        """Подписывается на обновления ордера через WebSocket"""
        if order_id in self.running_orders:
            return

        await self._connect_websocket()

        # Подписываемся на user fills для получения обновлений по ордерам
        if not self._user_fills_subscribed:
            subscription_msg = {
                "method": "subscribe",
                "subscription": {
                    "type": "userFills",
                    "user": self.account_address
                }
            }
            await self.ws_connection.send(json.dumps(subscription_msg))
            self._user_fills_subscribed = True
            logger.info(f"📡 Подписка на userFills для {self.account_address}")

        self.running_orders[order_id] = True
        logger.info(f"🔔 Подписка на ордер {order_id}")

    async def unsubscribe_order(self, order_id: str):
        """Отписывается от обновлений ордера"""
        self.running_orders.pop(order_id, None)
        logger.info(f"🔕 Отписка от ордера {order_id}")

    async def subscribe_orderbook(self, symbol: str):
        """Подписывается на поток ордербука"""
        await self.connect_ws()
        subscription_id = f"orderbook_{symbol}"
        if subscription_id in self.subscription_handlers:
            return

        sub_msg = {
            "method": "subscribe",
            "subscription": {"type": "l2Book", "coin": symbol}
        }
        await self.ws_connection.send(json.dumps(sub_msg))
        self.subscription_handlers[subscription_id] = lambda data: None
        logger.info(f"📡 Подписка на ордербук {symbol}")

    async def unsubscribe_orderbook(self, symbol: str):
        """Отписывается от потока ордербука"""
        await self.connect_ws()
        subscription_id = f"orderbook_{symbol}"
        if subscription_id not in self.subscription_handlers:
            return

        unsub_msg = {
            "method": "unsubscribe",
            "subscription": {"type": "l2Book", "coin": symbol}
        }
        await self.ws_connection.send(json.dumps(unsub_msg))
        self.subscription_handlers.pop(subscription_id, None)
        self._orderbook_cache.pop(symbol, None)
        logger.info(f"🔕 Отписка от ордербука {symbol}")

    async def cancel_order(self, symbol: str, order_id: str):
        """Отменяет ордер"""
        try:
            await asyncio.get_running_loop().run_in_executor(
                None, self.exchange.cancel, symbol, int(order_id)
            )

            # Отписываемся от ордера при отмене
            await self.unsubscribe_order(order_id)

            logger.info(f"❌ Canceled {order_id} on {symbol}")
            return {"status": "canceled", "orderId": order_id}

        except Exception as e:
            logger.error(f"❌ Ошибка отмены ордера: {e}")
            raise

    async def get_order_status(self, symbol: str, order_id: str):
        """Получает статус ордера"""
        try:
            data = await asyncio.get_running_loop().run_in_executor(
                None, self.exchange.info.query_order_by_oid, self.account_address, int(order_id)
            )

            sz = float(data['order']['order']['sz'])
            origSz = float(data['order']['order']['origSz'])
            fillSz = origSz - sz

            # Обновляем данные в БД
            await self.db.save_order(
                order_id=order_id,
                fill_sz=fillSz,
                price=float(data['order']['order'].get('limitPx', 0))
            )

            return {
                "orderId": order_id,
                "symbol": symbol,
                "fillSz": fillSz,
                "origSz": origSz,
                "remainingSz": sz
            }

        except Exception as e:
            logger.error(f"❌ Ошибка получения статуса ордера: {e}")
            raise

    async def _process_fill_event(self, data: Dict):
        """Обрабатывает события заполнения ордеров"""
        try:
            # Правильно извлекаем массив fills из структуры данных
            data_content = data.get("data", {})

            # Проверяем, что data_content является словарем
            if not isinstance(data_content, dict):
                logger.warning(f"⚠️ data не является словарем: {data_content}")
                return

            fills = data_content.get("fills", [])

            if not isinstance(fills, list):
                logger.warning(f"⚠️ fills не является списком: {fills}")
                return

            logger.info(f"💰 Обработка {len(fills)} fill событий")

            # Логируем структуру данных для отладки
            logger.debug(f"💰 Fill data structure: {fills}")

            for fill in fills:
                # Проверяем, что fill является словарем
                if not isinstance(fill, dict):
                    logger.warning(f"⚠️ Пропускаем fill, не является словарем: {fill} (тип: {type(fill)})")
                    continue

                order_id = str(fill.get("oid", ""))

                # Проверяем наличие обязательных полей
                if not order_id:
                    logger.warning(f"⚠️ Пропускаем fill без order_id: {fill}")
                    continue

                fill_sz = float(fill.get("sz", 0))
                price = float(fill.get("px", 0))
                coin = fill.get("coin", "")
                side = fill.get("side", "")

                logger.info(f"💰 [FILL] Order {order_id}: {coin} {side} filled={fill_sz} price={price}")

                # Всегда сохраняем в БД (не только для отслеживаемых ордеров)
                await self.db.save_order(
                    order_id=order_id,
                    fill_sz=fill_sz,
                    price=price
                )

                # Отправляем через ZMQ
                self.zmq_socket.send_json({
                    "exchange": "hyperliquid",
                    "type": "fill",
                    "orderId": order_id,
                    "fillSz": fill_sz,
                    "price": price,
                    "coin": coin,
                    "side": side
                })

                # Если ордер в списке отслеживаемых, дополнительно логируем
                if order_id in self.running_orders:
                    logger.info(f"🎯 Отслеживаемый ордер {order_id} исполнен!")

        except Exception as e:
            logger.error(f"❌ Ошибка обработки fill события: {e}")
            # Дополнительное логирование для отладки
            logger.error(f"❌ Данные, вызвавшие ошибку: {data}")
            import traceback
            logger.error(f"❌ Traceback: {traceback.format_exc()}")

    async def _connect_websocket(self):
        max_retries = 5
        retry_delay = 2
        for attempt in range(max_retries):
            try:
                if self._is_connection_closed():
                    self.ws_connection = await websockets.connect(
                        self.ws_url,
                        ping_interval=20,
                        ping_timeout=10,
                        close_timeout=10
                    )
                    logger.info("🔌 WebSocket connection established")
                return self.ws_connection
            except Exception as e:
                logger.error(f"❌ WebSocket connection failed (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                else:
                    raise Exception("Failed to connect to WebSocket after retries")

    def _is_connection_closed(self):
        """Проверяет, закрыто ли WebSocket соединение"""
        if self.ws_connection is None:
            return True
        try:
            if hasattr(self.ws_connection, 'closed'):
                return self.ws_connection.closed
            elif hasattr(self.ws_connection, 'state'):
                return self.ws_connection.state.name in ['CLOSED', 'CLOSING']
            elif hasattr(self.ws_connection, 'close_code'):
                return self.ws_connection.close_code is not None
            else:
                return False
        except:
            return True

    async def close(self):
        """Правильно закрываем все соединения"""
        try:
            # Останавливаем все активные подписки
            self.running_orders.clear()
            self._listener_started = False

            # Отменяем задачу слушателя
            if self.listen_task and not self.listen_task.done():
                self.listen_task.cancel()
                try:
                    await self.listen_task
                except asyncio.CancelledError:
                    pass

            # Закрываем WebSocket соединение
            if self.ws_connection and not self._is_connection_closed():
                await self.ws_connection.close()

            # Закрываем ZMQ socket
            if hasattr(self, 'zmq_socket'):
                self.zmq_socket.close()

            logger.info("🔌 Все соединения закрыты")

        except Exception as e:
            logger.error(f"❌ Ошибка при закрытии соединений: {e}")

    async def __aenter__(self):
        await self.connect_ws()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    # Остальные методы остаются без изменений...
    async def close_limit_order(self, symbol: str, side: str, price: float, qty: float):
        """Размещает закрывающий лимитный ордер (reduce_only) через REST API"""
        try:
            if side.lower() == 'long':
                side = 'buy'
            elif side.lower() == 'short':
                side = 'sell'

            is_buy = side.lower() == "buy"
            order_type: OrderType = {"limit": {"tif": "Gtc"}}

            # Отправляем закрывающий ордер через REST (reduce_only = True)
            data = await asyncio.get_running_loop().run_in_executor(
                None, self.exchange.order, symbol, is_buy, qty, price, order_type, True
            )

            logger.info(f"Placed close limit {side} {symbol} {price}@{qty}")

            # Извлекаем order ID из ответа
            statuses = data.get('response', {}).get('data', {}).get('statuses', [])
            order_id = None

            if statuses:
                first = statuses[0]
                if 'filled' in first and first['filled']:
                    order_id = str(first['filled'].get('oid'))
                elif 'resting' in first:
                    order_id = str(first['resting'].get('oid'))

            if order_id:
                # Подписываемся на обновления ордера
                await self.subscribe_order(order_id)

                # Сохраняем в БД
                await self.db.save_order(
                    order_id=order_id,
                    fill_sz=0.0,
                    price=price
                )

                # Отправляем через ZMQ
                self.zmq_socket.send_json({
                    "exchange": "hyperliquid",
                    "type": "order",
                    "orderId": order_id,
                    "fillSz": 0.0,
                    "price": price,
                    "status": "NEW",
                    "reduceOnly": True
                })

            return {
                "orderId": order_id,
                "symbol": symbol,
                "side": side,
                "price": price,
                "quantity": qty,
                "status": "NEW",
                "reduceOnly": True
            }

        except Exception as e:
            logger.error(f"Ошибка размещения закрывающего лимитного ордера: {e}")
            raise

    async def close_market_order(self, symbol: str, side: str, qty: float):
        """Размещает закрывающий маркет ордер (reduce_only) через REST API"""
        try:
            if side.lower() == 'long':
                side = 'buy'
            elif side.lower() == 'short':
                side = 'sell'

            # Для закрытия позиции нужно торговать в противоположном направлении
            opposite_side = "sell" if side.lower() == "buy" else "buy"
            is_buy = opposite_side.lower() == "buy"

            # Отправляем закрывающий маркет ордер через REST
            data = await asyncio.get_running_loop().run_in_executor(
                None, self.exchange.market_close, symbol, qty
            )

            logger.info(f"Placed close market {side} {symbol} {qty}")

            order_id = str(data.get("oid", data.get("orderId", "")))

            if order_id:
                # Подписываемся на обновления ордера
                await self.subscribe_order(order_id)

                # Сохраняем в БД
                await self.db.save_order(
                    order_id=order_id,
                    fill_sz=qty,
                    price=0.0
                )

                # Отправляем через ZMQ
                self.zmq_socket.send_json({
                    "exchange": "hyperliquid",
                    "type": "order",
                    "orderId": order_id,
                    "fillSz": qty,
                    "price": 0.0,
                    "status": "FILLED",
                    "reduceOnly": True
                })

            return {
                "orderId": order_id,
                "symbol": symbol,
                "side": opposite_side,
                "quantity": qty,
                "status": "FILLED",
                "reduceOnly": True
            }

        except Exception as e:
            logger.error(f"Ошибка размещения закрывающего маркет ордера: {e}")
            raise

    async def set_leverage(self, symbol: str, leverage: int, margin_mode: str = "isolated", pos_side: str = None):
        """Устанавливает плечо"""
        try:
            is_cross = (margin_mode == "cross")
            await asyncio.get_running_loop().run_in_executor(
                None,
                self.exchange.update_leverage,
                symbol,
                leverage,
                is_cross,
            )

            logger.info(f"Leverage set: {symbol} x{leverage} ({margin_mode})")
            return {
                "status": "success",
                "symbol": symbol,
                "leverage": leverage,
                "margin_mode": margin_mode
            }

        except Exception as e:
            logger.error(f"Ошибка установки плеча: {e}")
            raise

    async def get_position_size(self, symbol: str, direction: str) -> float:
        """Получает размер позиции"""
        try:
            # Получаем позиции через REST API
            data = await asyncio.get_running_loop().run_in_executor(
                None, self.exchange.info.user_state, self.account_address
            )

            # Ищем позицию по символу
            asset_positions = data.get('assetPositions', [])

            for pos in asset_positions:
                if pos['position']['coin'] == symbol:
                    # szi - размер позиции (может быть строкой)
                    position_size_str = pos['position']['szi']
                    position_size = float(position_size_str)

                    # Проверяем направление
                    if direction.lower() == 'long' and position_size > 0:
                        return abs(position_size)
                    elif direction.lower() == 'short' and position_size < 0:
                        return abs(position_size)

            return 0.0

        except Exception as e:
            logger.error(f"❌ Ошибка получения размера позиции: {e}")
            return 0.0

    async def get_position_info(self, symbol: str):
        data = await asyncio.get_running_loop().run_in_executor(
            None, self.exchange.info.user_state, self.account_address
        )

        for pos in data.get('assetPositions', []):
            if pos['position']['coin'] == symbol:
                size = float(pos['position']['szi'])
                if size != 0:
                    return {
                        "avg_price": float(pos['position']['entryPx']),
                        "size": abs(size)
                    }
        return None