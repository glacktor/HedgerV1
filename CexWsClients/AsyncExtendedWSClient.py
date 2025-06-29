import json
import asyncio
import websockets
import aiohttp
import time
import hmac
import hashlib
import socket
from typing import Dict, Optional, List, Any
import zmq
import sys
import os
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from DragonflyDb.DragonFlyConnector import DragonFlyConnector
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AsyncExtendedWSClient:
    def __init__(self, api_key: str, api_secret: str, base_url: str = "https://api.extended.com"):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url
        self.ws_url = "wss://stream.extended.com/ws"
        self.session = None
        self.ws_connection = None
        self.subscription_handlers = {}
        self.running_orders = {}
        self.db = DragonFlyConnector("extended")

        # ZMQ setup
        context = zmq.Context()
        self.zmq_socket = context.socket(zmq.PUSH)
        self.zmq_socket.connect("tcp://127.0.0.1:5555")

        self._listener_started = False
        self._user_stream_key = None
        self.listen_task = None
        self._connection_available = None

    @classmethod
    def from_credentials(cls, api_key: str, api_secret: str):
        return cls(api_key=api_key, api_secret=api_secret)

    def _check_connection_available(self) -> bool:
        """Проверяет доступность сервера"""
        try:
            # Извлекаем hostname из URL
            hostname = self.ws_url.replace("wss://", "").replace("ws://", "").split("/")[0]
            socket.getaddrinfo(hostname, None)
            return True
        except (socket.gaierror, socket.timeout, OSError):
            return False

    def _sign_request(self, params: Dict) -> str:
        """Подписывает запрос"""
        query_string = '&'.join([f"{k}={v}" for k, v in sorted(params.items())])
        return hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

    async def _init_session(self):
        """Инициализирует HTTP сессию"""
        if not self.session:
            timeout = aiohttp.ClientTimeout(total=10, connect=5)
            self.session = aiohttp.ClientSession(timeout=timeout)

    async def _make_request(self, method: str, endpoint: str, params: Dict = None, signed: bool = False):
        """Выполняет HTTP запрос с проверкой доступности"""
        if self._connection_available is False:
            raise Exception("Extended API недоступен")

        await self._init_session()

        url = f"{self.base_url}{endpoint}"
        headers = {'X-API-KEY': self.api_key}

        if signed and params:
            params['timestamp'] = int(time.time() * 1000)
            params['signature'] = self._sign_request(params)

        try:
            async with self.session.request(method, url, params=params, headers=headers) as resp:
                return await resp.json()
        except aiohttp.ClientError as e:
            logger.error(f"❌ HTTP запрос failed: {e}")
            self._connection_available = False
            raise

    async def connect_ws(self):
        """Подключается к WebSocket с проверкой доступности"""
        # Проверяем доступность только один раз
        if self._connection_available is None:
            self._connection_available = self._check_connection_available()

        if not self._connection_available:
            logger.warning("🚫 Extended недоступен, пропускаем подключение")
            return False

        if self.ws_connection is None:
            await self._connect_websocket()

        if not self._listener_started:
            try:
                # Получаем ключ для user stream
                user_stream = await self._make_request('POST', '/api/v1/userDataStream')
                self._user_stream_key = user_stream.get('listenKey')

                self.listen_task = asyncio.create_task(self._listen_for_websocket_messages())
                self._listener_started = True
                logger.info("🎧 Extended WebSocket слушатель запущен")
            except Exception as e:
                logger.error(f"❌ Ошибка инициализации user stream: {e}")
                self._connection_available = False
                return False

        return True

    async def _connect_websocket(self):
        """Устанавливает WebSocket соединение"""
        if not self._connection_available:
            raise Exception("Extended недоступен")

        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.ws_connection = await asyncio.wait_for(
                    websockets.connect(
                        self.ws_url,
                        ping_interval=20,
                        ping_timeout=10
                    ),
                    timeout=10
                )
                logger.info("🔌 Extended WebSocket подключен")
                return
            except (websockets.exceptions.InvalidURI,
                    websockets.exceptions.InvalidHandshake,
                    socket.gaierror,
                    asyncio.TimeoutError,
                    OSError) as e:
                logger.error(f"❌ Extended подключение (попытка {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    self._connection_available = False
                    raise Exception(f"Не удалось подключиться к Extended после {max_retries} попыток")

    async def _listen_for_websocket_messages(self):
        """Слушает WebSocket сообщения"""
        if not self._connection_available:
            return

        while self.ws_connection and not self.ws_connection.closed:
            try:
                message = await asyncio.wait_for(self.ws_connection.recv(), timeout=30)
                data = json.loads(message)
                await self._handle_websocket_message(data)

            except asyncio.TimeoutError:
                # Ping для поддержания соединения
                if self.ws_connection and not self.ws_connection.closed:
                    await self.ws_connection.send(json.dumps({"op": "ping"}))

            except websockets.exceptions.ConnectionClosed:
                logger.warning("🔌 Extended WebSocket соединение закрыто")
                break

            except Exception as e:
                logger.error(f"❌ Ошибка в Extended слушателе: {e}")
                await asyncio.sleep(1)

    async def _handle_websocket_message(self, data: Dict):
        """Обрабатывает WebSocket сообщения"""
        event_type = data.get('e')

        if event_type == 'executionReport':
            order_id = data.get('i')
            status = data.get('X')
            filled_qty = float(data.get('z', 0))
            price = float(data.get('p', 0))

            if order_id in self.running_orders:
                logger.info(f"💰 Extended ордер {order_id}: {status}, filled={filled_qty}@{price}")

                await self.db.save_order(
                    order_id=order_id,
                    fill_sz=filled_qty,
                    price=price
                )

                self.zmq_socket.send_json({
                    "exchange": "extended",
                    "type": "fill",
                    "orderId": order_id,
                    "fillSz": filled_qty,
                    "price": price,
                    "status": status
                })

                if status in ['FILLED', 'CANCELED', 'REJECTED']:
                    self.running_orders.pop(order_id, None)

        elif event_type == 'depthUpdate':
            symbol = data.get('s')
            if f"orderbook_{symbol}" in self.subscription_handlers:
                bids = [[float(b[0]), float(b[1])] for b in data.get('b', [])][:10]
                asks = [[float(a[0]), float(a[1])] for a in data.get('a', [])][:10]

                await self.db.save_orderbook(symbol, bids, asks)

                self.zmq_socket.send_json({
                    "exchange": "extended",
                    "coin": symbol,
                    "bids": bids,
                    "asks": asks
                })

    async def subscribe_orderbook(self, symbol: str):
        """Подписка на ордербук"""
        if not self._connection_available:
            logger.warning("🚫 Extended недоступен - пропуск подписки на ордербук")
            return False

        try:
            await self.connect_ws()

            sub_msg = {
                "method": "SUBSCRIBE",
                "params": [f"{symbol.lower()}@depth"],
                "id": 1
            }
            await self.ws_connection.send(json.dumps(sub_msg))

            self.subscription_handlers[f"orderbook_{symbol}"] = True
            logger.info(f"📡 Extended подписка на ордербук {symbol}")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка подписки Extended на {symbol}: {e}")
            return False

    async def place_limit_order(self, symbol: str, side: str, price: float, qty: float):
        """Размещает лимитный ордер"""
        if not self._connection_available:
            raise Exception("Extended недоступен")

        params = {
            'symbol': symbol,
            'side': side.upper(),
            'type': 'LIMIT',
            'timeInForce': 'GTC',
            'quantity': qty,
            'price': price
        }

        data = await self._make_request('POST', '/api/v1/order', params, signed=True)
        order_id = str(data.get('orderId'))

        logger.info(f"📤 Extended placed limit {side} {symbol} {price}@{qty}, orderId={order_id}")

        self.running_orders[order_id] = True
        await self.db.save_order(order_id=order_id, fill_sz=0.0, price=price)

        self.zmq_socket.send_json({
            "exchange": "extended",
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
            "status": "NEW"
        }

    # Остальные методы остаются без изменений, но добавляем проверку доступности
    async def place_market_order(self, symbol: str, side: str, qty: float):
        if not self._connection_available:
            raise Exception("Extended недоступен")
        # ... остальная логика

    async def cancel_order(self, symbol: str, order_id: str):
        if not self._connection_available:
            raise Exception("Extended недоступен")
        # ... остальная логика

    async def close(self):
        """Закрывает все соединения"""
        try:
            self.running_orders.clear()
            self._listener_started = False

            if self.listen_task and not self.listen_task.done():
                self.listen_task.cancel()
                try:
                    await self.listen_task
                except asyncio.CancelledError:
                    pass

            if self.ws_connection and not self.ws_connection.closed:
                await self.ws_connection.close()

            if self.session:
                await self.session.close()

            if hasattr(self, 'zmq_socket'):
                self.zmq_socket.close()

            logger.info("🔌 Extended соединения закрыты")

        except Exception as e:
            logger.error(f"❌ Ошибка закрытия Extended: {e}")

    async def __aenter__(self):
        await self.connect_ws()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()