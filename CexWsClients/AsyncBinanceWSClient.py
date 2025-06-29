import asyncio
import json
import logging
import time
import zmq
import hmac
import hashlib
import urllib.parse
import aiohttp
import websockets
from DragonflyDb.DragonFlyConnector import DragonFlyConnector
from binance import AsyncClient, BinanceSocketManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AsyncBinanceWSClient:
    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.client = None
        self.bm = None
        self.AsyncClient = AsyncClient
        self.BinanceSocketManager = BinanceSocketManager
        self.running_orders = {}
        self.order_tasks = {}
        self.running_orderbooks = {}
        self.orderbook_tasks = {}
        self.db = DragonFlyConnector("binance")
        context = zmq.Context()
        self.zmq_socket = context.socket(zmq.PUSH)
        self.zmq_socket.connect("tcp://127.0.0.1:5555")
        self.session = None
        self._initialized = False
        self.listen_key = None
        self.user_stream_task = None

    def _sign_request(self, params):
        query_string = urllib.parse.urlencode(params)
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature

    async def _create_session(self):
        """Создание aiohttp сессии с проверками"""
        try:
            if self.session is None or self.session.closed:
                connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
                self.session = aiohttp.ClientSession(
                    headers={"Content-Type": "application/json"},
                    connector=connector,
                    timeout=aiohttp.ClientTimeout(total=30)
                )
                print(f"✅ Создана новая aiohttp сессия")
            return self.session
        except Exception as e:
            print(f"❌ Ошибка создания сессии: {e}")
            self.session = None
            raise

    async def connect_ws(self):
        """Подключение с принудительным созданием сессии"""
        try:
            # СНАЧАЛА создаем сессию
            await self._create_session()

            if not self._initialized:
                self.client = await self.AsyncClient.create(self.api_key, self.api_secret)
                self.bm = self.BinanceSocketManager(self.client)
                self._initialized = True
                print(f"✅ Binance клиент инициализирован")

                # Только после успешной инициализации запускаем user stream
                await self._start_user_stream()

        except Exception as e:
            print(f"❌ Ошибка в connect_ws: {e}")
            self._initialized = False
            self.session = None
            raise

    async def _get_listen_key(self):
        """Получает listen key с проверкой сессии"""
        if self.session is None or self.session.closed:
            await self._create_session()

        url = "https://fapi.binance.com/fapi/v1/listenKey"
        headers = {"X-MBX-APIKEY": self.api_key}

        try:
            async with self.session.post(url, headers=headers) as resp:
                data = await resp.json()
                return data["listenKey"]
        except Exception as e:
            print(f"❌ Ошибка получения listen key: {e}")
            # Пересоздаем сессию при ошибке
            await self._create_session()
            raise

    async def _start_user_stream(self):
        """Запуск user stream с проверками"""
        if self.user_stream_task and not self.user_stream_task.done():
            return

        print("🔄 Запуск user stream...")

        async def _user_stream_handler():
            while True:
                try:
                    # Убеждаемся что сессия существует
                    if self.session is None or self.session.closed:
                        await self._create_session()

                    self.listen_key = await self._get_listen_key()
                    ws_url = f"wss://fstream.binance.com/ws/{self.listen_key}"

                    print(f"✅ Подключен к user stream {self.listen_key}")

                    async with self.session.ws_connect(ws_url) as ws:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                await self._handle_user_stream_message(data)
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                print(f"WebSocket error: {ws.exception()}")
                                break

                except Exception as e:
                    print(f"Ошибка в user stream: {e}")
                    await asyncio.sleep(5)

        self.user_stream_task = asyncio.create_task(_user_stream_handler())

    async def _handle_user_stream_message(self, data):
        """Обработка сообщений user stream"""
        if data.get("e") == "executionReport":
            order_id = str(data.get("i"))

            if order_id in self.running_orders:
                fill_qty = float(data.get("z", 0))
                avg_price = float(data.get("ap", 0))
                last_fill_price = float(data.get("L", 0))
                order_status = data.get("X")

                print(
                    f"[ORDER STATUS] {order_id}: {order_status} filled={fill_qty} avgPrice={avg_price} lastPrice={last_fill_price}")

                await self.db.save_order(
                    order_id=order_id,
                    fill_sz=fill_qty,
                    price=avg_price if avg_price > 0 else last_fill_price
                )

                self.zmq_socket.send_json({
                    "exchange": "binance",
                    "type": "order",
                    "orderId": order_id,
                    "fillSz": fill_qty,
                    "price": avg_price if avg_price > 0 else last_fill_price,
                    "status": order_status
                })

                if order_status in ["FILLED", "CANCELED", "EXPIRED"]:
                    await self.unsubscribe_order(order_id)

    async def subscribe_order(self, order_id: str):
        """Простая подписка на ордер"""
        if order_id in self.running_orders:
            return

        self.running_orders[order_id] = True
        print(f"🔔 Отслеживание ордера {order_id}")

    async def unsubscribe_order(self, order_id: str):
        """Отписка от ордера"""
        self.running_orders.pop(order_id, None)
        print(f"🔕 Отписка от ордера {order_id}")

    async def subscribe_orderbook(self, symbol):
        """Подписка на ордербук с проверкой сессии"""
        # Убеждаемся что сессия создана
        if self.session is None or self.session.closed:
            await self._create_session()

        if symbol.endswith("USDT"):
            full_symbol = symbol.upper()
        else:
            full_symbol = (symbol + "USDT").upper()

        ws_symbol = full_symbol.lower()

        # Получаем начальный snapshot
        url = f"https://fapi.binance.com/fapi/v1/depth?symbol={full_symbol}&limit=1000"

        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    bids = [[float(p), float(q)] for p, q in data.get("bids", [])[:10]]
                    asks = [[float(p), float(q)] for p, q in data.get("asks", [])[:10]]
                    await self.db.save_orderbook(full_symbol, bids, asks)
                    print(f"✅ Начальный ордербук {full_symbol}: bids={len(bids)}, asks={len(asks)}")
                else:
                    print(f"❌ Ошибка получения ордербука {full_symbol}: {await response.text()}")
        except Exception as e:
            print(f"❌ Исключение при получении ордербука: {e}")
            raise

        # Запускаем WebSocket для обновлений
        ws_url = f"wss://fstream.binance.com/ws/{ws_symbol}@depth10@100ms"
        self.running_orderbooks[full_symbol] = True
        self.orderbook_tasks[full_symbol] = asyncio.create_task(self._listen(full_symbol, ws_url))

    async def _listen(self, save_symbol, url):
        """WebSocket слушатель ордербука"""
        while self.running_orderbooks.get(save_symbol):
            try:
                async with websockets.connect(url) as ws:
                    async for msg in ws:
                        if not self.running_orderbooks.get(save_symbol):
                            break
                        try:
                            data = json.loads(msg)
                            bids = [[float(p), float(q)] for p, q in data.get("b", [])[:10]]
                            asks = [[float(p), float(q)] for p, q in data.get("a", [])[:10]]
                            if bids and asks:
                                await self.db.save_orderbook(save_symbol, bids, asks)
                                self.zmq_socket.send_json({
                                    "exchange": "binance",
                                    "coin": save_symbol,
                                    "bids": bids,
                                    "asks": asks,
                                })
                        except Exception as e:
                            print(f"Ошибка обработки сообщения {save_symbol}: {e}")
            except Exception as e:
                print(f"Ошибка WebSocket {save_symbol}: {e}")
                await asyncio.sleep(1)

    async def unsubscribe_orderbook(self, symbol: str):
        """Отписка от ордербука"""
        symbol = symbol.lower()
        self.running_orderbooks.pop(symbol, None)
        task = self.orderbook_tasks.pop(symbol, None)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def close(self):
        """Закрытие всех соединений"""
        for symbol in list(self.running_orderbooks.keys()):
            self.running_orderbooks[symbol] = False
            if symbol in self.orderbook_tasks and not self.orderbook_tasks[symbol].done():
                self.orderbook_tasks[symbol].cancel()
                try:
                    await self.orderbook_tasks[symbol]
                except asyncio.CancelledError:
                    print(f"Task for {symbol} cancelled")

        if self.user_stream_task and not self.user_stream_task.done():
            self.user_stream_task.cancel()

        if self.session and not self.session.closed:
            await self.session.close()
        if self.client:
            await self.client.close_connection()

    async def _get_symbol_precision(self, symbol: str):
        """Получение precision с проверкой сессии"""
        if self.session is None or self.session.closed:
            await self._create_session()

        full_symbol = symbol.upper() + "USDT"
        url_info = f"https://fapi.binance.com/fapi/v1/exchangeInfo"

        async with self.session.get(url_info) as response:
            data = await response.json()
            for s in data['symbols']:
                if s['symbol'] == full_symbol:
                    return s['quantityPrecision'], s['pricePrecision']
            raise ValueError(f"Symbol {full_symbol} not found")

    # ============= ОСТАЛЬНЫЕ МЕТОДЫ С ПРОВЕРКОЙ СЕССИИ =============

    async def place_limit_order(self, symbol: str, side: str, price: float, qty: float):
            return {
                "orderId": result['orderId'],
                "symbol": result['symbol'],
                "qty": result['origQty']
            }

    async def place_market_order(self, symbol: str, side: str, qty: float):
        await self.connect_ws()
        full_symbol = symbol.upper() + "USDT"
        timestamp = int(time.time() * 1000)

        qty_prec, _ = await self._get_symbol_precision(symbol)

        params = {
            "symbol": full_symbol,
            "side": "BUY" if side.lower() == "long" else "SELL",
            "type": "MARKET",
            "quantity": round(qty, qty_prec),
            "timestamp": timestamp
        }
        params["signature"] = self._sign_request(params)
        headers = {"X-MBX-APIKEY": self.api_key}
        url = f"https://fapi.binance.com/fapi/v1/order"

        async with self.session.post(url, params=params, headers=headers) as response:
            result = await response.json()

            if "orderId" in result:
                order_id = str(result["orderId"])
                self.running_orders[order_id] = True

                await self.db.save_order(
                    order_id=order_id,
                    fill_sz=float(result.get("executedQty", 0)),
                    price=float(result.get("avgPrice", 0)) if float(result.get("avgPrice", 0)) > 0 else float(
                        result.get("price", 0))
                )

            return result

    async def close_limit_order(self, symbol: str, side: str, price: float, qty: float):
        await self.connect_ws()
        full_symbol = symbol.upper() + "USDT"
        timestamp = int(time.time() * 1000)

        qty_prec, price_prec = await self._get_symbol_precision(symbol)

        params = {
            "symbol": full_symbol,
            "side": "BUY" if side.lower() == "long" else "SELL",
            "type": "LIMIT",
            "quantity": round(qty, qty_prec),
            "price": round(price, price_prec),
            "timeInForce": "GTC",
            "reduceOnly": "true",
            "timestamp": timestamp
        }
        params["signature"] = self._sign_request(params)
        headers = {"X-MBX-APIKEY": self.api_key}
        url = f"https://fapi.binance.com/fapi/v1/order"

        async with self.session.post(url, params=params, headers=headers) as response:
            result = await response.json()

            if "orderId" in result:
                order_id = str(result["orderId"])
                self.running_orders[order_id] = True

                await self.db.save_order(
                    order_id=order_id,
                    fill_sz=float(result.get("executedQty", 0)),
                    price=float(result.get("avgPrice", 0)) if float(result.get("avgPrice", 0)) > 0 else float(
                        result.get("price", 0))
                )

            return result

    async def close_market_order(self, symbol: str, side: str, qty: float):
        await self.connect_ws()
        full_symbol = symbol.upper() + "USDT"
        timestamp = int(time.time() * 1000)

        qty_prec, _ = await self._get_symbol_precision(symbol)

        params = {
            "symbol": full_symbol,
            "side": "BUY" if side.lower() == "long" else "SELL",
            "type": "MARKET",
            "quantity": round(qty, qty_prec),
            "reduceOnly": "true",
            "timestamp": timestamp
        }
        params["signature"] = self._sign_request(params)
        headers = {"X-MBX-APIKEY": self.api_key}
        url = f"https://fapi.binance.com/fapi/v1/order"

        async with self.session.post(url, params=params, headers=headers) as response:
            result = await response.json()

            if "orderId" in result:
                order_id = str(result["orderId"])
                self.running_orders[order_id] = True

                await self.db.save_order(
                    order_id=order_id,
                    fill_sz=float(result.get("executedQty", 0)),
                    price=float(result.get("avgPrice", 0)) if float(result.get("avgPrice", 0)) > 0 else float(
                        result.get("price", 0))
                )

            return result

    async def set_leverage(self, symbol: str, leverage: int, margin_mode: str = "isolated", pos_side: str = None):
        await self.connect_ws()
        full_symbol = symbol.upper() + "USDT"
        params = {
            "symbol": full_symbol,
            "leverage": leverage,
            "timestamp": int(time.time() * 1000)
        }
        params["signature"] = self._sign_request(params)
        headers = {"X-MBX-APIKEY": self.api_key}
        url = f"https://fapi.binance.com/fapi/v1/leverage"

        async with self.session.post(url, params=params, headers=headers) as response:
            return await response.json()

    async def get_symbol_info(self, symbol: str):
        await self.connect_ws()
        url = f"https://fapi.binance.com/fapi/v1/exchangeInfo"

        async with self.session.get(url) as response:
            data = await response.json()
            for s in data['symbols']:
                if s['symbol'] == symbol.upper() + "USDT":
                    return {
                        'quantityPrecision': s['quantityPrecision'],
                        'pricePrecision': s['pricePrecision']
                    }
            raise ValueError(f"Symbol {symbol} not found")

    async def get_order_status(self, symbol: str, order_id: str):
        await self.connect_ws()
        full_symbol = symbol.upper() + "USDT"
        params = {
            "symbol": full_symbol,
            "orderId": order_id,
            "timestamp": int(time.time() * 1000)
        }
        params['signature'] = self._sign_request(params)
        headers = {"X-MBX-APIKEY": self.api_key}
        url = f"https://fapi.binance.com/fapi/v1/order"

        async with self.session.get(url, params=params, headers=headers) as response:
            result = await response.json()

            if "orderId" in result:
                await self.db.save_order(
                    order_id=str(result["orderId"]),
                    fill_sz=float(result.get("executedQty", 0)),
                    price=float(result.get("avgPrice", 0)) if float(result.get("avgPrice", 0)) > 0 else float(
                        result.get("price", 0))
                )

            return result

    async def cancel_order(self, symbol: str, order_id: str):
        await self.connect_ws()
        full_symbol = symbol.upper() + "USDT"
        params = {
            "symbol": full_symbol,
            "orderId": order_id,
            "timestamp": int(time.time() * 1000)
        }
        params['signature'] = self._sign_request(params)
        headers = {"X-MBX-APIKEY": self.api_key}
        url = f"https://fapi.binance.com/fapi/v1/order"

        async with self.session.delete(url, params=params, headers=headers) as response:
            result = await response.json()
            await self.unsubscribe_order(order_id)
            return result

    async def get_tick_size(self, symbol: str) -> str:
        await self.connect_ws()
        full_symbol = symbol.upper() + "USDT"
        url = f"https://fapi.binance.com/fapi/v1/exchangeInfo"

        async with self.session.get(url) as response:
            data = await response.json()
            for s in data['symbols']:
                if s['symbol'] == full_symbol:
                    for f in s['filters']:
                        if f['filterType'] == 'PRICE_FILTER':
                            return format(float(f['tickSize']), ".5f")
            raise ValueError(f"Tick size for {symbol} not found")

    async def get_funding_rate(self, symbol: str) -> float:
        await self.connect_ws()
        full_symbol = symbol.upper() + "USDT"
        url = f"https://fapi.binance.com/fapi/v1/premiumIndex"
        params = {"symbol": full_symbol}

        async with self.session.get(url, params=params) as response:
            data = await response.json()
            return float(data['lastFundingRate'])

    async def get_position_size(self, symbol: str, direction: str) -> float:
        await self.connect_ws()
        try:
            full_symbol = symbol.upper() + "USDT"
            endpoint = "/fapi/v2/positionRisk"
            params = {
                "symbol": full_symbol,
                "timestamp": int(time.time() * 1000)
            }
            params["signature"] = self._sign_request(params)
            headers = {"X-MBX-APIKEY": self.api_key}
            url = f"https://fapi.binance.com{endpoint}"

            async with self.session.get(url, params=params, headers=headers) as response:
                data = await response.json()
                if response.status == 200 and isinstance(data, list):
                    for pos in data:
                        pos_qty = float(pos['positionAmt'])
                        pos_dir = 'long' if pos_qty > 0 else 'short' if pos_qty < 0 else None
                        if pos_dir == direction.lower():
                            return abs(pos_qty)
                    return 0.0
                else:
                    raise Exception(f"Failed to fetch position: {data.get('msg', 'Unknown error')}")
        except Exception as e:
            raise Exception(f"Error fetching position for {symbol}: {str(e)}")

    async def get_position_info(self, symbol: str):
        await self.connect_ws()
        full_symbol = symbol.upper() + "USDT"

        params = {
            "timestamp": int(time.time() * 1000)
        }
        params["signature"] = self._sign_request(params)
        headers = {"X-MBX-APIKEY": self.api_key}

        try:
            async with self.session.get(
                    "https://fapi.binance.com/fapi/v2/positionRisk",
                    params=params,
                    headers=headers
            ) as response:
                if response.status != 200:
                    error_text = await response.text()
                    #print(f"❌ API error {response.status}: {error_text}")
                    raise Exception(f"API error {response.status}: {error_text}")

                data = await response.json()
                #print(f"🔍 Raw response: {data}")  # Отладка

                # Ищем позицию для конкретного символа
                for pos in data:
                    #print(f"🔍 Checking position: {pos}")  # Отладка
                    if pos['symbol'] == full_symbol:
                        position_amt = float(pos['positionAmt'])
                        #print(f"🔍 Position amount: {position_amt}")  # Отладка

                        if position_amt != 0:  # Есть открытая позиция
                            return {
                                "symbol": full_symbol,
                                "avg_price": float(pos['entryPrice']),
                                "size": abs(position_amt),
                                "side": "long" if position_amt > 0 else "short",
                                "unrealized_pnl": float(pos['unRealizedProfit'])
                            }
                        else:
                            #print(f"🔍 No position for {full_symbol}")
                            return None

                # print(f"🔍 Symbol {full_symbol} not found in response")
                return None

        except Exception as e:
            print(f"❌ Ошибка получения позиции {symbol}: {e}")
            raise

    async def __aenter__(self):
        await self.connect_ws()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()