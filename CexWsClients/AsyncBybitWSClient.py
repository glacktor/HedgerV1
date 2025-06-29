import hashlib
import hmac
import sys
import asyncio
import json
import time

import aiohttp
import websockets
import zmq
from DragonflyDb.DragonFlyConnector import DragonFlyConnector

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

class AsyncBybitWSClient:
    def __init__(self, api_key: str, api_secret: str):
        self.url = "wss://stream.bybit.com/v5/public/linear"
        self.private_url = "wss://stream.bybit.com/v5/private"
        self.db = DragonFlyConnector("bybit")
        self.api_key = api_key
        self.api_secret = api_secret
        context = zmq.Context()
        self.zmq_socket = context.socket(zmq.PUSH)
        self.zmq_socket.connect("tcp://127.0.0.1:5555")
        self.running_orders = {}
        self.public_ws = None
        self.private_ws = None

    async def connect_ws(self):
        if self.public_ws is None:
            self.public_ws = await websockets.connect(self.url)

        if self.private_ws is None:
            self.private_ws = await websockets.connect(self.private_url)
            timestamp = str(int(time.time() * 1000))
            sign_payload = f"{self.api_key}{timestamp}"
            signature = hmac.new(self.api_secret.encode(), sign_payload.encode(), hashlib.sha256).hexdigest()

            auth_msg = {
                "op": "auth",
                "args": [self.api_key, timestamp, signature]
            }

            await self.private_ws.send(json.dumps(auth_msg))
            auth_resp = json.loads(await self.private_ws.recv())

            if not auth_resp.get("success", False):
                print(f"[Bybit] Auth failed: {auth_resp}")
                self.private_ws = None

    async def subscribe_orderbook(self, symbol):
        symbol+='USDT'
        topic = f"orderbook.50.{symbol}"
        async with websockets.connect(self.url) as ws:
            await ws.send(json.dumps({"op": "subscribe", "args": [topic]}))
            while True:
                msg = json.loads(await ws.recv())
                data = msg.get("data", {})
                bids = data.get("b", [])[:10]
                asks = data.get("a", [])[:10]
                await self.db.save_orderbook(symbol, bids, asks)
                self.zmq_socket.send_json({
                    "exchange": "bybit",
                    "coin": symbol,
                    "bids": bids,
                    "asks": asks
                })

    async def subscribe_order(self, symbol, order_id: str):
        self.running_orders[order_id] = True
        symbol+='USDT'
        api_key = self.api_key
        api_secret = self.api_secret
        timestamp = str(int(time.time() * 1000))
        sign_payload = f"{api_key}{timestamp}"
        signature = hmac.new(api_secret.encode(), sign_payload.encode(), hashlib.sha256).hexdigest()

        auth_msg = {
            "op": "auth",
            "args": [api_key, timestamp, signature]
        }

        topic = f"execution.{symbol}"
        sub_msg = {
            "op": "subscribe",
            "args": [topic]
        }

        async with websockets.connect(self.private_url) as ws:
            await ws.send(json.dumps(auth_msg))
            auth_resp = json.loads(await ws.recv())
            if not auth_resp.get("success"):
                print(f"[Bybit] Auth failed: {auth_resp}")
                return

            await ws.send(json.dumps(sub_msg))

            while self.running_orders.get(order_id, False):
                try:
                    msg = json.loads(await ws.recv())
                    if msg.get("topic", "") == topic:
                        data = msg.get("data", [])
                        if not isinstance(data, list):
                            data = [data]

                        for entry in data:
                            if entry.get("orderId") == order_id:
                                fill_sz = float(entry.get("cumExecQty", 0))
                                price = float(entry.get("price", 0))

                                await self.db.save_order(order_id, fill_sz, price)
                                self.zmq_socket.send_json({
                                    "exchange": "bybit",
                                    "order_id": order_id,
                                    "fill_sz": fill_sz
                                })

                except Exception as e:
                    print(f"[Bybit] WS error: {e}")
                    await asyncio.sleep(1)

    async def unsubscribe_order(self, order_id: str):
            self.running_orders.pop(order_id, None)
            await self.db.delete_order(order_id)

    def _sign(self, params: dict) -> str:
        ordered_params = sorted(params.items())
        query_string = "&".join(f"{k}={v}" for k, v in ordered_params)
        return hmac.new(self.api_secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()

    def _auth_params(self) -> dict:
        ts = str(int(time.time() * 1000))
        params = {
            "apiKey": self.api_key,
            "timestamp": ts
        }
        params["sign"] = self._sign(params)
        return params

    def _auth_headers(self, body: dict = None) -> dict:
        ts = str(int(time.time() * 1000))
        recv_window = "5000"
        body_str = json.dumps(body) if body else ""
        sign_payload = ts + self.api_key + recv_window + body_str
        sign = hmac.new(self.api_secret.encode(), sign_payload.encode(), hashlib.sha256).hexdigest()
        return {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-RECV-WINDOW": recv_window,
            "X-BAPI-SIGN": sign,
            "Content-Type": "application/json"
        }

    async def place_limit_order(self, symbol: str, side: str, price: float, qty: float):
        url = "https://api.bybit.com/v5/order/create"
        payload = {
            "category": "linear",
            "symbol": symbol.upper() + "USDT",
            "side": "Buy" if side.lower() == "long" else "Sell",
            "orderType": "Limit",
            "qty": str(qty),
            "price": str(price),
            "timeInForce": "GTC"
        }
        headers = self._auth_headers(payload)
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as resp:
                data = await resp.json()
                order_id = data.get("result", {}).get("orderId")
                if not order_id:
                    raise RuntimeError(f"Bybit не вернул orderId: {data}")
                await self.subscribe_order(symbol, order_id)
                return {"orderId": order_id, "qty": qty}

    async def close_limit_order(self, symbol: str, side: str, price: float, qty: float):
        url = "https://api.bybit.com/v5/order/create"
        payload = {
            "category": "linear",
            "symbol": symbol.upper() + "USDT",
            "side": "Buy" if side.lower() == "long" else "Sell",
            "orderType": "Limit",
            "qty": str(qty),
            "price": str(price),
            "timeInForce": "GTC",
            "reduceOnly": True
        }
        headers = self._auth_headers(payload)
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as resp:
                data = await resp.json()
                order_id = data.get("result", {}).get("orderId")
                if not order_id:
                    raise RuntimeError(f"Bybit не вернул orderId при close_limit_order: {data}")
                await self.subscribe_order(symbol, order_id)
                return {"orderId": order_id, "qty": qty}

    async def cancel_order(self, symbol: str, order_id: str):
        url = "https://api.bybit.com/v5/order/cancel"
        payload = {
            "category": "linear",
            "symbol": symbol.upper() + "USDT",
            "orderId": order_id
        }
        headers = self._auth_headers(payload)
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as resp:
                data = await resp.json()
                if data.get("retCode") == 0:
                    await self.db.delete_order(order_id)
                    return True
                return False

    async def set_leverage(self, symbol: str, leverage: int, margin_mode: str = "isolated", pos_side: str = None):
        url = "https://api.bybit.com/v5/position/set-leverage"
        payload = {
            "category": "linear",
            "symbol": symbol.upper() + "USDT",
            "buyLeverage": str(leverage),
            "sellLeverage": str(leverage)
        }
        headers = self._auth_headers(payload)
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as resp:
                await resp.json()
                return {"status": "success", "symbol": symbol, "leverage": leverage}


    async def place_market_order(self, symbol: str, side: str, qty: float):
        url = "https://api.bybit.com/v5/order/create"
        headers = self._auth_headers()
        payload = {
            "symbol": symbol.upper() + "USDT",
            "side": "Buy" if side.lower() == "long" else "Sell",
            "orderType": "Market",
            "qty": str(qty)
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as resp:
                data = await resp.json()
                order_id = data.get("result", {}).get("orderId")
                if not order_id:
                    raise RuntimeError(f"Bybit не вернул orderId: {data}")
                await self.subscribe_order(symbol, order_id)
                return {"orderId": order_id, "qty": qty}

    async def close_market_order(self, symbol: str, side: str, qty: float):
        url = "https://api.bybit.com/v5/order/create"
        headers = self._auth_headers()
        payload = {
            "symbol": symbol.upper() + "USDT",
            "side": "Buy" if side.lower() == "long" else "Sell",
            "orderType": "Market",
            "qty": str(qty),
            "reduceOnly": True
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as resp:
                data = await resp.json()
                order_id = data.get("result", {}).get("orderId")
                if not order_id:
                    raise RuntimeError(f"Bybit не вернул orderId при close_market_order: {data}")
                await self.subscribe_order(symbol, order_id)
                return {"orderId": order_id, "qty": qty}

    async def get_symbol_info(self, symbol: str):
        url = f"https://api.bybit.com/v5/market/instruments-info?category=linear&symbol={symbol.upper()}USDT"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                data = await resp.json()
                info = data["result"]["list"][0]
                return {
                    "quantityPrecision": int(info.get("lotSizeFilter", {}).get("qtyStep", "1").split(".")[-1].count("0")),
                    "pricePrecision": int(info.get("priceFilter", {}).get("tickSize", "1").split(".")[-1].count("0"))
                }

    async def get_tick_size(self, symbol: str) -> str:
        info = await self.get_symbol_info(symbol)
        return f"0.{''.join(['0'] * (info['pricePrecision'] - 1))}1"

