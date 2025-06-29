import asyncio
import logging
import time
import json
import os
import sys
from typing import Dict, Any

# Добавляем корневую папку проекта в путь
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from DragonflyDb.DragonFlyConnector import DragonFlyConnector
from InfoClients.AsyncBinanceInfoClient import AsyncBinanceInfoClient
from InfoClients.AsyncHyperliquidInfoClient import AsyncHyperliquidInfoClient
from CexWsClients.AsyncBinanceWSClient import AsyncBinanceWSClient
from CexWsClients.AsyncHyperliquidWSClient import AsyncHyperliquidWSClient

logger = logging.getLogger(__name__)


class ScalpStrategy:
    def __init__(self, exchange1_name: str, exchange2_name: str, asset: str, config_path: str):
        self.exchange1_name = exchange1_name
        self.exchange2_name = exchange2_name
        self.asset = asset

        # Загружаем конфиг
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file {config_path} not found")

        with open(config_path, 'r') as f:
            self.config = json.load(f)

        # Клиенты (будут инициализированы в init)
        self.exchange1_ws = None
        self.exchange2_ws = None
        self.exchange1_info = None
        self.exchange2_info = None

        # Параметры из конфига
        scalp_params = self.config['scalp_parameters']
        self.base_qty = scalp_params['base_quantity']
        self.target_profit = scalp_params['target_profit_usd']
        self.max_position_size = scalp_params['max_position_size']
        self.min_quantity = scalp_params['min_quantity']
        self.min_profit_per_unit = scalp_params['min_profit_per_unit']

        # Статистика
        self.total_profit = 0.0
        self.successful_attacks = 0
        self.total_attempts = 0
        self.start_time = None

    async def init_clients(self):
        """Быстрая инициализация клиентов"""
        logger.info(f"🔄 Initializing {self.exchange1_name} <-> {self.exchange2_name}")

        db = DragonFlyConnector(exchange="scalp")

        # Создаем клиентов
        if self.exchange1_name == "Binance":
            self.exchange1_ws = AsyncBinanceWSClient(
                self.config["api_keys"]["binance"]["api_key"],
                self.config["api_keys"]["binance"]["api_secret"]
            )
            self.exchange1_info = AsyncBinanceInfoClient(db)

        elif self.exchange1_name == "Hyperliquid":
            self.exchange1_ws = AsyncHyperliquidWSClient.from_key(
                self.config["api_keys"]["hyperliquid"]["api_key"]
            )
            self.exchange1_info = AsyncHyperliquidInfoClient(db)

        if self.exchange2_name == "Binance":
            self.exchange2_ws = AsyncBinanceWSClient(
                self.config["api_keys"]["binance"]["api_key"],
                self.config["api_keys"]["binance"]["api_secret"]
            )
            self.exchange2_info = AsyncBinanceInfoClient(db)

        elif self.exchange2_name == "Hyperliquid":
            self.exchange2_ws = AsyncHyperliquidWSClient.from_key(
                self.config["api_keys"]["hyperliquid"]["api_key"]
            )
            self.exchange2_info = AsyncHyperliquidInfoClient(db)

        # Подключаемся параллельно
        await asyncio.gather(
            self.exchange1_ws.connect_ws(),
            self.exchange2_ws.connect_ws()
        )

        # Подписываемся на ордербуки (учитываем разные форматы символов)
        symbol1 = self.asset + "USDT" if self.exchange1_name == "Binance" else self.asset
        symbol2 = self.asset + "USDT" if self.exchange2_name == "Binance" else self.asset

        await asyncio.gather(
            self.exchange1_ws.subscribe_orderbook(symbol1),
            self.exchange2_ws.subscribe_orderbook(symbol2)
        )

        # Ждем стабилизации
        await asyncio.sleep(2)
        logger.info("✅ Clients ready")

    async def dual_open(self, exchange1_ws, exchange1_info, price1, exchange2_ws, exchange2_info, price2, qty):
        """
        Dual FOK attack на обеих биржах

        Returns:
            {
                "success": bool,
                "both_filled": bool,
                "exchange1_result": dict,
                "exchange2_result": dict,
                "action_taken": str,
                "profit": float
            }
        """
        logger.debug(f'🎯 DUAL FOK: {qty:.4f} @ {price1:.4f}/{price2:.4f}')

        start_time = time.time()

        # Правильные символы для FOK ордеров
        symbol1 = self.asset + "USDT" if exchange1_ws == self.exchange1_ws and self.exchange1_name == "Binance" else self.asset
        symbol2 = self.asset + "USDT" if exchange2_ws == self.exchange2_ws and self.exchange2_name == "Binance" else self.asset

        # Одновременная отправка FOK ордеров
        results = await asyncio.gather(
            exchange1_ws.place_fok_order(symbol1, "short", price1, qty),  # Продаем дороже
            exchange2_ws.place_fok_order(symbol2, "long", price2, qty),  # Покупаем дешевле
            return_exceptions=True
        )

        execution_time = (time.time() - start_time) * 1000
        self.total_attempts += 1

        result1, result2 = results

        # Обработка исключений
        if isinstance(result1, Exception):
            logger.error(f'❌ Exchange1 exception: {result1}')
            result1 = {"success": False, "error": str(result1)}

        if isinstance(result2, Exception):
            logger.error(f'❌ Exchange2 exception: {result2}')
            result2 = {"success": False, "error": str(result2)}

        # Проверяем успешность
        success1 = result1.get('success', False)
        success2 = result2.get('success', False)

        if success1 and success2:
            # ✅ ОБА ИСПОЛНЕНЫ - ПРОФИТ!
            profit = (price1 - price2) * qty
            self.total_profit += profit
            self.successful_attacks += 1

            logger.info(f'✅ PROFIT: ${profit:.4f} | Total: ${self.total_profit:.4f} | Time: {execution_time:.1f}ms')

            return {
                "success": True,
                "both_filled": True,
                "exchange1_result": result1,
                "exchange2_result": result2,
                "action_taken": "both_filled",
                "profit": profit
            }

        elif success1 and not success2:
            # ⚠️ ТОЛЬКО ПЕРВЫЙ ИСПОЛНЕН
            logger.warning(f'⚠️ Only {self.exchange1_name} filled - closing')

            try:
                filled_qty = result1['filledQty']
                close_symbol = symbol1  # Используем тот же символ
                await exchange1_ws.close_market_order(close_symbol, "long", filled_qty)

                return {
                    "success": False,
                    "both_filled": False,
                    "exchange1_result": result1,
                    "exchange2_result": result2,
                    "action_taken": "closed_exchange1",
                    "profit": 0
                }

            except Exception as e:
                logger.error(f'❌ Failed to close {self.exchange1_name}: {e}')
                return {
                    "success": False,
                    "both_filled": False,
                    "exchange1_result": result1,
                    "exchange2_result": result2,
                    "action_taken": "close_failed",
                    "profit": 0,
                    "error": str(e)
                }

        elif not success1 and success2:
            # ⚠️ ТОЛЬКО ВТОРОЙ ИСПОЛНЕН
            logger.warning(f'⚠️ Only {self.exchange2_name} filled - closing')

            try:
                filled_qty = result2['filledQty']
                close_symbol = symbol2  # Используем тот же символ
                await exchange2_ws.close_market_order(close_symbol, "short", filled_qty)

                return {
                    "success": False,
                    "both_filled": False,
                    "exchange1_result": result1,
                    "exchange2_result": result2,
                    "action_taken": "closed_exchange2",
                    "profit": 0
                }

            except Exception as e:
                logger.error(f'❌ Failed to close {self.exchange2_name}: {e}')
                return {
                    "success": False,
                    "both_filled": False,
                    "exchange1_result": result1,
                    "exchange2_result": result2,
                    "action_taken": "close_failed",
                    "profit": 0,
                    "error": str(e)
                }
        else:
            # 🔄 ОБА НЕ ИСПОЛНЕНЫ
            return {
                "success": False,
                "both_filled": False,
                "exchange1_result": result1,
                "exchange2_result": result2,
                "action_taken": "both_rejected",
                "profit": 0
            }

    async def fast_scan(self):
        """
        Ультрабыстрый скан ордербуков

        Returns:
            {
                "found": bool,
                "qty": float,
                "price1": float,
                "price2": float,
                "profit_estimate": float
            }
        """
        try:
            # Правильные символы для каждой биржи
            symbol1 = self.asset + "USDT" if self.exchange1_name == "Binance" else self.asset
            symbol2 = self.asset + "USDT" if self.exchange2_name == "Binance" else self.asset

            # Параллельно получаем ордербуки
            ob1, ob2 = await asyncio.gather(
                self.exchange1_info.get_orderbook(symbol1),
                self.exchange2_info.get_orderbook(symbol2)
            )

            # Быстрые проверки
            if not (ob1.get('bids') and ob2.get('asks')):
                return {"found": False}

            # Извлекаем данные
            bid_price = float(ob1['bids'][0][0])  # Продаем сюда (дороже)
            bid_volume = float(ob1['bids'][0][1])

            ask_price = float(ob2['asks'][0][0])  # Покупаем тут (дешевле)
            ask_volume = float(ob2['asks'][0][1])

            # Проверяем профитабельность
            price_diff = bid_price - ask_price

            if price_diff <= self.min_profit_per_unit:
                return {"found": False}

            # Максимальный объем
            max_qty = min(bid_volume, ask_volume, self.max_position_size)

            if max_qty < self.min_quantity:
                return {"found": False}

            # Оптимальный размер
            optimal_qty = min(max_qty, self.base_qty)
            profit_estimate = price_diff * optimal_qty

            return {
                "found": True,
                "qty": optimal_qty,
                "price1": bid_price,
                "price2": ask_price,
                "profit_estimate": profit_estimate
            }

        except:
            return {"found": False}

    async def run_scalping(self):
        """Основной цикл скальпинга"""
        await self.init_clients()

        self.start_time = time.time()
        logger.info(f'🎯 SCALPING STARTED')
        logger.info(f'   Asset: {self.asset}')
        logger.info(f'   Target profit: ${self.target_profit:.2f}')
        logger.info(f'   Base quantity: {self.base_qty}')
        logger.info(f'   Min profit/unit: ${self.min_profit_per_unit:.2f}')

        scan_count = 0
        last_stats_time = time.time()

        while self.total_profit < self.target_profit:
            scan_count += 1

            # Быстрый скан
            opportunity = await self.fast_scan()

            if opportunity['found']:
                # FOK атака
                result = await self.dual_open(
                    self.exchange1_ws, self.exchange1_info, opportunity['price1'],
                    self.exchange2_ws, self.exchange2_info, opportunity['price2'],
                    opportunity['qty']
                )

                if result['success']:
                    # Статистика после успешной атаки
                    progress = (self.total_profit / self.target_profit) * 100
                    success_rate = (self.successful_attacks / self.total_attempts) * 100
                    runtime = time.time() - self.start_time

                    logger.info(f'💰 Progress: {progress:.1f}% | Success: {success_rate:.1f}% | Runtime: {runtime:.0f}s')

                    # Микропауза после успеха
                    await asyncio.sleep(0.01)
                else:
                    # Пауза после неудачи
                    await asyncio.sleep(0.02)
            else:
                # Статистика каждые 30 секунд
                current_time = time.time()
                if current_time - last_stats_time > 30:
                    runtime = current_time - self.start_time
                    scans_per_sec = scan_count / runtime if runtime > 0 else 0
                    success_rate = (
                                               self.successful_attacks / self.total_attempts) * 100 if self.total_attempts > 0 else 0

                    logger.info(
                        f'🔍 Scanning: {scans_per_sec:.0f}/sec | Profit: ${self.total_profit:.4f} | Success: {success_rate:.1f}%')
                    last_stats_time = current_time

                # Быстрый retry
                await asyncio.sleep(0.002)

        # Финальная статистика
        total_runtime = time.time() - self.start_time
        scans_per_sec = scan_count / total_runtime
        success_rate = (self.successful_attacks / self.total_attempts) * 100 if self.total_attempts > 0 else 0

        logger.info(f'🏁 SCALPING COMPLETE!')
        logger.info(f'   Total profit: ${self.total_profit:.4f}')
        logger.info(f'   Runtime: {total_runtime:.0f} seconds')
        logger.info(f'   Successful attacks: {self.successful_attacks}/{self.total_attempts}')
        logger.info(f'   Success rate: {success_rate:.1f}%')
        logger.info(f'   Scan speed: {scans_per_sec:.0f} scans/sec')
        logger.info(f'   Total scans: {scan_count:,}')

        return {
            "success": True,
            "total_profit": self.total_profit,
            "successful_attacks": self.successful_attacks,
            "total_attempts": self.total_attempts,
            "success_rate": success_rate,
            "total_scans": scan_count,
            "runtime": total_runtime,
            "scans_per_sec": scans_per_sec
        }

    async def close_connections(self):
        """Закрытие всех соединений"""
        try:
            tasks = []
            if self.exchange1_ws:
                tasks.append(self.exchange1_ws.close())
            if self.exchange2_ws:
                tasks.append(self.exchange2_ws.close())

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

            logger.info("🔌 All connections closed")
        except Exception as e:
            logger.error(f"❌ Error closing connections: {e}")

    def get_stats(self):
        """Получение текущей статистики"""
        runtime = time.time() - self.start_time if self.start_time else 0
        success_rate = (self.successful_attacks / self.total_attempts) * 100 if self.total_attempts > 0 else 0

        return {
            "total_profit": self.total_profit,
            "successful_attacks": self.successful_attacks,
            "total_attempts": self.total_attempts,
            "success_rate": success_rate,
            "runtime": runtime,
            "target_profit": self.target_profit,
            "progress": (self.total_profit / self.target_profit) * 100
        }