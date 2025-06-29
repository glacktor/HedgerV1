import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
import asyncio
import json
import os

from DragonflyDb.DragonFlyConnector import DragonFlyConnector
from InfoClients.AsyncBinanceInfoClient import AsyncBinanceInfoClient
from InfoClients.AsyncHyperliquidInfoClient import AsyncHyperliquidInfoClient
from config.config import CLIENTS as WS_CLIENTS
from CexClients.AsyncBinanceClient import AsyncBinanceClient
from CexClients.AsyncOKXClient import AsyncOKXClient
from CexClients.AsyncBybitClient import AsyncBybitClient
from CexClients.AsyncHyperliquidClient import AsyncHyperliquidClient

from CexWsClients.AsyncOKXWSClient import AsyncOKXWSClient
from CexWsClients.AsyncBinanceWSClient import AsyncBinanceWSClient
from CexWsClients.AsyncBybitWSClient import AsyncBybitWSClient
from CexWsClients.AsyncHyperliquidWSClient import AsyncHyperliquidWSClient
from eth_account.signers.local import LocalAccount
from eth_account.account import Account

logger = logging.getLogger(__name__)


def get_decimal_places(price: float) -> int:
    price_str = str(price)
    return len(price_str.split('.')[1]) if '.' in price_str else 0


def round_to_decimals(value: float, decimals: int) -> float:
    return round(value, decimals)


def adjust_quantity(deal_qty, constraints):
    min_quantity = float(constraints['min_quantity'])
    lot_size = float(constraints['lot_size'])

    rounded_quantity = round(deal_qty / lot_size) * lot_size
    adjusted_quantity = max(rounded_quantity, min_quantity)
    return int(adjusted_quantity)


async def init_exchange_client(exchange_name: str, config: dict, asset: str, db):
    """
    Универсальная функция инициализации любого обменника

    Args:
        exchange_name: "Binance" или "Hyperliquid"
        config: конфиг с api ключами
        asset: торговый актив
        db: объект базы данных

    Returns:
        (ws_client, info_client) - готовые к работе клиенты
    """
    print(f"🔄 Инициализация {exchange_name}...")

    if exchange_name == "Binance":
        # Создаем клиентов
        ws_client = AsyncBinanceWSClient(
            config["api_keys"]["binance"]["api_key"],
            config["api_keys"]["binance"]["api_secret"]
        )
        info_client = AsyncBinanceInfoClient(db)

        # Подключаемся и подписываемся
        await ws_client.connect_ws()
        await ws_client.subscribe_orderbook(asset)

    elif exchange_name == "Hyperliquid":
        # Создаем клиентов
        ws_client = AsyncHyperliquidWSClient.from_key(
            config["api_keys"]["hyperliquid"]["api_key"]
        )
        info_client = AsyncHyperliquidInfoClient(db)

        # Подключаемся и подписываемся
        await ws_client.connect_ws()
        await ws_client.subscribe_orderbook(asset)

    else:
        raise ValueError(f"Неизвестный обменник: {exchange_name}")

    print(f"✅ {exchange_name} готов")
    return ws_client, info_client


class LongShort:
    def __init__(self, exchange1_name: Any, exchange2_name: Any, reverse: bool, asset: str,
                 config_path: str = r"C:/Users/strel/PycharmProjects/rn_need_projects/jekius_maximus/logic/config.json"):
        self.exchange1_name = exchange1_name
        self.exchange2_name = exchange2_name
        self.exchange1_client = None
        self.exchange2_client = None
        self.reverse = reverse
        self.asset = asset
        if not os.path.exists(config_path):
            logger.error(f"Config file {config_path} not found")
            raise FileNotFoundError(f"Config file {config_path} not found")
        with open(config_path, 'r') as config_file:
            self.config = json.load(config_file)
        self.exchange1_funding_pnl = 0.0
        self.exchange2_funding_pnl = 0.0
        self.exchange2_funding_count = 0
        self.max_exchange2_funding = 1
        self.final_funding_done = False
        self.prcDcmls1 = None
        self.prcDcmls2 = None
        self.qtyDcmls1 = None
        self.qtyDcmls2 = None
        self.frstDrctn = "long"
        self.scndDrctn = "short"
        self.dealqty = self.config['trading_parameters']['margin'] * self.config['trading_parameters']['leverage'] / \
                       self.config['trading_parameters']['parts']
        self.credited_tokens1 = 0
        self.credited_tokens2 = 0

    async def before_start(self):
        """Инициализация клиентов обменников и настройка торговых параметров"""
        await self._init_exchange_clients()
        await self._setup_trading_parameters()

    async def _init_exchange_clients(self):
        """Простая инициализация через универсальную функцию"""
        # ФИКС: создаем отдельные DB коннекторы для каждой биржи
        db1 = DragonFlyConnector(exchange=self.exchange1_name.lower())  # "binance"
        db2 = DragonFlyConnector(exchange=self.exchange2_name.lower())  # "hyperliquid"

        # Инициализируем первый обменник с его DB
        self.exchange1WebSocket, self.exchange1Info = await init_exchange_client(
            self.exchange1_name, self.config, self.asset, db1
        )

        # Инициализируем второй обменник с его DB
        self.exchange2WebSocket, self.exchange2Info = await init_exchange_client(
            self.exchange2_name, self.config, self.asset, db2
        )

        # Ждём стабилизации ордербуков
        print("⏳ Ожидание ордербуков...")
        await asyncio.sleep(3)
        await self._wait_for_orderbook()
        print("🚀 Все готово!")

    async def _wait_for_orderbook(self):
        """Быстрая проверка что ордербуки не пустые"""
        for attempt in range(5):
            try:
                ob1, ob2 = await asyncio.gather(
                    self.exchange1Info.get_orderbook(self.asset),
                    self.exchange2Info.get_orderbook(self.asset)
                )

                if (ob1.get('bids', []) and ob2.get('asks', [])):
                    logger.info("✅ Ордербуки готовы")
                    return True

            except Exception as e:
                pass  # Игнорируем ошибки, просто ждем

            await asyncio.sleep(1)

        raise Exception("Ордербуки не заполнились за 5 секунд")

    def _create_websocket_client(self, exchange_name: str):
        key = exchange_name.lower()
        if key not in WS_CLIENTS:
            raise ValueError(f"WebSocket клиент для '{exchange_name}' не найден в config.py")
        return WS_CLIENTS[key]

    def _create_info_client(self, exchange_name: str):
        """Создание информационного клиента (для работы с БД) для конкретного обменника"""
        db = DragonFlyConnector(exchange="binance")  # Используем одну базу
        if exchange_name == "Binance":
            return AsyncBinanceInfoClient(db)
        elif exchange_name == "Hyperliquid":
            return AsyncHyperliquidInfoClient(db)
        else:
            raise ValueError(f"Неизвестный обменник: {exchange_name}")

    async def _setup_trading_parameters(self):
        """Настройка одинаковых торговых параметров для обеих бирж"""
        # Вычисляем базовый размер в USD
        base_usd_amount = self._calculate_base_quantity()

        # Получаем цены для расчета количества
        ob1 = await self.exchange1Info.get_orderbook(self.asset)
        ob2 = await self.exchange2Info.get_orderbook(self.asset)

        if not (ob1.get('asks') and ob2.get('asks')):
            raise Exception("Не удалось получить цены для расчета размеров")

        # Берем среднюю цену для расчета
        avg_price = (float(ob1['asks'][0][0]) + float(ob2['asks'][0][0])) / 2

        # Рассчитываем количество в базовой валюте
        base_quantity = base_usd_amount / avg_price

        # Настраиваем precision для каждой биржи
        await self._setup_exchange_quantity(base_quantity, is_first=True)
        await self._setup_exchange_quantity(base_quantity, is_second=True)

        logger.info(f"💰 Базовый размер: ${base_usd_amount:.2f} | ~{base_quantity:.6f} {self.asset}")
        logger.info(f"📊 Размеры: {self.exchange1_name}={self.dealqty1} | {self.exchange2_name}={self.dealqty2}")

    async def _setup_exchange_quantity(self, base_quantity: float, is_first: bool = False, is_second: bool = False):
        """Настройка количества для конкретной биржи с учетом precision"""

        if is_first:
            exchange_name = self.exchange1_name
            websocket = self.exchange1WebSocket
        else:
            exchange_name = self.exchange2_name
            websocket = self.exchange2WebSocket

        if exchange_name == "Binance":
            # Для Binance получаем precision
            symbol_info = await websocket.get_symbol_info(self.asset)
            qty_precision = symbol_info['quantityPrecision']
            final_qty = round(base_quantity, qty_precision)

        elif exchange_name == "Hyperliquid":
            # Для Hyperliquid получаем precision
            symbol_info = await websocket.get_symbol_info(self.asset)
            qty_precision = symbol_info['quantityPrecision']
            final_qty = round(base_quantity, qty_precision)

        else:
            final_qty = round(base_quantity, 6)  # По умолчанию 6 знаков

        # Устанавливаем одинаковые размеры
        if is_first:
            self.dealqty1 = final_qty
        else:
            self.dealqty2 = final_qty

        # ВАЖНО: делаем размеры идентичными
        if hasattr(self, 'dealqty1') and hasattr(self, 'dealqty2'):
            # Берем минимальный precision для синхронизации
            min_precision = min(
                len(str(self.dealqty1).split('.')[-1]) if '.' in str(self.dealqty1) else 0,
                len(str(self.dealqty2).split('.')[-1]) if '.' in str(self.dealqty2) else 0
            )
            sync_qty = round(min(self.dealqty1, self.dealqty2), min_precision)
            self.dealqty1 = sync_qty
            self.dealqty2 = sync_qty

    def _calculate_base_quantity(self) -> float:
        """Вычисление базового количества для торговли"""
        return (self.config['trading_parameters']['margin'] *
                self.config['trading_parameters']['leverage'] /
                self.config['trading_parameters']['parts'])

    async def verify_and_balance_positions(self, current_step: int):
        """
        Проверяет реальные позиции и балансирует их при необходимости

        Args:
            current_step: текущий шаг (сколько частей уже набрано)
        """
        try:
            logger.info(f"🔍 Проверка позиций после шага {current_step}")

            # Получаем реальные размеры позиций
            position1 = await self.exchange1WebSocket.get_position_size(self.asset, self.frstDrctn)
            position2 = await self.exchange2WebSocket.get_position_size(self.asset, self.scndDrctn)

            # Приводим к float с проверкой
            pos1_float = self._safe_float(position1)
            pos2_float = self._safe_float(position2)

            # Ожидаемые размеры позиций
            expected_size = self.dealqty1 * current_step  # Должно быть набрано

            logger.info(f"📊 Позиции: {self.exchange1_name}={pos1_float:.6f} | {self.exchange2_name}={pos2_float:.6f}")
            logger.info(f"📊 Ожидаемый размер: {expected_size:.6f} каждая")

            # Вычисляем отклонения
            delta1 = pos1_float - expected_size
            delta2 = pos2_float - expected_size

            tolerance = 0.001  # Допустимое отклонение

            logger.info(f"📊 Отклонения: delta1={delta1:.6f} | delta2={delta2:.6f}")

            # Балансируем первую позицию
            if abs(delta1) > tolerance:
                await self._balance_position(
                    exchange_ws=self.exchange1WebSocket,
                    exchange_name=self.exchange1_name,
                    delta=delta1,
                    direction=self.frstDrctn,
                    symbol=self.asset
                )

            # Балансируем вторую позицию
            if abs(delta2) > tolerance:
                await self._balance_position(
                    exchange_ws=self.exchange2WebSocket,
                    exchange_name=self.exchange2_name,
                    delta=delta2,
                    direction=self.scndDrctn,
                    symbol=self.asset
                )

            logger.info("✅ Проверка позиций завершена")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка проверки позиций: {e}")
            return False

    def _safe_float(self, value) -> float:
        """Безопасно приводит значение к float"""
        if value is None:
            return 0.0
        if isinstance(value, str):
            try:
                return float(value)
            except (ValueError, TypeError):
                return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        return 0.0

    async def _balance_position(self, exchange_ws, exchange_name: str, delta: float, direction: str, symbol: str):
        """
        Балансирует позицию на конкретной бирже

        Args:
            exchange_ws: WebSocket клиент биржи
            exchange_name: название биржи
            delta: отклонение (+ значит избыток, - недостаток)
            direction: направление основной позиции ('long' или 'short')
            symbol: торговый символ
        """
        try:
            abs_delta = abs(delta)

            if abs_delta < 0.001:  # Слишком маленькое отклонение
                return

            logger.info(f"⚖️ Балансировка {exchange_name}: delta={delta:.6f}")

            if delta > 0:
                # Избыток позиции - нужно закрыть часть
                if direction == 'long':
                    # У нас лонг больше чем нужно - продаем излишек
                    await exchange_ws.close_market_order(symbol, 'short', abs_delta)
                    logger.info(f"📉 {exchange_name}: закрыли излишек лонга {abs_delta:.6f}")
                else:
                    # У нас шорт больше чем нужно - покупаем для закрытия
                    await exchange_ws.close_market_order(symbol, 'long', abs_delta)
                    logger.info(f"📈 {exchange_name}: закрыли излишек шорта {abs_delta:.6f}")

            else:
                # Недостаток позиции - нужно добрать
                if direction == 'long':
                    # Лонга не хватает - покупаем
                    await exchange_ws.place_market_order(symbol, 'long', abs_delta)
                    logger.info(f"📈 {exchange_name}: добрали лонг {abs_delta:.6f}")
                else:
                    # Шорта не хватает - продаем
                    await exchange_ws.place_market_order(symbol, 'short', abs_delta)
                    logger.info(f"📉 {exchange_name}: добрали шорт {abs_delta:.6f}")

        except Exception as e:
            logger.error(f"❌ Ошибка балансировки {exchange_name}: {e}")

    async def findDeal(self):
        await self.before_start()
        time.sleep(10)
        step = 0
        logger.info('🔍 Поиск арбитражных возможностей...')

        while step < self.config['trading_parameters']['parts']:
            spread_threshold = self.config['trading_parameters']['max_spread_percent']
            firstOrderbook = await self.exchange1Info.get_orderbook(symbol=self.asset)
            secondOrderbook = await self.exchange2Info.get_orderbook(symbol=self.asset)

            if not (firstOrderbook.get('bids') and len(firstOrderbook['bids']) > 0 and
                    secondOrderbook.get('asks') and len(secondOrderbook['asks']) > 0):
                logger.warning("⚠️ Пустые ордербуки, ждем...")
                await asyncio.sleep(2)
                continue

            first_bid, bid_volume = float(firstOrderbook['bids'][0][0]), float(firstOrderbook['bids'][0][1])
            second_ask, ask_volume = float(secondOrderbook['asks'][0][0]), float(secondOrderbook['asks'][0][1])

            self.prcDcmls1, self.qtyDcmls1 = get_decimal_places(first_bid), get_decimal_places(bid_volume)
            self.prcDcmls2, self.qtyDcmls2 = get_decimal_places(second_ask), get_decimal_places(ask_volume)

            current_spread = first_bid / second_ask
            spread_pct = (current_spread - 1) * 100

            logger.info(
                f"💱 {self.exchange1_name} bid={first_bid} | {self.exchange2_name} ask={second_ask} | спред={spread_pct:.4f}%")

            if current_spread < spread_threshold:
                logger.info('🎯 Выгодный спред! Размещаем ордера...')
                frstOrdr, scndOrdr = await asyncio.gather(
                    self.exchange1WebSocket.place_limit_order(
                        symbol=self.asset, side=self.frstDrctn, price=first_bid, qty=self.dealqty1
                    ),
                    self.exchange2WebSocket.place_limit_order(
                        symbol=self.asset, side=self.scndDrctn, price=second_ask, qty=self.dealqty2
                    ),
                    return_exceptions=True
                )

                fill_deal = await self.fillDeal(frstOrdr, scndOrdr)
                if fill_deal:
                    step += 1
                    logger.info(f'✅ Шаг {step}/{self.config["trading_parameters"]["parts"]} завершен')

                    # ВОТ ТУТ - ПРОВЕРЯЕМ И БАЛАНСИРУЕМ ПОЗИЦИИ
                    await self.verify_and_balance_positions(step)

        logger.info(f'🎉 Все {self.config["trading_parameters"]["parts"]} шагов завершены!')

        # Финальная проверка позиций
        await self.verify_and_balance_positions(self.config['trading_parameters']['parts'])
        return True

    async def fillDeal(self, frstOrdr, scndOrdr):
        """Быстрое заполнение с тайм-аутом"""
        logger.info(f"🎯 Исполнение ордеров: {frstOrdr['orderId']} | {scndOrdr['orderId']}")

        # Максимум 6 секунд на обычное исполнение
        start_time = time.time()

        while time.time() - start_time < 6.0:
            try:
                frstOrdrStts, scndOrdrStts = await asyncio.gather(
                    self.exchange1Info.get_order_status(order_id=frstOrdr['orderId']),
                    self.exchange2Info.get_order_status(order_id=scndOrdr['orderId']),
                    return_exceptions=True
                )

                if isinstance(frstOrdrStts, Exception) or isinstance(scndOrdrStts, Exception):
                    await asyncio.sleep(0.3)
                    continue

                first_fill = self._extract_fill_size(frstOrdrStts, frstOrdr)
                second_fill = self._extract_fill_size(scndOrdrStts, scndOrdr)
                target_qty1 = float(frstOrdr.get('qty', 0))
                target_qty2 = float(scndOrdr.get('qty', 0))

                logger.info(f"💰 Заполнение: {first_fill:.4f}/{target_qty1:.4f} | {second_fill:.4f}/{target_qty2:.4f}")

                # Оба исполнены
                if first_fill >= target_qty1 and second_fill >= target_qty2:
                    logger.info("✅ Оба ордера исполнены")
                    return True

                # Первый исполнен, добиваем второй
                if first_fill >= target_qty1 and second_fill < target_qty2 :
                    logger.info("🔄 Первый готов, добиваем второй")
                    await self.cancel_order(self.exchange2WebSocket, self.asset, scndOrdr['orderId'])
                    remaining = target_qty2 - second_fill
                    return await self.fillOrdr(self.exchange2WebSocket, self.exchange2Info, "new_order", "short",
                                               remaining)

                # Второй исполнен, добиваем первый
                if second_fill >= target_qty2 and first_fill < target_qty1 :
                    logger.info("🔄 Второй готов, добиваем первый")
                    await self.cancel_order(self.exchange1WebSocket, self.asset, frstOrdr['orderId'])
                    remaining = target_qty1 - first_fill
                    return await self.fillOrdr(self.exchange1WebSocket, self.exchange1Info, "new_order", "long",
                                               remaining)

                await asyncio.sleep(0.4)

            except Exception as e:
                logger.error(f"❌ Ошибка в fillDeal: {e}")
                await asyncio.sleep(0.5)

        # Время истекло - принудительно добиваем
        logger.warning("⏰ Время истекло, добиваем остатки")

        # Получаем финальные статусы
        try:
            frstOrdrStts, scndOrdrStts = await asyncio.gather(
                self.exchange1Info.get_order_status(order_id=frstOrdr['orderId']),
                self.exchange2Info.get_order_status(order_id=scndOrdr['orderId'])
            )

            first_fill = self._extract_fill_size(frstOrdrStts, frstOrdr)
            second_fill = self._extract_fill_size(scndOrdrStts, scndOrdr)
            target_qty1 = float(frstOrdr.get('qty', 0))
            target_qty2 = float(scndOrdr.get('qty', 0))

            tasks = []

            # Добиваем первый если нужно
            if first_fill < target_qty1:
                remaining1 = target_qty1 - first_fill
                tasks.append(self._complete_with_market(
                    self.exchange1WebSocket, self.exchange1Info, frstOrdr['orderId'], "long", remaining1
                ))

            # Добиваем второй если нужно
            if second_fill < target_qty2:
                remaining2 = target_qty2 - second_fill
                tasks.append(self._complete_with_market(
                    self.exchange2WebSocket, self.exchange2Info, scndOrdr['orderId'], "short", remaining2
                ))

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

        except Exception as e:
            logger.error(f"❌ Ошибка финального добивания: {e}")

        return True

    def _extract_fill_size(self, order_status, original_order):
        """Извлекает размер заполнения с проверками"""
        if not order_status:
            return 0.0

        fill_sz = order_status.get('fillSz', 0)

        # Обработка разных типов данных
        if isinstance(fill_sz, str):
            if fill_sz in ['Order has zero size.', '']:
                return float(original_order.get('qty', 0))
            try:
                return float(fill_sz)
            except:
                return 0.0

        return float(fill_sz) if fill_sz else 0.0

    async def _complete_with_market(self, exchange_ws, info_client, order_id, direction, remaining_qty):
        """Завершает ордер маркет-ордером"""
        try:
            if remaining_qty <= 0.001:
                return

            exchange_name = self.exchange1_name if exchange_ws == self.exchange1WebSocket else self.exchange2_name

            # Отменяем старый ордер
            await self.cancel_order(exchange_ws, self.asset, order_id)

            # Размещаем маркет на остаток
            logger.info(f"💥 {exchange_name}: завершаем маркетом {remaining_qty:.6f}")
            await exchange_ws.place_market_order(self.asset, direction, remaining_qty)

        except Exception as e:
            logger.error(f"❌ Ошибка завершения маркетом: {e}")

    async def fillOrdr(self, exchange_client, info_client, orderId, direction, qty):
        """Простая агрессивная логика: тик-сайз → маркет"""
        exchange_name = self.exchange1_name if exchange_client == self.exchange1WebSocket else self.exchange2_name

        # ЭТАП 1: Ждем обычное исполнение (3 секунды)
        start_time = time.time()
        logger.info(f"🔄 Ждем исполнения {exchange_name} ордера {orderId}...")

        while time.time() - start_time < 3.0:
            status = await info_client.get_order_status(order_id=orderId)
            if not status:
                await asyncio.sleep(0.2)
                continue

            total_filled = self._extract_fill_size(status, {'qty': qty})

            if total_filled >= qty:
                logger.info(f"✅ {exchange_name} ордер исполнен: {total_filled:.6f}")
                return True

            await asyncio.sleep(0.2)

        # ЭТАП 2: Тик-сайз лимит (1 попытка)
        try:
            logger.info(f"🎯 {exchange_name}: ордер +1 тик от лучшей цены")

            # Отменяем текущий ордер
            await self.cancel_order(exchange_client, self.asset, orderId)

            # Получаем остаток
            status = await info_client.get_order_status(order_id=orderId)
            current_filled = self._extract_fill_size(status, {'qty': qty})
            remaining = qty - current_filled

            if remaining <= 0.001:
                logger.info(f"✅ {exchange_name}: почти всё заполнено {remaining:.6f}")
                return True

            # Получаем тик-сайз и текущие цены
            tick_size = float(await exchange_client.get_tick_size(self.asset))
            orderbook = await self._get_orderbook_for_client(exchange_client, self.asset)

            if direction == "long":
                # Покупаем - берем лучший ask + 1 тик
                best_price = float(orderbook['asks'][0][0])
                tick_price = best_price + tick_size
            else:
                # Продаем - берем лучший bid - 1 тик
                best_price = float(orderbook['bids'][0][0])
                tick_price = best_price - tick_size

            # Размещаем лимит +/- 1 тик
            tick_order = await exchange_client.place_limit_order(
                symbol=self.asset,
                side=direction,
                price=tick_price,
                qty=remaining
            )

            logger.info(f"📌 {exchange_name}: лимит по цене {tick_price:.6f} (тик={tick_size})")

            # Ждем исполнения тик-лимита (2 секунды)
            tick_start = time.time()
            while time.time() - tick_start < 2.0:
                status = await info_client.get_order_status(tick_order['orderId'])
                if status:
                    filled = self._extract_fill_size(status, {'qty': remaining})
                    if filled >= remaining * 0.99:
                        logger.info(f"✅ {exchange_name}: тик-лимит исполнен")
                        return True
                await asyncio.sleep(0.1)

            # Тик-лимит не сработал - отменяем
            await self.cancel_order(exchange_client, self.asset, tick_order['orderId'])

            # Обновляем остаток после тик-лимита
            status = await info_client.get_order_status(tick_order['orderId'])
            tick_filled = self._extract_fill_size(status, {'qty': remaining})
            final_remaining = remaining - tick_filled

        except Exception as e:
            logger.error(f"❌ Ошибка тик-лимита {exchange_name}: {e}")
            final_remaining = remaining

        # ЭТАП 3: Маркет ордер
        try:
            if final_remaining > 0.001:
                logger.info(f"💥 {exchange_name}: МАРКЕТ ОРДЕР {final_remaining:.6f}")

                await exchange_client.place_market_order(
                    symbol=self.asset,
                    side=direction,
                    qty=final_remaining
                )

                logger.info(f"✅ {exchange_name}: маркет исполнен")

            return True

        except Exception as e:
            logger.error(f"❌ Критическая ошибка маркет ордера {exchange_name}: {e}")
            return False

    async def _get_orderbook_for_client(self, exchange_client, symbol, depth=10):
        """Получение ордербука через Info клиент для соответствующего WebSocket клиента"""
        if exchange_client == self.exchange1WebSocket:
            return await self.exchange1Info.get_orderbook(symbol=symbol)
        elif exchange_client == self.exchange2WebSocket:
            return await self.exchange2Info.get_orderbook(symbol=symbol)
        else:
            raise ValueError("Unknown exchange client")

    async def close_dual_position(self, qty1=None, qty2=None):
        if qty1 is None and qty2 is None:
            qty1 = await self.exchange1WebSocket.get_position_size(self.asset, self.frstDrctn)
            qty2 = await self.exchange2WebSocket.get_position_size(self.asset, self.scndDrctn)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: input(
            "Press Enter to start closing the deal...    QTY ДОЛЖЕН БЫТЬ ОДНОЙ ЧАСТЬЮ ОТ СДЕЛКИ"))
        await self.before_start()
        step = 0
        while step < self.config['trading_parameters']['parts']:
            firstOrderbook = await self.exchange1Info.get_orderbook(symbol=self.asset)
            secondOrderbook = await self.exchange2Info.get_orderbook(symbol=self.asset)
            first_bid, first_ask = float(firstOrderbook['bids'][0][0]), float(firstOrderbook['asks'][0][0])
            second_bid, second_ask = float(secondOrderbook['bids'][0][0]), float(secondOrderbook['asks'][0][0])
            current_spread = second_bid / first_ask
            spread_threshold = self.config['trading_parameters']['max_spread_close_percent']
            logger.info(f'spread - {spread_threshold}')
            if current_spread < spread_threshold:
                logger.info('spread to close')
                step += 1
                frstOrdr = await self.exchange1WebSocket.close_limit_order(
                    symbol=self.asset,
                    side="short",
                    price=first_ask,
                    qty=qty1
                )
                scndOrdr = await self.exchange2WebSocket.close_limit_order(
                    symbol=self.asset,
                    side="long",
                    price=second_bid,
                    qty=qty2
                )
                while True:
                    frstOrdrStts, scndOrdrStts = await asyncio.gather(
                        self.exchange1Info.get_order_status(order_id=frstOrdr['orderId']),
                        self.exchange2Info.get_order_status(order_id=scndOrdr['orderId'])
                    )
                    if frstOrdrStts['fillSz'] == qty1 and scndOrdrStts['fillSz'] != qty2:
                        await self.cancel_order(self.exchange2WebSocket, self.asset, scndOrdr['orderId'])
                        order = await self.exchange2WebSocket.close_limit_order(
                            symbol=self.asset,
                            side="long",
                            price=second_ask,
                            qty=float(qty2) - float(scndOrdrStts['fillSz'])
                        )
                        await self.fillcloseOrdr(self.exchange2Info, self.exchange2WebSocket, order['orderId'], "long",
                                                 float(qty2) - float(scndOrdrStts['fillSz']))
                        return True
                    if frstOrdrStts['fillSz'] != qty1 and scndOrdrStts['fillSz'] == qty2:
                        await self.cancel_order(self.exchange1WebSocket, self.asset, frstOrdr['orderId'])
                        order = await self.exchange1WebSocket.close_limit_order(
                            symbol=self.asset,
                            side="short",
                            price=first_bid,
                            qty=float(qty1) - float(frstOrdrStts['fillSz'])
                        )
                        await self.fillcloseOrdr(self.exchange1Info, self.exchange1WebSocket, order['orderId'], "short",
                                                 float(qty1) - float(frstOrdrStts['fillSz']))
                        return True
                    await asyncio.sleep(0.07)

    async def fillcloseOrdr(self, exchangeInfo, exchangeWebSocket, orderId, direction, qty):
        exchange_name = self.exchange1_name if exchangeWebSocket == self.exchange1WebSocket else self.exchange2_name
        start_time = time.time()
        tickSize = await exchangeWebSocket.get_tick_size(self.asset)
        while True:
            status = await exchangeInfo.get_order_status(order_id=orderId)
            total_filled = float(status['fillSz'])
            logger.info(f'total_filled {total_filled}')
            if float(total_filled) >= float(qty):
                logger.info(f'зафилило ордер1 - {exchange_name}')
                return True
            if time.time() - start_time > 4.7:
                await self.cancel_order(exchangeWebSocket=exchangeWebSocket, symbol=self.asset, order_id=orderId)
                total_filled = float(status['fillSz'])
                remaining = qty - total_filled
                logger.info(f'remaining - {remaining}')
                orderbook = await exchangeInfo.get_orderbook(symbol=self.asset, depth=10)
                price = float(orderbook['bids'][0][0] if direction == "short" else orderbook['asks'][0][0])
                new_order = await exchangeWebSocket.close_limit_order(symbol=self.asset, side=direction, price=price,
                                                                      qty=remaining)
                await asyncio.sleep(1)
                status = await exchangeInfo.get_order_status(new_order['orderId'])
                position = float(status['fillSz'])
                if position == remaining:
                    logger.info(f'зафилило ордер2 - {exchange_name}')
                    return True
                else:
                    await self.cancel_order(exchangeWebSocket, self.asset, new_order['orderId'])
                    lstchnce = remaining - position
                    logger.info(f'ласт шанс - {lstchnce}')
                    new_order = await exchangeWebSocket.close_limit_order(
                        symbol=self.asset,
                        side=direction,
                        price=float(price) + float(tickSize) if direction == 'long' else float(price) - float(tickSize),
                        qty=lstchnce
                    )
                    await asyncio.sleep(1)
                    status = await exchangeInfo.get_order_status(new_order['orderId'])
                    position = float(status['fillSz'])
                    if position == lstchnce:
                        logger.info(f'зафилило ордер3 - {exchange_name}')
                        return True
                    else:
                        await self.cancel_order(exchangeWebSocket, self.asset, orderId)
                        size = lstchnce - position
                        market_order = await exchangeWebSocket.close_market_order(symbol=self.asset, side=direction,
                                                                                  qty=size)
                        logger.info(f'зафилило ордер4 - {exchange_name}')
                        return True

    async def cancel_order(self, exchangeWebSocket, symbol, order_id):
        max_attempts = 2
        for attempt in range(1, max_attempts + 1):
            try:
                return await exchangeWebSocket.cancel_order(symbol, order_id)
            except Exception as e:
                txt = str(e).lower()
                keywords_map = {
                    "poz filled": ["filled", "already filled", "cannot cancel filled"],
                    "order not found": ["not found", "does not exist", "unknown order"],
                    "invalid order": ["invalid order", "bad order", "unsupported"],
                }
                for result, keys in keywords_map.items():
                    if any(k in txt for k in keys):
                        return result
                if attempt < max_attempts:
                    await asyncio.sleep(0.1)
                    continue
                return f"error: {txt}"
        return "cancel failed"

    async def dual_open(self, exchange1WebSocket, price1, exchange2WebSocket, price2, qty):
        """
        Отправляет лимитные ордера на обеих биржах и обрабатывает результаты
        Использует обычные лимитные ордера вместо FOK
        """
        logger.info(f'🎯 DUAL OPEN: qty={qty} prices={price1}/{price2}')

        start_time = time.time()

        try:
            # Одновременная отправка лимитных ордеров
            results = await asyncio.gather(
                exchange1WebSocket.place_limit_order(self.asset, self.frstDrctn, price1, qty),
                exchange2WebSocket.place_limit_order(self.asset, self.scndDrctn, price2, qty),
                return_exceptions=True
            )

            execution_time = (time.time() - start_time) * 1000
            logger.info(f'⚡ Dual execution time: {execution_time:.1f}ms')

            result1, result2 = results

            # Проверяем исключения
            if isinstance(result1, Exception):
                logger.error(f'❌ Exchange1 exception: {result1}')
                result1 = {"success": False, "error": str(result1)}

            if isinstance(result2, Exception):
                logger.error(f'❌ Exchange2 exception: {result2}')
                result2 = {"success": False, "error": str(result2)}

            # Получаем order_id для отслеживания
            order1_id = result1.get('orderId') if not isinstance(result1, Exception) else None
            order2_id = result2.get('orderId') if not isinstance(result2, Exception) else None

            # Быстро проверяем статус (2 секунды)
            await asyncio.sleep(2.0)

            # Проверяем заполнение
            status1 = await self.exchange1Info.get_order_status(order1_id) if order1_id else None
            status2 = await self.exchange2Info.get_order_status(order2_id) if order2_id else None

            fill1 = self._extract_fill_size(status1, result1) if status1 else 0
            fill2 = self._extract_fill_size(status2, result2) if status2 else 0

            # Анализируем результат
            filled1 = fill1 >= qty * 0.99
            filled2 = fill2 >= qty * 0.99

            if filled1 and filled2:
                # ✅ ОБА ИСПОЛНЕНЫ
                logger.info(f'✅ DUAL SUCCESS!')
                logger.info(f'   Exchange1: {fill1:.6f}@{price1}')
                logger.info(f'   Exchange2: {fill2:.6f}@{price2}')

                return {
                    "success": True,
                    "both_filled": True,
                    "exchange1_result": {"filledQty": fill1, "avgPrice": price1, "orderId": order1_id},
                    "exchange2_result": {"filledQty": fill2, "avgPrice": price2, "orderId": order2_id},
                    "action_taken": "both_filled"
                }

            elif filled1 and not filled2:
                # ⚠️ ТОЛЬКО ПЕРВЫЙ ИСПОЛНЕН - закрываем
                logger.warning(f'⚠️ Only Exchange1 filled - closing position')

                # Отменяем второй ордер
                if order2_id:
                    await self.cancel_order(exchange2WebSocket, self.asset, order2_id)

                # Закрываем первую позицию
                close_side = "short" if self.frstDrctn == "long" else "long"
                await exchange1WebSocket.close_market_order(self.asset, close_side, fill1)

                logger.info(f'🚨 Exchange1 position closed via market')

                return {
                    "success": False,
                    "both_filled": False,
                    "exchange1_result": {"filledQty": fill1, "avgPrice": price1},
                    "exchange2_result": {"filledQty": fill2, "avgPrice": price2},
                    "action_taken": "closed_exchange1"
                }

            elif not filled1 and filled2:
                # ⚠️ ТОЛЬКО ВТОРОЙ ИСПОЛНЕН - закрываем
                logger.warning(f'⚠️ Only Exchange2 filled - closing position')

                # Отменяем первый ордер
                if order1_id:
                    await self.cancel_order(exchange1WebSocket, self.asset, order1_id)

                # Закрываем вторую позицию
                close_side = "short" if self.scndDrctn == "long" else "long"
                await exchange2WebSocket.close_market_order(self.asset, close_side, fill2)

                logger.info(f'🚨 Exchange2 position closed via market')

                return {
                    "success": False,
                    "both_filled": False,
                    "exchange1_result": {"filledQty": fill1, "avgPrice": price1},
                    "exchange2_result": {"filledQty": fill2, "avgPrice": price2},
                    "action_taken": "closed_exchange2"
                }

            else:
                # 🔄 ОБА НЕ ИСПОЛНЕНЫ - отменяем
                logger.info(f'🔄 Both orders not filled - canceling')

                if order1_id:
                    await self.cancel_order(exchange1WebSocket, self.asset, order1_id)
                if order2_id:
                    await self.cancel_order(exchange2WebSocket, self.asset, order2_id)

                return {
                    "success": False,
                    "both_filled": False,
                    "exchange1_result": {"filledQty": fill1, "avgPrice": price1},
                    "exchange2_result": {"filledQty": fill2, "avgPrice": price2},
                    "action_taken": "both_rejected"
                }

        except Exception as e:
            logger.error(f'❌ Critical error in dual_open: {e}')
            return {
                "success": False,
                "both_filled": False,
                "exchange1_result": {"error": str(e)},
                "exchange2_result": {"error": str(e)},
                "action_taken": "exception"
            }