import json
import logging
import os
import asyncio
import time

from CexWsClients.AsyncBinanceWSClient import AsyncBinanceWSClient
from CexWsClients.AsyncHyperliquidWSClient import AsyncHyperliquidWSClient
from DragonflyDb.DragonFlyConnector import DragonFlyConnector
from InfoClients.AsyncBinanceInfoClient import AsyncBinanceInfoClient
from InfoClients.AsyncHyperliquidInfoClient import AsyncHyperliquidInfoClient
from config.config import ROI
logger = logging.getLogger(__name__)

async def init_exchange_client(exchange_name: str, config: dict, asset: str, db):
    print(f"🔄 Инициализация {exchange_name}...")

    if exchange_name == "Binance":
        ws_client = AsyncBinanceWSClient(
            config["api_keys"]["binance"]["api_key"],
            config["api_keys"]["binance"]["api_secret"]
        )
        info_client = AsyncBinanceInfoClient(db)
        await ws_client.connect_ws()
        await ws_client.subscribe_orderbook(asset)

    elif exchange_name == "Hyperliquid":
        ws_client = AsyncHyperliquidWSClient.from_key(
            config["api_keys"]["hyperliquid"]["api_key"]
        )
        info_client = AsyncHyperliquidInfoClient(db)
        await ws_client.connect_ws()
        await ws_client.subscribe_orderbook(asset)

    else:
        raise ValueError(f"Неизвестный обменник: {exchange_name}")

    print(f"✅ {exchange_name} готов")
    return ws_client, info_client

class Scalper:
    def _calculate_base_quantity(self) -> float:
        """Вычисление базового количества для торговли"""
        return (self.config['trading_parameters']['margin'] *
                self.config['trading_parameters']['leverage'] /
                self.config['trading_parameters']['parts'])

    async def _setup_exchange_quantity(self, base_quantity: float, is_first: bool = False, is_second: bool = False):
        """Настройка количества для конкретной биржи с учетом precision"""

        if is_first:
            exchange_name = self.exchange1_name
            websocket = self.exchange1
        else:
            exchange_name = self.exchange2_name
            websocket = self.exchange2

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

    def __init__(self,asset,config_path: str = r"C:/Users/strel/PycharmProjects/rn_need_projects/jekius_maximus/logic/config.json"):
        self.asset = asset
        self.initialized = False
        self.exchange1_name = 'Binance'
        self.exchange2_name = 'Hyperliquid'
        self.frstDrctn = None
        self.scndDrctn = None
        if not os.path.exists(config_path):
            logger.error(f"Config file {config_path} not found")
            raise FileNotFoundError(f"Config file {config_path} not found")
        with open(config_path, 'r') as config_file:
            self.config = json.load(config_file)

    async def checkBeforeStart(self):
        """
        Проверяет готовность к работе и инициализирует если нужно
        Унифицированный метод для всех торговых классов
        """
        if self.initialized:
            return True

        try:
            # Инициализация клиентов
            await self._init_exchange_clients()

            # Настройка торговых параметров
            await self._setup_trading_parameters()

            self.initialized = True
            logger.info("✅ Scalper готов к работе")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка инициализации Scalper: {e}")
            return False

    async def _init_exchange_clients(self):
        """Простая инициализация через универсальную функцию"""
        # ФИКС: создаем отдельные DB коннекторы для каждой биржи
        db1 = DragonFlyConnector(exchange=self.exchange1_name.lower())  # "binance"
        db2 = DragonFlyConnector(exchange=self.exchange2_name.lower())  # "hyperliquid"

        # Инициализируем первый обменник с его DB
        self.exchange1, self.exchange1Info = await init_exchange_client(
            self.exchange1_name, self.config, self.asset, db1
        )

        # Инициализируем второй обменник с его DB
        self.exchange2, self.exchange2Info = await init_exchange_client(
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

    async def _setup_trading_parameters(self):
        """Настройка одинаковых торговых параметров для обеих бирж"""
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

    async def _complete_with_market(self, exchange_ws, info_client, order_id, direction, remaining_qty):
        """Завершает ордер маркет-ордером"""
        try:
            if remaining_qty <= 0.001:
                return

            exchange_name = self.exchange1_name if exchange_ws == self.exchange1 else self.exchange2_name

            # Отменяем старый ордер
            await self.cancel_order(exchange_ws, self.asset, order_id)

            # Размещаем маркет на остаток
            logger.info(f"💥 {exchange_name}: завершаем маркетом {remaining_qty:.6f}")
            await exchange_ws.place_market_order(self.asset, direction, remaining_qty)

        except Exception as e:
            logger.error(f"❌ Ошибка завершения маркетом: {e}")

    async def fillOrdr(self, exchange_client, info_client, orderId, direction, qty):
        """Простая агрессивная логика: тик-сайз → маркет"""
        exchange_name = self.exchange1_name if exchange_client == self.exchange1 else self.exchange2_name

        # ЭТАП 1: Ждем обычное исполнение (3 секунды)
        start_time = time.time()
        logger.info(f"🔄 Ждем исполнения {exchange_name} ордера {orderId}...")

        while time.time() - start_time < 3.0:
            status = await info_client.get_order_status(order_id=orderId)
            if not status:
                await asyncio.sleep(0.2)
                continue

            total_filled = self._extract_fill_size(status, {'qty': qty})

            if total_filled == qty:
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
        if exchange_client == self.exchange1:
            return await self.exchange1Info.get_orderbook(symbol=symbol)
        elif exchange_client == self.exchange2:
            return await self.exchange2Info.get_orderbook(symbol=symbol)
        else:
            raise ValueError("Unknown exchange client")

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

    async def fillDeal(self, frstOrdr, scndOrdr):
        """Заполнение с активным удержанием на пике"""
        logger.info(f"🎯 Исполнение ордеров: {frstOrdr['orderId']} | {scndOrdr['orderId']}")

        try:
            # Проверяем заполнение после пикового удержания
            target_qty1 = float(frstOrdr.get('qty', 0))
            target_qty2 = float(scndOrdr.get('qty', 0))

            # Получаем текущие позиции для проверки заполнения
            pos1_info = await self.exchange1.get_position_info(self.asset)
            pos2_info = await self.exchange2.get_position_info(self.asset)

            # Извлекаем размеры позиций
            pos1_size = 0
            pos2_size = 0

            if isinstance(pos1_info, list):
                for pos in pos1_info:
                    if pos.get('symbol') == f"{self.asset}USDT":
                        pos1_size = abs(float(pos.get('positionAmt', 0)))
                        break
            elif pos1_info:
                pos1_size = abs(float(pos1_info.get('size', 0)))

            if pos2_info:
                pos2_size = abs(float(pos2_info.get('size', 0)))

            logger.info(f"💰 Позиции : {pos1_size:.4f}/{target_qty1:.4f} | {pos2_size:.4f}/{target_qty2:.4f}")

            # Проверяем заполнение с толерантностью
            tolerance = 0.001
            filled1 = pos1_size >= target_qty1
            filled2 = pos2_size >= target_qty2

            # Оба заполнены
            if filled1 and filled2:
                logger.info("✅ Оба ордера исполнены на пике")
                return True

            # Первый заполнен, добиваем второй
            if filled1 and not filled2:
                logger.info("🔄 Первый готов, добиваем второй")
                remaining = target_qty2 - pos2_size
                print("remaining"+str(remaining))
                return await self.fillOrdr(self.exchange2, self.exchange2Info, scndOrdr['orderId'] , self.scndDrctn,
                                           remaining)

            # Второй заполнен, добиваем первый
            if filled2 and not filled1:
                logger.info("🔄 Второй готов, добиваем первый")
                remaining = target_qty1 - pos1_size
                print("remaining"+str(remaining))
                return await self.fillOrdr(self.exchange1, self.exchange1Info, frstOrdr['orderId'], self.frstDrctn,
                                           remaining)

            logger.info("🎯 Запуск удержания на пике (6с)")
            await self.highestOrdersFor(6)
            tasks = []
            if not filled1:
                remaining1 = target_qty1 - pos1_size
                tasks.append(self.exchange1.place_market_order(self.asset, self.frstDrctn, remaining1))

            if not filled2:
                remaining2 = target_qty2 - pos2_size
                tasks.append(self.exchange2.place_market_order(self.asset, self.scndDrctn, remaining2))

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
                logger.info("✅ Принудительное заполнение маркетом")

            return True

        except Exception as e:
            logger.error(f"❌ Критическая ошибка fillDeal: {e}")

            # Экстренное заполнение маркетом
            try:
                await asyncio.gather(
                    self.exchange1.place_market_order(self.asset, self.frstDrctn, self.dealqty1),
                    self.exchange2.place_market_order(self.asset, self.scndDrctn, self.dealqty2),
                    return_exceptions=True
                )
                logger.info("🚨 Экстренное заполнение маркетом")
            except Exception as emergency_e:
                logger.error(f"❌ Провал экстренного заполнения: {emergency_e}")

            return False

    async def highestOrdersFor(self, seconds):
        """
        Удерживает ордеры на пике ордербука в течение указанного времени
        Использует self.frstDrctn, self.scndDrctn, self.dealqty1, self.dealqty2
        """
        if not self.initialized:
            raise RuntimeError("Scalper не инициализирован")

        if not hasattr(self, 'frstDrctn') or not self.frstDrctn:
            raise RuntimeError("Направления сделки не установлены")

        # Преобразуем направления в side для определения целевых цен
        side1 = 'buy' if self.frstDrctn == 'long' else 'sell'
        side2 = 'buy' if self.scndDrctn == 'long' else 'sell'

        start_time = time.time()
        order1_id = None
        order2_id = None

        logger.info(f"🎯 Старт мониторинга пика на {seconds}с")

        try:
            while time.time() - start_time < seconds:
                # Получаем ордербуки
                ob1, ob2 = await asyncio.gather(
                    self.exchange1Info.get_orderbook(self.asset),
                    self.exchange2Info.get_orderbook(self.asset),
                    return_exceptions=True
                )

                if isinstance(ob1, Exception) or isinstance(ob2, Exception):
                    await asyncio.sleep(0.1)
                    continue

                if not (ob1.get('bids') and ob1.get('asks') and ob2.get('bids') and ob2.get('asks')):
                    await asyncio.sleep(0.1)
                    continue

                # Определяем целевые цены на пике
                tick1 = float(await self.exchange1.get_tick_size(self.asset))
                tick2 = float(await self.exchange2.get_tick_size(self.asset))

                if side1 == 'buy':
                    target_price1 = float(ob1['bids'][0][0]) + tick1
                else:
                    target_price1 = float(ob1['asks'][0][0]) - tick1

                if side2 == 'buy':
                    target_price2 = float(ob2['bids'][0][0]) + tick2
                else:
                    target_price2 = float(ob2['asks'][0][0]) - tick2

                # Проверяем нужно ли обновить ордер на exchange1
                needs_update1 = True
                if order1_id:
                    try:
                        status = await self.exchange1Info.get_order_status(order1_id)
                        if status and abs(float(status.get('price', 0)) - target_price1) < tick1 / 2:
                            needs_update1 = False
                    except:
                        pass

                # Проверяем нужно ли обновить ордер на exchange2
                needs_update2 = True
                if order2_id:
                    try:
                        status = await self.exchange2Info.get_order_status(order2_id)
                        if status and abs(float(status.get('price', 0)) - target_price2) < tick2 / 2:
                            needs_update2 = False
                    except:
                        pass

                # Обновляем ордеры если нужно
                tasks = []

                if needs_update1:
                    if order1_id:
                        tasks.append(self.cancel_order(self.exchange1, self.asset, order1_id))
                    tasks.append(
                        self.exchange1.place_limit_order(self.asset, self.frstDrctn, target_price1, self.dealqty1))

                if needs_update2:
                    if order2_id:
                        tasks.append(self.cancel_order(self.exchange2, self.asset, order2_id))
                    tasks.append(
                        self.exchange2.place_limit_order(self.asset, self.scndDrctn, target_price2, self.dealqty2))

                if tasks:
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    # Обновляем ID ордеров
                    if needs_update1:
                        order1_id = results[-2]['orderId'] if len(results) >= 2 and isinstance(results[-2],
                                                                                               dict) else None
                    if needs_update2:
                        order2_id = results[-1]['orderId'] if len(results) >= 1 and isinstance(results[-1],
                                                                                               dict) else None

                    logger.info(f"🔄 Обновлены ордеры: {target_price1:.6f} | {target_price2:.6f}")

                await asyncio.sleep(0.2)  # Быстрая проверка

        except Exception as e:
            logger.error(f"❌ Ошибка в highestOrdersFor: {e}")
        finally:
            # Отменяем оставшиеся ордеры
            cleanup_tasks = []
            if order1_id:
                cleanup_tasks.append(self.cancel_order(self.exchange1, self.asset, order1_id))
            if order2_id:
                cleanup_tasks.append(self.cancel_order(self.exchange2, self.asset, order2_id))

            if cleanup_tasks:
                await asyncio.gather(*cleanup_tasks, return_exceptions=True)

            logger.info(f"✅ Мониторинг пика завершен")

        return {'order1_id': order1_id, 'order2_id': order2_id}

    # Использование:
    # await scalper.highestOrdersFor(30)  # 30 секунд на пике

    async def FindDeal(self):
        if not self.initialized:
            raise RuntimeError("Scalper не инициализирован")
            # 1. ПРОВЕРЯЕМ ЕСТЬ ЛИ ОТКРЫТЫЕ ПОЗИЦИИ
        # has_open_positions = await self._check_has_positions()

        # 2. ЕСЛИ ЕСТЬ ПОЗИЦИИ - ПЫТАЕМСЯ ЗАКРЫТЬ
        # if has_open_positions:
        #     logger.info("📍 Есть позиции - пытаемся закрыть")
        #     closed = await self.closingDeal()
        #     if closed:
        #         logger.info("✅ Позиции закрыты")
        #         await self.verify_positions_after_close()
        #     await asyncio.sleep(0.5)

            # 3. ЕСЛИ НЕТ ПОЗИЦИЙ - ИЩЕМ НОВУЮ СДЕЛКУ
        while True:
            try:
                ob1 = await self.exchange1Info.get_orderbook(self.asset)
                ob2 = await self.exchange2Info.get_orderbook(self.asset)

                if not (ob1.get('bids') and ob1.get('asks') and ob2.get('bids') and ob2.get('asks')):
                    print("⚠️ Пустые ордербуки")
                    await asyncio.sleep(0.1)
                    continue

                bid1, ask1 = float(ob1['bids'][0][0]), float(ob1['asks'][0][0])
                bid2, ask2 = float(ob2['bids'][0][0]), float(ob2['asks'][0][0])

                spread1 = 1 - ask1 / bid2  # Binance long / Hyperliquid short
                spread2 = 1 - ask2 / bid1  # Hyperliquid long / Binance short
                min_spread = 0.0026

                # ПОКАЗЫВАЕМ СПРЕДЫ ПОСТОЯННО
                print(f"🔍 Поиск: spread1={spread1:.6f} | spread2={spread2:.6f} | мин={min_spread}")

                # 4. ЕСЛИ ХОРОШИЙ СПРЕД - ОТКРЫВАЕМ
                if spread1 >= min_spread or spread2 >= min_spread:

                    if spread1 >= min_spread:
                        self.frstDrctn = 'long'
                        self.scndDrctn = 'short'
                        logger.info(f'🎯 Открываем spread1: {spread1:.4f}')
                        print(ob1)
                        print(ob2)
                        print(f"📊 {self.exchange1_name} long@{bid1} | {self.exchange2_name} short@{ask2}")

                        frstOrdr, scndOrdr = await asyncio.gather(
                            self.exchange1.place_limit_order(self.asset, self.frstDrctn, ask1, self.dealqty1),
                            self.exchange2.place_limit_order(self.asset, self.scndDrctn, bid2, self.dealqty2)
                        )
                    else:
                        self.frstDrctn = 'short'
                        self.scndDrctn = 'long'
                        logger.info(f'🎯 Открываем spread2: {spread2:.4f}')
                        print(f"📊 {self.exchange1_name} short@{ask1} | {self.exchange2_name} long@{bid2}")
                        print(ob1)
                        print(ob2)
                        frstOrdr, scndOrdr = await asyncio.gather(
                            self.exchange1.place_limit_order(self.asset, self.frstDrctn, bid1, self.dealqty1),
                            self.exchange2.place_limit_order(self.asset, self.scndDrctn, ask2, self.dealqty2)
                        )

                    # 5. ИСПОЛНЯЕМ СДЕЛКУ
                    success = await self.fillDeal(frstOrdr, scndOrdr)

                    if success:
                        logger.info("✅ Сделка открыта")
                        await self.verify_positions_after_open()
                        await asyncio.get_event_loop().run_in_executor(None, input, "Нажмите Enter для продолжения...")
                        await self.closingDeal()
                    else:
                        logger.info("❌ Сделка не открылась")

                    await asyncio.sleep(1)

                else:
                    # Показываем что спреды маленькие
                    if spread1 > 0.0005 or spread2 > 0.0005:
                        print(f"📉 Спреды близко: s1={spread1:.6f} s2={spread2:.6f}")
                    await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"❌ Ошибка в FindDeal: {e}")
                print(f"❌ Ошибка: {e}")
                await asyncio.sleep(1)

    async def _check_has_positions(self):
        """Быстрая проверка наличия позиций"""
        try:
            pos1_info = await self.exchange1.get_position_info(self.asset)
            pos2_info = await self.exchange2.get_position_info(self.asset)

            # Проверяем первую биржу
            has_pos1 = False
            if isinstance(pos1_info, list):
                for pos in pos1_info:
                    if pos.get('symbol') == f"{self.asset}USDT" and abs(float(pos.get('positionAmt', 0))) > 0.001:
                        has_pos1 = True
                        break
            elif pos1_info and abs(pos1_info.get('size', 0)) > 0.001:
                has_pos1 = True

            # Проверяем вторую биржу
            has_pos2 = pos2_info and abs(pos2_info.get('size', 0)) > 0.001

            return has_pos1 or has_pos2

        except Exception as e:
            logger.error(f"❌ Ошибка проверки позиций: {e}")
            return False

    async def verify_positions_after_open(self):
        """Верификация позиций после открытия сделки"""
        try:
            logger.info("🔍 Верификация позиций после открытия")

            pos1_info = await self.exchange1.get_position_info(self.asset)
            pos2_info = await self.exchange2.get_position_info(self.asset)

            # Получаем РЕАЛЬНЫЕ размеры с учетом направления
            pos1_size = 0
            pos2_size = 0

            # Binance - используем positionAmt напрямую (уже с знаком)
            if isinstance(pos1_info, list):
                for pos in pos1_info:
                    if pos.get('symbol') == f"{self.asset}USDT":
                        pos1_size = float(pos.get('positionAmt', 0))  # БЕЗ abs()
                        break
            elif pos1_info and pos1_info.get('size', 0) != 0:
                # Если get_position_info возвращает dict с size и side
                size = float(pos1_info.get('size', 0))
                side = pos1_info.get('side', 'long')
                pos1_size = size if side == 'long' else -size

            # Hyperliquid - тоже правильный знак
            if pos2_info and pos2_info.get('size', 0) != 0:
                # Получаем правильный знак из user_state
                try:
                    user_state = await asyncio.get_running_loop().run_in_executor(
                        None, self.exchange2.exchange.info.user_state, self.exchange2.account_address
                    )
                    for pos in user_state.get('assetPositions', []):
                        if pos['position']['coin'] == self.asset:
                            pos2_size = float(pos['position']['szi'])  # szi с правильным знаком
                            break
                except:
                    # Фоллбэк - определяем по направлению сделки
                    size = float(pos2_info.get('size', 0))
                    pos2_size = size if self.scndDrctn == 'long' else -size

            # Ожидаемые размеры
            expected1 = self.dealqty1 if self.frstDrctn == 'long' else -self.dealqty1
            expected2 = self.dealqty2 if self.scndDrctn == 'long' else -self.dealqty2

            logger.info(f"📊 Позиции: {self.exchange1_name}={pos1_size:.6f} | {self.exchange2_name}={pos2_size:.6f}")
            logger.info(f"📊 Ожидаемые: {expected1:.6f} | {expected2:.6f}")

            # ТОЛЬКО логируем расхождения, НЕ исправляем автоматически
            tolerance = 0.01
            delta1 = pos1_size - expected1
            delta2 = pos2_size - expected2

            if abs(delta1) > tolerance:
                logger.warning(f"⚠️ Расхождение {self.exchange1_name}: delta={delta1:.6f}")

            if abs(delta2) > tolerance:
                logger.warning(f"⚠️ Расхождение {self.exchange2_name}: delta={delta2:.6f}")

            # Если расхождения критичны - НЕ торгуем дальше
            if abs(delta1) > self.dealqty1 * 0.1 or abs(delta2) > self.dealqty2 * 0.1:
                logger.error("❌ Критичные расхождения позиций!")
                return False

            logger.info("✅ Верификация позиций завершена")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка верификации: {e}")
            return False

    async def verify_positions_after_close(self):
        """Верификация что позиции действительно закрыты"""
        try:
            logger.info("🔍 Верификация закрытия позиций")

            pos1_info = await self.exchange1.get_position_info(self.asset)
            pos2_info = await self.exchange2.get_position_info(self.asset)

            # Проверяем что позиции закрыты
            pos1_size = 0
            pos2_size = 0

            if isinstance(pos1_info, list):
                for pos in pos1_info:
                    if pos.get('symbol') == f"{self.asset}USDT":
                        pos1_size = float(pos.get('positionAmt', 0))
                        break
            elif pos1_info:
                pos1_size = abs(float(pos1_info.get('size', 0)))

            if pos2_info:
                pos2_size = abs(float(pos2_info.get('size', 0)))

            logger.info(
                f"📊 Остатки позиций: {self.exchange1_name}={pos1_size:.6f} | {self.exchange2_name}={pos2_size:.6f}")

            # Принудительно закрываем остатки
            if pos1_size > 0.001:
                logger.info(f"🔧 Принудительно закрываем остаток на {self.exchange1_name}: {pos1_size:.6f}")
                close_side = "short" if self.frstDrctn == 'long' else "long"
                await self.exchange1.place_market_order(self.asset, close_side, pos1_size)

            if pos2_size > 0.001:
                logger.info(f"🔧 Принудительно закрываем остаток на {self.exchange2_name}: {pos2_size:.6f}")
                close_side = "long" if self.scndDrctn == 'short' else "short"
                await self.exchange2.place_market_order(self.asset, close_side, pos2_size)

            logger.info("✅ Верификация закрытия завершена")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка верификации закрытия: {e}")
            return False

    async def _balance_position(self, exchange_ws, exchange_name: str, delta: float, direction: str):
        """Балансирует позицию на конкретной бирже (как в LongShort)"""
        try:
            abs_delta = abs(delta)
            if abs_delta < 0.001:
                return

            logger.info(f"⚖️ Балансировка {exchange_name}: delta={delta:.6f}")

            if delta > 0:
                # Избыток позиции - закрываем
                close_side = "short" if direction == 'long' else "long"
                await exchange_ws.place_market_order(self.asset, close_side, abs_delta)
                logger.info(f"📉 {exchange_name}: закрыли излишек {abs_delta:.6f}")
            else:
                # Недостаток позиции - добираем
                await exchange_ws.place_market_order(self.asset, direction, abs_delta)
                logger.info(f"📈 {exchange_name}: добрали {abs_delta:.6f}")

        except Exception as e:
            logger.error(f"❌ Ошибка балансировки {exchange_name}: {e}")

    async def closingDeal(self):
        """Мониторинг ROI и закрытие позиций при достижении цели"""
        logger.info("🎯 Старт мониторинга ROI для закрытия позиций")

        while True:
            try:
                pos1_info = await self.exchange1.get_position_info(self.asset)
                pos2_info = await self.exchange2.get_position_info(self.asset)

                # Проверяем что позиции существуют и не пустые
                if not pos1_info or not pos2_info:
                    logger.info("ℹ️ Нет позиций для мониторинга")
                    await asyncio.sleep(5)
                    continue

                # Получаем размеры позиций и цены входа
                pos1_size = 0
                pos2_size = 0
                entry_price1 = 0
                entry_price2 = 0

                # Для Binance - ищем позицию по символу в списке
                if isinstance(pos1_info, list):
                    for pos in pos1_info:
                        if pos.get('symbol') == f"{self.asset}USDT":
                            pos1_size = float(pos.get('positionAmt', 0))
                            entry_price1 = float(pos.get('entryPrice', 0))
                            break
                elif isinstance(pos1_info, dict):
                    pos1_size = abs(float(pos1_info.get('size', 0)))
                    entry_price1 = float(pos1_info.get('avg_price', 0))
                else:
                    pos1_size = abs(float(pos1_info)) if pos1_info != 0 else 0

                if isinstance(pos2_info, dict):
                    pos2_size = abs(float(pos2_info.get('size', 0)))
                    entry_price2 = float(pos2_info.get('avg_price', 0))
                else:
                    pos2_size = abs(float(pos2_info)) if pos2_info != 0 else 0

                # Если позиции меньше минимального размера - нет позиций
                if pos1_size < 0.001 or pos2_size < 0.001:
                    logger.info("ℹ️ Нет активных позиций для закрытия")
                    await asyncio.sleep(5)
                    continue

                if entry_price1 <= 0 or entry_price2 <= 0:
                    logger.error("❌ Некорректные цены входа")
                    await asyncio.sleep(2)
                    continue

                # Получаем текущие цены выхода
                ob1 = await self.exchange1Info.get_orderbook(self.asset)
                ob2 = await self.exchange2Info.get_orderbook(self.asset)

                if not (ob1.get('bids') and ob1.get('asks') and ob2.get('bids') and ob2.get('asks')):
                    await asyncio.sleep(1)
                    continue

                # Определяем направления позиций автоматически
                is_long1 = pos1_size > 0 if isinstance(pos1_info, list) else True

                if is_long1:
                    exit_price1 = float(ob1['bids'][0][0])
                    exit_price2 = float(ob2['asks'][0][0])
                    roi1 = (exit_price1 / entry_price1 - 1) * 100
                    roi2 = (entry_price2 / exit_price2 - 1) * 100
                else:
                    exit_price1 = float(ob1['asks'][0][0])
                    exit_price2 = float(ob2['bids'][0][0])
                    roi1 = (entry_price1 / exit_price1 - 1) * 100
                    roi2 = (exit_price2 / entry_price2 - 1) * 100

                total_roi = roi1 + roi2
                target_roi = ROI

                # Показываем ROI каждые 5 итераций
                print(f"💰 ROI: {total_roi:.3f}% | Target: {target_roi}% | Позиции: {pos1_size:.3f}/{pos2_size:.3f}")

                if total_roi >= target_roi:
                    qty = min(abs(pos1_size), abs(pos2_size))

                    # Проверяем достаточность объема для закрытия
                    if is_long1:
                        available_qty1 = float(ob1['bids'][0][1])
                        available_qty2 = float(ob2['asks'][0][1])
                    else:
                        available_qty1 = float(ob1['asks'][0][1])
                        available_qty2 = float(ob2['bids'][0][1])

                    if available_qty1 < qty or available_qty2 < qty:
                        logger.warning("⚠️ Недостаточно ликвидности для закрытия")
                        await asyncio.sleep(1)
                        continue

                    logger.info(f'🎯 ROI {total_roi:.2f}% достигнут - закрываем!')

                    # Закрываем позиции
                    close1_side = self.opposite_side(self.frstDrctn)
                    close2_side = self.opposite_side(self.scndDrctn)

                    close1_task = self.exchange1.close_limit_order(
                        self.asset, close1_side, exit_price1, qty
                    )
                    close2_task = self.exchange2.close_limit_order(
                        self.asset, close2_side, exit_price2, qty
                    )

                    close1, close2 = await asyncio.gather(close1_task, close2_task)
                    print(close1)
                    print(close2)
                    success = await self.fillDeal(close1, close2)

                    if success:
                        logger.info("✅ Позиции успешно закрыты")
                        await self.verify_positions_after_close()
                        return True
                    else:
                        logger.error("❌ Ошибка закрытия позиций")

                await asyncio.sleep(1)  # Пауза между проверками

            except Exception as e:
                logger.error(f"❌ Ошибка в closingDeal: {e}")
                await asyncio.sleep(2)
                continue

    def opposite_side(self, side: str) -> str:
        return 'short' if side.lower() == 'long' else 'long'