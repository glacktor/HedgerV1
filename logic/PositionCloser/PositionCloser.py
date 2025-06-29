import logging
import time
import asyncio
import json
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class PositionCloser:
    def __init__(self, exchange1_ws, exchange1_info, exchange2_ws, exchange2_info,
                 exchange1_name: str, exchange2_name: str, asset: str, config: dict):
        """
        Инициализация закрывателя позиций

        Args:
            exchange1_ws: WebSocket клиент первой биржи
            exchange1_info: Info клиент первой биржи
            exchange2_ws: WebSocket клиент второй биржи
            exchange2_info: Info клиент второй биржи
            exchange1_name: Название первой биржи
            exchange2_name: Название второй биржи
            asset: Торговый актив
            config: Конфигурация с параметрами
        """
        self.exchange1_ws = exchange1_ws
        self.exchange1_info = exchange1_info
        self.exchange2_ws = exchange2_ws
        self.exchange2_info = exchange2_info
        self.exchange1_name = exchange1_name
        self.exchange2_name = exchange2_name
        self.asset = asset
        self.config = config

    async def close_positions_limit(self, qty1: float, qty2: float,
                                    direction1: str = "short", direction2: str = "long"):
        """
        Закрытие позиций лимитными ордерами (классический способ)

        Args:
            qty1: Количество для закрытия на первой бирже
            qty2: Количество для закрытия на второй бирже
            direction1: Направление закрытия на первой бирже ("long" или "short")
            direction2: Направление закрытия на второй бирже ("long" или "short")
        """
        logger.info(f'🔄 Закрытие лимитами: {qty1} {direction1} | {qty2} {direction2}')

        step = 0
        parts = self.config.get('trading_parameters', {}).get('parts', 3)

        while step < parts:
            # Получаем ордербуки
            firstOrderbook = await self.exchange1_info.get_orderbook(symbol=self.asset)
            secondOrderbook = await self.exchange2_info.get_orderbook(symbol=self.asset)

            if not (firstOrderbook.get('bids') and firstOrderbook.get('asks') and
                    secondOrderbook.get('bids') and secondOrderbook.get('asks')):
                logger.warning("⚠️ Пустые ордербуки при закрытии")
                await asyncio.sleep(1)
                continue

            # Определяем цены для закрытия
            first_bid = float(firstOrderbook['bids'][0][0])
            first_ask = float(firstOrderbook['asks'][0][0])
            second_bid = float(secondOrderbook['bids'][0][0])
            second_ask = float(secondOrderbook['asks'][0][0])

            # Рассчитываем спред для закрытия
            current_spread = second_bid / first_ask
            spread_threshold = self.config.get('trading_parameters', {}).get('max_spread_close_percent', 1.001)

            logger.info(f'📊 Спред закрытия: {current_spread:.6f} | порог: {spread_threshold}')

            if current_spread < spread_threshold:
                logger.info('🎯 Выгодный спред для закрытия!')

                # Определяем цены для ордеров
                price1 = first_ask if direction1 == "long" else first_bid
                price2 = second_bid if direction2 == "short" else second_ask

                # Размещаем закрывающие ордера
                start_time = time.time()
                frstOrdr, scndOrdr = await asyncio.gather(
                    self.exchange1_ws.close_limit_order(
                        symbol=self.asset, side=direction1, price=price1, qty=qty1
                    ),
                    self.exchange2_ws.close_limit_order(
                        symbol=self.asset, side=direction2, price=price2, qty=qty2
                    ),
                    return_exceptions=True
                )

                execution_time = (time.time() - start_time) * 1000
                logger.info(f'⚡ Закрывающие ордера: {execution_time:.1f}ms')

                if isinstance(frstOrdr, Exception) or isinstance(scndOrdr, Exception):
                    logger.error(f'❌ Ошибка ордеров: {frstOrdr if isinstance(frstOrdr, Exception) else scndOrdr}')
                    await asyncio.sleep(1)
                    continue

                # Ждем исполнения
                success = await self._wait_for_fill(frstOrdr, scndOrdr, qty1, qty2)
                if success:
                    step += 1
                    logger.info(f'✅ Закрытие шаг {step}/{parts} завершен')

            else:
                await asyncio.sleep(0.5)

        logger.info(f'🏁 Закрытие лимитами завершено!')
        return True

    async def close_positions_market(self, qty1: float, qty2: float,
                                     direction1: str = "short", direction2: str = "long"):
        """
        Мгновенное закрытие позиций маркет ордерами

        Args:
            qty1: Количество для закрытия на первой бирже
            qty2: Количество для закрытия на второй бирже
            direction1: Направление закрытия на первой бирже
            direction2: Направление закрытия на второй бирже
        """
        logger.info(f'💥 МАРКЕТ ЗАКРЫТИЕ: {qty1} {direction1} | {qty2} {direction2}')

        start_time = time.time()

        # Одновременные маркет ордера
        results = await asyncio.gather(
            self.exchange1_ws.close_market_order(self.asset, direction1, qty1),
            self.exchange2_ws.close_market_order(self.asset, direction2, qty2),
            return_exceptions=True
        )

        execution_time = (time.time() - start_time) * 1000
        logger.info(f'⚡ Маркет закрытие: {execution_time:.1f}ms')

        # Проверяем результаты
        result1, result2 = results

        if isinstance(result1, Exception):
            logger.error(f'❌ {self.exchange1_name} маркет ошибка: {result1}')
        else:
            logger.info(f'✅ {self.exchange1_name} закрыт')

        if isinstance(result2, Exception):
            logger.error(f'❌ {self.exchange2_name} маркет ошибка: {result2}')
        else:
            logger.info(f'✅ {self.exchange2_name} закрыт')

        return not (isinstance(result1, Exception) or isinstance(result2, Exception))

    async def close_positions_aggressive(self, qty1: float, qty2: float,
                                         direction1: str = "short", direction2: str = "long"):
        """
        Агрессивное закрытие: лимиты с тайм-аутом → маркеты

        Args:
            qty1: Количество для закрытия на первой бирже
            qty2: Количество для закрытия на второй бирже
            direction1: Направление закрытия на первой бирже
            direction2: Направление закрытия на второй бирже
        """
        logger.info(f'🎯 АГРЕССИВНОЕ ЗАКРЫТИЕ: {qty1} {direction1} | {qty2} {direction2}')

        # ЭТАП 1: Пробуем лимиты (3 секунды)
        logger.info("🔄 Этап 1: Лимитные ордера")

        # Получаем ордербуки
        firstOrderbook, secondOrderbook = await asyncio.gather(
            self.exchange1_info.get_orderbook(symbol=self.asset),
            self.exchange2_info.get_orderbook(symbol=self.asset)
        )

        if firstOrderbook.get('bids') and secondOrderbook.get('asks'):
            # Размещаем лимиты по лучшим ценам
            price1 = float(firstOrderbook['asks'][0][0]) if direction1 == "long" else float(
                firstOrderbook['bids'][0][0])
            price2 = float(secondOrderbook['bids'][0][0]) if direction2 == "short" else float(
                secondOrderbook['asks'][0][0])

            start_time = time.time()
            frstOrdr, scndOrdr = await asyncio.gather(
                self.exchange1_ws.close_limit_order(self.asset, direction1, price1, qty1),
                self.exchange2_ws.close_limit_order(self.asset, direction2, price2, qty2),
                return_exceptions=True
            )

            if not (isinstance(frstOrdr, Exception) or isinstance(scndOrdr, Exception)):
                # Ждем исполнения 3 секунды
                while time.time() - start_time < 3.0:
                    fill1, fill2 = await self._check_fills(frstOrdr, scndOrdr)

                    if fill1 >= qty1 * 0.99 and fill2 >= qty2 * 0.99:
                        logger.info("✅ Лимиты исполнены!")
                        return True

                    await asyncio.sleep(0.2)

                # Отменяем неисполненные ордера
                logger.info("⏰ Лимиты не исполнились, отменяем")
                await asyncio.gather(
                    self._cancel_order_safe(self.exchange1_ws, frstOrdr['orderId']),
                    self._cancel_order_safe(self.exchange2_ws, scndOrdr['orderId']),
                    return_exceptions=True
                )

        # ЭТАП 2: Маркет ордера
        logger.info("💥 Этап 2: Маркет ордера")
        return await self.close_positions_market(qty1, qty2, direction1, direction2)

    async def close_single_exchange(self, exchange_ws, exchange_name: str,
                                    qty: float, direction: str):
        """
        Закрытие позиции только на одной бирже

        Args:
            exchange_ws: WebSocket клиент биржи
            exchange_name: Название биржи
            qty: Количество для закрытия
            direction: Направление закрытия
        """
        logger.info(f'🎯 Закрытие {exchange_name}: {qty} {direction}')

        try:
            result = await exchange_ws.close_market_order(self.asset, direction, qty)
            logger.info(f'✅ {exchange_name} позиция закрыта: {result}')
            return True
        except Exception as e:
            logger.error(f'❌ Ошибка закрытия {exchange_name}: {e}')
            return False

    async def emergency_close_all(self):
        """
        Экстренное закрытие всех позиций маркет ордерами
        """
        logger.warning("🚨 ЭКСТРЕННОЕ ЗАКРЫТИЕ ВСЕХ ПОЗИЦИЙ!")

        try:
            # Получаем размеры позиций
            pos1 = await self.exchange1_ws.get_position_size(self.asset, "long")
            pos2 = await self.exchange1_ws.get_position_size(self.asset, "short")
            pos3 = await self.exchange2_ws.get_position_size(self.asset, "long")
            pos4 = await self.exchange2_ws.get_position_size(self.asset, "short")

            tasks = []

            # Закрываем все ненулевые позиции
            if pos1 > 0:
                tasks.append(self.exchange1_ws.close_market_order(self.asset, "short", pos1))
            if pos2 > 0:
                tasks.append(self.exchange1_ws.close_market_order(self.asset, "long", pos2))
            if pos3 > 0:
                tasks.append(self.exchange2_ws.close_market_order(self.asset, "short", pos3))
            if pos4 > 0:
                tasks.append(self.exchange2_ws.close_market_order(self.asset, "long", pos4))

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                logger.info(f"🚨 Экстренное закрытие завершено: {len(tasks)} ордеров")
                return True
            else:
                logger.info("✅ Позиций для закрытия не найдено")
                return True

        except Exception as e:
            logger.error(f"❌ Ошибка экстренного закрытия: {e}")
            return False

    # === ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ===

    async def _wait_for_fill(self, order1, order2, target_qty1: float, target_qty2: float):
        """Ожидание исполнения ордеров"""
        start_time = time.time()

        while time.time() - start_time < 10.0:  # 10 секунд максимум
            fill1, fill2 = await self._check_fills(order1, order2)

            if fill1 >= target_qty1 * 0.99 and fill2 >= target_qty2 * 0.99:
                return True

            await asyncio.sleep(0.3)

        return False

    async def _check_fills(self, order1, order2):
        """Проверка заполнения ордеров"""
        try:
            status1, status2 = await asyncio.gather(
                self.exchange1_info.get_order_status(order1['orderId']),
                self.exchange2_info.get_order_status(order2['orderId']),
                return_exceptions=True
            )

            fill1 = float(status1.get('fillSz', 0)) if not isinstance(status1, Exception) else 0
            fill2 = float(status2.get('fillSz', 0)) if not isinstance(status2, Exception) else 0

            return fill1, fill2
        except:
            return 0, 0

    async def _cancel_order_safe(self, exchange_ws, order_id: str):
        """Безопасная отмена ордера"""
        try:
            await exchange_ws.cancel_order(self.asset, order_id)
        except Exception as e:
            logger.debug(f"Отмена ордера {order_id}: {e}")


# === ФАБРИКА ДЛЯ СОЗДАНИЯ ЗАКРЫВАТЕЛЯ ===

async def create_position_closer(exchange1_name: str, exchange2_name: str,
                                 asset: str, config_path: str):
    """
    Фабрика для создания PositionCloser с инициализированными клиентами

    Args:
        exchange1_name: Название первой биржи
        exchange2_name: Название второй биржи
        asset: Торговый актив
        config_path: Путь к конфигу

    Returns:
        PositionCloser: Готовый к работе закрыватель позиций
    """
    from DragonflyDb.DragonFlyConnector import DragonFlyConnector
    from InfoClients.AsyncBinanceInfoClient import AsyncBinanceInfoClient
    from InfoClients.AsyncHyperliquidInfoClient import AsyncHyperliquidInfoClient
    from CexWsClients.AsyncBinanceWSClient import AsyncBinanceWSClient
    from CexWsClients.AsyncHyperliquidWSClient import AsyncHyperliquidWSClient
    import json

    # Загружаем конфиг
    with open(config_path, 'r') as f:
        config = json.load(f)

    # Создаем DB коннекторы
    db1 = DragonFlyConnector(exchange=exchange1_name.lower())
    db2 = DragonFlyConnector(exchange=exchange2_name.lower())

    # Инициализируем клиентов
    if exchange1_name == "Binance":
        ws1 = AsyncBinanceWSClient(
            config["api_keys"]["binance"]["api_key"],
            config["api_keys"]["binance"]["api_secret"]
        )
        info1 = AsyncBinanceInfoClient(db1)
        await ws1.connect_ws()

    elif exchange1_name == "Hyperliquid":
        ws1 = AsyncHyperliquidWSClient.from_key(
            config["api_keys"]["hyperliquid"]["api_key"]
        )
        info1 = AsyncHyperliquidInfoClient(db1)
        await ws1.connect_ws()

    if exchange2_name == "Binance":
        ws2 = AsyncBinanceWSClient(
            config["api_keys"]["binance"]["api_key"],
            config["api_keys"]["binance"]["api_secret"]
        )
        info2 = AsyncBinanceInfoClient(db2)
        await ws2.connect_ws()

    elif exchange2_name == "Hyperliquid":
        ws2 = AsyncHyperliquidWSClient.from_key(
            config["api_keys"]["hyperliquid"]["api_key"]
        )
        info2 = AsyncHyperliquidInfoClient(db2)
        await ws2.connect_ws()

    return PositionCloser(
        ws1, info1, ws2, info2,
        exchange1_name, exchange2_name, asset, config
    )
