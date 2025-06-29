import logging
import time
import asyncio
from typing import Dict, Tuple

logger = logging.getLogger(__name__)


class SimplePnLCloser:
    def __init__(self, closer):
        """
        Простая PnL логика для закрытия позиций

        Args:
            closer: Инициализированный PositionCloser
        """
        self.closer = closer
        self.entry_data = None

    async def start_monitoring(self, target_pnl_percent: float = 0.3):
        """
        Запуск простого мониторинга PnL с автозакрытием

        Args:
            target_pnl_percent: Целевая прибыль в % (default: 0.3%)
        """
        logger.info(f"🎯 Запуск PnL мониторинга: цель = {target_pnl_percent}%")

        # Получаем данные о входе
        self.entry_data = await self._capture_entry_state()
        if not self.entry_data:
            logger.error("❌ Не удалось получить данные входа")
            return False

        logger.info(f"📊 Entry данные: {self.entry_data}")

        start_time = time.time()
        check_count = 0

        while True:
            check_count += 1

            # Рассчитываем текущий PnL
            current_pnl = await self._calculate_simple_pnl()

            runtime_minutes = (time.time() - start_time) / 60

            # Логируем каждые 10 проверок или каждую минуту
            if check_count % 10 == 0 or runtime_minutes >= 1:
                logger.info(f"💰 PnL: {current_pnl:.4f}% | Runtime: {runtime_minutes:.1f}m | Checks: {check_count}")

            # Проверяем условие закрытия
            if current_pnl >= target_pnl_percent:
                logger.info(f"🎉 ЦЕЛЬ ДОСТИГНУТА! PnL={current_pnl:.4f}% >= {target_pnl_percent}%")

                # Закрываем позицию
                success = await self._execute_close()
                if success:
                    logger.info("✅ Позиция успешно закрыта с прибылью!")
                    return True
                else:
                    logger.error("❌ Ошибка закрытия позиции")
                    return False

            # Защитный стоп-лосс
            elif current_pnl <= -2.0:
                logger.warning(f"🛑 СТОП-ЛОСС! PnL={current_pnl:.4f}% <= -2.0%")
                await self._execute_emergency_close()
                return False

            await asyncio.sleep(2)  # Проверка каждые 2 секунды

    async def _capture_entry_state(self) -> Dict:
        """Захватывает состояние на момент входа"""
        try:
            # Получаем текущие позиции
            pos1_long = await self.closer.exchange1_ws.get_position_size(self.closer.asset, "long")
            pos1_short = await self.closer.exchange1_ws.get_position_size(self.closer.asset, "short")
            pos2_long = await self.closer.exchange2_ws.get_position_size(self.closer.asset, "long")
            pos2_short = await self.closer.exchange2_ws.get_position_size(self.closer.asset, "short")

            # Получаем текущие цены
            current_prices = await self.closer._get_current_prices()

            # Параметры из конфига
            config = self.closer.config.get('trading_parameters', {})
            leverage = config.get('leverage', 3.0)
            margin = config.get('margin', 50.0)

            return {
                "positions": {
                    "exchange1_long": pos1_long,
                    "exchange1_short": pos1_short,
                    "exchange2_long": pos2_long,
                    "exchange2_short": pos2_short
                },
                "entry_prices": current_prices,
                "leverage": leverage,
                "margin": margin,
                "timestamp": time.time()
            }

        except Exception as e:
            logger.error(f"❌ Ошибка захвата entry данных: {e}")
            return None

    async def _calculate_simple_pnl(self) -> float:
        """
        Простой расчет PnL в процентах

        Returns:
            PnL в процентах от маржи
        """
        try:
            if not self.entry_data:
                return 0

            # Текущие цены
            current_prices = await self.closer._get_current_prices()

            # Entry данные
            entry_prices = self.entry_data["entry_prices"]
            positions = self.entry_data["positions"]
            leverage = self.entry_data["leverage"]
            margin = self.entry_data["margin"]

            # Рассчитываем PnL для каждой позиции
            total_pnl_usd = 0

            # Exchange 1 - лонг позиция
            if positions["exchange1_long"] > 0:
                price_diff = current_prices["exchange1"] - entry_prices["exchange1"]
                pnl_usd = price_diff * positions["exchange1_long"]
                total_pnl_usd += pnl_usd
                logger.debug(f"Exchange1 Long PnL: {pnl_usd:.4f} USD")

            # Exchange 1 - шорт позиция
            if positions["exchange1_short"] > 0:
                price_diff = entry_prices["exchange1"] - current_prices["exchange1"]
                pnl_usd = price_diff * positions["exchange1_short"]
                total_pnl_usd += pnl_usd
                logger.debug(f"Exchange1 Short PnL: {pnl_usd:.4f} USD")

            # Exchange 2 - лонг позиция
            if positions["exchange2_long"] > 0:
                price_diff = current_prices["exchange2"] - entry_prices["exchange2"]
                pnl_usd = price_diff * positions["exchange2_long"]
                total_pnl_usd += pnl_usd
                logger.debug(f"Exchange2 Long PnL: {pnl_usd:.4f} USD")

            # Exchange 2 - шорт позиция
            if positions["exchange2_short"] > 0:
                price_diff = entry_prices["exchange2"] - current_prices["exchange2"]
                pnl_usd = price_diff * positions["exchange2_short"]
                total_pnl_usd += pnl_usd
                logger.debug(f"Exchange2 Short PnL: {pnl_usd:.4f} USD")

            # Учитываем комиссии (примерно 0.05% от каждой сделки)
            total_volume = sum(positions.values())
            commission_usd = total_volume * 0.0005 * leverage  # 0.05% комиссии

            # Итоговый PnL в USD с вычетом комиссий
            net_pnl_usd = total_pnl_usd - commission_usd

            # Конвертируем в проценты от маржи
            pnl_percent = (net_pnl_usd / margin) * 100

            logger.debug(
                f"PnL расчет: {total_pnl_usd:.4f} USD - {commission_usd:.4f} комиссии = {net_pnl_usd:.4f} USD ({pnl_percent:.4f}%)")

            return pnl_percent

        except Exception as e:
            logger.error(f"❌ Ошибка расчета PnL: {e}")
            return -999

    async def _execute_close(self) -> bool:
        """Исполнение закрытия при достижении цели"""
        try:
            logger.info("🎯 Исполнение прибыльного закрытия...")

            # Определяем что закрывать
            positions = self.entry_data["positions"]

            # Логика для LongShort стратегии (long на 1й, short на 2й)
            if positions["exchange1_long"] > 0 and positions["exchange2_short"] > 0:
                return await self.closer.close_positions_aggressive(
                    positions["exchange1_long"],  # Закрываем лонг
                    positions["exchange2_short"],  # Закрываем шорт
                    "short",  # Продаем чтобы закрыть лонг
                    "long"  # Покупаем чтобы закрыть шорт
                )

            # Логика для ShortLong стратегии (short на 1й, long на 2й)
            elif positions["exchange1_short"] > 0 and positions["exchange2_long"] > 0:
                return await self.closer.close_positions_aggressive(
                    positions["exchange1_short"],  # Закрываем шорт
                    positions["exchange2_long"],  # Закрываем лонг
                    "long",  # Покупаем чтобы закрыть шорт
                    "short"  # Продаем чтобы закрыть лонг
                )

            # Если неопределенная конфигурация - экстренное закрытие
            else:
                logger.warning("⚠️ Неопределенная конфигурация позиций - экстренное закрытие")
                return await self.closer.emergency_close_all()

        except Exception as e:
            logger.error(f"❌ Ошибка исполнения закрытия: {e}")
            return False

    async def _execute_emergency_close(self) -> bool:
        """Экстренное закрытие при стоп-лоссе"""
        logger.warning("🚨 Экстренное закрытие по стоп-лоссу!")
        try:
            return await self.closer.emergency_close_all()
        except Exception as e:
            logger.error(f"❌ Ошибка экстренного закрытия: {e}")
            return False

    async def check_current_pnl(self) -> float:
        """Простая проверка текущего PnL"""
        if not self.entry_data:
            self.entry_data = await self._capture_entry_state()

        current_pnl = await self._calculate_simple_pnl()

        logger.info(f"📊 Текущий PnL: {current_pnl:.4f}%")

        if current_pnl >= 0.3:
            logger.info("🎉 PnL достиг цели 0.3%!")
        elif current_pnl <= -2.0:
            logger.warning("🛑 PnL достиг стоп-лосса -2.0%!")
        else:
            logger.info("⏳ PnL в норме")

        return current_pnl


# === ПРОСТЫЕ ФУНКЦИИ ДЛЯ run_strategy.py ===

async def simple_pnl_close():
    """Простое PnL закрытие - основная функция"""
    try:
        # Импорты
        from logic.PositionCloser import create_position_closer

        # Создаем базовый PositionCloser
        closer = await create_position_closer(
            exchange1_name="Binance",
            exchange2_name="Hyperliquid",
            asset="FARTCOIN",
            config_path="config.json"  # Путь к твоему конфигу
        )

        # Создаем простой PnL монитор
        pnl_monitor = SimplePnLCloser(closer)

        logger.info("✅ SimplePnLCloser инициализирован")
        logger.info("🎯 Цель: 0.3% левереджной прибыли")
        logger.info("🛑 Стоп: -2.0% убытка")

        # Запускаем мониторинг
        result = await pnl_monitor.start_monitoring(target_pnl_percent=0.3)

        logger.info(f"🏁 PnL мониторинг завершен: {result}")
        return result

    except Exception as e:
        logger.error(f"❌ Ошибка simple_pnl_close: {e}")
        return False


async def quick_pnl_check():
    """Быстрая проверка текущего PnL"""
    try:
        from logic.PositionCloser import create_position_closer

        closer = await create_position_closer("Binance", "Hyperliquid", "FARTCOIN", "config.json")
        pnl_monitor = SimplePnLCloser(closer)

        current_pnl = await pnl_monitor.check_current_pnl()

        return current_pnl

    except Exception as e:
        logger.error(f"❌ Ошибка quick_pnl_check: {e}")
        return None


async def force_close_if_profitable():
    """Принудительное закрытие если PnL > 0.3%"""
    try:
        from logic.PositionCloser import create_position_closer

        closer = await create_position_closer("Binance", "Hyperliquid", "FARTCOIN", "config.json")
        pnl_monitor = SimplePnLCloser(closer)

        # Проверяем PnL
        current_pnl = await pnl_monitor.check_current_pnl()

        if current_pnl >= 0.3:
            logger.info(f"💰 PnL={current_pnl:.4f}% >= 0.3% - ПРИНУДИТЕЛЬНОЕ ЗАКРЫТИЕ!")
            return await pnl_monitor._execute_close()
        else:
            logger.info(f"⏳ PnL={current_pnl:.4f}% < 0.3% - закрытие не требуется")
            return False

    except Exception as e:
        logger.error(f"❌ Ошибка force_close_if_profitable: {e}")
        return False