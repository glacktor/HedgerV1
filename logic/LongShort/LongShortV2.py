import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
import asyncio
import json
import os

# ПРАВИЛЬНЫЙ ИМПОРТ - класс, а не модуль
from logic.LongShort.LongShort import LongShort

logger = logging.getLogger(__name__)


class LongShortV2(LongShort):
    def __init__(self, exchange1_name: Any, exchange2_name: Any, reverse: bool, asset: str,
                 config_path: str = r"C:/Users/strel/PycharmProjects/rn_need_projects/jekius_maximus/logic/config.json"):
        super().__init__(exchange1_name, exchange2_name, reverse, asset, config_path)

        # КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: обратные направления
        self.frstDrctn = "long"  # Лонг на первой бирже
        self.scndDrctn = "short"  # Шорт на второй бирже

    async def findDeal(self):
        await self.before_start()
        # time.sleep(10)  # Убираем лишние задержки
        step = 0
        logger.info('🔍 Поиск арбитражных возможностей V2...')

        while step < self.config['trading_parameters']['parts']:
            spread_threshold = self.config['trading_parameters']['max_spread_percent']
            firstOrderbook = await self.exchange1Info.get_orderbook(symbol=self.asset)
            secondOrderbook = await self.exchange2Info.get_orderbook(symbol=self.asset)

            if not (firstOrderbook.get('asks') and len(firstOrderbook['asks']) > 0 and
                    secondOrderbook.get('bids') and len(secondOrderbook['bids']) > 0):
                logger.warning("⚠️ Пустые ордербуки, ждем...")
                await asyncio.sleep(0.1)  # Быстрее
                continue

            # КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: берем ask первой и bid второй
            first_ask, ask_volume = float(firstOrderbook['asks'][0][0]), float(firstOrderbook['asks'][0][1])
            second_bid, bid_volume = float(secondOrderbook['bids'][0][0]), float(secondOrderbook['bids'][0][1])

            # КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: обратный расчет спреда
            current_spread = first_ask / second_bid
            spread_pct = (current_spread - 1) * 100

            logger.info(
                f"💱 V2: {self.exchange1_name} ask={first_ask} | {self.exchange2_name} bid={second_bid} | спред={spread_pct:.4f}%")

            if current_spread < spread_threshold:
                logger.info('🎯 Выгодный спред V2! Атакуем!')

                start_time = time.time()
                # Одновременная отправка - покупаем ask, продаем bid
                frstOrdr, scndOrdr = await asyncio.gather(
                    self.exchange1WebSocket.place_limit_order(
                        symbol=self.asset, side=self.frstDrctn, price=first_ask, qty=self.dealqty1
                    ),
                    self.exchange2WebSocket.place_limit_order(
                        symbol=self.asset, side=self.scndDrctn, price=second_bid, qty=self.dealqty2
                    ),
                    return_exceptions=True
                )

                execution_time = (time.time() - start_time) * 1000
                logger.info(f'⚡ V2 Orders placed in {execution_time:.1f}ms')

                # Проверка исключений
                if isinstance(frstOrdr, Exception) or isinstance(scndOrdr, Exception):
                    logger.error(f'❌ V2 Order failed: {frstOrdr if isinstance(frstOrdr, Exception) else scndOrdr}')
                    await asyncio.sleep(0.05)
                    continue

                fill_deal = await self.fillDeal(frstOrdr, scndOrdr)
                if fill_deal:
                    step += 1
                    logger.info(f'✅ V2 Шаг {step}/{self.config["trading_parameters"]["parts"]} завершен')
                    await self.verify_and_balance_positions(step)
            else:
                await asyncio.sleep(0.05)  # Быстрые проверки

        logger.info(f'🎉 V2 - Все {self.config["trading_parameters"]["parts"]} шагов завершены!')
        await self.verify_and_balance_positions(self.config['trading_parameters']['parts'])
        return True

    async def market_attack_v2(self):
        """Максимально агрессивная V2 стратегия с маркет ордерами"""
        await self.before_start()
        step = 0
        logger.info('🚀 V2 MARKET ATTACK MODE!')

        while step < self.config['trading_parameters']['parts']:
            spread_threshold = self.config['trading_parameters']['max_spread_percent']

            # Быстрое получение ордербуков
            firstOrderbook, secondOrderbook = await asyncio.gather(
                self.exchange1Info.get_orderbook(symbol=self.asset),
                self.exchange2Info.get_orderbook(symbol=self.asset)
            )

            if not (firstOrderbook.get('asks') and secondOrderbook.get('bids')):
                await asyncio.sleep(0.01)
                continue

            first_ask = float(firstOrderbook['asks'][0][0])
            second_bid = float(secondOrderbook['bids'][0][0])
            current_spread = first_ask / second_bid
            spread_pct = (current_spread - 1) * 100

            if current_spread < spread_threshold:
                logger.info(f'🎯 V2 MARKET ATTACK! spread={spread_pct:.4f}%')

                start_time = time.time()
                # Мгновенные маркет ордера
                await asyncio.gather(
                    self.exchange1WebSocket.place_market_order(self.asset, "long", self.dealqty1),
                    self.exchange2WebSocket.place_market_order(self.asset, "short", self.dealqty2)
                )

                execution_time = (time.time() - start_time) * 1000
                logger.info(f'⚡ V2 Market attack: {execution_time:.1f}ms')

                step += 1
                logger.info(f'✅ V2 Market step {step} completed')

                # Быстрая проверка позиций
                if step % 2 == 0:  # Проверяем каждый второй шаг
                    await self.verify_and_balance_positions(step)

        logger.info(f'🏁 V2 MARKET ATTACK COMPLETE!')
        return True