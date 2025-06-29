import asyncio
import os
import json
import logging
from typing import Any

logger = logging.getLogger(__name__)
def get_decimal_places(price: float) -> int:
    price_str = str(price)
    return len(price_str.split('.')[1]) if '.' in price_str else 0

class SpreadStrategy:
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

    def createScalpConfig(self):
        #на основе csv надо создать конфиг
        #тут нам нужно получить значение спреда, от которого нам нормально заходить в позу, тоесть на основе данных из csv файла нам нужно посчитать сзнаечния стандартного расхождения цен, что бы мы могли зарабатывать на аномальном отклонении из за крупных сделок и тп,

    async def findDeal(self):
        #нужно
        firstOrderbook = await self.exchange1Info.get_orderbook(symbol=self.asset)
        secondOrderbook = await self.exchange2Info.get_orderbook(symbol=self.asset)
        if not (firstOrderbook.get('bids') and len(firstOrderbook['bids']) > 0 and
                secondOrderbook.get('asks') and len(secondOrderbook['asks']) > 0):
            logger.warning("⚠️ Пустые ордербуки, ждем...")
            await asyncio.sleep(2)
            # continue

        first_bid, bid_volume = float(firstOrderbook['bids'][0][0]), float(firstOrderbook['bids'][0][1])
        second_ask, ask_volume = float(secondOrderbook['asks'][0][0]), float(secondOrderbook['asks'][0][1])
        self.prcDcmls1, self.qtyDcmls1 = get_decimal_places(first_bid), get_decimal_places(bid_volume)
        self.prcDcmls2, self.qtyDcmls2 = get_decimal_places(second_ask), get_decimal_places(ask_volume)
        #здесь мы считаем какой спред сейчас и сравниваем его со спредом который получаем из createScalpConfig


