import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
import asyncio
import json
import os

from CexClients.AsyncBinanceClient import AsyncBinanceClient
from CexClients.AsyncOKXClient import AsyncOKXClient

logger = logging.getLogger(__name__)

def get_decimal_places(price: float) -> int:
    price_str = str(price)
    return len(price_str.split('.')[1]) if '.' in price_str else 0

def round_to_decimals(value: float, decimals: int) -> float:
    return round(value, decimals)

def adjust_quantity(deal_qty, constraints):
    """
    Подгоняет желаемое количество контрактов под параметры инструмента.

    Аргументы:
        deal_qty (float): Желаемое количество контрактов (self.dealqty2).
        constraints (dict): Словарь с параметрами:
            - min_quantity (float): Минимальное допустимое количество.
            - lot_size (float): Шаг округления.

    Возвращает:
        int: Округленное количество, кратное lot_size и не меньше min_quantity.
    """
    min_quantity = float(constraints['min_quantity'])
    lot_size = float(constraints['lot_size'])

    # Округляем deal_qty до ближайшего значения, кратного lot_size
    rounded_quantity = round(deal_qty / lot_size) * lot_size

    # Убеждаемся, что результат не меньше min_quantity
    adjusted_quantity = max(rounded_quantity, min_quantity)

    # Возвращаем целое число, так как quantityPrecision предполагает целые значения
    return int(adjusted_quantity)

class ShortLong:
    def __init__(self, exchange1_name: Any, exchange2_name: Any, reverse: bool, asset: str,
                 config_path: str = "config.json"):
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
        self.frstDrctn = "short"
        self.scndDrctn = "long"
        self.dealqty = self.config['trading_parameters']['margin'] * self.config['trading_parameters']['leverage'] / self.config['trading_parameters']['parts']#это в долларах
        self.credited_tokens1 = 0
        self.credited_tokens2 = 0

    async def before_start(self):
        if self.exchange1_name == "Binance":
            self.exchange1_client = AsyncBinanceClient(api_key='fcaUIecvfxnbJsmsvBZvKUzod2VfqNze1KbzsW7lRsw6rwqbzIeanjcumWTitORb', api_secret='ekTjDZONsRvUBYFfwk8sZxo5aMhCG1fV1IH5inY4PD7wENa2WzY9vfQjhp0VhKQr')
        if self.exchange1_name == "OKX":
            self.exchange1_client = AsyncOKXClient(api_key='87aaa32f-4e11-4637-8a2e-b01e463c5cb1', api_secret='591624966329886D2300DC5E3E2D7C6F', passphrase='Popochka228!')
            vvv = await self.exchange1_client.get_symbol_info(self.asset)
            self.nmbrtknsOkxCntrcts = vvv['ctVal']
            self.nmbrdcmlsOkxCntrcts = vvv['quantityPrecision']
            self.OKXprmtrs = await self.exchange1_client.get_quantity_constraints(self.asset)
            secondOrderbook = await self.exchange1_client.get_orderbook(symbol=self.asset, depth=10)
            first_ask = float(secondOrderbook['asks'][0][0])
            self.dealqty1 = adjust_quantity((self.config['trading_parameters']['margin'] *
                                             self.config['trading_parameters']['leverage'] /
                                             self.config['trading_parameters']['parts']) / first_ask, self.OKXprmtrs)
            self.dealqty2 = self.dealqty1
        if self.exchange2_name == "Binance":
            self.exchange2_client = AsyncBinanceClient(api_key='fcaUIecvfxnbJsmsvBZvKUzod2VfqNze1KbzsW7lRsw6rwqbzIeanjcumWTitORb', api_secret='ekTjDZONsRvUBYFfwk8sZxo5aMhCG1fV1IH5inY4PD7wENa2WzY9vfQjhp0VhKQr')
        if self.exchange2_name == "OKX":
            self.exchange2_client = AsyncOKXClient(api_key='87aaa32f-4e11-4637-8a2e-b01e463c5cb1', api_secret='591624966329886D2300DC5E3E2D7C6F', passphrase='Popochka228!')
            vvv = await self.exchange2_client.get_symbol_info(self.asset)
            self.nmbrtknsOkxCntrcts = vvv['ctVal']
            self.nmbrdcmlsOkxCntrcts = vvv['quantityPrecision']
            self.OKXprmtrs = await self.exchange2_client.get_quantity_constraints(self.asset)
            secondOrderbook = await self.exchange2_client.get_orderbook(symbol=self.asset, depth=10)
            second_ask = float(secondOrderbook['asks'][0][0])
            self.dealqty2 = float(adjust_quantity((self.config['trading_parameters']['margin']*self.config['trading_parameters']['leverage']/self.config['trading_parameters']['parts'])/second_ask, self.OKXprmtrs))
            self.dealqty1 = self.dealqty2
            # await self.exchange1_client.set_leverage(self.asset, self.config['trading_parameters']['leverage'],
            #                                          'isolated', "long")
            # await self.exchange1_client.set_leverage(self.asset, self.config['trading_parameters']['leverage'],
            #                                          'isolated', "short")
            await self.exchange2_client.set_leverage(self.asset, self.config['trading_parameters']['leverage'],
                                                     'isolated', "long")
            await self.exchange2_client.set_leverage(self.asset, self.config['trading_parameters']['leverage'],
                                                     'isolated', "short")

    async def findDeal(self):
        #открываем лонг на первой бирже и шорт на второй бирже
        await self.before_start()
        step = 0
        print('searching')
        while step < self.config['trading_parameters']['parts']:
            if self.exchange1_client != 0:
                #вот тут нужно закрывать перебор
                pass
            if self.exchange2_client != 0:
                # вот тут нужно закрывать перебор
                pass

            spread_threshold = self.config['trading_parameters']['max_spread_percent']
            firstOrderbook = await self.exchange1_client.get_orderbook(symbol=self.asset, depth=10)
            secondOrderbook = await self.exchange2_client.get_orderbook(symbol=self.asset, depth=10)
            first_ask, ask_volume = float(firstOrderbook['asks'][0][0]), float(firstOrderbook['asks'][0][1])
            second_bid,bid_volume  = float(secondOrderbook['bids'][0][0]), float(secondOrderbook['bids'][0][1])
            self.prcDcmls1, self.qtyDcmls1 = get_decimal_places(first_ask), get_decimal_places(ask_volume)
            self.prcDcmls2, self.qtyDcmls2 = get_decimal_places(second_bid), get_decimal_places(bid_volume)
            current_spread = second_bid / first_ask
            if current_spread < spread_threshold:
                print('spread')
                print('place_limit_order')
                frstOrdr = await self.exchange1_client.place_limit_order(symbol=self.asset, side=self.frstDrctn, price=first_ask, qty=self.dealqty1)
                scndOrdr = await self.exchange2_client.place_limit_order(symbol=self.asset, side=self.scndDrctn, price=second_bid, qty=self.dealqty2)
                fill_deal = await self.fillDeal(frstOrdr, scndOrdr)
                if fill_deal:
                    step+=1
                    print('+шаг')
        #вот тут нужна проверка на то какие сайзы открылись
        position1 = await self.exchange1_client.get_position_size(self.asset, self.frstDrctn)
        position2 = await self.exchange2_client.get_position_size(self.asset, self.scndDrctn)
        delta1 = abs(position1) - self.dealqty1*self.config['trading_parameters']['parts']
        delta2 = abs(position2) - self.dealqty2*self.config['trading_parameters']['parts']
        if float(delta1) > float(0):
            self.exchange1_client.close_market_order(self.asset,'short', delta1)
        if float(delta1) < float(0):
            self.exchange1_client.place_market_order(self.asset, 'long', delta1)
        if float(delta2) > float(0):
            self.exchange2_client.close_market_order(self.asset, 'long', delta1)
        if float(delta2) < float(0):
            self.exchange2_client.place_market_order(self.asset, 'short', delta1)
        return True

    async def fillDeal(self, frstOrdr, scndOrdr):
        while True:
            print('filldill')
            frstOrdrStts = await self.exchange1_client.get_order_status(symbol=self.asset, order_id=frstOrdr['orderId'])
            scndOrdrStts = await self.exchange2_client.get_order_status(symbol=self.asset, order_id=scndOrdr['orderId'])
            firstOrderbook = await self.exchange1_client.get_orderbook(symbol=self.asset, depth=10)
            secondOrderbook = await self.exchange2_client.get_orderbook(symbol=self.asset, depth=10)
            first_bid = float(firstOrderbook['bids'][0][0])
            first_ask = float(firstOrderbook['asks'][0][0])
            second_bid = float(secondOrderbook['bids'][0][0])
            second_ask = float(secondOrderbook['asks'][0][0])

            # Оба ордера исполнены
            if float(frstOrdrStts['FillSz']) == float(frstOrdr['qty']) and float(scndOrdrStts['FillSz']) == float(scndOrdr['qty']):
                print("# Оба ордера исполнены")
                return True
            if float(frstOrdrStts['FillSz']) == float(frstOrdr['qty']) and float(scndOrdrStts['FillSz']) != float(scndOrdr['qty']):
                print('# Первый исполнен, второй нет — отменяем второй, ставим лимит по биду')
                await self.exchange2_client.cancel_order(symbol=self.asset, order_id=scndOrdr['orderId'])
                scndOrdrStts = await self.exchange2_client.get_order_status(symbol=self.asset,
                                                                            order_id=scndOrdr['orderId'])
                if float(scndOrdrStts['FillSz']) != float(scndOrdr['qty']):
                    order = await self.exchange2_client.place_limit_order(
                            symbol=self.asset,
                            side="long",
                            price=second_ask,
                            qty=float(scndOrdr['qty']) - float(scndOrdrStts['FillSz']))

                    await self.fillOrdr(
                        self.exchange2_client,
                        order['orderId'],
                        "long",float(scndOrdr['qty']) - float(scndOrdrStts['FillSz'])
                    )
                    return True

            if float(frstOrdrStts['FillSz']) != float(frstOrdr['qty']) and float(scndOrdrStts['FillSz']) == float(scndOrdr['qty']):
                print('Второй исполнен, первый нет — отменяем первый, ставим лимит по аску')
                await self.exchange1_client.cancel_order(symbol=self.asset, order_id=frstOrdr['orderId'])
                frstOrdrStts = await self.exchange1_client.get_order_status(symbol=self.asset,
                                                                            order_id=frstOrdr['orderId'])
                if float(frstOrdrStts['FillSz']) != float(frstOrdr['qty']):
                    order = await self.exchange1_client.place_limit_order(
                            symbol=self.asset,
                            side="short",
                            price=first_bid,
                            qty=float(frstOrdr['qty']) - float(frstOrdrStts['FillSz'])
                        )
                    await self.fillOrdr(
                        self.exchange1_client,
                        order['orderId'],
                        "short",float(frstOrdr['qty']) - float(frstOrdrStts['FillSz'])
                    )
                    return True

            if frstOrdrStts['FillSz'] != frstOrdr['qty'] and scndOrdrStts['FillSz'] != scndOrdr['qty']:
                print('# Ни один не исполнен')
                await asyncio.sleep(0.001)

    async def fillOrdr(self, exchange_client, orderId, direction, qty):
        #ебашит по лимткам пока не исполнится
        print(qty)
        start_time = time.time()
        total_filled = 0
        tickSize = await exchange_client.get_tick_size(self.asset)

        while True:
            status = await exchange_client.get_order_status(symbol=self.asset, order_id=orderId)
            filled = float(status['FillSz'])
            print('filled - ' + str(filled))
            direcion = 'long' if self.exchange1_name=='Binance' else 'short'
            # position = await exchange_client.get_position_size(symbol=self.asset, direction=direcion)
            total_filled = filled
            print('total_filled' + str(total_filled))
            if total_filled >= qty:
                return True

            if time.time() - start_time > 4.7:
                status = await exchange_client.get_order_status(symbol=self.asset, order_id=orderId)
                filled = float(status['FillSz'])
                await exchange_client.cancel_order(symbol=self.asset, order_id=orderId)
                print('cencel order')
                status = await exchange_client.get_order_status(symbol=self.asset, order_id=orderId)
                filled = float(status['FillSz'])
                remaining = qty - total_filled
                print('remaining - ' + str(remaining))
                orderbook = await exchange_client.get_orderbook(symbol=self.asset, depth=1)
                price = float(orderbook['bids'][0][0] if direction == "short" else orderbook['asks'][0][0])
                #нужно проверять позицию и если позиция = тому кол-ву
                position = await exchange_client.get_position_size(self.asset, direcion)
                if position == qty:
                    return True
                else:
                    print('ласт шанс - '+ str(remaining))
                    new_order = await exchange_client.place_limit_order(
                        symbol=self.asset,
                        side=direction,
                        price=float(str(price))+float(str(tickSize)) if direcion=='long' else float(str(price))-float(str(tickSize)),
                        qty=remaining
                    )
                orderId = new_order['orderId']
                position = await exchange_client.get_position_size(self.asset, direcion)
                if position == qty:
                    print('зафилило ордер')
                    return True
                status = await exchange_client.get_order_status(symbol=self.asset, order_id=orderId)
                filled = float(status['FillSz'])
                await exchange_client.cancel_order(symbol=self.asset, order_id=orderId)
                size = remaining - filled
                market_order = exchange_client.place_market_order(symbol=self.asset, side=self.scndDrctn, qty=size)
                print(market_order)
                print('зафилило')
                return True

    async def close_dual_position(self, qty1=None, qty2=None):
        if qty1 == None and qty2 == None:
            qty1 = self.dealqty1*self.config['trading_parameters']['parts']
            qty2 = self.dealqty2 * self.config['trading_parameters']['parts']
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: input("Press Enter to start closing the deal..."))
        await self.before_start()
        firstOrderbook = await self.exchange1_client.get_orderbook(symbol=self.asset, depth=10)
        secondOrderbook = await self.exchange2_client.get_orderbook(symbol=self.asset, depth=10)
        first_bid, first_ask = float(firstOrderbook['bids'][0][0]), float(firstOrderbook['asks'][0][0])
        second_bid, second_ask = float(secondOrderbook['bids'][0][0]), float(secondOrderbook['asks'][0][0])
        current_spread = first_bid / second_ask
        spread_threshold = self.config['trading_parameters']['max_spread_percent']
        if current_spread < spread_threshold:
            print('spread to close')
            frstOrdr = await self.exchange1_client.close_limit_order(
                symbol=self.asset,
                side="long",  # закрытие short
                price=first_bid,
                qty=qty1
            )
            scndOrdr = await self.exchange2_client.close_limit_order(
                symbol=self.asset,
                side="short",  # закрытие long
                price=second_ask,
                qty=qty2
            )

            while True:
                frstOrdrStts = await self.exchange1_client.get_order_status(symbol=self.asset, order_id=frstOrdr['orderId'])
                scndOrdrStts = await self.exchange2_client.get_order_status(symbol=self.asset, order_id=scndOrdr['orderId'])

                if frstOrdrStts['FillSz'] == qty1 and scndOrdrStts['FillSz'] == qty2:
                    return True

                if frstOrdrStts['FillSz'] == qty1 and scndOrdrStts['FillSz'] != qty2:
                    await self.exchange2_client.cancel_order(symbol=self.asset, order_id=scndOrdr['orderId'])
                    await asyncio.sleep(0.1)
                    order = await self.exchange2_client.close_limit_order(
                        symbol=self.asset,
                        side="short",
                        price=second_ask,
                        qty=float(qty2) - float(scndOrdrStts['FillSz'])
                    )
                    await self.fillOrdr(self.exchange2_client, order['orderId'], "short",
                                        float(qty2) - float(scndOrdrStts['FillSz']))
                    return True

                if frstOrdrStts['FillSz'] != qty1 and scndOrdrStts['FillSz'] == qty2:
                    await self.exchange1_client.cancel_order(symbol=self.asset, order_id=frstOrdr['orderId'])
                    await asyncio.sleep(0.1)
                    order = await self.exchange1_client.close_limit_order(
                        symbol=self.asset,
                        side="long",
                        price=first_bid,
                        qty=float(qty1) - float(frstOrdrStts['FillSz'])
                    )
                    await self.fillOrdr(self.exchange1_client, order['orderId'], "long",
                                        float(qty1) - float(frstOrdrStts['FillSz']))
                    return True

                await asyncio.sleep(0.07)
