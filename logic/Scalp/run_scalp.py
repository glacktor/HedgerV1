import asyncio
from logic.LongShort.LongShort import LongShort


async def main():
    strategy = LongShort(
        exchange1_name="Binance",
        exchange2_name="Hyperliquid",
        reverse=False,
        asset="ETH",
        config_path=r"C:/Users/strel/PycharmProjects/rn_need_projects/jekius_maximus/logic/config.json"
    )

    await strategy.before_start()
    await asyncio.sleep(2)

    # Получаем ордербуки
    ob1 = await strategy.exchange1Info.get_orderbook(strategy.asset)
    ob2 = await strategy.exchange2Info.get_orderbook(strategy.asset)

    # ОЧЕНЬ агрессивные цены для гарантированного исполнения
    price1 = float(ob1['asks'][0][0])  # Binance: продаем по ASK (мгновенно)
    price2 = float(ob2['bids'][0][0])  # Hyperliquid: покупаем по BID (мгновенно)

    # Получаем precision
    info1 = await strategy.exchange1WebSocket.get_symbol_info(strategy.asset)
    info2 = await strategy.exchange2WebSocket.get_symbol_info(strategy.asset)

    price1 = round(price1, info1['pricePrecision'])
    price2 = round(price2, info2['pricePrecision'])

    # Маленький размер
    test_qty = 0.004  # ~$10

    print(f"АГРЕССИВНЫЙ ТЕСТ:")
    print(f"Binance SELL по ASK: {price1}")
    print(f"Hyperliquid BUY по BID: {price2}")
    print(f"Размер: {test_qty} ETH (~${test_qty * price1:.2f})")
    print(f"Спред: {((price1 / price2 - 1) * 100):.3f}%")

    result = await strategy.dual_open(
        strategy.exchange1WebSocket, price1,
        strategy.exchange2WebSocket, price2,
        test_qty
    )

    print(f"\nРезультат: {result['success']}")
    print(f"Оба исполнены: {result['both_filled']}")
    print(f"Действие: {result['action_taken']}")

    if result.get('exchange1_result'):
        print(f"Binance filled: {result['exchange1_result'].get('filledQty', 0)}")
    if result.get('exchange2_result'):
        print(f"Hyperliquid filled: {result['exchange2_result'].get('filledQty', 0)}")

    await strategy.exchange1WebSocket.close()
    await strategy.exchange2WebSocket.close()


asyncio.run(main())