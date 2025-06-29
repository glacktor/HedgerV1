import asyncio
import json
import csv
import time
from datetime import datetime
import redis.asyncio as aioredis

# Конфиг
COINS = ["ETH", "BTC", "SOL", "LINK", "AVAX"]  # 5 монет
DURATION = 300  # 5 минут
INTERVAL = 0.33  # ~3 раза в секунду


async def collect_spreads():
    # Redis
    redis = aioredis.Redis(
        host='localhost',
        port=6379,
        password='strongpassword',
        decode_responses=True
    )

    # WebSockets
    from CexWsClients.AsyncBinanceWSClient import AsyncBinanceWSClient
    from CexWsClients.AsyncHyperliquidWSClient import AsyncHyperliquidWSClient

    with open(r'C:/Users/strel/PycharmProjects/rn_need_projects/jekius_maximus/logic/config.json') as f:
        cfg = json.load(f)

    binance = AsyncBinanceWSClient(
        cfg["api_keys"]["binance"]["api_key"],
        cfg["api_keys"]["binance"]["api_secret"]
    )
    hyper = AsyncHyperliquidWSClient.from_key(
        cfg["api_keys"]["hyperliquid"]["api_key"]
    )

    # Подключение
    await asyncio.gather(binance.connect_ws(), hyper.connect_ws())

    # Подписка
    for coin in COINS:
        await binance.subscribe_orderbook(coin)
        await hyper.subscribe_orderbook(coin)

    print(f"✅ Подключено. Сбор {DURATION} сек...")
    await asyncio.sleep(5)  # Ждем данные

    # Сбор данных
    data = []
    start = time.time()
    errors = 0

    print("🔄 Начинаем сбор данных...")

    while time.time() - start < DURATION:
        for coin in COINS:
            try:
                # Читаем ордербуки
                b_bids, b_asks, h_bids, h_asks = await asyncio.gather(
                    redis.get(f"orderbook:binance:{coin}USDT:bids"),
                    redis.get(f"orderbook:binance:{coin}USDT:asks"),
                    redis.get(f"orderbook:hyperliquid:{coin}:bids"),
                    redis.get(f"orderbook:hyperliquid:{coin}:asks")
                )

                # Отладка для первой итерации
                if len(data) == 0 and coin == COINS[0]:
                    print(f"🔍 Проверка Redis для {coin}:")
                    print(f"   Binance bids: {'✅' if b_bids else '❌'}")
                    print(f"   Binance asks: {'✅' if b_asks else '❌'}")
                    print(f"   Hyper bids: {'✅' if h_bids else '❌'}")
                    print(f"   Hyper asks: {'✅' if h_asks else '❌'}")

                if all([b_bids, b_asks, h_bids, h_asks]):
                    # Парсим
                    bb = json.loads(b_bids)[0]  # [price, volume]
                    ba = json.loads(b_asks)[0]
                    hb = json.loads(h_bids)[0]
                    ha = json.loads(h_asks)[0]

                    # Считаем спреды
                    spread1 = ((float(bb[0]) / float(ha[0])) - 1) * 100
                    spread2 = ((float(hb[0]) / float(ba[0])) - 1) * 100

                    data.append({
                        'time': datetime.now().isoformat(),
                        'coin': coin,
                        'binance_bid': float(bb[0]),
                        'binance_bid_vol': float(bb[1]),
                        'binance_ask': float(ba[0]),
                        'binance_ask_vol': float(ba[1]),
                        'hyper_bid': float(hb[0]),
                        'hyper_bid_vol': float(hb[1]),
                        'hyper_ask': float(ha[0]),
                        'hyper_ask_vol': float(ha[1]),
                        'spread_b2h': round(spread1, 4),  # Binance->Hyper
                        'spread_h2b': round(spread2, 4),  # Hyper->Binance
                        'best_spread': round(max(spread1, spread2), 4)
                    })

            except Exception as e:
                errors += 1
                if errors % 50 == 0:  # Каждые 50 ошибок
                    print(f"⚠️ Ошибки сбора: {errors} | Последняя: {str(e)[:50]}")

        # Прогресс каждые 30 сек
        elapsed = time.time() - start
        if len(data) > 0 and (len(data) == 1 or elapsed - getattr(collect_spreads, 'last_progress', 0) >= 30):
            collect_spreads.last_progress = elapsed
            last = [d for d in data if d['coin'] == COINS[0]][-1]
            print(f"📊 {len(data)} записей | {COINS[0]} спред: {last['best_spread']}% | {int(elapsed)}с из {DURATION}с")

        await asyncio.sleep(INTERVAL)

    # Сохранение
    filename = f"spreads_{datetime.now():%Y%m%d_%H%M%S}.csv"

    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

    # Статистика
    print(f"\n✅ Сохранено: {filename}")
    print(f"📝 Записей: {len(data)}")

    for coin in COINS:
        coin_data = [d for d in data if d['coin'] == coin]
        if coin_data:
            spreads = [d['best_spread'] for d in coin_data]
            print(f"{coin}: средний={sum(spreads) / len(spreads):.4f}% макс={max(spreads):.4f}%")

    # Закрытие
    await redis.close()
    await binance.close()
    await hyper.close()


if __name__ == "__main__":
    import sys

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(collect_spreads())