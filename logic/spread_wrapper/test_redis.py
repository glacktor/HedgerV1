import asyncio
import redis.asyncio as aioredis
import json


async def test_redis():
    redis = aioredis.Redis(
        host='localhost',
        port=6379,
        password='strongpassword',
        decode_responses=True
    )

    coins = ["ETH", "BTC", "SOL", "LINK", "AVAX"]

    print("🔍 Проверка данных в Redis:\n")

    for coin in coins:
        print(f"📊 {coin}:")

        # Проверяем все 4 ключа
        keys = [
            f"orderbook:binance:{coin}USDT:bids",
            f"orderbook:binance:{coin}USDT:asks",
            f"orderbook:hyperliquid:{coin}:bids",
            f"orderbook:hyperliquid:{coin}:asks"
        ]

        for key in keys:
            try:
                data = await redis.get(key)
                if data:
                    parsed = json.loads(data)
                    if parsed and len(parsed) > 0:
                        price = float(parsed[0][0])
                        volume = float(parsed[0][1])
                        print(f"   ✅ {key}: ${price:.2f} vol={volume:.2f}")
                    else:
                        print(f"   ⚠️ {key}: пустой массив")
                else:
                    print(f"   ❌ {key}: нет данных")
            except Exception as e:
                print(f"   ❌ {key}: ошибка {str(e)[:30]}")

        print()

    await redis.close()


if __name__ == "__main__":
    asyncio.run(test_redis())