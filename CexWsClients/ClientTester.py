import asyncio
import sys
import time


async def check_client(client):
    """Проверяет клиент вызывая все функции по порядку"""

    print(f"🚀 Проверка {client.__class__.__name__}")

    functions = [
        ("connect_ws", []),
        ("place_limit_order", ["BTC", "long", 40000.0, 1.0]),
        ("place_market_order", ["BTC", "long", 1.0]),
        ("cancel_order", ["BTC", "123"]),
        ("get_position_info", ["BTC"]),
        ("get_position_size", ["BTC", "long"]),
        ("set_leverage", ["BTC", 10]),
        ("subscribe_order", ["123"]),
        ("unsubscribe_order", ["123"]),
        ("subscribe_orderbook", ["BTC"]),
        ("get_order_status", ["BTC", "123"]),
        ("get_tick_size", ["BTC"]),
        ("get_funding_rate", ["BTC"]),
        ("get_symbol_info", ["BTC"]),
        ("close", [])
    ]

    passed = 0
    total = 0

    for func_name, args in functions:
        if hasattr(client, func_name):
            total += 1
            start = time.perf_counter()
            try:
                func = getattr(client, func_name)
                result = await func(*args)
                duration = time.perf_counter() - start
                print(f"✅ {func_name}: {duration:.3f}s")
                passed += 1
            except Exception as e:
                duration = time.perf_counter() - start
                print(f"❌ {func_name}: {str(e)[:50]}... ({duration:.3f}s)")
        else:
            print(f"⚠️ {func_name}: не найдена")

    print(f"📊 Результат: {passed}/{total} ({passed / total * 100:.1f}%)")
    return passed, total


async def run_checks():
    # Пример использования с любым клиентом
    try:
        from AsyncExtendedWSClient import AsyncExtendedWSClient
        client = AsyncExtendedWSClient("d58a9923e7323c2f4dba928cc1c9cf01", "0x8c9728b0b2a0df538e0187e278891465f6b4a3a702d51a5f800f68546168cb")
        await check_client(client)
    except Exception as e:
        print(f"Ошибка импорта: {e}")

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

if __name__ == "__main__":
    asyncio.run(run_checks())