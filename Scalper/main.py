# test_scalper.py
import asyncio
import json
import logging
import sys

from Scalper import Scalper

logger = logging.getLogger(__name__)
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def wait_for_orderbooks(binance_info, hyperliquid_info, symbol):
    for attempt in range(5):
        try:
            ob1, ob2 = await asyncio.gather(
                binance_info.get_orderbook(symbol),
                hyperliquid_info.get_orderbook(symbol)
            )

            if (ob1.get('bids', []) and ob1.get('asks', []) and
                ob2.get('bids', []) and ob2.get('asks', [])):
                print("✅ Ордербуки готовы")
                return True

        except Exception as e:
            pass  # Игнорируем ошибки, просто ждем

        await asyncio.sleep(1)

    raise Exception("Ордербуки не заполнились за 5 секунд")


async def main():
    scalper = Scalper("MOVE")

    try:
        await scalper.checkBeforeStart()
        await scalper.closingDeal()  # Один вызов с while True внутри

    except KeyboardInterrupt:
        logger.info("🛑 Остановка скальпера...")
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    asyncio.run(main())