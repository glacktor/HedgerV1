import asyncio
import sys
import time
import csv
import json
import logging
from datetime import datetime
from CexWsClients.AsyncBinanceWSClient import AsyncBinanceWSClient
from CexWsClients.AsyncHyperliquidWSClient import AsyncHyperliquidWSClient
from InfoClients.AsyncBinanceInfoClient import AsyncBinanceInfoClient
from InfoClients.AsyncHyperliquidInfoClient import AsyncHyperliquidInfoClient
from DragonflyDb.DragonFlyConnector import DragonFlyConnector

logger = logging.getLogger(__name__)
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class OrderbookRecorder:
    def __init__(self, asset: str = "MOVE",
                 config_path: str = r"C:/Users/strel/PycharmProjects/rn_need_projects/jekius_maximus/Scalper/config/config.json"):
        self.asset = asset
        self.csv_file = None
        self.csv_writer = None
        self.record_count = 0

        with open(config_path, 'r') as f:
            self.config = json.load(f)

    async def init_clients(self):
        """Инициализация клиентов"""
        self.binance_db = DragonFlyConnector(exchange="binance")
        self.hyperliquid_db = DragonFlyConnector(exchange="hyperliquid")

        self.binance_ws = AsyncBinanceWSClient(
            self.config["api_keys"]["binance"]["api_key"],
            self.config["api_keys"]["binance"]["api_secret"]
        )

        self.hyperliquid_ws = AsyncHyperliquidWSClient.from_key(
            self.config["api_keys"]["hyperliquid"]["api_key"]
        )

        self.binance_info = AsyncBinanceInfoClient(self.binance_db)
        self.hyperliquid_info = AsyncHyperliquidInfoClient(self.hyperliquid_db)

        await asyncio.gather(
            self.binance_ws.connect_ws(),
            self.hyperliquid_ws.connect_ws()
        )

        await asyncio.gather(
            self.binance_ws.subscribe_orderbook(self.asset),
            self.hyperliquid_ws.subscribe_orderbook(self.asset)
        )

        print("✅ Клиенты инициализированы")
        await asyncio.sleep(2)

    def _init_csv(self):
        """Создает CSV файл и записывает заголовки"""
        filename = f"orderbook_{self.asset}_{datetime.now().strftime('%H%M%S')}.csv"

        self.csv_file = open(filename, 'w', newline='', encoding='utf-8')
        self.csv_writer = csv.writer(self.csv_file)

        headers = ['timestamp',
                   'binance_bid1_price', 'binance_bid1_qty', 'binance_bid2_price', 'binance_bid2_qty',
                   'binance_ask1_price', 'binance_ask1_qty', 'binance_ask2_price', 'binance_ask2_qty',
                   'hyperliquid_bid1_price', 'hyperliquid_bid1_qty', 'hyperliquid_bid2_price', 'hyperliquid_bid2_qty',
                   'hyperliquid_ask1_price', 'hyperliquid_ask1_qty', 'hyperliquid_ask2_price', 'hyperliquid_ask2_qty']

        self.csv_writer.writerow(headers)
        self.csv_file.flush()

        print(f"💾 CSV файл создан: {filename}")
        return filename

    def _extract_prices(self, orderbook, prefix):
        """Извлекает цены из ордербука с fallback на нули"""
        try:
            if not isinstance(orderbook, dict) or not orderbook.get('bids') or not orderbook.get('asks'):
                return ['0', '0', '0', '0', '0', '0', '0', '0']

            bids = orderbook['bids']
            asks = orderbook['asks']

            return [
                bids[0][0] if len(bids) > 0 else '0', bids[0][1] if len(bids) > 0 else '0',
                bids[1][0] if len(bids) > 1 else '0', bids[1][1] if len(bids) > 1 else '0',
                asks[0][0] if len(asks) > 0 else '0', asks[0][1] if len(asks) > 0 else '0',
                asks[1][0] if len(asks) > 1 else '0', asks[1][1] if len(asks) > 1 else '0'
            ]
        except:
            return ['0', '0', '0', '0', '0', '0', '0', '0']

    async def record_orderbook(self, duration: int = 150):
        """Записывает ордербук напрямую в CSV"""
        await self.init_clients()
        filename = self._init_csv()

        start_time = time.time()
        end_time = start_time + duration

        print(f"🎬 Запись ордербука {self.asset} на {duration}с...")

        try:
            while time.time() < end_time:
                timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]

                # Получаем ордербуки асинхронно
                results = await asyncio.gather(
                    self.binance_info.get_orderbook(self.asset),
                    self.hyperliquid_info.get_orderbook(self.asset),
                    return_exceptions=True
                )

                # Формируем строку для записи
                row = [timestamp]
                row.extend(self._extract_prices(results[0], 'binance'))
                row.extend(self._extract_prices(results[1], 'hyperliquid'))

                # Записываем сразу в файл
                self.csv_writer.writerow(row)
                self.record_count += 1

                # Принудительно сбрасываем в файл каждые 10 записей
                if self.record_count % 10 == 0:
                    self.csv_file.flush()

                # Прогресс каждые 25 записей
                if self.record_count % 25 == 0:
                    remaining = int(end_time - time.time())
                    print(f"📊 Записано: {self.record_count} | Осталось: {remaining}с")

                await asyncio.sleep(0.2)

        except KeyboardInterrupt:
            print(f"\n⏹️ Запись остановлена пользователем")
        except Exception as e:
            print(f"❌ Ошибка записи: {e}")
        finally:
            if self.csv_file:
                self.csv_file.close()
            print(f"✅ Запись завершена: {self.record_count} записей в {filename}")

    async def close_clients(self):
        """Закрывает соединения"""
        try:
            await asyncio.gather(
                self.binance_ws.close(),
                self.hyperliquid_ws.close(),
                return_exceptions=True
            )
        except Exception as e:
            print(f"Ошибка закрытия: {e}")


async def main():
    recorder = OrderbookRecorder("MOVE")
    try:
        await recorder.record_orderbook(150)
    finally:
        await recorder.close_clients()


if __name__ == "__main__":
    asyncio.run(main())