#!/usr/bin/env python3
"""
SpreadCollector.py - Устойчивый сборщик спредов
Версия с улучшенной обработкой ошибок и промежуточным сохранением
"""

import asyncio
import json
import csv
import time
import signal
import logging
from datetime import datetime
from typing import List, Dict, Optional
import redis.asyncio as aioredis

import sys
import os

# Добавляем корневую папку проекта в путь
current_file = os.path.abspath(__file__)
print(f"Текущий файл: {current_file}")

# Поднимаемся до корня jekius_maximus
root_path = os.path.dirname(os.path.dirname(os.path.dirname(current_file)))
print(f"Корневая папка: {root_path}")

if root_path not in sys.path:
    sys.path.insert(0, root_path)
    print(f"Добавлен путь: {root_path}")

# Проверяем, что папка CexWsClients существует
cex_path = os.path.join(root_path, "CexWsClients")
print(f"Папка CexWsClients: {cex_path} - {'существует' if os.path.exists(cex_path) else 'НЕ НАЙДЕНА'}")

# Отключаем избыточные логи
logging.getLogger().setLevel(logging.WARNING)

# Глобальные переменные
stop_flag = False
CONFIG_PATH = r"C:/Users/strel/PycharmProjects/rn_need_projects/jekius_maximus/logic/config.json"


def signal_handler(sig, frame):
    global stop_flag
    print("\n⏹️ Остановка...")
    stop_flag = True


signal.signal(signal.SIGINT, signal_handler)


class SpreadCollector:
    """Быстрый и надежный сборщик спредов"""

    def __init__(self, config_path: str = CONFIG_PATH):
        self.config_path = config_path
        self.redis = None
        self.binance_ws = None
        self.hyper_ws = None
        self.ws_task = None

    async def initialize(self):
        """Инициализация подключений"""
        try:
            # Redis
            self.redis = aioredis.Redis(
                host='localhost', port=6379, password='strongpassword',
                decode_responses=True, socket_connect_timeout=5
            )

            # Проверка подключения к Redis
            await self.redis.ping()
            print("✅ Redis подключен")

            # WebSocket клиенты
            with open(self.config_path) as f:
                cfg = json.load(f)

            from CexWsClients.AsyncBinanceWSClient import AsyncBinanceWSClient
            from CexWsClients.AsyncHyperliquidWSClient import AsyncHyperliquidWSClient

            self.binance_ws = AsyncBinanceWSClient(
                cfg["api_keys"]["binance"]["api_key"],
                cfg["api_keys"]["binance"]["api_secret"]
            )
            self.hyper_ws = AsyncHyperliquidWSClient.from_key(
                cfg["api_keys"]["hyperliquid"]["api_key"]
            )

            print("✅ WebSocket клиенты созданы")

        except Exception as e:
            print(f"❌ Ошибка инициализации: {e}")
            raise

    async def start_websockets(self, coins: List[str]):
        """Запуск WebSocket для списка монет"""
        print(f"🔌 Подключение к WebSocket для {len(coins)} монет...")

        try:
            # Подключение
            await asyncio.gather(
                self.binance_ws.connect_ws(),
                self.hyper_ws.connect_ws()
            )

            # Подписка на все монеты
            subscribe_tasks = []
            for coin in coins:
                subscribe_tasks.extend([
                    self.binance_ws.subscribe_orderbook(coin),
                    self.hyper_ws.subscribe_orderbook(coin)
                ])

            await asyncio.gather(*subscribe_tasks, return_exceptions=True)
            print(f"✅ WebSocket готов для {coins}")

        except Exception as e:
            print(f"❌ Ошибка подключения WebSocket: {e}")
            raise

    async def collect_spread_data(self, coin: str, duration: int = 300, interval: float = 0.33) -> List[Dict]:
        """Сбор данных спредов для одной монеты - УСТОЙЧИВАЯ ВЕРСИЯ"""
        data = []
        start_time = time.time()
        last_progress = start_time
        last_save = start_time
        error_count = 0
        successful_reads = 0

        print(f"📊 {coin}: сбор спредов на {duration}с...")

        while not stop_flag and (time.time() - start_time) < duration:
            try:
                # Быстрое чтение всех сторон ордербука
                keys = [
                    f"orderbook:binance:{coin}USDT:bids",
                    f"orderbook:binance:{coin}USDT:asks",
                    f"orderbook:hyperliquid:{coin}:bids",
                    f"orderbook:hyperliquid:{coin}:asks"
                ]

                values = await asyncio.gather(
                    *[asyncio.wait_for(self.redis.get(k), timeout=0.5) for k in keys],
                    return_exceptions=True
                )

                if all(isinstance(v, str) and v for v in values):
                    try:
                        # Парсинг цен
                        bids_b, asks_b, bids_h, asks_h = [json.loads(v) for v in values]

                        if all([bids_b, asks_b, bids_h, asks_h]) and all(
                                len(x) > 0 for x in [bids_b, asks_b, bids_h, asks_h]):
                            # Цены
                            b_bid, b_ask = float(bids_b[0][0]), float(asks_b[0][0])
                            h_bid, h_ask = float(bids_h[0][0]), float(asks_h[0][0])

                            # Проверяем что цены разумные
                            if all(price > 0 for price in [b_bid, b_ask, h_bid, h_ask]):
                                # Спреды в обоих направлениях
                                spread_b_short = ((b_bid / h_ask) - 1) * 100  # Binance продать, Hyper купить
                                spread_h_short = ((h_bid / b_ask) - 1) * 100  # Hyper продать, Binance купить

                                best_spread = max(spread_b_short, spread_h_short)
                                direction = "binance_short" if spread_b_short > spread_h_short else "hyper_short"

                                data.append({
                                    'time': datetime.now().isoformat(),
                                    'elapsed': round(time.time() - start_time, 1),
                                    'coin': coin,
                                    'b_bid': b_bid, 'b_ask': b_ask,
                                    'h_bid': h_bid, 'h_ask': h_ask,
                                    'spread_b_short': round(spread_b_short, 4),
                                    'spread_h_short': round(spread_h_short, 4),
                                    'best_spread': round(best_spread, 4),
                                    'direction': direction
                                })

                                successful_reads += 1
                                error_count = max(0, error_count - 1)  # Уменьшаем счетчик ошибок при успехе

                    except (json.JSONDecodeError, IndexError, KeyError, ValueError, TypeError, ZeroDivisionError) as e:
                        error_count += 1
                        if error_count % 50 == 0:
                            print(f"⚠️ {coin}: ошибок парсинга: {error_count} (последняя: {type(e).__name__})")

                else:
                    # Пустые или неполные данные
                    error_count += 1

                # Прогресс каждые 30 сек
                if time.time() - last_progress > 30:
                    success_rate = (successful_reads / (successful_reads + error_count)) * 100 if (
                                                                                                              successful_reads + error_count) > 0 else 0
                    print(f"✅ {coin}: {len(data)} записей | Успех: {success_rate:.1f}% | Ошибок: {error_count}")
                    last_progress = time.time()

                # Промежуточное сохранение каждые 60 секунд
                if time.time() - last_save > 60 and len(data) > 0:
                    await self._save_temp_data(coin, data)
                    last_save = time.time()

            except Exception as e:
                error_count += 1
                if error_count % 100 == 0:
                    print(f"⚠️ {coin}: общих ошибок: {error_count} - {type(e).__name__}: {str(e)[:100]}")

            await asyncio.sleep(interval)

        print(f"💾 {coin}: собрано {len(data)} записей (успешных: {successful_reads}, ошибок: {error_count})")
        return data

    async def _save_temp_data(self, coin: str, data: List[Dict]):
        """Промежуточное сохранение данных"""
        try:
            temp_filename = f"temp_spread_{coin}_{datetime.now():%H%M%S}.csv"
            with open(temp_filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
            print(f"💾 {coin}: промежуточное сохранение в {temp_filename}")
        except Exception as e:
            print(f"❌ {coin}: ошибка промежуточного сохранения: {e}")

    def save_data(self, coin: str, data: List[Dict]) -> str:
        """Сохранение данных в CSV"""
        if not data:
            return ""

        try:
            filename = f"spread_{coin}_{datetime.now():%Y%m%d_%H%M%S}.csv"

            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)

            print(f"✅ {coin}: данные сохранены в {filename}")
            return filename

        except Exception as e:
            print(f"❌ {coin}: ошибка сохранения: {e}")
            return ""

    async def collect_single_coin(self, coin: str, duration: int = 300, interval: float = 0.33):
        """Сбор для одной монеты"""
        try:
            await self.initialize()
            await self.start_websockets([coin])
            await asyncio.sleep(5)  # Ждем данных

            data = await self.collect_spread_data(coin, duration, interval)
            filename = self.save_data(coin, data)

            if data:
                spreads = [d['best_spread'] for d in data]
                print(f"\n📊 {coin} СТАТИСТИКА:")
                print(f"├─ Записей: {len(data)}")
                print(f"├─ Средний спред: {sum(spreads) / len(spreads):.4f}%")
                print(f"├─ Лучший спред: {max(spreads):.4f}%")
                print(f"├─ Худший спред: {min(spreads):.4f}%")
                print(f"└─ Файл: {filename}")
            else:
                print(f"❌ {coin}: нет данных для сохранения")

        except Exception as e:
            print(f"❌ Критическая ошибка для {coin}: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await self.cleanup()

    async def collect_multi_coins(self, coins: List[str], duration: int = 300, interval: float = 0.33):
        """Сбор для нескольких монет параллельно"""
        try:
            await self.initialize()
            await self.start_websockets(coins)
            await asyncio.sleep(10)  # Больше времени для множественных подписок

            # Параллельный сбор
            print(f"🚀 Запуск параллельного сбора для {len(coins)} монет...")
            tasks = [self.collect_spread_data(coin, duration, interval) for coin in coins]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Сохранение
            files = []
            successful_coins = []

            for coin, data in zip(coins, results):
                if isinstance(data, Exception):
                    print(f"❌ {coin}: ошибка сбора - {type(data).__name__}: {data}")
                    continue

                filename = self.save_data(coin, data)
                if filename:
                    files.append(filename)
                    successful_coins.append((coin, data))

            # Статистика
            print(f"\n{'=' * 70}")
            print(f"📊 СВОДНАЯ СТАТИСТИКА ({len(successful_coins)}/{len(coins)} монет успешно):")
            print(f"{'=' * 70}")
            print(f"{'Монета':<8} {'Записей':<8} {'Средний':<10} {'Лучший':<10} {'Худший':<10} {'Файл'}")
            print("-" * 70)

            for coin, data in successful_coins:
                if data:
                    spreads = [d['best_spread'] for d in data]
                    avg_spread = sum(spreads) / len(spreads)
                    max_spread = max(spreads)
                    min_spread = min(spreads)
                    filename = f"spread_{coin}_*.csv"
                    print(
                        f"{coin:<8} {len(data):<8} {avg_spread:<10.4f} {max_spread:<10.4f} {min_spread:<10.4f} {filename}")

            print(f"{'=' * 70}")
            print(f"✅ Сохранено файлов: {len(files)}")

            # Рекомендации
            if successful_coins:
                print(f"\n🎯 РЕКОМЕНДАЦИИ:")
                best_coin = max(successful_coins, key=lambda x: max([d['best_spread'] for d in x[1]]))
                most_data = max(successful_coins, key=lambda x: len(x[1]))
                print(f"├─ Лучший спред: {best_coin[0]} ({max([d['best_spread'] for d in best_coin[1]]):.4f}%)")
                print(f"└─ Больше всего данных: {most_data[0]} ({len(most_data[1])} записей)")

        except Exception as e:
            print(f"❌ Критическая ошибка мульти-сбора: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await self.cleanup()

    async def cleanup(self):
        """Закрытие подключений"""
        try:
            print("🧹 Закрытие подключений...")

            if self.redis:
                await self.redis.close()
                print("✅ Redis закрыт")

            if self.binance_ws:
                await self.binance_ws.close()
                print("✅ Binance WS закрыт")

            if self.hyper_ws:
                await self.hyper_ws.close()
                print("✅ Hyperliquid WS закрыт")

        except Exception as e:
            print(f"⚠️ Ошибка при закрытии: {e}")


# Основные функции
async def collect_one(coin: str, duration: int = 300):
    """Сбор для одной монеты"""
    collector = SpreadCollector()
    await collector.collect_single_coin(coin.upper(), duration)


async def collect_many(coins: List[str], duration: int = 300):
    """Сбор для нескольких монет"""
    collector = SpreadCollector()
    coins_upper = [c.upper() for c in coins]
    await collector.collect_multi_coins(coins_upper, duration)


# Предустановленные списки монет
POPULAR_COINS = ["ETH", "BTC", "SOL", "ARB", "DOGE"]
ALTCOINS = ["UMA", "HYPE", "SOPH", "SPX", "WCT"]
TRENDING_COINS = ["ZRO", "AIXBT", "BERA", "TRUMP"]

if __name__ == "__main__":
    import sys

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    print("🚀 УНИВЕРСАЛЬНЫЙ СБОРЩИК СПРЕДОВ v2.0")
    print("=" * 60)
    print("Устойчивая версия с обработкой ошибок и промежуточным сохранением")
    print("=" * 60)

    # Примеры использования
    if len(sys.argv) > 1:
        if sys.argv[1] == "one":
            coin = sys.argv[2] if len(sys.argv) > 2 else "ETH"
            duration = int(sys.argv[3]) if len(sys.argv) > 3 else 300
            print(f"📊 Сбор для {coin} ({duration}с)")
            asyncio.run(collect_one(coin, duration))

        elif sys.argv[1] == "popular":
            duration = int(sys.argv[2]) if len(sys.argv) > 2 else 300
            print(f"📊 Сбор для популярных монет ({duration}с)")
            asyncio.run(collect_many(POPULAR_COINS, duration))

        elif sys.argv[1] == "altcoins":
            duration = int(sys.argv[2]) if len(sys.argv) > 2 else 300
            print(f"📊 Сбор для альткоинов ({duration}с)")
            asyncio.run(collect_many(ALTCOINS, duration))

        elif sys.argv[1] == "trending":
            duration = int(sys.argv[2]) if len(sys.argv) > 2 else 300
            print(f"📊 Сбор для трендовых монет ({duration}с)")
            asyncio.run(collect_many(TRENDING_COINS, duration))

        elif sys.argv[1] == "custom":
            coins = sys.argv[2].split(',')
            duration = int(sys.argv[3]) if len(sys.argv) > 3 else 300
            print(f"📊 Сбор для {coins} ({duration}с)")
            asyncio.run(collect_many(coins, duration))

        elif sys.argv[1] == "test":
            # Быстрый тест на 1 минуту
            coin = sys.argv[2] if len(sys.argv) > 2 else "ETH"
            print(f"🧪 Быстрый тест для {coin} (60с)")
            asyncio.run(collect_one(coin, 60))

    else:
        # Интерактивный режим
        print("Использование:")
        print("  python SpreadCollector.py one ETH 300           # Одна монета")
        print("  python SpreadCollector.py popular 300           # Популярные монеты")
        print("  python SpreadCollector.py altcoins 300          # Альткоины")
        print("  python SpreadCollector.py trending 300          # Трендовые (ZRO,AIXBT,BERA,TRUMP)")
        print("  python SpreadCollector.py custom ETH,BTC,SOL 300 # Свой список")
        print("  python SpreadCollector.py test ETH              # Быстрый тест (60с)")
        print("\nЗапуск по умолчанию: ETH на 5 минут")
        asyncio.run(collect_one("ETH", 300))