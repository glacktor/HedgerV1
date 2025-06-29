#!/usr/bin/env python3
"""
Анализатор спредов - исследование и статистика без торговой логики
Помогает понять на каких монетах и как работать
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import glob
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import warnings

warnings.filterwarnings('ignore')


class SpreadAnalyzer:
    """Анализатор спредов для исследования"""

    def __init__(self):
        self.data = {}
        self.stats = {}

    def load_data(self, coin: str = None, pattern: str = "spread_*.csv") -> Dict[str, pd.DataFrame]:
        """Загрузка данных спредов"""
        if coin:
            pattern = f"spread_{coin}_*.csv"

        files = glob.glob(pattern)
        print(f"📁 Найдено {len(files)} файлов")

        for file in sorted(files):
            try:
                df = pd.read_csv(file)
                if 'coin' in df.columns:
                    coin_name = df['coin'].iloc[0]
                else:
                    # Извлекаем из имени файла
                    coin_name = file.split('_')[1].split('.')[0]

                df['timestamp'] = pd.to_datetime(df['time'], errors='coerce')
                self.data[coin_name] = df
                print(f"✅ {coin_name}: {len(df)} записей")

            except Exception as e:
                print(f"❌ Ошибка загрузки {file}: {e}")

        return self.data

    def calculate_statistics(self) -> Dict:
        """Расчет детальной статистики"""
        print(f"\n📊 АНАЛИЗ {len(self.data)} МОНЕТ")
        print("=" * 80)

        results = {}

        for coin, df in self.data.items():
            if 'best_spread' in df.columns:
                spreads = df['best_spread'].values
            elif 'spread' in df.columns:
                spreads = df['spread'].values
            else:
                continue

            # Основная статистика
            stats = {
                'records': len(df),
                'duration_min': df['elapsed'].max() if 'elapsed' in df.columns else len(df) * 0.33 / 60,
                'mean': np.mean(spreads),
                'median': np.median(spreads),
                'std': np.std(spreads),
                'min': np.min(spreads),
                'max': np.max(spreads),
                'q10': np.percentile(spreads, 10),
                'q25': np.percentile(spreads, 25),
                'q75': np.percentile(spreads, 75),
                'q90': np.percentile(spreads, 90),
            }

            # Дополнительная аналитика
            stats['cv'] = stats['std'] / abs(stats['mean']) if stats['mean'] != 0 else 0  # Коэффициент вариации
            stats['profitable_10_pct'] = len(spreads[spreads <= stats['q10']]) / len(spreads) * 100
            stats['profitable_25_pct'] = len(spreads[spreads <= stats['q25']]) / len(spreads) * 100
            stats['negative_spread_pct'] = len(spreads[spreads < 0]) / len(spreads) * 100

            # Анализ направлений (если есть)
            if 'direction' in df.columns:
                direction_counts = df['direction'].value_counts()
                if len(direction_counts) >= 2:
                    stats['binance_short_pct'] = (direction_counts.get('binance_short', 0) / len(df)) * 100
                    stats['hyper_short_pct'] = (direction_counts.get('hyper_short', 0) / len(df)) * 100
                    stats['preferred_direction'] = direction_counts.index[0]

            # Временная стабильность
            if len(spreads) > 100:
                # Разбиваем на части и смотрим стабильность
                chunk_size = len(spreads) // 5
                chunk_means = []
                for i in range(5):
                    start_idx = i * chunk_size
                    end_idx = (i + 1) * chunk_size if i < 4 else len(spreads)
                    chunk_means.append(np.mean(spreads[start_idx:end_idx]))
                stats['temporal_stability'] = np.std(chunk_means)

            results[coin] = stats

        self.stats = results
        return results

    def print_summary_table(self):
        """Вывод сводной таблицы"""
        if not self.stats:
            self.calculate_statistics()

        print(
            f"\n{'МОНЕТА':<8} {'Записей':<8} {'Средний':<10} {'Мин':<8} {'Макс':<8} {'Q10':<8} {'Q25':<8} {'CV':<6} {'Негат%':<7}")
        print("-" * 85)

        # Сортируем по привлекательности (низкий средний спред)
        sorted_coins = sorted(self.stats.items(), key=lambda x: x[1]['mean'])

        for coin, stats in sorted_coins:
            print(f"{coin:<8} {stats['records']:<8} {stats['mean']:<10.4f} "
                  f"{stats['min']:<8.4f} {stats['max']:<8.4f} {stats['q10']:<8.4f} "
                  f"{stats['q25']:<8.4f} {stats['cv']:<6.2f} {stats['negative_spread_pct']:<7.1f}")

    def find_best_opportunities(self) -> Dict:
        """Поиск лучших возможностей"""
        if not self.stats:
            self.calculate_statistics()

        opportunities = {}

        for coin, stats in self.stats.items():
            # Скоринг возможностей
            score = 0
            reasons = []

            # Низкий средний спред = хорошо
            if stats['mean'] < -0.1:
                score += 30
                reasons.append(f"Низкий средний спред ({stats['mean']:.4f}%)")

            # Много выгодных моментов
            if stats['profitable_25_pct'] > 40:
                score += 20
                reasons.append(f"Часто выгодно ({stats['profitable_25_pct']:.1f}% времени)")

            # Низкая волатильность = стабильность
            if stats['cv'] < 0.5:
                score += 15
                reasons.append(f"Стабильный (CV={stats['cv']:.2f})")

            # Хороший минимум
            if stats['min'] < -0.5:
                score += 10
                reasons.append(f"Отличный минимум ({stats['min']:.4f}%)")

            # Много данных = надежность
            if stats['records'] > 500:
                score += 5
                reasons.append(f"Много данных ({stats['records']})")

            opportunities[coin] = {
                'score': score,
                'reasons': reasons,
                'stats': stats
            }

        return opportunities

    def print_opportunities_ranking(self):
        """Рейтинг возможностей"""
        opportunities = self.find_best_opportunities()

        print(f"\n🏆 РЕЙТИНГ ТОРГОВЫХ ВОЗМОЖНОСТЕЙ")
        print("=" * 60)

        sorted_opps = sorted(opportunities.items(), key=lambda x: x[1]['score'], reverse=True)

        for i, (coin, opp) in enumerate(sorted_opps, 1):
            print(f"\n{i}. {coin} (Очки: {opp['score']})")
            print(f"   Средний спред: {opp['stats']['mean']:.4f}%")
            print(f"   Диапазон: [{opp['stats']['min']:.4f}%, {opp['stats']['max']:.4f}%]")
            print(f"   Причины:")
            for reason in opp['reasons']:
                print(f"     • {reason}")

    def analyze_entry_exit_levels(self, coin: str) -> Dict:
        """Анализ уровней входа и выхода"""
        if coin not in self.data:
            print(f"❌ Нет данных для {coin}")
            return {}

        df = self.data[coin]

        if 'best_spread' in df.columns:
            spreads = df['best_spread'].values
        elif 'spread' in df.columns:
            spreads = df['spread'].values
        else:
            return {}

        # Анализ уровней
        levels = {
            'very_profitable': np.percentile(spreads, 5),  # Топ 5% - отличный вход
            'profitable': np.percentile(spreads, 15),  # Топ 15% - хороший вход
            'break_even': np.percentile(spreads, 50),  # Медиана
            'loss_exit': np.percentile(spreads, 85),  # 85% - пора выходить
            'stop_loss': np.percentile(spreads, 95),  # 95% - стоп-лосс
        }

        # Частота попадания в зоны
        zones = {
            'very_profitable_freq': len(spreads[spreads <= levels['very_profitable']]) / len(spreads) * 100,
            'profitable_freq': len(spreads[spreads <= levels['profitable']]) / len(spreads) * 100,
            'loss_zone_freq': len(spreads[spreads >= levels['loss_exit']]) / len(spreads) * 100,
        }

        print(f"\n📊 АНАЛИЗ УРОВНЕЙ: {coin}")
        print("-" * 40)
        print(f"Отличный вход:  {levels['very_profitable']:.4f}% (попадание {zones['very_profitable_freq']:.1f}%)")
        print(f"Хороший вход:   {levels['profitable']:.4f}% (попадание {zones['profitable_freq']:.1f}%)")
        print(f"Безубыток:      {levels['break_even']:.4f}%")
        print(f"Пора выходить:  {levels['loss_exit']:.4f}% (попадание {zones['loss_zone_freq']:.1f}%)")
        print(f"Стоп-лосс:      {levels['stop_loss']:.4f}%")

        return {**levels, **zones}

    def create_visual_analysis(self, coins: List[str] = None):
        """Создание графиков для анализа"""
        if not coins:
            coins = list(self.data.keys())[:6]  # Максимум 6 монет

        plt.style.use('dark_background')
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Анализ спредов для торговых решений', fontsize=16)

        # График 1: Сравнение распределений
        ax1 = axes[0, 0]
        colors = ['cyan', 'magenta', 'yellow', 'lime', 'orange', 'red']

        for i, coin in enumerate(coins):
            if coin in self.data:
                df = self.data[coin]
                spreads = df['best_spread'] if 'best_spread' in df.columns else df['spread']
                ax1.hist(spreads, alpha=0.6, bins=30, label=coin, color=colors[i % len(colors)])

        ax1.set_xlabel('Спред (%)')
        ax1.set_ylabel('Частота')
        ax1.set_title('Распределение спредов')
        ax1.legend()
        ax1.grid(True, alpha=0.3)

        # График 2: Box plot сравнение
        ax2 = axes[0, 1]
        box_data = []
        box_labels = []

        for coin in coins:
            if coin in self.data:
                df = self.data[coin]
                spreads = df['best_spread'] if 'best_spread' in df.columns else df['spread']
                box_data.append(spreads)
                box_labels.append(coin)

        ax2.boxplot(box_data, labels=box_labels)
        ax2.set_ylabel('Спред (%)')
        ax2.set_title('Сравнение диапазонов')
        ax2.grid(True, alpha=0.3)

        # График 3: Временные ряды
        ax3 = axes[1, 0]

        for i, coin in enumerate(coins):
            if coin in self.data:
                df = self.data[coin]
                if 'elapsed' in df.columns:
                    spreads = df['best_spread'] if 'best_spread' in df.columns else df['spread']
                    # Показываем только каждую 10-ю точку для читаемости
                    sample_idx = np.arange(0, len(df), max(1, len(df) // 100))
                    ax3.plot(df['elapsed'].iloc[sample_idx], spreads.iloc[sample_idx],
                             label=coin, color=colors[i % len(colors)], alpha=0.8)

        ax3.set_xlabel('Время (сек)')
        ax3.set_ylabel('Спред (%)')
        ax3.set_title('Динамика во времени')
        ax3.legend()
        ax3.grid(True, alpha=0.3)

        # График 4: Статистика возможностей
        ax4 = axes[1, 1]

        if self.stats:
            coins_stats = [(coin, stats) for coin, stats in self.stats.items() if coin in coins]
            coin_names = [item[0] for item in coins_stats]
            q10_values = [item[1]['q10'] for item in coins_stats]
            negative_pcts = [item[1]['negative_spread_pct'] for item in coins_stats]

            x = np.arange(len(coin_names))
            width = 0.35

            ax4_twin = ax4.twinx()

            bars1 = ax4.bar(x - width / 2, q10_values, width, label='Q10 спред', color='cyan', alpha=0.7)
            bars2 = ax4_twin.bar(x + width / 2, negative_pcts, width, label='% отрицательных', color='magenta',
                                 alpha=0.7)

            ax4.set_xlabel('Монета')
            ax4.set_ylabel('Q10 спред (%)', color='cyan')
            ax4_twin.set_ylabel('% отрицательных спредов', color='magenta')
            ax4.set_title('Показатели привлекательности')
            ax4.set_xticks(x)
            ax4.set_xticklabels(coin_names)
            ax4.grid(True, alpha=0.3)

        plt.tight_layout()

        # Сохранение
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"spread_analysis_{timestamp}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.show()

        print(f"📊 График сохранен: {filename}")

    def compare_directions(self, coin: str):
        """Сравнение направлений торговли"""
        if coin not in self.data:
            print(f"❌ Нет данных для {coin}")
            return

        df = self.data[coin]

        if 'direction' not in df.columns:
            print(f"❌ Нет данных о направлениях для {coin}")
            return

        print(f"\n🔄 АНАЛИЗ НАПРАВЛЕНИЙ: {coin}")
        print("-" * 40)

        for direction in df['direction'].unique():
            subset = df[df['direction'] == direction]
            spreads = subset['best_spread'] if 'best_spread' in subset.columns else subset['spread']

            print(f"\n{direction.upper()}:")
            print(f"  Записей: {len(subset)}")
            print(f"  Средний: {spreads.mean():.4f}%")
            print(f"  Лучший:  {spreads.min():.4f}%")
            print(f"  Q10:     {np.percentile(spreads, 10):.4f}%")
            print(f"  Q25:     {np.percentile(spreads, 25):.4f}%")


def main():
    """Основная функция для интерактивного анализа"""
    analyzer = SpreadAnalyzer()

    print("🔍 АНАЛИЗАТОР СПРЕДОВ")
    print("=" * 50)

    # Загрузка данных
    data = analyzer.load_data()

    if not data:
        print("❌ Нет данных для анализа!")
        print("Убедитесь, что в текущей папке есть файлы spread_*.csv")
        return

    while True:
        print(f"\n📋 МЕНЮ АНАЛИЗА:")
        print("1. Сводная таблица")
        print("2. Рейтинг возможностей")
        print("3. Анализ уровней входа/выхода")
        print("4. Визуальный анализ")
        print("5. Сравнение направлений")
        print("6. Детальная статистика")
        print("0. Выход")

        try:
            choice = input("\nВыберите действие: ").strip()

            if choice == "0":
                break
            elif choice == "1":
                analyzer.print_summary_table()
            elif choice == "2":
                analyzer.print_opportunities_ranking()
            elif choice == "3":
                coin = input("Введите монету: ").upper()
                analyzer.analyze_entry_exit_levels(coin)
            elif choice == "4":
                coins_input = input("Монеты через запятую (или Enter для автовыбора): ").strip()
                coins = [c.strip().upper() for c in coins_input.split(',')] if coins_input else None
                analyzer.create_visual_analysis(coins)
            elif choice == "5":
                coin = input("Введите монету: ").upper()
                analyzer.compare_directions(coin)
            elif choice == "6":
                analyzer.calculate_statistics()

        except KeyboardInterrupt:
            print("\n👋 До свидания!")
            break
        except Exception as e:
            print(f"❌ Ошибка: {e}")


if __name__ == "__main__":
    main()