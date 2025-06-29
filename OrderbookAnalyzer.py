import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings('ignore')


class OrderbookAnalyzer:
    def __init__(self, csv_path):
        self.df = pd.read_csv(csv_path)
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
        self._calculate_spreads()

    def _calculate_spreads(self):
        """Расчет спредов как в скальпере"""
        # Spread1: Binance long / Hyperliquid short
        self.df['spread1'] = (1 - self.df['binance_ask1_price'] / self.df['hyperliquid_bid1_price']) * 100

        # Spread2: Hyperliquid long / Binance short
        self.df['spread2'] = (1 - self.df['hyperliquid_ask1_price'] / self.df['binance_bid1_price']) * 100

        # Максимальный спред
        self.df['max_spread'] = np.maximum(self.df['spread1'], self.df['spread2'])

        # Лучший спред (который бы выбрал скальпер)
        self.df['best_spread'] = np.where(
            self.df['spread1'] > self.df['spread2'],
            self.df['spread1'],
            self.df['spread2']
        )

        # Направление сделки
        self.df['direction'] = np.where(
            self.df['spread1'] > self.df['spread2'],
            'Binance_long_HL_short',
            'HL_long_Binance_short'
        )

    def analyze_profitability(self, min_spread_pct=0.26, max_hold_seconds=30):
        """Анализ прибыльности с учетом времени удержания"""
        tradeable = self.df[self.df['best_spread'] >= min_spread_pct].copy()

        if len(tradeable) == 0:
            return {"error": "Нет торговых возможностей"}

        # Симуляция времени удержания позиций
        profits = []
        hold_times = []

        for idx in tradeable.index:
            entry_spread = tradeable.loc[idx, 'best_spread']
            entry_time = tradeable.loc[idx, 'timestamp']

            # Ищем момент закрытия (когда спред станет меньше)
            future_data = self.df[
                (self.df['timestamp'] > entry_time) &
                (self.df['timestamp'] <= entry_time + timedelta(seconds=max_hold_seconds))
                ]

            if len(future_data) > 0:
                # Находим когда спред упал до приемлемого уровня
                exit_points = future_data[future_data['best_spread'] < min_spread_pct * 0.3]

                if len(exit_points) > 0:
                    exit_time = exit_points.iloc[0]['timestamp']
                    hold_time = (exit_time - entry_time).total_seconds()

                    # Простая модель: профит = entry_spread - комиссии - проскальзывание
                    profit = entry_spread - 0.05 - 0.02  # 0.05% комиссии, 0.02% проскальзывание

                    profits.append(profit)
                    hold_times.append(hold_time)

        return {
            "total_trades": len(profits),
            "avg_profit": np.mean(profits) if profits else 0,
            "avg_hold_time": np.mean(hold_times) if hold_times else 0,
            "win_rate": len([p for p in profits if p > 0]) / len(profits) * 100 if profits else 0,
            "profits": profits,
            "hold_times": hold_times
        }

    def optimize_settings(self):
        """Поиск оптимальных настроек"""
        thresholds = np.arange(0.05, 1.0, 0.05)
        results = []

        for threshold in thresholds:
            trades = len(self.df[self.df['best_spread'] >= threshold])
            avg_spread = self.df[self.df['best_spread'] >= threshold]['best_spread'].mean()

            if trades > 0:
                profitability = self.analyze_profitability(threshold)
                daily_trades = trades / (len(self.df) / (24 * 60))  # на день

                results.append({
                    'threshold': threshold,
                    'trades_count': trades,
                    'daily_trades': daily_trades,
                    'avg_spread': avg_spread,
                    'avg_profit': profitability.get('avg_profit', 0),
                    'win_rate': profitability.get('win_rate', 0)
                })

        return pd.DataFrame(results)

    def plot_comprehensive_analysis(self, min_spread=0.26):
        """Комплексная визуализация"""
        fig, axes = plt.subplots(3, 2, figsize=(16, 18))
        fig.suptitle('📊 АНАЛИЗ ОРДЕРБУКА MOVE - ПОЛНЫЙ РАЗБОР', fontsize=16, fontweight='bold')

        # 1. Динамика спредов во времени
        ax1 = axes[0, 0]
        sample_data = self.df.iloc[::max(1, len(self.df) // 500)]  # сэмплируем для скорости

        ax1.plot(sample_data.index, sample_data['spread1'], alpha=0.7, label='Spread1 (Bin→HL)', linewidth=1)
        ax1.plot(sample_data.index, sample_data['spread2'], alpha=0.7, label='Spread2 (HL→Bin)', linewidth=1)
        ax1.plot(sample_data.index, sample_data['best_spread'], color='red', label='Best Spread', linewidth=2)
        ax1.axhline(y=min_spread, color='green', linestyle='--', label=f'Порог {min_spread}%')
        ax1.set_title('🔥 Динамика спредов')
        ax1.set_ylabel('Спред (%)')
        ax1.legend()
        ax1.grid(True, alpha=0.3)

        # 2. Распределение спредов
        ax2 = axes[0, 1]
        spread_data = self.df['best_spread'].dropna()
        ax2.hist(spread_data, bins=50, alpha=0.7, color='skyblue', edgecolor='black')
        ax2.axvline(x=min_spread, color='red', linestyle='--', linewidth=2, label=f'Порог {min_spread}%')
        ax2.set_title('📈 Распределение лучших спредов')
        ax2.set_xlabel('Спред (%)')
        ax2.set_ylabel('Частота')
        ax2.legend()
        ax2.grid(True, alpha=0.3)

        # 3. Торговые возможности по времени
        ax3 = axes[1, 0]
        tradeable = self.df[self.df['best_spread'] >= min_spread]
        hourly_trades = tradeable.groupby(tradeable['timestamp'].dt.hour).size()

        bars = ax3.bar(hourly_trades.index, hourly_trades.values, color='lightgreen', alpha=0.8)
        ax3.set_title('⏰ Торговые возможности по часам')
        ax3.set_xlabel('Час (UTC)')
        ax3.set_ylabel('Количество сигналов')
        ax3.grid(True, alpha=0.3)

        # Добавляем значения на столбцы
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                ax3.text(bar.get_x() + bar.get_width() / 2., height + 0.5,
                         f'{int(height)}', ha='center', va='bottom', fontsize=9)

        # 4. Оптимизация порога спреда
        ax4 = axes[1, 1]
        opt_results = self.optimize_settings()

        ax4_twin = ax4.twinx()
        line1 = ax4.plot(opt_results['threshold'], opt_results['daily_trades'], 'bo-', label='Сделок/день')
        line2 = ax4_twin.plot(opt_results['threshold'], opt_results['avg_profit'], 'ro-', label='Средняя прибыль %')

        ax4.set_xlabel('Порог спреда (%)')
        ax4.set_ylabel('Сделок в день', color='blue')
        ax4_twin.set_ylabel('Средняя прибыль (%)', color='red')
        ax4.set_title('⚙️ Оптимизация настроек')
        ax4.grid(True, alpha=0.3)

        # Легенда
        lines = line1 + line2
        labels = [l.get_label() for l in lines]
        ax4.legend(lines, labels, loc='upper right')

        # 5. Корреляция цен между биржами
        ax5 = axes[2, 0]
        sample_for_corr = self.df.iloc[::max(1, len(self.df) // 1000)]
        ax5.scatter(sample_for_corr['binance_ask1_price'], sample_for_corr['hyperliquid_ask1_price'],
                    alpha=0.6, s=1, color='purple')

        # Линия идеальной корреляции
        min_price = min(sample_for_corr['binance_ask1_price'].min(), sample_for_corr['hyperliquid_ask1_price'].min())
        max_price = max(sample_for_corr['binance_ask1_price'].max(), sample_for_corr['hyperliquid_ask1_price'].max())
        ax5.plot([min_price, max_price], [min_price, max_price], 'r--', alpha=0.8, label='Идеальная корреляция')

        correlation = np.corrcoef(sample_for_corr['binance_ask1_price'], sample_for_corr['hyperliquid_ask1_price'])[
            0, 1]
        ax5.set_title(f'🔗 Корреляция цен (r={correlation:.4f})')
        ax5.set_xlabel('Binance Ask')
        ax5.set_ylabel('Hyperliquid Ask')
        ax5.legend()
        ax5.grid(True, alpha=0.3)

        # 6. Статистика по направлениям сделок
        ax6 = axes[2, 1]
        direction_stats = tradeable['direction'].value_counts()

        colors = ['lightcoral', 'lightblue']
        wedges, texts, autotexts = ax6.pie(direction_stats.values,
                                           labels=[d.replace('_', '\n') for d in direction_stats.index],
                                           autopct='%1.1f%%',
                                           colors=colors,
                                           startangle=90)

        ax6.set_title('🎯 Распределение направлений сделок')

        plt.tight_layout()
        plt.show()

    def print_detailed_stats(self, min_spread=0.26):
        """Детальная статистика в консоль"""
        print("=" * 80)
        print("🚀 ДЕТАЛЬНЫЙ АНАЛИЗ ТОРГОВЫХ ВОЗМОЖНОСТЕЙ")
        print("=" * 80)

        total_points = len(self.df)
        tradeable = self.df[self.df['best_spread'] >= min_spread]
        trade_count = len(tradeable)

        print(f"📊 ОБЩАЯ СТАТИСТИКА:")
        print(f"   • Всего точек данных: {total_points:,}")
        print(f"   • Торговых возможностей: {trade_count:,}")
        print(f"   • Процент времени в торговле: {trade_count / total_points * 100:.2f}%")
        print(f"   • Сделок в час: ~{trade_count / 24:.1f}")
        print()

        print(f"💰 АНАЛИЗ СПРЕДОВ:")
        print(f"   • Средний спред (все): {self.df['best_spread'].mean():.4f}%")
        print(f"   • Средний спред (торговые): {tradeable['best_spread'].mean():.4f}%")
        print(f"   • Максимальный спред: {self.df['best_spread'].max():.4f}%")
        print(f"   • Медианный спред: {self.df['best_spread'].median():.4f}%")
        print()

        # Анализ прибыльности
        profit_analysis = self.analyze_profitability(min_spread)
        if 'error' not in profit_analysis:
            print(f"🎯 АНАЛИЗ ПРИБЫЛЬНОСТИ:")
            print(f"   • Средняя прибыль на сделку: {profit_analysis['avg_profit']:.4f}%")
            print(f"   • Винрейт: {profit_analysis['win_rate']:.1f}%")
            print(f"   • Среднее время удержания: {profit_analysis['avg_hold_time']:.1f} сек")
            print()

        # Рекомендации
        print(f"⚙️ РЕКОМЕНДАЦИИ ПО НАСТРОЙКАМ:")
        opt_df = self.optimize_settings()
        best_config = opt_df.loc[opt_df['avg_profit'].idxmax()] if len(opt_df) > 0 else None

        if best_config is not None:
            print(f"   • Оптимальный порог: {best_config['threshold']:.3f}%")
            print(f"   • Ожидаемых сделок в день: {best_config['daily_trades']:.1f}")
            print(f"   • Средняя прибыль: {best_config['avg_profit']:.4f}%")
            print()

        # Альтернативные пороги
        print(f"📋 АЛЬТЕРНАТИВНЫЕ НАСТРОЙКИ:")
        for threshold in [0.1, 0.15, 0.2, 0.3, 0.4, 0.5]:
            trades = len(self.df[self.df['best_spread'] >= threshold])
            if trades > 0:
                daily = trades / (len(self.df) / (24 * 60))
                avg_spread = self.df[self.df['best_spread'] >= threshold]['best_spread'].mean()
                print(
                    f"   • Порог {threshold:.1f}%: {trades} сделок ({daily:.1f}/день), средний спред {avg_spread:.3f}%")

        print("=" * 80)


# Использование
if __name__ == "__main__":
    # Создаем анализатор
    analyzer = OrderbookAnalyzer('orderbook_MOVE_120306.csv')

    # Печатаем статистику
    analyzer.print_detailed_stats(min_spread=0.26)

    # Строим графики
    analyzer.plot_comprehensive_analysis(min_spread=0.26)

    # Дополнительный анализ для разных порогов
    print("\n🔍 СРАВНЕНИЕ РАЗНЫХ ПОРОГОВ:")
    for threshold in [0.15, 0.2, 0.26, 0.3, 0.4]:
        profit_data = analyzer.analyze_profitability(threshold)
        if 'error' not in profit_data:
            print(f"Порог {threshold}%: {profit_data['total_trades']} сделок, "
                  f"прибыль {profit_data['avg_profit']:.3f}%, "
                  f"винрейт {profit_data['win_rate']:.1f}%")