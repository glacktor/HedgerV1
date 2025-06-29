import pandas as pd
import matplotlib.pyplot as plt
import sys


def visualize_spreads(filename):
    # Загрузка
    df = pd.read_csv(filename)
    coins = df['coin'].unique()

    # График
    plt.style.use('dark_background')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))

    # 1. Спреды во времени
    colors = ['cyan', 'magenta', 'yellow', 'lime', 'orange']
    for i, coin in enumerate(coins):
        coin_data = df[df['coin'] == coin]
        ax1.plot(range(len(coin_data)), coin_data['best_spread'],
                 label=f"{coin}: {coin_data['best_spread'].mean():.3f}%",
                 color=colors[i], alpha=0.8)

    ax1.set_ylabel('Спред (%)')
    ax1.set_title('Динамика спредов')
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # 2. Средние значения + объемы
    stats = []
    for coin in coins:
        coin_data = df[df['coin'] == coin]
        stats.append({
            'coin': coin,
            'avg_spread': coin_data['best_spread'].mean(),
            'max_spread': coin_data['best_spread'].max(),
            'avg_volume': (coin_data['binance_bid_vol'].mean() +
                           coin_data['hyper_bid_vol'].mean()) / 2
        })

    stats_df = pd.DataFrame(stats)
    x = range(len(coins))

    # Спреды
    bars = ax2.bar(x, stats_df['avg_spread'], color='cyan', alpha=0.7)
    ax2.set_ylabel('Средний спред (%)', color='cyan')
    ax2.tick_params(axis='y', labelcolor='cyan')

    # Объемы на второй оси
    ax2_twin = ax2.twinx()
    ax2_twin.plot(x, stats_df['avg_volume'], 'o-', color='yellow', markersize=8)
    ax2_twin.set_ylabel('Средний объем', color='yellow')
    ax2_twin.tick_params(axis='y', labelcolor='yellow')

    ax2.set_xticks(x)
    ax2.set_xticklabels(coins)
    ax2.set_title('Средние спреды и объемы')
    ax2.grid(True, alpha=0.3, axis='y')

    # Значения на барах
    for i, (spread, vol) in enumerate(zip(stats_df['avg_spread'], stats_df['avg_volume'])):
        ax2.text(i, spread + 0.001, f'{spread:.3f}%', ha='center', va='bottom', fontsize=9)

    plt.tight_layout()
    plt.savefig(filename.replace('.csv', '_analysis.png'), dpi=150)
    plt.show()

    # Статистика
    print(f"\n{'=' * 60}")
    print("📊 СТАТИСТИКА:")
    print(f"{'=' * 60}")
    print(f"{'Монета':<8} {'Записей':<10} {'Сред.спред%':<12} {'Макс.спред%':<12} {'Сред.объем':<15}")
    print("-" * 60)

    for _, row in stats_df.iterrows():
        coin_count = len(df[df['coin'] == row['coin']])
        print(f"{row['coin']:<8} {coin_count:<10} {row['avg_spread']:<12.4f} "
              f"{row['max_spread']:<12.4f} {row['avg_volume']:<15.2f}")

    # Лучшие моменты
    print(f"\n{'=' * 60}")
    print("🎯 ЛУЧШИЕ СПРЕДЫ:")
    print(f"{'=' * 60}")

    for coin in coins:
        coin_data = df[df['coin'] == coin]
        best_idx = coin_data['best_spread'].idxmax()
        best_row = coin_data.loc[best_idx]
        direction = "Binance→Hyper" if best_row['spread_b2h'] > best_row['spread_h2b'] else "Hyper→Binance"
        print(f"{coin}: {best_row['best_spread']:.4f}% ({direction}) в {best_row['time'][11:19]}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        visualize_spreads(sys.argv[1])
    else:
        import glob

        files = sorted(glob.glob("spreads_*.csv"))
        if files:
            print(f"Найдено файлов: {len(files)}")
            print(f"Используем последний: {files[-1]}")
            visualize_spreads(files[-1])
        else:
            print("❌ CSV файлы не найдены!")