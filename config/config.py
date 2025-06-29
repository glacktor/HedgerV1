import json
import os
from CexWsClients.AsyncBinanceWSClient import AsyncBinanceWSClient
from CexWsClients.AsyncBybitWSClient import AsyncBybitWSClient
from CexWsClients.AsyncOKXWSClient import AsyncOKXWSClient
from CexWsClients.AsyncHyperliquidWSClient import AsyncHyperliquidWSClient

# Путь к конфигу
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")

with open(CONFIG_PATH, "r") as f:
    cfg = json.load(f)

# Клиенты
CLIENTS = {
    "binance": AsyncBinanceWSClient(
        api_key=cfg["binance"]["api_key"],
        api_secret=cfg["binance"]["api_secret"]
    ),
    # "bybit": AsyncBybitWSClient(),  # можно расширить позже
    "okx": AsyncOKXWSClient(
        api_key=cfg["okx"]["api_key"],
        api_secret=cfg["okx"]["api_secret"],
        pass_phrase=cfg["okx"]["passphrase"]
    ),
    "hyperliquid": AsyncHyperliquidWSClient.from_key(cfg["hyperliquid"]["private_key"])
}
