from time import sleep
from collections import defaultdict
from pybit.unified_trading import WebSocket
import sqlite3
import time
import logging

# Настройка логирования
logging.basicConfig(filename="pybit.log", level=logging.DEBUG,
                    format="%(asctime)s %(levelname)s %(message)s")

# Подключение к WebSocket
ws = WebSocket(
    testnet=False,  # Укажите False для подключения к реальной сети
    channel_type="linear",  # Используем линейные контракты (USDT Perpetual)
)

# Хранилище свечей
candles = defaultdict(lambda: {
    "open": None,
    "high": -float("inf"),
    "low": float("inf"),
    "close": None,
    "volume": 0
})

# Сохранение свечей в SQLite
DB_PATH = "db/data.db"

def save_candle_to_db(symbol, interval, open_time, candle):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS candles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                interval TEXT NOT NULL,
                open_time INTEGER NOT NULL,
                open_price REAL NOT NULL,
                high_price REAL NOT NULL,
                low_price REAL NOT NULL,
                close_price REAL NOT NULL,
                volume REAL NOT NULL,
                close_time INTEGER,
                UNIQUE(symbol, interval, open_time)
            )
        ''')
        cursor.execute('''
            INSERT OR IGNORE INTO candles (
                symbol, interval, open_time, open_price, high_price, low_price, close_price, volume, close_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (symbol, interval, open_time, candle["open"], candle["high"], candle["low"], candle["close"], candle["volume"], open_time + 1))
        conn.commit()

def handle_trade(message):
    """
    Обработчик сообщений из потока сделок.
    """
    if "data" not in message:
        return

    for trade in message["data"]:
        symbol = trade["s"]  # Символ торговой пары
        timestamp = int(trade["T"] / 1000)  # Время сделки в секундах
        price = float(trade["p"])  # Цена сделки
        volume = float(trade.get("v", 0))  # Объём сделки (может быть под ключом 'v')

        candle = candles[timestamp]
        if candle["open"] is None:
            candle["open"] = price
        candle["close"] = price
        candle["high"] = max(candle["high"], price)
        candle["low"] = min(candle["low"], price)
        candle["volume"] += volume

        # Сохраняем свечу в базу данных, если время изменилось
        if timestamp != int(time.time()):
            save_candle_to_db(symbol, "1s", timestamp, candle)
            del candles[timestamp]


# Подписка на поток сделок для BTCUSDT
ws.trade_stream("BTCUSDT", handle_trade)

# Бесконечный цикл для работы программы
while True:
    sleep(1)
