import json
import websocket
import gzip
import io
import threading
import time
from cex_connectors import BingXConnector
import telegram_bot
import asyncio
import os
try:
    from statistics import log_trade, update_trade_exit
except ImportError:
    from .statistics import log_trade, update_trade_exit

# URL для WebSocket с listenKey
LISTEN_KEY_URL = "wss://open-api-swap.bingx.com/swap-market?listenKey={}"
ORDER_CACHE_FILE = 'order_cache.json'

# Класс для работы с WebSocket
class BingXWebSocket:
    def __init__(self, api_key, secret_key, chat_id):
        self.api_key = api_key
        self.secret_key = secret_key
        self.chat_id = chat_id  # Добавлен chat_id
        self.listen_key = None
        self.ws = None
        self.connector = BingXConnector()
        self.should_reconnect = True  # Флаг для управления переподключением
        self.open_trades = {}  # Кэш открытых сделок: order_id -> dict

    def generate_listen_key(self):
        """Генерация listenKey через REST API."""
        response = self.connector.generate_listen_key()
        if response and "listenKey" in response:
            self.listen_key = response["listenKey"]
            print(f"Generated listenKey: {self.listen_key}")
        else:
            print("Failed to generate listenKey")

    def renew_listen_key(self):
        """Обновление listenKey каждые 50 минут."""
        while True:
            time.sleep(3000)  # 50 минут
            if self.listen_key:
                response = self.connector.renew_listen_key(self.listen_key)
                if response and response.get("code") == 0:
                    print("ListenKey renewed successfully")
                else:
                    print("Failed to renew listenKey. Generating new listenKey...")
                    self.generate_listen_key()  # Генерируем новый listenKey, если обновление не удалось
            else:
                print("No listenKey available. Generating new listenKey...")
                self.generate_listen_key()  # Генерируем новый listenKey, если его нет

    def on_open(self, ws):
        """Обработчик открытия соединения."""
        print("WebSocket connected")
        # Убрана подписка на канал, так как она не требуется для аккаунтовых данных

    def on_message(self, ws, message):
        """Обработчик сообщений от WebSocket."""
        try:
            if not message:
                print("Received empty message from WebSocket.")
                return

            # Декомпрессия и декодирование
            compressed_data = gzip.GzipFile(fileobj=io.BytesIO(message), mode='rb')
            decompressed_data = compressed_data.read()
            utf8_data = decompressed_data.decode('utf-8')

            if not utf8_data:
                print("Decompressed data is empty.")
                return

            if utf8_data == "Ping":
                ws.send("Pong")
                return

            try:
                data = json.loads(utf8_data)
            except json.JSONDecodeError as e:
                print(f"Failed to decode JSON: {e}")
                return

            # Пропускаем сообщения типа SNAPSHOT
            if data.get("e") == "SNAPSHOT":
                return  # Пропускаем сообщения типа SNAPSHOT

            # Логируем сырое сообщение только для ORDER_TRADE_UPDATE
            print(f"Raw WebSocket message: {utf8_data}")
            print(f"Parsed WebSocket data: {json.dumps(data, indent=2)}")

            # Обрабатываем только сообщения типа ORDER_TRADE_UPDATE
            if data.get("e") == "ORDER_TRADE_UPDATE":
                order_data = data.get("o")
                if order_data:
                    # Запускаем асинхронную задачу для обработки обновления ордера
                    asyncio.run(self.handle_order_update(order_data))
                else:
                    print("Order data is missing in the message")

        except Exception as e:
            print(f"Error processing message: {e}")

    async def handle_order_update(self, order_data):
        """Обработка обновлений ордеров."""
        try:
            print(f"Processing order update: {order_data}")
    
            order_status = order_data.get("X")
            order_id = str(order_data.get("i"))
            symbol = order_data.get("s")
            side = order_data.get("S")  # BUY/SELL
            price = float(order_data.get("p", 0) or 0)
            order_type = order_data.get("o")
            quantity = float(order_data.get("q", 0) or 0)
            close_time = order_data.get("T")  # время закрытия (timestamp)

            # --- Закрытие позиции: отменяем трейлинг-ордера ---
            if order_status in ("TRADE", "FILLED", "CLOSED", "PARTIALLY_FILLED"):
                print(f"Order {order_id} executed/closed")
                await telegram_bot.send_message(
                    f"☑️ {side} {order_type} ORDER executed: Coin: {symbol}, Price: {price}",
                    self.chat_id
                )
                
                # Отменяем трейлинг-ордера ТОЛЬКО при закрытии позиции
                if order_type in ("STOP", "STOP_MARKET", "TAKE_PROFIT", "TAKE_PROFIT_MARKET", "TRAILING_STOP_MARKET"):
                    try:
                        from bot_gc import cancel_all_trailing_orders
                        
                        # Используем positionSide из WebSocket события (ps)
                        position_side = order_data.get("ps", "LONG")  # по умолчанию LONG
                        
                        print(f"[CLOSE] Закрытие позиции {symbol} ({position_side}) через {order_type}")
                        
                        # Отменяем трейлинг-ордера для этой позиции
                        cancel_all_trailing_orders(self.connector, symbol, position_side)
                        print(f"[CANCELED] Трейлинг-ордера отменены для {symbol} ({position_side})")
                        
                    except Exception as e:
                        print(f"[ERROR] Ошибка отмены трейлинг-ордеров: {e}")
                        await telegram_bot.send_message(
                            f"❌ Ошибка отмены трейлинг-ордеров для {symbol}: {e}",
                            self.chat_id
                        )
                
                # Логирование сделки (если есть данные в кэше)
                cache = {}
                if os.path.exists(ORDER_CACHE_FILE):
                    with open(ORDER_CACHE_FILE, 'r', encoding='utf-8') as f:
                        try:
                            cache = json.load(f)
                        except Exception:
                            cache = {}
                trade_data = cache.pop(order_id, None)
                if trade_data:
                    # Определяем причину закрытия
                    if order_type in ("TAKE_PROFIT", "TAKE_PROFIT_MARKET"):
                        reason = "TP"
                    elif order_type in ("STOP", "STOP_MARKET"):
                        reason = "SL"
                    elif order_type == "TRAILING_STOP_MARKET":
                        reason = "TRAILING"
                    elif order_type == "LIMIT":
                        reason = "LIMIT"
                    else:
                        reason = "MANUAL"
                    exit_data = {
                        'exit_time': close_time,
                        'exit_price': price,
                        'reason': reason,
                        'pnl': '',  # можно рассчитать, если есть данные
                        'fee': '',
                    }
                    print("UPDATE_TRADE_EXIT", trade_data['symbol'], trade_data['direction'], trade_data['entry_time'], exit_data)
                    update_trade_exit(
                        symbol=trade_data['symbol'],
                        direction=trade_data['direction'],
                        entry_time=trade_data['entry_time'],
                        exit_data=exit_data
                    )
                    print(f"[UPDATED] Сделка {order_id} обновлена в статистике.")
                    
                    # Сохраняем обновлённый кэш
                    with open(ORDER_CACHE_FILE, 'w', encoding='utf-8') as f:
                        json.dump(cache, f, ensure_ascii=False, indent=2)
                else:
                    print(f"[WARN] Нет данных по открытию сделки {order_id} для логирования!")
                return

            # --- Открытие позиции: ничего не делаем ---
            if order_status in ("NEW", "OPEN", "LIMIT"):
                print(f"Order {order_id} opened: {order_type}")
                return

            # --- Отмена ордера: отменяем трейлинг-ордера ---
            if order_status == "CANCELED":
                print(f"Order {order_id} canceled")
                await telegram_bot.send_message(
                    f"⚠️ {side} ORDER canceled: Coin: {symbol}, Price: {price}",
                    self.chat_id
                )
                
                # Отменяем трейлинг-ордера при отмене основного ордера
                try:
                    from bot_gc import cancel_all_trailing_orders
                    # Используем positionSide из WebSocket события (ps)
                    position_side = order_data.get("ps", "LONG")  # по умолчанию LONG
                    cancel_all_trailing_orders(self.connector, symbol, position_side)
                    print(f"[CANCELED] Трейлинг-ордера отменены при отмене ордера {order_id} ({position_side})")
                except Exception as e:
                    print(f"[ERROR] Ошибка отмены трейлинг-ордеров при отмене ордера: {e}")
                return
            else:
                print(f"Unknown order status: {order_status}")

        except Exception as e:
            print(f"Error in handle_order_update: {e}")

    def on_error(self, ws, error):
        """Обработчик ошибок."""
        print(f"WebSocket error: {error}")
        # Если произошла ошибка, пытаемся переподключиться
        self.reconnect()

    def on_close(self, ws, close_status_code, close_msg):
        """Обработчик закрытия соединения."""
        print("WebSocket connection closed")
        # Если соединение закрыто, пытаемся переподключиться
        self.reconnect()

    def reconnect(self):
        """Переподключение к WebSocket."""
        if self.should_reconnect:
            print("Attempting to reconnect...")
            time.sleep(5)  # Ждем 5 секунд перед повторным подключением
            self.start()

    def stop(self):
        """Остановка WebSocket."""
        self.should_reconnect = False
        if self.ws:
            self.ws.close()

    def start(self):
        """Запуск WebSocket."""
        self.generate_listen_key()
        if not self.listen_key:
            print("Failed to start WebSocket: no listenKey")
            return

        # Запуск потока для обновления listenKey
        threading.Thread(target=self.renew_listen_key, daemon=True).start()

        # Подключение к WebSocket
        self.ws = websocket.WebSocketApp(
            LISTEN_KEY_URL.format(self.listen_key),
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        self.ws.run_forever()

# Запуск WebSocket
if __name__ == "__main__":
    from config import API_KEY, SECRET_KEY
    bingx_ws = BingXWebSocket(API_KEY, SECRET_KEY, chat_id=285029874)  # Пример chat_id
    bingx_ws.start()