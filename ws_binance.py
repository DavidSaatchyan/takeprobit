import json
import websocket
import threading
import time
import logging
import requests
import hmac
import hashlib
import os
import asyncio
from datetime import datetime
from cex_connectors import BinanceConnector
import telegram_bot
from precision import round_price, round_step_size, get_symbol_precision

try:
    from statistics import log_trade, update_trade_exit
except ImportError:
    from .statistics import log_trade, update_trade_exit

BINANCE_WS_URL = "wss://fstream.binance.com/ws/{}"
BINANCE_API_URL = "https://fapi.binance.com"
ORDER_CACHE_FILE = 'order_cache.json'
USE_TRAILING_ORDERS = False

class BinanceWebSocket:
    def __init__(self, api_key, secret_key, chat_id):
        self.api_key = api_key
        self.secret_key = secret_key
        self.chat_id = chat_id
        self.listen_key = None
        self.ws = None
        self.connector = BinanceConnector()
        self.should_reconnect = True
        self.open_trades = {}
        self._time_offset = 0
        self.last_message_time = 0
        self.message_delay = 2  # Минимальная задержка между сообщениями (сек)
        self.message_counter = 0  # Счетчик сообщений для дебага

    async def _send_telegram_message(self, message):
        """Унифицированная отправка сообщений с обработкой ошибок"""
        self.message_counter += 1
        msg_num = self.message_counter
        logging.info(f"[Telegram#{msg_num}] Попытка отправить: {message[:100]}...")
    
        current_time = time.time()
        elapsed = current_time - self.last_message_time
    
        if elapsed < self.message_delay:
            wait_time = self.message_delay - elapsed
            logging.info(f"[Telegram#{msg_num}] Ждем {wait_time:.2f} сек (rate limit)")
            await asyncio.sleep(wait_time)
    
        try:
            logging.info(f"[Telegram#{msg_num}] Отправка...")
            await telegram_bot.send_direct_message(message, self.chat_id)
            self.last_message_time = time.time()
            logging.info(f"[Telegram#{msg_num}] Успешно отправлено")
        except Exception as e:
            logging.error(f"[Telegram#{msg_num}] Ошибка: {str(e)}", exc_info=True)
            raise  # Пробрасываем исключение дальше для видимости в логах

    def _sync_time(self):
        """Синхронизация времени с сервером Binance"""
        try:
            response = requests.get(f"{BINANCE_API_URL}/fapi/v1/time")
            server_time = response.json().get('serverTime')
            if server_time:
                local_time = int(time.time() * 1000)
                self._time_offset = server_time - local_time
                logging.info(f"[TimeSync] Смещение времени: {self._time_offset}ms")
        except Exception as e:
            logging.warning(f"[TimeSync] Ошибка синхронизации: {e}")

    def generate_listen_key(self):
        """Генерация нового listenKey"""
        self._sync_time()
        try:
            response = self.connector.generate_listen_key()
            if response and response.get("status") == "ok":
                self.listen_key = response.get("listenKey")
                logging.info(f"[ListenKey] Получен новый ключ: {self.listen_key}")
                return True
        except Exception as e:
            logging.error(f"[ListenKey] Ошибка генерации: {e}")
        return False

    def renew_listen_key(self):
        """Периодическое обновление listenKey"""
        while self.should_reconnect:
            time.sleep(1800)  # 30 минут
            if self.listen_key:
                try:
                    response = self.connector.renew_listen_key(self.listen_key)
                    if not response or response.get("status") != "ok":
                        logging.warning("[ListenKey] Не удалось продлить, генерируем новый")
                        self.generate_listen_key()
                except Exception as e:
                    logging.error(f"[ListenKey] Ошибка продления: {e}")
                    self.generate_listen_key()

    def on_open(self, ws):
        """Обработчик открытия соединения"""
        logging.info("[WebSocket] Соединение установлено")
        asyncio.run_coroutine_threadsafe(
            self._send_telegram_message("🔌 WebSocket подключен к Binance"),
            asyncio.get_event_loop()
        )

    async def _place_sl_tp_orders(self, trade_data):
        """Выставление ордеров Stop-Loss и Take-Profit"""
        try:
            symbol = trade_data['symbol']
            direction = trade_data['direction']
            quantity = float(trade_data['size'])
            stop_loss = float(trade_data['stop_loss'])
            take_profit = float(trade_data.get('take_profit', 0))
            position_side = trade_data.get('position_side', "LONG" if trade_data['direction'] == "buy" else "SHORT")

            # Получаем точность для символа
            precision = get_symbol_precision(symbol, self.connector)
            tick_size = float(precision.get('tick_size', 0.0001))
            step_size = float(precision.get('step_size', 0.001))

            # Выставляем Stop-Loss
            sl_price = round_price(stop_loss, tick_size)
            sl_quantity = round_step_size(quantity, step_size)
            
            sl_response = self.connector.place_order(
                symbol=symbol,
                price=None,
                quantity=sl_quantity,
                side="SELL" if direction == "buy" else "BUY",
                position_side=position_side,
                order_type="STOP_MARKET",
                stopPrice=sl_price,
                reduceOnly=True
            )

            if sl_response and sl_response.get('orderId'):
                await self._send_telegram_message(
                    f"🛡️ *Stop-Loss установлен*\n"
                    f"```\n"
                    f"Символ: {symbol}\n"
                    f"Цена: {sl_price:.8f}\n"
                    f"```"
                )

            # Выставляем Take-Profit если указан
            if take_profit > 0:
                tp_price = round_price(take_profit, tick_size)
                tp_response = self.connector.place_order(
                    symbol=symbol,
                    price=None,
                    quantity=sl_quantity,
                    side="BUY" if direction == "sell" else "SELL",
                    position_side=position_side,
                    order_type="TAKE_PROFIT_MARKET",
                    stopPrice=tp_price,
                    reduceOnly=True
                )

                if tp_response and tp_response.get('orderId'):
                    await self._send_telegram_message(
                        f"🎯 *Take-Profit установлен*\n"
                        f"```\n"
                        f"Символ: {symbol}\n"
                        f"Цена: {tp_price:.8f}\n"
                        f"```"
                    )

        except Exception as e:
            logging.error(f"[SL/TP] Ошибка: {e}")
            await self._send_telegram_message(
                f"❌ *Ошибка SL/TP*\n"
                f"```\n"
                f"Символ: {symbol}\n"
                f"Ошибка: {str(e)[:200]}\n"
                f"```"
            )

    async def _handle_limit_order_fill(self, order_data):
        """Обработка исполнения лимитного ордера"""
        order_id = str(order_data.get("i"))
        symbol = order_data.get("s")
        price = float(order_data.get("ap", 0))
        quantity = float(order_data.get("q", 0))

        # Загрузка кэша
        cache = {}
        if os.path.exists(ORDER_CACHE_FILE):
            try:
                with open(ORDER_CACHE_FILE, 'r', encoding='utf-8') as f:
                    cache = json.load(f)
            except Exception as e:
                logging.error(f"[Cache] Ошибка чтения: {e}")
                return

        trade_data = cache.get(order_id)
        if not trade_data:
            logging.error(f"[Cache] Нет данных для ордера {order_id}")
            await self._send_telegram_message(
                f"⚠️ *Нет данных в кэше*\n"
                f"Ордер {order_id} ({symbol}) исполнен, но нет данных для SL/TP"
            )
            return

        # Отправка уведомления
        await self._send_telegram_message(
            f"✅ *Лимитный ордер исполнен*\n"
            f"```\n"
            f"Символ: {symbol}\n"
            f"Направление: {trade_data['direction'].upper()}\n"
            f"Цена: {price:.8f}\n"
            f"Объем: {quantity:.4f}\n"
            f"ID: {order_id}\n"
            f"```"
        )

        # Выставление SL/TP
        await self._place_sl_tp_orders(trade_data)

        # Логирование закрытия сделки
        cache.pop(order_id, None)
        with open(ORDER_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)

    async def handle_order_update(self, order_data):
        """Обработка обновлений ордеров"""
        try:
            order_status = order_data.get("X")
            order_id = str(order_data.get("i"))
            symbol = order_data.get("s")
            side = order_data.get("S")
            price = float(order_data.get("ap", 0) or 0)
            order_type = order_data.get("o")
            quantity = float(order_data.get("q", 0) or 0)
            close_time = order_data.get("T")
            position_side = order_data.get("ps", "BOTH")

            # NEW - ордер создан на бирже
            if order_status == "NEW":
                # Добавляем проверку типа ордера
                if order_type in ("LIMIT", "MARKET", "STOP_MARKET", "TAKE_PROFIT_MARKET"):
                    try:
                        await self._send_telegram_message(
                            f"🆕 *Ордер создан ({order_type})*\n"
                            f"```\n"
                            f"Символ: {symbol}\n"
                            f"Направление: {'BUY' if side == 'BUY' else 'SELL'}\n"
                            f"Цена: {price:.8f}\n"
                            f"Объем: {quantity:.4f}\n"
                            f"ID: {order_id}\n"
                            f"```"
                        )
                    except Exception as e:
                        logging.error(f"[Order-NEW] Ошибка уведомления: {e}")

            # FILLED - ордер исполнен
            elif order_status == "FILLED":
                if order_type == "LIMIT":
                    await self._handle_limit_order_fill(order_data)
                elif order_type in ("STOP_MARKET", "TAKE_PROFIT_MARKET"):
                    reason = "TP" if order_type == "TAKE_PROFIT_MARKET" else "SL"
                    await self._send_telegram_message(
                        f"🔒 *Позиция закрыта ({reason})*\n"
                        f"```\n"
                        f"Символ: {symbol}\n"
                        f"Цена: {price:.8f}\n"
                        f"Объем: {quantity:.4f}\n"
                        f"```"
                    )

                    # Логирование закрытия сделки
                    cache = {}
                    if os.path.exists(ORDER_CACHE_FILE):
                        with open(ORDER_CACHE_FILE, 'r', encoding='utf-8') as f:
                            cache = json.load(f)
                    
                    trade_data = cache.pop(order_id, None)
                    if trade_data:
                        exit_data = {
                            'exit_time': close_time,
                            'exit_price': price,
                            'reason': reason,
                            'pnl': '',
                            'fee': '',
                        }
                        update_trade_exit(
                            symbol=trade_data['symbol'],
                            direction=trade_data['direction'],
                            entry_time=trade_data['entry_time'],
                            exit_data=exit_data
                        )
                        
                        with open(ORDER_CACHE_FILE, 'w', encoding='utf-8') as f:
                            json.dump(cache, f, ensure_ascii=False, indent=2)

            # CANCELED - ордер отменен
            elif order_status == "CANCELED":
                await self._send_telegram_message(
                    f"⚠️ *Ордер отменен*\n"
                    f"```\n"
                    f"Символ: {symbol}\n"
                    f"Тип: {order_type}\n"
                    f"ID: {order_id}\n"
                    f"```"
                )

            # EXPIRED - ордер истек
            elif order_status == "EXPIRED":
                await self._send_telegram_message(
                    f"🕒 *Ордер истек*\n"
                    f"```\n"
                    f"Символ: {symbol}\n"
                    f"Тип: {order_type}\n"
                    f"ID: {order_id}\n"
                    f"```"
                )

        except Exception as e:
            logging.error(f"[OrderUpdate] Ошибка: {e}")

    def on_message(self, ws, message):
        """Обработчик входящих сообщений"""
        try:
            data = json.loads(message)
            event_type = data.get("e")
            
            if event_type == "ORDER_TRADE_UPDATE":
                asyncio.run_coroutine_threadsafe(
                    self.handle_order_update(data.get("o")),
                    asyncio.get_event_loop()
                )
            elif event_type == "ACCOUNT_UPDATE":
                logging.debug(f"[AccountUpdate] {data}")
            elif event_type == "listenKeyExpired":
                logging.warning("[ListenKey] Истек, переподключаемся...")
                self.generate_listen_key()
                self.reconnect()

        except Exception as e:
            logging.error(f"[Message] Ошибка обработки: {e}")

    def on_error(self, ws, error):
        """Обработчик ошибок WebSocket"""
        logging.error(f"[WebSocket] Ошибка: {error}")
        asyncio.run_coroutine_threadsafe(
            self._send_telegram_message(f"🚨 *WebSocket ошибка*\n```\n{str(error)[:200]}\n```"),
            asyncio.get_event_loop()
        )
        self.reconnect()

    def on_close(self, ws, close_status_code, close_msg):
        """Обработчик закрытия соединения"""
        logging.warning(f"[WebSocket] Закрыто: {close_status_code} - {close_msg}")
        asyncio.run_coroutine_threadsafe(
            self._send_telegram_message("🔌 WebSocket соединение закрыто"),
            asyncio.get_event_loop()
        )
        self.reconnect()

    def reconnect(self):
        """Переподключение к WebSocket"""
        if self.should_reconnect:
            time.sleep(5)
            self.start()

    def stop(self):
        """Остановка WebSocket"""
        self.should_reconnect = False
        if self.ws:
            self.ws.close()
        asyncio.run_coroutine_threadsafe(
            self._send_telegram_message("🛑 WebSocket остановлен"),
            asyncio.get_event_loop()
        )

    def start(self):
        """Запуск WebSocket"""
        if not self.generate_listen_key():
            logging.error("[WebSocket] Не удалось получить listenKey")
            return

        # Запуск потока для обновления listenKey
        threading.Thread(target=self.renew_listen_key, daemon=True).start()

        # Создание WebSocket соединения
        self.ws = websocket.WebSocketApp(
            BINANCE_WS_URL.format(self.listen_key),
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )

        # Запуск в отдельном потоке
        self.ws_thread = threading.Thread(target=self.ws.run_forever, daemon=True)
        self.ws_thread.start()

if __name__ == "__main__":
    from config import API_KEY, SECRET_KEY
    logging.basicConfig(level=logging.INFO)
    binance_ws = BinanceWebSocket(API_KEY, SECRET_KEY, chat_id=285029874)
    binance_ws.start()