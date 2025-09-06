import time
import asyncio
import sys
import threading
import json
import os
import math
from cex_connectors import get_exchange_connector
from strategy_gc import generate_trading_setup
from config import MARGIN, EXECUTION_TIME_LIMIT, EXCHANGE, API_KEY, SECRET_KEY
import telegram_bot
from ws_bingx import BingXWebSocket
from ws_binance import BinanceWebSocket
from filter_gc import get_filtered_symbols
from precision import round_price, round_step_size
from strategy_gc import USE_TRAILING_ORDERS
# Исправляю импорт log_trade, если он не найден
try:
    from statistics import log_trade
except ImportError:
    def log_trade(data):
        print(f"[STAT] Логируем сделку: {data}")
import logging


def get_symbol_precision(symbol: str, connector=None):
    """Возвращает точность для разных символов"""
    if EXCHANGE == 'Binance':
        # Используем динамическое получение точности через API
        if connector and hasattr(connector, 'get_symbol_precision'):
            return connector.get_symbol_precision(symbol)
        else:
            # Fallback на безопасные значения по умолчанию
            return {'price_precision': 2, 'quantity_precision': 3}
    else:
        # Для других бирж используем безопасные значения по умолчанию
        return {'price_precision': 2, 'quantity_precision': 3}



ORDER_CACHE_FILE = 'order_cache.json'

def get_time_to_next_candle():
    now = time.time()
    candle_interval = 60
    next_candle_time = ((int(now // candle_interval) + 1)) * candle_interval
    time_to_wait = next_candle_time - now + 2
    return time_to_wait

async def check_and_cancel_expired_order(connector, order_id, order_info, chat_id, open_orders_info):
    """Проверка и отмена просроченных ордеров"""
    while order_id in open_orders_info:
        time_passed = time.time() - order_info['open_time']
        
        if time_passed > EXECUTION_TIME_LIMIT:
            try:
                # Получаем статус ордера через API
                order_status = connector.get_order_status(order_id, order_info['symbol'])
                if not isinstance(order_status, dict):  # Добавлена проверка
                    logging.error(f"[Timeout] Неверный формат ответа: {order_status}")
                    break
                    
                if order_status.get('status') in ('FILLED', 'PARTIALLY_FILLED'):
                    break
                    
                logging.info(f"[Timeout] Отменяем ордер {order_id}")
                cancel_response = connector.cancel_order(order_id, order_info['symbol'])
                
                # Проверка успешности для разных бирж
                if (EXCHANGE == 'Binance' and cancel_response.get('status') == 'CANCELED') or \
                   (EXCHANGE == 'BingX' and cancel_response.get('code') == 0):
                    del open_orders_info[order_id]
                    break
                    
            except Exception as e:
                logging.error(f"[Timeout] Ошибка: {e}")
                await asyncio.sleep(10)
                continue
                
        await asyncio.sleep(5)

# --- Откат: cancel_all_trailing_orders снова фильтрует по symbol и position_side ---
def cancel_all_trailing_orders(connector, symbol, position_side):
    print(f"🧹 Отмена всех трейлинг-ордеров для {symbol} ({position_side})")
    try:
        open_orders = connector.get_open_orders(symbol=symbol)
        orders = open_orders if isinstance(open_orders, list) else []
        trailing_orders = [order for order in orders if order.get("type") == "TRAILING_STOP_MARKET" and order.get("positionSide") == position_side]
        print(f"🎯 Найдено {len(trailing_orders)} трейлинг-ордеров для {symbol} ({position_side})")
        for order in trailing_orders:
            order_id = order.get('orderId')
            print(f"🛑 Отменяем трейлинг-ордер {order_id} для {symbol} (позиция: {position_side})")
            response = connector.cancel_order(order_id, symbol)
            is_success = response.get('status') == 'CANCELED' or response.get('code') == 0
            if is_success:
                print(f"✅ Трейлинг-ордер {order_id} успешно отменен")
            else:
                print(f"❌ Ошибка отмены трейлинг-ордера {order_id}: {response}")
    except Exception as e:
        print(f"❌ Ошибка при отмене трейлинг-ордеров: {e}")
        import traceback
        traceback.print_exc()

# --- Откат: cancel_all_stop_orders снова фильтрует по symbol и position_side ---
def cancel_all_stop_orders(connector, symbol, position_side):
    print(f"🧹 Отмена всех STOP_MARKET (SL) ордеров для {symbol} ({position_side})")
    try:
        open_orders = connector.get_open_orders(symbol=symbol)
        orders = open_orders if isinstance(open_orders, list) else []
        stop_orders = [order for order in orders if order.get("type") in ("STOP_MARKET", "STOP_LIMIT") and order.get("positionSide") == position_side]
        print(f"🎯 Найдено {len(stop_orders)} STOP_MARKET/STOP_LIMIT ордеров для {symbol} ({position_side})")
        for order in stop_orders:
            order_id = order.get('orderId')
            print(f"🛑 Отменяем SL-ордер {order_id} для {symbol} (позиция: {position_side})")
            response = connector.cancel_order(order_id, symbol)
            is_success = response.get('status') == 'CANCELED' or response.get('code') == 0
            if is_success:
                print(f"✅ SL-ордер {order_id} успешно отменен")
            else:
                print(f"❌ Ошибка отмены SL-ордера {order_id}: {response}")
    except Exception as e:
        print(f"❌ Ошибка при отмене SL-ордеров: {e}")
        import traceback
        traceback.print_exc()

# --- Откат: cancel_all_tp_orders снова фильтрует по symbol и position_side ---
def cancel_all_tp_orders(connector, symbol, position_side):
    print(f"🧹 Отмена всех TP ордеров для {symbol} ({position_side})")
    try:
        open_orders = connector.get_open_orders(symbol=symbol)
        orders = open_orders if isinstance(open_orders, list) else []
        tp_orders = [order for order in orders if order.get("type") in ("TAKE_PROFIT_MARKET", "LIMIT") and order.get("positionSide") == position_side]
        print(f"🎯 Найдено {len(tp_orders)} TP ордеров для {symbol} ({position_side})")
        for order in tp_orders:
            order_id = order.get('orderId')
            print(f"🛑 Отменяем TP-ордер {order_id} для {symbol} (позиция: {position_side})")
            response = connector.cancel_order(order_id, symbol)
            is_success = response.get('status') == 'CANCELED' or response.get('code') == 0
            if is_success:
                print(f"✅ TP-ордер {order_id} успешно отменен")
            else:
                print(f"❌ Ошибка отмены TP-ордера {order_id}: {response}")
    except Exception as e:
        print(f"❌ Ошибка при отмене TP-ордеров: {e}")
        import traceback
        traceback.print_exc()

def cancel_all_trailing_orders_by_symbol(connector, symbol):
    """Отменяет ВСЕ трейлинг-ордера для символа, независимо от позиции"""
    print(f"🧹 Принудительная отмена ВСЕХ трейлинг-ордеров для {symbol}")
    try:
        open_orders = connector.get_open_orders(symbol=symbol)
        
        # Адаптируем под разные биржи
        if EXCHANGE == 'BingX':
            orders = open_orders.get("data", {}).get("orders", [])
        else:  # Binance
            orders = open_orders if isinstance(open_orders, list) else []
        
        if not orders:
            print(f"⚠️ Нет данных об открытых ордерах для {symbol}")
            return
            
        all_trailing_orders = [order for order in orders 
                              if order.get("type") == "TRAILING_STOP_MARKET"]
        
        print(f"🎯 Найдено {len(all_trailing_orders)} трейлинг-ордеров для принудительной отмены")
        
        for order in all_trailing_orders:
            order_id = order.get('orderId')
            order_position_side = order.get('positionSide', 'UNKNOWN')
            print(f"🛑 Принудительно отменяем трейлинг-ордер {order_id} для {symbol} (позиция: {order_position_side})")
            response = connector.cancel_order(order_id, symbol)
            
            # Проверяем успешность отмены для разных бирж
            is_success = False
            if EXCHANGE == 'BingX':
                is_success = response and response.get("code") == 0
            elif EXCHANGE == 'Binance':
                is_success = response.get('status') == 'CANCELED' or response.get('code') == 0
            
            if is_success:
                print(f"✅ Трейлинг-ордер {order_id} успешно отменен")
            else:
                print(f"❌ Ошибка отмены трейлинг-ордера {order_id}: {response}")
                
    except Exception as e:
        print(f"❌ Ошибка при принудительной отмене трейлинг-ордеров: {e}")
        import traceback
        traceback.print_exc()

def save_order_cache(order_id, data):
    """Безопасное сохранение данных ордера в кэш"""
    try:
        cache = {}
        if os.path.exists(ORDER_CACHE_FILE):
            with open(ORDER_CACHE_FILE, 'r', encoding='utf-8') as f:
                try:
                    cache = json.load(f)
                except json.JSONDecodeError:
                    cache = {}
        
        # Конвертируем все числа в строки
        safe_data = {
            'symbol': data['symbol'],
            'direction': data['direction'],
            'size': str(data['size']),
            'entry_price': str(data['entry_price']),
            'stop_loss': str(data['stop_loss']),
            'take_profit': str(data.get('take_profit', '')),
            'position_side': data['position_side'],
            'leverage': str(data['leverage']),
            'entry_time': str(int(time.time() * 1000)),
            'tick_size': str(data.get('tick_size', '')),
            'step_size': str(data.get('step_size', ''))
        }
        
        cache[str(order_id)] = safe_data
        
        with open(ORDER_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
    except Exception as e:
        logging.error(f"[Cache] Ошибка сохранения: {e}")

async def run_bot(chat_id):
    logging.info(f"[run_bot] chat_id: {chat_id}")
    print(f"🔑 Используемые API ключи: API_KEY={API_KEY[:10]}..., SECRET_KEY={SECRET_KEY[:10]}...")
    print(f"🏢 Выбранная биржа: {EXCHANGE}")
    connector = get_exchange_connector()
    print(f"🔌 Тип коннектора: {type(connector).__name__}")
    open_orders_info = {}

    # Получаем отфильтрованный список символов при запуске
    symbols = get_filtered_symbols()
    if not symbols:
        print("❌ Не удалось получить список символов. Завершение работы.")
        await telegram_bot.send_direct_message("❌ Не удалось получить список символов. Бот остановлен.", chat_id)
        return
    
    print(f"📊 Загружен список символов: {symbols}")
    await telegram_bot.send_direct_message(f"🤖 Бот запущен с {len(symbols)} символами:\n{', '.join(symbols)}", chat_id)
    
    prev_trends: dict[str, int | None] = {symbol: None for symbol in symbols}  # хранение тренда по каждому символу

    if EXCHANGE == 'Binance':
        ws = BinanceWebSocket(API_KEY, SECRET_KEY, chat_id)
        logging.info(f"[run_bot] BinanceWebSocket создан с chat_id: {chat_id}")
    else:
        ws = BingXWebSocket(API_KEY, SECRET_KEY, chat_id)
    threading.Thread(target=ws.start, daemon=True).start()

    while True:
        try:
            
            # Проверяем обновление списка символов (filter_gc сам решает, нужно ли обновлять)
            new_symbols = get_filtered_symbols()
            if new_symbols != symbols:
                symbols = new_symbols
                print(f"📊 Обновленный список символов: {symbols}")
                await telegram_bot.send_direct_message(f"🔄 Список символов обновлен ({len(symbols)} символов):\n{', '.join(symbols)}", chat_id)
                
                # Обновляем словарь трендов для новых символов
                new_prev_trends = {}
                for symbol in symbols:
                    if symbol in prev_trends:
                        new_prev_trends[symbol] = prev_trends[symbol]  # сохраняем существующий тренд
                    else:
                        new_prev_trends[symbol] = None  # новый символ
                prev_trends = new_prev_trends
                print(f"🔄 Обновлен словарь трендов: {list(prev_trends.keys())}")
            
            time_to_wait = get_time_to_next_candle()
            print(f"⏳ Ожидание следующей свечи: {time_to_wait:.2f} сек")
            await asyncio.sleep(time_to_wait)

            for symbol in symbols:
                print(f"🔍 Проверяем символ: {symbol}")
                setup, new_trend = await generate_trading_setup(connector, symbol, prev_trends[symbol])
                prev_trends[symbol] = new_trend

                if setup is None:
                    print(f"Нет сетапа для {symbol}")
                    continue

                print(f"🔍 Анализируем сетап для {symbol}")

                # Проверка открытых ордеров ТОЛЬКО перед лимитным ордером
                open_orders_for_symbol = {
                    k: v for k, v in open_orders_info.items()
                    if v['symbol'] == symbol
                }
                print(f"📋 Открытые ордера для {symbol}: {open_orders_for_symbol}")

                if open_orders_for_symbol:
                    print(f"⚠️ Есть открытые ордера по {symbol}, пропускаем")
                    continue

                # Подготовка параметров ордера
                if setup is not None:
                    direction = setup['direction']
                    entry_price = setup['entry_price']
                    take_profit = setup['take_profit']
                    stop_loss = setup['stop_loss']
                    leverage = setup['leverage']
                    quantity = MARGIN * leverage / entry_price
                else:
                    continue
                
                # Проверяем минимальную сумму ордера для Binance (5 USDT)
                if EXCHANGE == 'Binance':
                    notional_value = quantity * entry_price
                    if notional_value < 5.0:
                        # Увеличиваем количество до минимума
                        min_quantity = 5.0 / entry_price
                        quantity = min_quantity
                
                # Для Binance округляем цены и количество согласно требованиям биржи
                if EXCHANGE == 'Binance':
                    precision = get_symbol_precision(symbol, connector)
                    tick_size = precision.get('tick_size')
                    step_size = precision.get('step_size')
                    if tick_size is not None:
                        tick_size = float(tick_size)
                    else:
                        tick_size = 10 ** (-precision['price_precision'])
                    if step_size is not None:
                        step_size = float(step_size)
                    else:
                        step_size = 10 ** (-precision['quantity_precision'])
                    entry_price = round_price(entry_price, tick_size)
                    take_profit = round_price(take_profit, tick_size)
                    stop_loss = round_price(stop_loss, tick_size)
                    quantity = round_step_size(quantity, step_size)
                            # entry_price, take_profit, stop_loss уже округлены через round_price
        # quantity уже округлен через round_step_size

                # Для Binance — устанавливаем изолированную маржу перед выставлением ордера
                if EXCHANGE == 'Binance':
                    margin_type_resp = connector.set_margin_type(symbol, 'ISOLATED')
                    if margin_type_resp.get('code') == -4066:
                        print(f"[Binance] Маржа для {symbol} уже установлена как ISOLATED")
                    elif margin_type_resp.get('status') == 'ok':
                        print(f"[Binance] Маржа для {symbol} установлена как ISOLATED")

                position_side = "LONG" if direction == "buy" else "SHORT"
                leverage_response = connector.set_leverage(symbol, leverage, position_side)
                
                # Проверяем успешность установки плеча для разных бирж
                leverage_success = False
                if EXCHANGE == 'BingX':
                    leverage_success = leverage_response.get("code") == 0
                elif EXCHANGE == 'Binance':
                    # Для Binance успешная установка плеча возвращает информацию о символе
                    leverage_success = (
                        leverage_response.get('code') == 0 or 
                        leverage_response.get('status') == 'ok' or
                        (leverage_response.get('symbol') is not None and leverage_response.get('leverage') is not None)
                    )
                
                if not leverage_success:
                    print(f"❌ Ошибка при установке плеча: {leverage_response}")
                    await telegram_bot.send_direct_message(f"❌ Ошибка при установке плеча:\n{leverage_response}", chat_id)
                    continue

                print(f"📈 Ордер: {symbol} | {direction.upper()} | entry={entry_price} | qty={quantity:.4f} | SL={stop_loss} | TP={take_profit} | leverage={leverage}x")
                print(f"🚀 Создаем лимитный ордер для {symbol}...")

                # Округление TP stop price
                tp_stop_price = setup.get('take_profit_stop_price')
                if tp_stop_price is not None and tick_size is not None:
                    tp_stop_price = round_price(tp_stop_price, tick_size)
                order_response = connector.place_order(
                    symbol=symbol,
                    price=entry_price,
                    quantity=quantity,
                    side=direction,
                    position_side="LONG" if direction == "buy" else "SHORT",
                    order_type="limit",
                    take_profit=take_profit,
                    take_profit_stopPrice=tp_stop_price,
                )
                logging.info(f"[bot_gc] Ответ place_order: {order_response}")
                order_success = False
                order_id = None
                sl_order_id = None
                
                if EXCHANGE == 'BingX':
                    order_success = order_response.get("status") == "success"
                    order_id = str(order_response.get("order_id"))
                elif EXCHANGE == 'Binance':
                    # Критические ошибки — сразу break/continue
                    if order_response.get('code') == -2019:
                        print(f"⚠️ Недостаточно средств для создания ордера по {symbol}")
                        await telegram_bot.send_direct_message(f"⚠️ Недостаточно средств для {symbol}. Пополните аккаунт или уменьшите размер ордера.", chat_id)
                        continue
                    elif order_response.get('code') == -1111:
                        print(f"⚠️ Ошибка precision для {symbol}: {order_response}")
                        await telegram_bot.send_direct_message(f"⚠️ Ошибка precision для {symbol}. Попробуйте другой символ.", chat_id)
                        continue
                    elif order_response.get('code') == -4014:
                        print(f"⚠️ Ошибка tick size для {symbol}: {order_response}")
                        print(f"🔍 Детали: entry_price={entry_price}, tick_size={tick_size}")
                        await telegram_bot.send_direct_message(f"⚠️ Ошибка tick size для {symbol}. Проблема с округлением цены.", chat_id)
                        continue
                    # Всё остальное — если есть orderId, продолжаем работу
                    order_success = order_response.get('orderId') is not None
                    order_id = str(order_response.get('orderId'))
                logging.info(f"[bot_gc] Ответ place_order: {order_response}")
                if order_success and order_id:
                    order_cache_data = setup.copy()
                    order_cache_data.update({
                        'symbol': symbol,
                        'direction': direction,
                        'entry_price': entry_price,
                        'size': quantity,
                        'order_type': "LIMIT",
                        'tick_size': tick_size,
                        'step_size': step_size,
                        'position_side': position_side,
                        'leverage': leverage,
                        'stop_loss': stop_loss,
                        'stop_loss_stop_price': setup.get('stop_loss_stop_price'),
                        'take_profit_stop_price': tp_stop_price,
                    })
                    if USE_TRAILING_ORDERS:
                        order_cache_data.update({
                            'trailing_sl_callback_rate': setup.get('trailing_sl_callback_rate'),
                            'trailing_sl_activation_price': setup.get('trailing_sl_activation_price'),
                            'trailing_tp_callback_rate': setup.get('trailing_tp_callback_rate'),
                            'trailing_tp_activation_price': setup.get('trailing_tp_activation_price'),
                        })
                    else:
                        order_cache_data.update({
                            'take_profit': setup.get('take_profit'),
                            'take_profit_stop_price': setup.get('take_profit_stop_price'),
                        })
                    print(f"[DEBUG] order_cache_data keys: {list(order_cache_data.keys())}")
                    save_order_cache(order_id, order_cache_data)
                    print(f"[DEBUG] order_id {order_id} сохранён в order_cache.json")

                    # Сохраняем в словарь open_orders_info для контроля времени исполнения
                    open_orders_info[order_id] = {
                    'symbol': symbol,
                    'type': 'LIMIT',
                    'open_time': time.time(),
                    }

                    asyncio.create_task(
                        check_and_cancel_expired_order(
                            connector, order_id, open_orders_info[order_id], chat_id, open_orders_info
                        )
                    )

                    # Логируем только открытие сделки (entry)
                    order_cache_data_log = order_cache_data.copy()
                    print("LOG_TRADE", symbol, direction, order_cache_data_log['entry_time'])
                    try:
                        log_trade(order_cache_data_log)
                        print(f"[STAT] Сделка {symbol} {direction} залогирована в статистике.")
                    except Exception as e:
                        print(f"[STAT] Ошибка логирования сделки: {e}")
                else:
                    print(f"❌ Ошибка размещения ордера: {order_response}")
                    await telegram_bot.send_direct_message(f"❌ Ошибка размещения ордера:\n{order_response}", chat_id)
                    print(f"🔍 Детали ошибки для {symbol}: {order_response}")

        except Exception as e:
            print(f"‼️ Ошибка в основном цикле: {str(e)}")
            await asyncio.sleep(60)

        # После отмены/исполнения ордера

if __name__ == "__main__":
    if len(sys.argv) > 1:
        chat_id = int(sys.argv[1])
    else:
        try:
            with open("chat_id.txt", "r") as f:
                chat_id = int(f.read().strip())
        except (FileNotFoundError, ValueError):
            print("Ошибка: не удалось прочитать chat_id")
            sys.exit(1)

    asyncio.run(run_bot(chat_id))
