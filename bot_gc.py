import time
import asyncio
import sys
import threading
from cex_connectors import get_exchange_connector
from strategy_gc import generate_trading_setup
from config import MARGIN, EXECUTION_TIME_LIMIT, EXCHANGE, API_KEY, SECRET_KEY
import telegram_bot
from ws_bingx import BingXWebSocket

def get_time_to_next_candle():
    now = time.time()
    candle_interval = 60
    next_candle_time = ((int(now // candle_interval) + 1)) * candle_interval
    time_to_wait = next_candle_time - now + 2
    return time_to_wait

async def check_and_cancel_expired_order(connector, order_id, order_info, chat_id):
    await asyncio.sleep(3)

    while True:
        current_time = time.time()
        time_passed = current_time - order_info['open_time']
        
        open_orders_response = connector.get_open_orders(symbol=order_info['symbol'])
        open_orders = open_orders_response.get("data", {}).get("orders", [])
        order_exists = any(str(order.get("orderId")) == order_id for order in open_orders)

        if not order_exists:
            return

        if time_passed > EXECUTION_TIME_LIMIT and order_info['type'] == "LIMIT":
            cancel_response = connector.cancel_order(order_id, order_info['symbol'])
            if cancel_response.get("status") == "success":
                message = f"❌ Ордер отменен по истечении времени:\nID: {order_id}\nСимвол: {order_info['symbol']}"
                await telegram_bot.send_message(message, chat_id)
                return

        await asyncio.sleep(30)

async def run_bot(chat_id):
    connector = get_exchange_connector()
    open_orders_info = {}

    symbols = ['SOL-USDT', 'ADA-USDT', 'AVAX-USDT', 'SUI-USDT', '1000PEPE-USDT', 'AAVE-USDT', 'WIF-USDT', 'UNI-USDT','BNB-USDT', 'DOGE-USDT']  # список активов
    prev_trends = {symbol: None for symbol in symbols}  # хранение тренда по каждому символу

    bingx_ws = BingXWebSocket(API_KEY, SECRET_KEY, chat_id)
    threading.Thread(target=bingx_ws.start, daemon=True).start()

    while True:
        try:
            time_to_wait = get_time_to_next_candle()
            print(f"⏳ Ожидание следующей свечи: {time_to_wait:.2f} сек")
            await asyncio.sleep(time_to_wait)

            for symbol in symbols:
                setup, new_trend = await generate_trading_setup(connector, symbol, prev_trends[symbol])
                prev_trends[symbol] = new_trend

                if setup is None:
                    print(f"Нет сетапа для {symbol}")
                    continue

                print(f"🔍 Анализируем сетап для {symbol}")

                # Проверка открытых ордеров
                open_orders_for_symbol = {
                    k: v for k, v in open_orders_info.items()
                    if v['symbol'] == symbol
                }

                if open_orders_for_symbol:
                    print(f"⚠️ Есть открытые ордера по {symbol}, пропускаем")
                    continue

                # Подготовка параметров ордера
                direction = setup['direction']
                entry_price = setup['entry_price']
                take_profit = setup['take_profit']
                stop_loss = setup['stop_loss']
                leverage = setup['leverage']
                quantity = MARGIN * leverage / entry_price

                print(f"📈 Ордер: {symbol} | {direction.upper()} | entry={entry_price} | qty={quantity:.4f} | SL={stop_loss} | TP={take_profit} | leverage={leverage}x")

                # Установка плеча с retry
                if EXCHANGE == 'BingX':
                    position_side = "LONG" if direction == "buy" else "SHORT"
                    max_retries = 3
                    delay_seconds = 2

                    for attempt in range(1, max_retries + 1):
                       leverage_response = connector.set_leverage(symbol, leverage, position_side)
                       print(f"🛠 Установка плеча (попытка {attempt}): {leverage_response}")

                       if leverage_response.get("code") == 0:
                           break  # успешно
                       elif leverage_response.get("code") == 109500 and attempt < max_retries:
                           await asyncio.sleep(delay_seconds * attempt)  # увеличивающаяся задержка
                           continue
                       else:
                           message = f"❌ Ошибка установки плеча: {leverage_response}"
                           await telegram_bot.send_message(message, chat_id)
                           continue  # переход к следующей сделке или завершение


                # Размещение ордера
                order_response = connector.place_order(
                    symbol=symbol,
                    price=entry_price,
                    quantity=quantity,
                    side=direction,
                    position_side="LONG" if direction == "buy" else "SHORT",
                    order_type="limit",
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    stop_loss_stopPrice=setup.get('stop_loss_stop_price')
                )

                print(f"📤 Ответ от place_order: {order_response}")

                if order_response.get("status") == "success":
                    order_id = str(order_response.get("order_id"))
                    open_orders_info[order_id] = {
                        'symbol': symbol,
                        'open_time': time.time(),
                        'type': "LIMIT"
                    }

                    message = (
                        f"⏳ Новый ордер:\n"
                        f"- Символ: {symbol}\n"
                        f"- Направление: {direction.upper()}\n"
                        f"- Цена: {entry_price}\n"
                        f"- Объем: {quantity:.4f}\n"
                        f"- Стоп-лосс: {stop_loss}\n"
                        f"- Тейк-профит: {take_profit}\n"
                        f"- Плечо: {leverage}x"
                    )
                    await telegram_bot.send_message(message, chat_id)

                    asyncio.create_task(
                        check_and_cancel_expired_order(
                            connector, order_id, open_orders_info[order_id], chat_id
                        )
                    )
                else:
                    print(f"❌ Ошибка размещения ордера: {order_response}")
                    await telegram_bot.send_message(f"❌ Ошибка размещения ордера:\n{order_response}", chat_id)

        except Exception as e:
            print(f"‼️ Ошибка в основном цикле: {str(e)}")
            await asyncio.sleep(60)

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
