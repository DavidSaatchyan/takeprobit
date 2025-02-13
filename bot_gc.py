import time
from cex_connectors import get_exchange_connector
from strategy_gc import analyze_and_generate_setup
from config import LEVERAGE_SETTINGS, MARGIN  # Импортируем только LEVERAGE_SETTINGS и MARGIN
import telegram_bot  # Импортируем телеграм-бота для отправки сообщений

# Основная функция бота
def run_bot():
    connector = get_exchange_connector()
    
    while True:
        # Проверяем наличие открытых ордеров
        setup = analyze_and_generate_setup()  # Получаем текущий сетап
        if setup:
            open_orders = connector.get_open_orders(setup['symbol'])  # Используем символ из сетапа
        else:
            open_orders = []
        
        if not open_orders:
            # Если нет открытых ордеров, проверяем сигналы
            if setup:
                direction = setup['direction']
                entry_price = setup['entry_price']
                take_profit = setup['take_profit']
                stop_loss = setup['stop_loss']
                
                # Устанавливаем плечо (если нужно, вручную через интерфейс биржи)
                leverage = LEVERAGE_SETTINGS  # Используем LEVERAGE_SETTINGS из конфига
                
                # Рассчитываем объем сделки
                quantity = MARGIN * leverage / entry_price  # Используем MARGIN из конфига
                
                # Открываем ордер
                order_response = connector.place_order(
                    symbol=setup['symbol'],  # Используем symbol из setup
                    price=entry_price,
                    quantity=quantity,  # Используем quantity вместо volume
                    side=direction,
                    order_type="limit",
                    stop_loss=stop_loss,
                    take_profit=take_profit
                )
                
                if order_response.get("status") == "success":
                    message = f"✅ Ордер успешно открыт:\n" \
                              f"- Символ: {setup['symbol']}\n" \
                              f"- Направление: {direction}\n" \
                              f"- Цена входа: {entry_price}\n" \
                              f"- Take Profit: {take_profit}\n" \
                              f"- Stop Loss: {stop_loss}"
                    print(message)
                    telegram_bot.send_message(message)  # Отправляем сообщение в телеграм
                else:
                    message = f"❌ Ошибка при открытии ордера: {order_response}"
                    print(message)
                    telegram_bot.send_message(message)  # Отправляем сообщение в телеграм
            else:
                print("Сигнал для входа не найден.")
        else:
            print("Есть открытые ордера, пропускаем проверку сигналов.")
        
        # Ожидание 30 секунд перед следующей проверкой
        time.sleep(30)

# Запуск бота
if __name__ == "__main__":
    run_bot()