import os
import asyncio
import nest_asyncio
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler, MessageHandler, filters, JobQueue
from importlib import reload
import config
import subprocess
import threading
from cex_connectors import get_exchange_connector
from datetime import datetime, timedelta

# Загружаем переменные окружения
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# Применяем патч для устранения проблемы с циклом событий
nest_asyncio.apply()

# Флаг для отслеживания состояния работы бота
bot_running = False
bot_process = None  # Процесс запущенного бота
chat_id = None  # ID чата для отправки сообщений

# Функция для отправки сообщений в чат
async def send_message(message):
    if chat_id:
        application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
        await application.bot.send_message(chat_id=chat_id, text=message)

# Функция для обновления клавиатуры
async def update_keyboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = ReplyKeyboardMarkup(
        [
            [KeyboardButton("⚙️ Настройки"), KeyboardButton("🚀 Запуск торговли" if not bot_running else "🛑 Остановить торговлю")],
            [KeyboardButton("📊 Анализ P&L")],
        ],
        resize_keyboard=True
    )
    if update.message:
        await update.message.reply_text("Выберите команду:", reply_markup=keyboard)
    elif update.callback_query:
        await update.callback_query.message.reply_text("Выберите команду:", reply_markup=keyboard)

# Команда /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global chat_id
    chat_id = update.effective_chat.id  # Сохраняем ID чата
    await update_keyboard(update, context)

# Команда /config для просмотра и изменения настроек
async def show_config(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    keyboard = [
        [InlineKeyboardButton("Изменить Symbol", callback_data='change_SYMBOL')],
        [InlineKeyboardButton("Изменить Leverage", callback_data='change_LEVERAGE_SETTINGS')],
        [InlineKeyboardButton("Изменить Margin", callback_data='change_MARGIN')],
        [InlineKeyboardButton("Изменить TP Multiplier", callback_data='change_ATR_MULTIPLIER_TP')],
        [InlineKeyboardButton("Изменить SL Multiplier", callback_data='change_ATR_MULTIPLIER_SL')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    config_text = (
        f"Текущие настройки:\n"
        f"Symbol: {config.SYMBOL}\n"
        f"Leverage: {config.LEVERAGE_SETTINGS}\n"
        f"Margin: {config.MARGIN}\n"
        f"TP Multiplier: {config.ATR_MULTIPLIER_TP}\n"
        f"SL Multiplier: {config.ATR_MULTIPLIER_SL}"
    )
    await update.message.reply_text(config_text, reply_markup=reply_markup)

# Обработка нажатий на кнопки изменения настроек
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    # Проверяем, начинается ли callback_data с "change_"
    if query.data.startswith("change_"):
        setting_to_change = query.data.split('_', 1)[1]
        context.user_data['setting_to_change'] = setting_to_change
        await query.edit_message_text(text=f"Введите новое значение для {setting_to_change}:")
    else:
        # Если callback_data не начинается с "change_", передаем управление другим обработчикам
        await pnl_period_handler(update, context)

# Обработка ввода новых значений и команд
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = update.message.text
    setting = context.user_data.get('setting_to_change')

    if setting:
        try:
            new_value = int(text) if setting != "SYMBOL" else text
            with open('config.py', 'r') as file:
                lines = file.readlines()
            with open('config.py', 'w') as file:
                for line in lines:
                    if line.startswith(f"{setting} ="):
                        file.write(f"{setting} = '{new_value}'\n" if setting == "SYMBOL" else f"{setting} = {new_value}\n")
                    else:
                        file.write(line)
            reload(config)
            await update.message.reply_text(f"Настройка {setting} успешно изменена на {new_value}.")
        except ValueError:
            await update.message.reply_text("Некорректное значение. Пожалуйста, введите правильное значение.")
        context.user_data.pop('setting_to_change')
    else:
        if text == "⚙️ Настройки":
            await show_config(update, context)
        elif text == "🚀 Запуск торговли":
            await start_bot(update, context)
        elif text == "🛑 Остановить торговлю":
            await stop_bot(update, context)
        elif text == "📊 Анализ P&L":
            await analyze_pnl(update, context)
        else:
            await update.message.reply_text("Используйте доступные кнопки для взаимодействия с ботом.")

# Запуск бота
async def start_bot(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global bot_running, bot_process
    if not bot_running:
        bot_running = True
        bot_process = subprocess.Popen(["python", "bot_gc.py"])
        await update.message.reply_text("🚀 Торговый бот запущен.")
        await update_keyboard(update, context)
    else:
        await update.message.reply_text("Бот уже запущен.")

# Остановка бота
async def stop_bot(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global bot_running, bot_process
    if bot_running:
        bot_running = False
        bot_process.terminate()
        bot_process = None
        await update.message.reply_text("🛑 Торговый бот остановлен.")
        await update_keyboard(update, context)
    else:
        await update.message.reply_text("Бот уже остановлен.")

# Анализ PnL
async def analyze_pnl(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    connector = get_exchange_connector()  # Используем правильное название функции
    symbol = config.SYMBOL  # Используем символ из конфига

    # Клавиатура для выбора периода анализа
    keyboard = [
        [InlineKeyboardButton("1 день", callback_data='pnl_1')],
        [InlineKeyboardButton("7 дней", callback_data='pnl_7')],
        [InlineKeyboardButton("30 дней", callback_data='pnl_30')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text("Выберите период для анализа P&L:", reply_markup=reply_markup)

# Обработка выбора периода для анализа PnL
async def pnl_period_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    # Получаем количество дней из callback_data
    period = int(query.data.split('_')[1])
    connector = get_exchange_connector()
    symbol = config.SYMBOL

    # Получаем данные о PnL за выбранный период
    pnl_data = connector.get_pnl(symbol, income_type="REALIZED_PNL", days=period)

    if pnl_data:
        total_pnl = sum(float(item['income']) for item in pnl_data if item['incomeType'] == "REALIZED_PNL")
        message = f"📊 P&L за последние {period} дней:\n" \
                  f"Общий P&L: {total_pnl:.2f} USDT\n" \
                  f"Детали:\n"
        for item in pnl_data:
            if item['incomeType'] == "REALIZED_PNL":
                message += f"- {item['time']}: {item['income']} {item['asset']}\n"
    else:
        message = "Нет данных о P&L за выбранный период."

    await query.edit_message_text(text=message)

# Основная функция запуска бота
async def main():
    application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    application.add_handler(CallbackQueryHandler(button_handler, pattern="^change_"))  # Обработчик для изменения настроек
    application.add_handler(CallbackQueryHandler(pnl_period_handler, pattern="^pnl_"))  # Обработчик для анализа PnL

    print("🔗 Телеграм-бот запущен...")
    await application.run_polling()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())