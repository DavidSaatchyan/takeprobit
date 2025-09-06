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
from filter_gc import get_filtered_symbols  # Импортируем функции из filter_gc
import pandas as pd  # Добавьте эту строку в начало файла

print("Forced stdout flush started", flush=True)
# Load environment variables
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# Apply patch for event loop issues
nest_asyncio.apply()

# Flag to track bot state
bot_running = False
bot_process = None  # Process of the running bot
chat_id = None  # Chat ID for sending messages

# Global Application object for sending messages
application = None


# Ваш chat_id
MY_CHAT_ID = 285029874  # Замените на ваш реальный chat_id

# Функция для проверки доступа
async def check_access(update: Update) -> bool:
    if update.effective_chat.id != MY_CHAT_ID:
        await update.message.reply_text("❌ Доступ запрещен. Этот бот доступен только для владельца.")
        return False
    return True

# Function to send messages to the chat
async def send_message(message, chat_id=None):
    global application
    if chat_id is None:
        chat_id = globals().get('chat_id')  # Use global chat_id if set
    if chat_id:
        try:
            if application is None:
                application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
            await application.bot.send_message(chat_id=chat_id, text=message)
        except Exception as e:
            print(f"Error sending message to Telegram: {e}")
            # Retry after 5 seconds
            await asyncio.sleep(5)
            await send_message(message, chat_id)
    else:
        print("chat_id not set. Message not sent.")

# Function to set chat_id
def set_chat_id(new_chat_id):
    global chat_id
    chat_id = new_chat_id
    # Save chat_id to a file so bot_gc.py can read it
    with open("chat_id.txt", "w") as file:
        file.write(str(new_chat_id))

# Function to update the keyboard
async def update_keyboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = ReplyKeyboardMarkup(
        [
            [KeyboardButton("⚙️ Settings"), KeyboardButton("🚀 Start Trading" if not bot_running else "🛑 Stop Trading")],
            [KeyboardButton("📊 PnL Analysis")],
            [KeyboardButton("🔍 Search Coins")],
        ],
        resize_keyboard=True
    )
    if update.message:
        await update.message.reply_text("Choose a command:", reply_markup=keyboard)
    elif update.callback_query:
        await update.callback_query.message.reply_text("Choose a command:", reply_markup=keyboard)

# Command /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global chat_id
    chat_id = update.effective_chat.id  # Save chat ID

    # Проверяем доступ
    if not await check_access(update):
        return

    set_chat_id(chat_id)  # Set chat_id for use in other modules
    await update.message.reply_text("Bot started. Use the buttons to control the bot.")
    await update_keyboard(update, context)

# Command /config to view and change settings
async def show_config(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Проверяем доступ
    if not await check_access(update):
        return

    keyboard = [
        [InlineKeyboardButton("Change Margin", callback_data='change_MARGIN')],
        [InlineKeyboardButton("Change TP Multiplier", callback_data='change_ATR_MULTIPLIER_TP')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    config_text = (
        f"Current settings:\n"
        f"Margin: {config.MARGIN}\n"
        f"TP Multiplier: {config.ATR_MULTIPLIER_TP}\n"
    )
    await update.message.reply_text(config_text, reply_markup=reply_markup)

# Handle button clicks for changing settings
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Проверяем доступ
    if not await check_access(update):
        return

    query = update.callback_query
    await query.answer()

    # Check if callback_data starts with "change_"
    if query.data.startswith("change_"):
        setting_to_change = query.data.split('_', 1)[1]
        context.user_data['setting_to_change'] = setting_to_change
        await query.edit_message_text(text=f"Enter a new value for {setting_to_change}:")
    else:
        # If callback_data does not start with "change_", pass control to other handlers
        await pnl_period_handler(update, context)

# Handle text input for new values and commands
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Проверяем доступ
    if not await check_access(update):
        return

    text = update.message.text
    setting = context.user_data.get('setting_to_change')

    if setting:
        try:
            # Check if the value needs to be converted to float
            if setting in ["ATR_MULTIPLIER_TP"]:
                new_value = float(text)  # Convert to float for decimal numbers
            else:
                new_value = int(text)  # Convert to int for other settings

            # Rewrite the value in config.py
            with open('config.py', 'r') as file:
                lines = file.readlines()
            with open('config.py', 'w') as file:
                for line in lines:
                    if line.startswith(f"{setting} ="):
                        file.write(f"{setting} = {new_value}\n")
                    else:
                        file.write(line)
            
            # Reload the config
            reload(config)
            await update.message.reply_text(f"Setting {setting} successfully changed to {new_value}.")
        except ValueError:
            await update.message.reply_text("Invalid value. Please enter a valid value.")
        context.user_data.pop('setting_to_change')
    else:
        if text == "⚙️ Settings":
            await show_config(update, context)
        elif text == "🚀 Start Trading":
            await start_bot(update, context)
        elif text == "🛑 Stop Trading":
            await stop_bot(update, context)
        elif text == "📊 PnL Analysis":
            await analyze_pnl(update, context)
        elif text == "🔍 Search Coins":
            await filter_coins(update, context)
        else:
            await update.message.reply_text("Use the available buttons to interact with the bot.")

# Start the bot
async def start_bot(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global bot_running, bot_process, chat_id

    # Проверяем доступ
    if not await check_access(update):
        return

    if not bot_running:
        if chat_id is None:
            await update.message.reply_text("❌ Error: chat_id not set. Please run the /start command.")
            return
        
        bot_running = True
        # Pass chat_id to bot_gc.py via command line arguments
        print(f"📌 Starting bot_gc.py with chat_id: {chat_id}")
        bot_process = subprocess.Popen(["/trading-bot-gc/venv/bin/python", "/trading-bot-gc/bot_gc.py", str(chat_id)])
        await update.message.reply_text("🚀 Trading bot started.")
        await update_keyboard(update, context)  # Update the keyboard
    else:
        await update.message.reply_text("Bot is already running.")

# Stop the bot
async def stop_bot(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global bot_running, bot_process

    # Проверяем доступ
    if not await check_access(update):
        return

    if bot_running:
        bot_running = False
        bot_process.terminate()
        bot_process = None
        await update.message.reply_text("🛑 Trading bot stopped.")
        await update_keyboard(update, context)  # Update the keyboard
    else:
        await update.message.reply_text("Bot is already stopped.")

# Filter coins
async def filter_coins(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Проверяем доступ
    if not await check_access(update):
        return

    await update.message.reply_text("Searching for the right coins... This may take a few seconds.")
    
    # Получаем отфильтрованные монеты из filter_gc
    top_symbols = get_filtered_symbols()
    
    if not top_symbols:
        await update.message.reply_text("Zero suitable coins found.")
        return
    
    # Получаем данные о тикерах для всех монет
    connector = get_exchange_connector()
    tickers = connector.get_ticker()
    tickers_df = pd.DataFrame(tickers)
    
    # Фильтруем данные для топ-5 монет
    filtered_data = []
    for symbol in top_symbols:
        ticker_info = tickers_df[tickers_df['symbol'] == symbol].iloc[0]
        filtered_data.append({
            'symbol': symbol,
            'volume': float(ticker_info['quoteVolume']),  # Объем в USDT
            'price_change': float(ticker_info['priceChangePercent'])  # Изменение цены в процентах
        })
    
    # Сортируем по абсолютному изменению цены (от большего к меньшему)
    filtered_data = sorted(filtered_data, key=lambda x: abs(x['price_change']), reverse=True)
    
    # Формируем сообщение с отфильтрованными монетами в формате таблицы
    message = "*Filtered Coins*\n\n"
    message += "```\n"
    message += "| Coin      | Volume ($) | Change (%) |\n"  # Уточняем заголовки
    message += "|-----------|------------|------------|\n"
    
    for coin_data in filtered_data:
        # Убираем "-USDT" из названия символа и обрезаем до 7 символов
        coin = coin_data['symbol'].replace("-USDT", "")
        if len(coin) > 7:
            coin = coin[:7] + "..."  # Обрезаем и добавляем многоточие
        
        # Форматируем объем в миллионах
        volume = coin_data['volume'] / 1_000_000  # Переводим в миллионы
        volume_str = f"{volume:.1f}M"  # Форматируем как "51.2M"
        
        # Форматируем изменение цены до десятых долей
        price_change = f"{coin_data['price_change']:.1f}%"  # Округляем до десятых
        
        # Форматируем строку с фиксированной шириной колонок
        coin_column = coin.ljust(10)  # Фиксированная ширина 10 символов
        volume_column = volume_str.rjust(10)  # Выравнивание по правому краю
        price_change_column = price_change.rjust(10)  # Выравнивание по правому краю
        
        message += f"| {coin_column} | {volume_column} | {price_change_column} |\n"
    
    message += "```"
    
    # Отправляем сообщение
    await update.message.reply_text(message, parse_mode="Markdown")

# PnL Analysis
async def analyze_pnl(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Проверяем доступ
    if not await check_access(update):
        return

    connector = get_exchange_connector()  # Use the correct function name

    # Keyboard for selecting the analysis period
    keyboard = [
        [InlineKeyboardButton("1 day", callback_data='pnl_1')],
        [InlineKeyboardButton("7 days", callback_data='pnl_7')],
        [InlineKeyboardButton("30 days", callback_data='pnl_30')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text("Select a period for PnL analysis:", reply_markup=reply_markup)

# Handle PnL period selection
async def pnl_period_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Проверяем доступ
    if not await check_access(update):
        return

    query = update.callback_query
    await query.answer()

    # Получаем количество дней из callback_data
    period = int(query.data.split('_')[1])
    connector = get_exchange_connector()

    # Получаем данные PnL за выбранный период
    pnl_data = connector.get_pnl(income_type="REALIZED_PNL", days=period)

    if pnl_data:
        total_pnl = 0.0  # Инициализируем суммарное значение PnL
        message = "📊 *PnL Analysis*\n\n"

        # Суммируем PnL
        for item in pnl_data:
            if item['incomeType'] == "REALIZED_PNL":
                total_pnl += float(item['income'])

        message += f"*Total PnL for {period} day(s):* `{total_pnl:.2f} USDT`\n\n"
        message += "```\n"
        message += "| Coin      | Date       | PnL (USDT) |\n"
        message += "|-----------|------------|------------|\n"

        for item in pnl_data:
            if item['incomeType'] == "REALIZED_PNL":
                # Конвертируем timestamp в читаемый формат (только дата)
                timestamp = int(item['time']) / 1000  # Конвертируем в секунды
                readable_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')  # Только дата
                
                # Убираем "-USDT" из названия символа и обрезаем до 10 символов
                symbol = item.get('symbol', 'N/A').replace("-USDT", "")
                if len(symbol) > 10:
                    symbol = symbol[:10] + "..."  # Обрезаем и добавляем многоточие
                
                # Сокращаем PnL до сотых
                pnl_value = float(item['income'])
                pnl_value_rounded = round(pnl_value, 2)
                
                # Форматируем строку с фиксированной шириной колонок
                coin_column = symbol.ljust(10)  # Фиксированная ширина 10 символов
                date_column = readable_time.ljust(10)  # Фиксированная ширина 10 символов
                pnl_column = f"{pnl_value_rounded:>10.2f}"  # Выравнивание по правому краю
                
                message += f"| {coin_column} | {date_column} | {pnl_column} |\n"

        message += "```"
    else:
        message = "No PnL data available for the selected period."

    # Отправляем сообщение
    await query.edit_message_text(text=message, parse_mode="Markdown")

# Main function to start the bot
async def main():
    try:
        global application
        application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

        application.add_handler(CommandHandler("start", start))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
        application.add_handler(CallbackQueryHandler(button_handler, pattern="^change_"))  # Handler for changing settings
        application.add_handler(CallbackQueryHandler(pnl_period_handler, pattern="^pnl_"))  # Handler for PnL analysis

        print("🔗 Telegram bot started...")
        await application.run_polling()
    except Exception as e:
        print(f"Error starting the bot: {e}")
        # Retry after 10 seconds
        await asyncio.sleep(10)
        await main()

if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except RuntimeError as e:
        print(f"Error in async loop: {e}")

import aiohttp

async def send_direct_message(text: str, chat_id: int):
    """
    Универсальная отправка сообщений в Telegram без зависимости от telegram.ext.
    Используется для отправки из внешних скриптов (например, bot_gc.py).
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=payload) as response:
                if response.status != 200:
                    error_text = await response.text()
                    print(f"[Telegram Error] Status {response.status}, Response: {error_text}")
    except Exception as e:
        print(f"[Telegram Exception] {e}")
