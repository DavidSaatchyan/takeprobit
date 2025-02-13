import os
from dotenv import load_dotenv

load_dotenv()

# Таймфрейм для анализа свечей
TIMEFRAME = '5m'

# Параметры фильтрации активов
PRICE_CHANGE_THRESHOLD = 10
VOLUME_THRESHOLD = 10_000_000

# Настройки плеча 
LEVERAGE_SETTINGS = 40

# Настройка маржи для сделок
MARGIN = 20  # Размер маржи в USDT

# Выбор биржи для торговли
EXCHANGE = 'BingX'  # Возможные значения: 'MEXC', 'BingX'

# Загрузка ключей в зависимости от выбранной биржи
if EXCHANGE == 'MEXC':
    API_KEY = os.getenv('MEXC_API_KEY')
    SECRET_KEY = os.getenv('MEXC_SECRET_KEY')
elif EXCHANGE == 'BingX':
    API_KEY = os.getenv('BINGX_API_KEY')
    SECRET_KEY = os.getenv('BINGX_SECRET_KEY')
else:
    raise ValueError(f"Неподдерживаемая биржа: {EXCHANGE}")

# Проверка наличия ключей
if not API_KEY or not SECRET_KEY:
    raise EnvironmentError(f"API ключи для {EXCHANGE} не найдены. Проверьте .env файл.")

# Параметры бота
SYMBOL = 'BTC-USDT'  # Торгуемый инструмент
ATR_MULTIPLIER_TP = 1.5  # Мультипликатор ATR для Take Profit
ATR_MULTIPLIER_SL = 1.0  # Мультипликатор ATR для Stop Loss