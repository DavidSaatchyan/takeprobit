import os
from dotenv import load_dotenv

load_dotenv()

# Таймфрейм для анализа свечей
TIMEFRAME = '1m'

# Параметры фильтрации активов
PRICE_CHANGE_THRESHOLD = 10
VOLUME_THRESHOLD = 10_000_000

# Настройка маржи для сделок
MARGIN = 8

# Лимит времени на исполнение ордера (в секундах)
EXECUTION_TIME_LIMIT = 330  # 3 минуты

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
ATR_MULTIPLIER_TP = 4.0

# Минимальное время с момента последнего пересечения EMA 
MIN_EMA_CROSS_TIME = 4.5  