import os
from dotenv import load_dotenv

# Грузим .env всегда при импорте config.py
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

# Таймфрейм для анализа свечей
TIMEFRAME = '1m'

# Настройка маржи для сделок
MARGIN = 100

# Лимит времени на исполнение ордера (в секундах)
EXECUTION_TIME_LIMIT = 480  # 8 минут

# Выбор биржи для торговли
EXCHANGE = os.getenv('EXCHANGE', 'Binance')  # Возможные значения: 'MEXC', 'BingX', 'Binance'

# Загрузка ключей в зависимости от выбранной биржи
if EXCHANGE == 'MEXC':
    API_KEY = os.getenv('MEXC_API_KEY')
    SECRET_KEY = os.getenv('MEXC_SECRET_KEY')
elif EXCHANGE == 'BingX':
    API_KEY = os.getenv('BINGX_API_KEY')
    SECRET_KEY = os.getenv('BINGX_SECRET_KEY')
elif EXCHANGE == 'Binance':
    API_KEY = os.getenv('BINANCE_API_KEY')
    SECRET_KEY = os.getenv('BINANCE_SECRET_KEY')
else:
    raise ValueError(f'Неподдерживаемая биржа: {EXCHANGE}')

if not API_KEY or not SECRET_KEY:
    raise EnvironmentError(f'API ключи для {EXCHANGE} не найдены. Проверьте .env файл.')

# Параметры бота
ATR_MULTIPLIER_TP = 3.0

# Минимальное время с момента последнего пересечения EMA 
MIN_EMA_CROSS_TIME = 4.5  