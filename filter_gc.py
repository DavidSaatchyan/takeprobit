import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from cex_connectors import get_exchange_connector
import time
import logging
from typing import List, Optional, Tuple

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SymbolFilter:
    def __init__(self):
        self.connector = get_exchange_connector()
        self.symbols_cache = []
        self.last_update_time = 0
        self.update_interval = 3600  # 2 часа в секундах
    
    def get_filtered_symbols(self) -> List[str]:
        """
        Возвращает отфильтрованный список символов.
        Обновляет список только если прошло больше 2 часов с последнего обновления.
        """
        current_time = time.time()
        
        # Проверяем, нужно ли обновить список
        if (current_time - self.last_update_time) >= self.update_interval or not self.symbols_cache:
            logger.info("Обновляем список символов...")
            self.symbols_cache = self._update_symbols_list()
            self.last_update_time = current_time
            logger.info(f"Обновленный список символов: {self.symbols_cache}")
        else:
            time_until_update = self.update_interval - (current_time - self.last_update_time)
            logger.debug(f"Используем кэшированный список. Обновление через {time_until_update/3600:.1f} часов")
        
        return self.symbols_cache
    
    def _update_symbols_list(self) -> List[str]:
        """
        Обновляет список символов согласно критериям:
        1. Топ-20 по объему за сутки
        2. Изменение цены за сутки в диапазоне [-10%, -2%] или [+3%, +10%]
        Унифицировано для Binance (symbol: BTCUSDT, quoteVolume, priceChangePercent)
        """
        try:
            # Получаем все тикеры
            tickers = self.connector.get_ticker()
            if not tickers or not isinstance(tickers, list):
                logger.error("Не удалось получить данные тикеров или неверный формат")
                return []

            # Для Binance тикеры — список словарей с ключами: symbol, quoteVolume, priceChangePercent
            df = pd.DataFrame(tickers)

            # Проверяем наличие необходимых колонок
            required_columns = ['symbol', 'quoteVolume', 'priceChangePercent']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"Отсутствуют необходимые колонки: {missing_columns}")
                logger.info(f"Доступные колонки: {list(df.columns)}")
                return []

            # Фильтруем только пары с USDT (Binance: BTCUSDT, ETHUSDT и т.д.)
            df = df[df['symbol'].str.endswith('USDT')]

            # Исключаем BTCUSDT и ETHUSDT на всех этапах
            df = df[~df['symbol'].isin(['BTCUSDT', 'ETHUSDT', 'BTC-USDT', 'ETH-USDT'])]

            # Приводим к числовым типам
            df['quoteVolume'] = pd.to_numeric(df['quoteVolume'], errors='coerce')
            df['priceChangePercent'] = pd.to_numeric(df['priceChangePercent'], errors='coerce')

            # Удаляем строки с NaN
            df = df.dropna(subset=['quoteVolume', 'priceChangePercent'])

            # Отбираем топ-20 по объему в USDT
            top_n = 22  # 20 + запас
            top_by_volume = df.nlargest(top_n, 'quoteVolume')
            top_20_by_volume = top_by_volume.head(20)
            logger.info(f"Топ-20 по объему (без BTCUSDT и ETHUSDT): {top_20_by_volume[['symbol', 'quoteVolume', 'priceChangePercent']].to_dict('records')}")

            # Фильтруем по изменению цены
            # Диапазон 1: [-12%, -3%] — падение
            falling_symbols = top_20_by_volume[
                (top_20_by_volume['priceChangePercent'] >= -12) & 
                (top_20_by_volume['priceChangePercent'] < -3)
            ]
            # Диапазон 2: [+3%, +12%] — рост
            rising_symbols = top_20_by_volume[
                (top_20_by_volume['priceChangePercent'] >= 3) & 
                (top_20_by_volume['priceChangePercent'] <= 12)
            ]
            # Объединяем результаты
            filtered_symbols = pd.concat([falling_symbols, rising_symbols])
            # Сортируем по объему в USDT и берем топ-10
            result_symbols = filtered_symbols.nlargest(10, 'quoteVolume')['symbol'].tolist()
            # Финальная фильтрация (на всякий случай)
            result_symbols = [s for s in result_symbols if s not in ['BTCUSDT', 'ETHUSDT', 'BTC-USDT', 'ETH-USDT']]
            logger.info(f"Отфильтрованные символы (финальный список): {result_symbols}")
            logger.info(f"Статистика фильтрации:")
            logger.info(f"  - Всего символов с USDT: {len(df)}")
            logger.info(f"  - Топ-20 по объему: {len(top_20_by_volume)}")
            logger.info(f"  - Падающие символы [-12%, -3%]: {len(falling_symbols)}")
            logger.info(f"  - Растущие символы [+3%, +12%]: {len(rising_symbols)}")
            logger.info(f"  - Итого отфильтровано: {len(result_symbols)}")
            return result_symbols
        except Exception as e:
            logger.error(f"Ошибка при обновлении списка символов: {str(e)}", exc_info=True)
            return []
    
    def force_update(self) -> List[str]:
        """
        Принудительно обновляет список символов
        """
        self.last_update_time = 0
        return self.get_filtered_symbols()

# Глобальный экземпляр фильтра
symbol_filter = SymbolFilter()

def get_filtered_symbols() -> List[str]:
    """
    Основная функция для получения отфильтрованного списка символов
    """
    return symbol_filter.get_filtered_symbols()

def force_update_symbols() -> List[str]:
    """
    Принудительное обновление списка символов
    """
    return symbol_filter.force_update()

def update_symbols_once() -> List[str]:
    """
    Принудительно обновляет и возвращает список символов (без кэша, всегда свежий запрос).
    Не влияет на кэш и last_update_time.
    """
    return symbol_filter._update_symbols_list()

# Для тестирования
if __name__ == "__main__":
    print("Тестирование фильтра символов...")
    symbols = get_filtered_symbols()
    print(f"Отфильтрованные символы: {symbols}")
    
    # Тест принудительного обновления
    print("\nПринудительное обновление...")
    symbols_forced = force_update_symbols()
    print(f"Обновленные символы: {symbols_forced}")