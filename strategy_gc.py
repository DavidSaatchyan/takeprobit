import pandas as pd
import numpy as np
from cex_connectors import get_exchange_connector
from datetime import datetime, timedelta, timezone
from config import SYMBOL, ATR_MULTIPLIER_TP, ATR_MULTIPLIER_SL  # Импортируем параметры из конфига

# Функция для расчета EMA
def calculate_ema(df, period):
    return df['close'].ewm(span=period, adjust=False).mean()

# Функция для расчета RSI
def calculate_rsi(df, period=14):
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/period, adjust=False).mean()  # Используем экспоненциальное скользящее среднее для роста
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/period, adjust=False).mean()  # Используем экспоненциальное скользящее среднее для снижения
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# Функция для расчета ATR
def calculate_atr(df, period=14):
    # Рассчитываем True Range (TR)
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())
    
    # True Range (TR) — это максимальное значение из трех вариантов
    true_range = np.maximum(high_low, high_close, low_close)
    
    # Первое значение ATR — это простое среднее за первые `period` значений TR
    atr = true_range.rolling(window=period, min_periods=1).mean()
    
    # Последующие значения ATR рассчитываются по формуле Уайлдера
    for i in range(period, len(df)):
        atr.iloc[i] = (atr.iloc[i-1] * (period - 1) + true_range.iloc[i]) / period
    
    return atr

# Функция для получения свечей
def fetch_candles(symbol, interval, connector, days=1):
    now = datetime.now(timezone.utc)
    end_time = int(now.timestamp() * 1000)  # Текущее время в миллисекундах
    start_time = int((now - timedelta(days=days)).timestamp() * 1000)  # Время начала в миллисекундах
    
    print(f"Запрос данных для {symbol} с {start_time} по {end_time}")
    
    candles = connector.get_candles(symbol, interval, start_time, end_time)
    
    if candles:
        df = pd.DataFrame(candles)
        
        # Сортируем данные по времени (от старых к новым)
        df = df.sort_values(by='time', ascending=True)
        
        # Проверка временных меток
        if df['time'].max() > end_time:
            print("Обнаружены некорректные временные метки. Исправляем...")
            # Исправляем временные метки, чтобы они соответствовали текущему времени
            df['time'] = pd.date_range(end=now, periods=len(df), freq=interval)
        
        df['timestamp'] = pd.to_datetime(df['time'], unit='ms')
        df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].apply(pd.to_numeric, errors='coerce')
        
        # Проверка данных
        print(f"Последние 5 свечей для {symbol}:")
        print(df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].tail())
        
        return df
    return pd.DataFrame()

# Функция для проверки сигналов входа
def check_entry_signal(df):
    # Рассчитываем индикаторы
    df['ema_9'] = calculate_ema(df, 9)
    df['ema_21'] = calculate_ema(df, 21)
    df['rsi'] = calculate_rsi(df, 14)
    df['atr'] = calculate_atr(df, 14)
    
    # Последние значения индикаторов
    ema_9 = df['ema_9'].iloc[-1]
    ema_21 = df['ema_21'].iloc[-1]
    rsi = df['rsi'].iloc[-1]
    atr = df['atr'].iloc[-1]
    
    # Отладочные сообщения
    print(f"Последние значения индикаторов:")
    print(f"- EMA 9: {ema_9}")
    print(f"- EMA 21: {ema_21}")
    print(f"- RSI: {rsi}")
    print(f"- ATR: {atr}")
    
    # Правила входа
    if ema_9 > ema_21 and df['ema_9'].iloc[-2] <= df['ema_21'].iloc[-2]:  # Золотой крест
        if 50 < rsi < 70:  # RSI выше 50, но не в зоне перекупленности
            print("Найден сигнал на покупку (бычий ордер блок).")
            return 'buy', atr
    elif ema_9 < ema_21 and df['ema_9'].iloc[-2] >= df['ema_21'].iloc[-2]:  # Крест смерти
        if 30 < rsi < 50:  # RSI ниже 50, но не в зоне перепроданности
            print("Найден сигнал на продажу (медвежий ордер блок).")
            return 'sell', atr
    
    # Если сигнал не найден, возвращаем None
    print("Сигнал для входа не найден.")
    return None, None

# Функция для расчета точек выхода (take profit и stop loss)
def calculate_exit_points(entry_price, atr, direction):
    if direction == 'buy':
        take_profit = entry_price + ATR_MULTIPLIER_TP * atr  # Используем ATR_MULTIPLIER_TP из конфига
        stop_loss = entry_price - ATR_MULTIPLIER_SL * atr  # Используем ATR_MULTIPLIER_SL из конфига
    elif direction == 'sell':
        take_profit = entry_price - ATR_MULTIPLIER_TP * atr
        stop_loss = entry_price + ATR_MULTIPLIER_SL * atr
    else:
        raise ValueError("Неправильное направление сделки.")
    
    print(f"Рассчитаны точки выхода: Take Profit = {take_profit}, Stop Loss = {stop_loss}")
    return take_profit, stop_loss

# Основная функция стратегии
def analyze_and_generate_setup(interval='5m'):
    connector = get_exchange_connector()
    df = fetch_candles(SYMBOL, interval, connector, days=1)  # Используем SYMBOL из конфига
    
    if df.empty:
        print("Нет данных для анализа.")
        return None
    
    signal, atr = check_entry_signal(df)
    
    if signal:
        # Получаем последнюю цену из тикера
        entry_price = connector.get_last_price(SYMBOL)
        if entry_price is None or entry_price == 0:
            print("Не удалось получить последнюю цену. Прерываем выполнение.")
            return None
        
        take_profit, stop_loss = calculate_exit_points(entry_price, atr, signal)
        
        setup = {
            'symbol': SYMBOL,  # Используем SYMBOL из конфига
            'direction': signal,
            'entry_price': entry_price,
            'take_profit': take_profit,
            'stop_loss': stop_loss,
            'timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        }
        
        print(f"Торговый сетап для {SYMBOL}:")  # Используем SYMBOL из конфига
        print(f"- Направление: {setup['direction']}")
        print(f"- Точка входа: {setup['entry_price']}")
        print(f"- Take Profit: {setup['take_profit']}")
        print(f"- Stop Loss: {setup['stop_loss']}")
        print(f"- Время: {setup['timestamp']}")
        
        return setup
    else:
        # Если сигнал не найден, просто возвращаем None
        return None

# Запуск стратегии
if __name__ == "__main__":
    analyze_and_generate_setup(interval='5m')