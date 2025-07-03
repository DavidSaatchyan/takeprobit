import pandas as pd
import numpy as np
from typing import Tuple

# Функция для расчета EMA
def calculate_ema(df, period):
    """
    Рассчитывает экспоненциальное скользящее среднее (EMA) для указанного периода.
    """
    return df['close'].ewm(span=period, adjust=False).mean()

# Функция для расчета Clever Algo V2 индикатора
def calculate_clever_algo_v2(df: pd.DataFrame,
                             length: int = 50,
                             volatility_mult: float = 1.5,
                             loop_start: int = 1,
                             loop_end: int = 70,
                             threshold_up: int = 5,
                             threshold_down: int = -5) -> pd.DataFrame:
    df = df.copy()
    close = df['close']
    high = df['high']
    low = df['low']

    # === 0. RMA helper ===
    def rma(series: pd.Series, length: int) -> pd.Series:
        alpha = 1 / length
        return series.ewm(alpha=alpha, adjust=False).mean()

    # === 1. Zero Lag Basis ===
    lag = (length - 1) // 2
    raw_zl = close + (close - close.shift(lag))
    df['zl_basis'] = raw_zl.ewm(span=length, adjust=True).mean()

    # === 2. Volatility ===
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)
    atr = rma(tr, length)
    df['volatility'] = atr.rolling(window=length * 3).max() * volatility_mult

    # === 3. Score Calculation ===
    zl = df['zl_basis'].values
    score = np.zeros(len(df))
    for i in range(len(df)):
        for j in range(loop_start, loop_end + 1):
            if i - j >= 0:
                score[i] += 1 if zl[i] > zl[i - j] else -1
    df['score'] = score

    # === 4. Signal Conditions ===
    df['long_signal'] = (df['score'] > threshold_up) & (close > df['zl_basis'] + df['volatility'].shift(1))
    df['short_signal'] = (df['score'] < threshold_down) & (close < df['zl_basis'] - df['volatility'].shift(1))

    # === 5. Trend Logic (Pine Script style) ===
    trend = []
    trend_changed = []
    prev = 0
    for i in range(len(df)):
        curr = prev
        if df['long_signal'].iloc[i]:
            curr = 1
        elif df['short_signal'].iloc[i]:
            curr = -1
        trend_changed.append(curr != prev)
        trend.append(curr)
        prev = curr

    df['trend'] = trend
    df['trend_changed'] = trend_changed

    return df

# Функция для расчета ATR (Average True Range)
def calculate_atr(df, period=14):
    """
    Рассчитывает Average True Range (ATR) для указанного периода.
    """
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

# Функция для расчета NATR (Normalized ATR)
def calculate_natr(df, period=14):
    """
    Рассчитывает Normalized ATR (NATR) для указанного периода.
    NATR выражается в процентах и показывает волатильность относительно цены.
    """
    atr = calculate_atr(df, period)
    natr = (atr / df['close']) * 100  # NATR в процентах
    return natr


# Функция для расчета VWAP (Volume Weighted Average Price)
def calculate_vwap(df, session_start_hour=16):
    """
    Рассчитывает VWAP начиная с указанного часа (по UTC).
    Использует максимум, минимум и закрытие для расчета типичной цены.
    """
    # Преобразуем время в UTC и фильтруем данные с начала сессии
    df['timestamp'] = pd.to_datetime(df['time'], unit='ms')
    df['hour'] = df['timestamp'].dt.hour
    df['date'] = df['timestamp'].dt.date  # Добавляем колонку с датой
    
    # Определяем начало сессии
    df['is_new_session'] = (df['hour'] == session_start_hour) & (df['hour'].shift(1) != session_start_hour)
    
    # Рассчитываем кумулятивные значения для каждой сессии
    df['session_id'] = df['is_new_session'].cumsum()
    
    # Рассчитываем типичную цену
    df['typical_price'] = (df['high'] + df['low'] + df['close']) / 3
    
    # Рассчитываем кумулятивный объем
    df['cumulative_volume'] = df.groupby('session_id')['volume'].cumsum()
    
    # Рассчитываем кумулятивную цену * объем для каждой сессии
    df['cumulative_typical_price_volume'] = (df['typical_price'] * df['volume']).groupby(df['session_id']).cumsum()
    
    # Рассчитываем VWAP для каждой сессии
    df['vwap'] = df['cumulative_typical_price_volume'] / df['cumulative_volume']
    
    # Возвращаем последнее значение VWAP, округленное до 4 знаков после запятой
    return round(df['vwap'].iloc[-1], 4)

def calculate_ma(source, length, ma_type="SMA"):
    """
    Рассчитывает скользящее среднее (MA) в зависимости от типа.
    Поддерживает SMA, EMA, RMA (SMMA).
    """
    if ma_type == "SMA":
        return source.rolling(window=length, min_periods=1).mean()
    elif ma_type == "EMA":
        return source.ewm(span=length, adjust=False).mean()
    elif ma_type == "RMA":
        return source.ewm(alpha=1/length, min_periods=length, adjust=False).mean()
    else:
        raise ValueError(f"Неизвестный тип скользящей средней: {ma_type}")

def calculate_bollinger_bands(df, period=20, std_dev=2, ma_type="SMA"):
    """
    Рассчитывает полосы Боллинджера.
    Возвращает верхнюю, среднюю (MA) и нижнюю полосы.
    """
    df['basis'] = calculate_ma(df['close'], period, ma_type)
    df['std'] = df['close'].rolling(window=period).std()
    df['upper_band'] = df['basis'] + (df['std'] * std_dev)
    df['lower_band'] = df['basis'] - (df['std'] * std_dev)
    return df['upper_band'], df['basis'], df['lower_band']


def calculate_dynamic_volume_threshold(df, lookback_hours=0.7, k=0.5, num_candles_to_sum=3):
    """
    Рассчитывает динамический порог объема на основе исторических данных.
    Параметры:
    - df: DataFrame с данными (должен содержать колонку 'volume').
    - lookback_hours: Количество часов для анализа (по умолчанию 0.7 часа).
    - k: Коэффициент для расчета порога (по умолчанию 0.5).
    - num_candles_to_sum: Количество свечей, объем которых суммируется (по умолчанию 3).
    Возвращает:
    - dynamic_threshold: Динамический порог объема.
    """
    # Количество свечей в указанном периоде (например, 60 свечей в час для 1-минутного таймфрейма)
    num_candles = int(lookback_hours * 60)  # Округляем до целого числа
    
    # Убедимся, что у нас достаточно данных
    if len(df) < num_candles:
        print(f"Недостаточно данных для расчета динамического порога объема. Требуется {num_candles} свечей, доступно {len(df)}.")
        return None
    
    # Берем последние `num_candles` свечей
    volume_values = df['volume'].tail(num_candles)
    
    # Рассчитываем суммарный объем для каждых `num_candles_to_sum` свечей
    summed_volumes = volume_values.rolling(window=num_candles_to_sum).sum().dropna()
    
    # Рассчитываем среднее значение и стандартное отклонение суммарного объема
    mean_summed_volume = summed_volumes.mean()
    std_summed_volume = summed_volumes.std()
    
    # Динамический порог: среднее + k * стандартное отклонение
    dynamic_threshold = mean_summed_volume + k * std_summed_volume
    
    return dynamic_threshold

# Функция для расчета RSI
def calculate_rsi(df, period=14):
    """
    Рассчитывает Relative Strength Index (RSI) для указанного периода.
    """
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# Функция для расчета RMA (Rolling Moving Average)
def calculate_rma(series, period):
    """
    Рассчитывает Rolling Moving Average (RMA) для указанного периода.
    RMA — это экспоненциальное скользящее среднее (EMA) с альфой = 1 / period.
    """
    return series.ewm(alpha=1/period, min_periods=period, adjust=False).mean()

# Функция для расчета ADX (Average Directional Index)
def calculate_adx(df, di_length=7, adx_smoothing=7):
    """
    Рассчитывает Average Directional Index (ADX) для указанных периодов.
    Адаптировано под расчет TradingView.
    """
    # Рассчитываем +DM и -DM
    df['up'] = df['high'].diff()
    df['down'] = -df['low'].diff()
    
    df['plus_dm'] = np.where((df['up'] > df['down']) & (df['up'] > 0), df['up'], 0)
    df['minus_dm'] = np.where((df['down'] > df['up']) & (df['down'] > 0), df['down'], 0)
    
    # Рассчитываем True Range (TR)
    df['tr'] = np.maximum(df['high'] - df['low'], np.abs(df['high'] - df['close'].shift()), np.abs(df['low'] - df['close'].shift()))
    
    # Сглаживаем +DM, -DM и TR с использованием RMA
    df['plus_dm_smooth'] = calculate_rma(df['plus_dm'], di_length)
    df['minus_dm_smooth'] = calculate_rma(df['minus_dm'], di_length)
    df['tr_smooth'] = calculate_rma(df['tr'], di_length)
    
    # Рассчитываем +DI и -DI
    df['plus_di'] = 100 * df['plus_dm_smooth'] / df['tr_smooth']
    df['minus_di'] = 100 * df['minus_dm_smooth'] / df['tr_smooth']
    
    # Рассчитываем DX
    sum_di = df['plus_di'] + df['minus_di']
    df['dx'] = 100 * np.abs(df['plus_di'] - df['minus_di']) / np.where(sum_di == 0, 1, sum_di)
    
    # Рассчитываем ADX как сглаженный DX с использованием RMA
    adx = calculate_rma(df['dx'], adx_smoothing)
    
    return adx

def calculate_trend_regression_x(df, len_1=8, len_2=21, len_3=34, len_reg=13):
    """
    Рассчитывает индикатор Trend Regression X и определяет тип тренда (bullish/bearish).
    
    Параметры:
    - df: DataFrame с данными (должен содержать колонку 'close').
    - len_1: Период для первой скользящей средней (по умолчанию 8).
    - len_2: Период для второй скользящей средней (по умолчанию 21).
    - len_3: Период для третьей скользящей средней (по умолчанию 34).
    - len_reg: Период для линейной регрессии (по умолчанию 13).
    
    Возвращает:
    - trend_type: Тип тренда ('bullish' или 'bearish').
    """
    
    # Функция для расчета линейной регрессии
    def f_reg(length, y, x):
        x_ = x.ewm(span=length, adjust=False).mean()  # EMA для x
        y_ = y.ewm(span=length, adjust=False).mean()  # EMA для y
        mx = x.rolling(window=length).std()  # Стандартное отклонение для x
        my = y.rolling(window=length).std()  # Стандартное отклонение для y
        c = x.rolling(window=length).corr(y)  # Корреляция между x и y
        slope = c * (my / mx)  # Наклон регрессии
        inter = y_ - slope * x_  # Пересечение
        reg = x * slope + inter  # Линейная регрессия
        return reg
    
    # Рассчитываем индексы баров (аналог bar_index в TradingView)
    df['bar_index'] = np.arange(len(df))
    
    # Рассчитываем регрессию для каждого из трех периодов
    a1_ = f_reg(len_1, df['close'], df['bar_index'])
    a2 = f_reg(len_2, df['close'], df['bar_index'])
    a3 = f_reg(len_3, df['close'], df['bar_index'])
    
    # Усредняем три регрессии
    avg_reg = (a1_ + a2 + a3) / 3
    
    # Рассчитываем итоговую регрессию
    trend_reg = f_reg(len_reg, avg_reg, df['bar_index'])
    
    # Определяем тип тренда
    if trend_reg.iloc[-1] > trend_reg.iloc[-2]:  # Текущее значение больше предыдущего
        trend_type = 'bullish'
    else:  # Текущее значение меньше или равно предыдущему
        trend_type = 'bearish'
    
    return trend_type



# Функция для расчета динамического порога NATR
def calculate_dynamic_natr_threshold(df, period=14, lookback_hours=2, k=0.5):
    """
    Рассчитывает динамический порог NATR на основе исторических данных.
    """
    df['natr'] = calculate_natr(df, period)
    natr_values = df['natr'].tail(lookback_hours * 12)  # 12 свечей в час для 5-минутного таймфрейма
    mean_natr = natr_values.mean()
    std_natr = natr_values.std()
    dynamic_threshold = mean_natr - k * std_natr
    return dynamic_threshold

# Функция для расчета кредитного плеча на основе NATR
def calculate_leverage(natr):
    """
    Ручное равномерное распределение плеча в зависимости от NATR
    От 25 до 2
    """
    if natr < 0.05:
        return 25
    elif natr < 0.07:
        return 23
    elif natr < 0.10:
        return 21
    elif natr < 0.13:
        return 19
    elif natr < 0.16:
        return 17
    elif natr < 0.20:
        return 15
    elif natr < 0.25:
        return 13
    elif natr < 0.30:
        return 11
    elif natr < 0.40:
        return 9
    elif natr < 0.55:
        return 7
    elif natr < 0.75:
        return 5
    elif natr < 0.95:
        return 3
    else:
        return 2

