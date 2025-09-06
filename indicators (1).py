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
    lag = int(np.floor((length - 1) / 2))
    zl_basis = (close + (close - close.shift(lag))).ewm(span=length, adjust=False).mean()
    df['zl_basis'] = zl_basis

    # === 2. Volatility (точно как в Pine Script) ===
    atr = rma(pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1), length)
    df['atr'] = atr
    volatility = atr.rolling(window=length * 3, min_periods=1).max() * volatility_mult
    df['volatility'] = volatility

    # === 3. Score Calculation (один в один с Pine Script) ===
    score = np.zeros(len(df))
    zl = df['zl_basis'].values
    for i in range(len(df)):
        s = 0
        for j in range(loop_start, loop_end + 1):
            if i - j >= 0:
                s += 1 if zl[i] > zl[i - j] else -1
        score[i] = s
    df['score'] = score

    # === 4. Signal Conditions ===
    df['long_signal'] = (df['score'] > threshold_up) & (close > df['zl_basis'] + df['volatility'])
    df['short_signal'] = (df['score'] < threshold_down) & (close < df['zl_basis'] - df['volatility'])

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

    # === DEBUG: Выводим ключевые параметры только для последнего бара ===
    # Сравнивайте значения: time, close, zl_basis, volatility, score, long_signal, short_signal, trend
    print('DEBUG: Последний бар Clever Algo:')
    debug_cols = ['time', 'close', 'zl_basis', 'volatility', 'score', 'long_signal', 'short_signal', 'trend']
    print(df[debug_cols].tail(1).to_string(index=False))

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
        prev_atr = atr.iloc[i-1]
        current_tr = true_range.iloc[i]
        
        # Проверяем на NaN
        if pd.notna(prev_atr) and pd.notna(current_tr):
            atr.iloc[i] = (prev_atr * (period - 1) + current_tr) / period
    
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

class OrderBlock:
    """Класс для представления order block'а в стиле LuxAlgo"""
    def __init__(self, high, low, bar_time, bias, impulse_index, strength=1):
        self.high = high
        self.low = low
        self.bar_time = bar_time
        self.bias = bias  # BULLISH или BEARISH
        self.impulse_index = impulse_index
        self.strength = strength
        self.mitigated = False
        self.mitigation_time = None

class MarketStructure:
    """Класс для отслеживания внутренней структуры рынка (как в LuxAlgo)"""
    def __init__(self):
        self.internal_high = None
        self.internal_low = None
        self.internal_trend = 0  # 0 = нет тренда, 1 = bullish, -1 = bearish
        self.last_bos_choch_time = None
        
    def update_structure(self, df, current_index):
        """Обновляет внутреннюю структуру (5-барная)"""
        if current_index < 5:
            return False, None
            
        # Определяем текущий leg (как в LuxAlgo)
        high_5 = df['high'].iloc[current_index-5:current_index].max()
        low_5 = df['low'].iloc[current_index-5:current_index].min()
        
        current_high = df['high'].iloc[current_index]
        current_low = df['low'].iloc[current_index]
        
        # Определяем новый leg
        new_leg = None
        if current_high > high_5:
            new_leg = 'bearish'  # новый максимум
        elif current_low < low_5:
            new_leg = 'bullish'  # новый минимум
            
        if new_leg is None:
            return False, None
            
        # Определяем BOS/CHoCH
        structure_changed = False
        structure_type = None
        
        if new_leg == 'bullish':
            if self.internal_trend <= 0:  # был bearish или нет тренда
                if self.internal_trend == -1:
                    structure_type = 'CHoCH'  # Change of Character
                else:
                    structure_type = 'BOS'    # Break of Structure
                self.internal_trend = 1
                structure_changed = True
        else:  # bearish
            if self.internal_trend >= 0:  # был bullish или нет тренда
                if self.internal_trend == 1:
                    structure_type = 'CHoCH'  # Change of Character
                else:
                    structure_type = 'BOS'    # Break of Structure
                self.internal_trend = -1
                structure_changed = True
                
        if structure_changed:
            self.last_bos_choch_time = df['time'].iloc[current_index]
            return True, structure_type
            
        return False, None

# Удаляем устаревшую функцию поиска order block'ов через консолидацию/импульс

def check_order_block_filter(df, signal_direction, lookback_periods=420, 
                           max_blocks=5, min_block_strength=1, block_distance_threshold=2.5, 
                           summary_only=True):
    """
    Проверяет, есть ли рядом с текущей ценой order block, который может помешать сигналу.
    Учитывает расстояние до блока: если блок дальше чем block_distance_threshold * ATR, он игнорируется.
    """
    if len(df) < lookback_periods:
        return True, "Недостаточно данных для анализа order block'ов"
    
    # Заменяем вызовы на новую функцию LuxAlgo-style
    bullish_blocks, bearish_blocks = find_internal_order_blocks_luxalgo(
        df,
        max_blocks=max_blocks,
        mitigation_mode='highlow',
        atr_period=200,
        high_volatility_mult=2.0,
        verbose=False
    )
    
    atr = calculate_atr(df, period=14).iloc[-1]
    current_price = df['close'].iloc[-1]
    max_distance = block_distance_threshold * atr
    
    # Краткое описание блоков
    bull_desc = ", ".join([f"{b['low']:.4f}-{b['high']:.4f}" for b in bullish_blocks]) or "нет"
    bear_desc = ", ".join([f"{b['low']:.4f}-{b['high']:.4f}" for b in bearish_blocks]) or "нет"
    
    result = True
    reason = "Нет order block'ов рядом"
    
    if signal_direction == 'buy':
        for block in bearish_blocks:
            # Проверяем расстояние до блока
            distance_to_block = block['low'] - current_price
            if distance_to_block > max_distance:
                # Блок слишком далеко, игнорируем
                continue
                
            # Если цена внутри блока — блокируем
            if block['low'] <= current_price <= block['high']:
                result = False
                reason = f"Цена внутри медвежьего блока: {block['low']:.4f}-{block['high']:.4f}"
                break
            # Блок выше цены и в пределах допустимого расстояния — блокируем сигнал
            elif block['low'] > current_price:
                result = False
                reason = f"Медвежий блок выше цены: {block['low']:.4f}-{block['high']:.4f}. Расстояние до блока: {distance_to_block:.4f}, ATR: {atr:.4f}, макс. расстояние: {max_distance:.4f}"
                break
    elif signal_direction == 'sell':
        for block in bullish_blocks:
            # Проверяем расстояние до блока
            distance_to_block = current_price - block['high']
            if distance_to_block > max_distance:
                # Блок слишком далеко, игнорируем
                continue
                
            # Если цена внутри блока — блокируем
            if block['low'] <= current_price <= block['high']:
                result = False
                reason = f"Цена внутри бычьего блока: {block['low']:.4f}-{block['high']:.4f}"
                break
            # Блок ниже цены и в пределах допустимого расстояния — блокируем сигнал
            elif block['high'] < current_price:
                result = False
                reason = f"Бычий блок ниже цены: {block['low']:.4f}-{block['high']:.4f}. Расстояние до блока: {distance_to_block:.4f}, ATR: {atr:.4f}, макс. расстояние: {max_distance:.4f}"
                break
    
    if summary_only:
        print("\n===== КРАТКОЕ РЕЗЮМЕ ПО ORDER BLOCK'АМ =====")
        print(f"Медвежьи блоки (сопротивление): {bear_desc}")
        print(f"Бычьи блоки (поддержка): {bull_desc}")
        print(f"Сигнал на покупку: {'ПРОШЕЛ' if signal_direction == 'buy' and result else 'НЕ ПРОШЕЛ' if signal_direction == 'buy' else '-'}")
        print(f"Сигнал на продажу: {'ПРОШЕЛ' if signal_direction == 'sell' and result else 'НЕ ПРОШЕЛ' if signal_direction == 'sell' else '-'}")
        print(f"Причина: {reason}")
    
    return result, reason

def get_order_blocks_summary(df, lookback_periods=420, max_blocks=5, min_block_strength=1):
    """
    Возвращает краткую сводку найденных order block'ов для логирования.
    Использует новую LuxAlgo-совместимую логику.
    """
    # Заменяем вызовы на новую функцию LuxAlgo-style
    bullish_blocks, bearish_blocks = find_internal_order_blocks_luxalgo(
        df,
        max_blocks=max_blocks,
        mitigation_mode='highlow',
        atr_period=200,
        high_volatility_mult=2.0,
        verbose=False
    )
    
    summary = []
    
    if bullish_blocks:
        bullish_str = ", ".join([f"Поддержка {b['low']:.4f} (сила: {b['strength']})" for b in bullish_blocks])
        summary.append(f"Бычьи блоки (поддержка): {bullish_str}")
    
    if bearish_blocks:
        bearish_str = ", ".join([f"Сопротивление {b['high']:.4f} (сила: {b['strength']})" for b in bearish_blocks])
        summary.append(f"Медвежьи блоки (сопротивление): {bearish_str}")
    
    if not summary:
        summary.append("Order block'ы не найдены")
    
    return " | ".join(summary)

# Оставляем старые функции для обратной совместимости, но помечаем как deprecated
def find_order_blocks(df, lookback_periods=420, consolidation_candles=2, impulse_threshold=1.1, 
                     volume_threshold=None, min_block_size=0.0, max_block_size=5.0, verbose=True, max_blocks=5, min_block_strength=1):
    """
    DEPRECATED: Используйте find_order_blocks_luxalgo для LuxAlgo-совместимой логики
    """
    # Удаляем устаревшие предупреждения и комментарии
    pass


# === LuxAlgo-style Internal Order Blocks (максимально идентично Pine Script) ===
def find_internal_order_blocks_luxalgo(
    df,
    max_blocks=5,
    mitigation_mode='highlow',
    swing_lookback=5,
    high_volatility_mult=2.0,
    order_block_filter='ATR',  # 'ATR' или 'RANGE'
    confluence_min_range_mult=1.0,  # минимальный диапазон BOS/CHoCH в ATR/Range для фильтра
    atr_period=200,
    verbose=True,
    max_bars_between_swing_and_bos=15  # LuxAlgo-style: не более 15 баров между swing и BOS/CHoCH
):
    """
    LuxAlgo-style Internal Order Blocks (по Pine Script):
    - parsedHigh/parsedLow с учётом highVolatilityBar (2*ATR или 2*Range)
    - Строит блоки по BOS/CHoCH с parsedHigh/parsedLow
    - Митигация по parsedHigh/parsedLow
    - Фильтрация по ATR/Range (order_block_filter)
    - Confluence filter: минимальный диапазон BOS/CHoCH
    - Ограничение: не более 15 баров между swing и BOS/CHoCH (как в LuxAlgo)
    Возвращает: (bullish_blocks, bearish_blocks)
    """
    if len(df) < swing_lookback * 2 + 2:
        if verbose:
            print("Недостаточно данных для поиска order block (нужно больше свечей)")
        return [], []
    df = df.copy().reset_index(drop=True)
    highs = df['high'].values
    lows = df['low'].values
    closes = df['close'].values
    times = df['time'].values if 'time' in df.columns else np.arange(len(df))
    bar_indices = np.arange(len(df))
    ranges = highs - lows

    # 1. ATR/Range для фильтрации и highVolatilityBar
    if order_block_filter == 'ATR':
        # Wilder’s smoothing (rma) для ATR, как в Pine Script ta.atr
        def rma(series, length):
            alpha = 1 / length
            return series.ewm(alpha=alpha, adjust=False).mean()
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        true_range = np.maximum.reduce([high_low, high_close, low_close])
        # Исправление: приводим к pd.Series для ewm
        true_range = pd.Series(true_range)
        atr = rma(true_range, atr_period)
        volatility_measure = atr.values
    else:
        # Кумулятивный средний диапазон (Range)
        volatility_measure = np.cumsum(ranges) / (np.arange(len(df)) + 1)

    highVolatilityBar = ranges >= (high_volatility_mult * volatility_measure)
    parsedHigh = np.where(highVolatilityBar, lows, highs)
    parsedLow = np.where(highVolatilityBar, highs, lows)

    # 2. Swing-детектор (pivot high/low) с разрешением равенства (>=, <=)
    pivots = []  # (index, 'high'/'low')
    for i in range(swing_lookback, len(df) - swing_lookback):
        is_pivot_high = all(highs[i] >= highs[i - j] and highs[i] >= highs[i + j] for j in range(1, swing_lookback + 1))
        is_pivot_low = all(lows[i] <= lows[i - j] and lows[i] <= lows[i + j] for j in range(1, swing_lookback + 1))
        if is_pivot_high:
            pivots.append((i, 'high'))
        if is_pivot_low:
            pivots.append((i, 'low'))

    # 3. BOS/CHoCH по пересечению close с pivot (move_range >= min_range)
    events = []  # (event_type, swing_index, cross_index, direction)
    for idx, typ in pivots:
        if typ == 'high':
            for j in range(idx + 1, len(df)):
                move_range = abs(closes[j] - highs[idx])
                min_range = confluence_min_range_mult * volatility_measure[j]
                if closes[j] > highs[idx] and move_range >= min_range:
                    if j - idx <= max_bars_between_swing_and_bos:
                        events.append(('BOS', idx, j, 'bullish'))
                        if verbose:
                            print(f"BOS (bullish): swing={idx}, cross={j}, move_range={move_range:.5f}, min_range={min_range:.5f}")
                    break
                elif closes[j] < lows[idx] and move_range >= min_range:
                    if j - idx <= max_bars_between_swing_and_bos:
                        events.append(('CHoCH', idx, j, 'bearish'))
                        if verbose:
                            print(f"CHoCH (bearish): swing={idx}, cross={j}, move_range={move_range:.5f}, min_range={min_range:.5f}")
                    break
        elif typ == 'low':
            for j in range(idx + 1, len(df)):
                move_range = abs(closes[j] - lows[idx])
                min_range = confluence_min_range_mult * volatility_measure[j]
                if closes[j] < lows[idx] and move_range >= min_range:
                    if j - idx <= max_bars_between_swing_and_bos:
                        events.append(('BOS', idx, j, 'bearish'))
                        if verbose:
                            print(f"BOS (bearish): swing={idx}, cross={j}, move_range={move_range:.5f}, min_range={min_range:.5f}")
                    break
                elif closes[j] > highs[idx] and move_range >= min_range:
                    if j - idx <= max_bars_between_swing_and_bos:
                        events.append(('CHoCH', idx, j, 'bullish'))
                        if verbose:
                            print(f"CHoCH (bullish): swing={idx}, cross={j}, move_range={move_range:.5f}, min_range={min_range:.5f}")
                    break

    # 4. Order block по parsedHigh/parsedLow между swing и BOS/CHoCH
    all_blocks = []
    for event_type, swing_idx, cross_idx, direction in events:
        reason = None
        if cross_idx - swing_idx < 1 or cross_idx - swing_idx > max_bars_between_swing_and_bos:
            reason = 'too long or zero range'
            if verbose:
                print(f"BLOCK SKIP: swing={swing_idx}, cross={cross_idx}, reason={reason}")
            continue
        if np.any(highVolatilityBar[swing_idx:cross_idx+1]):
            reason = 'highVolatilityBar inside block'
            if verbose:
                print(f"BLOCK SKIP: swing={swing_idx}, cross={cross_idx}, reason={reason}")
            continue
        if direction == 'bullish':
            block_low = np.min(parsedLow[swing_idx:cross_idx+1])
            block_high = np.max(parsedHigh[swing_idx:cross_idx+1])
            block = {
                'high': block_high,
                'low': block_low,
                'bar_index': cross_idx,
                'type': 'BULLISH',
                'event': event_type,
                'swing': swing_idx,
                'cross': cross_idx,
                'mitigated': False,
                'mitigation_time': None
            }
            all_blocks.append(block)
            if verbose:
                print(f"BLOCK OK: BULLISH swing={swing_idx}, cross={cross_idx}, low={block_low:.5f}, high={block_high:.5f}")
        elif direction == 'bearish':
            block_low = np.min(parsedLow[swing_idx:cross_idx+1])
            block_high = np.max(parsedHigh[swing_idx:cross_idx+1])
            block = {
                'high': block_high,
                'low': block_low,
                'bar_index': cross_idx,
                'type': 'BEARISH',
                'event': event_type,
                'swing': swing_idx,
                'cross': cross_idx,
                'mitigated': False,
                'mitigation_time': None
            }
            all_blocks.append(block)
            if verbose:
                print(f"BLOCK OK: BEARISH swing={swing_idx}, cross={cross_idx}, low={block_low:.5f}, high={block_high:.5f}")

    # 5. Митигация по parsedHigh/parsedLow
    for block in all_blocks:
        for j in range(block['bar_index']+1, len(df)):
            if mitigation_mode == 'highlow':
                if block['type'] == 'BULLISH' and parsedLow[j] < block['low']:
                    block['mitigated'] = True
                    block['mitigation_time'] = times[j]
                    break
                elif block['type'] == 'BEARISH' and parsedHigh[j] > block['high']:
                    block['mitigated'] = True
                    block['mitigation_time'] = times[j]
                    break
            elif mitigation_mode == 'close':
                if block['type'] == 'BULLISH' and closes[j] < block['low']:
                    block['mitigated'] = True
                    block['mitigation_time'] = times[j]
                    break
                elif block['type'] == 'BEARISH' and closes[j] > block['high']:
                    block['mitigated'] = True
                    block['mitigation_time'] = times[j]
                    break
    # 6. Оставляем только не митигаированные
    bullish_blocks = [b for b in all_blocks if b['type'] == 'BULLISH' and not b['mitigated']]
    bearish_blocks = [b for b in all_blocks if b['type'] == 'BEARISH' and not b['mitigated']]
    # Сортируем: последние сверху
    bullish_blocks = bullish_blocks[::-1][:max_blocks]
    bearish_blocks = bearish_blocks[::-1][:max_blocks]
    if verbose:
        print(f"[LuxAlgo Internal OB] Найдено бычьих блоков: {len(bullish_blocks)}")
        for b in bullish_blocks:
            print(f"BULLISH {b['low']:.4f}-{b['high']:.4f} (bar_index: {b['bar_index']}, event: {b['event']})")
        print(f"[LuxAlgo Internal OB] Найдено медвежьих блоков: {len(bearish_blocks)}")
        for b in bearish_blocks:
            print(f"BEARISH {b['low']:.4f}-{b['high']:.4f} (bar_index: {b['bar_index']}, event: {b['event']})")
    return bullish_blocks, bearish_blocks

