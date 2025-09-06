import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from config import ATR_MULTIPLIER_TP
from indicators import calculate_atr, calculate_natr, calculate_leverage, calculate_clever_algo_v2, calculate_rsi
from indicators import find_internal_order_blocks_luxalgo
import logging
from typing import Optional, Dict, Any, Tuple
import os
from logging.handlers import RotatingFileHandler
import asyncio
from precision import round_price

# Настройка логирования
# Создаём каталог для логов, если не существует
log_dir = "/var/log/trading_bot"
os.makedirs(log_dir, exist_ok=True)

log_file_path = os.path.join(log_dir, "strategy.log")

# Настройка логирования с ротацией
handler = RotatingFileHandler(
    filename=log_file_path,
    maxBytes=10 * 1024 * 1024,  # 10 MB
    backupCount=5,              # Храним до 5 файлов: strategy.log.1, .2 и т.д.
    encoding="utf-8"
)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

console = logging.StreamHandler()
console.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)
logger.addHandler(console)
logger.propagate = False  # Чтобы не дублировались сообщения

# ML функции временно отключены
ML_AVAILABLE = False
ML_LOGGING_AVAILABLE = False
ml_optimizer = None

USE_TF_CONFIRMATION = False  # подтверждение тренда на 3m и 5m
USE_RSI15_CONFIRMATION = False  # подтверждение по RSI 15m
INVERT_SIGNALS = False  # Инвертировать сигналы buy/sell
USE_ORDER_BLOCK_FILTER = False  # Включить фильтрацию по ордер блокам
USE_SUPPORT_BLOCK_FILTER = False  # Требовать поддержки/сопротивления сзади для отскока
USE_TRAILING_ORDERS = False  # Временно отключить трейлинг-ордера

# === КЭШ ДЛЯ СВЕЧЕЙ ===
candles_cache: dict[str, dict[str, dict[str, Any]]] = {}  # {symbol: {interval: {'df': ..., 'last_ts': ...}}}

async def get_actual_candles(symbol: str, interval: str, connector: Any, heikin_ashi: bool = True) -> pd.DataFrame:
    """
    Возвращает актуальные свечи из кэша или запрашивает новые, если свеча устарела.
    Гарантирует, что последняя свеча — всегда закрытая (для Binance и других CEX).
    """
    now = datetime.now(timezone.utc)
    cache = candles_cache.get(symbol, {}).get(interval)
    if cache and is_candle_actual(cache['last_ts'], interval, now):
        df = cache['df']
    else:
        # Если нет — запрашиваем новые свечи
        end_time = int(now.timestamp() * 1000)
        if interval.endswith('m'):
            tf_minutes = int(interval.replace('m', ''))
            start_time = int((now - timedelta(minutes=tf_minutes * 420)).timestamp() * 1000)
        else:
            start_time = int((now - timedelta(minutes=420)).timestamp() * 1000)
        df = await fetch_candles(symbol, interval, connector, start_time, end_time, heikin_ashi=heikin_ashi)
        if not df.empty:
            if symbol not in candles_cache:
                candles_cache[symbol] = {}
            candles_cache[symbol][interval] = {'df': df, 'last_ts': df['timestamp'].iloc[-1]}
    # --- ГАРАНТИЯ: только закрытые свечи ---
    if not df.empty:
        tf_minutes = int(interval.replace('m', '')) if interval.endswith('m') else 1
        tf_ms = tf_minutes * 60 * 1000
        now_ms = int(now.timestamp() * 1000)
        # Время открытия последней свечи
        open_time = int(df['time'].iloc[-1])
        if now_ms < open_time + tf_ms:
            # Последняя свеча не закрыта — убираем её
            df = df.iloc[:-1]
    return df

def convert_to_heikin_ashi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Точная конвертация в Heikin Ashi с валидацией данных
    Соответствует расчетам TradingView
    """
    if df.empty or not {'open', 'high', 'low', 'close'}.issubset(df.columns):
        return df
        
    try:
        ha_df = df.copy()
        
        # Расчет цен Heikin Ashi
        ha_df['ha_close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
        
        # Расчет ha_open с учетом предыдущих значений
        ha_open = [(df['open'].iloc[0] + df['close'].iloc[0]) / 2]
        for i in range(1, len(df)):
            prev_ha_open = ha_open[i-1]
            prev_ha_close = ha_df['ha_close'].iloc[i-1]
            ha_open.append((prev_ha_open + prev_ha_close) / 2)
        
        ha_df['ha_open'] = ha_open
        ha_df['ha_high'] = ha_df[['high', 'ha_open', 'ha_close']].max(axis=1)
        ha_df['ha_low'] = ha_df[['low', 'ha_open', 'ha_close']].min(axis=1)
        
        # Замена оригинальных цен
        ha_df.loc[:, ['open', 'high', 'low', 'close']] = ha_df[['ha_open', 'ha_high', 'ha_low', 'ha_close']].values
        
        return ha_df.drop(['ha_open', 'ha_high', 'ha_low', 'ha_close'], axis=1, errors='ignore')
        
    except Exception as e:
        logger.error(f"Heikin Ashi conversion error: {str(e)}", exc_info=True)
        return df

def is_flat_market_natr_relative(df: pd.DataFrame, period: int = 14, window: int = 120, sensitivity: float = 0.9) -> bool:
    """
    Определяет флэт на основе того, ниже ли текущий NATR среднего значения за окно,
    умноженного на коэффициент чувствительности.
    """
    natr_series = calculate_natr(df, period)
    recent_natr = natr_series[-window:]  # последние N значений

    current_natr = natr_series.iloc[-1]
    mean_natr = recent_natr.mean()

    # Лог для отладки
    logging.debug(f"NATR check — current: {current_natr:.4f}, mean: {mean_natr:.4f}, threshold: {sensitivity * mean_natr:.4f}")

    return current_natr < sensitivity * mean_natr

def is_price_range_too_narrow(df: pd.DataFrame, min_range_pct: float = 0.5) -> bool:
    """
    Проверка, узкий ли диапазон цены за последние 100 свечей.
    """
    if len(df) < 100:
        return False  # недостаточно данных для проверки

    high = df['high'].iloc[-100:].max()
    low = df['low'].iloc[-100:].min()
    price_range_pct = (high - low) / df['close'].iloc[-1] * 100

    logging.debug(f"Price range check — high: {high}, low: {low}, range %: {price_range_pct:.4f}")

    return price_range_pct < min_range_pct

def is_candle_actual(last_ts: datetime, interval: str, now: datetime, tolerance_sec: int = 2) -> bool:
    """
    Проверяет, актуальна ли последняя свеча для данного таймфрейма.
    last_ts — время последней свечи (datetime, UTC)
    interval — строка, например '1m', '3m', '5m'
    now — текущее время (datetime, UTC)
    tolerance_sec — запас в секундах
    """
    if not interval.endswith('m'):
        return False  # поддерживаем только минутные интервалы
    tf_minutes = int(interval.replace('m', ''))
    # Ожидаемая последняя свеча
    expected_minute = (now.minute // tf_minutes) * tf_minutes
    expected_ts = now.replace(minute=expected_minute, second=0, microsecond=0)
    # Если сейчас меньше чем tf + tolerance — свеча актуальна
    if now < expected_ts + timedelta(minutes=tf_minutes, seconds=tolerance_sec):
        return last_ts >= expected_ts
    else:
        return False

async def fetch_candles(
    symbol: str,
    interval: str,
    connector: Any,
    start_time: int,
    end_time: int,
    heikin_ashi: bool = True,
    retries: int = 3,
    retry_delay: int = 2
) -> pd.DataFrame:
    """
    Асинхронное получение свечей с улучшенной обработкой ошибок и проверкой актуальности по таймфрейму
    """
    for attempt in range(retries):
        try:
            logger.info(f"Fetching candles for {symbol} ({interval}) from {start_time} to {end_time}")
            candles = connector.get_candles(symbol, interval, start_time, end_time)
            if not candles:
                logger.warning("Empty candles data received")
                await asyncio.sleep(retry_delay)
                continue
            # --- Унификация структуры свечей для Binance (list) и BingX (dict) ---
            if isinstance(candles, list) and candles and isinstance(candles[0], list):
                # Binance: массивы
                columns = ['time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base', 'taker_buy_quote', 'ignore']
                df = pd.DataFrame(candles, columns=columns)
                # Приводим типы
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                df['time'] = pd.to_numeric(df['time'], errors='coerce')
            else:
                # BingX: dict
                df = pd.DataFrame(candles)
                # Приводим типы
                numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'time']
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
            # Валидация колонок
            required_cols = ['open', 'high', 'low', 'close', 'volume', 'time']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.error(f"Missing required columns: {missing_cols}")
                await asyncio.sleep(retry_delay)
                continue
            # Сортировка и обработка времени
            df = df.sort_values('time')
            df['timestamp'] = pd.to_datetime(df['time'], unit='ms', utc=True)
            # Удаление строк с NaN
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            if df[numeric_cols].isnull().values.any():
                logger.warning("Dropping rows with NaN values")
                df = df.dropna(subset=numeric_cols)
            logger.info(f"Successfully fetched {len(df)} candles")
            if df.empty:
                await asyncio.sleep(retry_delay)
                continue
            last_ts = df['timestamp'].iloc[-1]
            if last_ts.tzinfo is None:
                last_ts = last_ts.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            if is_candle_actual(last_ts, interval, now, tolerance_sec=2):
                return convert_to_heikin_ashi(df) if heikin_ashi else df
            else:
                logger.warning(f"Данные устарели для {interval}. Последняя свеча: {last_ts}, сейчас: {now}. Попытка {attempt+1}/{retries}. Жду {retry_delay} сек и пробую снова.")
                await asyncio.sleep(retry_delay)
        except Exception as e:
            logger.error(f"Error fetching candles: {str(e)}", exc_info=True)
            await asyncio.sleep(retry_delay)
    logger.error(f"Не удалось получить актуальные свечи для {symbol} ({interval}) после {retries} попыток.")
    return pd.DataFrame()

def analyze_market_conditions(df: pd.DataFrame, prev_trend: Optional[int], trend_3m: Optional[int] = None, trend_5m: Optional[int] = None, rsi_5m: Optional[float] = None, rsi_15m: Optional[float] = None) -> Tuple[Optional[str], Optional[float], int]:
    try:
        LENGTH = 50
        VOLATILITY_MULT = 1.5
        LOOP_START = 1
        LOOP_END = 70
        THRESHOLD_UP = 5
        THRESHOLD_DOWN = -5

        if len(df) < 100:
            logger.warning(f"Insufficient data: {len(df)} candles")
            return None, None, prev_trend if prev_trend is not None else 0
        
        # if is_flat_market_natr_relative(df, period=14, window=120, sensitivity=0.9):
        #     logger.info("Рынок во флэте по относительному NATR — сигнал игнорируется.")
        #     return None, None, prev_trend if prev_trend is not None else 0
        
        # if is_price_range_too_narrow(df):
        #     logger.info("Флэт по узкому диапазону цен — сигнал игнорируется.")
        #     return None, None, prev_trend if prev_trend is not None else 0

        df = calculate_clever_algo_v2(
            df,
            length=LENGTH,
            volatility_mult=VOLATILITY_MULT,
            loop_start=LOOP_START,
            loop_end=LOOP_END,
            threshold_up=THRESHOLD_UP,
            threshold_down=THRESHOLD_DOWN
        )

        df['atr'] = calculate_atr(df, 14)

        current = df.iloc[-1]
        current_trend = int(current['trend'])
        prev_trend_correct = int(df.iloc[-2]['trend']) if len(df) > 1 else (prev_trend if prev_trend is not None else 0)

        # Улучшенное определение смены тренда
        trend_changed = (
            (prev_trend_correct in [-1, 1]) and
            (current_trend in [-1, 1]) and
            (prev_trend_correct != current_trend)
        )

        buy_cond = trend_changed and current_trend == 1
        sell_cond = trend_changed and current_trend == -1

        # Строгая проверка трендов на обоих таймфреймах
        if USE_TF_CONFIRMATION:
            if trend_3m is None or trend_5m is None:
                logger.info(f"TF confirmation: нет данных по тренду (trend_3m={trend_3m}, trend_5m={trend_5m}) — сигнал игнорируется.")
                return None, current['atr'], current_trend
            if INVERT_SIGNALS:
                # Для инвертированных сигналов логика обратная
                if buy_cond:
                    if trend_3m != -1 or trend_5m != -1:
                        logger.info(f"Buy сигнал проигнорирован — тренд 3m: {trend_3m}, тренд 5m: {trend_5m}. Оба должны быть -1 (инвертированная логика).")
                        return None, current['atr'], current_trend
                elif sell_cond:
                    if trend_3m != 1 or trend_5m != 1:
                        logger.info(f"Sell сигнал проигнорирован — тренд 3m: {trend_3m}, тренд 5m: {trend_5m}. Оба должны быть 1 (инвертированная логика).")
                        return None, current['atr'], current_trend
            else:
                # Обычная логика без инверсии
                if buy_cond:
                    if trend_3m != 1 or trend_5m != 1:
                        logger.info(f"Buy сигнал проигнорирован — тренд 3m: {trend_3m}, тренд 5m: {trend_5m}. Оба должны быть 1.")
                        return None, current['atr'], current_trend
                elif sell_cond:
                    if trend_3m != -1 or trend_5m != -1:
                        logger.info(f"Sell сигнал проигнорирован — тренд 3m: {trend_3m}, тренд 5m: {trend_5m}. Оба должны быть -1.")
                        return None, current['atr'], current_trend

        logger.debug(
            f"[DEBUG] score={current['score']}, close={current['close']}, "
            f"trend={current_trend}, prev_trend={prev_trend_correct}, "
            f"trend_changed={trend_changed}, short_signal={current['short_signal']}, "
            f"RSI 5m: {f'{rsi_5m:.2f}' if rsi_5m is not None else 'N/A'}, RSI 15m: {f'{rsi_15m:.2f}' if rsi_15m is not None else 'N/A'}"
        )

        if buy_cond:
            logger.info("*** СИГНАЛ ПОКУПКИ ***")
            return 'buy', current['atr'], current_trend
        elif sell_cond:
            logger.info("*** СИГНАЛ ПРОДАЖИ ***")
            return 'sell', current['atr'], current_trend

        return None, current['atr'], current_trend

    except Exception as e:
        logger.error(f"Ошибка анализа: {str(e)}", exc_info=True)
        return None, None, prev_trend if prev_trend is not None else 0

async def generate_trading_setup(
    connector: Any,
    symbol: str,
    prev_trend: Optional[int],
    interval: str = '1m',
    risk_multiplier: float = 2.5
) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
    """
    Генерация торгового сетапа с возвратом текущего тренда
    """
    try:
        logger.info(f"Generating setup for {symbol} ({interval})")

        now = datetime.now(timezone.utc)
        # Получаем свечи для основного анализа
        df = await get_actual_candles(symbol, interval, connector, heikin_ashi=True)
        if df.empty:
            logger.error("No data available for analysis")
            return None, prev_trend

        trend_3m = None
        trend_5m = None
        rsi_5m = None
        rsi_15m = None

        if USE_TF_CONFIRMATION:
            # Получаем свечи 3m для подтверждения тренда
            df_3m = await get_actual_candles(symbol, '3m', connector, heikin_ashi=True)
            # Получаем свечи 5m для подтверждения тренда
            df_5m = await get_actual_candles(symbol, '5m', connector, heikin_ashi=True)

            if not df_3m.empty and len(df_3m) >= 50:
                df_3m = calculate_clever_algo_v2(
                    df_3m,
                    length=50,
                    volatility_mult=1.5,
                    loop_start=1,
                    loop_end=70,
                    threshold_up=5,
                    threshold_down=-5
                )
                trend_3m = int(df_3m.iloc[-1]['trend'])
                logger.debug(f"3m trend confirmation: {trend_3m}")

            if not df_5m.empty and len(df_5m) >= 50:
                df_5m = calculate_clever_algo_v2(
                    df_5m,
                    length=50,
                    volatility_mult=1.5,
                    loop_start=1,
                    loop_end=70,
                    threshold_up=5,
                    threshold_down=-5
                )
                trend_5m = int(df_5m.iloc[-1]['trend'])
                rsi_5m_raw = calculate_rsi(df_5m, 14).iloc[-1]
                rsi_5m = float(rsi_5m_raw) if pd.notna(rsi_5m_raw) else None
                logger.debug(f"5m trend confirmation: {trend_5m}, RSI: {f'{rsi_5m:.2f}' if rsi_5m is not None else 'N/A'}")

        if USE_RSI15_CONFIRMATION:
            # Получаем свечи 15m только для RSI
            df_15m = await get_actual_candles(symbol, '15m', connector, heikin_ashi=True)
            if not df_15m.empty and len(df_15m) >= 50:
                rsi_15m_raw = calculate_rsi(df_15m, 14).iloc[-1]
                rsi_15m = float(rsi_15m_raw) if pd.notna(rsi_15m_raw) else None
                logger.debug(f"15m RSI: {f'{rsi_15m:.2f}' if rsi_15m is not None else 'N/A'}")

        logger.info(f"Analyzing {len(df)} candles up to {df['timestamp'].iloc[-1]}")

        signal, atr, current_trend = analyze_market_conditions(df, prev_trend, trend_3m, trend_5m, rsi_5m, rsi_15m)
        # --- ИНВЕРСИЯ СИГНАЛОВ ПОСЛЕ АНАЛИЗА ---
        original_signal = None
        if INVERT_SIGNALS and signal in ("buy", "sell"):
            original_signal = signal
            signal = "sell" if signal == "buy" else "buy"
            logger.info(f"Сигнал инвертирован: {original_signal} -> {signal}")
        
        # --- RSI ФИЛЬТР ПОСЛЕ ИНВЕРСИИ (применяется к итоговому направлению) ---
        if USE_RSI15_CONFIRMATION and rsi_15m is not None and signal in ("buy", "sell"):
            if signal == 'buy' and rsi_15m > 65:
                logger.info(f"Buy сигнал (итоговый) проигнорирован — RSI 15m: {rsi_15m:.2f} > 65 (перекупленность).")
                return None, current_trend
            elif signal == 'sell' and rsi_15m < 35:
                logger.info(f"Sell сигнал (итоговый) проигнорирован — RSI 15m: {rsi_15m:.2f} < 35 (перепроданность).")
                return None, current_trend
        
        if not signal or atr is None:
            logger.info("No trading signals detected")
            return None, current_trend

        atr = float(atr)

        current_price = connector.get_last_price(symbol)
        if not current_price:
            logger.error("Failed to get current price")
            return None, current_trend

        natr = calculate_natr(df, 14).iloc[-1]
        leverage = calculate_leverage(natr)

        # Получаем ВСЕ не митигаированные блоки для фильтрации сигналов
        bullish_blocks_all, bearish_blocks_all = find_internal_order_blocks_luxalgo(df, max_blocks=10000, mitigation_mode='highlow', verbose=False)
        # --- ДИНАМИЧЕСКИЙ ENTRY ПО ORDER BLOCK (НОВАЯ ЛОГИКА) ---
        entry = None
        used_block = None
        block_distance_threshold = 2.5  # максимум 2.5 ATR
        if signal == 'buy' and bullish_blocks_all:
            # Ищем бычий блок, верхняя граница которого строго ниже текущей цены и не дальше 2.5 ATR
            candidate_blocks = [b for b in bullish_blocks_all if b['high'] < current_price and (current_price - b['high']) <= block_distance_threshold * atr]
            if candidate_blocks:
                # Берём самый "верхний" (максимальный high)
                used_block = max(candidate_blocks, key=lambda b: b['high'])
                entry = used_block['high']
        elif signal == 'sell' and bearish_blocks_all:
            # Ищем медвежий блок, нижняя граница которого строго выше текущей цены и не дальше 2.5 ATR
            candidate_blocks = [b for b in bearish_blocks_all if b['low'] > current_price and (b['low'] - current_price) <= block_distance_threshold * atr]
            if candidate_blocks:
                # Берём самый "нижний" (минимальный low)
                used_block = min(candidate_blocks, key=lambda b: b['low'])
                entry = used_block['low']
        # Fallback: если не нашли подходящий блок — старый способ
        if entry is None:
            if signal == 'buy':
                entry = float(current_price - 1.0 * atr)
            else:
                entry = float(current_price + 1.0 * atr)
        # ---
        if signal == 'buy':
            tp = entry + ATR_MULTIPLIER_TP * atr
            sl = entry - risk_multiplier * atr
            sl_stop_price = sl + 0.1 * atr
        else:
            tp = entry - ATR_MULTIPLIER_TP * atr
            sl = entry + risk_multiplier * atr
            sl_stop_price = sl - 0.1 * atr
        # Коэффициенты для трейлинга
        SL_ACTIVATION_MULT = 2.2  # трейлинг-стоп активируется при +1.5 ATR (лонг) или -1.5 ATR (шорт)
        TP_TRAILING_MULT = 0.8    # трейлинг-TP: 1.5 ATR
        SL_TRAILING_MULT = 1.8    # трейлинг-SL: 1.5 ATR
        CALLBACK_MIN = 0.2        # минимальный процент
        CALLBACK_MAX = 1.0        # максимальный процент

        # Расчет callback_rate (в процентах) с защитой от None и деления на 0
        if atr is not None and entry is not None and entry != 0:
            trailing_tp_callback_rate = float((TP_TRAILING_MULT * atr / entry) * 100)
            trailing_tp_callback_rate = min(max(trailing_tp_callback_rate, CALLBACK_MIN), CALLBACK_MAX)
            trailing_sl_callback_rate = float((SL_TRAILING_MULT * atr / entry) * 100)
            trailing_sl_callback_rate = min(max(trailing_sl_callback_rate, CALLBACK_MIN), CALLBACK_MAX)
        else:
            trailing_tp_callback_rate = None
            trailing_sl_callback_rate = None

        # Активация трейлинг-стопа только в прибыльной зоне
        if atr is not None and entry is not None:
            if signal == 'buy':
                trailing_sl_activation_price = float(round(entry + SL_ACTIVATION_MULT * atr, 6))
            else:
                trailing_sl_activation_price = float(round(entry - SL_ACTIVATION_MULT * atr, 6))
        else:
            trailing_sl_activation_price = None

        block_distance_threshold = 2.5  # стандартный порог
        # Для логирования оставляем только последние max_blocks
        max_blocks_vis = 5
        bullish_blocks = bullish_blocks_all[::-1][:max_blocks_vis]
        bearish_blocks = bearish_blocks_all[::-1][:max_blocks_vis]
        order_block_check = True
        ob_reason = ""
        current_price = df['close'].iloc[-1]
        atr = float(atr)
        # --- Основной фильтр (нет блоков по направлению сделки) ---
        if USE_ORDER_BLOCK_FILTER:
            if signal == 'buy':
                for block in bearish_blocks_all:
                    if block['low'] <= current_price <= block['high']:
                        order_block_check = False
                        ob_reason = f"Цена внутри медвежьего блока: {block['low']:.4f}-{block['high']:.4f}"
                        break
                if order_block_check:
                    min_distance = None
                    for block in bearish_blocks_all:
                        if current_price < block['low']:
                            distance = block['low'] - current_price
                            if min_distance is None or distance < min_distance:
                                min_distance = distance
                    if min_distance is not None and min_distance < block_distance_threshold * atr:
                        order_block_check = False
                        ob_reason = f"Слишком близко к медвежьему блоку: расстояние {min_distance:.4f} < {block_distance_threshold}*ATR ({block_distance_threshold*atr:.4f})"
            elif signal == 'sell':
                for block in bullish_blocks_all:
                    if block['low'] <= current_price <= block['high']:
                        order_block_check = False
                        ob_reason = f"Цена внутри бычьего блока: {block['low']:.4f}-{block['high']:.4f}"
                        break
                if order_block_check:
                    min_distance = None
                    for block in bullish_blocks_all:
                        if current_price > block['high']:
                            distance = current_price - block['high']
                            if min_distance is None or distance < min_distance:
                                min_distance = distance
                    if min_distance is not None and min_distance < block_distance_threshold * atr:
                        order_block_check = False
                        ob_reason = f"Слишком близко к бычьему блоку: расстояние {min_distance:.4f} < {block_distance_threshold}*ATR ({block_distance_threshold*atr:.4f})"
            if not order_block_check:
                logger.info(f"Сигнал отклонен фильтром order block'ов: {ob_reason}")
                return None, current_trend
        # --- Новый фильтр: поддержка/сопротивление сзади ---
        if USE_SUPPORT_BLOCK_FILTER:
            support_block_ok = False
            if signal == 'buy':
                for block in bullish_blocks_all:
                    # Блок снизу или цена внутри блока
                    if (block['low'] <= current_price <= block['high']) or (block['high'] < current_price and current_price - block['high'] <= block_distance_threshold * atr):
                        support_block_ok = True
                        break
                if not support_block_ok:
                    logger.info(f"Buy сигнал отклонён: нет бычьего блока снизу или он слишком далеко")
                    return None, current_trend
            elif signal == 'sell':
                for block in bearish_blocks_all:
                    # Блок сверху или цена внутри блока
                    if (block['low'] <= current_price <= block['high']) or (block['low'] > current_price and block['low'] - current_price <= block_distance_threshold * atr):
                        support_block_ok = True
                        break
                if not support_block_ok:
                    logger.info(f"Sell сигнал отклонён: нет медвежьего блока сверху или он слишком далеко")
                    return None, current_trend
        # Итоговый лог: найденные блоки
        blocks_info = ""
        if bullish_blocks:
            blocks_info += " | ".join([
                f"BULLISH {b['low']:.4f}-{b['high']:.4f} (bar_index: {b['bar_index']}, event: {b['event']}, swing: {b['swing']}, cross: {b['cross']})"
                for b in bullish_blocks
            ])
        if bearish_blocks:
            if blocks_info:
                blocks_info += " | "
            blocks_info += " | ".join([
                f"BEARISH {b['low']:.4f}-{b['high']:.4f} (bar_index: {b['bar_index']}, event: {b['event']}, swing: {b['swing']}, cross: {b['cross']})"
                for b in bearish_blocks
            ])
        if not blocks_info:
            blocks_info = "Order block'ы не найдены"
        logger.info(f"Order block'ы для {symbol}: {blocks_info}")
        
        # --- ДОБАВЛЯЕМ ИНДИКАТОРЫ CLEVER ALGO В SETUP ---
        last = df.iloc[-1]
        
        # Безопасное извлечение значений индикаторов с проверкой на пустые значения
        def safe_float(value, default=None):
            if pd.isna(value) or value == '' or value is None:
                return default
            try:
                return float(value)
            except (ValueError, TypeError):
                return default
        
        def safe_int(value, default=None):
            if pd.isna(value) or value == '' or value is None:
                return default
            try:
                return int(value)
            except (ValueError, TypeError):
                return default
        
        # --- Расчет take_profit_stop_price ---
        offset = 0.1 * atr
        if signal == 'buy':
            tp_stop_price = tp - offset
        else:
            tp_stop_price = tp + offset
        # Округление до tick_size
        tick_size = None
        if hasattr(connector, 'get_symbol_precision'):
            precision = connector.get_symbol_precision(symbol)
            tick_size = precision.get('tick_size') or 10 ** (-precision['price_precision'])
        if tick_size:
            tp_stop_price = round_price(tp_stop_price, tick_size)

        setup = {
            'symbol': symbol,
            'direction': signal,
            'entry_price': float(round(entry, 6)),
            'take_profit': float(round(tp, 6)),
            'take_profit_stop_price': float(tp_stop_price),
            'stop_loss': float(round(sl, 6)),
            'stop_loss_stop_price': float(round(sl_stop_price, 6)),
            'leverage': leverage,
            'atr': round(atr, 6),
            'natr': round(natr, 4),
            'natr_median': float(df['natr'].rolling(20).median().iloc[-1]) if 'natr' in df.columns and len(df) >= 20 else None,
            'timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'timeframe': interval,
            'score': safe_float(last.get('score')),
            'close': safe_float(last.get('close')),
            'zl_basis': safe_float(last.get('zl_basis')),
            'volatility': safe_float(last.get('volatility')),
            'trend': safe_int(last.get('trend')),
            'rsi_5m': round(rsi_5m, 2) if rsi_5m is not None else None,
            'rsi_15m': round(rsi_15m, 2) if rsi_15m is not None else None,
            'order_block_info': blocks_info,
        }
        # --- trailing-поля ---
        if USE_TRAILING_ORDERS:
            setup.update({
                'trailing_tp_callback_rate': float(round(trailing_tp_callback_rate, 4)) if trailing_tp_callback_rate is not None else None,
                'trailing_tp_activation_price': float(round(tp, 6)),
                'trailing_sl_callback_rate': float(round(trailing_sl_callback_rate, 4)) if trailing_sl_callback_rate is not None else None,
                'trailing_sl_activation_price': trailing_sl_activation_price if trailing_sl_activation_price is not None else None,
            })
        else:
            setup.update({
                'trailing_tp_callback_rate': None,
                'trailing_tp_activation_price': None,
                'trailing_sl_callback_rate': None,
                'trailing_sl_activation_price': None,
            })
        logger.info(f"Generated setup (trailing): TP_callback_rate={setup['trailing_tp_callback_rate']}%, TP_activation={setup['trailing_tp_activation_price']}, SL_callback_rate={setup['trailing_sl_callback_rate']}%, SL_activation={setup['trailing_sl_activation_price']}")
        logger.info(f"RSI values: 5m={setup['rsi_5m']}, 15m={setup['rsi_15m']}")

        return setup, current_trend

    except Exception as e:
        logger.error(f"Setup generation failed: {str(e)}", exc_info=True)
        return None, prev_trend

async def execute_strategy(connector: Any, chat_id: Optional[str] = None) -> None:
    try:
        setup, _ = await generate_trading_setup(connector, "SOL-USDT", prev_trend=None)
        if not setup:
            logger.info("Нет подходящего сетапа для исполнения.")
        else:
            logger.info(f"Сетап найден: {setup}")
    except Exception as e:
        logger.error(f"Strategy execution error: {str(e)}", exc_info=True)

if __name__ == "__main__":
    import asyncio
    from cex_connectors import get_exchange_connector
    
    async def main():
        connector = get_exchange_connector()
        await execute_strategy(connector)
    
    asyncio.run(main())
