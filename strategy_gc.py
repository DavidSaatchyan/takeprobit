import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from config import ATR_MULTIPLIER_TP
from indicators import calculate_atr, calculate_natr, calculate_leverage, calculate_clever_algo_v2
import logging
from typing import Optional, Dict, Any, Tuple
import os
from logging.handlers import RotatingFileHandler

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

def is_daily_change_too_small(connector: Any, symbol: str, threshold_pct: float = 1.5) -> bool:
    """
    Проверяет, находится ли дневное изменение цены в пределах заданного процента.
    """
    tickers = connector.get_ticker()
    for t in tickers:
        if t.get("symbol") == symbol:
            try:
                change_pct = float(t.get("priceChangePercent", 0))
                logging.debug(f"Суточное изменение цены для {symbol}: {change_pct:.2f}%")
                return abs(change_pct) < threshold_pct
            except (ValueError, TypeError):
                return False
    return False  # если не нашли символ или ошибка


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

def is_flat_market_natr_relative(df: pd.DataFrame, period: int = 14, window: int = 120, sensitivity: float = 1.1) -> bool:
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

def is_price_range_too_narrow(df: pd.DataFrame, min_range_pct: float = 0.62) -> bool:
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


async def fetch_candles(
    symbol: str,
    interval: str,
    connector: Any,
    start_time: int,
    end_time: int,
    heikin_ashi: bool = True
) -> pd.DataFrame:
    """
    Асинхронное получение свечей с улучшенной обработкой ошибок
    """
    try:
        logger.info(f"Fetching candles for {symbol} ({interval}) from {start_time} to {end_time}")
        
        # Получение данных
        candles = connector.get_candles(symbol, interval, start_time, end_time)  # Sync version
        
        if not candles:
            logger.warning("Empty candles data received")
            return pd.DataFrame()

        # Создание DataFrame
        df = pd.DataFrame(candles)
        
        # Валидация колонок
        required_cols = ['open', 'high', 'low', 'close', 'volume', 'time']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return pd.DataFrame()

        # Сортировка и обработка времени
        df = df.sort_values('time')
        df['timestamp'] = pd.to_datetime(df['time'], unit='ms', utc=True)

        
        # Конвертация числовых значений
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
        
        # Удаление строк с NaN
        if df[numeric_cols].isnull().values.any():
            logger.warning("Dropping rows with NaN values")
            df = df.dropna(subset=numeric_cols)
            
        logger.info(f"Successfully fetched {len(df)} candles")
        
        # Проверка актуальности данных для 1m и 15m
        last_ts = df['timestamp'].iloc[-1]
        if last_ts.tzinfo is None:
            last_ts = last_ts.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)

        if interval == '1m':
            max_delay = 90  # сек
        elif interval == '15m':
            max_delay = 900  # сек

        if (now - last_ts).total_seconds() > max_delay:
            logger.warning(f"Данные устарели. Последняя свеча: {last_ts}, сейчас: {now} для интервала {interval}")
            return pd.DataFrame()

        return convert_to_heikin_ashi(df) if heikin_ashi else df
        
    except Exception as e:
        logger.error(f"Error fetching candles: {str(e)}", exc_info=True)
        return pd.DataFrame()

def analyze_market_conditions(df: pd.DataFrame, prev_trend: Optional[int], higher_tf_trend: Optional[int] = None) -> Tuple[Optional[str], Optional[float], int]:
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
        
        # Проверка на флэт по Z-оценке ATR
        if is_flat_market_natr_relative(df, period=14, window=120, sensitivity=1.1):
            logger.info("Рынок во флэте по относительному NATR — сигнал игнорируется.")
            return None, None, prev_trend if prev_trend is not None else 0
        
        if is_price_range_too_narrow(df):
            logger.info("Флэт по узкому диапазону цен — сигнал игнорируется.")
            return None, None, prev_trend if prev_trend is not None else 0

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

        # Жесткая проверка старшего ТФ
        if higher_tf_trend is not None:
            if buy_cond and higher_tf_trend != 1:
                logger.info("Buy сигнал проигнорирован — тренд 15m не бычий.")
                return None, current['atr'], current_trend
            if sell_cond and higher_tf_trend != -1:
                logger.info("Sell сигнал проигнорирован — тренд 15m не медвежий.")
                return None, current['atr'], current_trend

        logger.debug(
            f"[DEBUG] score={current['score']}, close={current['close']}, "
            f"trend={current_trend}, prev_trend={prev_trend_correct}, "
            f"trend_changed={trend_changed}, short_signal={current['short_signal']}"
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
    risk_multiplier: float = 2.4
) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
    """
    Генерация торгового сетапа с возвратом текущего тренда
    """
    try:
        logger.info(f"Generating setup for {symbol} ({interval})")
        if is_daily_change_too_small(connector, symbol):
            logger.info(f"Дневное изменение цены по {symbol} слишком мало — сигнал пропущен.")
            return None, prev_trend

        now = datetime.now(timezone.utc)
        end_time = int(now.timestamp() * 1000)
        start_time = int((now - timedelta(minutes=300)).timestamp() * 1000)

        df = await fetch_candles(symbol, interval, connector, start_time, end_time, heikin_ashi=True)
        if df.empty:
            logger.error("No data available for analysis")
            return None, prev_trend

        # Получаем свечи 15m для подтверждения тренда
        start_time_15m = int((now - timedelta(minutes=15 * 300)).timestamp() * 1000)
        df_15m = await fetch_candles(symbol, '15m', connector, start_time_15m, end_time, heikin_ashi=True)

        if df_15m.empty or len(df_15m) < 50:
            logger.warning("Недостаточно данных 15m — сигнал не будет подтверждён.")
            higher_tf_trend = None
        else:
            # Проверка: последняя 15m свеча должна быть актуальной
            last_15m_ts = df_15m['timestamp'].iloc[-1]

            now = datetime.now(timezone.utc)

            expected_last_15m = now.replace(second=0, microsecond=0)
            expected_last_15m -= timedelta(minutes=expected_last_15m.minute % 15)

            if last_15m_ts < expected_last_15m:
                logger.warning(f"15m свеча еще не обновилась. Последняя: {last_15m_ts}, ожидаем: {expected_last_15m}")
                higher_tf_trend = None
            else:
                df_15m = calculate_clever_algo_v2(
                    df_15m,
                    length=50,
                    volatility_mult=1.5,
                    loop_start=1,
                    loop_end=70,
                    threshold_up=5,
                    threshold_down=-5
                )
                higher_tf_trend = int(df_15m.iloc[-1]['trend'])
                logger.debug(f"15m trend confirmation: {higher_tf_trend}")

        
        logger.info(f"Analyzing {len(df)} candles up to {df['timestamp'].iloc[-1]}")

        signal, atr, current_trend = analyze_market_conditions(df, prev_trend, higher_tf_trend)
        if not signal:
            logger.info("No trading signals detected")
            return None, current_trend

        current_price = connector.get_last_price(symbol)
        if not current_price:
            logger.error("Failed to get current price")
            return None, current_trend

        natr = calculate_natr(df, 14).iloc[-1]
        leverage = calculate_leverage(natr)

        if signal == 'buy':
            entry = current_price - 0.2 * atr
            tp = entry + ATR_MULTIPLIER_TP * atr
            sl = entry - risk_multiplier * atr
            sl_stop_price = sl + 0.1 * atr
        else:
            entry = current_price + 0.2 * atr
            tp = entry - ATR_MULTIPLIER_TP * atr
            sl = entry + risk_multiplier * atr
            sl_stop_price = sl - 0.1 * atr

        setup = {
            'symbol': symbol,
            'direction': signal,
            'entry_price': round(entry, 6),
            'take_profit': round(tp, 6),
            'stop_loss': round(sl, 6),
            'stop_loss_stop_price': round(sl_stop_price, 6),
            'leverage': leverage,
            'atr': round(atr, 6),
            'natr': round(natr, 4),
            'timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'timeframe': interval
        }

        logger.info(f"Generated setup:\n{setup}")
        return setup, current_trend

    except Exception as e:
        logger.error(f"Setup generation failed: {str(e)}", exc_info=True)
        return None, prev_trend

async def execute_strategy(connector: Any, chat_id: Optional[str] = None) -> None:
    try:
        setup, _ = await generate_trading_setup(connector, prev_trend=None)
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