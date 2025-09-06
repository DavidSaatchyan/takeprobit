import csv
import os
from typing import Dict, Any, Optional
from datetime import datetime

CSV_FILE = 'trade_statistics.csv'
CSV_FILE_ML = 'trade_statistics_ml.csv'  # Новый файл для расширенных данных

FIELDNAMES = [
    'symbol', 'direction', 'entry_time', 'entry_price', 'exit_time', 'exit_price', 'size', 'reason', 'pnl', 'fee',
    'score', 'close', 'zl_basis', 'volatility', 'trend', 'atr', 'natr', 'natr_median', 'rsi_5m', 'rsi_15m'
]

def log_trade(trade_data: Dict[str, Any], csv_file: Optional[str] = None):
    """
    Сохраняет параметры сделки и значения индикаторов в CSV.
    trade_data: словарь с ключами из FIELDNAMES (лишние игнорируются)
    """
    file_path = csv_file or CSV_FILE
    file_exists = os.path.isfile(file_path)

    # Оставляем только нужные поля
    row = {k: trade_data.get(k, '') for k in FIELDNAMES}

    # Проверка: если индикаторы не заполнены, логируем предупреждение
    missing_indicators = [k for k in FIELDNAMES[10:] if not row.get(k)]
    if missing_indicators:
        print(f"[WARN] Не заполнены индикаторы при открытии сделки: {missing_indicators} | {row.get('symbol')} {row.get('direction')} {row.get('entry_time')}")

    # Преобразуем datetime в строку, если нужно
    for time_field in ['entry_time', 'exit_time']:
        if isinstance(row[time_field], datetime):
            row[time_field] = row[time_field].strftime('%Y-%m-%d %H:%M:%S')

    with open(file_path, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

def get_trade_statistics(csv_file: Optional[str] = None) -> Dict[str, Any]:
    """
    Возвращает статистику по сделкам для анализа
    """
    file_path = csv_file or CSV_FILE
    if not os.path.exists(file_path):
        return {'error': 'Файл статистики не найден'}
    
    trades = []
    with open(file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            trades.append(row)
    
    if not trades:
        return {'error': 'Нет данных о сделках'}
    
    # Базовые метрики
    total_trades = len(trades)
    winning_trades = len([t for t in trades if float(t.get('pnl', 0)) > 0])
    losing_trades = total_trades - winning_trades
    win_rate = winning_trades / total_trades if total_trades > 0 else 0
    
    total_pnl = sum(float(t.get('pnl', 0)) for t in trades)
    avg_pnl = total_pnl / total_trades if total_trades > 0 else 0
    
    return {
        'total_trades': total_trades,
        'winning_trades': winning_trades,
        'losing_trades': losing_trades,
        'win_rate': win_rate,
        'total_pnl': total_pnl,
        'avg_pnl': avg_pnl,
        'trades': trades
    }

def update_trade_exit(symbol: str, direction: str, entry_time: str, exit_data: Dict[str, Any], csv_file: Optional[str] = None):
    """
    Обновляет строку сделки в CSV по уникальному ключу (symbol, direction, entry_time),
    добавляя exit_time, exit_price, pnl, reason и т.д.
    exit_data: словарь с новыми данными (exit_time, exit_price, pnl, reason, fee ...)
    """
    file_path = csv_file or CSV_FILE
    if not os.path.exists(file_path):
        print(f"Файл статистики {file_path} не найден!")
        return False
    
    updated = False
    rows = []
    print(f"[DEBUG] update_trade_exit ищет: symbol={symbol}, direction={direction}, entry_time={entry_time}")
    mismatches = []
    with open(file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Сравниваем по уникальному ключу
            if (
                row['symbol'] == symbol and
                row['direction'] == direction and
                row['entry_time'] == entry_time
            ):
                # Обновляем только exit-поля
                for k, v in exit_data.items():
                    if k in FIELDNAMES:
                        row[k] = v
                updated = True
            else:
                # Логируем несовпадения
                mismatch = {
                    'row_symbol': row['symbol'],
                    'row_direction': row['direction'],
                    'row_entry_time': row['entry_time'],
                    'match_symbol': row['symbol'] == symbol,
                    'match_direction': row['direction'] == direction,
                    'match_entry_time': row['entry_time'] == entry_time
                }
                mismatches.append(mismatch)
            rows.append(row)
    if not updated:
        print(f"[ERROR] Не найдена сделка для обновления: {symbol} {direction} {entry_time}")
        print(f"[DEBUG] Несовпавшие строки:")
        for m in mismatches:
            print(m)
        return False
    # Перезаписываем файл
    with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Обновлена сделка {symbol} {direction} {entry_time} в статистике.")
    return True 