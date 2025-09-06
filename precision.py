def round_step_size(quantity: float, step_size: float) -> float:
    import decimal
    if step_size <= 0:
        return quantity
    quantity_decimal = decimal.Decimal(str(quantity))
    step_size_decimal = decimal.Decimal(str(step_size))
    rounded_decimal = (quantity_decimal / step_size_decimal).quantize(
        decimal.Decimal('1'), rounding=decimal.ROUND_HALF_UP
    ) * step_size_decimal
    return float(rounded_decimal)

def round_price(price: float, tick_size: float) -> float:
    import decimal
    if tick_size <= 0:
        return price
    price_decimal = decimal.Decimal(str(price))
    tick_size_decimal = decimal.Decimal(str(tick_size))
    rounded_decimal = (price_decimal / tick_size_decimal).quantize(
        decimal.Decimal('1'), rounding=decimal.ROUND_HALF_UP
    ) * tick_size_decimal
    return float(rounded_decimal)

def get_symbol_precision(symbol: str, connector=None):
    """Возвращает точность для разных символов (Binance/BingX)."""
    from config import EXCHANGE
    if EXCHANGE == 'Binance':
        if connector and hasattr(connector, 'get_symbol_precision'):
            return connector.get_symbol_precision(symbol)
        else:
            return {'price_precision': 2, 'quantity_precision': 3}
    else:
        return {'price_precision': 2, 'quantity_precision': 3} 