from config import EXCHANGE, API_KEY, SECRET_KEY
from abc import ABC, abstractmethod
import requests
import time
import hmac
import hashlib
import json
from datetime import datetime, timedelta

# Абстрактный базовый класс
class ExchangeConnector(ABC):
    @abstractmethod
    def get_ticker(self):
        pass

    @abstractmethod
    def get_candles(self, symbol, interval, start_time, end_time):
        pass

    @abstractmethod
    def place_order(self, symbol, price, quantity, side, order_type, stop_loss=None, take_profit=None):
        pass

    @abstractmethod
    def get_open_orders(self, symbol):
        pass

    @abstractmethod
    def set_leverage(self, symbol, leverage, side):
        pass

    @abstractmethod
    def get_last_price(self, symbol):
        pass

    @abstractmethod
    def get_pnl(self, symbol, income_type="REALIZED_PNL", days=1):
        pass

# Класс для MEXC
class MEXCConnector(ExchangeConnector):
    BASE_URL = "https://contract.mexc.com/api/v1"

    def get_ticker(self):
        response = requests.get(f"{self.BASE_URL}/contract/ticker")
        return response.json().get("data", [])

    def get_candles(self, symbol, interval, start_time, end_time):
        url = f"{self.BASE_URL}/contract/kline/{symbol}?interval={interval}&start={start_time}&end={end_time}"
        response = requests.get(url)
        return response.json().get("data", [])

    def place_order(self, symbol, price, quantity, side, order_type, stop_loss=None, take_profit=None):
        url = f"{self.BASE_URL}/private/order/submit"
        headers = {"Content-Type": "application/json"}
        payload = {
            "symbol": symbol,
            "price": price,
            "vol": quantity,  # Используем quantity вместо volume
            "side": side,
            "type": order_type,
            "openType": 1,
            "stopLossPrice": stop_loss,
            "takeProfitPrice": take_profit
        }
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        return response.json()

    def get_open_orders(self, symbol):
        # MEXC не поддерживает получение открытых ордеров через API
        return []

    def set_leverage(self, symbol, leverage, side):
        # MEXC не поддерживает изменение плеча через API
        return {"status": "error", "message": "Not supported"}

    def get_last_price(self, symbol):
        # MEXC не поддерживает получение последней цены через API
        return 0

    def get_pnl(self, symbol, income_type="REALIZED_PNL", days=1):
        # MEXC не поддерживает получение PnL через API
        return {"status": "error", "message": "Not supported"}

# Класс для BingX
class BingXConnector(ExchangeConnector):
    BASE_URL = "https://open-api.bingx.com"

    def get_ticker(self):
        url = f"{self.BASE_URL}/openApi/swap/v2/quote/ticker"
        response = requests.get(url)
        return response.json().get("data", [])

    def get_candles(self, symbol, interval='1d', start_time=None, end_time=None):
        formatted_symbol = symbol if symbol.endswith(('-USDT', '-USDC')) else symbol.replace("_", "-")

        params = {
            "symbol": formatted_symbol,
            "interval": interval,
            "limit": 1000,
            "timestamp": int(time.time() * 1000)
        }

        # Добавляем start_time и end_time, если они указаны
        if start_time:
            params['startTime'] = start_time
        if end_time:
            params['endTime'] = end_time

        query_string = '&'.join([f"{key}={value}" for key, value in sorted(params.items())])
        signature = hmac.new(SECRET_KEY.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
        params['signature'] = signature

        url = f"{self.BASE_URL}/openApi/swap/v3/quote/klines"
        response = requests.get(url, params=params, headers={'X-BX-APIKEY': API_KEY})

        if response.status_code == 200:
            data = response.json()
            candles = data.get("data", [])
            return candles
        else:
            return []

    def place_order(self, symbol, price, quantity, side, order_type, stop_loss=None, take_profit=None):
        path = "/openApi/swap/v2/trade/order"
        method = "POST"
        paramsMap = {
            "symbol": symbol,
            "side": "BUY" if side == "buy" else "SELL",
            "positionSide": "LONG" if side == "buy" else "SHORT",
            "type": order_type.upper(),
            "quantity": quantity,  # Используем quantity
            "timestamp": int(time.time() * 1000)
        }

        if order_type.lower() == "limit":  # Добавляем price только для лимитных ордеров
            paramsMap["price"] = price

        if stop_loss:
            paramsMap["stopLoss"] = json.dumps({
                "type": "STOP_MARKET",
                "stopPrice": stop_loss,
                "price": stop_loss,
                "workingType": "MARK_PRICE"
            })

        if take_profit:
            paramsMap["takeProfit"] = json.dumps({
                "type": "TAKE_PROFIT_MARKET",
                "stopPrice": take_profit,
                "price": take_profit,
                "workingType": "MARK_PRICE"
            })

        paramsStr = self._parse_params(paramsMap)
        url = f"{self.BASE_URL}{path}?{paramsStr}&signature={self._get_sign(paramsStr)}"
        headers = {'X-BX-APIKEY': API_KEY}
        response = requests.post(url, headers=headers)
        return response.json()

    def get_open_orders(self, symbol):
        path = "/openApi/swap/v2/trade/openOrders"
        method = "GET"
        paramsMap = {
            "symbol": symbol,
            "timestamp": int(time.time() * 1000)
        }
        paramsStr = self._parse_params(paramsMap)
        url = f"{self.BASE_URL}{path}?{paramsStr}&signature={self._get_sign(paramsStr)}"
        headers = {'X-BX-APIKEY': API_KEY}
        response = requests.get(url, headers=headers)
        
        # Отладочная информация
        print("Ответ от API BingX (открытые ордера):")
        print(response.json())
        
        if response.status_code == 200:
            data = response.json()
            # Извлекаем список ордеров из поля 'orders'
            return data.get("data", {}).get("orders", [])
        return []

    def set_leverage(self, symbol, leverage, side):
        path = "/openApi/swap/v2/trade/leverage"
        method = "POST"
        paramsMap = {
            "symbol": symbol,
            "side": "BUY" if side == "buy" else "SELL",
            "leverage": leverage,
            "timestamp": int(time.time() * 1000)
        }
        paramsStr = self._parse_params(paramsMap)
        url = f"{self.BASE_URL}{path}?{paramsStr}&signature={self._get_sign(paramsStr)}"
        headers = {'X-BX-APIKEY': API_KEY}
        response = requests.post(url, headers=headers)
        return response.json()

    def get_last_price(self, symbol):
        url = f"{self.BASE_URL}/openApi/swap/v2/quote/ticker"
        params = {
            "symbol": symbol,
            "timestamp": int(time.time() * 1000)
        }
        paramsStr = self._parse_params(params)
        url = f"{self.BASE_URL}/openApi/swap/v2/quote/ticker?{paramsStr}&signature={self._get_sign(paramsStr)}"
        headers = {'X-BX-APIKEY': API_KEY}
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            ticker = data.get("data", [])
            if ticker:
                last_price = ticker[0].get("lastPrice")
                if last_price:
                    return float(last_price)
                else:
                    print(f"Ошибка: не удалось получить lastPrice для символа {symbol}.")
                    return None
            else:
                print(f"Ошибка: данные по тикеру для символа {symbol} отсутствуют.")
                return None
        else:
            print(f"Ошибка при запросе последней цены: {response.status_code}, {response.text}")
            return None

    def get_pnl(self, symbol, income_type="REALIZED_PNL", days=1):
        path = "/openApi/swap/v2/user/income"
        method = "GET"
        
        end_time = int(time.time() * 1000)
        start_time = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
        
        paramsMap = {
            "symbol": symbol,
            "incomeType": income_type,
            "startTime": start_time,
            "endTime": end_time,
            "limit": 1000,
            "timestamp": int(time.time() * 1000)
        }
        
        paramsStr = self._parse_params(paramsMap)
        url = f"{self.BASE_URL}{path}?{paramsStr}&signature={self._get_sign(paramsStr)}"
        headers = {'X-BX-APIKEY': API_KEY}
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            return data.get("data", [])
        else:
            print(f"Ошибка при запросе PnL: {response.status_code}, {response.text}")
            return []

    def _parse_params(self, paramsMap):
        sortedKeys = sorted(paramsMap)
        paramsStr = "&".join([f"{key}={paramsMap[key]}" for key in sortedKeys])
        return paramsStr + "&timestamp=" + str(int(time.time() * 1000))

    def _get_sign(self, payload):
        return hmac.new(SECRET_KEY.encode('utf-8'), payload.encode('utf-8'), hashlib.sha256).hexdigest()

# Функция для получения нужного коннектора
def get_exchange_connector():
    if EXCHANGE == 'MEXC':
        return MEXCConnector()
    elif EXCHANGE == 'BingX':
        return BingXConnector()
    else:
        raise ValueError(f"Биржа {EXCHANGE} не поддерживается.")