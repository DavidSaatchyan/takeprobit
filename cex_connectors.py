from config import EXCHANGE, API_KEY, SECRET_KEY
from abc import ABC, abstractmethod
import requests
import time
import hmac
import hashlib
import json
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from requests.exceptions import ConnectionError
from urllib3.exceptions import ProtocolError

# Абстрактный базовый класс
class ExchangeConnector(ABC):
    @abstractmethod
    def get_ticker(self):
        pass

    @abstractmethod
    def get_candles(self, symbol, interval, start_time, end_time):
        pass

    @abstractmethod
    def place_order(self, symbol, price, quantity, side, order_type, position_side=None, stop_loss=None, take_profit=None):
        pass

    @abstractmethod
    def get_open_orders(self, symbol):
        pass

    @abstractmethod
    def set_leverage(self, symbol, leverage, position_side):
        pass

    @abstractmethod
    def get_last_price(self, symbol):
        pass

    @abstractmethod
    def get_pnl(self, symbol, income_type="REALIZED_PNL", days=1):
        pass

    @abstractmethod
    def cancel_order(self, order_id, symbol):
        pass

    @abstractmethod
    def get_order_history(self, symbol, start_time=None, end_time=None, limit=100):
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

    def place_order(self, symbol, price, quantity, side, order_type, position_side=None, stop_loss=None, take_profit=None, stop_loss_stopPrice=None, take_profit_stopPrice=None):
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

    def set_leverage(self, symbol, leverage, position_side):
        # MEXC не поддерживает изменение плеча через API
        return {"status": "error", "message": "Not supported"}

    def get_last_price(self, symbol):
        # MEXC не поддерживает получение последней цены через API
        return 0

    def get_pnl(self, symbol, income_type="REALIZED_PNL", days=1):
        # MEXC не поддерживает получение PnL через API
        return {"status": "error", "message": "Not supported"}

    def cancel_order(self, order_id, symbol):
        # MEXC не поддерживает отмену ордеров через API
        return {"status": "error", "message": "Not supported"}

    def get_order_history(self, symbol, start_time=None, end_time=None, limit=100):
        # MEXC не поддерживает получение истории ордеров через API
        return []

# Класс для BingX
class BingXConnector(ExchangeConnector):
    BASE_URL = "https://open-api.bingx.com"

    def get_ticker(self):
        url = f"{self.BASE_URL}/openApi/swap/v2/quote/ticker"
        response = requests.get(url)
        return response.json().get("data", [])

    @retry(
    stop=stop_after_attempt(3),  # Повторять запрос до 3 раз
    wait=wait_fixed(2),  # Ждать 2 секунды между попытками
    retry=retry_if_exception_type((ConnectionError, ProtocolError))  # Повторять при ConnectionError или ProtocolError
)

    def get_candles(self, symbol, interval='1d', start_time=None, end_time=None):
        formatted_symbol = symbol if symbol.endswith(('-USDT', '-USDC')) else symbol.replace("_", "-")

        params = {
            "symbol": formatted_symbol,
            "interval": interval,
            "limit": 1000
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
        try:
            response = requests.get(url, params=params, headers={'X-BX-APIKEY': API_KEY}, timeout=(10, 30))  # Увеличиваем таймаут
            response.raise_for_status()  # Проверяем, что ответ успешный
        except (ConnectionError, ProtocolError) as e:
            print(f"Ошибка соединения: {e}. Повторная попытка...")
            raise  # Повторяем запрос
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при запросе данных: {e}")
            return []

        if response.status_code == 200:
            data = response.json()
            candles = data.get("data", [])
            return candles
        else:
            return []
   
    def place_order(self, symbol, price, quantity, side, order_type, position_side=None, stop_loss=None, take_profit=None, stop_loss_stopPrice=None, take_profit_stopPrice=None):
        path = "/openApi/swap/v2/trade/order"
        method = "POST"
        paramsMap = {
            "symbol": symbol,
            "side": "BUY" if side == "buy" else "SELL",
            "positionSide": position_side if position_side else ("LONG" if side == "buy" else "SHORT"),
            "type": order_type.upper(),
            "quantity": quantity,  # Используем quantity
            "timestamp": int(time.time() * 1000)
        }

        if order_type.lower() == "limit":  # Добавляем price только для лимитных ордеров
            paramsMap["price"] = price

        if stop_loss:
            paramsMap["stopLoss"] = json.dumps({
                "type": "STOP",
                "stopPrice": stop_loss_stopPrice if stop_loss_stopPrice else stop_loss,  # Используем stop_loss_stopPrice, если он задан
                "price": stop_loss,
                "workingType": "CONTRACT_PRICE",
                "stopGuaranteed": "false"  # Добавляем параметр stopGuaranteed
            })

        if take_profit:
            paramsMap["takeProfit"] = json.dumps({
                "type": "TAKE_PROFIT",
                "stopPrice": take_profit_stopPrice if take_profit_stopPrice else take_profit,  # Используем take_profit_stopPrice, если он задан
                "price": take_profit,
                "workingType": "CONTRACT_PRICE",
                "stopGuaranteed": "false"  # Добавляем параметр stopGuaranteed
            })

        paramsStr = self._parse_params(paramsMap)
        url = f"{self.BASE_URL}{path}?{paramsStr}&signature={self._get_sign(paramsStr)}"
        headers = {'X-BX-APIKEY': API_KEY}
        response = requests.post(url, headers=headers)
        order_response = response.json()

        # Логируем ответ для отладки
        print("Ответ от API BingX (ордер):")
        print(order_response)

        # Проверяем статус ордера
        if order_response.get("code") == 0:
            order_data = order_response.get("data", {}).get("order", {})
            order_status = order_data.get("status")
        
            if order_status in ["NEW", "FILLED"]:
                # Извлекаем необходимые данные об ордере
                order_info = {
                    "status": "success",
                    "side": order_data.get("side"),
                    "price": order_data.get("price"),
                    "quantity": order_data.get("quantity"),
                    "stop_loss": json.loads(order_data.get("stopLoss", "{}")).get("stopPrice"),
                    "take_profit": json.loads(order_data.get("takeProfit", "{}")).get("stopPrice"),
                    "order_id": order_data.get("orderId")
                }
                return order_info
            else:
                return {"status": "error", "message": f"Ордер не был исполнен. Статус: {order_status}"}
        else:
            return {"status": "error", "message": order_response}

    def get_open_orders(self, symbol=None):
        path = "/openApi/swap/v2/trade/openOrders"
        method = "GET"
        paramsMap = {
        "timestamp": int(time.time() * 1000)
        }
        # Добавляем symbol в параметры, если он указан
        if symbol:
            paramsMap["symbol"] = symbol

        paramsStr = self._parse_params(paramsMap)
        url = f"{self.BASE_URL}{path}?{paramsStr}&signature={self._get_sign(paramsStr)}"
        headers = {'X-BX-APIKEY': API_KEY}
        response = requests.get(url, headers=headers)

        # Отладочная информация
        print("Ответ от API BingX (открытые ордера):")
        print(response.json())

        if response.status_code == 200:
            data = response.json()
            # Возвращаем данные в формате, который ожидает bot_gc.py
            return {
                "data": {
                    "orders": data.get("data", {}).get("orders", [])
                }
            }
        return {"data": {"orders": []}}  # Возвращаем пустой список ордеров в случае ошибки

    def set_leverage(self, symbol, leverage, position_side):
        path = "/openApi/swap/v2/trade/leverage"
        method = "POST"
        paramsMap = {
            "symbol": symbol,
            "side": position_side,  # Используем position_side (LONG, SHORT, BOTH)
            "leverage": leverage,
            "timestamp": int(time.time() * 1000)
        }
        paramsStr = self._parse_params(paramsMap)
        url = f"{self.BASE_URL}{path}?{paramsStr}&signature={self._get_sign(paramsStr)}"
        headers = {'X-BX-APIKEY': API_KEY}
        response = requests.post(url, headers=headers)
        return response.json()

    def get_last_price(self, symbol):
        # Форматируем символ, если это необходимо
        formatted_symbol = symbol if symbol.endswith(('-USDT', '-USDC')) else symbol.replace("_", "-")
    
        # Параметры запроса
        params = {
            "symbol": formatted_symbol,
            "timestamp": int(time.time() * 1000)
        }
    
        # Формируем строку параметров
        paramsStr = self._parse_params(params)
    
        # Формируем URL запроса
        url = f"{self.BASE_URL}/openApi/swap/v2/quote/ticker?{paramsStr}&signature={self._get_sign(paramsStr)}"
    
        # Заголовки запроса
        headers = {'X-BX-APIKEY': API_KEY}
    
        # Выполняем запрос
        response = requests.get(url, headers=headers)
    
        # Проверяем статус ответа
        if response.status_code == 200:
            data = response.json()
        
            # Логируем весь ответ для отладки
            print("Ответ от API BingX:")
            print(data)
        
            # Проверяем, что ответ содержит поле 'data'
            if 'data' in data:
                ticker = data['data']
            
                # Проверяем, что ticker не пустой и содержит поле 'lastPrice'
                if ticker and 'lastPrice' in ticker:
                    last_price = ticker['lastPrice']
                    try:
                        # Преобразуем lastPrice в float
                        return float(last_price)
                    except ValueError:
                        print(f"Ошибка: не удалось преобразовать lastPrice в число для символа {symbol}.")
                        return None
                else:
                    print(f"Ошибка: поле 'lastPrice' отсутствует в ответе для символа {symbol}.")
                    return None
            else:
                print(f"Ошибка: поле 'data' отсутствует в ответе для символа {symbol}.")
                return None
        else:
            print(f"Ошибка при запросе последней цены: {response.status_code}, {response.text}")
            return None

    def get_pnl(self, symbol=None, income_type="REALIZED_PNL", days=1):
        path = "/openApi/swap/v2/user/income"
        method = "GET"
    
        end_time = int(time.time() * 1000)
        start_time = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
    
        paramsMap = {
            "incomeType": income_type,
            "startTime": start_time,
            "endTime": end_time,
            "limit": 1000,
            "timestamp": int(time.time() * 1000)
        }
    
        # Добавляем symbol в параметры, если он указан
        if symbol:
            paramsMap["symbol"] = symbol
    
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

    def cancel_order(self, order_id, symbol):
        path = "/openApi/swap/v2/trade/order"
        method = "DELETE"
        paramsMap = {
            "orderId": order_id,
            "symbol": symbol,
            "timestamp": int(time.time() * 1000)
        }
        paramsStr = self._parse_params(paramsMap)
        url = f"{self.BASE_URL}{path}?{paramsStr}&signature={self._get_sign(paramsStr)}"
        headers = {'X-BX-APIKEY': API_KEY}
        response = requests.delete(url, headers=headers)
        return response.json()

    def get_order_history(self, symbol, start_time=None, end_time=None, limit=100):
        path = "/openApi/swap/v1/trade/fullOrder"
        method = "GET"
        paramsMap = {
            "symbol": symbol,
            "limit": limit,
            "timestamp": int(time.time() * 1000)
        }

        if start_time:
            paramsMap["startTime"] = start_time
        if end_time:
           paramsMap["endTime"] = end_time
        
        paramsStr = self._parse_params(paramsMap)
        url = f"{self.BASE_URL}{path}?{paramsStr}&signature={self._get_sign(paramsStr)}"
        headers = {'X-BX-APIKEY': API_KEY}
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            try:
                data = response.json()
                # Извлекаем список ордеров из поля data.orders
                if isinstance(data, dict) and "data" in data and "orders" in data["data"]:
                    return data["data"]["orders"]
                else:
                     print(f"Ошибка: неожиданный формат ответа: {data}")
                     return []
            except json.JSONDecodeError:
                print("Ошибка: не удалось декодировать JSON из ответа: {response.text}")
                return []
        else:
            print(f"Ошибка при запросе истории ордеров: {response.status_code}, {response.text}")
            return []

    def _parse_params(self, paramsMap):
        sortedKeys = sorted(paramsMap)
        paramsStr = "&".join([f"{key}={paramsMap[key]}" for key in sortedKeys])
        return paramsStr + "&timestamp=" + str(int(time.time() * 1000))

    def _get_sign(self, payload):
        return hmac.new(SECRET_KEY.encode('utf-8'), payload.encode('utf-8'), hashlib.sha256).hexdigest()
    
    def generate_listen_key(self):
        """Генерация listenKey."""
        path = "/openApi/user/auth/userDataStream"
        method = "POST"
        paramsMap = {
            "timestamp": int(time.time() * 1000)
        }
        paramsStr = self._parse_params(paramsMap)
        url = f"{self.BASE_URL}{path}?{paramsStr}&signature={self._get_sign(paramsStr)}"
        headers = {'X-BX-APIKEY': API_KEY}
        response = requests.post(url, headers=headers)

        # Логируем сырой ответ
        print(f"Ответ от сервера при генерации listenKey: {response.text}")

        # Проверяем, что ответ не пустой
        if not response.text:
            print("Пустой ответ от сервера при генерации listenKey")
            return None

        try:
            return response.json()
        except json.JSONDecodeError:
            print("Ошибка декодирования JSON. Ответ сервера:", response.text)
            return None

    def renew_listen_key(self, listen_key):
        """Обновление listenKey."""
        path = "/openApi/user/auth/userDataStream"
        method = "PUT"
        paramsMap = {
            "listenKey": listen_key,
            "timestamp": int(time.time() * 1000)
        }
        paramsStr = self._parse_params(paramsMap)
        url = f"{self.BASE_URL}{path}?{paramsStr}&signature={self._get_sign(paramsStr)}"
        headers = {'X-BX-APIKEY': API_KEY}
        response = requests.put(url, headers=headers)

        # Логируем сырой ответ
        print(f"Ответ от сервера при обновлении listenKey: {response.text}")

        # Проверяем, что ответ не пустой
        if not response.text:
            print("Пустой ответ от сервера при обновлении listenKey")
            return None

        try:
            return response.json()
        except json.JSONDecodeError:
            print("Ошибка декодирования JSON. Ответ сервера:", response.text)
            return None

# Функция для получения нужного коннектора
def get_exchange_connector():
    if EXCHANGE == 'MEXC':
        return MEXCConnector()
    elif EXCHANGE == 'BingX':
        return BingXConnector()
    else:
        raise ValueError(f"Биржа {EXCHANGE} не поддерживается.")