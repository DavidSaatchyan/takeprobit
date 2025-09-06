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
import logging
import math
import decimal
from precision import round_price

# Абстрактный базовый класс
class ExchangeConnector(ABC):
    @abstractmethod
    def get_ticker(self):
        pass

    @abstractmethod
    def get_candles(self, symbol, interval, start_time, end_time):
        pass

    @abstractmethod
    def place_order(self, symbol, price, quantity, side, order_type, position_side=None, stop_loss=None, take_profit=None, stop_loss_stopPrice=None, take_profit_stopPrice=None, trailing_tp_callback_rate=None, trailing_tp_activation_price=None):
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

    def place_order(self, symbol, price, quantity, side, order_type, position_side=None, stop_loss=None, take_profit=None, stop_loss_stopPrice=None, take_profit_stopPrice=None, trailing_tp_callback_rate=None, trailing_tp_activation_price=None):
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
   
    def place_order(self, symbol, price, quantity, side, order_type, position_side=None, stop_loss=None, take_profit=None, stop_loss_stopPrice=None, take_profit_stopPrice=None, trailing_tp_callback_rate=None, trailing_tp_activation_price=None, **kwargs):
        path = "/openApi/swap/v2/trade/order"
        method = "POST"
        paramsMap = {
            "symbol": symbol,
            "side": "BUY" if side == "buy" else "SELL",
            "positionSide": position_side if position_side else ("LONG" if side == "buy" else "SHORT"),
            "type": order_type.upper(),
            "quantity": quantity,
            "timestamp": int(time.time() * 1000)
        }

        # Добавляем stopLoss, если задан
        if stop_loss is not None:
            # Используем stop_loss_stopPrice для stopPrice, если передан, иначе fallback на stop_loss
            stop_price = stop_loss_stopPrice if stop_loss_stopPrice is not None else stop_loss
            stop_loss_json = {
                "type": "STOP",
                "stopPrice": stop_price,  # Цена активации SL
                "price": stop_loss,       # Цена исполнения SL
                "workingType": "MARK_PRICE"
            }
            paramsMap["stopLoss"] = json.dumps(stop_loss_json)
            print(f"🔧 SL JSON: {json.dumps(stop_loss_json, indent=2)}")
            print(f"🔧 SL параметры: stopPrice={stop_price} (активация), price={stop_loss} (исполнение)")
        # Для BingX и Binance:
        # --- TP ---
        if take_profit is not None:
            stop_price = take_profit_stopPrice if take_profit_stopPrice is not None else take_profit
            # Округление
            if 'tick_size' in kwargs and stop_price is not None:
                stop_price = round_price(float(stop_price), float(kwargs['tick_size']))
            take_profit_json = {
                "type": "TAKE_PROFIT",
                "stopPrice": stop_price,
                "price": take_profit,
                "workingType": "MARK_PRICE"
            }
            paramsMap["takeProfit"] = json.dumps(take_profit_json)

        # Для TRAILING_STOP_MARKET добавляем priceRate и activationPrice
        if order_type.upper() == "TRAILING_STOP_MARKET":
            if trailing_tp_callback_rate is not None:
                paramsMap["priceRate"] = float(trailing_tp_callback_rate)
            if trailing_tp_activation_price is not None:
                paramsMap["activationPrice"] = float(trailing_tp_activation_price)
            # Не добавляем price для этого типа ордера
            paramsMap.pop("price", None)
        # Для лимитного ордера обязательно указываем price
        if order_type.lower() == "limit":
            paramsMap["price"] = price
        print("DEBUG paramsMap:", paramsMap)  # Для отладки

        paramsStr = self._parse_params(paramsMap)
        url = f"{self.BASE_URL}{path}?{paramsStr}&signature={self._get_sign(paramsStr)}"
        headers = {'X-BX-APIKEY': API_KEY}
        response = requests.post(url, headers=headers)
        try:
            order_response = response.json()
        except Exception as e:
            print(f"Ошибка парсинга JSON: {e}")
            print(f"HTTP статус: {response.status_code}, текст: {response.text}")
            return {"status": "error", "message": f"JSON parse error: {e}", "raw": response.text, "http_status": response.status_code}

        # Логируем ответ для отладки
        print("Ответ от API BingX (ордер):")
        print(order_response)

        # Проверяем статус ордера
        if order_response.get("code") == 0:
            order_data = order_response.get("data", {}).get("order", {})
            order_status = order_data.get("status")
        
            if order_status in ["NEW", "FILLED"]:
                # Корректно обрабатываем stopLoss и takeProfit
                stop_loss_raw = order_data.get("stopLoss", "")
                if stop_loss_raw:
                    stop_loss_val = json.loads(stop_loss_raw).get("stopPrice")
                else:
                    stop_loss_val = None

                take_profit_raw = order_data.get("takeProfit", "")
                if take_profit_raw:
                    take_profit_val = json.loads(take_profit_raw).get("stopPrice")
                else:
                    take_profit_val = None

                order_info = {
                    "status": "success",
                    "side": order_data.get("side"),
                    "price": order_data.get("price"),
                    "quantity": order_data.get("quantity"),
                    "stop_loss": stop_loss_val,
                    "take_profit": take_profit_val,
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

    def set_tp_sl(self, symbol, position_side, take_profit=None, stop_loss=None, stop_loss_stopPrice=None, take_profit_stopPrice=None, **kwargs):
        """
        Устанавливает TP/SL (в том числе трейлинг-TP) для открытой позиции на BingX.
        take_profit — строка (JSON) для трейлинг-TP, либо число для обычного TP.
        stop_loss — число, stop_loss_stopPrice — число (если нужно).
        """
        path = "/openApi/swap/v2/trade/modifyTPSL"
        method = "POST"
        paramsMap = {
            "symbol": symbol,
            "positionSide": position_side,
            "timestamp": int(time.time() * 1000)
        }
        if take_profit:
            stop_price = take_profit_stopPrice if take_profit_stopPrice is not None else take_profit
            # Округление
            if 'tick_size' in kwargs and stop_price is not None:
                stop_price = round_price(float(stop_price), float(kwargs['tick_size']))
            # Если это строка (JSON трейлинг-TP), передаём как есть
            if isinstance(take_profit, str):
                paramsMap["takeProfit"] = take_profit
            else:
                paramsMap["takeProfit"] = json.dumps({
                    "type": "TAKE_PROFIT",
                    "stopPrice": stop_price,
                    "price": take_profit,
                    "workingType": "CONTRACT_PRICE"
                })
        if stop_loss:
            paramsMap["stopLoss"] = json.dumps({
                "type": "STOP",
                "stopPrice": stop_loss_stopPrice if stop_loss_stopPrice is not None else stop_loss,
                "price": stop_loss,
                "workingType": "CONTRACT_PRICE",
                "stopGuaranteed": "false"
            })
        paramsStr = self._parse_params(paramsMap)
        url = f"{self.BASE_URL}{path}?{paramsStr}&signature={self._get_sign(paramsStr)}"
        headers = {'X-BX-APIKEY': API_KEY}
        response = requests.post(url, headers=headers)
        return response.json()

class BinanceConnector(ExchangeConnector):
    BASE_URL = "https://fapi.binance.com"
    MAX_RETRIES = 3
    RETRY_SLEEP = 2

    def _sync_time(self):
        url = f"{self.BASE_URL}/fapi/v1/time"
        try:
            response = requests.get(url)
            server_time = response.json().get('serverTime')
            if server_time:
                local_time = int(time.time() * 1000)
                offset = server_time - local_time
                return offset
        except Exception as e:
            logging.warning(f"[Binance] Не удалось синхронизировать время: {e}")
        return 0

    def _sign(self, params: dict) -> dict:
        # Удаляем все None
        params = {k: v for k, v in params.items() if v is not None}
        # Все значения приводим к строке
        params = {k: str(v) for k, v in params.items()}
        offset = getattr(self, '_time_offset', 0)
        params['timestamp'] = str(int(time.time() * 1000) + offset)
        # Строка для подписи — отсортированные параметры
        query_string = '&'.join([f"{k}={params[k]}" for k in sorted(params)])
        signature = hmac.new(SECRET_KEY.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
        params['signature'] = signature
        return params

    def _headers(self):
        return {'X-MBX-APIKEY': API_KEY}

    def _headers_post(self):
        return {
            'X-MBX-APIKEY': API_KEY,
            'Content-Type': 'application/x-www-form-urlencoded'
        }

    def _request(self, method, url, params=None, data=None, headers=None, retries=MAX_RETRIES):
        for attempt in range(retries):
            try:
                if method == 'GET':
                    resp = requests.get(url, params=params, headers=headers)
                elif method == 'POST':
                    resp = requests.post(url, data=data, headers=headers)
                elif method == 'DELETE':
                    resp = requests.delete(url, params=params, headers=headers)
                else:
                    raise ValueError(f"Unsupported method: {method}")
                if resp.status_code == 429 or resp.status_code == 418:
                    logging.warning(f"[Binance] Rate limit hit (HTTP {resp.status_code}), sleeping {self.RETRY_SLEEP}s...")
                    time.sleep(self.RETRY_SLEEP)
                    continue
                if resp.status_code == 400 and 'timestamp' in resp.text:
                    # Проблема с временем — синхронизируем
                    self._time_offset = self._sync_time()
                    logging.info("[Binance] Синхронизировано время с сервером.")
                    continue
                try:
                    data = resp.json()
                except Exception as e:
                    logging.error(f"[Binance] Ошибка парсинга JSON: {e}, raw: {resp.text}")
                    return {"status": "error", "message": f"JSON parse error: {e}", "raw": resp.text, "http_status": resp.status_code}
                if 'code' in data and data['code'] < 0:
                    logging.error(f"[Binance] Ошибка API: {data}")
                return data
            except Exception as e:
                logging.error(f"[Binance] Ошибка запроса: {e}")
                time.sleep(self.RETRY_SLEEP)
        return {"status": "error", "message": "Max retries exceeded"}

    def get_ticker(self):
        url = f"{self.BASE_URL}/fapi/v1/ticker/24hr"
        return self._request('GET', url)

    def get_candles(self, symbol, interval, start_time, end_time):
        url = f"{self.BASE_URL}/fapi/v1/klines"
        params = {
            'symbol': symbol.replace('-', ''),
            'interval': interval,
            'startTime': start_time,
            'endTime': end_time,
            'limit': 1000
        }
        return self._request('GET', url, params=params)

    def place_order(self, symbol, price, quantity, side, order_type, position_side=None, stop_loss=None, take_profit=None, stop_loss_stopPrice=None, take_profit_stopPrice=None, trailing_tp_callback_rate=None, trailing_tp_activation_price=None, client_order_id=None, reduceOnly=None):
        # Очищаем символ от дефисов
        clean_symbol = symbol.replace('-', '').replace('_', '')
        
        url = f"{self.BASE_URL}/fapi/v1/order"
        params = {
            'symbol': clean_symbol,
            'side': side.upper(),
            'quantity': quantity,
            'newClientOrderId': client_order_id if client_order_id else f"bot_{int(time.time())}"
        }
        
        # Добавляем reduceOnly строго по доке Binance (только если явно передан)
        if reduceOnly is not None:
            params['reduceOnly'] = 'true' if reduceOnly else 'false'
        
        # Определяем тип ордера и параметры
        if trailing_tp_callback_rate is not None and trailing_tp_activation_price is not None:
            # Создаем трейлинг-стоп ордер
            params['type'] = 'TRAILING_STOP_MARKET'
            params['callbackRate'] = trailing_tp_callback_rate
            params['activationPrice'] = trailing_tp_activation_price  # Всегда activationPrice с большой буквы
        else:
            # Обычный ордер
            params['type'] = order_type.upper()
            # Корректно формируем параметры для разных типов ордеров
            if order_type.lower() == 'limit':
                params['price'] = price
                params['timeInForce'] = 'GTC'
            # --- КРИТИЧЕСКО: правильная обработка stopPrice для SL и TP ---
            if order_type.lower() in ('stop', 'stop_market'):
                params['stopPrice'] = stop_loss_stopPrice if stop_loss_stopPrice else stop_loss
            if order_type.lower() in ('take_profit', 'take_profit_market'):
                params['stopPrice'] = take_profit_stopPrice if take_profit_stopPrice else take_profit
            if price and order_type.lower() not in ('stop_market', 'take_profit_market'):
                params['price'] = price
        
        # Удаляем None значения
        params = {k: v for k, v in params.items() if v is not None}
        params = self._sign(params)
        headers = self._headers_post()
        
        # Формируем data_string в том же порядке, что и для подписи
        sorted_params = dict(sorted([(k, v) for k, v in params.items() if k != 'signature']))
        query_string = '&'.join([f"{k}={v}" for k, v in sorted_params.items()])
        data_string = query_string + f"&signature={params['signature']}"
        
        resp = requests.post(url, data=data_string, headers=headers)
        try:
            data = resp.json()
        except Exception as e:
            logging.error(f"[Binance] Ошибка парсинга JSON: {e}, raw: {resp.text}")
            return {"status": "error", "message": f"JSON parse error: {e}", "raw": resp.text, "http_status": resp.status_code}
        # --- ЛОГИРУЕМ ПОЛНЫЙ ОТВЕТ ОТ BINANCE ---
        logging.info(f"[Binance][place_order] Ответ API: {data}")
        if 'code' in data and data['code'] < 0:
            logging.error(f"[Binance] Ошибка API: {data}")
        # --- КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: всегда возвращаем orderId и clientOrderId на верхнем уровне ---
        if 'orderId' in data:
            return {**data, 'orderId': data['orderId'], 'clientOrderId': data.get('clientOrderId')}
        elif 'data' in data and isinstance(data['data'], dict) and 'orderId' in data['data']:
            return {**data, 'orderId': data['data']['orderId'], 'clientOrderId': data['data'].get('clientOrderId')}
        if 'clientOrderId' in data:
            return {**data, 'orderId': data['orderId'], 'clientOrderId': data['clientOrderId']}
        elif 'data' in data and isinstance(data['data'], dict) and 'clientOrderId' in data['data']:
            return {**data, 'orderId': data['data']['orderId'], 'clientOrderId': data['data']['clientOrderId']}
        return data

    def get_open_orders(self, symbol):
        # Очищаем символ от дефисов
        clean_symbol = symbol.replace('-', '').replace('_', '')
        
        url = f"{self.BASE_URL}/fapi/v1/openOrders"
        params = {'symbol': clean_symbol}
        params = self._sign(params)
        headers = self._headers()
        resp = requests.get(url, params=params, headers=headers)
        try:
            data = resp.json()
        except Exception as e:
            logging.error(f"[Binance] Ошибка парсинга JSON: {e}, raw: {resp.text}")
            return {"status": "error", "message": f"JSON parse error: {e}", "raw": resp.text, "http_status": resp.status_code}
        if 'code' in data and data['code'] < 0:
            logging.error(f"[Binance] Ошибка API: {data}")
        return data

    def set_leverage(self, symbol, leverage, position_side):
        # Очищаем символ от дефисов
        clean_symbol = symbol.replace('-', '').replace('_', '')
        
        url = f"{self.BASE_URL}/fapi/v1/leverage"
        params = {'symbol': clean_symbol, 'leverage': leverage}
        params = self._sign(params)
        headers = self._headers_post()
        sorted_params = dict(sorted([(k, v) for k, v in params.items() if k != 'signature']))
        query_string = '&'.join([f"{k}={v}" for k, v in sorted_params.items()])
        data_string = query_string + f"&signature={params['signature']}"
        resp = requests.post(url, data=data_string, headers=headers)
        try:
            data = resp.json()
        except Exception as e:
            logging.error(f"[Binance] Ошибка парсинга JSON: {e}, raw: {resp.text}")
            return {"status": "error", "message": f"JSON parse error: {e}", "raw": resp.text, "http_status": resp.status_code}
        if 'code' in data and data['code'] < 0:
            logging.error(f"[Binance] Ошибка API: {data}")
        return data

    def get_last_price(self, symbol):
        url = f"{self.BASE_URL}/fapi/v1/ticker/price"
        params = {'symbol': symbol.replace('-', '')}
        resp = self._request('GET', url, params=params)
        return float(resp['price']) if 'price' in resp else None

    def get_pnl(self, symbol, income_type="REALIZED_PNL", days=1):
        url = f"{self.BASE_URL}/fapi/v1/income"
        end_time = int(time.time() * 1000)
        start_time = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
        params = {
            'symbol': symbol.replace('-', ''),
            'incomeType': income_type,
            'startTime': start_time,
            'endTime': end_time,
            'limit': 1000
        }
        params = self._sign(params)
        return self._request('GET', url, params=params, headers=self._headers())

    def cancel_order(self, order_id, symbol):
        # Очищаем символ от дефисов
        clean_symbol = symbol.replace('-', '').replace('_', '')
        
        url = f"{self.BASE_URL}/fapi/v1/order"
        params = {'symbol': clean_symbol, 'orderId': order_id}
        params = self._sign(params)
        headers = self._headers()
        
        # Для DELETE запросов Binance требует параметры в URL
        # Формируем query string в том же порядке, что и для подписи
        sorted_params = dict(sorted([(k, v) for k, v in params.items() if k != 'signature']))
        query_string = '&'.join([f"{k}={v}" for k, v in sorted_params.items()])
        full_url = f"{url}?{query_string}&signature={params['signature']}"
        
        resp = requests.delete(full_url, headers=headers)
        try:
            data = resp.json()
        except Exception as e:
            logging.error(f"[Binance] Ошибка парсинга JSON: {e}, raw: {resp.text}")
            return {"status": "error", "message": f"JSON parse error: {e}", "raw": resp.text, "http_status": resp.status_code}
        if 'code' in data and data['code'] < 0:
            logging.error(f"[Binance] Ошибка API: {data}")
        return data

    def get_order_history(self, symbol, start_time=None, end_time=None, limit=100):
        url = f"{self.BASE_URL}/fapi/v1/allOrders"
        params = {
            'symbol': symbol.replace('-', ''),
            'limit': limit,
            'startTime': start_time,
            'endTime': end_time
        }
        params = {k: v for k, v in params.items() if v is not None}
        params = self._sign(params)
        return self._request('GET', url, params=params, headers=self._headers())

    def set_margin_type(self, symbol, margin_type='ISOLATED'):
        """
        Устанавливает тип маржи для символа (ISOLATED или CROSSED).
        Если маржа уже установлена, Binance вернет ошибку -4066 — это не критично.
        """
        # Очищаем символ от дефисов
        clean_symbol = symbol.replace('-', '').replace('_', '')
        
        url = f"{self.BASE_URL}/fapi/v1/marginType"
        params = {
            'symbol': clean_symbol,
            'marginType': margin_type.upper()
        }
        params = self._sign(params)
        headers = self._headers_post()
        sorted_params = dict(sorted([(k, v) for k, v in params.items() if k != 'signature']))
        query_string = '&'.join([f"{k}={v}" for k, v in sorted_params.items()])
        data_string = query_string + f"&signature={params['signature']}"
        resp = requests.post(url, data=data_string, headers=headers)
        try:
            data = resp.json()
        except Exception as e:
            logging.error(f"[Binance] Ошибка парсинга JSON: {e}, raw: {resp.text}")
            return {"status": "error", "message": f"JSON parse error: {e}", "raw": resp.text, "http_status": resp.status_code}
        if 'code' in data and data['code'] < 0:
            logging.error(f"[Binance] Ошибка API: {data}")
        # Если маржа уже установлена, Binance вернет ошибку -4066, это не критично
        if isinstance(data, dict) and data.get('code') == -4066:
            logging.info(f"[Binance] Маржа для {symbol} уже {margin_type}")
            return {'status': 'ok', 'message': 'Already set'}
        return data

    def generate_listen_key(self):
        """Генерация listenKey для WebSocket."""
        url = f"{self.BASE_URL}/fapi/v1/listenKey"
        headers = self._headers()
        try:
            resp = requests.post(url, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                listen_key = data.get("listenKey")
                logging.info(f"[Binance] Получен listenKey: {listen_key}")
                return {"status": "ok", "listenKey": listen_key}
            else:
                logging.error(f"[Binance] Ошибка получения listenKey: HTTP {resp.status_code}, {resp.text}")
                return {"status": "error", "message": f"HTTP {resp.status_code}", "raw": resp.text}
        except Exception as e:
            logging.error(f"[Binance] Ошибка получения listenKey: {e}")
            return {"status": "error", "message": str(e)}

    def renew_listen_key(self, listen_key):
        """Обновление listenKey."""
        url = f"{self.BASE_URL}/fapi/v1/listenKey"
        headers = self._headers()
        try:
            resp = requests.put(url, headers=headers, params={"listenKey": listen_key})
            if resp.status_code == 200:
                logging.info("[Binance] listenKey успешно продлён")
                return {"status": "ok"}
            else:
                logging.warning(f"[Binance] Не удалось продлить listenKey: {resp.text}")
                return {"status": "error", "message": f"HTTP {resp.status_code}", "raw": resp.text}
        except Exception as e:
            logging.error(f"[Binance] Ошибка продления listenKey: {e}")
            return {"status": "error", "message": str(e)}

    def get_symbol_info(self, symbol):
        """
        Получает актуальную информацию о символе через API Binance
        Включает точность цены и количества, минимальные размеры и т.д.
        """
        # Очищаем символ от дефисов
        clean_symbol = symbol.replace('-', '').replace('_', '')
        
        url = f"{self.BASE_URL}/fapi/v1/exchangeInfo"
        try:
            resp = requests.get(url)
            if resp.status_code == 200:
                data = resp.json()
                symbols = data.get('symbols', [])
                
                # Ищем нужный символ
                for symbol_info in symbols:
                    if symbol_info['symbol'] == clean_symbol:
                        return {
                            'symbol': symbol_info['symbol'],
                            'pricePrecision': symbol_info.get('pricePrecision', 2),
                            'quantityPrecision': symbol_info.get('quantityPrecision', 3),
                            'baseAssetPrecision': symbol_info.get('baseAssetPrecision', 8),
                            'quotePrecision': symbol_info.get('quotePrecision', 8),
                            'filters': symbol_info.get('filters', []),
                            'status': symbol_info.get('status', 'UNKNOWN')
                        }
                
                # Если символ не найден, возвращаем значения по умолчанию
                logging.warning(f"[Binance] Символ {clean_symbol} не найден в exchangeInfo")
                return {
                    'symbol': clean_symbol,
                    'pricePrecision': 2,
                    'quantityPrecision': 3,
                    'baseAssetPrecision': 8,
                    'quotePrecision': 8,
                    'filters': [],
                    'status': 'UNKNOWN'
                }
            else:
                logging.error(f"[Binance] Ошибка получения exchangeInfo: HTTP {resp.status_code}")
                return None
        except Exception as e:
            logging.error(f"[Binance] Ошибка получения информации о символе {clean_symbol}: {e}")
            return None

    def round_price_precise(self, price: float, tick_size: float) -> float:
        """
        Точное округление цены до кратного tick_size с использованием Decimal
        """
        try:
            if tick_size <= 0:
                logging.warning(f"[Binance] tick_size должен быть положительным, получен: {tick_size}")
                return price
            
            # Используем Decimal для точных вычислений
            price_decimal = decimal.Decimal(str(price))
            tick_size_decimal = decimal.Decimal(str(tick_size))
            
            # Округляем до ближайшего кратного tick_size
            rounded_decimal = (price_decimal / tick_size_decimal).quantize(
                decimal.Decimal('1'), rounding=decimal.ROUND_HALF_UP
            ) * tick_size_decimal
            
            return float(rounded_decimal)
        except Exception as e:
            logging.error(f"[Binance] Ошибка в round_price_precise: {e}, price={price}, tick_size={tick_size}")
            return price

    def round_quantity_precise(self, quantity: float, step_size: float) -> float:
        """
        Точное округление количества до кратного step_size с использованием Decimal
        """
        try:
            if step_size <= 0:
                logging.warning(f"[Binance] step_size должен быть положительным, получен: {step_size}")
                return quantity
            
            # Используем Decimal для точных вычислений
            quantity_decimal = decimal.Decimal(str(quantity))
            step_size_decimal = decimal.Decimal(str(step_size))
            
            # Округляем до ближайшего кратного step_size
            rounded_decimal = (quantity_decimal / step_size_decimal).quantize(
                decimal.Decimal('1'), rounding=decimal.ROUND_HALF_UP
            ) * step_size_decimal
            
            return float(rounded_decimal)
        except Exception as e:
            logging.error(f"[Binance] Ошибка в round_quantity_precise: {e}, quantity={quantity}, step_size={step_size}")
            return quantity

    def get_symbol_precision(self, symbol):
        """
        Получает точность для символа (price и quantity precision)
        Возвращает словарь с price_precision и quantity_precision
        """
        symbol_info = self.get_symbol_info(symbol)
        if symbol_info:
            # Извлекаем реальные фильтры из API
            filters = symbol_info.get('filters', [])
            tick_size = None
            step_size = None
            
            for filter_info in filters:
                filter_type = filter_info.get('filterType')
                if filter_type == 'PRICE_FILTER':
                    tick_size = float(filter_info.get('tickSize', '0.01'))
                elif filter_type == 'LOT_SIZE':
                    step_size = float(filter_info.get('stepSize', '0.001'))
            
            # Если получили фильтры, используем их для расчета точности
            if tick_size and step_size:
                price_precision = int(round(-math.log(tick_size, 10), 0))
                quantity_precision = int(round(-math.log(step_size, 10), 0))
                return {
                    'price_precision': price_precision,
                    'quantity_precision': quantity_precision,
                    'tick_size': tick_size,
                    'step_size': step_size
                }
            else:
                # Fallback на pricePrecision/quantityPrecision если фильтры не найдены
                return {
                    'price_precision': symbol_info['pricePrecision'],
                    'quantity_precision': symbol_info['quantityPrecision']
                }
        else:
            # Fallback на безопасные значения по умолчанию
            return {'price_precision': 2, 'quantity_precision': 3}

# Функция для получения нужного коннектора
def get_exchange_connector():
    if EXCHANGE == 'MEXC':
        return MEXCConnector()
    elif EXCHANGE == 'BingX':
        return BingXConnector()
    elif EXCHANGE == 'Binance':
        return BinanceConnector()
    else:
        raise ValueError(f"Биржа {EXCHANGE} не поддерживается.")