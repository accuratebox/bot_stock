import time
from typing import Any

import requests

from config import settings
from runtime.alpaca_state import AlpacaRuntimeState


class MarketDataService:
    def __init__(self, logger: Any, account_name: str | None = None, runtime_state: AlpacaRuntimeState | None = None) -> None:
        self.logger = logger
        self._session = requests.Session()
        self._cache: dict[str, tuple[float, float]] = {}
        self._quote_cache: dict[str, tuple[float, dict]] = {}
        self._bars_cache: dict[str, tuple[float, list[dict]]] = {}
        self._cache_ttl_seconds = 30.0
        self._max_retries = 3
        self.endpoint = settings.alpaca_endpoint
        self.api_key = settings.alpaca_api_key
        self.api_secret = settings.alpaca_api_secret
        self.account_name = account_name or self.endpoint
        self.runtime_state = runtime_state or AlpacaRuntimeState()

    def set_connection(self, endpoint: str, api_key: str, api_secret: str) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.api_key = api_key
        self.api_secret = api_secret
        self.account_name = self.account_name or self.endpoint

    def update_latest_price(self, symbol: str, price: float) -> None:
        normalized = symbol.upper().replace(" ", "")
        self._cache[normalized] = (time.time(), float(price))
        self.runtime_state.set_latest_price_value(self.account_name, normalized, float(price))

    def get_last_price(self, symbol: str) -> float:
        normalized = symbol.upper().replace(" ", "")

        cached = self._cache.get(normalized)
        if cached is not None:
            cached_at, cached_price = cached
            if (time.time() - cached_at) <= self._cache_ttl_seconds:
                return cached_price

        shared_price = self.runtime_state.get_latest_price(self.account_name, normalized, ttl_seconds=30.0)
        if shared_price is not None:
            self._cache[normalized] = (time.time(), float(shared_price))
            return float(shared_price)

        if self._is_crypto_symbol(normalized):
            value = self._get_crypto_price_alpaca(normalized)
        else:
            value = self._get_stock_price_alpaca(normalized)
        self._cache[normalized] = (time.time(), value)
        self.runtime_state.set_latest_price_value(self.account_name, normalized, value)
        return value

    def get_latest_quote(self, symbol: str) -> dict:
        normalized = symbol.upper().replace(" ", "")
        cached = self._quote_cache.get(normalized)
        if cached is not None:
            cached_at, cached_quote = cached
            if (time.time() - cached_at) <= self._cache_ttl_seconds:
                return cached_quote

        if self._is_crypto_symbol(normalized):
            url = "https://data.alpaca.markets/v1beta3/crypto/us/latest/quotes"
            params = {"symbols": self._to_crypto_data_symbol(normalized)}
        else:
            url = f"https://data.alpaca.markets/v2/stocks/{normalized}/quotes/latest"
            params = {"feed": "iex"}
        headers = self._auth_headers()
        payload = self._request_json_with_retry(url=url, params=params, headers=headers)

        if self._is_crypto_symbol(normalized):
            quote = payload.get("quotes", {}).get(self._to_crypto_data_symbol(normalized), {})
            bid = float(quote.get("bp", 0.0) or 0.0)
            ask = float(quote.get("ap", 0.0) or 0.0)
            bid_size = quote.get("bs", 0)
            ask_size = quote.get("as", 0)
            timestamp = quote.get("t")
        else:
            quote = payload.get("quote", {})
            bid = float(quote.get("bp", 0.0) or 0.0)
            ask = float(quote.get("ap", 0.0) or 0.0)
            bid_size = quote.get("bs", 0)
            ask_size = quote.get("as", 0)
            timestamp = quote.get("t")

        result = {
            "bid": bid,
            "ask": ask,
            "bid_size": bid_size,
            "ask_size": ask_size,
            "timestamp": timestamp,
        }
        spread = max(result["ask"] - result["bid"], 0.0)
        mid = (result["ask"] + result["bid"]) / 2 if result["ask"] and result["bid"] else 0.0
        result["spread"] = spread
        result["spread_pct"] = (spread / mid * 100.0) if mid > 0 else 0.0

        self._quote_cache[normalized] = (time.time(), result)
        self.runtime_state.set_quote(self.account_name, normalized, result)
        return result

    def get_latest_trade(self, symbol: str) -> dict:
        normalized = symbol.upper().replace(" ", "")
        if self._is_crypto_symbol(normalized):
            url = "https://data.alpaca.markets/v1beta3/crypto/us/latest/trades"
            params = {"symbols": self._to_crypto_data_symbol(normalized)}
        else:
            url = f"https://data.alpaca.markets/v2/stocks/{normalized}/trades/latest"
            params = {"feed": "iex"}
        headers = self._auth_headers()
        payload = self._request_json_with_retry(url=url, params=params, headers=headers)
        if self._is_crypto_symbol(normalized):
            trade = payload.get("trades", {}).get(self._to_crypto_data_symbol(normalized), {})
        else:
            trade = payload.get("trade", {})
        return {
            "price": float(trade.get("p", 0.0) or 0.0),
            "size": trade.get("s", 0),
            "timestamp": trade.get("t"),
        }

    def get_stock_bars(self, symbol: str, interval: str = "1m", limit: int = 100) -> list[dict]:
        normalized = symbol.upper().replace(" ", "")
        cache_key = f"{normalized}:{interval}:{limit}"
        cached = self._bars_cache.get(cache_key)
        if cached is not None:
            cached_at, cached_bars = cached
            if (time.time() - cached_at) <= self._cache_ttl_seconds:
                return cached_bars

        timeframe = self._timeframe_for_interval(interval)
        if self._is_crypto_symbol(normalized):
            url = "https://data.alpaca.markets/v1beta3/crypto/us/bars"
            params = {"timeframe": timeframe, "limit": str(limit), "symbols": self._to_crypto_data_symbol(normalized)}
        else:
            url = f"https://data.alpaca.markets/v2/stocks/{normalized}/bars"
            params = {"timeframe": timeframe, "limit": str(limit), "adjustment": "raw", "feed": "iex"}
        headers = self._auth_headers()
        payload = self._request_json_with_retry(url=url, params=params, headers=headers)

        bars = []
        if self._is_crypto_symbol(normalized):
            source_bars = payload.get("bars", {}).get(self._to_crypto_data_symbol(normalized), [])
        else:
            source_bars = payload.get("bars", [])

        if source_bars is None:
            self.logger.warning("Alpaca devolvio barras vacias para %s (%s)", normalized, interval)
            source_bars = []

        for bar in source_bars:
            bars.append(
                {
                    "open": float(bar.get("o", 0.0) or 0.0),
                    "high": float(bar.get("h", 0.0) or 0.0),
                    "low": float(bar.get("l", 0.0) or 0.0),
                    "close": float(bar.get("c", 0.0) or 0.0),
                    "volume": float(bar.get("v", 0.0) or 0.0),
                    "timestamp": bar.get("t"),
                }
            )

        self._bars_cache[cache_key] = (time.time(), bars)
        return bars

    def _get_stock_price_alpaca(self, symbol: str) -> float:
        payload = self._request_json_with_retry(
            url=f"https://data.alpaca.markets/v2/stocks/{symbol}/trades/latest",
            params={"feed": "iex"},
            headers=self._auth_headers(),
        )

        trade = payload.get("trade", {})
        price = trade.get("p")
        if price is None:
            raise ValueError(f"Sin trade reciente en Alpaca para {symbol}")
        return float(price)

    def _get_crypto_price_alpaca(self, symbol: str) -> float:
        symbol_for_data = self._to_crypto_data_symbol(symbol)
        payload = self._request_json_with_retry(
            url="https://data.alpaca.markets/v1beta3/crypto/us/latest/trades",
            params={"symbols": symbol_for_data},
            headers=self._auth_headers(),
        )

        trade = payload.get("trades", {}).get(symbol_for_data, {})
        price = trade.get("p")
        if price is None:
            raise ValueError(f"Sin trade reciente en Alpaca para {symbol}")
        return float(price)

    def _auth_headers(self) -> dict[str, str]:
        if not self.api_key or not self.api_secret:
            raise ValueError("Credenciales de Alpaca no disponibles para market data")

        return {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.api_secret,
            "Accept": "application/json",
        }

    @staticmethod
    def _timeframe_for_interval(interval: str) -> str:
        mapping = {
            "1m": "1Min",
            "1min": "1Min",
            "5m": "5Min",
            "5min": "5Min",
            "15m": "15Min",
            "15min": "15Min",
        }
        return mapping.get(interval.lower(), "1Min")

    def _request_json_with_retry(
        self,
        url: str,
        params: dict[str, str],
        headers: dict[str, str] | None = None,
    ) -> dict:
        last_error: Exception | None = None
        for attempt in range(self._max_retries):
            try:
                self.runtime_state.acquire(self.account_name)
                response = self._session.get(url, params=params, headers=headers, timeout=15)
                self.runtime_state.record_response(self.account_name, response)
                if response.status_code == 429 and attempt < (self._max_retries - 1):
                    retry_after = response.headers.get("Retry-After")
                    if retry_after is not None and retry_after.isdigit():
                        wait_seconds = float(retry_after)
                    else:
                        wait_seconds = float(2 ** attempt)

                    self.logger.warning("Rate limit (429). Reintentando en %.1f s", wait_seconds)
                    time.sleep(wait_seconds)
                    continue

                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as ex:
                last_error = ex
                if attempt >= (self._max_retries - 1):
                    break
                wait_seconds = float(2 ** attempt)
                self.logger.warning(
                    "Fallo de red/HTTP en market data (%s). Reintentando en %.1f s",
                    ex.__class__.__name__,
                    wait_seconds,
                )
                time.sleep(wait_seconds)

        if last_error is not None:
            raise last_error
        return {}

    def get_candles(self, symbol: str, interval: str = "1m", limit: int = 100) -> list[dict]:
        return self.get_stock_bars(symbol=symbol, interval=interval, limit=limit)

    @staticmethod
    def _is_crypto_symbol(symbol: str) -> bool:
        return "/" in symbol or symbol.endswith("USD")

    @staticmethod
    def _to_crypto_data_symbol(symbol: str) -> str:
        normalized = symbol.upper().replace(" ", "")
        normalized = normalized.replace("/USDC", "/USD")
        normalized = normalized.replace("/USDT", "/USD")
        if normalized.endswith("USDC") and "/" not in normalized:
            normalized = normalized[:-4] + "USD"
        if normalized.endswith("USDT") and "/" not in normalized:
            normalized = normalized[:-4] + "USD"
        if "/" in normalized:
            return normalized
        if normalized.endswith("USD") and len(normalized) > 3:
            return f"{normalized[:-3]}/USD"
        return normalized

    def invalidate_price(self, symbol: str) -> None:
        normalized = symbol.upper().replace(" ", "")
        self._cache.pop(normalized, None)
        self._quote_cache.pop(normalized, None)

    @staticmethod
    def calculate_vwap(candles: list[dict]) -> float:
        total_volume = 0.0
        total_price_volume = 0.0
        for candle in candles:
            close = float(candle.get("close", 0.0) or 0.0)
            volume = float(candle.get("volume", 0.0) or 0.0)
            total_volume += volume
            total_price_volume += close * volume
        if total_volume <= 0:
            return 0.0
        return total_price_volume / total_volume
