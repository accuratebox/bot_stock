import time
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlsplit

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
        self._http_timeout_seconds = int(getattr(settings, "http_timeout_alpaca_seconds", 10) or 10)
        self.provider = str(getattr(settings, "broker_provider", "alpaca") or "alpaca").lower()
        if self.provider == "binance":
            self.endpoint = str(getattr(settings, "binance_demo_endpoint", "https://testnet.binancefuture.com") or "https://testnet.binancefuture.com")
            self.api_key = str(getattr(settings, "binance_demo_key", "") or "")
            self.api_secret = str(getattr(settings, "binance_demo_secret", "") or "")
        else:
            self.endpoint = settings.alpaca_endpoint
            self.api_key = settings.alpaca_api_key
            self.api_secret = settings.alpaca_api_secret
        self.account_name = account_name or self.endpoint
        self.runtime_state = runtime_state or AlpacaRuntimeState()

    def set_connection(self, endpoint: str, api_key: str, api_secret: str) -> None:
        self.endpoint = endpoint.rstrip("/")
        host = str(urlsplit(self.endpoint).netloc or "").lower()
        self.provider = "binance" if "binance" in host else "alpaca"
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

        if self.provider == "binance":
            value = self._get_crypto_price_binance(normalized)
            self._cache[normalized] = (time.time(), value)
            self.runtime_state.set_latest_price_value(self.account_name, normalized, value)
            return value

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

        if self.provider == "binance":
            symbol_for_data = self._to_binance_symbol(normalized)
            payload = self._request_json_with_retry(
                url=f"{self.endpoint}/fapi/v1/ticker/bookTicker",
                params={"symbol": symbol_for_data},
                headers=self._auth_headers(),
            )
            bid = float(payload.get("bidPrice", 0.0) or 0.0)
            ask = float(payload.get("askPrice", 0.0) or 0.0)
            bid_size = float(payload.get("bidQty", 0.0) or 0.0)
            ask_size = float(payload.get("askQty", 0.0) or 0.0)
            result = {
                "bid": bid,
                "ask": ask,
                "bid_size": bid_size,
                "ask_size": ask_size,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            spread = max(result["ask"] - result["bid"], 0.0)
            mid = (result["ask"] + result["bid"]) / 2 if result["ask"] and result["bid"] else 0.0
            result["spread"] = spread
            result["spread_pct"] = (spread / mid * 100.0) if mid > 0 else 0.0
            self._quote_cache[normalized] = (time.time(), result)
            self.runtime_state.set_quote(self.account_name, normalized, result)
            return result

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
        if self.provider == "binance":
            payload = self._request_json_with_retry(
                url=f"{self.endpoint}/fapi/v1/trades",
                params={"symbol": self._to_binance_symbol(normalized), "limit": "1"},
                headers=self._auth_headers(),
            )
            trade = payload[0] if isinstance(payload, list) and payload else {}
            return {
                "price": float(trade.get("price", 0.0) or 0.0),
                "size": float(trade.get("qty", 0.0) or 0.0),
                "timestamp": trade.get("time"),
            }
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

    def get_recent_trade_stats(self, symbol: str, lookback_seconds: int = 60, limit: int = 1000) -> dict[str, Any]:
        normalized = symbol.upper().replace(" ", "")
        now_utc = datetime.now(timezone.utc)
        start_utc = now_utc - timedelta(seconds=max(int(lookback_seconds or 60), 1))
        start_iso = start_utc.isoformat().replace("+00:00", "Z")
        end_iso = now_utc.isoformat().replace("+00:00", "Z")
        request_limit = max(min(int(limit or 1000), 10000), 1)

        if self.provider == "binance":
            symbol_for_data = self._to_binance_symbol(normalized)
            params = {
                "symbol": symbol_for_data,
                "startTime": str(int(start_utc.timestamp() * 1000)),
                "endTime": str(int(now_utc.timestamp() * 1000)),
                "limit": str(min(request_limit, 1000)),
            }
            trades = self._request_json_with_retry(
                url=f"{self.endpoint}/fapi/v1/aggTrades",
                params=params,
                headers=self._auth_headers(),
            )
            if not isinstance(trades, list):
                trades = []
            volume = 0.0
            volume_usd = 0.0
            for trade in trades:
                size = float(trade.get("q", 0.0) or 0.0)
                price = float(trade.get("p", 0.0) or 0.0)
                volume += size
                volume_usd += size * price
            return {
                "symbol": symbol_for_data,
                "lookback_seconds": max(int(lookback_seconds or 60), 1),
                "count": len(trades),
                "volume": volume,
                "volume_usd": volume_usd,
                "start": start_iso,
                "end": end_iso,
                "source": "binance_agg_trades",
            }

        if self._is_crypto_symbol(normalized):
            symbol_for_data = self._to_crypto_data_symbol(normalized)
            url = "https://data.alpaca.markets/v1beta3/crypto/us/trades"
            params = {
                "symbols": symbol_for_data,
                "start": start_iso,
                "end": end_iso,
                "limit": str(request_limit),
                "sort": "asc",
            }
        else:
            symbol_for_data = normalized
            url = f"https://data.alpaca.markets/v2/stocks/{normalized}/trades"
            params = {
                "start": start_iso,
                "end": end_iso,
                "limit": str(request_limit),
                "feed": "iex",
                "sort": "asc",
            }

        payload = self._request_json_with_retry(url=url, params=params, headers=self._auth_headers())
        if self._is_crypto_symbol(normalized):
            trades = payload.get("trades", {}).get(symbol_for_data, [])
        else:
            trades = payload.get("trades", [])
        if trades is None:
            trades = []

        volume = 0.0
        volume_usd = 0.0
        for trade in trades:
            size = float(trade.get("s", 0.0) or 0.0)
            price = float(trade.get("p", 0.0) or 0.0)
            volume += size
            volume_usd += size * price

        return {
            "symbol": symbol_for_data,
            "lookback_seconds": max(int(lookback_seconds or 60), 1),
            "count": len(trades),
            "volume": volume,
            "volume_usd": volume_usd,
            "start": start_iso,
            "end": end_iso,
            "source": "alpaca_trades",
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
        if self.provider == "binance":
            timeframe = self._timeframe_for_interval_binance(interval)
            payload = self._request_json_with_retry(
                url=f"{self.endpoint}/fapi/v1/klines",
                params={
                    "symbol": self._to_binance_symbol(normalized),
                    "interval": timeframe,
                    "limit": str(max(min(int(limit or 100), 1000), 1)),
                },
                headers=self._auth_headers(),
            )
            bars: list[dict[str, Any]] = []
            if isinstance(payload, list):
                for row in payload:
                    if not isinstance(row, list) or len(row) < 6:
                        continue
                    bars.append(
                        {
                            "open": float(row[1] or 0.0),
                            "high": float(row[2] or 0.0),
                            "low": float(row[3] or 0.0),
                            "close": float(row[4] or 0.0),
                            "volume": float(row[5] or 0.0),
                            "timestamp": datetime.fromtimestamp(float(row[0]) / 1000.0, tz=timezone.utc).isoformat(),
                        }
                    )
            self._bars_cache[cache_key] = (time.time(), bars)
            return bars

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
        if self.provider == "binance":
            headers = {"Accept": "application/json"}
            if self.api_key:
                headers["X-MBX-APIKEY"] = self.api_key
            return headers

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

    @staticmethod
    def _timeframe_for_interval_binance(interval: str) -> str:
        mapping = {
            "1m": "1m",
            "1min": "1m",
            "5m": "5m",
            "5min": "5m",
            "15m": "15m",
            "15min": "15m",
        }
        return mapping.get(interval.lower(), "1m")

    def _request_json_with_retry(
        self,
        url: str,
        params: dict[str, str],
        headers: dict[str, str] | None = None,
    ) -> Any:
        last_error: Exception | None = None
        for attempt in range(self._max_retries):
            try:
                self.runtime_state.acquire(self.account_name)
                response = self._session.get(url, params=params, headers=headers, timeout=self._http_timeout_seconds)
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
    def _to_binance_symbol(symbol: str) -> str:
        normalized = symbol.upper().replace(" ", "")
        normalized = normalized.replace("/USDC", "/USDT").replace("/USD", "/USDT")
        if "/" in normalized:
            base, quote = normalized.split("/", 1)
            quote = "USDT" if quote in {"USD", "USDC", "USDT"} else quote
            return f"{base}{quote}"
        if normalized.endswith("USD") and len(normalized) > 3:
            return normalized[:-3] + "USDT"
        if normalized.endswith("USDC") and len(normalized) > 4:
            return normalized[:-4] + "USDT"
        return normalized

    def _get_crypto_price_binance(self, symbol: str) -> float:
        payload = self._request_json_with_retry(
            url=f"{self.endpoint}/fapi/v1/ticker/price",
            params={"symbol": self._to_binance_symbol(symbol)},
            headers=self._auth_headers(),
        )
        price = payload.get("price") if isinstance(payload, dict) else None
        if price is None:
            raise ValueError(f"Sin trade reciente en Binance para {symbol}")
        return float(price)

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
