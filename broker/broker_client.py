from typing import Any
import time
from urllib.parse import quote

import requests


class AlpacaBrokerClient:
    def __init__(self, endpoint: str, api_key: str, api_secret: str, logger: Any) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.api_key = api_key
        self.api_secret = api_secret
        self.logger = logger
        self._session = requests.Session()
        self._stocks_cache: dict[tuple[str, bool], tuple[float, list[dict]]] = {}
        self._crypto_cache: dict[tuple[str, bool], tuple[float, list[dict]]] = {}
        self._all_crypto_cache: tuple[float, list[dict]] | None = None
        self._stocks_cache_ttl_seconds = 900.0

    def set_connection(self, endpoint: str, api_key: str, api_secret: str) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.api_key = api_key
        self.api_secret = api_secret

    def _headers(self) -> dict[str, str]:
        return {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.api_secret,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def get_account(self) -> dict:
        url = f"{self.endpoint}/account"
        response = requests.get(url, headers=self._headers(), timeout=15)
        response.raise_for_status()
        return response.json()

    def send_market_order(self, symbol: str, qty: float, side: str, time_in_force: str | None = None) -> dict:
        url = f"{self.endpoint}/orders"
        normalized_symbol = self._normalize_order_symbol(symbol)
        tif_value = (str(time_in_force).lower().strip() if time_in_force else "gtc")
        payload = {
            "symbol": normalized_symbol,
            "qty": qty,
            "side": side,
            "type": "market",
            "time_in_force": tif_value,
        }
        response = requests.post(url, headers=self._headers(), json=payload, timeout=15)
        response.raise_for_status()
        return response.json()

    def send_limit_order(
        self,
        symbol: str,
        qty: float,
        side: str,
        limit_price: float,
        time_in_force: str | None = None,
    ) -> dict:
        url = f"{self.endpoint}/orders"
        normalized_symbol = self._normalize_order_symbol(symbol)
        tif_value = (str(time_in_force).lower().strip() if time_in_force else "gtc")
        payload = {
            "symbol": normalized_symbol,
            "qty": qty,
            "side": side,
            "type": "limit",
            "limit_price": limit_price,
            "time_in_force": tif_value,
        }
        response = requests.post(url, headers=self._headers(), json=payload, timeout=15)
        response.raise_for_status()
        return response.json()

    def cancel_order(self, order_id: str) -> bool:
        url = f"{self.endpoint}/orders/{order_id}"
        response = requests.delete(url, headers=self._headers(), timeout=15)
        if response.status_code in (200, 204):
            return True
        response.raise_for_status()
        return False

    def list_orders(self, status: str = "open", limit: int = 50) -> list[dict]:
        url = f"{self.endpoint}/orders"
        params = {"status": status, "limit": limit}
        response = requests.get(url, headers=self._headers(), params=params, timeout=15)
        response.raise_for_status()
        return response.json()

    def get_order(self, order_id: str) -> dict:
        url = f"{self.endpoint}/orders/{order_id}"
        response = requests.get(url, headers=self._headers(), timeout=15)
        response.raise_for_status()
        return response.json()

    def get_positions(self) -> list[dict]:
        url = f"{self.endpoint}/positions"
        response = requests.get(url, headers=self._headers(), timeout=15)
        response.raise_for_status()
        return response.json()

    def get_position(self, symbol: str) -> dict:
        encoded_symbol = quote(symbol.upper(), safe="")
        url = f"{self.endpoint}/positions/{encoded_symbol}"
        response = requests.get(url, headers=self._headers(), timeout=15)
        response.raise_for_status()
        return response.json()

    def close_position(self, symbol: str) -> dict:
        encoded_symbol = quote(symbol.upper(), safe="")
        url = f"{self.endpoint}/positions/{encoded_symbol}"
        response = requests.delete(url, headers=self._headers(), timeout=15)
        response.raise_for_status()
        return response.json() if response.text else {}

    def get_clock(self) -> dict:
        url = f"{self.endpoint}/clock"
        response = requests.get(url, headers=self._headers(), timeout=15)
        response.raise_for_status()
        return response.json()

    def list_stocks(self, status: str = "active", only_tradable: bool = False) -> list[dict]:
        cache_key = (status, only_tradable)
        cached = self._stocks_cache.get(cache_key)
        if cached is not None:
            cached_at, cached_assets = cached
            if (time.time() - cached_at) <= self._stocks_cache_ttl_seconds:
                return cached_assets

        url = f"{self.endpoint}/assets"
        params = {"status": status, "asset_class": "us_equity"}
        response = self._session.get(url, headers=self._headers(), params=params, timeout=60)
        response.raise_for_status()

        assets = response.json()
        if only_tradable:
            assets = [asset for asset in assets if asset.get("tradable") is True]
        self._stocks_cache[cache_key] = (time.time(), assets)
        return assets

    def list_cryptos(self, status: str = "active", only_tradable: bool = False) -> list[dict]:
        cache_key = (status, only_tradable)
        cached = self._crypto_cache.get(cache_key)
        if cached is not None:
            cached_at, cached_assets = cached
            if (time.time() - cached_at) <= self._stocks_cache_ttl_seconds:
                return cached_assets

        url = f"{self.endpoint}/assets"
        params = {"status": status, "asset_class": "crypto"}
        response = self._session.get(url, headers=self._headers(), params=params, timeout=60)
        response.raise_for_status()

        assets = response.json()
        if only_tradable:
            assets = [asset for asset in assets if asset.get("tradable") is True]
        self._crypto_cache[cache_key] = (time.time(), assets)
        return assets

    def list_all_cryptos(self) -> list[dict]:
        cached = self._all_crypto_cache
        if cached is not None:
            cached_at, cached_assets = cached
            if (time.time() - cached_at) <= self._stocks_cache_ttl_seconds:
                return cached_assets

        all_assets: list[dict] = []
        seen: set[str] = set()

        for status in ("active", "inactive"):
            assets = self.list_cryptos(status=status, only_tradable=False)
            for asset in assets:
                symbol = str(asset.get("symbol", "")).upper().strip()
                if not symbol or symbol in seen:
                    continue
                seen.add(symbol)
                all_assets.append(asset)

        self._all_crypto_cache = (time.time(), all_assets)
        return all_assets

    def list_tradable_assets(self) -> list[dict]:
        stocks = self.list_stocks(status="active", only_tradable=True)
        cryptos = self.list_cryptos(status="active", only_tradable=True)
        return stocks + cryptos

    @staticmethod
    def _normalize_order_symbol(symbol: str) -> str:
        normalized = str(symbol or "").upper().replace(" ", "")
        normalized = normalized.replace("/USDT", "/USD")
        if normalized.endswith("USDT") and "/" not in normalized:
            normalized = normalized[:-4] + "USD"
        if "/" in normalized:
            return normalized.replace("/", "")
        return normalized
