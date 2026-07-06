from __future__ import annotations

from typing import Any
import time
from urllib.parse import quote, urlsplit, urlunsplit

import requests

from config import settings
from runtime.alpaca_state import AlpacaRuntimeState


class AlpacaBrokerClient:
    def __init__(
        self,
        endpoint: str,
        api_key: str,
        api_secret: str,
        logger: Any,
        account_name: str | None = None,
        runtime_state: AlpacaRuntimeState | None = None,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.api_key = api_key
        self.api_secret = api_secret
        self.logger = logger
        self.account_name = account_name or self.endpoint
        self.runtime_state = runtime_state or AlpacaRuntimeState()
        self._session = requests.Session()
        self._stocks_cache: dict[tuple[str, bool], tuple[float, list[dict]]] = {}
        self._crypto_cache: dict[tuple[str, bool], tuple[float, list[dict]]] = {}
        self._all_crypto_cache: tuple[float, list[dict]] | None = None
        self._stocks_cache_ttl_seconds = 900.0
        self.endpoint = self._normalize_trading_endpoint(self.endpoint)

    def set_connection(self, endpoint: str, api_key: str, api_secret: str) -> None:
        self.endpoint = self._normalize_trading_endpoint(endpoint)
        self.api_key = api_key
        self.api_secret = api_secret

    def _headers(self) -> dict[str, str]:
        return {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.api_secret,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        self.runtime_state.acquire(self.account_name)
        timeout = kwargs.pop("timeout", int(getattr(settings, "http_timeout_alpaca_seconds", 10) or 10))
        response = self._session.request(method=method, url=url, headers=self._headers(), timeout=timeout, **kwargs)
        self.runtime_state.record_response(self.account_name, response)
        return response

    @staticmethod
    def _response_json(response: requests.Response) -> Any:
        response.raise_for_status()
        return response.json()

    def get_account(self, force_refresh: bool = False) -> dict:
        cached = None if force_refresh else self.runtime_state.get_cached("_account_cache", self.account_name, 45.0)
        if cached is not None:
            return dict(cached)
        response = self._request("GET", f"{self.endpoint}/account")
        payload = self._response_json(response)
        self.runtime_state.set_cached("_account_cache", self.account_name, payload)
        return payload

    def send_market_order(self, symbol: str, qty: float, side: str, time_in_force: str | None = None) -> dict:
        normalized_symbol = self._normalize_order_symbol(symbol)
        tif_value = (str(time_in_force).lower().strip() if time_in_force else "gtc")
        payload = {
            "symbol": normalized_symbol,
            "qty": qty,
            "side": side,
            "type": "market",
            "time_in_force": tif_value,
        }
        response = self._request("POST", f"{self.endpoint}/orders", json=payload)
        payload_json = self._response_json(response)
        self.runtime_state.invalidate_orders(self.account_name)
        self.runtime_state.invalidate_positions(self.account_name)
        return payload_json

    def send_limit_order(
        self,
        symbol: str,
        qty: float,
        side: str,
        limit_price: float,
        time_in_force: str | None = None,
    ) -> dict:
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
        response = self._request("POST", f"{self.endpoint}/orders", json=payload)
        payload_json = self._response_json(response)
        self.runtime_state.invalidate_orders(self.account_name)
        self.runtime_state.invalidate_positions(self.account_name)
        return payload_json

    def cancel_order(self, order_id: str) -> bool:
        response = self._request("DELETE", f"{self.endpoint}/orders/{order_id}")
        if response.status_code in (200, 204):
            self.runtime_state.invalidate_orders(self.account_name)
            self.runtime_state.invalidate_positions(self.account_name)
            return True
        response.raise_for_status()
        return False

    def list_orders(self, status: str = "open", limit: int = 50, force_refresh: bool = False) -> list[dict]:
        cache_bucket = "_open_orders_cache" if str(status).lower() == "open" else "_all_orders_cache"
        cached = None if force_refresh else self.runtime_state.get_cached(cache_bucket, self.account_name, 45.0)
        if cached is not None:
            return list(cached)
        response = self._request("GET", f"{self.endpoint}/orders", params={"status": status, "limit": limit})
        payload = self._response_json(response)
        self.runtime_state.set_cached(cache_bucket, self.account_name, payload)
        return payload

    def get_order(self, order_id: str) -> dict:
        response = self._request("GET", f"{self.endpoint}/orders/{order_id}")
        return self._response_json(response)

    def get_positions(self, force_refresh: bool = False) -> list[dict]:
        cached = None if force_refresh else self.runtime_state.get_cached("_positions_cache", self.account_name, 20.0)
        if cached is not None:
            return list(cached)
        response = self._request("GET", f"{self.endpoint}/positions")
        payload = self._response_json(response)
        self.runtime_state.set_cached("_positions_cache", self.account_name, payload)
        return payload

    def get_position(self, symbol: str) -> dict:
        encoded_symbol = quote(symbol.upper(), safe="")
        response = self._request("GET", f"{self.endpoint}/positions/{encoded_symbol}")
        return self._response_json(response)

    def close_position(self, symbol: str) -> dict:
        encoded_symbol = quote(symbol.upper(), safe="")
        response = self._request("DELETE", f"{self.endpoint}/positions/{encoded_symbol}")
        self.runtime_state.invalidate_positions(self.account_name)
        self.runtime_state.invalidate_orders(self.account_name)
        response.raise_for_status()
        return response.json() if response.text else {}

    def get_clock(self) -> dict:
        response = self._request("GET", f"{self.endpoint}/clock")
        return self._response_json(response)

    def list_stocks(self, status: str = "active", only_tradable: bool = False) -> list[dict]:
        cache_key = (status, only_tradable)
        cached = self._stocks_cache.get(cache_key)
        if cached is not None:
            cached_at, cached_assets = cached
            if (time.time() - cached_at) <= self._stocks_cache_ttl_seconds:
                return cached_assets

        response = self._request("GET", f"{self.endpoint}/assets", params={"status": status, "asset_class": "us_equity"})
        assets = self._response_json(response)
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

        response = self._request("GET", f"{self.endpoint}/assets", params={"status": status, "asset_class": "crypto"})
        assets = self._response_json(response)
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
        return self.list_stocks(status="active", only_tradable=True) + self.list_cryptos(status="active", only_tradable=True)

    def get_eligible_cryptos_for_scalping(self) -> list[dict]:
        all_cryptos = self.list_all_cryptos()
        eligible = []
        for asset in all_cryptos:
            symbol = str(asset.get("symbol", "")).upper().strip()
            if not symbol:
                continue
            is_tradable = asset.get("tradable") is True
            is_active = str(asset.get("status", "")).lower() == "active"
            is_fractionable = asset.get("fractionable") is True
            is_shortable = asset.get("shortable") is True
            if is_tradable and is_active and is_fractionable and not is_shortable:
                eligible.append(asset)
        eligible.sort(key=lambda a: str(a.get("symbol", "")))
        return eligible

    @staticmethod
    def _normalize_order_symbol(symbol: str) -> str:
        normalized = str(symbol or "").upper().replace(" ", "")
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

    @staticmethod
    def _normalize_trading_endpoint(endpoint: str) -> str:
        normalized = str(endpoint or "").strip().rstrip("/")
        if not normalized:
            return normalized

        parsed = urlsplit(normalized)
        if not parsed.scheme or not parsed.netloc:
            return normalized

        path = parsed.path or ""
        if path in {"", "/"}:
            path = "/v2"
        return urlunsplit((parsed.scheme, parsed.netloc, path, parsed.query, parsed.fragment))
