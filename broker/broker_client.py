from __future__ import annotations

import hashlib
import hmac
import json
import math
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
        self.provider = self._detect_provider(self.endpoint)
        self.account_name = account_name or self.endpoint
        self.runtime_state = runtime_state or AlpacaRuntimeState()
        self._session = requests.Session()
        self._stocks_cache: dict[tuple[str, bool], tuple[float, list[dict]]] = {}
        self._crypto_cache: dict[tuple[str, bool], tuple[float, list[dict]]] = {}
        self._all_crypto_cache: tuple[float, list[dict]] | None = None
        self._binance_symbol_rules_cache: dict[str, tuple[float, dict[str, float]]] = {}
        self._stocks_cache_ttl_seconds = 900.0
        self.endpoint = self._normalize_endpoint(self.endpoint)

    def set_connection(self, endpoint: str, api_key: str, api_secret: str) -> None:
        self.provider = self._detect_provider(endpoint)
        self.endpoint = self._normalize_endpoint(endpoint)
        self.api_key = api_key
        self.api_secret = api_secret

    def _headers(self) -> dict[str, str]:
        if self.provider == "binance":
            return {
                "X-MBX-APIKEY": self.api_key,
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        return {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.api_secret,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _binance_sign(self, params: dict[str, Any]) -> dict[str, Any]:
        payload = dict(params)
        payload["timestamp"] = int(time.time() * 1000)
        payload["recvWindow"] = 5000
        query = requests.models.RequestEncodingMixin._encode_params(payload)
        signature = hmac.new(self.api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
        payload["signature"] = signature
        return payload

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        self.runtime_state.acquire(self.account_name)
        timeout = kwargs.pop("timeout", int(getattr(settings, "http_timeout_alpaca_seconds", 10) or 10))
        response = self._session.request(method=method, url=url, headers=self._headers(), timeout=timeout, **kwargs)
        self.runtime_state.record_response(self.account_name, response)
        return response

    def _request_binance(self, method: str, path: str, params: dict[str, Any] | None = None, signed: bool = False) -> requests.Response:
        payload = dict(params or {})
        if signed:
            payload = self._binance_sign(payload)
        return self._request(method, f"{self.endpoint}{path}", params=payload)

    @staticmethod
    def _response_json(response: requests.Response) -> Any:
        try:
            response.raise_for_status()
        except requests.HTTPError as ex:
            body = ""
            try:
                body = (response.text or "").strip()
            except Exception:
                body = ""
            if body:
                raise requests.HTTPError(f"{ex} | response={body}", response=response, request=response.request)
            raise
        return response.json()

    def get_account(self, force_refresh: bool = False) -> dict:
        cached = None if force_refresh else self.runtime_state.get_cached("_account_cache", self.account_name, 45.0)
        if cached is not None:
            return dict(cached)
        if self.provider == "binance":
            try:
                response = self._request_binance("GET", "/fapi/v2/account", signed=True)
                payload = self._response_json(response)
                mapped = self._map_binance_account(payload)
                self.runtime_state.set_cached("_account_cache", self.account_name, mapped)
                return mapped
            except requests.exceptions.RequestException:
                fallback = self.runtime_state.get_cached("_account_cache", self.account_name, 3600.0)
                if fallback is not None:
                    return dict(fallback)
                raise
        response = self._request("GET", f"{self.endpoint}/account")
        payload = self._response_json(response)
        self.runtime_state.set_cached("_account_cache", self.account_name, payload)
        return payload

    def send_market_order(self, symbol: str, qty: float, side: str, time_in_force: str | None = None) -> dict:
        if self.provider == "binance":
            binance_symbol = self._to_binance_symbol(symbol)
            rules = self._binance_symbol_rules(binance_symbol)
            quantity = self._normalize_binance_quantity(float(qty or 0.0), rules)
            if quantity <= 0.0:
                raise ValueError(f"Cantidad invalida para {binance_symbol} tras ajustar step/minQty")
            response = self._request_binance(
                "POST",
                "/fapi/v1/order",
                params={
                    "symbol": binance_symbol,
                    "side": str(side or "").upper(),
                    "type": "MARKET",
                    "quantity": self._fmt_qty(quantity),
                },
                signed=True,
            )
            payload_json = self._response_json(response)
            self.runtime_state.invalidate_orders(self.account_name)
            self.runtime_state.invalidate_positions(self.account_name)
            return self._map_binance_order(payload_json)

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
        if self.provider == "binance":
            binance_symbol = self._to_binance_symbol(symbol)
            rules = self._binance_symbol_rules(binance_symbol)
            normalized_price = self._normalize_binance_price(float(limit_price or 0.0), rules)
            normalized_qty = self._normalize_binance_quantity(float(qty or 0.0), rules)
            if normalized_price <= 0.0:
                raise ValueError(f"Precio invalido para {binance_symbol} tras ajustar tickSize")
            if normalized_qty <= 0.0:
                raise ValueError(f"Cantidad invalida para {binance_symbol} tras ajustar step/minQty")
            min_notional = float(rules.get("min_notional", 0.0) or 0.0)
            if min_notional > 0.0 and (normalized_price * normalized_qty) < min_notional:
                raise ValueError(
                    f"Notional insuficiente para {binance_symbol}: {normalized_price * normalized_qty:.6f} < {min_notional:.6f}"
                )
            tif_value = (str(time_in_force).upper().strip() if time_in_force else "GTC")
            side_value = str(side or "").upper()
            try:
                response = self._request_binance(
                    "POST",
                    "/fapi/v1/order",
                    params={
                        "symbol": binance_symbol,
                        "side": side_value,
                        "type": "LIMIT",
                        "timeInForce": tif_value,
                        "quantity": self._fmt_qty(normalized_qty),
                        "price": self._fmt_price(normalized_price),
                    },
                    signed=True,
                )
                payload_json = self._response_json(response)
            except requests.HTTPError as ex:
                response = getattr(ex, "response", None)
                status_code = int(getattr(response, "status_code", 0) or 0)
                if status_code == 400:
                    self.logger.warning(
                        "Binance LIMIT rechazada (%s). Reintentando MARKET para %s qty=%s",
                        ex,
                        binance_symbol,
                        self._fmt_qty(normalized_qty),
                    )
                    return self.send_market_order(
                        symbol=symbol,
                        qty=normalized_qty,
                        side=side_value,
                        time_in_force=time_in_force,
                    )
                raise
            self.runtime_state.invalidate_orders(self.account_name)
            self.runtime_state.invalidate_positions(self.account_name)
            return self._map_binance_order(payload_json)

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
        if self.provider == "binance":
            # Binance requires symbol + orderId. Try to resolve symbol from open orders first.
            open_orders = self.list_orders(status="open", limit=200)
            match = next((row for row in open_orders if str(row.get("id", "")) == str(order_id)), None)
            if match is None:
                return False
            symbol = self._to_binance_symbol(str(match.get("symbol", "") or ""))
            response = self._request_binance(
                "DELETE",
                "/fapi/v1/order",
                params={"symbol": symbol, "orderId": str(order_id)},
                signed=True,
            )
            if response.status_code in (200, 204):
                self.runtime_state.invalidate_orders(self.account_name)
                self.runtime_state.invalidate_positions(self.account_name)
                return True
            response.raise_for_status()
            return False

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
        if self.provider == "binance":
            if str(status).lower() == "open":
                response = self._request_binance("GET", "/fapi/v1/openOrders", params={}, signed=True)
            else:
                # Spot API has no generic "all orders without symbol" endpoint.
                response = self._request_binance("GET", "/fapi/v1/openOrders", params={}, signed=True)
            payload = [self._map_binance_order(item) for item in (self._response_json(response) or [])]
            self.runtime_state.set_cached(cache_bucket, self.account_name, payload)
            return payload
        response = self._request("GET", f"{self.endpoint}/orders", params={"status": status, "limit": limit})
        payload = self._response_json(response)
        self.runtime_state.set_cached(cache_bucket, self.account_name, payload)
        return payload

    def get_order(self, order_id: str) -> dict:
        if self.provider == "binance":
            # Best-effort lookup from open orders cache
            for row in self.list_orders(status="open", limit=200, force_refresh=True):
                if str(row.get("id", "")) == str(order_id):
                    return dict(row)
            raise ValueError(f"Binance order not found in open orders: {order_id}")
        response = self._request("GET", f"{self.endpoint}/orders/{order_id}")
        return self._response_json(response)

    def get_positions(self, force_refresh: bool = False) -> list[dict]:
        cached = None if force_refresh else self.runtime_state.get_cached("_positions_cache", self.account_name, 20.0)
        if cached is not None:
            return list(cached)
        if self.provider == "binance":
            response = self._request_binance("GET", "/fapi/v2/positionRisk", signed=True)
            rows = list(self._response_json(response) or [])
            positions: list[dict[str, Any]] = []
            for row in rows:
                symbol_raw = str(row.get("symbol", "") or "").upper().strip()
                if not symbol_raw.endswith("USDT"):
                    continue
                qty_signed = float(row.get("positionAmt", 0.0) or 0.0)
                qty = abs(qty_signed)
                if qty <= 0.0:
                    continue
                symbol = self._from_binance_symbol(symbol_raw)
                current_price = float(row.get("markPrice", 0.0) or 0.0)
                avg_entry = float(row.get("entryPrice", 0.0) or 0.0)
                market_value = qty * current_price
                unrealized = float(row.get("unRealizedProfit", 0.0) or 0.0)
                basis = qty * avg_entry
                unrealized_plpc = (unrealized / basis) if basis > 0 else 0.0
                positions.append(
                    {
                        "symbol": symbol,
                        "qty": str(qty),
                        "avg_entry_price": str(avg_entry),
                        "current_price": str(current_price),
                        "market_value": str(market_value),
                        "unrealized_pl": str(unrealized),
                        "unrealized_plpc": str(unrealized_plpc),
                        "side": "long",
                    }
                )
            self.runtime_state.set_cached("_positions_cache", self.account_name, positions)
            return positions
        response = self._request("GET", f"{self.endpoint}/positions")
        payload = self._response_json(response)
        self.runtime_state.set_cached("_positions_cache", self.account_name, payload)
        return payload

    def get_position(self, symbol: str) -> dict:
        if self.provider == "binance":
            key = self._symbol_key(symbol)
            for row in self.get_positions(force_refresh=True):
                if self._symbol_key(str(row.get("symbol", ""))) == key:
                    return row
            raise ValueError(f"Position not found for symbol: {symbol}")
        encoded_symbol = quote(symbol.upper(), safe="")
        response = self._request("GET", f"{self.endpoint}/positions/{encoded_symbol}")
        return self._response_json(response)

    def close_position(self, symbol: str) -> dict:
        if self.provider == "binance":
            position = self.get_position(symbol)
            qty = float(position.get("qty", 0.0) or 0.0)
            if qty <= 0:
                return {}
            return self.send_market_order(symbol=symbol, qty=qty, side="sell", time_in_force="gtc")
        encoded_symbol = quote(symbol.upper(), safe="")
        response = self._request("DELETE", f"{self.endpoint}/positions/{encoded_symbol}")
        self.runtime_state.invalidate_positions(self.account_name)
        self.runtime_state.invalidate_orders(self.account_name)
        response.raise_for_status()
        return response.json() if response.text else {}

    def set_futures_leverage(self, symbol: str, leverage: int) -> dict[str, Any]:
        if self.provider != "binance":
            return {}
        target = max(int(leverage or 1), 1)
        response = self._request_binance(
            "POST",
            "/fapi/v1/leverage",
            params={
                "symbol": self._to_binance_symbol(symbol),
                "leverage": target,
            },
            signed=True,
        )
        return self._response_json(response)

    def get_clock(self) -> dict:
        if self.provider == "binance":
            response = self._request_binance("GET", "/fapi/v1/time", signed=False)
            payload = self._response_json(response)
            return {"is_open": True, "timestamp": payload.get("serverTime")}
        response = self._request("GET", f"{self.endpoint}/clock")
        return self._response_json(response)

    def list_stocks(self, status: str = "active", only_tradable: bool = False) -> list[dict]:
        if self.provider == "binance":
            return []
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
        if self.provider == "binance":
            cache_key = (status, only_tradable)
            cached = self._crypto_cache.get(cache_key)
            if cached is not None:
                cached_at, cached_assets = cached
                if (time.time() - cached_at) <= self._stocks_cache_ttl_seconds:
                    return cached_assets
            response = self._request_binance("GET", "/fapi/v1/exchangeInfo", signed=False)
            payload = self._response_json(response)
            symbols = list((payload or {}).get("symbols", []) or [])
            assets: list[dict[str, Any]] = []
            for row in symbols:
                if str(row.get("quoteAsset", "")).upper() != "USDT":
                    continue
                if str(row.get("status", "")).upper() != "TRADING":
                    continue
                assets.append(
                    {
                        "symbol": self._from_binance_symbol(str(row.get("symbol", "") or "")),
                        "status": "active",
                        "tradable": True,
                        "fractionable": True,
                        "shortable": False,
                        "asset_class": "crypto",
                    }
                )
            self._crypto_cache[cache_key] = (time.time(), assets)
            return assets

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
        if self.provider == "binance":
            return self.list_cryptos(status="active", only_tradable=False)
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
    def _symbol_key(symbol: str) -> str:
        return str(symbol or "").upper().replace(" ", "").replace("/", "")

    @staticmethod
    def _fmt_qty(value: float) -> str:
        return f"{float(value or 0.0):.8f}".rstrip("0").rstrip(".") or "0"

    @staticmethod
    def _fmt_price(value: float) -> str:
        return f"{float(value or 0.0):.8f}".rstrip("0").rstrip(".") or "0"

    @staticmethod
    def _decimals_from_step(step: float) -> int:
        text = (f"{float(step or 0.0):.12f}").rstrip("0").rstrip(".")
        if "." not in text:
            return 0
        return max(0, len(text.split(".", 1)[1]))

    @staticmethod
    def _floor_to_step(value: float, step: float) -> float:
        if step <= 0.0:
            return float(value or 0.0)
        floored = math.floor((float(value or 0.0) + 1e-12) / step) * step
        decimals = AlpacaBrokerClient._decimals_from_step(step)
        return round(max(floored, 0.0), decimals)

    def _binance_symbol_rules(self, binance_symbol: str) -> dict[str, float]:
        key = str(binance_symbol or "").upper().strip()
        cached = self._binance_symbol_rules_cache.get(key)
        now = time.time()
        if cached is not None:
            cached_at, rules = cached
            if (now - cached_at) <= 900.0:
                return dict(rules)

        response = self._request_binance("GET", "/fapi/v1/exchangeInfo", signed=False)
        payload = self._response_json(response)
        symbols = list((payload or {}).get("symbols", []) or [])
        target = next((row for row in symbols if str(row.get("symbol", "") or "").upper() == key), None)
        if target is None:
            rules = {"step_size": 0.001, "tick_size": 0.01, "min_qty": 0.0, "min_notional": 0.0}
            self._binance_symbol_rules_cache[key] = (now, rules)
            return dict(rules)

        step_size = 0.001
        tick_size = 0.01
        min_qty = 0.0
        min_notional = 0.0
        for filt in list(target.get("filters", []) or []):
            ftype = str(filt.get("filterType", "") or "").upper().strip()
            if ftype == "LOT_SIZE":
                step_size = float(filt.get("stepSize", step_size) or step_size)
                min_qty = float(filt.get("minQty", min_qty) or min_qty)
            elif ftype == "PRICE_FILTER":
                tick_size = float(filt.get("tickSize", tick_size) or tick_size)
            elif ftype in {"MIN_NOTIONAL", "NOTIONAL"}:
                min_notional = float(filt.get("notional", filt.get("minNotional", min_notional)) or min_notional)

        rules = {
            "step_size": max(step_size, 0.0),
            "tick_size": max(tick_size, 0.0),
            "min_qty": max(min_qty, 0.0),
            "min_notional": max(min_notional, 0.0),
        }
        self._binance_symbol_rules_cache[key] = (now, rules)
        return dict(rules)

    def _normalize_binance_quantity(self, qty: float, rules: dict[str, float]) -> float:
        step = float(rules.get("step_size", 0.0) or 0.0)
        min_qty = float(rules.get("min_qty", 0.0) or 0.0)
        normalized = self._floor_to_step(float(qty or 0.0), step)
        if min_qty > 0.0 and normalized < min_qty:
            return 0.0
        return normalized

    def _normalize_binance_price(self, price: float, rules: dict[str, float]) -> float:
        tick = float(rules.get("tick_size", 0.0) or 0.0)
        return self._floor_to_step(float(price or 0.0), tick)

    @staticmethod
    def _to_binance_symbol(symbol: str) -> str:
        normalized = str(symbol or "").upper().replace(" ", "")
        normalized = normalized.replace("/USDC", "/USDT").replace("/USD", "/USDT")
        if "/" in normalized:
            base, quote = normalized.split("/", 1)
            quote = "USDT" if quote in {"USD", "USDC", "USDT"} else quote
            return f"{base}{quote}"
        if normalized.endswith("USD"):
            return normalized[:-3] + "USDT"
        if normalized.endswith("USDC"):
            return normalized[:-4] + "USDT"
        return normalized

    @staticmethod
    def _from_binance_symbol(symbol: str) -> str:
        raw = str(symbol or "").upper().strip()
        if raw.endswith("USDT") and len(raw) > 4:
            return f"{raw[:-4]}/USD"
        return raw

    def _binance_last_price(self, asset: str) -> float:
        response = self._request_binance("GET", "/fapi/v1/ticker/price", params={"symbol": f"{asset}USDT"}, signed=False)
        payload = self._response_json(response)
        return float(payload.get("price", 0.0) or 0.0)

    @staticmethod
    def _map_binance_order(order: dict[str, Any]) -> dict[str, Any]:
        status = str(order.get("status", "") or "").lower()
        status_map = {
            "new": "new",
            "partially_filled": "partially_filled",
            "filled": "filled",
            "canceled": "canceled",
            "rejected": "rejected",
            "expired": "expired",
        }
        mapped_status = status_map.get(status, status or "submitted")
        symbol = AlpacaBrokerClient._from_binance_symbol(str(order.get("symbol", "") or ""))
        return {
            "id": str(order.get("orderId", order.get("id", "")) or ""),
            "symbol": symbol,
            "qty": float(order.get("origQty", order.get("executedQty", 0.0)) or 0.0),
            "filled_qty": float(order.get("executedQty", 0.0) or 0.0),
            "side": str(order.get("side", "") or "").lower(),
            "status": mapped_status,
            "limit_price": float(order.get("price", 0.0) or 0.0),
            "filled_avg_price": float(order.get("price", 0.0) or 0.0),
            "time_in_force": str(order.get("timeInForce", "GTC") or "GTC").lower(),
        }

    @staticmethod
    def _map_binance_account(payload: dict[str, Any]) -> dict[str, Any]:
        wallet_balance = float(payload.get("totalWalletBalance", 0.0) or 0.0)
        unrealized = float(payload.get("totalUnrealizedProfit", 0.0) or 0.0)
        margin_balance = float(payload.get("totalMarginBalance", wallet_balance + unrealized) or (wallet_balance + unrealized))
        available_balance = float(payload.get("availableBalance", 0.0) or 0.0)
        can_trade = bool(payload.get("canTrade", True))

        return {
            "status": "ACTIVE" if can_trade else "RESTRICTED",
            "currency": "USDT",
            "cash": f"{available_balance:.2f}",
            "buying_power": f"{available_balance:.2f}",
            "equity": f"{margin_balance:.2f}",
            "last_equity": f"{wallet_balance:.2f}",
            "provider": "binance",
            "raw": payload,
        }

    @staticmethod
    def _detect_provider(endpoint: str) -> str:
        host = str(urlsplit(str(endpoint or "")).netloc or "").lower()
        if "binance" in host:
            return "binance"
        return "alpaca"

    def _normalize_endpoint(self, endpoint: str) -> str:
        if self.provider == "binance":
            normalized = str(endpoint or "https://testnet.binancefuture.com").strip().rstrip("/")
            return normalized
        return self._normalize_trading_endpoint(endpoint)

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
