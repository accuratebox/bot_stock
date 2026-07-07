from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import os
import threading
import time
from typing import Any


@dataclass
class _CacheEntry:
    value: Any
    timestamp: float


class AlpacaRuntimeState:
    def __init__(
        self,
        *,
        soft_limit_per_minute: int = 120,
        hard_limit_per_minute: int = 180,
        window_seconds: float = 60.0,
    ) -> None:
        self.soft_limit_per_minute = int(soft_limit_per_minute)
        self.hard_limit_per_minute = int(hard_limit_per_minute)
        self.window_seconds = float(window_seconds)
        self._lock = threading.Lock()
        self._requests: dict[str, deque[float]] = {}
        self._cooldowns: dict[str, float] = {}
        self._last_429_at: dict[str, float] = {}
        self._last_sync_at: dict[str, float] = {}

        self._account_cache: dict[str, _CacheEntry] = {}
        self._positions_cache: dict[str, _CacheEntry] = {}
        self._open_orders_cache: dict[str, _CacheEntry] = {}
        self._all_orders_cache: dict[str, _CacheEntry] = {}
        self._assets_cache: dict[tuple[str, str, bool], _CacheEntry] = {}
        self._latest_price_cache: dict[tuple[str, str], _CacheEntry] = {}
        self._quotes_cache: dict[tuple[str, str], _CacheEntry] = {}
        self._acquire_max_wait_seconds = max(float(os.getenv("ALPACA_ACQUIRE_MAX_WAIT_SECONDS", "2.5") or 2.5), 0.2)

    def _prune(self, timestamps: deque[float], now: float) -> None:
        cutoff = now - self.window_seconds
        while timestamps and timestamps[0] < cutoff:
            timestamps.popleft()

    def acquire(self, account_key: str) -> None:
        key = str(account_key or "default")
        started_at = time.monotonic()
        while True:
            wait_seconds = 0.0
            with self._lock:
                now = time.monotonic()
                waited = max(now - started_at, 0.0)
                if waited >= self._acquire_max_wait_seconds:
                    # Fail-open after bounded wait: avoid piling up worker threads forever.
                    self._last_sync_at[key] = now
                    return
                cooldown_until = self._cooldowns.get(key, 0.0)
                if cooldown_until > now:
                    wait_seconds = cooldown_until - now
                else:
                    timestamps = self._requests.setdefault(key, deque())
                    self._prune(timestamps, now)
                    count = len(timestamps)
                    limit = self.soft_limit_per_minute if self.soft_limit_per_minute > 0 else self.hard_limit_per_minute
                    if limit > 0 and count >= limit:
                        oldest = timestamps[0]
                        wait_seconds = (oldest + self.window_seconds) - now + 0.01
                    elif self.hard_limit_per_minute > 0 and count >= self.hard_limit_per_minute:
                        oldest = timestamps[0]
                        wait_seconds = (oldest + self.window_seconds) - now + 0.05
                    else:
                        timestamps.append(now)
                        self._last_sync_at[key] = now
                        return

            time.sleep(max(wait_seconds, 0.05))

    def record_response(self, account_key: str, response: Any) -> None:
        key = str(account_key or "default")
        status = int(getattr(response, "status_code", 0) or 0)
        if status == 429:
            retry_after = 0.0
            headers = getattr(response, "headers", {}) or {}
            raw_retry_after = str(headers.get("Retry-After", "")).strip()
            if raw_retry_after:
                try:
                    retry_after = max(float(raw_retry_after), 0.0)
                except ValueError:
                    retry_after = 0.0
            if retry_after <= 0.0:
                retry_after = 2.0
            with self._lock:
                self._cooldowns[key] = max(self._cooldowns.get(key, 0.0), time.monotonic() + retry_after)
                self._last_429_at[key] = time.monotonic()

    def record_failure(self, account_key: str, status_code: int | None = None, retry_after: float = 0.0) -> None:
        key = str(account_key or "default")
        if status_code == 429:
            cooldown = max(float(retry_after or 0.0), 2.0)
            with self._lock:
                self._cooldowns[key] = max(self._cooldowns.get(key, 0.0), time.monotonic() + cooldown)
                self._last_429_at[key] = time.monotonic()

    def in_cooldown(self, account_key: str) -> bool:
        key = str(account_key or "default")
        with self._lock:
            return self._cooldowns.get(key, 0.0) > time.monotonic()

    def cooldown_remaining(self, account_key: str) -> float:
        key = str(account_key or "default")
        with self._lock:
            return max(self._cooldowns.get(key, 0.0) - time.monotonic(), 0.0)

    def last_global_sync_age(self, account_key: str) -> float:
        key = str(account_key or "default")
        with self._lock:
            last_sync = self._last_sync_at.get(key, 0.0)
        if last_sync <= 0:
            return float("inf")
        return max(time.monotonic() - last_sync, 0.0)

    def set_cooldown(self, account_key: str, cooldown_seconds: float) -> None:
        key = str(account_key or "default")
        with self._lock:
            self._cooldowns[key] = max(self._cooldowns.get(key, 0.0), time.monotonic() + max(float(cooldown_seconds), 0.0))

    def get_cached(self, bucket: str, account_key: str, ttl_seconds: float) -> Any | None:
        key = str(account_key or "default")
        now = time.monotonic()
        with self._lock:
            storage = getattr(self, bucket)
            entry = storage.get(key)
            if entry is None:
                return None
            if (now - entry.timestamp) > ttl_seconds:
                return None
            return entry.value

    def set_cached(self, bucket: str, account_key: str, value: Any) -> None:
        key = str(account_key or "default")
        with self._lock:
            storage = getattr(self, bucket)
            storage[key] = _CacheEntry(value=value, timestamp=time.monotonic())

    def get_nested_cached(self, bucket: str, account_key: str, nested_key: str, ttl_seconds: float) -> Any | None:
        outer = self.get_cached(bucket, account_key, ttl_seconds)
        if outer is None:
            return None
        return outer.get(nested_key) if isinstance(outer, dict) else None

    def set_latest_price(self, account_key: str, symbol: str, price: float) -> None:
        self.set_cached("_latest_price_cache", account_key, {str(symbol or "").upper(): float(price)})

    def get_latest_price(self, account_key: str, symbol: str, ttl_seconds: float) -> float | None:
        key = (str(account_key or "default"), str(symbol or "").upper())
        now = time.monotonic()
        with self._lock:
            entry = self._latest_price_cache.get(key)
            if entry is None:
                return None
            if (now - entry.timestamp) > ttl_seconds:
                return None
            return float(entry.value)

    def set_latest_price_value(self, account_key: str, symbol: str, price: float) -> None:
        key = (str(account_key or "default"), str(symbol or "").upper())
        with self._lock:
            self._latest_price_cache[key] = _CacheEntry(value=float(price), timestamp=time.monotonic())

    def set_quote(self, account_key: str, symbol: str, quote: dict[str, Any]) -> None:
        key = (str(account_key or "default"), str(symbol or "").upper())
        with self._lock:
            self._quotes_cache[key] = _CacheEntry(value=dict(quote), timestamp=time.monotonic())

    def get_quote(self, account_key: str, symbol: str, ttl_seconds: float) -> dict[str, Any] | None:
        key = (str(account_key or "default"), str(symbol or "").upper())
        now = time.monotonic()
        with self._lock:
            entry = self._quotes_cache.get(key)
            if entry is None:
                return None
            if (now - entry.timestamp) > ttl_seconds:
                return None
            return dict(entry.value)

    def invalidate_account(self, account_key: str) -> None:
        key = str(account_key or "default")
        with self._lock:
            self._account_cache.pop(key, None)
            self._positions_cache.pop(key, None)
            self._open_orders_cache.pop(key, None)
            self._all_orders_cache.pop(key, None)

    def invalidate_orders(self, account_key: str) -> None:
        key = str(account_key or "default")
        with self._lock:
            self._open_orders_cache.pop(key, None)
            self._all_orders_cache.pop(key, None)

    def invalidate_positions(self, account_key: str) -> None:
        key = str(account_key or "default")
        with self._lock:
            self._positions_cache.pop(key, None)

    def invalidate_prices(self, account_key: str) -> None:
        key = str(account_key or "default")
        with self._lock:
            self._latest_price_cache = {k: v for k, v in self._latest_price_cache.items() if k[0] != key}
            self._quotes_cache = {k: v for k, v in self._quotes_cache.items() if k[0] != key}
