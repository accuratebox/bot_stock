"""CryptoPanic news caching to respect 600 req/month quota."""

import time
import json
from pathlib import Path
from typing import Any
from datetime import datetime, timezone


class CryptoPanicCache:
    """
    In-memory and file-based cache for CryptoPanic news.
    
    Respects quota by:
    - Caching results for 30-60 minutes
    - Tracking daily request count (max 20/day)
    - Only fetching general news every 2 hours
    - Only fetching symbol-specific news on strong price movements
    """

    def __init__(self, cache_dir: str | None = None) -> None:
        self.cache_dir = Path(cache_dir) if cache_dir else Path(__file__).resolve().parent / "cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # In-memory caches
        self._general_news_cache: dict[str, Any] = {}
        self._symbol_news_cache: dict[str, dict[str, Any]] = {}
        self._request_count_today = 0
        self._last_request_date = self._today_key()
        
        # TTL configuration (in seconds)
        self.general_news_ttl = 7200  # 2 hours
        self.symbol_news_ttl = 1800  # 30 minutes
        self.daily_request_limit = 20  # Max 20 requests/day to stay under 600/month

    def should_fetch_general_news(self) -> bool:
        """Check if we should fetch general news (cache expired or not fetched today)."""
        if not self._general_news_cache:
            return True
        
        cached_at = self._general_news_cache.get("cached_at", 0.0)
        elapsed = time.time() - cached_at
        return elapsed > self.general_news_ttl

    def should_fetch_symbol_news(self, symbol: str) -> bool:
        """Check if we should fetch news for a specific symbol."""
        if symbol not in self._symbol_news_cache:
            return True
        
        cached = self._symbol_news_cache[symbol]
        cached_at = cached.get("cached_at", 0.0)
        elapsed = time.time() - cached_at
        return elapsed > self.symbol_news_ttl

    def can_make_request(self) -> bool:
        """Check if we can make another request (within daily quota)."""
        # Reset counter if it's a new day
        today = self._today_key()
        if today != self._last_request_date:
            self._request_count_today = 0
            self._last_request_date = today
        
        return self._request_count_today < self.daily_request_limit

    def get_daily_request_count(self) -> int:
        """Get current request count for today."""
        today = self._today_key()
        if today != self._last_request_date:
            self._request_count_today = 0
            self._last_request_date = today
        
        return self._request_count_today

    def record_request(self) -> None:
        """Record that a request was made."""
        today = self._today_key()
        if today != self._last_request_date:
            self._request_count_today = 0
            self._last_request_date = today
        
        self._request_count_today += 1

    def cache_general_news(self, data: dict[str, Any]) -> None:
        """Cache general news."""
        self._general_news_cache = dict(data)
        self._general_news_cache["cached_at"] = time.time()
        self._persist_cache("general_news", self._general_news_cache)

    def get_cached_general_news(self) -> dict[str, Any] | None:
        """Get cached general news if available."""
        if not self._general_news_cache:
            self._general_news_cache = self._load_cache("general_news") or {}
        
        if not self._general_news_cache:
            return None
        
        cached_at = self._general_news_cache.get("cached_at", 0.0)
        if time.time() - cached_at > self.general_news_ttl:
            return None  # Cache expired
        
        return self._general_news_cache

    def cache_symbol_news(self, symbol: str, data: dict[str, Any]) -> None:
        """Cache news for a specific symbol."""
        cache_entry = dict(data)
        cache_entry["cached_at"] = time.time()
        self._symbol_news_cache[symbol] = cache_entry
        self._persist_cache(f"symbol_news_{symbol}", cache_entry)

    def get_cached_symbol_news(self, symbol: str) -> dict[str, Any] | None:
        """Get cached news for a specific symbol."""
        if symbol not in self._symbol_news_cache:
            self._symbol_news_cache[symbol] = self._load_cache(f"symbol_news_{symbol}") or {}
        
        if symbol not in self._symbol_news_cache or not self._symbol_news_cache[symbol]:
            return None
        
        cached = self._symbol_news_cache[symbol]
        cached_at = cached.get("cached_at", 0.0)
        if time.time() - cached_at > self.symbol_news_ttl:
            return None  # Cache expired
        
        return cached

    def _persist_cache(self, key: str, data: dict[str, Any]) -> None:
        """Persist cache to disk."""
        try:
            cache_file = self.cache_dir / f"{key}.json"
            cache_file.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass  # Cache failure is not critical

    def _load_cache(self, key: str) -> dict[str, Any] | None:
        """Load cache from disk."""
        try:
            cache_file = self.cache_dir / f"{key}.json"
            if cache_file.exists():
                return json.loads(cache_file.read_text(encoding="utf-8"))
        except Exception:
            pass  # Cache load failure is not critical
        
        return None

    @staticmethod
    def _today_key() -> str:
        """Get today's date in ISO format."""
        return datetime.now(timezone.utc).date().isoformat()
