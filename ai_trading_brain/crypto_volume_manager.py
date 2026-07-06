from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta, timezone
import threading
from typing import Any, Callable


VolumeGlobalFetcher = Callable[[str], dict[str, Any]]
WebsocketConnectedProvider = Callable[[], bool]


class CryptoVolumeManager:
    VALID_STATUSES = {
        "OK",
        "INSUFFICIENT_DATA",
        "STALE",
        "API_ERROR",
        "WEBSOCKET_DISCONNECTED",
        "SYMBOL_ERROR",
        "FALLBACK_USED",
        "UNKNOWN",
    }

    def __init__(
        self,
        *,
        database: Any,
        market_data: Any,
        logger: Any,
        global_fetcher: VolumeGlobalFetcher,
        websocket_connected: WebsocketConnectedProvider,
        websocket_reconnect: Callable[[], None] | None = None,
        global_cache_seconds: int = 600,
        latest_bar_stale_seconds: float = 15.0,
    ) -> None:
        self.database = database
        self.market_data = market_data
        self.logger = logger
        self.global_fetcher = global_fetcher
        self.websocket_connected = websocket_connected
        self.websocket_reconnect = websocket_reconnect
        self.global_cache_seconds = max(300, min(900, int(global_cache_seconds or 600)))
        self.latest_bar_stale_seconds = max(5.0, float(latest_bar_stale_seconds or 15.0))

        self._lock = threading.RLock()
        self._ws_bars_by_symbol: dict[str, deque[dict[str, Any]]] = {}
        self._ws_trades_by_symbol: dict[str, deque[dict[str, Any]]] = {}
        self._global_cache_by_symbol: dict[str, dict[str, Any]] = {}
        self._last_snapshot_by_symbol: dict[str, dict[str, Any]] = {}
        self._last_reconnect_attempt_ts = 0.0

    @staticmethod
    def normalize_symbol(symbol: str) -> str:
        raw = str(symbol or "").upper().replace(" ", "")
        if not raw:
            return ""
        if "/" in raw:
            base, quote = raw.split("/", 1)
            quote = quote.strip()
            if quote in {"USD", "USDC", "USDT"} and base.strip():
                return f"{base.strip()}/{quote}"
            return raw
        for quote in ("USDC", "USDT", "USD"):
            if raw.endswith(quote) and len(raw) > len(quote):
                return f"{raw[:-len(quote)]}/{quote}"
        return raw

    def handle_websocket_event(self, message_type: str, payload: dict[str, Any]) -> None:
        normalized_symbol = self.normalize_symbol(str(payload.get("S", "") or payload.get("symbol", "") or ""))
        if not normalized_symbol:
            return
        if "/" not in normalized_symbol:
            return

        msg_type = str(message_type or "").lower().strip()
        if msg_type not in {"t", "b", "u", "d"}:
            return

        ts = self._parse_timestamp(payload.get("t"))
        if ts is None:
            ts = datetime.now(timezone.utc)

        with self._lock:
            if msg_type == "t":
                trades = self._ws_trades_by_symbol.setdefault(normalized_symbol, deque(maxlen=4000))
                trades.append(
                    {
                        "timestamp": ts,
                        "size": float(payload.get("s", 0.0) or 0.0),
                        "price": float(payload.get("p", payload.get("c", 0.0)) or 0.0),
                    }
                )
                return

            bars = self._ws_bars_by_symbol.setdefault(normalized_symbol, deque(maxlen=300))
            bars.append(
                {
                    "timestamp": ts,
                    "open": float(payload.get("o", payload.get("c", 0.0)) or 0.0),
                    "high": float(payload.get("h", payload.get("c", 0.0)) or 0.0),
                    "low": float(payload.get("l", payload.get("c", 0.0)) or 0.0),
                    "close": float(payload.get("c", 0.0) or 0.0),
                    "volume": float(payload.get("v", 0.0) or 0.0),
                }
            )

    def build_snapshot(
        self,
        symbol: str,
        price: float,
        binance_24h_volume: float = 0.0,
        alpaca_24h_volume: float | None = None,
    ) -> dict[str, Any]:
        normalized_symbol = self.normalize_symbol(symbol)
        if alpaca_24h_volume is not None and float(binance_24h_volume or 0.0) <= 0.0:
            binance_24h_volume = float(alpaca_24h_volume or 0.0)
        now_utc = datetime.now(timezone.utc)
        if not normalized_symbol or "/" not in normalized_symbol:
            snapshot = self._build_error_snapshot(
                symbol=symbol,
                normalized_symbol=normalized_symbol,
                price=price,
                status="SYMBOL_ERROR",
                message=f"Símbolo inválido para volumen: {symbol}",
                now_utc=now_utc,
            )
            self._persist_snapshot(snapshot)
            return snapshot

        ws_connected = bool(self.websocket_connected())
        volume_source = "websocket_bars+websocket_trades"
        error_message = ""
        warning_messages: list[str] = []

        ws_bars_1m = self._bars_from_websocket(normalized_symbol)
        bars_1m = list(ws_bars_1m)
        trade_stats = {
            60: self._trade_stats_from_websocket(normalized_symbol, 60),
            300: self._trade_stats_from_websocket(normalized_symbol, 300),
            900: self._trade_stats_from_websocket(normalized_symbol, 900),
        }

        ws_latest_bar_age_seconds = self._latest_bar_age_seconds(ws_bars_1m, now_utc)
        websocket_stale = (not ws_connected) or ws_latest_bar_age_seconds > self.latest_bar_stale_seconds
        if websocket_stale:
            self._attempt_websocket_reconnect()
            warning_messages.append(
                f"WebSocket stale/disconnected; switching to REST fallback (latest_ws_bar_age={ws_latest_bar_age_seconds:.1f}s)"
            )

        used_rest_fallback = False
        used_snapshot_fallback = False
        if websocket_stale or len(bars_1m) < 15:
            rest_bars, rest_error = self._bars_from_rest(normalized_symbol)
            if rest_bars:
                bars_1m = rest_bars
                used_rest_fallback = True
                volume_source = "rest_bars"
                self.logger.warning("Using fallback REST bars for %s", normalized_symbol)
            elif rest_error:
                error_message = self._append_message(error_message, rest_error)

        for window in (60, 300, 900):
            current = trade_stats[window]
            if current["count"] > 0:
                continue
            rest_trade, trade_error = self._trade_stats_from_rest(normalized_symbol, window)
            if rest_trade["count"] > 0 or rest_trade["volume"] > 0.0:
                trade_stats[window] = rest_trade
                used_rest_fallback = True
                if volume_source.startswith("websocket"):
                    volume_source = "rest_trades"
                else:
                    volume_source = "rest_bars+trades"
            elif trade_error:
                error_message = self._append_message(error_message, trade_error)

        if used_rest_fallback and "rest_bars+trades" not in volume_source:
            if volume_source == "rest_bars":
                volume_source = "rest_bars+trades fallback"
            elif volume_source == "rest_trades":
                volume_source = "rest_bars+trades fallback"

        local_volume_unit = "BASE_ASSET"
        volume_has_clear_unit = True

        if len(bars_1m) < 15:
            fallback_row = self.database.latest_crypto_volume_record(normalized_symbol)
            if fallback_row:
                used_snapshot_fallback = True
                volume_source = "snapshot_fallback"
                local_volume_unit = "UNKNOWN"
                volume_has_clear_unit = False
                warning_messages.append("Using snapshot fallback due to missing REST/WS bars")
                bars_1m = []
                trade_stats = {
                    60: {
                        "count": int(fallback_row.get("trade_count_1m", 0) or 0),
                        "volume": float(fallback_row.get("local_volume_1m", 0.0) or 0.0),
                        "volume_usd": 0.0,
                    },
                    300: {
                        "count": int(fallback_row.get("trade_count_5m", 0) or 0),
                        "volume": float(fallback_row.get("local_volume_5m", 0.0) or 0.0),
                        "volume_usd": 0.0,
                    },
                    900: {
                        "count": int(fallback_row.get("trade_count_15m", 0) or 0),
                        "volume": float(fallback_row.get("local_volume_15m", 0.0) or 0.0),
                        "volume_usd": 0.0,
                    },
                }

        vol_1m_base = self._sum_last_minutes_volume_base(bars_1m, 1, now_utc)
        vol_5m_base = self._sum_last_minutes_volume_base(bars_1m, 5, now_utc)
        vol_15m_base = self._sum_last_minutes_volume_base(bars_1m, 15, now_utc)
        vol_1m_usd = self._sum_last_minutes_volume_usd(bars_1m, 1, now_utc)
        vol_5m_usd = self._sum_last_minutes_volume_usd(bars_1m, 5, now_utc)
        vol_15m_usd = self._sum_last_minutes_volume_usd(bars_1m, 15, now_utc)

        if vol_1m_base <= 0.0 and trade_stats[60]["volume"] > 0.0:
            vol_1m_base = trade_stats[60]["volume"]
        if vol_5m_base <= 0.0 and trade_stats[300]["volume"] > 0.0:
            vol_5m_base = trade_stats[300]["volume"]
        if vol_15m_base <= 0.0 and trade_stats[900]["volume"] > 0.0:
            vol_15m_base = trade_stats[900]["volume"]
        if vol_1m_usd <= 0.0 and trade_stats[60]["volume_usd"] > 0.0:
            vol_1m_usd = trade_stats[60]["volume_usd"]
        if vol_5m_usd <= 0.0 and trade_stats[300]["volume_usd"] > 0.0:
            vol_5m_usd = trade_stats[300]["volume_usd"]
        if vol_15m_usd <= 0.0 and trade_stats[900]["volume_usd"] > 0.0:
            vol_15m_usd = trade_stats[900]["volume_usd"]

        global_payload = self._global_volume(normalized_symbol)
        global_volume = float(global_payload.get("total_volume", 0.0) or 0.0)
        global_source = self._normalize_global_source(global_payload.get("source", "coingecko"))
        global_status = str(global_payload.get("status", "UNKNOWN") or "UNKNOWN")

        binance_24h_status = "OK"
        warning = ""
        if float(binance_24h_volume or 0.0) <= 0.0:
            binance_24h_status = "UNAVAILABLE"
            warning = "Binance 24h volume returned 0.00, ignored"
            self.logger.warning("Binance 24h volume returned 0.00, ignored")

        status, reason = self._evaluate_status(
            ws_connected=ws_connected,
            bars=bars_1m,
            used_rest_fallback=used_rest_fallback,
            used_snapshot_fallback=used_snapshot_fallback,
            has_api_error=bool(error_message),
            websocket_stale=websocket_stale,
            now_utc=now_utc,
        )
        if reason:
            error_message = self._append_message(error_message, reason)

        latest_bar_age_seconds = self._latest_bar_age_seconds(bars_1m, now_utc)
        data_stale = latest_bar_age_seconds > self.latest_bar_stale_seconds
        if data_stale:
            warning_messages.append(f"DATA_STALE=true (latest_bar_age={latest_bar_age_seconds:.1f}s > {self.latest_bar_stale_seconds:.1f}s)")

        warning_text = " | ".join(msg for msg in warning_messages if str(msg or "").strip())
        final_error_message = self._append_message(error_message, warning)
        final_error_message = self._append_message(final_error_message, warning_text)

        volume_valid_for_live_analysis = self._compute_live_volume_validity(
            volume_status=status,
            websocket_stale=websocket_stale,
            data_stale=data_stale,
            latest_bar_age_seconds=latest_bar_age_seconds,
            volume_source=volume_source,
            volume_has_clear_unit=volume_has_clear_unit,
        )
        if (
            not volume_valid_for_live_analysis
            and used_rest_fallback
            and volume_has_clear_unit
            and int(trade_stats[300]["count"]) > 0
            and float(trade_stats[300]["volume"]) > 0.0
        ):
            volume_valid_for_live_analysis = True
            warning_messages.append("REST trades valid for 5m live analysis despite stale bars")
            warning_text = " | ".join(msg for msg in warning_messages if str(msg or "").strip())
            final_error_message = self._append_message(error_message, warning)
            final_error_message = self._append_message(final_error_message, warning_text)

        snapshot = {
            "timestamp": now_utc.isoformat(),
            "symbol": str(symbol or "").upper().replace(" ", ""),
            "normalized_symbol": normalized_symbol,
            "price": float(price or 0.0),
            "local_volume_1m": float(vol_1m_base),
            "local_volume_5m": float(vol_5m_base),
            "local_volume_15m": float(vol_15m_base),
            "local_volume_1m_base": float(vol_1m_base),
            "local_volume_5m_base": float(vol_5m_base),
            "local_volume_15m_base": float(vol_15m_base),
            "local_volume_1m_usd": float(vol_1m_usd),
            "local_volume_5m_usd": float(vol_5m_usd),
            "local_volume_15m_usd": float(vol_15m_usd),
            "local_volume_unit": local_volume_unit,
            "volume_has_clear_unit": bool(volume_has_clear_unit),
            "trade_count_1m": int(trade_stats[60]["count"]),
            "trade_count_5m": int(trade_stats[300]["count"]),
            "trade_count_15m": int(trade_stats[900]["count"]),
            "global_volume_24h_usd": float(global_volume),
            "binance_24h_volume": float(binance_24h_volume or 0.0),
            "binance_24h_volume_status": binance_24h_status,
            "alpaca_24h_volume": float(binance_24h_volume or 0.0),
            "alpaca_24h_volume_status": binance_24h_status,
            "volume_source": volume_source,
            "local_volume_source": volume_source,
            "global_volume_source": global_source,
            "global_volume_status": global_status,
            "volume_status": status,
            "latest_bar_age_seconds": float(latest_bar_age_seconds),
            "ws_latest_bar_age_seconds": float(ws_latest_bar_age_seconds),
            "data_stale": bool(data_stale),
            "websocket_stale": bool(websocket_stale),
            "volume_valid_for_live_analysis": bool(volume_valid_for_live_analysis),
            "error_message": final_error_message,
            "last_update": now_utc.isoformat(),
        }

        self._persist_snapshot(snapshot)
        return snapshot

    def validate_volume_data(self, symbol: str) -> dict[str, Any]:
        normalized = self.normalize_symbol(symbol)
        with self._lock:
            row = dict(self._last_snapshot_by_symbol.get(normalized, {}))
        if not row:
            row = self.database.latest_crypto_volume_record(normalized) or {}
        if not row:
            return {
                "is_valid": False,
                "status": "UNKNOWN",
                "reason": "No hay datos de volumen persistidos",
                "last_update": "",
                "source": "none",
            }

        status = str(row.get("volume_status", "UNKNOWN") or "UNKNOWN")
        latest_bar_age_seconds = float(row.get("latest_bar_age_seconds", 999999.0) or 999999.0)
        data_stale = bool(row.get("data_stale", False)) or (latest_bar_age_seconds > self.latest_bar_stale_seconds)
        websocket_stale = bool(row.get("websocket_stale", False))
        volume_source = str(row.get("volume_source", "unknown") or "unknown")
        volume_has_clear_unit = bool(row.get("volume_has_clear_unit", False))

        is_valid_for_live = self._compute_live_volume_validity(
            volume_status=status,
            websocket_stale=websocket_stale,
            data_stale=data_stale,
            latest_bar_age_seconds=latest_bar_age_seconds,
            volume_source=volume_source,
            volume_has_clear_unit=volume_has_clear_unit,
        )

        is_valid = bool(row.get("volume_valid_for_live_analysis", is_valid_for_live))
        reason = str(row.get("error_message", "") or "")
        if not is_valid and not reason:
            reason = f"Volumen no válido: estado={status}"
        return {
            "is_valid": is_valid,
            "volume_valid_for_live_analysis": is_valid,
            "status": status,
            "reason": reason,
            "last_update": str(row.get("last_update", row.get("timestamp", "")) or ""),
            "source": volume_source,
            "data_stale": data_stale,
            "websocket_stale": websocket_stale,
            "latest_bar_age_seconds": latest_bar_age_seconds,
            "volume_has_clear_unit": volume_has_clear_unit,
        }

    def _evaluate_status(
        self,
        *,
        ws_connected: bool,
        bars: list[dict[str, Any]],
        used_rest_fallback: bool,
        used_snapshot_fallback: bool,
        has_api_error: bool,
        websocket_stale: bool,
        now_utc: datetime,
    ) -> tuple[str, str]:
        if has_api_error and not bars:
            return "API_ERROR", "Volume fetch failed: API timeout/error"
        if used_snapshot_fallback:
            return "STALE", "Using snapshot_fallback; data is not live"
        if not ws_connected and not used_rest_fallback:
            return "WEBSOCKET_DISCONNECTED", "WebSocket desconectado y sin fallback REST"
        if len(bars) < 15:
            return "INSUFFICIENT_DATA", f"Insufficient 1m bars: only {len(bars)} available, need 15"
        age = self._latest_bar_age_seconds(bars, now_utc)
        if age > self.latest_bar_stale_seconds:
            if used_rest_fallback:
                return "FALLBACK_USED", f"REST fallback active + latest bar age={age:.0f}s"
            return "STALE", f"Latest bar stale age={age:.0f}s"
        if websocket_stale and used_rest_fallback:
            return "FALLBACK_USED", "WebSocket stale/disconnected; using REST fallback"
        if used_rest_fallback:
            return "FALLBACK_USED", "Using fallback REST bars/trades"
        return "OK", ""

    def _latest_bar_age_seconds(self, bars: list[dict[str, Any]], now_utc: datetime) -> float:
        if not bars:
            return 999999.0
        latest_ts = self._parse_timestamp((bars[-1] or {}).get("timestamp"))
        if latest_ts is None:
            return 999999.0
        return max(0.0, (now_utc - latest_ts).total_seconds())

    def _attempt_websocket_reconnect(self) -> None:
        if self.websocket_reconnect is None:
            return
        now_ts = datetime.now(timezone.utc).timestamp()
        with self._lock:
            if (now_ts - self._last_reconnect_attempt_ts) < 10.0:
                return
            self._last_reconnect_attempt_ts = now_ts
        try:
            self.websocket_reconnect()
            self.logger.warning("WebSocket reconnect triggered by CryptoVolumeManager")
        except Exception as ex:
            self.logger.warning("WebSocket reconnect attempt failed: %s", ex)

    @staticmethod
    def _normalize_global_source(source_value: Any) -> str:
        source = str(source_value or "coingecko").strip().lower()
        mapping = {
            "coingecko": "CoinGecko",
            "coinmarketcap": "CoinMarketCap",
            "coinbase": "Coinbase",
        }
        return mapping.get(source, source_value if isinstance(source_value, str) and source_value else "CoinGecko")

    def _compute_live_volume_validity(
        self,
        *,
        volume_status: str,
        websocket_stale: bool,
        data_stale: bool,
        latest_bar_age_seconds: float,
        volume_source: str,
        volume_has_clear_unit: bool,
    ) -> bool:
        source = str(volume_source or "").strip().lower()
        if str(volume_status or "UNKNOWN").upper() == "STALE":
            return False
        if websocket_stale:
            return False
        if data_stale:
            return False
        if float(latest_bar_age_seconds) > self.latest_bar_stale_seconds:
            return False
        if source == "snapshot_fallback":
            return False
        if not volume_has_clear_unit:
            return False
        return True

    def _sum_last_minutes_volume_base(self, bars: list[dict[str, Any]], minutes: int, now_utc: datetime) -> float:
        if minutes <= 0:
            return 0.0
        selected = self._bars_inside_window(bars, minutes, now_utc)
        return sum(float(row.get("volume", 0.0) or 0.0) for row in selected)

    def _bars_from_websocket(self, symbol: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = list(self._ws_bars_by_symbol.get(symbol, deque()))
        return self._normalize_minute_bars(rows)

    def _bars_from_rest(self, symbol: str) -> tuple[list[dict[str, Any]], str]:
        try:
            bars = self.market_data.get_candles(symbol=symbol, interval="1m", limit=90)
            normalized_rows: list[dict[str, Any]] = []
            for row in bars:
                normalized_rows.append(
                    {
                        "timestamp": row.get("timestamp"),
                        "open": float(row.get("open", row.get("close", 0.0)) or 0.0),
                        "high": float(row.get("high", row.get("close", 0.0)) or 0.0),
                        "low": float(row.get("low", row.get("close", 0.0)) or 0.0),
                        "close": float(row.get("close", 0.0) or 0.0),
                        "volume": float(row.get("volume", 0.0) or 0.0),
                    }
                )
            return self._normalize_minute_bars(normalized_rows), ""
        except Exception as ex:
            message = f"Volume fetch failed for {symbol}: {ex}"
            self.logger.warning(message)
            return [], message

    def _trade_stats_from_websocket(self, symbol: str, lookback_seconds: int) -> dict[str, Any]:
        now_utc = datetime.now(timezone.utc)
        cutoff = now_utc - timedelta(seconds=int(lookback_seconds))
        with self._lock:
            trades = list(self._ws_trades_by_symbol.get(symbol, deque()))
        selected = [row for row in trades if isinstance(row.get("timestamp"), datetime) and row["timestamp"] >= cutoff]
        volume = sum(float(row.get("size", 0.0) or 0.0) for row in selected)
        volume_usd = sum(
            float(row.get("size", 0.0) or 0.0) * float(row.get("price", 0.0) or 0.0)
            for row in selected
        )
        return {
            "count": len(selected),
            "volume": volume,
            "volume_usd": volume_usd,
        }

    def _trade_stats_from_rest(self, symbol: str, lookback_seconds: int) -> tuple[dict[str, Any], str]:
        try:
            payload = self.market_data.get_recent_trade_stats(symbol=symbol, lookback_seconds=lookback_seconds, limit=5000)
            return {
                "count": int(payload.get("count", 0) or 0),
                "volume": float(payload.get("volume", 0.0) or 0.0),
                "volume_usd": float(payload.get("volume_usd", 0.0) or 0.0),
            }, ""
        except Exception as ex:
            message = f"Volume fetch failed for {symbol}: {ex}"
            self.logger.warning(message)
            return {"count": 0, "volume": 0.0, "volume_usd": 0.0}, message

    def _sum_last_minutes_volume_usd(self, bars: list[dict[str, Any]], minutes: int, now_utc: datetime) -> float:
        if minutes <= 0:
            return 0.0
        selected = self._bars_inside_window(bars, minutes, now_utc)
        return sum(
            float(row.get("volume", 0.0) or 0.0) * float(row.get("close", 0.0) or 0.0)
            for row in selected
        )

    def _bars_inside_window(self, bars: list[dict[str, Any]], minutes: int, now_utc: datetime) -> list[dict[str, Any]]:
        cutoff = now_utc - timedelta(minutes=int(minutes))
        selected: list[dict[str, Any]] = []
        for row in bars:
            ts = self._parse_timestamp(row.get("timestamp"))
            if ts is None or ts < cutoff or ts > now_utc + timedelta(seconds=5):
                continue
            selected.append(row)
        return selected

    def _normalize_minute_bars(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        ordered = sorted(rows, key=lambda item: str(item.get("timestamp", "")))
        aggregated: list[dict[str, Any]] = []
        current_key = ""
        current_row: dict[str, Any] | None = None

        for row in ordered:
            ts = self._parse_timestamp(row.get("timestamp"))
            if ts is None:
                continue
            minute_ts = ts.replace(second=0, microsecond=0)
            key = minute_ts.isoformat()
            if key != current_key:
                if current_row is not None:
                    aggregated.append(current_row)
                current_key = key
                current_row = {
                    "timestamp": key,
                    "open": float(row.get("open", row.get("close", 0.0)) or 0.0),
                    "high": float(row.get("high", row.get("close", 0.0)) or 0.0),
                    "low": float(row.get("low", row.get("close", 0.0)) or 0.0),
                    "close": float(row.get("close", row.get("open", 0.0)) or 0.0),
                    "volume": float(row.get("volume", 0.0) or 0.0),
                }
                continue

            if current_row is None:
                continue
            current_row["high"] = max(float(current_row.get("high", 0.0) or 0.0), float(row.get("high", 0.0) or 0.0))
            current_row["low"] = min(float(current_row.get("low", 0.0) or 0.0), float(row.get("low", 0.0) or 0.0))
            current_row["close"] = float(row.get("close", current_row.get("close", 0.0)) or 0.0)
            current_row["volume"] = max(float(current_row.get("volume", 0.0) or 0.0), float(row.get("volume", 0.0) or 0.0))

        if current_row is not None:
            aggregated.append(current_row)
        return aggregated

    def _global_volume(self, symbol: str) -> dict[str, Any]:
        now_monotonic = datetime.now(timezone.utc).timestamp()
        with self._lock:
            cached = dict(self._global_cache_by_symbol.get(symbol, {}))
        if cached:
            fetched_ts = float(cached.get("_cached_ts", 0.0) or 0.0)
            if (now_monotonic - fetched_ts) <= float(self.global_cache_seconds):
                return cached

        try:
            payload = dict(self.global_fetcher(symbol) or {})
            payload["_cached_ts"] = now_monotonic
            with self._lock:
                self._global_cache_by_symbol[symbol] = dict(payload)
            self.logger.info("CoinGecko global volume updated successfully")
            return payload
        except Exception as ex:
            message = f"CoinGecko global volume failed for {symbol}: {ex}"
            self.logger.warning(message)
            fallback = {
                "total_volume": 0.0,
                "source": "coingecko",
                "status": "API_ERROR",
                "error": str(ex),
                "_cached_ts": now_monotonic,
            }
            with self._lock:
                self._global_cache_by_symbol[symbol] = dict(fallback)
            return fallback

    @staticmethod
    def _parse_timestamp(raw_value: Any) -> datetime | None:
        raw = str(raw_value or "").strip()
        if not raw:
            return None
        normalized = raw.replace("Z", "+00:00") if raw.endswith("Z") else raw
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _persist_snapshot(self, snapshot: dict[str, Any]) -> None:
        with self._lock:
            self._last_snapshot_by_symbol[str(snapshot.get("normalized_symbol", ""))] = dict(snapshot)
        self.database.insert_crypto_volume_record(snapshot)

    @staticmethod
    def _append_message(current: str, extra: str) -> str:
        left = str(current or "").strip()
        right = str(extra or "").strip()
        if not left:
            return right
        if not right:
            return left
        return f"{left} | {right}"

    def _build_error_snapshot(
        self,
        *,
        symbol: str,
        normalized_symbol: str,
        price: float,
        status: str,
        message: str,
        now_utc: datetime,
    ) -> dict[str, Any]:
        final_status = status if status in self.VALID_STATUSES else "UNKNOWN"
        return {
            "timestamp": now_utc.isoformat(),
            "symbol": str(symbol or "").upper().replace(" ", ""),
            "normalized_symbol": normalized_symbol,
            "price": float(price or 0.0),
            "local_volume_1m": 0.0,
            "local_volume_5m": 0.0,
            "local_volume_15m": 0.0,
            "local_volume_1m_base": 0.0,
            "local_volume_5m_base": 0.0,
            "local_volume_15m_base": 0.0,
            "local_volume_1m_usd": 0.0,
            "local_volume_5m_usd": 0.0,
            "local_volume_15m_usd": 0.0,
            "local_volume_unit": "UNKNOWN",
            "volume_has_clear_unit": False,
            "trade_count_1m": 0,
            "trade_count_5m": 0,
            "trade_count_15m": 0,
            "global_volume_24h_usd": 0.0,
            "binance_24h_volume": 0.0,
            "binance_24h_volume_status": "UNAVAILABLE",
            "volume_source": "none",
            "local_volume_source": "none",
            "global_volume_source": "CoinGecko",
            "global_volume_status": "UNKNOWN",
            "volume_status": final_status,
            "latest_bar_age_seconds": 999999.0,
            "ws_latest_bar_age_seconds": 999999.0,
            "data_stale": True,
            "websocket_stale": True,
            "volume_valid_for_live_analysis": False,
            "error_message": message,
            "last_update": now_utc.isoformat(),
        }
