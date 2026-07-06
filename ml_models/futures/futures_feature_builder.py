from __future__ import annotations

import json
import math
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests


@dataclass(frozen=True)
class FuturesFeatureBuilderConfig:
    endpoint: str
    api_key: str = ""
    symbol: str = "BTCUSDT"
    db_path: str = "ml_models/futures/futures_dataset.sqlite"
    lookback_limit: int = 1500


class FuturesFeatureBuilder:
    """Builds Binance Futures feature rows isolated from stock/spot datasets."""

    def __init__(self, config: FuturesFeatureBuilderConfig, news_provider: Any | None = None, logger: Any | None = None) -> None:
        self.config = config
        self.news_provider = news_provider
        self.logger = logger
        self._session = requests.Session()
        self._db_path = Path(config.db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_storage()

    def _log(self, level: str, message: str, *args: Any) -> None:
        if self.logger is not None:
            fn = getattr(self.logger, level, None)
            if callable(fn):
                fn(message, *args)
                return
        if args:
            message = message % args
        print(f"[{level.upper()}] {message}")

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.config.api_key:
            headers["X-MBX-APIKEY"] = self.config.api_key
        return headers

    def _request_json(self, path: str, params: dict[str, Any] | None = None, timeout: int = 12) -> Any:
        url = f"{self.config.endpoint.rstrip('/')}{path}"
        response = self._session.get(url, params=params or {}, headers=self._headers(), timeout=timeout)
        response.raise_for_status()
        return response.json()

    def _safe_request_json(self, path: str, params: dict[str, Any] | None = None, timeout: int = 12, fallback: Any = None) -> Any:
        try:
            return self._request_json(path=path, params=params, timeout=timeout)
        except Exception as ex:
            self._log("warning", "Futures feature request failed %s params=%s: %s", path, params or {}, ex)
            return fallback

    def _init_storage(self) -> None:
        with sqlite3.connect(self._db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS futures_features (
                    ts TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    PRIMARY KEY (ts, symbol)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS futures_labels (
                    ts TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    PRIMARY KEY (ts, symbol)
                )
                """
            )

    @staticmethod
    def _ema(values: list[float], period: int) -> float:
        if not values:
            return 0.0
        alpha = 2.0 / (period + 1.0)
        ema_val = float(values[0])
        for value in values[1:]:
            ema_val = alpha * float(value) + (1.0 - alpha) * ema_val
        return float(ema_val)

    @staticmethod
    def _std(values: list[float]) -> float:
        if not values:
            return 0.0
        mean = sum(values) / len(values)
        var = sum((value - mean) ** 2 for value in values) / len(values)
        return math.sqrt(var)

    @staticmethod
    def _atr(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> float:
        if len(highs) < 2 or len(lows) < 2 or len(closes) < 2:
            return 0.0
        trs: list[float] = []
        for idx in range(1, len(closes)):
            high = float(highs[idx])
            low = float(lows[idx])
            prev_close = float(closes[idx - 1])
            trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
        window = trs[-period:] if len(trs) >= period else trs
        return sum(window) / max(len(window), 1)

    @staticmethod
    def _rsi(closes: list[float], period: int = 14) -> float:
        if len(closes) <= period:
            return 50.0
        gains: list[float] = []
        losses: list[float] = []
        for idx in range(1, len(closes)):
            change = float(closes[idx]) - float(closes[idx - 1])
            gains.append(max(change, 0.0))
            losses.append(abs(min(change, 0.0)))
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        if avg_loss <= 1e-9:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    @staticmethod
    def _vwap(closes: list[float], volumes: list[float]) -> float:
        num = 0.0
        den = 0.0
        for close, volume in zip(closes, volumes):
            num += float(close) * float(volume)
            den += float(volume)
        return (num / den) if den > 0 else 0.0

    @staticmethod
    def _iso_from_ms(ms: int) -> str:
        return datetime.fromtimestamp(float(ms) / 1000.0, tz=timezone.utc).isoformat()

    def _fetch_news_scores(self, symbol: str, ts_iso: str) -> dict[str, float]:
        if self.news_provider is None:
            return {
                "news_score": 0.0,
                "sentiment_score": 0.0,
                "event_risk_score": 0.0,
                "catalyst_score": 0.0,
            }
        try:
            ts_dt = datetime.fromisoformat(ts_iso)
            since = (ts_dt - timedelta(hours=2)).isoformat()
            rows = self.news_provider.list_news_events_since(since_iso=since, symbol=symbol, limit=60)
        except Exception:
            rows = []

        if not rows:
            return {
                "news_score": 0.0,
                "sentiment_score": 0.0,
                "event_risk_score": 0.0,
                "catalyst_score": 0.0,
            }

        influence_total = sum(float(row.get("influence_score", 0.0) or 0.0) for row in rows)
        if influence_total <= 0:
            influence_total = 1.0
        sentiment = sum(float(row.get("sentiment_score", 0.0) or 0.0) * float(row.get("influence_score", 0.0) or 0.0) for row in rows) / influence_total
        event_risk = 0.0
        catalyst = 0.0
        for row in rows:
            payload = row.get("raw_payload") or {}
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except Exception:
                    payload = {}
            risk_score = float(payload.get("risk_score", 0.0) or 0.0) / 100.0
            importance = float(payload.get("importance_score", 0.0) or 0.0) / 100.0
            event_risk += risk_score
            catalyst += importance
        event_risk /= max(len(rows), 1)
        catalyst /= max(len(rows), 1)
        news_score = max(0.0, min(1.0, (catalyst + (sentiment + 1.0) / 2.0) / 2.0))
        return {
            "news_score": news_score,
            "sentiment_score": sentiment,
            "event_risk_score": max(0.0, min(1.0, event_risk)),
            "catalyst_score": max(0.0, min(1.0, catalyst)),
        }

    def build_rows(self, symbol: str | None = None, limit: int | None = None) -> list[dict[str, Any]]:
        ticker = str(symbol or self.config.symbol).upper().strip()
        max_rows = max(50, min(int(limit or self.config.lookback_limit), 1500))
        klines = self._safe_request_json(
            "/fapi/v1/klines",
            params={"symbol": ticker, "interval": "1m", "limit": max_rows},
            fallback=[],
        )
        if not isinstance(klines, list) or not klines:
            return []

        premium = self._safe_request_json("/fapi/v1/premiumIndex", params={"symbol": ticker}, fallback={}) or {}
        open_interest_payload = self._safe_request_json("/fapi/v1/openInterest", params={"symbol": ticker}, fallback={}) or {}
        orderbook = self._safe_request_json("/fapi/v1/depth", params={"symbol": ticker, "limit": 20}, fallback={}) or {}
        force_orders = self._safe_request_json("/fapi/v1/forceOrders", params={"symbol": ticker, "limit": 50}, fallback=[]) or []
        long_short = self._safe_request_json(
            "/futures/data/globalLongShortAccountRatio",
            params={"symbol": ticker, "period": "5m", "limit": 1},
            fallback=[],
        )
        leverage_brackets = self._safe_request_json("/fapi/v1/leverageBracket", params={"symbol": ticker}, fallback=[])

        bids = orderbook.get("bids", []) if isinstance(orderbook, dict) else []
        asks = orderbook.get("asks", []) if isinstance(orderbook, dict) else []
        best_bid = float(bids[0][0]) if bids else 0.0
        best_ask = float(asks[0][0]) if asks else 0.0
        bid_qty = sum(float(row[1]) for row in bids[:5]) if bids else 0.0
        ask_qty = sum(float(row[1]) for row in asks[:5]) if asks else 0.0
        orderbook_imbalance = ((bid_qty - ask_qty) / (bid_qty + ask_qty)) if (bid_qty + ask_qty) > 0 else 0.0

        long_short_ratio = 0.0
        if isinstance(long_short, list) and long_short:
            long_short_ratio = float(long_short[0].get("longShortRatio", 0.0) or 0.0)

        liquidation_events = len(force_orders) if isinstance(force_orders, list) else 0
        mark_price = float(premium.get("markPrice", 0.0) or 0.0)
        index_price = float(premium.get("indexPrice", 0.0) or 0.0)
        funding_rate = float(premium.get("lastFundingRate", 0.0) or 0.0)
        next_funding_time = float(premium.get("nextFundingTime", 0.0) or 0.0)
        open_interest = float(open_interest_payload.get("openInterest", 0.0) or 0.0)

        leverage_bracket = 1.0
        maintenance_margin = 0.0
        if isinstance(leverage_brackets, list) and leverage_brackets:
            first = leverage_brackets[0]
            brackets = first.get("brackets", []) if isinstance(first, dict) else []
            if brackets:
                leverage_bracket = float(brackets[0].get("initialLeverage", 1.0) or 1.0)
                maintenance_margin = float(brackets[0].get("maintMarginRatio", 0.0) or 0.0)

        closes = [float(row[4]) for row in klines]
        highs = [float(row[2]) for row in klines]
        lows = [float(row[3]) for row in klines]
        vols = [float(row[5]) for row in klines]

        rows: list[dict[str, Any]] = []
        for idx, row in enumerate(klines):
            if idx < 25:
                continue
            close = float(row[4])
            prev_close_1 = float(klines[idx - 1][4])
            prev_close_3 = float(klines[idx - 3][4]) if idx >= 3 else prev_close_1
            prev_close_5 = float(klines[idx - 5][4]) if idx >= 5 else prev_close_1
            prev_close_15 = float(klines[idx - 15][4]) if idx >= 15 else prev_close_1

            window_close_5 = closes[max(0, idx - 4) : idx + 1]
            window_close_15 = closes[max(0, idx - 14) : idx + 1]
            window_high_15 = highs[max(0, idx - 14) : idx + 1]
            window_low_15 = lows[max(0, idx - 14) : idx + 1]
            window_vol_20 = vols[max(0, idx - 19) : idx + 1]
            vwap = self._vwap(window_close_15, window_vol_20[-len(window_close_15) :])

            taker_buy_volume = float(row[9])
            taker_sell_volume = max(float(row[5]) - taker_buy_volume, 0.0)
            avg_volume_20 = (sum(window_vol_20) / len(window_vol_20)) if window_vol_20 else 0.0
            volume_spike_score = (float(row[5]) / avg_volume_20) if avg_volume_20 > 0 else 0.0
            ts_iso = self._iso_from_ms(int(row[0]))
            news_scores = self._fetch_news_scores(symbol=ticker, ts_iso=ts_iso)

            payload = {
                "ts": ts_iso,
                "symbol": ticker,
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": close,
                "volume": float(row[5]),
                "quote_volume": float(row[7]),
                "number_of_trades": float(row[8]),
                "taker_buy_volume": taker_buy_volume,
                "taker_sell_volume": taker_sell_volume,
                "spread": max(best_ask - best_bid, 0.0),
                "best_bid": best_bid,
                "best_ask": best_ask,
                "orderbook_imbalance": orderbook_imbalance,
                "mark_price": mark_price,
                "index_price": index_price,
                "basis": (mark_price - index_price) if mark_price and index_price else 0.0,
                "funding_rate": funding_rate,
                "open_interest": open_interest,
                "liquidation_events": float(liquidation_events),
                "long_short_ratio": long_short_ratio,
                "leverage_bracket": leverage_bracket,
                "maintenance_margin": maintenance_margin,
                "returns_1m": ((close - prev_close_1) / prev_close_1) if prev_close_1 > 0 else 0.0,
                "returns_3m": ((close - prev_close_3) / prev_close_3) if prev_close_3 > 0 else 0.0,
                "returns_5m": ((close - prev_close_5) / prev_close_5) if prev_close_5 > 0 else 0.0,
                "returns_15m": ((close - prev_close_15) / prev_close_15) if prev_close_15 > 0 else 0.0,
                "volatility_5m": self._std(window_close_5),
                "volatility_15m": self._std(window_close_15),
                "ATR": self._atr(window_high_15, window_low_15, window_close_15, period=14),
                "RSI": self._rsi(window_close_15, period=14),
                "VWAP": vwap,
                "EMA_9": self._ema(closes[max(0, idx - 20) : idx + 1], period=9),
                "EMA_21": self._ema(closes[max(0, idx - 40) : idx + 1], period=21),
                "volume_spike_score": volume_spike_score,
                "taker_buy_sell_delta": (taker_buy_volume - taker_sell_volume),
                "news_score": float(news_scores["news_score"]),
                "sentiment_score": float(news_scores["sentiment_score"]),
                "event_risk_score": float(news_scores["event_risk_score"]),
                "catalyst_score": float(news_scores["catalyst_score"]),
                "next_funding_time": next_funding_time,
                "data_fresh": True,
                "websocket_connected": True,
            }
            rows.append(payload)
        return rows

    def save_features(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        with sqlite3.connect(self._db_path) as conn:
            conn.executemany(
                "INSERT OR REPLACE INTO futures_features(ts, symbol, payload) VALUES (?, ?, ?)",
                [
                    (
                        str(row.get("ts", "")),
                        str(row.get("symbol", self.config.symbol)),
                        json.dumps(row, ensure_ascii=False),
                    )
                    for row in rows
                ],
            )
        return len(rows)

    def build_and_save(self, symbol: str | None = None, limit: int | None = None) -> list[dict[str, Any]]:
        rows = self.build_rows(symbol=symbol, limit=limit)
        self.save_features(rows)
        return rows

    def load_features(self, symbol: str | None = None) -> list[dict[str, Any]]:
        ticker = str(symbol or self.config.symbol).upper().strip()
        with sqlite3.connect(self._db_path) as conn:
            rows = conn.execute(
                "SELECT payload FROM futures_features WHERE symbol = ? ORDER BY ts ASC",
                (ticker,),
            ).fetchall()
        payloads: list[dict[str, Any]] = []
        for row in rows:
            try:
                payloads.append(json.loads(str(row[0] or "{}")))
            except Exception:
                continue
        return payloads

    def export_parquet_if_available(self, output_path: str) -> bool:
        try:
            import pandas as pd  # type: ignore

            rows = self.load_features(symbol=self.config.symbol)
            if not rows:
                return False
            frame = pd.DataFrame(rows)
            frame.to_parquet(output_path, index=False)
            return True
        except Exception as ex:
            self._log("warning", "Parquet export skipped: %s", ex)
            return False
