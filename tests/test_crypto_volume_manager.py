from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from typing import Any

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ai_trading_brain.crypto_volume_manager import CryptoVolumeManager


class FakeLogger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def info(self, message: str, *args: Any) -> None:
        self.messages.append(str(message % args) if args else str(message))

    def warning(self, message: str, *args: Any) -> None:
        self.messages.append(str(message % args) if args else str(message))


class FakeDatabase:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []

    def insert_crypto_volume_record(self, payload: dict[str, Any]) -> int:
        self.rows.append(dict(payload))
        return len(self.rows)

    def latest_crypto_volume_record(self, normalized_symbol: str) -> dict[str, Any] | None:
        key = str(normalized_symbol or "").upper().replace(" ", "")
        for row in reversed(self.rows):
            if str(row.get("normalized_symbol", "") or "").upper().replace(" ", "") == key:
                return dict(row)
        return None


class FakeMarketData:
    def __init__(self) -> None:
        self.bars: list[dict[str, Any]] = []
        self.trade_stats_by_window: dict[int, dict[str, Any]] = {}
        self.fail_bars = False
        self.fail_trades = False

    def get_candles(self, symbol: str, interval: str = "1m", limit: int = 90) -> list[dict[str, Any]]:
        if self.fail_bars:
            raise TimeoutError("API timeout")
        return list(self.bars[-limit:])

    def get_recent_trade_stats(self, symbol: str, lookback_seconds: int = 60, limit: int = 5000) -> dict[str, Any]:
        if self.fail_trades:
            raise RuntimeError("API 500")
        payload = dict(self.trade_stats_by_window.get(int(lookback_seconds), {}))
        return {
            "count": int(payload.get("count", 0) or 0),
            "volume": float(payload.get("volume", 0.0) or 0.0),
            "volume_usd": float(payload.get("volume_usd", payload.get("volume", 0.0)) or 0.0),
            "lookback_seconds": int(lookback_seconds),
        }


def _minute_bar(minutes_ago: int, volume: float, close: float = 100.0) -> dict[str, Any]:
    ts = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    return {
        "timestamp": ts.isoformat(),
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": volume,
    }


def test_symbol_error_status() -> None:
    manager = CryptoVolumeManager(
        database=FakeDatabase(),
        market_data=FakeMarketData(),
        logger=FakeLogger(),
        global_fetcher=lambda _symbol: {"total_volume": 0.0, "source": "coingecko", "status": "OK"},
        websocket_connected=lambda: True,
    )
    row = manager.build_snapshot(symbol="INVALID", price=100.0, binance_24h_volume=0.0)
    assert row["volume_status"] == "SYMBOL_ERROR"


def test_api_error_when_rest_fails() -> None:
    db = FakeDatabase()
    market = FakeMarketData()
    market.fail_bars = True
    market.fail_trades = True
    manager = CryptoVolumeManager(
        database=db,
        market_data=market,
        logger=FakeLogger(),
        global_fetcher=lambda _symbol: {"total_volume": 1.0, "source": "coingecko", "status": "OK"},
        websocket_connected=lambda: False,
    )
    row = manager.build_snapshot(symbol="SOL/USD", price=100.0, binance_24h_volume=0.0)
    assert row["volume_status"] == "API_ERROR"
    assert "API timeout" in row["error_message"] or "API 500" in row["error_message"]


def test_binance_24h_zero_marked_unavailable() -> None:
    manager = CryptoVolumeManager(
        database=FakeDatabase(),
        market_data=FakeMarketData(),
        logger=FakeLogger(),
        global_fetcher=lambda _symbol: {"total_volume": 42.0, "source": "coingecko", "status": "OK"},
        websocket_connected=lambda: True,
    )
    for i in range(15):
        manager.handle_websocket_event("b", {"S": "SOL/USD", "t": _minute_bar(14 - i, 0.0)["timestamp"], "c": 100.0, "v": float(i + 1)})
    row = manager.build_snapshot(symbol="SOL/USD", price=100.0, binance_24h_volume=0.0)
    assert row["binance_24h_volume_status"] == "UNAVAILABLE"


def test_websocket_disconnected_status() -> None:
    manager = CryptoVolumeManager(
        database=FakeDatabase(),
        market_data=FakeMarketData(),
        logger=FakeLogger(),
        global_fetcher=lambda _symbol: {"total_volume": 12.0, "source": "coingecko", "status": "OK"},
        websocket_connected=lambda: False,
    )
    row = manager.build_snapshot(symbol="SOL/USD", price=100.0, binance_24h_volume=10.0)
    assert row["volume_status"] == "WEBSOCKET_DISCONNECTED"


def test_insufficient_bars_status() -> None:
    market = FakeMarketData()
    market.bars = [_minute_bar(minutes_ago=0, volume=1.0)]
    manager = CryptoVolumeManager(
        database=FakeDatabase(),
        market_data=market,
        logger=FakeLogger(),
        global_fetcher=lambda _symbol: {"total_volume": 12.0, "source": "coingecko", "status": "OK"},
        websocket_connected=lambda: True,
    )
    row = manager.build_snapshot(symbol="SOL/USD", price=100.0, binance_24h_volume=10.0)
    assert row["volume_status"] == "INSUFFICIENT_DATA"


def test_volume_5m_15m_calculation_from_minute_bars() -> None:
    manager = CryptoVolumeManager(
        database=FakeDatabase(),
        market_data=FakeMarketData(),
        logger=FakeLogger(),
        global_fetcher=lambda _symbol: {"total_volume": 100.0, "source": "coingecko", "status": "OK"},
        websocket_connected=lambda: True,
    )
    base = datetime.now(timezone.utc) - timedelta(minutes=14)
    for i in range(15):
        ts = (base + timedelta(minutes=i)).isoformat()
        manager.handle_websocket_event("b", {"S": "SOL/USD", "t": ts, "c": 100.0 + i, "v": float(i + 1)})
    row = manager.build_snapshot(symbol="SOL/USD", price=101.0, binance_24h_volume=50.0)
    expected_5m_base = sum(float(v) for v in [11, 12, 13, 14, 15])
    expected_15m_base = sum(float(i + 1) for i in range(15))
    expected_5m_usd = sum(float(v) * float(c) for v, c in [(11, 110), (12, 111), (13, 112), (14, 113), (15, 114)])
    expected_15m_usd = sum(float(i + 1) * float(100 + i) for i in range(15))
    assert abs(float(row["local_volume_5m"]) - expected_5m_base) < 1e-8
    assert abs(float(row["local_volume_15m"]) - expected_15m_base) < 1e-8
    assert abs(float(row["local_volume_5m_usd"]) - expected_5m_usd) < 1e-8
    assert abs(float(row["local_volume_15m_usd"]) - expected_15m_usd) < 1e-8
    assert str(row.get("local_volume_unit", "")) == "BASE_ASSET"


def test_data_stale_and_live_validation_flags() -> None:
    market = FakeMarketData()
    market.bars = [_minute_bar(minutes_ago=16 - i, volume=float(i + 1), close=100.0) for i in range(15)]
    manager = CryptoVolumeManager(
        database=FakeDatabase(),
        market_data=market,
        logger=FakeLogger(),
        global_fetcher=lambda _symbol: {"total_volume": 100.0, "source": "coingecko", "status": "OK"},
        websocket_connected=lambda: False,
    )
    row = manager.build_snapshot(symbol="SOL/USD", price=100.0, binance_24h_volume=10.0)
    assert bool(row["data_stale"]) is True
    assert bool(row["websocket_stale"]) is True
    assert bool(row["volume_valid_for_live_analysis"]) is False


def test_old_rest_bars_do_not_count_as_current_windows() -> None:
    market = FakeMarketData()
    market.bars = [_minute_bar(minutes_ago=30 - i, volume=float(i + 1), close=100.0) for i in range(15)]
    manager = CryptoVolumeManager(
        database=FakeDatabase(),
        market_data=market,
        logger=FakeLogger(),
        global_fetcher=lambda _symbol: {"total_volume": 100.0, "source": "coingecko", "status": "OK"},
        websocket_connected=lambda: False,
    )
    row = manager.build_snapshot(symbol="SOL/USD", price=100.0, binance_24h_volume=10.0)
    assert float(row["local_volume_5m"]) == 0.0
    assert float(row["local_volume_15m"]) == 0.0


def test_trade_fallback_populates_5m_and_15m_when_bars_are_stale() -> None:
    market = FakeMarketData()
    market.bars = [_minute_bar(minutes_ago=30 - i, volume=0.0, close=100.0) for i in range(15)]
    market.trade_stats_by_window = {
        60: {"count": 1, "volume": 0.25, "volume_usd": 25.0},
        300: {"count": 2, "volume": 3.65, "volume_usd": 295.0},
        900: {"count": 6, "volume": 11.75, "volume_usd": 952.0},
    }
    manager = CryptoVolumeManager(
        database=FakeDatabase(),
        market_data=market,
        logger=FakeLogger(),
        global_fetcher=lambda _symbol: {"total_volume": 100.0, "source": "coingecko", "status": "OK"},
        websocket_connected=lambda: False,
    )
    row = manager.build_snapshot(symbol="SOL/USDC", price=100.0, binance_24h_volume=10.0)
    assert float(row["local_volume_1m"]) == 0.25
    assert float(row["local_volume_5m"]) == 3.65
    assert float(row["local_volume_15m"]) == 11.75
    assert float(row["local_volume_5m_usd"]) == 295.0
    assert float(row["local_volume_15m_usd"]) == 952.0


def test_coingecko_failure_sets_api_error_status_global() -> None:
    manager = CryptoVolumeManager(
        database=FakeDatabase(),
        market_data=FakeMarketData(),
        logger=FakeLogger(),
        global_fetcher=lambda _symbol: (_ for _ in ()).throw(RuntimeError("cg down")),
        websocket_connected=lambda: True,
    )
    for i in range(15):
        manager.handle_websocket_event("b", {"S": "SOL/USD", "t": _minute_bar(14 - i, float(i + 1))["timestamp"], "c": 100.0, "v": float(i + 1)})
    row = manager.build_snapshot(symbol="SOL/USD", price=100.0, binance_24h_volume=1.0)
    assert row["global_volume_status"] == "API_ERROR"


def test_fallback_rest_used_successfully() -> None:
    market = FakeMarketData()
    market.bars = [_minute_bar(minutes_ago=14 - i, volume=float(i + 1)) for i in range(15)]
    market.trade_stats_by_window = {
        60: {"count": 1, "volume": 0.25, "volume_usd": 25.0},
        300: {"count": 5, "volume": 1.20, "volume_usd": 120.0},
        900: {"count": 7, "volume": 1.80, "volume_usd": 180.0},
    }
    manager = CryptoVolumeManager(
        database=FakeDatabase(),
        market_data=market,
        logger=FakeLogger(),
        global_fetcher=lambda _symbol: {"total_volume": 999.0, "source": "coingecko", "status": "OK"},
        websocket_connected=lambda: False,
    )
    row = manager.build_snapshot(symbol="SOL/USDC", price=99.0, binance_24h_volume=0.0)
    assert row["volume_status"] == "FALLBACK_USED"
    assert "rest" in str(row["volume_source"]).lower()
    assert int(row["trade_count_5m"]) == 5
