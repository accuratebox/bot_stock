from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class FuturesLabelerConfig:
    db_path: str = "ml_models/futures/futures_dataset.sqlite"
    fee_bps: float = 4.0
    slippage_bps: float = 2.0
    min_positive_ev: float = 0.0002
    max_allowed_spread: float = 0.0020
    max_allowed_volatility_5m: float = 0.01
    tp_candidates: tuple[float, ...] = (0.003, 0.005, 0.008)
    sl_candidates: tuple[float, ...] = (0.002, 0.0035, 0.005)
    horizons: tuple[int, ...] = (5, 10, 15)


class FuturesLabeler:
    LABELS = ("LONG", "SHORT", "NO_TRADE")

    def __init__(self, config: FuturesLabelerConfig, logger: Any | None = None) -> None:
        self.config = config
        self.logger = logger
        self._db_path = Path(config.db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

    def _log(self, level: str, message: str, *args: Any) -> None:
        if self.logger is not None:
            fn = getattr(self.logger, level, None)
            if callable(fn):
                fn(message, *args)
                return
        if args:
            message = message % args
        print(f"[{level.upper()}] {message}")

    def _load_features(self, symbol: str) -> list[dict[str, Any]]:
        with sqlite3.connect(self._db_path) as conn:
            rows = conn.execute(
                "SELECT payload FROM futures_features WHERE symbol = ? ORDER BY ts ASC",
                (symbol.upper(),),
            ).fetchall()
        payloads: list[dict[str, Any]] = []
        for row in rows:
            try:
                payloads.append(json.loads(str(row[0] or "{}")))
            except Exception:
                continue
        return payloads

    def _save_labels(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        with sqlite3.connect(self._db_path) as conn:
            conn.executemany(
                "INSERT OR REPLACE INTO futures_labels(ts, symbol, payload) VALUES (?, ?, ?)",
                [
                    (
                        str(row.get("ts", "")),
                        str(row.get("symbol", "")),
                        json.dumps(row, ensure_ascii=False),
                    )
                    for row in rows
                ],
            )
        return len(rows)

    @staticmethod
    def _long_path_returns(entry: float, high: float, low: float, close: float) -> tuple[float, float, float]:
        if entry <= 0:
            return 0.0, 0.0, 0.0
        max_profit = (high - entry) / entry
        max_drawdown = (low - entry) / entry
        close_ret = (close - entry) / entry
        return max_profit, max_drawdown, close_ret

    @staticmethod
    def _short_path_returns(entry: float, high: float, low: float, close: float) -> tuple[float, float, float]:
        if entry <= 0:
            return 0.0, 0.0, 0.0
        max_profit = (entry - low) / entry
        max_drawdown = (entry - high) / entry
        close_ret = (entry - close) / entry
        return max_profit, max_drawdown, close_ret

    def _simulate_direction(
        self,
        side: str,
        entry: float,
        future_rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        fees_and_slippage = (self.config.fee_bps + self.config.slippage_bps) / 10000.0
        outcomes: list[dict[str, float]] = []

        for horizon in self.config.horizons:
            path = future_rows[:horizon]
            if not path:
                continue

            for tp in self.config.tp_candidates:
                for sl in self.config.sl_candidates:
                    win = 0.0
                    loss = 0.0
                    neutral = 0.0
                    max_profit = -1e9
                    max_drawdown = 1e9

                    touched = False
                    for row in path:
                        high = float(row.get("high", entry) or entry)
                        low = float(row.get("low", entry) or entry)
                        close = float(row.get("close", entry) or entry)

                        if side == "LONG":
                            p, d, c = self._long_path_returns(entry, high, low, close)
                        else:
                            p, d, c = self._short_path_returns(entry, high, low, close)

                        max_profit = max(max_profit, p)
                        max_drawdown = min(max_drawdown, d)

                        if p >= tp:
                            win = tp
                            touched = True
                            break
                        if d <= -sl:
                            loss = -sl
                            touched = True
                            break

                    if not touched:
                        last = path[-1]
                        high = float(last.get("high", entry) or entry)
                        low = float(last.get("low", entry) or entry)
                        close = float(last.get("close", entry) or entry)
                        if side == "LONG":
                            _, _, neutral = self._long_path_returns(entry, high, low, close)
                        else:
                            _, _, neutral = self._short_path_returns(entry, high, low, close)

                    pnl = (win if win > 0 else (loss if loss < 0 else neutral)) - fees_and_slippage
                    outcomes.append(
                        {
                            "horizon": float(horizon),
                            "tp": float(tp),
                            "sl": float(sl),
                            "pnl": float(pnl),
                            "is_win": 1.0 if pnl > 0 else 0.0,
                            "is_loss": 1.0 if pnl < 0 else 0.0,
                            "max_profit": float(max_profit if max_profit > -1e8 else 0.0),
                            "max_drawdown": float(max_drawdown if max_drawdown < 1e8 else 0.0),
                        }
                    )

        if not outcomes:
            return {
                "result": "neutral",
                "prob_win": 0.0,
                "prob_loss": 0.0,
                "avg_win": 0.0,
                "avg_loss": 0.0,
                "expected_value": 0.0,
                "max_profit": 0.0,
                "max_drawdown": 0.0,
            }

        wins = [row for row in outcomes if row["pnl"] > 0]
        losses = [row for row in outcomes if row["pnl"] < 0]
        prob_win = len(wins) / len(outcomes)
        prob_loss = len(losses) / len(outcomes)
        avg_win = (sum(row["pnl"] for row in wins) / len(wins)) if wins else 0.0
        avg_loss = abs(sum(row["pnl"] for row in losses) / len(losses)) if losses else 0.0
        ev = (prob_win * avg_win) - (prob_loss * avg_loss)

        best_profit = max((row["max_profit"] for row in outcomes), default=0.0)
        worst_drawdown = min((row["max_drawdown"] for row in outcomes), default=0.0)

        if ev > self.config.min_positive_ev:
            result = "win"
        elif ev < -self.config.min_positive_ev:
            result = "loss"
        else:
            result = "neutral"

        return {
            "result": result,
            "prob_win": prob_win,
            "prob_loss": prob_loss,
            "avg_win": avg_win,
            "avg_loss": avg_loss,
            "expected_value": ev,
            "max_profit": best_profit,
            "max_drawdown": worst_drawdown,
        }

    def _no_trade_score(self, row: dict[str, Any], long_ev: float, short_ev: float) -> tuple[float, list[str]]:
        spread = float(row.get("spread", 0.0) or 0.0)
        price = float(row.get("close", 0.0) or 0.0)
        spread_pct = (spread / price) if price > 0 else 1.0
        vol_5m = abs(float(row.get("volatility_5m", 0.0) or 0.0))
        data_fresh = bool(row.get("data_fresh", False))
        websocket_connected = bool(row.get("websocket_connected", False))

        reasons: list[str] = []
        score = 0.0
        if not data_fresh:
            reasons.append("DATA_STALE")
            score += 1.0
        if not websocket_connected:
            reasons.append("WEBSOCKET_DISCONNECTED")
            score += 1.0
        if spread_pct > self.config.max_allowed_spread:
            reasons.append("SPREAD_HIGH")
            score += 0.8
        if vol_5m > self.config.max_allowed_volatility_5m:
            reasons.append("VOLATILITY_HIGH")
            score += 0.5
        if max(long_ev, short_ev) < self.config.min_positive_ev:
            reasons.append("NO_CLEAR_EDGE")
            score += 0.7
        return score, reasons

    def build_labels(self, symbol: str) -> list[dict[str, Any]]:
        rows = self._load_features(symbol)
        if len(rows) < max(self.config.horizons) + 20:
            return []

        labeled: list[dict[str, Any]] = []
        max_horizon = max(self.config.horizons)
        for idx in range(len(rows) - max_horizon):
            row = rows[idx]
            entry = float(row.get("close", 0.0) or 0.0)
            if entry <= 0:
                continue

            future_rows = rows[idx + 1 : idx + 1 + max_horizon]
            long_eval = self._simulate_direction("LONG", entry=entry, future_rows=future_rows)
            short_eval = self._simulate_direction("SHORT", entry=entry, future_rows=future_rows)
            no_trade_score, no_trade_reasons = self._no_trade_score(row, long_eval["expected_value"], short_eval["expected_value"])

            action = "NO_TRADE"
            reason = "No clear edge"
            if long_eval["expected_value"] > short_eval["expected_value"] and long_eval["expected_value"] > self.config.min_positive_ev and no_trade_score < 0.8:
                action = "LONG"
                reason = "LONG EV dominates"
            elif short_eval["expected_value"] > long_eval["expected_value"] and short_eval["expected_value"] > self.config.min_positive_ev and no_trade_score < 0.8:
                action = "SHORT"
                reason = "SHORT EV dominates"
            else:
                reason = "NO_TRADE: " + ",".join(no_trade_reasons or ["edge_below_threshold"])

            enriched = dict(row)
            enriched.update(
                {
                    "label": action,
                    "label_reason": reason,
                    "long_result": long_eval["result"],
                    "long_max_profit": float(long_eval["max_profit"]),
                    "long_max_drawdown": float(long_eval["max_drawdown"]),
                    "short_result": short_eval["result"],
                    "short_max_profit": float(short_eval["max_profit"]),
                    "short_max_drawdown": float(short_eval["max_drawdown"]),
                    "EV_LONG": float(long_eval["expected_value"]),
                    "EV_SHORT": float(short_eval["expected_value"]),
                    "NO_TRADE_SCORE": float(no_trade_score),
                }
            )
            labeled.append(enriched)

        self._save_labels(labeled)
        return labeled

    def load_labels(self, symbol: str) -> list[dict[str, Any]]:
        with sqlite3.connect(self._db_path) as conn:
            rows = conn.execute(
                "SELECT payload FROM futures_labels WHERE symbol = ? ORDER BY ts ASC",
                (symbol.upper(),),
            ).fetchall()
        payloads: list[dict[str, Any]] = []
        for row in rows:
            try:
                payloads.append(json.loads(str(row[0] or "{}")))
            except Exception:
                continue
        return payloads
