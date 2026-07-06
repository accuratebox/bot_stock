from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class FuturesRiskLimits:
    max_daily_loss: float = 0.03
    max_trades_per_hour: int = 30
    max_consecutive_losses: int = 6
    max_expected_drawdown: float = 0.006
    max_spread_pct: float = 0.002
    max_bad_funding_rate: float = 0.0015
    max_hold_minutes: int = 15


class FuturesRiskManager:
    def __init__(self, limits: FuturesRiskLimits = FuturesRiskLimits()) -> None:
        self.limits = limits

    def can_open_trade(self, context: dict[str, Any]) -> tuple[bool, str]:
        if not bool(context.get("websocket_connected", False)):
            return False, "WEBSOCKET_DISCONNECTED"
        if not bool(context.get("data_fresh", False)):
            return False, "DATA_STALE"
        if bool(context.get("api_error", False)):
            return False, "API_ERROR"
        if bool(context.get("rate_limited", False)):
            return False, "RATE_LIMIT"
        if bool(context.get("incomplete_data", False)):
            return False, "INCOMPLETE_DATA"
        if bool(context.get("cannot_place_stop", False)):
            return False, "STOP_REQUIRED"
        if bool(context.get("cannot_place_take_profit", False)):
            return False, "TAKE_PROFIT_REQUIRED"

        spread_pct = float(context.get("spread_pct", 0.0) or 0.0)
        if spread_pct > self.limits.max_spread_pct:
            return False, "SPREAD_TOO_HIGH"

        funding_rate = abs(float(context.get("funding_rate", 0.0) or 0.0))
        if funding_rate > self.limits.max_bad_funding_rate:
            return False, "FUNDING_UNFAVORABLE"

        expected_drawdown = abs(float(context.get("expected_drawdown", 0.0) or 0.0))
        if expected_drawdown > self.limits.max_expected_drawdown:
            return False, "DRAWDOWN_TOO_HIGH"

        if int(context.get("consecutive_losses", 0) or 0) >= self.limits.max_consecutive_losses:
            return False, "CONSECUTIVE_LOSSES_LIMIT"
        if int(context.get("trades_last_hour", 0) or 0) >= self.limits.max_trades_per_hour:
            return False, "TRADE_RATE_LIMIT"
        if abs(float(context.get("daily_pnl", 0.0) or 0.0)) >= self.limits.max_daily_loss:
            return False, "DAILY_LOSS_LIMIT"

        return True, "OK"

    def compute_position_size(
        self,
        account_equity: float,
        risk_per_trade: float,
        entry_price: float,
        stop_loss_price: float,
        leverage: int,
    ) -> float:
        equity = max(float(account_equity or 0.0), 0.0)
        risk_fraction = max(min(float(risk_per_trade or 0.0), 1.0), 0.0)
        entry = max(float(entry_price or 0.0), 1e-9)
        stop = max(float(stop_loss_price or 0.0), 1e-9)
        lev = max(int(leverage or 1), 1)

        stop_distance = abs(entry - stop) / entry
        if stop_distance <= 1e-9:
            return 0.0

        capital_at_risk = equity * risk_fraction
        notional = capital_at_risk / stop_distance
        leveraged_notional = notional * lev
        return max(leveraged_notional / entry, 0.0)

    def enforce_exit_requirements(self, prediction: dict[str, Any]) -> tuple[bool, str]:
        action = str(prediction.get("action", "NO_TRADE") or "NO_TRADE").upper().strip()
        if action == "NO_TRADE":
            return False, "MODEL_NO_TRADE"
        if float(prediction.get("suggested_stop_loss", 0.0) or 0.0) <= 0:
            return False, "STOP_REQUIRED"
        if float(prediction.get("suggested_take_profit", 0.0) or 0.0) <= 0:
            return False, "TAKE_PROFIT_REQUIRED"
        if int(prediction.get("max_hold_minutes", 0) or 0) <= 0:
            return False, "MAX_HOLD_REQUIRED"
        return True, "OK"
