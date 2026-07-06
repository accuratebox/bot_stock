from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class FuturesBacktestConfig:
    min_profit_factor_for_approval: float = 1.2
    max_drawdown_abs_limit: float = 0.08
    min_expectancy_for_approval: float = 0.0
    max_trade_rate_per_sample: float = 0.5


class FuturesBacktester:
    def __init__(self, config: FuturesBacktestConfig = FuturesBacktestConfig()) -> None:
        self.config = config

    @staticmethod
    def _trade_pnl(row: dict[str, Any], action: str) -> float:
        if action == "LONG":
            return float(row.get("EV_LONG", 0.0) or 0.0)
        if action == "SHORT":
            return float(row.get("EV_SHORT", 0.0) or 0.0)
        return 0.0

    def evaluate(self, rows: list[dict[str, Any]], predicted_actions: list[str]) -> dict[str, Any]:
        if not rows or not predicted_actions or len(rows) != len(predicted_actions):
            return {
                "win_rate": 0.0,
                "profit_factor": 0.0,
                "max_drawdown": 0.0,
                "average_win": 0.0,
                "average_loss": 0.0,
                "expectancy": 0.0,
                "number_of_trades": 0,
                "no_trade_pct": 100.0,
                "approved": False,
            }

        pnls: list[float] = []
        equity = 0.0
        peak = 0.0
        max_drawdown = 0.0

        no_trade_count = 0
        for row, action in zip(rows, predicted_actions):
            action_norm = str(action or "NO_TRADE").upper().strip()
            if action_norm not in {"LONG", "SHORT"}:
                no_trade_count += 1
                continue
            pnl = self._trade_pnl(row, action_norm)
            pnls.append(pnl)
            equity += pnl
            peak = max(peak, equity)
            max_drawdown = min(max_drawdown, equity - peak)

        wins = [value for value in pnls if value > 0]
        losses = [value for value in pnls if value < 0]
        gross_profit = sum(wins)
        gross_loss = abs(sum(losses))
        trade_count = len(pnls)
        sample_count = len(rows)

        win_rate = (len(wins) / trade_count) if trade_count > 0 else 0.0
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (gross_profit if gross_profit > 0 else 0.0)
        average_win = (sum(wins) / len(wins)) if wins else 0.0
        average_loss = (sum(losses) / len(losses)) if losses else 0.0
        expectancy = (sum(pnls) / trade_count) if trade_count > 0 else 0.0
        no_trade_pct = (no_trade_count / sample_count) * 100.0 if sample_count > 0 else 100.0

        approved = (
            profit_factor > self.config.min_profit_factor_for_approval
            and abs(max_drawdown) <= self.config.max_drawdown_abs_limit
            and expectancy > self.config.min_expectancy_for_approval
            and (trade_count / max(sample_count, 1)) <= self.config.max_trade_rate_per_sample
        )

        return {
            "win_rate": win_rate,
            "profit_factor": profit_factor,
            "max_drawdown": abs(max_drawdown),
            "average_win": average_win,
            "average_loss": average_loss,
            "expectancy": expectancy,
            "number_of_trades": trade_count,
            "no_trade_pct": no_trade_pct,
            "approved": approved,
        }

    def evaluate_with_predictions(self, rows: list[dict[str, Any]], predictor: Any) -> dict[str, Any]:
        actions: list[str] = []
        for row in rows:
            pred = predictor.predict(row)
            actions.append(str(pred.get("action", "NO_TRADE")))
        return self.evaluate(rows, actions)
