from dataclasses import dataclass


@dataclass
class RiskManager:
    max_daily_loss: float
    risk_per_trade_pct: float
    stop_loss_pct: float

    def can_trade(self, current_daily_pnl: float) -> bool:
        return current_daily_pnl > (-1.0 * self.max_daily_loss)

    def calculate_position_size(self, account_equity: float, entry_price: float) -> float:
        if account_equity <= 0 or entry_price <= 0:
            return 0.0

        risk_amount = account_equity * (self.risk_per_trade_pct / 100.0)
        price_risk = entry_price * (self.stop_loss_pct / 100.0)

        if price_risk <= 0:
            return 0.0

        qty = risk_amount / price_risk
        return round(max(qty, 0.0), 6)

    def stop_loss_price(self, entry_price: float, side: str) -> float:
        pct = self.stop_loss_pct / 100.0
        if side == "buy":
            return round(entry_price * (1.0 - pct), 2)
        return round(entry_price * (1.0 + pct), 2)
