import json
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config import Settings


@dataclass
class ScheduledTrade:
    id: str
    symbol: str
    capital: float
    target_profit_per_share: float
    status: str
    created_at: str
    trigger_on_market_open: bool = True
    note: str = ""
    last_error: str = ""
    executed_at: str = ""
    trade_id: str = ""


class MarketOpenScheduler:
    def __init__(
        self,
        broker: Any,
        market_data: Any,
        strategy: Any,
        position_manager: Any,
        risk_manager: Any,
        logger: Any,
        settings: Settings,
        storage_path: str | None = None,
    ) -> None:
        self.broker = broker
        self.market_data = market_data
        self.strategy = strategy
        self.position_manager = position_manager
        self.risk_manager = risk_manager
        self.logger = logger
        self.settings = settings
        base_path = Path(storage_path) if storage_path else Path(__file__).resolve().parents[1] / "scheduled_trades.json"
        self.path = base_path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def schedule_trade(
        self,
        symbol: str,
        capital: float,
        target_profit_per_share: float,
        note: str = "",
    ) -> ScheduledTrade:
        symbol = symbol.upper().strip()
        if not symbol:
            raise ValueError("El simbolo es obligatorio")
        if capital <= 0:
            raise ValueError("El capital programado debe ser mayor que cero")
        if target_profit_per_share <= 0:
            raise ValueError("El target de ganancia por accion debe ser mayor que cero")

        schedules = self.load_schedules()
        existing = next((item for item in schedules if item.symbol == symbol and item.status == "pending"), None)
        if existing is not None:
            existing.capital = capital
            existing.target_profit_per_share = target_profit_per_share
            existing.note = note
            existing.last_error = ""
            self._save_schedules(schedules)
            return existing

        tradable_symbols = {
            str(asset.get("symbol", "")).upper()
            for asset in self.broker.list_tradable_assets()
            if str(asset.get("symbol", "")).strip()
        }
        if symbol not in tradable_symbols:
            raise ValueError(f"Ticker no valido o no tradable: {symbol}")

        schedule = ScheduledTrade(
            id=str(uuid.uuid4()),
            symbol=symbol,
            capital=capital,
            target_profit_per_share=target_profit_per_share,
            status="pending",
            created_at=self._now_iso(),
            note=note,
        )
        schedules.append(schedule)
        self._save_schedules(schedules)
        self.logger.info("Stock programado para apertura: %s", symbol)
        return schedule

    def cancel_schedule(self, schedule_id: str) -> bool:
        schedules = self.load_schedules()
        changed = False
        for schedule in schedules:
            if schedule.id == schedule_id and schedule.status == "pending":
                schedule.status = "cancelled"
                schedule.last_error = "cancelled_by_user"
                changed = True
        if changed:
            self._save_schedules(schedules)
        return changed

    def delete_schedule(self, schedule_id: str) -> bool:
        schedules = self.load_schedules()
        kept = [schedule for schedule in schedules if schedule.id != schedule_id]
        if len(kept) == len(schedules):
            return False
        self._save_schedules(kept)
        return True

    def list_schedules(self) -> list[ScheduledTrade]:
        return self.load_schedules()

    def get_pending_schedules(self) -> list[ScheduledTrade]:
        return [schedule for schedule in self.load_schedules() if schedule.status == "pending"]

    def process_pending_schedules(self) -> list[dict[str, Any]]:
        actions: list[dict[str, Any]] = []
        clock = self.broker.get_clock()
        if not clock.get("is_open", False):
            return actions

        schedules = self.load_schedules()
        updated = False
        for schedule in schedules:
            if schedule.status != "pending" or not schedule.trigger_on_market_open:
                continue

            can_open, reason_text = self.position_manager.can_open_new_trade(
                schedule.symbol,
                ignore_close_window=True,
            )
            if not can_open:
                schedule.last_error = reason_text
                actions.append(
                    {
                        "symbol": schedule.symbol,
                        "action": "WAIT",
                        "reason": reason_text,
                        "schedule_id": schedule.id,
                    }
                )
                continue

            try:
                account = self.broker.get_account()
                cash_available = float(account.get("cash", 0.0) or 0.0)
                if schedule.capital > cash_available:
                    schedule.last_error = (
                        f"Capital programado ({schedule.capital:.2f}) excede cash disponible ({cash_available:.2f})"
                    )
                    actions.append(
                        {
                            "symbol": schedule.symbol,
                            "action": "WAIT",
                            "reason": schedule.last_error,
                            "schedule_id": schedule.id,
                        }
                    )
                    continue

                candles_1m = self.market_data.get_candles(symbol=schedule.symbol, interval="1m", limit=50)
                candles_5m = self.market_data.get_candles(symbol=schedule.symbol, interval="5m", limit=50)
                current_price = self.market_data.get_last_price(schedule.symbol)
                quote = self.market_data.get_latest_quote(schedule.symbol)
                vwap = self.market_data.calculate_vwap(candles_1m)
                signal = self.strategy.generate_signal(
                    symbol=schedule.symbol,
                    candles_1m=candles_1m,
                    candles_5m=candles_5m,
                    current_price=current_price,
                    spread_pct=float(quote.get("spread_pct", 0.0) or 0.0),
                    vwap=vwap,
                    asset_type=self._asset_type_for_symbol(schedule.symbol),
                )

                if signal.action != "buy":
                    schedule.last_error = signal.reason
                    actions.append(
                        {
                            "symbol": schedule.symbol,
                            "action": "HOLD",
                            "reason": signal.reason,
                            "details": signal.details,
                            "schedule_id": schedule.id,
                        }
                    )
                    continue

                trade_capital = float(schedule.capital)
                qty = round(trade_capital / current_price, 4)
                if qty <= 0:
                    schedule.last_error = "Cantidad calculada invalida"
                    actions.append(
                        {
                            "symbol": schedule.symbol,
                            "action": "WAIT",
                            "reason": schedule.last_error,
                            "schedule_id": schedule.id,
                        }
                    )
                    continue

                try:
                    current_daily_pnl = float(self.position_manager.journal.get_daily_realized_pnl())
                except Exception:
                    current_daily_pnl = 0.0

                if not self.risk_manager.can_trade(current_daily_pnl=current_daily_pnl):
                    schedule.last_error = "Bloqueado por limite de perdida diaria"
                    actions.append(
                        {
                            "symbol": schedule.symbol,
                            "action": "WAIT",
                            "reason": schedule.last_error,
                            "schedule_id": schedule.id,
                        }
                    )
                    continue

                result = self.position_manager.open_position(
                    symbol=schedule.symbol,
                    qty=qty,
                    reason=f"scheduled_at_open: {signal.reason}",
                    spread_pct=float(quote.get("spread_pct", 0.0) or 0.0),
                    target_profit_per_share=float(schedule.target_profit_per_share),
                )
                schedule.status = "executed"
                schedule.executed_at = self._now_iso()
                schedule.trade_id = str(result.get("trade_id", ""))
                schedule.last_error = ""
                actions.append(
                    {
                        "symbol": schedule.symbol,
                        "action": "BUY",
                        "reason": signal.reason,
                        "schedule_id": schedule.id,
                        "trade_id": schedule.trade_id,
                        "entry_price": result.get("entry_price"),
                    }
                )
                updated = True
            except Exception as ex:
                schedule.last_error = str(ex)
                actions.append(
                    {
                        "symbol": schedule.symbol,
                        "action": "ERROR",
                        "reason": str(ex),
                        "schedule_id": schedule.id,
                    }
                )

        if updated:
            self._save_schedules(schedules)
        else:
            self._save_schedules(schedules)
        return actions

    def load_schedules(self) -> list[ScheduledTrade]:
        if not self.path.exists():
            return []

        schedules: list[ScheduledTrade] = []
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return []

        for item in data:
            try:
                schedules.append(ScheduledTrade(**item))
            except TypeError:
                continue
        return schedules

    def _save_schedules(self, schedules: list[ScheduledTrade]) -> None:
        payload = [asdict(schedule) for schedule in schedules]
        self.path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _asset_type_for_symbol(symbol: str) -> str:
        normalized = symbol.upper().replace(" ", "")
        return "crypto" if "/" in normalized or normalized.endswith("USD") else "stock"
