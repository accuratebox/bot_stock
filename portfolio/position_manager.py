import json
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config import Settings


@dataclass(frozen=True)
class PositionSnapshot:
    symbol: str
    qty: float
    avg_entry_price: float
    current_price: float
    unrealized_pl: float
    unrealized_plpc: float
    state: str
    entry_time: str = ""
    duration_seconds: float = 0.0
    reason_buy: str = ""
    reason_sell: str = ""

    @property
    def pnl_per_share(self) -> float:
        if self.qty <= 0:
            return 0.0
        return self.unrealized_pl / self.qty


class TradeJournal:
    def __init__(self, journal_path: str | None = None) -> None:
        base_path = Path(journal_path) if journal_path else Path(__file__).resolve().parents[1] / "trade_history.jsonl"
        self.path = base_path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def record(self, payload: dict[str, Any]) -> None:
        payload = dict(payload)
        payload["recorded_at"] = self._now_iso()
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def load_records(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        if not self.path.exists():
            return records

        with self.path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return records

    def delete_trade(self, trade_id: str) -> int:
        trade_id = str(trade_id or "").strip()
        if not trade_id or not self.path.exists():
            return 0

        kept_lines: list[str] = []
        removed = 0
        with self.path.open("r", encoding="utf-8") as handle:
            for raw in handle:
                line = raw.rstrip("\n")
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    kept_lines.append(line)
                    continue
                if str(payload.get("trade_id", "")).strip() == trade_id:
                    removed += 1
                    continue
                kept_lines.append(line)

        self.path.write_text(("\n".join(kept_lines) + ("\n" if kept_lines else "")), encoding="utf-8")
        return removed

    def open_trades(self) -> dict[str, dict[str, Any]]:
        open_trades: dict[str, dict[str, Any]] = {}
        records = self.load_records()
        for record in records:
            trade_id = record.get("trade_id")
            if not trade_id:
                continue
            record_type = record.get("record_type")
            if record_type == "entry":
                open_trades[trade_id] = record
            elif record_type == "exit" and trade_id in open_trades:
                open_trades.pop(trade_id, None)
        return open_trades

    def get_open_entry_by_symbol(self, symbol: str) -> dict[str, Any] | None:
        symbol_key = self._symbol_key(symbol)
        candidates = [
            record
            for record in self.open_trades().values()
            if self._symbol_key(str(record.get("symbol", ""))) == symbol_key
        ]
        if not candidates:
            return None
        candidates.sort(key=lambda record: record.get("entry_time", ""))
        return candidates[-1]

    @staticmethod
    def _symbol_key(symbol: str) -> str:
        return str(symbol or "").upper().replace(" ", "").replace("/", "")

    def get_daily_realized_pnl(self) -> float:
        today = self._today_key()
        total = 0.0
        for record in self.load_records():
            if record.get("record_type") != "exit":
                continue
            if str(record.get("exit_date", "")) != today:
                continue
            total += float(record.get("realized_pnl", 0.0) or 0.0)
        return total

    def get_closed_winners_today(self) -> int:
        today = self._today_key()
        winners = 0
        for record in self.load_records():
            if record.get("record_type") != "exit":
                continue
            if str(record.get("exit_date", "")) != today:
                continue
            if float(record.get("realized_pnl", 0.0) or 0.0) > 0:
                winners += 1
        return winners

    @staticmethod
    def _today_key() -> str:
        return datetime.now(timezone.utc).date().isoformat()

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()


class PositionManager:
    def __init__(
        self,
        broker: Any,
        market_data: Any,
        order_manager: Any,
        logger: Any,
        settings: Settings,
        journal: TradeJournal | None = None,
    ) -> None:
        self.broker = broker
        self.market_data = market_data
        self.order_manager = order_manager
        self.logger = logger
        self.settings = settings
        self.journal = journal or TradeJournal()
        self.target_profit_per_share = float(settings.target_profit_per_share)

    def set_target_profit_per_share(self, value: float) -> None:
        if value <= 0:
            raise ValueError("El target de ganancia por accion debe ser mayor que cero")
        self.target_profit_per_share = float(value)

    def synchronize_open_positions(self) -> dict[str, int]:
        positions = self.get_open_positions()
        synced = 0
        already_linked = 0

        for position in positions:
            symbol = str(position.get("symbol", "")).upper()
            if not symbol:
                continue

            existing_entry = self.journal.get_open_entry_by_symbol(symbol)
            if existing_entry is not None:
                already_linked += 1
                continue

            qty = float(position.get("qty", 0.0) or 0.0)
            avg_entry_price = float(position.get("avg_entry_price", 0.0) or 0.0)
            current_price = float(self.market_data.get_last_price(symbol))
            unrealized_pl = (current_price - avg_entry_price) * qty

            self.journal.record(
                {
                    "record_type": "entry",
                    "trade_id": str(uuid.uuid4()),
                    "symbol": symbol,
                    "entry_time": self._now_iso(),
                    "entry_price": avg_entry_price,
                    "qty": qty,
                    "current_price": current_price,
                    "floating_pnl": unrealized_pl,
                    "state": "RECOVERED",
                    "reason_buy": "recovered_after_restart",
                    "reason_sell": "",
                    "duration_seconds": 0,
                    "order_type": "market",
                    "slippage_estimated": 0.0,
                    "spread_at_entry": 0.0,
                    "spread_pct_at_entry": 0.0,
                    "status": "OPEN",
                }
            )
            synced += 1

        if synced > 0:
            self.logger.info("Recuperacion de posiciones abierta(s): %s", synced)

        return {
            "open_positions": len(positions),
            "synced": synced,
            "already_linked": already_linked,
        }

    def get_open_positions(self) -> list[dict[str, Any]]:
        return self.broker.get_positions()

    def get_dashboard_snapshot(self) -> dict[str, Any]:
        positions = [self._build_snapshot(position) for position in self.get_open_positions()]
        pending_orders = self.broker.list_orders(status="open", limit=50)
        return {
            "positions": positions,
            "pending_orders": pending_orders,
            "open_positions": len(positions),
            "profit_positions": sum(1 for position in positions if position.state == "PROFIT"),
            "loss_positions": sum(1 for position in positions if position.state == "LOSS"),
            "hold_positions": sum(1 for position in positions if position.state == "HOLD"),
            "floating_pnl": sum(position.unrealized_pl for position in positions),
            "floating_pnl_per_share": sum(position.pnl_per_share for position in positions),
            "realized_today": self.journal.get_daily_realized_pnl(),
            "closed_winners_today": self.journal.get_closed_winners_today(),
            "monitored_symbols": self._get_monitored_symbols_count(),
            "minutes_to_close": self.minutes_to_close(),
            "mode": "PAPER" if "paper" in str(getattr(self.broker, "endpoint", "")).lower() else "LIVE",
        }

    def auto_manage_positions(self) -> list[dict[str, Any]]:
        actions: list[dict[str, Any]] = []
        actions.extend(self._finalize_filled_limit_exits())
        open_positions = self.get_open_positions()
        minutes_to_close = self.minutes_to_close()

        for position in open_positions:
            snapshot = self._build_snapshot(position)
            if snapshot.state == "LOSS":
                actions.append({"symbol": snapshot.symbol, "action": "HOLD", "reason": "position_in_loss"})
                continue

            exit_reason = self._should_auto_sell(snapshot, minutes_to_close)
            if not exit_reason:
                actions.append({"symbol": snapshot.symbol, "action": "HOLD", "reason": "profit_not_ready"})
                continue

            close_result = self._close_profitable_position(snapshot, exit_reason)
            actions.append(close_result)

        return actions

    def can_open_new_trade(self, symbol: str, ignore_close_window: bool = False) -> tuple[bool, str]:
        symbol = symbol.upper()
        is_crypto = self._is_crypto_symbol(symbol)
        open_positions = self.get_open_positions()
        hold_count = 0
        open_count = 0
        same_symbol_open = None

        for position in open_positions:
            snapshot = self._build_snapshot(position)
            open_count += 1
            if snapshot.state == "HOLD":
                hold_count += 1
            if snapshot.symbol == symbol:
                same_symbol_open = snapshot

        if open_count >= self.settings.max_open_positions:
            return False, f"Maximo de posiciones abiertas alcanzado: {open_count}/{self.settings.max_open_positions}"
        if hold_count >= self.settings.max_hold_positions:
            return False, f"Maximo de posiciones HOLD alcanzado: {hold_count}/{self.settings.max_hold_positions}"
        if (
            (not is_crypto)
            and (not ignore_close_window)
            and self.minutes_to_close() <= self.settings.stop_new_trades_minutes_before_close
        ):
            return False, "Demasiado cerca del cierre para abrir nuevas operaciones"
        if same_symbol_open is not None and same_symbol_open.state == "LOSS" and not self.settings.allow_averaging_down:
            return False, f"{symbol} esta en perdida y averaging down esta desactivado"
        if same_symbol_open is not None:
            return False, f"{symbol} ya tiene una posicion abierta"

        return True, "ok"

    def open_position(self, symbol: str, qty: float, reason: str, spread_pct: float = 0.0) -> dict[str, Any]:
        symbol = symbol.upper()
        can_open, reason_text = self.can_open_new_trade(symbol)
        if not can_open:
            raise ValueError(reason_text)

        current_price = self.market_data.get_last_price(symbol)
        quote = self.market_data.get_latest_quote(symbol)
        entry_tif = "gtc"
        if self._is_crypto_symbol(symbol):
            configured_tif = str(getattr(self.settings, "crypto_entry_time_in_force", "ioc") or "ioc").lower().strip()
            entry_tif = configured_tif or "ioc"
        order = self.order_manager.create_market_order(symbol=symbol, qty=qty, side="buy", time_in_force=entry_tif)

        resolved_order = self._wait_order_fill(order)
        order_status = str(resolved_order.get("status", "")).lower()
        if order_status != "filled":
            order_id = str(resolved_order.get("id", order.get("id", "")))
            filled_qty = float(resolved_order.get("filled_qty", 0.0) or 0.0)
            raise ValueError(
                (
                    f"Orden enviada pero no llenada todavia (status={order_status}, "
                    f"filled_qty={filled_qty}, order_id={order_id})."
                )
            )

        filled_price = float(
            resolved_order.get("filled_avg_price")
            or resolved_order.get("avg_entry_price")
            or order.get("filled_avg_price")
            or order.get("avg_entry_price")
            or current_price
        )
        trade_id = str(uuid.uuid4())
        slippage = abs(filled_price - current_price)
        self.journal.record(
            {
                "record_type": "entry",
                "trade_id": trade_id,
                "symbol": symbol,
                "entry_time": self._now_iso(),
                "entry_price": filled_price,
                "qty": qty,
                "current_price": current_price,
                "floating_pnl": 0.0,
                "state": "OPEN",
                "reason_buy": reason,
                "reason_sell": "",
                "duration_seconds": 0,
                "order_type": "market",
                "slippage_estimated": slippage,
                "spread_at_entry": quote.get("spread", 0.0),
                "spread_pct_at_entry": quote.get("spread_pct", spread_pct),
                "status": "OPEN",
            }
        )

        self.logger.info("Entrada registrada %s qty=%s entry=%s", symbol, qty, filled_price)
        return {
            "trade_id": trade_id,
            "order": resolved_order,
            "entry_price": filled_price,
            "current_price": current_price,
            "spread_pct": quote.get("spread_pct", spread_pct),
        }

    def _wait_order_fill(self, order: dict[str, Any], max_attempts: int = 8, sleep_seconds: float = 0.5) -> dict[str, Any]:
        order_id = str(order.get("id", "")).strip()
        if not order_id or not hasattr(self.broker, "get_order"):
            return order

        current = order
        for _ in range(max_attempts):
            status = str(current.get("status", "")).lower()
            if status in {"filled", "canceled", "rejected", "expired"}:
                return current
            time.sleep(sleep_seconds)
            try:
                current = self.broker.get_order(order_id)
            except Exception:
                break
        return current

    def manual_sell(self, symbol: str) -> dict[str, Any]:
        if not self.settings.allow_manual_sell:
            raise ValueError("La venta manual esta desactivada")

        symbol = symbol.upper()
        position = self._find_open_position(symbol)
        if position is None:
            raise ValueError(f"No hay posicion abierta para {symbol}")

        snapshot = self._build_snapshot(position)
        return self._close_position(snapshot, reason_sell="manual_sell", force=True)

    def _close_profitable_position(self, snapshot: PositionSnapshot, reason_sell: str) -> dict[str, Any]:
        return self._place_limit_exit(snapshot, reason_sell=reason_sell)

    def _place_limit_exit(self, snapshot: PositionSnapshot, reason_sell: str) -> dict[str, Any]:
        if snapshot.state == "LOSS" and self.settings.never_sell_at_loss:
            return {"symbol": snapshot.symbol, "action": "HOLD", "reason": "never_sell_at_loss"}

        existing_order = self._find_pending_sell_order(snapshot.symbol)
        if existing_order is not None:
            return {
                "symbol": snapshot.symbol,
                "action": "LIMIT_SELL_PENDING",
                "reason": reason_sell,
                "order_id": str(existing_order.get("id", "")),
                "limit_price": float(existing_order.get("limit_price", snapshot.current_price) or snapshot.current_price),
            }

        limit_price = self._suggest_limit_exit_price(
            current_price=snapshot.current_price,
            avg_entry_price=snapshot.avg_entry_price,
            target_profit_per_share=float(self.target_profit_per_share),
        )
        order = self.order_manager.create_limit_order(
            symbol=snapshot.symbol,
            qty=snapshot.qty,
            side="sell",
            limit_price=limit_price,
            time_in_force="gtc",
        )
        return {
            "symbol": snapshot.symbol,
            "action": "LIMIT_SELL_PLACED",
            "reason": reason_sell,
            "order_id": str(order.get("id", "")),
            "limit_price": float(order.get("limit_price", limit_price) or limit_price),
            "qty": snapshot.qty,
        }

    def _find_pending_sell_order(self, symbol: str) -> dict[str, Any] | None:
        target = self._symbol_key(symbol)
        pending_statuses = {
            "new",
            "accepted",
            "pending_new",
            "partially_filled",
            "accepted_for_bidding",
            "pending_replace",
            "stopped",
            "calculated",
        }
        try:
            orders = self.broker.list_orders(status="open", limit=200)
        except Exception:
            return None

        for order in orders:
            side = str(order.get("side", "")).lower().strip()
            status = str(order.get("status", "")).lower().strip()
            if side != "sell" or status not in pending_statuses:
                continue
            if self._symbol_key(str(order.get("symbol", ""))) != target:
                continue
            return order
        return None

    @staticmethod
    def _suggest_limit_exit_price(current_price: float, avg_entry_price: float, target_profit_per_share: float) -> float:
        # Never place an automatic sell limit below the configured target-profit threshold.
        current = float(current_price)
        min_target_price = float(avg_entry_price) + float(target_profit_per_share)
        limit_price = max(current, min_target_price)
        return round(limit_price, 6)

    def _finalize_filled_limit_exits(self) -> list[dict[str, Any]]:
        actions: list[dict[str, Any]] = []
        open_positions = self.get_open_positions()
        open_symbols = {self._symbol_key(str(position.get("symbol", ""))) for position in open_positions}

        open_trades = list(self.journal.open_trades().values())
        if not open_trades:
            return actions

        try:
            orders = self.broker.list_orders(status="all", limit=200)
        except Exception:
            return actions

        for entry in open_trades:
            symbol = str(entry.get("symbol", "")).upper()
            symbol_key = self._symbol_key(symbol)
            if not symbol or symbol_key in open_symbols:
                continue

            trade_id = str(entry.get("trade_id", "")).strip()
            if not trade_id:
                continue

            matched_order = self._latest_filled_sell_order_for_symbol(symbol=symbol, orders=orders)
            if matched_order is None:
                continue

            qty = float(matched_order.get("filled_qty", entry.get("qty", 0.0)) or 0.0)
            entry_price = float(entry.get("entry_price", 0.0) or 0.0)
            exit_price = float(matched_order.get("filled_avg_price", 0.0) or 0.0)
            if qty <= 0 or exit_price <= 0:
                continue

            realized_pnl = (exit_price - entry_price) * qty
            duration_seconds = self._duration_seconds(str(entry.get("entry_time", "")))
            exit_time = str(matched_order.get("filled_at", "") or matched_order.get("updated_at", "") or self._now_iso())

            self.journal.record(
                {
                    "record_type": "exit",
                    "trade_id": trade_id,
                    "symbol": symbol,
                    "exit_time": exit_time,
                    "exit_date": self._today_key(),
                    "exit_price": exit_price,
                    "trigger_price": float(matched_order.get("limit_price", exit_price) or exit_price),
                    "close_order_id": str(matched_order.get("id", "")),
                    "qty": qty,
                    "current_price": exit_price,
                    "floating_pnl": 0.0,
                    "state": "PROFIT" if realized_pnl > 0 else "LOSS" if realized_pnl < 0 else "EVEN",
                    "reason_buy": str(entry.get("reason_buy", "")),
                    "reason_sell": "limit_exit_filled",
                    "duration_seconds": duration_seconds,
                    "order_type": "limit",
                    "slippage_estimated": abs(exit_price - float(matched_order.get("limit_price", exit_price) or exit_price)),
                    "spread_at_entry": 0.0,
                    "realized_pnl": realized_pnl,
                    "status": "CLOSED",
                }
            )
            actions.append(
                {
                    "symbol": symbol,
                    "action": "SELL",
                    "reason": "limit_exit_filled",
                    "trigger_price": float(matched_order.get("limit_price", exit_price) or exit_price),
                    "exit_price": exit_price,
                    "realized_pnl": realized_pnl,
                    "close_order_id": str(matched_order.get("id", "")),
                }
            )
            self.logger.info("Salida limit registrada %s pnl=%s", symbol, realized_pnl)

        return actions

    def _latest_filled_sell_order_for_symbol(self, symbol: str, orders: list[dict[str, Any]]) -> dict[str, Any] | None:
        symbol_key = self._symbol_key(symbol)
        candidates = [
            order
            for order in orders
            if str(order.get("side", "")).lower().strip() == "sell"
            and str(order.get("status", "")).lower().strip() == "filled"
            and self._symbol_key(str(order.get("symbol", ""))) == symbol_key
        ]
        if not candidates:
            return None

        candidates.sort(key=lambda order: str(order.get("filled_at", "") or order.get("updated_at", "")), reverse=True)
        return candidates[0]

    def _close_position(self, snapshot: PositionSnapshot, reason_sell: str, force: bool) -> dict[str, Any]:
        if not force and snapshot.state == "LOSS" and self.settings.never_sell_at_loss:
            return {"symbol": snapshot.symbol, "action": "HOLD", "reason": "never_sell_at_loss"}

        trigger_price = float(snapshot.current_price)
        close_result = self.broker.close_position(snapshot.symbol)
        close_order_id = str(close_result.get("id", "") or "")
        exit_price = self._resolve_exit_price(close_result=close_result, close_order_id=close_order_id, fallback_price=trigger_price)
        realized_pnl = (exit_price - snapshot.avg_entry_price) * snapshot.qty
        duration_seconds = self._duration_seconds(snapshot.entry_time)
        trade_id = self._trade_id_for_symbol(snapshot.symbol) or str(uuid.uuid4())
        self.journal.record(
            {
                "record_type": "exit",
                "trade_id": trade_id,
                "symbol": snapshot.symbol,
                "exit_time": self._now_iso(),
                "exit_date": self._today_key(),
                "exit_price": exit_price,
                "trigger_price": trigger_price,
                "close_order_id": close_order_id,
                "qty": snapshot.qty,
                "current_price": snapshot.current_price,
                "floating_pnl": snapshot.unrealized_pl,
                "state": snapshot.state,
                "reason_buy": snapshot.reason_buy,
                "reason_sell": reason_sell,
                "duration_seconds": duration_seconds,
                "order_type": "market",
                "slippage_estimated": abs(exit_price - snapshot.current_price),
                "spread_at_entry": 0.0,
                "realized_pnl": realized_pnl,
                "status": "CLOSED",
            }
        )
        self.logger.info("Salida registrada %s pnl=%s", snapshot.symbol, realized_pnl)
        return {
            "symbol": snapshot.symbol,
            "action": "SELL",
            "reason": reason_sell,
            "trigger_price": trigger_price,
            "exit_price": exit_price,
            "realized_pnl": realized_pnl,
            "duration_seconds": duration_seconds,
            "close_result": close_result,
            "close_order_id": close_order_id,
        }

    def _resolve_exit_price(self, close_result: dict[str, Any], close_order_id: str, fallback_price: float) -> float:
        raw_price = close_result.get("filled_avg_price") or close_result.get("avg_price")
        if raw_price is not None:
            try:
                return float(raw_price)
            except (TypeError, ValueError):
                pass

        if close_order_id:
            for _ in range(8):
                try:
                    order = self.broker.get_order(close_order_id)
                except Exception:
                    break

                price = order.get("filled_avg_price") or order.get("filled_price") or order.get("avg_price")
                if price is not None:
                    try:
                        return float(price)
                    except (TypeError, ValueError):
                        pass

                status = str(order.get("status", "")).lower()
                if status in {"filled", "canceled", "rejected", "expired"}:
                    break
                time.sleep(0.35)

        return float(fallback_price)

    def _should_auto_sell(self, snapshot: PositionSnapshot, minutes_to_close: int) -> str | None:
        min_hold_seconds = int(getattr(self.settings, "min_hold_seconds_before_auto_sell", 0) or 0)
        if min_hold_seconds > 0 and snapshot.duration_seconds < min_hold_seconds:
            return None

        if snapshot.unrealized_pl <= 0 and self.settings.never_sell_at_loss:
            return None

        if snapshot.pnl_per_share >= self.target_profit_per_share:
            return "target_profit_per_share"

        if (
            (not self._is_crypto_symbol(snapshot.symbol))
            and minutes_to_close <= self.settings.stop_new_trades_minutes_before_close
            and snapshot.unrealized_pl > 0
        ):
            return "near_close_positive"

        candles = self.market_data.get_stock_bars(snapshot.symbol, interval="1m", limit=20)
        closes = [float(c.get("close", 0.0) or 0.0) for c in candles]
        volumes = [float(c.get("volume", 0.0) or 0.0) for c in candles]
        if len(closes) < 5:
            return None

        recent_high = max(closes)
        average_volume = sum(volumes[:-1]) / max(len(volumes[:-1]), 1)
        last_volume = volumes[-1]
        short_ma = sum(closes[-5:]) / 5.0
        current_price = snapshot.current_price

        if current_price >= recent_high * 0.998 and snapshot.unrealized_pl > 0:
            return "near_resistance"
        if last_volume < (average_volume * 0.7) and snapshot.unrealized_pl > 0:
            return "volume_weakening"
        if current_price < short_ma and snapshot.unrealized_pl > 0:
            return "momentum_weakening"

        return None

    def _build_snapshot(self, position: dict[str, Any]) -> PositionSnapshot:
        symbol = str(position.get("symbol", "")).upper()
        qty = float(position.get("qty", 0.0) or 0.0)
        avg_entry_price = float(position.get("avg_entry_price", 0.0) or 0.0)
        current_price = float(self.market_data.get_last_price(symbol))
        unrealized_pl = (current_price - avg_entry_price) * qty
        unrealized_plpc = (unrealized_pl / (avg_entry_price * qty)) if avg_entry_price > 0 and qty > 0 else 0.0
        state = "HOLD" if unrealized_pl < 0 else "PROFIT" if unrealized_pl > 0 else "EVEN"
        entry_record = self.journal.get_open_entry_by_symbol(symbol) or {}
        entry_time = str(entry_record.get("entry_time", ""))
        reason_buy = str(entry_record.get("reason_buy", ""))
        duration_seconds = self._duration_seconds(entry_time) if entry_time else 0.0
        return PositionSnapshot(
            symbol=symbol,
            qty=qty,
            avg_entry_price=avg_entry_price,
            current_price=current_price,
            unrealized_pl=unrealized_pl,
            unrealized_plpc=unrealized_plpc,
            state=state,
            entry_time=entry_time,
            duration_seconds=duration_seconds,
            reason_buy=reason_buy,
        )

    def _find_open_position(self, symbol: str) -> dict[str, Any] | None:
        for position in self.get_open_positions():
            if str(position.get("symbol", "")).upper() == symbol.upper():
                return position
        return None

    def _trade_id_for_symbol(self, symbol: str) -> str:
        entry = self.journal.get_open_entry_by_symbol(symbol)
        return str(entry.get("trade_id", "")) if entry else ""

    def _get_monitored_symbols_count(self) -> int:
        return len({str(position.get("symbol", "")).upper() for position in self.get_open_positions()})

    def minutes_to_close(self) -> int:
        clock = self.broker.get_clock()
        if not clock.get("is_open", False):
            return 0

        next_close = self._parse_datetime(clock.get("next_close"))
        now = datetime.now(timezone.utc)
        return max(int((next_close - now).total_seconds() // 60), 0) if next_close else 0

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        if not value:
            return None
        if isinstance(value, datetime):
            return value
        text = str(value).replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @classmethod
    def _now_iso(cls) -> str:
        return datetime.now(timezone.utc).isoformat()

    @classmethod
    def _today_key(cls) -> str:
        return datetime.now(timezone.utc).date().isoformat()

    @classmethod
    def _duration_seconds(cls, entry_time: str) -> float:
        if not entry_time:
            return 0.0
        parsed = cls._parse_datetime(entry_time)
        if parsed is None:
            return 0.0
        return max((datetime.now(timezone.utc) - parsed).total_seconds(), 0.0)

    @staticmethod
    def _is_crypto_symbol(symbol: str) -> bool:
        normalized = str(symbol or "").upper().replace(" ", "")
        return "/" in normalized or normalized.endswith("USD")

    @staticmethod
    def _symbol_key(symbol: str) -> str:
        return str(symbol or "").upper().replace(" ", "").replace("/", "")