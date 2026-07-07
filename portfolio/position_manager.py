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
    side: str
    leverage: float
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
        self.target_mode = self._target_mode()
        self.target_total_usd = float(getattr(settings, "target_total_usd", settings.target_profit_per_share))
        self.target_price_delta = float(getattr(settings, "target_price_delta", 0.0) or 0.0)
        self.target_profit_per_share = float(settings.target_profit_per_share)

    def _target_mode(self) -> str:
        mode = str(getattr(self.settings, "target_mode", "TOTAL_USD") or "TOTAL_USD").strip().upper()
        return mode if mode in {"TOTAL_USD", "PERCENT", "PRICE_DELTA"} else "TOTAL_USD"

    @staticmethod
    def _exit_side_for_position(side: str) -> str:
        return "buy" if str(side or "").lower().strip() == "short" else "sell"

    def _fees_and_slippage_buffer_usd(self) -> float:
        fees_buffer = max(float(getattr(self.settings, "ai_fees_buffer", 0.0) or 0.0), 0.0)
        slippage_buffer = max(float(getattr(self.settings, "ai_slippage_buffer", 0.0) or 0.0), 0.0)
        return fees_buffer + slippage_buffer

    def _build_target_plan(
        self,
        *,
        side: str,
        entry_price: float,
        current_price: float,
        qty: float,
        target_mode: str | None = None,
        target_total_usd: float | None = None,
        target_price_delta: float | None = None,
    ) -> dict[str, Any]:
        side_key = str(side or "long").lower().strip()
        if side_key not in {"long", "short"}:
            side_key = "long"

        entry_price = float(entry_price or 0.0)
        current_price = float(current_price or 0.0)
        qty = float(qty or 0.0)
        mode = str(target_mode or self._target_mode() or "TOTAL_USD").strip().upper()
        if mode not in {"TOTAL_USD", "PERCENT", "PRICE_DELTA"}:
            mode = "TOTAL_USD"

        configured_total_usd = max(float(target_total_usd or 0.0), 0.0)
        configured_price_delta = max(float(target_price_delta or 0.0), 0.0)
        if mode == "PRICE_DELTA":
            target_price_delta_value = configured_price_delta or configured_total_usd or float(self.target_price_delta) or float(self.target_profit_per_share)
            expected_gross_profit_usd = max(target_price_delta_value, 0.0) * qty
        elif mode == "PERCENT":
            percent_value = configured_total_usd or float(self.target_total_usd) or float(self.target_profit_per_share)
            expected_gross_profit_usd = max(entry_price * qty * (percent_value / 100.0), 0.0)
            target_price_delta_value = expected_gross_profit_usd / qty if qty > 0 else 0.0
        else:
            expected_gross_profit_usd = configured_total_usd or float(self.target_total_usd) or float(self.target_profit_per_share)
            target_price_delta_value = expected_gross_profit_usd / qty if qty > 0 else 0.0

        total_buffer_usd = self._fees_and_slippage_buffer_usd()
        expected_net_profit_usd = expected_gross_profit_usd - total_buffer_usd
        target_price_delta_with_buffer = target_price_delta_value + (total_buffer_usd / qty if qty > 0 else 0.0)

        if side_key == "short":
            target_price = entry_price - target_price_delta_with_buffer
        else:
            target_price = entry_price + target_price_delta_with_buffer

        take_profit_available = False
        if side_key == "short" and current_price > 0:
            take_profit_available = current_price <= target_price
        elif side_key == "long" and current_price > 0:
            take_profit_available = current_price >= target_price

        warnings: list[str] = []
        if qty <= 0 or entry_price <= 0:
            return {
                "valid": False,
                "status": "INVALID_INPUT",
                "warnings": ["qty_or_entry_invalid"],
            }
        if target_price <= 0:
            return {
                "valid": False,
                "status": "TARGET_PRICE_NON_POSITIVE",
                "warnings": ["target_price_non_positive"],
            }
        if side_key == "short" and target_price >= entry_price:
            return {
                "valid": False,
                "status": "TARGET_DIRECTION_INVALID",
                "warnings": ["short_target_above_entry"],
            }
        if side_key == "long" and target_price <= entry_price:
            return {
                "valid": False,
                "status": "TARGET_DIRECTION_INVALID",
                "warnings": ["long_target_below_entry"],
            }
        if entry_price > 0 and target_price_delta_value > (entry_price * 0.10):
            return {
                "valid": False,
                "status": "TARGET_TOO_FAR",
                "warnings": ["target_delta_gt_10pct"],
            }
        if expected_net_profit_usd <= 0:
            warnings.append("target_does_not_cover_fees_and_slippage")

        return {
            "valid": True,
            "status": "TAKE_PROFIT_AVAILABLE" if take_profit_available else "TARGET_READY",
            "side": side_key,
            "target_mode": mode,
            "target_total_usd": expected_gross_profit_usd,
            "target_price_delta": target_price_delta_with_buffer,
            "target_price": target_price,
            "expected_gross_profit_usd": expected_gross_profit_usd,
            "expected_net_profit_usd": expected_net_profit_usd,
            "take_profit_available": take_profit_available,
            "warnings": warnings,
        }

    def get_target_total_usd_for_symbol(self, symbol: str) -> float:
        entry = self.journal.get_open_entry_by_symbol(symbol)
        if entry is not None:
            for key in ("target_total_usd", "target_profit_total", "target_profit_per_share"):
                try:
                    value = float(entry.get(key, 0.0) or 0.0)
                    if value > 0:
                        return value
                except (TypeError, ValueError):
                    continue
        return float(self.target_total_usd)

    def get_target_plan_for_symbol(self, symbol: str, side: str, entry_price: float, current_price: float, qty: float) -> dict[str, Any]:
        entry = self.journal.get_open_entry_by_symbol(symbol) or {}
        return self._build_target_plan(
            side=side,
            entry_price=entry_price,
            current_price=current_price,
            qty=qty,
            target_mode=str(entry.get("target_mode", "") or self._target_mode()),
            target_total_usd=float(entry.get("target_total_usd", entry.get("target_profit_total", 0.0)) or self.get_target_total_usd_for_symbol(symbol)),
            target_price_delta=float(entry.get("target_price_delta", 0.0) or 0.0),
        )

    def set_target_profit_per_share(self, value: float) -> None:
        if value <= 0:
            raise ValueError("El target de ganancia por accion debe ser mayor que cero")
        self.target_profit_per_share = float(value)

    def synchronize_open_positions(self) -> dict[str, int]:
        positions = self.get_open_positions()
        synced = 0
        already_linked = 0
        repaired_limits = 0

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
                    "order_type": "recovered",
                    "slippage_estimated": 0.0,
                    "spread_at_entry": 0.0,
                    "spread_pct_at_entry": 0.0,
                    "status": "OPEN",
                }
            )
            synced += 1

        repaired_actions = self._ensure_target_limits_for_open_positions(positions)
        repaired_limits = sum(
            1
            for action in repaired_actions
            if str(action.get("action", "")) in {"LIMIT_EXIT_PLACED", "LIMIT_EXIT_PENDING"}
        )

        if synced > 0:
            self.logger.info("Recuperacion de posiciones abierta(s): %s", synced)
        if repaired_limits > 0:
            self.logger.info("Targets limit reparados al iniciar: %s", repaired_limits)

        return {
            "open_positions": len(positions),
            "synced": synced,
            "already_linked": already_linked,
            "repaired_limits": repaired_limits,
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
        actions.extend(self._ensure_target_limits_for_open_positions(open_positions))
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

    def _ensure_target_limits_for_open_positions(self, open_positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
        actions: list[dict[str, Any]] = []
        for position in open_positions:
            snapshot = self._build_snapshot(position)
            if snapshot.qty <= 0:
                continue

            existing_order = self._find_pending_exit_order(snapshot.symbol, side=snapshot.side)
            if existing_order is not None:
                continue

            try:
                result = self._place_immediate_target_exit(
                    symbol=snapshot.symbol,
                    qty=snapshot.qty,
                    avg_entry_price=snapshot.avg_entry_price,
                    current_price=snapshot.current_price,
                    side=snapshot.side,
                    reason_sell="reconcile_missing_target_limit",
                )
                action = str(result.get("action", "") or "")
                if action in {"LIMIT_EXIT_PLACED", "LIMIT_EXIT_PENDING"}:
                    actions.append(result)
            except Exception as ex:
                if "Notional insuficiente" in str(ex):
                    self.logger.info(
                        "Reconcile limit omitido para %s por notional minimo: %s",
                        snapshot.symbol,
                        ex,
                    )
                else:
                    self.logger.warning(
                        "No se pudo reconciliar salida limit faltante para %s: %s",
                        snapshot.symbol,
                        ex,
                    )
        return actions

    def can_open_new_trade(self, symbol: str, ignore_close_window: bool = False) -> tuple[bool, str]:
        symbol = symbol.upper()
        is_crypto = self._is_crypto_symbol(symbol)

        if self._has_pending_buy_order(symbol):
            return False, f"{symbol} tiene una compra pendiente en broker"

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

    def open_position(
        self,
        symbol: str,
        qty: float,
        reason: str,
        spread_pct: float = 0.0,
        target_profit_per_share: float | None = None,
        target_profit_total: float | None = None,
    ) -> dict[str, Any]:
        symbol = symbol.upper()
        
        # Validation for CRYPTO: require stop loss to be configured
        if self._is_crypto_symbol(symbol) and self.settings.crypto_allow_stop_loss:
            if self.settings.crypto_max_hold_minutes <= 0:
                raise ValueError(
                    f"CRYPTO {symbol}: max_hold_minutes must be > 0 (configured: {self.settings.crypto_max_hold_minutes})"
                )
            self.logger.warning(
                "CRYPTO %s: Opening position with max_hold_time=%d minutes. Stop loss enabled.",
                symbol,
                self.settings.crypto_max_hold_minutes,
            )
        
        can_open, reason_text = self.can_open_new_trade(symbol)
        if not can_open:
            raise ValueError(reason_text)

        requested_qty = float(qty)
        if requested_qty <= 0:
            raise ValueError("La cantidad de compra debe ser mayor que cero")

        current_price = self.market_data.get_last_price(symbol)
        quote = self.market_data.get_latest_quote(symbol)
        configured_target_total = max(float(target_profit_total or target_profit_per_share or self.target_total_usd), 0.0)
        configured_target_price_delta = max(float(target_profit_per_share or self.target_price_delta), 0.0)
        if configured_target_total <= 0 and configured_target_price_delta <= 0:
            raise ValueError("El target de ganancia debe ser mayor que cero")
        entry_tif = "gtc"
        if self._is_crypto_symbol(symbol):
            configured_tif = str(getattr(self.settings, "crypto_entry_time_in_force", "ioc") or "ioc").lower().strip()
            entry_tif = configured_tif or "ioc"
        buy_submitted_at = time.monotonic()
        order = self.order_manager.create_market_order(symbol=symbol, qty=requested_qty, side="buy", time_in_force=entry_tif)

        # Fast path: for market buys we only need the first confirmed fill to post target limit quickly.
        resolved_order = self._wait_order_fill(
            order,
            requested_qty=requested_qty,
            max_attempts=10,
            sleep_seconds=0.12,
            stop_on_any_fill=True,
        )
        order_status = str(resolved_order.get("status", "")).lower()
        filled_qty = float(resolved_order.get("filled_qty", 0.0) or 0.0)
        if filled_qty <= 1e-8:
            # Fallback: wait a bit longer before failing hard.
            resolved_order = self._wait_order_fill(order, requested_qty=requested_qty, max_attempts=20, sleep_seconds=0.35)
            order_status = str(resolved_order.get("status", "")).lower()
            filled_qty = float(resolved_order.get("filled_qty", 0.0) or 0.0)
        if filled_qty <= 1e-8:
            order_id = str(resolved_order.get("id", order.get("id", "")))
            raise ValueError(
                (
                    f"Orden market sin llenado (status={order_status}, "
                    f"filled_qty={filled_qty:.8f}, requested_qty={requested_qty:.8f}, order_id={order_id})."
                )
            )

        if order_status != "filled" or filled_qty < (requested_qty - 1e-8):
            self.logger.warning(
                "Entrada parcial %s status=%s filled_qty=%.8f requested_qty=%.8f",
                symbol,
                order_status,
                filled_qty,
                requested_qty,
            )

        filled_price = float(
            resolved_order.get("filled_avg_price")
            or resolved_order.get("avg_entry_price")
            or order.get("filled_avg_price")
            or order.get("avg_entry_price")
            or current_price
        )
        entry_cost = filled_price * filled_qty
        target_plan = self._build_target_plan(
            side="long",
            entry_price=filled_price,
            current_price=current_price,
            qty=filled_qty,
            target_total_usd=configured_target_total,
            target_price_delta=configured_target_price_delta,
        )
        if not bool(target_plan.get("valid", False)):
            raise ValueError(", ".join(target_plan.get("warnings", []) or [str(target_plan.get("status", "target_invalid"))]))
        effective_target_total_usd = float(target_plan.get("target_total_usd", configured_target_total) or configured_target_total)
        effective_target_price_delta = float(target_plan.get("target_price_delta", 0.0) or 0.0)
        effective_target_price = float(target_plan.get("target_price", 0.0) or 0.0)
        trade_id = str(uuid.uuid4())
        slippage = abs(filled_price - current_price)
        self.journal.record(
            {
                "record_type": "entry",
                "trade_id": trade_id,
                "symbol": symbol,
                "entry_time": self._now_iso(),
                "entry_price": filled_price,
                "qty": filled_qty,
                "entry_cost": entry_cost,
                "current_price": current_price,
                "floating_pnl": 0.0,
                "state": "OPEN",
                "reason_buy": reason,
                "reason_sell": "",
                "duration_seconds": 0,
                "order_type": "market",
                "side": "long",
                "target_mode": str(target_plan.get("target_mode", self._target_mode())),
                "target_total_usd": effective_target_total_usd,
                "target_price_delta": effective_target_price_delta,
                "target_price": effective_target_price,
                "target_profit_per_share": effective_target_price_delta,
                "slippage_estimated": slippage,
                "spread_at_entry": quote.get("spread", 0.0),
                "spread_pct_at_entry": quote.get("spread_pct", spread_pct),
                "status": "OPEN",
            }
        )

        self.logger.info(
            "Entrada registrada %s qty=%.8f entry=%.8f cost=%.8f",
            symbol,
            filled_qty,
            filled_price,
            entry_cost,
        )

        immediate_exit: dict[str, Any] | None = None
        try:
            immediate_exit = self._place_immediate_target_exit(
                symbol=symbol,
                qty=filled_qty,
                avg_entry_price=filled_price,
                current_price=current_price,
                side="long",
                reason_sell="target_immediate_after_buy",
                target_total_usd=effective_target_total_usd,
                target_price_delta=effective_target_price_delta,
            )
            elapsed_ms = (time.monotonic() - buy_submitted_at) * 1000.0
            self.logger.info("Latency buy->limit %s %.0fms", symbol, elapsed_ms)
        except Exception as ex:
            self.logger.warning("No se pudo crear salida inmediata para %s: %s", symbol, ex)

        return {
            "trade_id": trade_id,
            "order": resolved_order,
            "entry_price": filled_price,
            "filled_qty": filled_qty,
            "requested_qty": requested_qty,
            "partial_fill": filled_qty < (requested_qty - 1e-8),
            "entry_cost": entry_cost,
            "current_price": current_price,
            "target_mode": str(target_plan.get("target_mode", self._target_mode())),
            "target_total_usd": effective_target_total_usd,
            "target_price_delta": effective_target_price_delta,
            "target_price": effective_target_price,
            "target_profit_per_share": effective_target_price_delta,
            "spread_pct": quote.get("spread_pct", spread_pct),
            "immediate_exit": immediate_exit,
        }

    def open_position_limit(
        self,
        symbol: str,
        qty: float,
        reason: str,
        spread_pct: float = 0.0,
        target_profit_per_share: float | None = None,
        target_profit_total: float | None = None,
    ) -> dict[str, Any]:
        symbol = symbol.upper()

        can_open, reason_text = self.can_open_new_trade(symbol)
        if not can_open:
            raise ValueError(reason_text)

        requested_qty = float(qty)
        if requested_qty <= 0:
            raise ValueError("La cantidad de compra debe ser mayor que cero")

        current_price = self.market_data.get_last_price(symbol)
        quote = self.market_data.get_latest_quote(symbol)
        configured_target_total = max(float(target_profit_total or target_profit_per_share or self.target_total_usd), 0.0)
        configured_target_price_delta = max(float(target_profit_per_share or self.target_price_delta), 0.0)
        if configured_target_total <= 0 and configured_target_price_delta <= 0:
            raise ValueError("El target de ganancia debe ser mayor que cero")

        entry_tif = "gtc"
        if self._is_crypto_symbol(symbol):
            configured_tif = str(getattr(self.settings, "crypto_entry_time_in_force", "gtc") or "gtc").lower().strip()
            entry_tif = configured_tif or "gtc"

        limit_price = self._suggest_limit_entry_price(symbol=symbol, current_price=current_price, quote=quote)
        buy_submitted_at = time.monotonic()
        order = self.order_manager.create_limit_order(
            symbol=symbol,
            qty=requested_qty,
            side="buy",
            limit_price=limit_price,
            time_in_force=entry_tif,
        )

        resolved_order = self._wait_order_fill(
            order,
            requested_qty=requested_qty,
            max_attempts=6,
            sleep_seconds=0.35,
            stop_on_any_fill=True,
        )
        order_status = str(resolved_order.get("status", "") or "").lower().strip()
        filled_qty = float(resolved_order.get("filled_qty", 0.0) or 0.0)

        if filled_qty <= 1e-8:
            order_id = str(resolved_order.get("id", order.get("id", "")) or "").strip()
            if order_id and order_status not in {"filled", "canceled", "rejected", "expired"}:
                try:
                    self.order_manager.cancel_order(order_id)
                    order_status = "canceled"
                except Exception:
                    pass
            raise ValueError(
                (
                    f"Orden limit IA no llenó a tiempo (status={order_status or 'unknown'}, "
                    f"limit={limit_price:.8f}). Se reintentará en el siguiente ciclo."
                )
            )

        if order_status != "filled" or filled_qty < (requested_qty - 1e-8):
            self.logger.warning(
                "Entrada limit parcial %s status=%s filled_qty=%.8f requested_qty=%.8f limit=%.8f",
                symbol,
                order_status,
                filled_qty,
                requested_qty,
                limit_price,
            )

        filled_price = float(
            resolved_order.get("filled_avg_price")
            or resolved_order.get("avg_entry_price")
            or order.get("filled_avg_price")
            or order.get("avg_entry_price")
            or limit_price
        )
        entry_cost = filled_price * filled_qty
        target_plan = self._build_target_plan(
            side="long",
            entry_price=filled_price,
            current_price=current_price,
            qty=filled_qty,
            target_total_usd=configured_target_total,
            target_price_delta=configured_target_price_delta,
        )
        if not bool(target_plan.get("valid", False)):
            raise ValueError(", ".join(target_plan.get("warnings", []) or [str(target_plan.get("status", "target_invalid"))]))
        effective_target_total_usd = float(target_plan.get("target_total_usd", configured_target_total) or configured_target_total)
        effective_target_price_delta = float(target_plan.get("target_price_delta", 0.0) or 0.0)
        effective_target_price = float(target_plan.get("target_price", 0.0) or 0.0)
        trade_id = str(uuid.uuid4())
        slippage = abs(filled_price - current_price)
        self.journal.record(
            {
                "record_type": "entry",
                "trade_id": trade_id,
                "symbol": symbol,
                "entry_time": self._now_iso(),
                "entry_price": filled_price,
                "qty": filled_qty,
                "entry_cost": entry_cost,
                "current_price": current_price,
                "floating_pnl": 0.0,
                "state": "OPEN",
                "reason_buy": reason,
                "reason_sell": "",
                "duration_seconds": 0,
                "order_type": "limit",
                "target_mode": str(target_plan.get("target_mode", self._target_mode())),
                "target_total_usd": effective_target_total_usd,
                "target_price_delta": effective_target_price_delta,
                "target_price": effective_target_price,
                "target_profit_per_share": effective_target_price_delta,
                "slippage_estimated": slippage,
                "spread_at_entry": quote.get("spread", 0.0),
                "spread_pct_at_entry": quote.get("spread_pct", spread_pct),
                "status": "OPEN",
            }
        )

        self.logger.info(
            "Entrada limit IA registrada %s qty=%.8f entry=%.8f limit=%.8f cost=%.8f",
            symbol,
            filled_qty,
            filled_price,
            limit_price,
            entry_cost,
        )

        immediate_exit: dict[str, Any] | None = None
        try:
            immediate_exit = self._place_immediate_target_exit(
                symbol=symbol,
                qty=filled_qty,
                avg_entry_price=filled_price,
                current_price=current_price,
                side="long",
                reason_sell="target_immediate_after_buy",
                target_total_usd=effective_target_total_usd,
                target_price_delta=effective_target_price_delta,
            )
            elapsed_ms = (time.monotonic() - buy_submitted_at) * 1000.0
            self.logger.info("Latency buy-limit->limit %s %.0fms", symbol, elapsed_ms)
        except Exception as ex:
            self.logger.warning("No se pudo crear salida inmediata para %s: %s", symbol, ex)

        return {
            "trade_id": trade_id,
            "order": resolved_order,
            "entry_price": filled_price,
            "filled_qty": filled_qty,
            "requested_qty": requested_qty,
            "partial_fill": filled_qty < (requested_qty - 1e-8),
            "entry_cost": entry_cost,
            "current_price": current_price,
            "target_mode": str(target_plan.get("target_mode", self._target_mode())),
            "target_total_usd": effective_target_total_usd,
            "target_price_delta": effective_target_price_delta,
            "target_price": effective_target_price,
            "target_profit_per_share": effective_target_price_delta,
            "spread_pct": quote.get("spread_pct", spread_pct),
            "immediate_exit": immediate_exit,
            "requested_limit_price": limit_price,
        }

    def _place_immediate_target_exit(
        self,
        symbol: str,
        qty: float,
        avg_entry_price: float,
        current_price: float,
        side: str,
        reason_sell: str,
        target_total_usd: float | None = None,
        target_price_delta: float | None = None,
    ) -> dict[str, Any]:
        if qty <= 0:
            return {
                "symbol": symbol,
                "action": "SKIP_IMMEDIATE_EXIT",
                "reason": "qty_zero",
            }

        existing_order = self._find_pending_exit_order(symbol, side=side)
        if existing_order is not None:
            return {
                "symbol": symbol,
                "action": "LIMIT_EXIT_PENDING",
                "reason": reason_sell,
                "order_id": str(existing_order.get("id", "")),
                "limit_price": float(existing_order.get("limit_price", current_price) or current_price),
            }

        target_plan = self._build_target_plan(
            side=side,
            entry_price=avg_entry_price,
            current_price=current_price,
            qty=qty,
            target_total_usd=target_total_usd if target_total_usd is not None else self.get_target_total_usd_for_symbol(symbol),
            target_price_delta=target_price_delta,
        )
        if not bool(target_plan.get("valid", False)):
            raise ValueError(", ".join(target_plan.get("warnings", []) or [str(target_plan.get("status", "target_invalid"))]))
        limit_price = float(target_plan.get("target_price", 0.0) or 0.0)
        order_side = self._exit_side_for_position(side)
        order = self.order_manager.create_limit_order(
            symbol=symbol,
            qty=float(qty),
            side=order_side,
            limit_price=limit_price,
            time_in_force="gtc",
        )
        self.logger.info(
            "Salida inmediata creada %s qty=%.8f limit=%.8f reason=%s",
            symbol,
            qty,
            limit_price,
            reason_sell,
        )
        return {
            "symbol": symbol,
            "action": "LIMIT_EXIT_PLACED",
            "reason": reason_sell,
            "order_id": str(order.get("id", "")),
            "limit_price": float(order.get("limit_price", limit_price) or limit_price),
            "side": order_side,
            "qty": float(qty),
            "target_mode": str(target_plan.get("target_mode", self._target_mode())),
            "target_total_usd": float(target_plan.get("target_total_usd", 0.0) or 0.0),
            "target_price_delta": float(target_plan.get("target_price_delta", 0.0) or 0.0),
            "target_price": float(target_plan.get("target_price", limit_price) or limit_price),
            "expected_gross_profit_usd": float(target_plan.get("expected_gross_profit_usd", 0.0) or 0.0),
            "expected_net_profit_usd": float(target_plan.get("expected_net_profit_usd", 0.0) or 0.0),
            "take_profit_available": bool(target_plan.get("take_profit_available", False)),
            "warnings": list(target_plan.get("warnings", []) or []),
        }

    def _wait_order_fill(
        self,
        order: dict[str, Any],
        requested_qty: float | None = None,
        max_attempts: int = 8,
        sleep_seconds: float = 0.5,
        stop_on_any_fill: bool = False,
    ) -> dict[str, Any]:
        order_id = str(order.get("id", "")).strip()
        if not order_id or not hasattr(self.broker, "get_order"):
            return order

        current = order
        for _ in range(max_attempts):
            status = str(current.get("status", "")).lower()
            filled_qty = float(current.get("filled_qty", 0.0) or 0.0)
            if stop_on_any_fill and filled_qty > 1e-8:
                return current
            if requested_qty is not None and filled_qty >= (float(requested_qty) - 1e-8):
                return current
            if status in {"filled", "canceled", "rejected", "expired"}:
                return current
            time.sleep(sleep_seconds)
            try:
                current = self.broker.get_order(order_id)
            except Exception:
                break
        return current

    def _has_pending_buy_order(self, symbol: str) -> bool:
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
            return False

        for order in orders:
            side = str(order.get("side", "")).lower().strip()
            status = str(order.get("status", "")).lower().strip()
            if side != "buy" or status not in pending_statuses:
                continue
            if self._symbol_key(str(order.get("symbol", ""))) != target:
                continue
            return True
        return False

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

        existing_order = self._find_pending_exit_order(snapshot.symbol, side=snapshot.side)
        if existing_order is not None:
            return {
                "symbol": snapshot.symbol,
                "action": "LIMIT_EXIT_PENDING",
                "reason": reason_sell,
                "order_id": str(existing_order.get("id", "")),
                "limit_price": float(existing_order.get("limit_price", snapshot.current_price) or snapshot.current_price),
            }

        target_plan = self._build_target_plan(
            side=snapshot.side,
            entry_price=snapshot.avg_entry_price,
            current_price=snapshot.current_price,
            qty=snapshot.qty,
            target_total_usd=self.get_target_total_usd_for_symbol(snapshot.symbol),
            target_price_delta=self.target_price_delta,
        )
        if not bool(target_plan.get("valid", False)):
            return {
                "symbol": snapshot.symbol,
                "action": "HOLD",
                "reason": str(target_plan.get("status", "target_invalid")),
                "warnings": list(target_plan.get("warnings", []) or []),
            }
        limit_price = float(target_plan.get("target_price", 0.0) or 0.0)
        order_side = self._exit_side_for_position(snapshot.side)
        order = self.order_manager.create_limit_order(
            symbol=snapshot.symbol,
            qty=snapshot.qty,
            side=order_side,
            limit_price=limit_price,
            time_in_force="gtc",
        )
        return {
            "symbol": snapshot.symbol,
            "action": "LIMIT_EXIT_PLACED",
            "reason": reason_sell,
            "order_id": str(order.get("id", "")),
            "limit_price": float(order.get("limit_price", limit_price) or limit_price),
            "side": order_side,
            "qty": snapshot.qty,
            "target_mode": str(target_plan.get("target_mode", self._target_mode())),
            "target_total_usd": float(target_plan.get("target_total_usd", 0.0) or 0.0),
            "target_price_delta": float(target_plan.get("target_price_delta", 0.0) or 0.0),
            "target_price": float(target_plan.get("target_price", limit_price) or limit_price),
            "expected_gross_profit_usd": float(target_plan.get("expected_gross_profit_usd", 0.0) or 0.0),
            "expected_net_profit_usd": float(target_plan.get("expected_net_profit_usd", 0.0) or 0.0),
            "take_profit_available": bool(target_plan.get("take_profit_available", False)),
            "warnings": list(target_plan.get("warnings", []) or []),
        }

    def _find_pending_exit_order(self, symbol: str, *, side: str, suppress_errors: bool = True) -> dict[str, Any] | None:
        target = self._symbol_key(symbol)
        order_side = self._exit_side_for_position(side)
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
            if not suppress_errors:
                raise
            return None

        for order in orders:
            side = str(order.get("side", "")).lower().strip()
            status = str(order.get("status", "")).lower().strip()
            if side != order_side or status not in pending_statuses:
                continue
            if self._symbol_key(str(order.get("symbol", ""))) != target:
                continue
            return order
        return None

    def get_pending_exit_order(self, symbol: str, *, side: str, suppress_errors: bool = True) -> dict[str, Any] | None:
        return self._find_pending_exit_order(symbol, side=side, suppress_errors=suppress_errors)

    def get_pending_sell_order(self, symbol: str, *, suppress_errors: bool = True) -> dict[str, Any] | None:
        return self.get_pending_exit_order(symbol, side="long", suppress_errors=suppress_errors)

    def _suggest_limit_entry_price(self, symbol: str, current_price: float, quote: dict[str, Any] | None) -> float:
        ask_price = 0.0
        spread = 0.0
        if quote is not None:
            ask_price = float(quote.get("ask", 0.0) or 0.0)
            spread = float(quote.get("spread", 0.0) or 0.0)

        reference_price = max(float(current_price or 0.0), ask_price)
        if reference_price <= 0:
            raise ValueError("No se pudo determinar precio de referencia para limit buy IA")

        if self._is_crypto_symbol(symbol):
            buffer_pct = 0.0015
        else:
            buffer_pct = 0.0008

        buffer_abs = max(reference_price * buffer_pct, spread * 1.25, 0.01)
        return round(reference_price + buffer_abs, 6)

    def _suggest_limit_exit_price(
        self,
        *,
        side: str,
        current_price: float,
        avg_entry_price: float,
        qty: float,
        target_total_usd: float | None = None,
        target_price_delta: float | None = None,
    ) -> float:
        target_plan = self._build_target_plan(
            side=side,
            entry_price=avg_entry_price,
            current_price=current_price,
            qty=qty,
            target_total_usd=target_total_usd,
            target_price_delta=target_price_delta,
        )
        if not bool(target_plan.get("valid", False)):
            raise ValueError(", ".join(target_plan.get("warnings", []) or [str(target_plan.get("status", "target_invalid"))]))
        return round(float(target_plan.get("target_price", 0.0) or 0.0), 6)

    @staticmethod
    def _suggest_force_limit_exit_price(current_price: float, quote: dict[str, Any] | None) -> float:
        bid_price = 0.0
        if quote is not None:
            bid_price = float(quote.get("bid", 0.0) or 0.0)
        base_price = bid_price if bid_price > 0 else float(current_price)
        if base_price <= 0:
            raise ValueError("No se pudo determinar precio limite de salida")
        return round(base_price, 6)

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

            entry_side = str(entry.get("side", "long") or "long").lower().strip()
            matched_order = self._latest_filled_exit_order_for_symbol(symbol=symbol, orders=orders, side=entry_side)
            if matched_order is None:
                continue

            qty = float(matched_order.get("filled_qty", entry.get("qty", 0.0)) or 0.0)
            entry_price = float(entry.get("entry_price", 0.0) or 0.0)
            exit_price = float(matched_order.get("filled_avg_price", 0.0) or 0.0)
            if qty <= 0 or exit_price <= 0:
                continue

            realized_pnl = (exit_price - entry_price) * qty if entry_side != "short" else (entry_price - exit_price) * qty
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
                    "side": entry_side,
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
                    "action": "BUY" if entry_side == "short" else "SELL",
                    "reason": "limit_exit_filled",
                    "trigger_price": float(matched_order.get("limit_price", exit_price) or exit_price),
                    "exit_price": exit_price,
                    "realized_pnl": realized_pnl,
                    "close_order_id": str(matched_order.get("id", "")),
                }
            )
            self.logger.info("Salida limit registrada %s pnl=%s", symbol, realized_pnl)

        return actions

    def _latest_filled_exit_order_for_symbol(self, symbol: str, orders: list[dict[str, Any]], side: str) -> dict[str, Any] | None:
        symbol_key = self._symbol_key(symbol)
        order_side = self._exit_side_for_position(side)
        candidates = [
            order
            for order in orders
            if str(order.get("side", "")).lower().strip() == order_side
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
        cancel_summary = self._cancel_open_limit_orders_for_symbol(snapshot.symbol)
        if int(cancel_summary.get("remaining", 0) or 0) > 0:
            raise ValueError(
                f"No se pudieron cancelar todos los LIMIT de {snapshot.symbol} antes de vender (restantes={cancel_summary.get('remaining')})."
            )

        close_result = self.broker.close_position(snapshot.symbol)
        close_order_id = str(close_result.get("id", "") or "")
        filled_qty = float(close_result.get("filled_qty", snapshot.qty) or snapshot.qty)
        if filled_qty <= 1e-8:
            filled_qty = float(snapshot.qty)
        exit_price = self._resolve_exit_price(close_result=close_result, close_order_id=close_order_id, fallback_price=trigger_price)
        realized_pnl = (
            (exit_price - snapshot.avg_entry_price) * filled_qty
            if snapshot.side != "short"
            else (snapshot.avg_entry_price - exit_price) * filled_qty
        )
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
                "qty": filled_qty,
                "current_price": snapshot.current_price,
                "floating_pnl": snapshot.unrealized_pl,
                "state": snapshot.state,
                "side": snapshot.side,
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
            "action": "BUY" if snapshot.side == "short" else "SELL",
            "reason": reason_sell,
            "trigger_price": trigger_price,
            "exit_price": exit_price,
            "realized_pnl": realized_pnl,
            "duration_seconds": duration_seconds,
            "close_result": close_result,
            "close_order_id": close_order_id,
            "cancelled_limit_orders": int(cancel_summary.get("cancelled", 0) or 0),
            "pending_limit_orders": int(cancel_summary.get("remaining", 0) or 0),
        }

    def _cancel_open_limit_orders_for_symbol(
        self,
        symbol: str,
        max_attempts: int = 10,
        sleep_seconds: float = 0.35,
    ) -> dict[str, int]:
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

        cancelled = 0
        failed = 0

        try:
            open_orders = self.broker.list_orders(status="open", limit=200)
        except Exception:
            open_orders = []

        limit_orders = [
            order
            for order in open_orders
            if self._symbol_key(str(order.get("symbol", ""))) == target
            and str(order.get("type", "")).lower().strip() == "limit"
            and str(order.get("status", "")).lower().strip() in pending_statuses
        ]

        for order in limit_orders:
            order_id = str(order.get("id", "")).strip()
            if not order_id:
                continue
            try:
                if self.order_manager.cancel_order(order_id):
                    cancelled += 1
                else:
                    failed += 1
            except Exception:
                failed += 1

        remaining = 0
        for _ in range(max_attempts):
            try:
                refreshed = self.broker.list_orders(status="open", limit=200)
            except Exception:
                break
            remaining = sum(
                1
                for order in refreshed
                if self._symbol_key(str(order.get("symbol", ""))) == target
                and str(order.get("type", "")).lower().strip() == "limit"
                and str(order.get("status", "")).lower().strip() in pending_statuses
            )
            if remaining <= 0:
                break
            time.sleep(sleep_seconds)

        return {
            "found": len(limit_orders),
            "cancelled": cancelled,
            "failed": failed,
            "remaining": max(int(remaining), 0),
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

    def get_target_profit_per_share_for_symbol(self, symbol: str) -> float:
        entry = self.journal.get_open_entry_by_symbol(symbol)
        if entry is not None:
            try:
                qty = float(entry.get("qty", 0.0) or 0.0)
                delta = float(entry.get("target_price_delta", 0.0) or 0.0)
                if delta > 0:
                    return delta
                total = float(entry.get("target_total_usd", entry.get("target_profit_total", 0.0)) or 0.0)
                if total > 0 and qty > 0:
                    return total / qty
                value = float(entry.get("target_profit_per_share", 0.0) or 0.0)
                if value > 0:
                    return value
            except (TypeError, ValueError):
                pass
        return float(self.target_profit_per_share)

    def _should_auto_sell(self, snapshot: PositionSnapshot, minutes_to_close: int) -> str | None:
        """
        Determine if a position should be automatically closed.
        
        Respects asset-type specific rules:
        - STOCKS: Hold allowed, no stop loss, no sell below average cost
        - CRYPTO: No overnight hold, allow stop loss, close quickly
        """
        is_crypto = self._is_crypto_symbol(snapshot.symbol)
        
        # Minimum hold time before auto-sell
        min_hold_seconds = int(getattr(self.settings, "min_hold_seconds_before_auto_sell", 0) or 0)
        if min_hold_seconds > 0 and snapshot.duration_seconds < min_hold_seconds:
            return None

        # ===== CRYPTO: Max hold time enforcement =====
        if is_crypto and self.settings.crypto_max_hold_minutes > 0:
            max_hold_seconds = self.settings.crypto_max_hold_minutes * 60
            if snapshot.duration_seconds >= max_hold_seconds:
                return f"crypto_max_hold_{self.settings.crypto_max_hold_minutes}m_exceeded"

        # ===== STOCKS: Never sell at loss (if configured) =====
        if not is_crypto and self.settings.stock_no_auto_sell_below_avg_cost:
            if snapshot.unrealized_pl <= 0:
                return None
        
        # ===== CRYPTO: Stop loss in percentage terms =====
        if is_crypto:
            thresholds = self.settings.get_crypto_thresholds(snapshot.symbol)
            stop_loss_pct = float(thresholds.get("stop_loss_pct", 0.0) or 0.0) / 100.0
            crypto_stop_loss_per_share = snapshot.avg_entry_price * stop_loss_pct if snapshot.avg_entry_price > 0 else 0.0
            if self.settings.crypto_allow_stop_loss and crypto_stop_loss_per_share > 0 and snapshot.pnl_per_share <= -crypto_stop_loss_per_share:
                return f"crypto_stop_loss_{stop_loss_pct * 100.0:.2f}%"
            if not self.settings.crypto_allow_stop_loss and snapshot.unrealized_pl <= 0:
                return None

        # ===== TARGET PROFIT (both stocks and crypto) =====
        target_total_usd = self.get_target_total_usd_for_symbol(snapshot.symbol)
        if snapshot.unrealized_pl >= target_total_usd:
            return "take_profit_available"

        # ===== STOCKS: Market close logic (stocks only) =====
        if not is_crypto and minutes_to_close <= self.settings.stop_new_trades_minutes_before_close:
            if snapshot.unrealized_pl > 0:
                return "near_close_positive"

        # ===== TECHNICAL ANALYSIS (candle-based) =====
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

        # Sell at resistance or momentum weakening (only if profitable for stocks)
        if not is_crypto:
            if current_price >= recent_high * 0.998 and snapshot.unrealized_pl > 0:
                return "near_resistance"
            if last_volume < (average_volume * 0.7) and snapshot.unrealized_pl > 0:
                return "volume_weakening"
            if current_price < short_ma and snapshot.unrealized_pl > 0:
                return "momentum_weakening"
        else:
            # CRYPTO: More aggressive exit conditions
            if current_price >= recent_high * 0.998:
                return "crypto_near_resistance"
            if last_volume < (average_volume * 0.5):  # More sensitive to volume
                return "crypto_volume_dried_up"
            if current_price < short_ma:
                return "crypto_momentum_broken"

        return None

    def _crypto_target_profit_amount(self, symbol: str, reference_price: float) -> float:
        if reference_price <= 0:
            return float(self.target_profit_per_share)
        thresholds = self.settings.get_crypto_thresholds(symbol)
        take_profit_pct = float(thresholds.get("tp1_pct", 0.5) or 0.5) / 100.0
        return round(reference_price * take_profit_pct, 6)

    def _build_snapshot(self, position: dict[str, Any]) -> PositionSnapshot:
        symbol = str(position.get("symbol", "")).upper()
        qty = float(position.get("qty", 0.0) or 0.0)
        side = str(position.get("side", "long") or "long").lower().strip()
        if side not in {"long", "short"}:
            side = "long"
        leverage = float(position.get("leverage", 1.0) or 1.0)
        if leverage <= 0:
            leverage = 1.0
        avg_entry_price = float(position.get("avg_entry_price", 0.0) or 0.0)
        current_price = float(self.market_data.get_last_price(symbol))
        if side == "short":
            unrealized_pl = (avg_entry_price - current_price) * qty
        else:
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
            side=side,
            leverage=leverage,
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
        target = self._symbol_key(symbol)
        for position in self.get_open_positions():
            if self._symbol_key(str(position.get("symbol", ""))) == target:
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