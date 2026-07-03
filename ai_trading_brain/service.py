from __future__ import annotations

import math
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

import requests

from ai_trading_brain.openai_analyzer import OpenAIAnalyzer
from ai_trading_brain.scoring import compute_composite_score
from ai_trading_brain.workers import AutoTradingController, DataCollectorWorker, ModelTrainerWorker, NewsSocialCollectorWorker, OutcomeLabelerWorker, SignalScannerWorker
from database.manager import TradingBrainDatabase
from ml_model.model_registry import ModelRegistry
from ml_model.model_trainer import ModelTrainer
from ml_model.predictor import SignalPredictor


class AITradingBrainService:
    def __init__(
        self,
        broker: Any,
        market_data: Any,
        order_manager: Any,
        position_manager: Any,
        risk_manager: Any,
        settings: Any,
        logger: Any,
    ) -> None:
        self.broker = broker
        self.market_data = market_data
        self.order_manager = order_manager
        self.position_manager = position_manager
        self.risk_manager = risk_manager
        self.settings = settings
        self.logger = logger
        self.database = TradingBrainDatabase(settings.ai_brain_db_path)
        models_dir = Path(settings.ai_models_dir)
        self.registry = ModelRegistry(str(models_dir))
        self.predictor = SignalPredictor(registry=self.registry, logger=logger)
        self.trainer = ModelTrainer(database=self.database, registry=self.registry, logger=logger)
        self.openai_analyzer = OpenAIAnalyzer(api_key=settings.openai_api_key, logger=logger, model=settings.openai_model)
        self._worker_lock = threading.Lock()
        self._active_account_for_workers = ""
        self._last_collector_cycle_at = ""
        self._last_news_cycle_at = ""
        self._last_training_cycle_at = ""
        self._last_api_error = ""
        self._last_openai_call_at = ""
        self._api_calls_today = 0
        self._openai_calls_today = 0
        self._stats_day = datetime.now(timezone.utc).date().isoformat()
        self._last_collected_at_by_symbol: dict[str, float] = {}
        self._seen_news_signatures: dict[str, float] = {}
        self._market_open_cached = True
        self._market_open_checked_at = 0.0
        self._last_auto_trained_outcomes = 0
        self._news_fetch_cache_by_symbol: dict[str, tuple[float, list[dict[str, str]]]] = {}
        self._news_fetch_cooldown_until_by_symbol: dict[str, float] = {}
        self.auto_controller = AutoTradingController(
            signal_only_mode=bool(settings.ai_signal_only_mode),
            paper_trading=bool(settings.paper_trading),
            live_trading_enabled=False,
            manual_approval_required=bool(settings.manual_approval_required),
        )
        self.data_collector_worker = DataCollectorWorker(
            name="DataCollectorWorker",
            loop_fn=self._collector_cycle,
            sleep_seconds_fn=self._collector_interval_seconds,
            logger=logger,
        )
        self.signal_scanner_worker = SignalScannerWorker(
            name="SignalScannerWorker",
            loop_fn=self._scanner_cycle,
            sleep_seconds_fn=lambda: 20.0,
            logger=logger,
        )
        self.outcome_labeler_worker = OutcomeLabelerWorker(
            name="OutcomeLabelerWorker",
            loop_fn=self._labeler_cycle,
            sleep_seconds_fn=lambda: 30.0,
            logger=logger,
        )
        self.news_social_worker = NewsSocialCollectorWorker(
            name="NewsSocialCollectorWorker",
            loop_fn=self._news_social_cycle,
            sleep_seconds_fn=self._news_interval_seconds,
            logger=logger,
        )
        self.model_trainer_worker = ModelTrainerWorker(
            name="ModelTrainerWorker",
            loop_fn=self._training_cycle,
            sleep_seconds_fn=self._training_interval_seconds,
            logger=logger,
        )
        self.initialize()

    def initialize(self) -> None:
        for account_name, profile in self.settings.account_profiles().items():
            account_id = self.database.upsert_account(
                broker="alpaca",
                account_name=account_name,
                account_type=str(profile.get("mode", "PAPER")),
                is_paper="paper" in str(profile.get("mode", "")).lower(),
                status="active" if profile.get("endpoint") and profile.get("key") and profile.get("secret") else "missing_credentials",
            )
            self.database.upsert_bot_funds(
                account_id=account_id,
                max_capital_assigned=float(self.settings.ai_default_max_capital_assigned),
                available_capital=float(self.settings.ai_default_max_capital_assigned),
                capital_used=0.0,
                max_position_size=float(self.settings.ai_default_max_position_size),
                max_daily_loss=float(self.settings.ai_default_max_daily_loss),
                enabled=True,
            )
            self.database.upsert_runtime_settings(
                account_id=account_id,
                signal_only_mode=bool(self.settings.ai_signal_only_mode),
                paper_trading=bool(self.settings.paper_trading),
                live_trading_enabled=bool(self.settings.live_trading_enabled),
                manual_approval_required=bool(self.settings.manual_approval_required),
                kill_switch=False,
            )
        self.database.cleanup_old_data(
            snapshot_minutes_days=int(self.settings.ai_snapshots_1m_days),
            snapshot_five_min_days=int(self.settings.ai_snapshots_5m_days),
            logs_days=int(self.settings.ai_logs_retention_days),
            keep_model_versions=int(self.settings.ai_keep_model_versions),
        )

    def refresh_account_context(self, account_name: str) -> dict[str, Any]:
        account = self.database.get_account_by_name(account_name)
        if account is None:
            self.initialize()
            account = self.database.get_account_by_name(account_name)
        if account is None:
            raise ValueError(f"Cuenta IA no encontrada: {account_name}")
        return account

    def update_runtime_controls(
        self,
        account_name: str,
        max_capital_assigned: float,
        max_position_size: float,
        max_daily_loss: float,
        enabled: bool,
        signal_only_mode: bool,
        paper_trading: bool,
        live_trading_enabled: bool,
        manual_approval_required: bool,
        kill_switch: bool,
    ) -> None:
        account = self.refresh_account_context(account_name)
        funds = self.database.get_bot_funds(int(account["id"]))
        capital_used = float(funds.get("capital_used", 0.0)) if funds else 0.0
        available_capital = max(float(max_capital_assigned) - capital_used, 0.0)
        self.database.upsert_bot_funds(
            account_id=int(account["id"]),
            max_capital_assigned=float(max_capital_assigned),
            available_capital=available_capital,
            capital_used=capital_used,
            max_position_size=float(max_position_size),
            max_daily_loss=float(max_daily_loss),
            enabled=enabled,
        )
        self.database.upsert_runtime_settings(
            account_id=int(account["id"]),
            signal_only_mode=signal_only_mode,
            paper_trading=paper_trading,
            live_trading_enabled=live_trading_enabled,
            manual_approval_required=manual_approval_required,
            kill_switch=kill_switch,
        )
        self.auto_controller.signal_only_mode = bool(signal_only_mode)
        self.auto_controller.paper_trading = bool(paper_trading)
        self.auto_controller.live_trading_enabled = bool(live_trading_enabled)
        self.auto_controller.manual_approval_required = bool(manual_approval_required)

    def start_automation(self, account_name: str) -> dict[str, Any]:
        self.refresh_account_context(account_name)
        with self._worker_lock:
            self._active_account_for_workers = account_name
        self.data_collector_worker.start()
        self.signal_scanner_worker.start()
        self.outcome_labeler_worker.start()
        self.news_social_worker.start()
        self.model_trainer_worker.start()
        self.logger.info("Workers automaticos iniciados para %s", account_name)
        return self.get_automation_status(account_name)

    def pause_automation(self) -> dict[str, Any]:
        self.data_collector_worker.stop()
        self.signal_scanner_worker.stop()
        self.outcome_labeler_worker.stop()
        self.news_social_worker.stop()
        self.model_trainer_worker.stop()
        self.logger.info("Workers automaticos en pausa")
        return self.get_automation_status(self._active_account_for_workers)

    def get_automation_status(self, account_name: str) -> dict[str, Any]:
        self._reset_daily_counters_if_needed()
        latest_signal = self.database.latest_signals(limit=1)
        snapshots_today = self.database.count_snapshots_today()
        signals_today = self.database.count_signals_today()
        news_today = self.database.count_news_events_today()
        evaluated_today = self.database.count_evaluated_outcomes_today()
        evaluated_total = self.database.count_evaluated_outcomes()
        account = self.database.get_account_by_name(account_name) if account_name else None
        runtime = self.database.get_runtime_settings(int(account["id"])) if account else None
        if runtime is not None:
            self.auto_controller.signal_only_mode = bool(runtime.get("signal_only_mode", 1))
            self.auto_controller.paper_trading = bool(runtime.get("paper_trading", 1))
            self.auto_controller.live_trading_enabled = bool(runtime.get("live_trading_enabled", 0))
            self.auto_controller.manual_approval_required = bool(runtime.get("manual_approval_required", 1))
        return {
            "collector": "Running" if self.data_collector_worker.running else "Stopped",
            "scanner": "Running" if self.signal_scanner_worker.running else "Stopped",
            "labeler": "Running" if self.outcome_labeler_worker.running else "Stopped",
            "news_social": "Running" if self.news_social_worker.running else "Stopped",
            "trainer": "Running" if self.model_trainer_worker.running else "Stopped",
            "last_data_update": self._last_collector_cycle_at,
            "last_news_update": self._last_news_cycle_at,
            "last_training_update": self._last_training_cycle_at,
            "snapshots_today": snapshots_today,
            "signals_today": signals_today,
            "news_events_today": news_today,
            "evaluated_outcomes_today": evaluated_today,
            "evaluated_outcomes": evaluated_total,
            "last_signal": latest_signal[0] if latest_signal else None,
            "last_api_error": self._last_api_error,
            "last_openai_call": self._last_openai_call_at,
            "mode": self.auto_controller.mode_label(),
            "openai_calls_today": self._openai_calls_today,
            "api_calls_today": self._api_calls_today,
            "model_current": self.registry.latest_version() or "heuristic",
            "model_approved_paper": self.registry.approved_version() or "manual_pending",
            "model_approved_live": "manual_required",
        }

    def get_dashboard(self, account_name: str) -> dict[str, Any]:
        account = self.refresh_account_context(account_name)
        try:
            self.sync_positions(account_name)
        except requests.exceptions.HTTPError as ex:
            response = getattr(ex, "response", None)
            status = getattr(response, "status_code", None)
            if status == 401:
                self.logger.warning(
                    "AI dashboard sin sincronizacion en vivo para %s: HTTP 401 Unauthorized. Usando datos persistidos.",
                    account_name,
                )
            else:
                raise
        except requests.exceptions.RequestException as ex:
            self.logger.warning(
                "AI dashboard sin sincronizacion en vivo para %s: %s. Usando datos persistidos.",
                account_name,
                ex,
            )
        dashboard = self.database.dashboard_summary(int(account["id"]))
        runtime = self.database.get_runtime_settings(int(account["id"])) or {}
        training = self.database.latest_training_run()
        return {
            **dashboard,
            "account": account,
            "runtime": runtime,
            "training": training,
        }

    def get_security_state(self, account_name: str) -> dict[str, Any]:
        account = self.refresh_account_context(account_name)
        runtime = self.database.get_runtime_settings(int(account["id"])) or {}
        funds = self.database.get_bot_funds(int(account["id"])) or {}
        return {
            "kill_switch": bool(runtime.get("kill_switch", 0)),
            "paper_trading": bool(runtime.get("paper_trading", 0)),
            "live_trading_enabled": bool(runtime.get("live_trading_enabled", 0)),
            "manual_approval_required": bool(runtime.get("manual_approval_required", 1)),
            "signal_only_mode": bool(runtime.get("signal_only_mode", 1)),
            "max_daily_loss": float(funds.get("max_daily_loss", 0.0) or 0.0),
            "max_position_size": float(funds.get("max_position_size", 0.0) or 0.0),
            "api_keys_ok": bool(self.settings.alpaca_api_key and self.settings.alpaca_api_secret),
            "openai_key_ok": bool(self.settings.openai_api_key),
            "recent_logs": self.database.latest_decision_logs(limit=15),
        }

    def analyze_text(
        self,
        symbol: str,
        asset_type: str,
        text: str,
        source: str,
        account_name: str,
        author: str = "",
    ) -> dict[str, Any]:
        context = {"account_name": account_name}
        analysis = self.openai_analyzer.analyze_text(
            symbol=symbol,
            asset_type=asset_type,
            text=text,
            source=source,
            author=author,
            context=context,
        )
        if text.strip() and bool(self.settings.openai_api_key):
            self._openai_calls_today += 1
            self._last_openai_call_at = self._now_iso()
        self.database.insert_news_event(
            {
                "timestamp": self._now_iso(),
                "symbol": symbol,
                "asset_type": asset_type,
                "source": source,
                "title_or_text": text,
                "url": "",
                "author": author,
                "influence_score": float(analysis.get("importance_score", 0.0) or 0.0) / 100.0,
                "sentiment_score": self._sentiment_to_float(str(analysis.get("sentiment", "neutral"))),
                "ai_summary": str(analysis.get("summary", "")),
                "ai_classification": str(analysis.get("event_type", "other")),
                "raw_payload": analysis,
            }
        )
        return analysis

    def generate_signal(
        self,
        symbol: str,
        asset_type: str,
        account_name: str,
        text_context: str = "",
        source: str = "manual",
    ) -> dict[str, Any]:
        account = self.refresh_account_context(account_name)
        account_id = int(account["id"])
        self.sync_positions(account_name)
        features = self._build_features(symbol=symbol, asset_type=asset_type, account_id=account_id)
        analysis = self.analyze_text(symbol=symbol, asset_type=asset_type, text=text_context, source=source, account_name=account_name) if text_context.strip() else self.openai_analyzer._neutral_response(symbol=symbol, asset_type=asset_type)
        features["news_sentiment_score"] = self._sentiment_to_float(str(analysis.get("sentiment", "neutral")))
        features["news_importance_score"] = float(analysis.get("importance_score", 0.0) or 0.0) / 100.0
        features["news_risk_score"] = float(analysis.get("risk_score", 0.0) or 0.0) / 100.0
        composite = compute_composite_score(features)
        features["composite_score"] = composite["score"]
        prediction = self.predictor.predict_signal(features)

        blocked_reason = self._blocked_buy_reason(symbol=symbol, account_id=account_id, features=features, account_name=account_name)
        signal_type = str(prediction["action"])
        reason = str(prediction["reason"])
        if blocked_reason:
            signal_type = "AVOID" if signal_type in {"BUY", "BUY_SMALL"} else signal_type
            reason = f"{reason} | blocked_reason: {blocked_reason}"

        current_price = float(features["price"])
        average_cost = float(features["average_cost"])
        min_sell_price = self._minimum_sell_price(average_cost)
        if average_cost > 0 and not self._is_sell_allowed(current_price=current_price, average_cost=average_cost):
            signal_type = "HOLD"
            reason = "Precio debajo del average cost. Venta automatica bloqueada"
        elif average_cost > 0 and self._is_sell_allowed(current_price=current_price, average_cost=average_cost):
            signal_type = "SELL_ALLOWED"
            reason = "Precio sobre average cost + buffers. Venta automatica permitida"

        suggested_limit = current_price
        if signal_type in {"BUY", "BUY_SMALL"}:
            suggested_limit = min(current_price, float(features["vwap"] or current_price))
        elif signal_type == "SELL_ALLOWED":
            suggested_limit = max(current_price, min_sell_price)

        signal_id = self.database.insert_signal(
            {
                "timestamp": self._now_iso(),
                "symbol": symbol,
                "asset_type": asset_type,
                "signal_type": signal_type,
                "confidence_score": float(prediction["confidence_score"]),
                "model_version": str(prediction.get("model_version", "general_model_heuristic")),
                "reason": reason,
                "entry_price": current_price,
                "suggested_limit_price": suggested_limit,
                "invalidation_price": max(current_price - float(features["atr"] or 0.0), 0.0),
                "take_profit_price": max(suggested_limit, current_price + float(self.settings.ai_minimum_profit)),
                "risk_level": self._risk_label(float(prediction["risk_score"])),
                "features_json": features,
                "openai_analysis_json": analysis,
            }
        )
        context = {
            "account_name": account_name,
            "features": features,
            "prediction": prediction,
            "analysis": analysis,
        }
        self.database.insert_decision_log(
            {
                "timestamp": self._now_iso(),
                "symbol": symbol,
                "decision": signal_type,
                "reason": reason,
                "blocked_reason": blocked_reason,
                "raw_context_json": context,
            }
        )
        latest = self.database.latest_signals(limit=1)[0]
        latest["id"] = signal_id
        latest["average_cost"] = average_cost
        latest["protection_status"] = "HOLD" if (average_cost > 0 and not self._is_sell_allowed(current_price=current_price, average_cost=average_cost)) else "SELL_ALLOWED" if (average_cost > 0 and self._is_sell_allowed(current_price=current_price, average_cost=average_cost)) else "ACTIVE"
        return latest

    def list_signals(self, limit: int = 25) -> list[dict[str, Any]]:
        return self.database.latest_signals(limit=limit)

    def list_history(self, account_name: str, limit: int = 100) -> list[dict[str, Any]]:
        account = self.refresh_account_context(account_name)
        return self.database.list_trades(int(account["id"]), limit=limit)

    def train_model(self) -> dict[str, Any]:
        evaluated = self.database.count_evaluated_outcomes()
        if evaluated < 200:
            return {
                "trained": False,
                "reason": "No hay suficientes datos para entrenar. Se necesitan mínimo 200 señales evaluadas.",
                "number_of_samples": evaluated,
            }
        return self.trainer.train_general_model()

    def ensure_automation_running(self, account_name: str) -> dict[str, Any]:
        status = self.get_automation_status(account_name)
        if all(status.get(key) == "Running" for key in ("collector", "scanner", "labeler", "news_social", "trainer")):
            return status
        return self.start_automation(account_name)

    def approve_latest_model(self) -> str:
        version = self.registry.latest_version()
        if not version:
            raise ValueError("No hay modelo para aprobar")
        self.registry.approve_model(version)
        return version

    def rollback_model(self) -> str | None:
        return self.registry.rollback_to_previous()

    def sync_positions(self, account_name: str) -> list[dict[str, Any]]:
        account = self.refresh_account_context(account_name)
        account_id = int(account["id"])
        positions = self.broker.get_positions()
        open_symbols: set[str] = set()
        persisted: list[dict[str, Any]] = []
        for position in positions:
            symbol = str(position.get("symbol", "")).upper().replace(" ", "")
            if not symbol:
                continue
            open_symbols.add(symbol)
            asset_type = "crypto" if "/" in symbol or symbol.endswith("USD") else "stock"
            metrics = self.calculate_average_cost(symbol=symbol, account_id=account_id)
            qty = float(position.get("qty", 0.0) or metrics["quantity_owned"] or 0.0)
            current_price = float(self.market_data.get_last_price(symbol))
            average_cost = float(metrics["average_cost"] or position.get("avg_entry_price", 0.0) or 0.0)
            total_cost_basis = float(metrics["total_cost_basis"] or (average_cost * qty))
            unrealized_pnl = (current_price - average_cost) * qty if qty > 0 else 0.0
            min_sell_price = self._minimum_sell_price(average_cost)
            if qty <= 0:
                status = "CLOSED"
            elif average_cost > 0 and not self._is_sell_allowed(current_price=current_price, average_cost=average_cost):
                status = "HOLD"
            elif average_cost > 0 and self._is_sell_allowed(current_price=current_price, average_cost=average_cost):
                status = "SELL_ALLOWED"
            else:
                status = "ACTIVE"
            payload = {
                "account_id": account_id,
                "symbol": symbol,
                "asset_type": asset_type,
                "qty": qty,
                "total_cost_basis": total_cost_basis,
                "average_cost": average_cost,
                "current_price": current_price,
                "unrealized_pnl": unrealized_pnl,
                "realized_pnl": float(metrics["realized_pnl"]),
                "status": status,
                "last_updated": self._now_iso(),
            }
            self.database.upsert_position(payload)
            persisted.append(payload)
        self.database.close_missing_positions(account_id=account_id, open_symbols=open_symbols)
        return persisted

    def calculate_average_cost(self, symbol: str, account_id: int) -> dict[str, float]:
        trades = self.database.list_trades(account_id=account_id, limit=5000)
        symbol_key = self._symbol_key(symbol)
        quantity_owned = 0.0
        total_cost_basis = 0.0
        realized_pnl = 0.0
        for trade in reversed(trades):
            if self._symbol_key(str(trade.get("symbol", ""))) != symbol_key:
                continue
            qty = float(trade.get("qty", 0.0) or 0.0)
            price = float(trade.get("filled_price", 0.0) or trade.get("limit_price", 0.0) or 0.0)
            fees = float(trade.get("fees", 0.0) or 0.0)
            side = str(trade.get("side", "")).lower()
            if side == "buy":
                quantity_owned += qty
                total_cost_basis += (qty * price) + fees
            elif side == "sell" and quantity_owned > 0:
                average_cost = total_cost_basis / quantity_owned if quantity_owned > 0 else 0.0
                quantity_to_reduce = min(qty, quantity_owned)
                proceeds = (quantity_to_reduce * price) - fees
                cost_removed = average_cost * quantity_to_reduce
                realized_pnl += proceeds - cost_removed
                quantity_owned -= quantity_to_reduce
                total_cost_basis -= cost_removed
        average_cost = total_cost_basis / quantity_owned if quantity_owned > 0 else 0.0
        return {
            "quantity_owned": quantity_owned,
            "total_cost_basis": total_cost_basis,
            "average_cost": average_cost,
            "realized_pnl": realized_pnl,
        }

    def place_limit_buy(self, signal_id: int, account_name: str, manual_approved: bool) -> dict[str, Any]:
        account = self.refresh_account_context(account_name)
        account_id = int(account["id"])
        runtime = self.database.get_runtime_settings(account_id) or {}
        funds = self.database.get_bot_funds(account_id) or {}
        signal = next((item for item in self.database.latest_signals(limit=100) if int(item["id"]) == int(signal_id)), None)
        if signal is None:
            raise ValueError("Senal no encontrada")
        blocked = self._blocked_buy_reason(symbol=str(signal["symbol"]), account_id=account_id, features=signal["features_json"], account_name=account_name)
        if blocked:
            raise ValueError(blocked)
        if bool(runtime.get("kill_switch", 0)):
            raise ValueError("Kill switch activo")
        if bool(runtime.get("manual_approval_required", 1)) and not manual_approved:
            raise ValueError("Se requiere aprobacion manual")
        if not bool(runtime.get("paper_trading", 1)) and not bool(runtime.get("live_trading_enabled", 0)):
            raise ValueError("Trading live deshabilitado")
        capital = min(float(funds.get("available_capital", 0.0) or 0.0), float(funds.get("max_position_size", 0.0) or 0.0))
        if capital <= 0:
            raise ValueError("No hay capital disponible")
        limit_price = float(signal["suggested_limit_price"] or signal["entry_price"] or 0.0)
        if limit_price <= 0:
            raise ValueError("Precio limit invalido")
        qty = round(capital / limit_price, 6)
        if qty <= 0:
            raise ValueError("Cantidad calculada invalida")
        if bool(runtime.get("signal_only_mode", 1)):
            return {"status": "signal_only", "qty": qty, "limit_price": limit_price}

        order = self.order_manager.create_limit_order(symbol=str(signal["symbol"]), qty=qty, side="buy", limit_price=limit_price, time_in_force="gtc")
        self.database.insert_trade(
            {
                "timestamp": self._now_iso(),
                "account_id": account_id,
                "symbol": str(signal["symbol"]),
                "asset_type": str(signal["asset_type"]),
                "side": "buy",
                "order_type": "limit",
                "qty": qty,
                "limit_price": limit_price,
                "filled_price": float(order.get("filled_avg_price", 0.0) or 0.0),
                "fees": 0.0,
                "status": str(order.get("status", "submitted")),
                "broker_order_id": str(order.get("id", "")),
                "signal_id": int(signal_id),
                "created_at": self._now_iso(),
            }
        )
        funds["capital_used"] = float(funds.get("capital_used", 0.0) or 0.0) + (qty * limit_price)
        funds["available_capital"] = max(float(funds.get("max_capital_assigned", 0.0) or 0.0) - float(funds["capital_used"]), 0.0)
        self.database.upsert_bot_funds(
            account_id=account_id,
            max_capital_assigned=float(funds.get("max_capital_assigned", 0.0) or 0.0),
            available_capital=float(funds["available_capital"]),
            capital_used=float(funds["capital_used"]),
            max_position_size=float(funds.get("max_position_size", 0.0) or 0.0),
            max_daily_loss=float(funds.get("max_daily_loss", 0.0) or 0.0),
            enabled=bool(funds.get("enabled", 1)),
        )
        return {"status": "submitted", "order": order, "qty": qty, "limit_price": limit_price}

    def place_limit_sell_if_allowed(self, symbol: str, account_name: str, manual_approved: bool = True) -> dict[str, Any]:
        account = self.refresh_account_context(account_name)
        account_id = int(account["id"])
        runtime = self.database.get_runtime_settings(account_id) or {}
        if bool(runtime.get("kill_switch", 0)):
            raise ValueError("Kill switch activo")
        if bool(runtime.get("manual_approval_required", 1)) and not manual_approved:
            raise ValueError("Se requiere aprobacion manual")

        self.sync_positions(account_name)
        position = next((row for row in self.database.list_positions(account_id) if self._symbol_key(str(row["symbol"])) == self._symbol_key(symbol)), None)
        if position is None or float(position.get("qty", 0.0) or 0.0) <= 0:
            raise ValueError("No hay posicion activa para vender")

        current_price = float(position["current_price"])
        average_cost = float(position["average_cost"])
        min_sell_price = self._minimum_sell_price(average_cost)
        if not self._is_sell_allowed(current_price=current_price, average_cost=average_cost):
            self.database.insert_decision_log(
                {
                    "timestamp": self._now_iso(),
                    "symbol": str(position["symbol"]),
                    "decision": "HOLD",
                    "reason": "Venta automatica bloqueada por average cost",
                    "blocked_reason": "below_average_cost_or_buffer",
                    "raw_context_json": {"position": position, "min_sell_price": min_sell_price},
                }
            )
            return {"status": "blocked", "position": position, "min_sell_price": min_sell_price}

        limit_price = max(current_price, min_sell_price)
        if bool(runtime.get("signal_only_mode", 1)):
            return {"status": "signal_only", "qty": float(position["qty"]), "limit_price": limit_price}

        order = self.order_manager.create_limit_order(
            symbol=str(position["symbol"]),
            qty=float(position["qty"]),
            side="sell",
            limit_price=limit_price,
            time_in_force="gtc",
        )
        self.database.insert_trade(
            {
                "timestamp": self._now_iso(),
                "account_id": account_id,
                "symbol": str(position["symbol"]),
                "asset_type": str(position["asset_type"]),
                "side": "sell",
                "order_type": "limit",
                "qty": float(position["qty"]),
                "limit_price": limit_price,
                "filled_price": float(order.get("filled_avg_price", 0.0) or 0.0),
                "fees": 0.0,
                "status": str(order.get("status", "submitted")),
                "broker_order_id": str(order.get("id", "")),
                "signal_id": None,
                "created_at": self._now_iso(),
            }
        )
        return {"status": "submitted", "order": order, "limit_price": limit_price}

    def _build_features(self, symbol: str, asset_type: str, account_id: int) -> dict[str, Any]:
        candles_1m = self.market_data.get_candles(symbol=symbol, interval="1m", limit=60)
        candles_5m = self.market_data.get_candles(symbol=symbol, interval="5m", limit=60)
        candles_15m = self.market_data.get_candles(symbol=symbol, interval="15m", limit=60)
        price = float(self.market_data.get_last_price(symbol))
        quote = self.market_data.get_latest_quote(symbol)
        metrics = self.calculate_average_cost(symbol=symbol, account_id=account_id)
        average_cost = float(metrics["average_cost"])
        rsi = self._calc_rsi(candles_1m)
        atr = self._calc_atr(candles_1m)
        volume = float(candles_1m[-1].get("volume", 0.0) or 0.0) if candles_1m else 0.0
        avg_volume = sum(float(c.get("volume", 0.0) or 0.0) for c in candles_1m[-20:]) / max(len(candles_1m[-20:]), 1)
        vwap = float(self.market_data.calculate_vwap(candles_1m)) if candles_1m else price
        spread = float(quote.get("spread", 0.0) or 0.0)
        percent_change_1m = self._pct_change(candles_1m, 1)
        percent_change_5m = self._pct_change(candles_5m, 5)
        percent_change_15m = self._pct_change(candles_15m, 15)
        distance_from_average_cost = ((price - average_cost) / average_cost) if average_cost > 0 else 1.0
        market_session = "crypto" if asset_type == "crypto" else "regular"
        return {
            "asset_type": asset_type,
            "price": price,
            "volume": volume,
            "vwap": vwap,
            "rsi": rsi,
            "atr": atr,
            "spread": spread,
            "percent_change_1m": percent_change_1m,
            "percent_change_5m": percent_change_5m,
            "percent_change_15m": percent_change_15m,
            "price_above_vwap": price > vwap,
            "volume_spike_score": min(volume / max(avg_volume, 1.0), 3.0) / 3.0,
            "news_sentiment_score": 0.0,
            "news_importance_score": 0.0,
            "news_risk_score": 0.0,
            "market_session": market_session,
            "existing_position": metrics["quantity_owned"] > 0,
            "distance_from_average_cost": distance_from_average_cost,
            "liquidity_score": max(0.0, 1.0 - min(spread / max(price, 1e-8), 0.05) / 0.05),
            "average_cost": average_cost,
        }

    def _collector_interval_seconds(self) -> float:
        # Fast loop with per-symbol throttling for crypto/stocks.
        return 10.0

    def _news_interval_seconds(self) -> float:
        configured = float(getattr(self.settings, "ai_news_social_interval_seconds", 180) or 180)
        return max(60.0, min(300.0, configured))

    def _collector_cycle(self) -> None:
        self._reset_daily_counters_if_needed()
        with self._worker_lock:
            account_name = self._active_account_for_workers
        if not account_name:
            return

        symbols = self._symbols_for_collection(account_name)
        collected = 0
        now_iso = self._now_iso()
        for item in symbols:
            symbol = str(item.get("symbol", "")).upper()
            asset_type = str(item.get("asset_type", "stock"))
            if not symbol:
                continue
            if not self._should_collect_symbol(symbol=symbol, asset_type=asset_type):
                continue
            try:
                candles_1m = self.market_data.get_candles(symbol=symbol, interval="1m", limit=60)
                candles_5m = self.market_data.get_candles(symbol=symbol, interval="5m", limit=60)
                candles_15m = self.market_data.get_candles(symbol=symbol, interval="15m", limit=60)
                price = float(self.market_data.get_last_price(symbol))
                quote = self.market_data.get_latest_quote(symbol)
                self._api_calls_today += 5

                base = candles_1m[-1] if candles_1m else {}
                payload = {
                    "timestamp": now_iso,
                    "symbol": symbol,
                    "asset_type": asset_type,
                    "price": price,
                    "open": float(base.get("open", price) or price),
                    "high": float(base.get("high", price) or price),
                    "low": float(base.get("low", price) or price),
                    "close": float(base.get("close", price) or price),
                    "volume": float(base.get("volume", 0.0) or 0.0),
                    "vwap": float(self.market_data.calculate_vwap(candles_1m)) if candles_1m else price,
                    "rsi": self._calc_rsi(candles_1m),
                    "atr": self._calc_atr(candles_1m),
                    "spread": float(quote.get("spread", 0.0) or 0.0),
                    "percent_change_1m": self._pct_change(candles_1m, 1),
                    "percent_change_5m": self._pct_change(candles_5m, 5),
                    "percent_change_15m": self._pct_change(candles_15m, 15),
                    "source": "worker",
                }
                self.database.insert_market_snapshot(payload)
                self._last_collected_at_by_symbol[self._symbol_key(symbol)] = time.monotonic()
                self._mark_api_recovered()
                collected += 1
            except Exception as ex:
                self._last_api_error = str(ex)
                self.logger.warning("DataCollectorWorker error %s: %s", symbol, ex)

        self._last_collector_cycle_at = now_iso
        self.database.insert_decision_log(
            {
                "timestamp": now_iso,
                "symbol": "*",
                "decision": "COLLECT",
                "reason": f"snapshots={collected}",
                "blocked_reason": "",
                "raw_context_json": {"symbols": len(symbols), "collected": collected},
            }
        )

    def _news_social_cycle(self) -> None:
        self._reset_daily_counters_if_needed()
        with self._worker_lock:
            account_name = self._active_account_for_workers
        if not account_name:
            return

        symbols = self._symbols_for_collection(account_name)[:12]
        now_iso = self._now_iso()
        inserted = 0
        for item in symbols:
            symbol = str(item.get("symbol", "")).upper().strip()
            asset_type = str(item.get("asset_type", "stock")).lower()
            if not symbol:
                continue
            try:
                events = self._fetch_news_social_events(symbol=symbol, asset_type=asset_type)
                for event in events:
                    title_or_text = str(event.get("title_or_text", "")).strip()
                    if not title_or_text:
                        continue
                    source = str(event.get("source", "news"))
                    url = str(event.get("url", ""))
                    author = str(event.get("author", ""))
                    signature = self._news_signature(symbol=symbol, source=source, title_or_text=title_or_text, url=url)
                    if self._is_duplicate_news_signature(signature):
                        continue

                    quick = self._quick_news_assessment(title_or_text)
                    strong_event = quick["importance_score"] >= float(getattr(self.settings, "ai_openai_news_trigger_importance", 65.0) or 65.0) or quick["risk_score"] >= 70
                    analysis = {
                        "symbol": symbol,
                        "asset_type": asset_type,
                        "sentiment": quick["sentiment"],
                        "event_type": quick["event_type"],
                        "importance_score": quick["importance_score"],
                        "risk_score": quick["risk_score"],
                        "summary": title_or_text[:180],
                        "possible_market_impact": "Potential impact detected" if strong_event else "Low impact",
                        "action_bias": "neutral",
                    }

                    if strong_event and bool(self.settings.openai_api_key):
                        self._openai_calls_today += 1
                        self._last_openai_call_at = now_iso
                        analysis = self.openai_analyzer.analyze_text(
                            symbol=symbol,
                            asset_type=asset_type,
                            text=title_or_text,
                            source=source,
                            author=author,
                            context={"account_name": account_name, "origin": "news_worker"},
                        )

                    self.database.insert_news_event(
                        {
                            "timestamp": now_iso,
                            "symbol": symbol,
                            "asset_type": asset_type,
                            "source": source,
                            "title_or_text": title_or_text,
                            "url": url,
                            "author": author,
                            "influence_score": float(analysis.get("importance_score", 0.0) or 0.0) / 100.0,
                            "sentiment_score": self._sentiment_to_float(str(analysis.get("sentiment", "neutral"))),
                            "ai_summary": str(analysis.get("summary", "")),
                            "ai_classification": str(analysis.get("event_type", "other")),
                            "raw_payload": analysis,
                        }
                    )
                    inserted += 1
                    self._mark_news_signature_seen(signature)
                    if strong_event:
                        self.database.insert_decision_log(
                            {
                                "timestamp": now_iso,
                                "symbol": symbol,
                                "decision": "NEWS",
                                "reason": f"source={source}; importance={analysis.get('importance_score', 0)}; risk={analysis.get('risk_score', 0)}",
                                "blocked_reason": "",
                                "raw_context_json": {
                                    "title": title_or_text,
                                    "source": source,
                                    "url": url,
                                },
                            }
                        )
            except Exception as ex:
                self._last_api_error = str(ex)
                self.logger.warning("NewsSocialCollectorWorker error %s: %s", symbol, ex)

        self._last_news_cycle_at = now_iso
        if inserted > 0:
            self.database.insert_decision_log(
                {
                    "timestamp": now_iso,
                    "symbol": "*",
                    "decision": "NEWS_COLLECT",
                    "reason": f"events={inserted}",
                    "blocked_reason": "",
                    "raw_context_json": {"events": inserted},
                }
            )

    def _scanner_cycle(self) -> None:
        self._reset_daily_counters_if_needed()
        with self._worker_lock:
            account_name = self._active_account_for_workers
        if not account_name:
            return
        account = self.refresh_account_context(account_name)
        account_id = int(account["id"])
        symbols = self._symbols_for_collection(account_name)
        for item in symbols:
            symbol = str(item.get("symbol", "")).upper()
            asset_type = str(item.get("asset_type", "stock"))
            snapshots = self.database.latest_market_snapshots(symbol=symbol, limit=120)
            if not snapshots:
                continue

            latest = snapshots[0]
            latest_price = float(latest.get("price", 0.0) or 0.0)
            if latest_price <= 0:
                self.database.insert_decision_log(
                    {
                        "timestamp": self._now_iso(),
                        "symbol": symbol,
                        "decision": "SKIP",
                        "reason": "snapshot sin precio valido",
                        "blocked_reason": "invalid_price",
                        "raw_context_json": {"asset_type": asset_type, "snapshot": latest},
                    }
                )
                continue

            volume_1m_current = float(latest.get("volume", 0.0) or 0.0)
            volume_1m_previous_closed = 0.0
            for row in snapshots[1:]:
                candidate = float(row.get("volume", 0.0) or 0.0)
                if candidate > 0:
                    volume_1m_previous_closed = candidate
                    break

            closed_1m = [float(row.get("volume", 0.0) or 0.0) for row in snapshots[1:61]]
            volume_5m_sum = sum(closed_1m[:5])
            volume_15m_sum = sum(closed_1m[:15])
            avg_volume_1m_20 = sum(closed_1m[:20]) / max(len(closed_1m[:20]), 1)

            try:
                candles_5m = self.market_data.get_candles(symbol=symbol, interval="5m", limit=20)
                self._api_calls_today += 1
                self._mark_api_recovered()
            except Exception as ex:
                candles_5m = []
                self._last_api_error = str(ex)
            candle_5m_volumes = [float(candle.get("volume", 0.0) or 0.0) for candle in candles_5m]
            avg_volume_5m_20 = sum(candle_5m_volumes[-20:]) / max(len(candle_5m_volumes[-20:]), 1)

            relative_volume_1m = (volume_1m_previous_closed / avg_volume_1m_20) if avg_volume_1m_20 > 0 else 0.0
            relative_volume_5m = (volume_5m_sum / avg_volume_5m_20) if avg_volume_5m_20 > 0 else 0.0
            dollar_volume_5m = latest_price * volume_5m_sum

            now_iso = self._now_iso()
            day_ago_iso = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
            snapshots_24h = self.database.market_snapshots_between(symbol=symbol, start_iso=day_ago_iso, end_iso=now_iso)
            if snapshots_24h:
                volume_24h_usd = sum(float(row.get("price", 0.0) or 0.0) * float(row.get("volume", 0.0) or 0.0) for row in snapshots_24h)
            else:
                volume_24h_usd = 0.0

            if volume_1m_current <= 0 and volume_1m_previous_closed > 0:
                # If current candle is empty/incomplete, fallback to latest closed candle volume.
                volume_1m_current = volume_1m_previous_closed

            if volume_1m_current <= 0 and asset_type.lower() == "crypto" and volume_5m_sum > 0:
                # Crypto fallback: allow using 5m aggregate when 1m is temporarily empty.
                volume_1m_current = volume_5m_sum / 5.0

            blocked_reasons: list[str] = []
            if volume_1m_previous_closed <= 0:
                blocked_reasons.append("Sin vela cerrada válida")
            if volume_1m_current <= 0 and volume_5m_sum <= 0 and volume_15m_sum <= 0:
                blocked_reasons.append("Volumen inválido")

            min_volume_24h_usd = float(getattr(self.settings, "ai_min_volume_24h_usd", 100000.0) or 100000.0)
            if volume_24h_usd > 0 and volume_24h_usd < min_volume_24h_usd:
                blocked_reasons.append("Liquidez insuficiente")

            if blocked_reasons and (volume_1m_current <= 0 and volume_5m_sum <= 0 and volume_15m_sum <= 0):
                self.database.insert_decision_log(
                    {
                        "timestamp": now_iso,
                        "symbol": symbol,
                        "decision": "SKIP",
                        "reason": "blocked_reason: " + " | ".join(blocked_reasons),
                        "blocked_reason": "invalid_volume",
                        "raw_context_json": {
                            "asset_type": asset_type,
                            "volume_1m_current": volume_1m_current,
                            "volume_1m_previous_closed": volume_1m_previous_closed,
                            "volume_5m_sum": volume_5m_sum,
                            "volume_15m_sum": volume_15m_sum,
                        },
                    }
                )
                continue

            rv_boost_1m = max(0.0, min(1.5, relative_volume_1m))
            rv_boost_5m = max(0.0, min(1.5, relative_volume_5m))
            volume_score = max(0.0, min(25.0, ((rv_boost_1m * 0.6) + (rv_boost_5m * 0.4)) * 16.0))
            price_action_score = max(
                0.0,
                min(
                    35.0,
                    15.0
                    + float(latest.get("percent_change_1m", 0.0) or 0.0) * 4.0
                    + float(latest.get("percent_change_5m", 0.0) or 0.0) * 2.0,
                ),
            )
            spread = float(latest.get("spread", 0.0) or 0.0)
            price = float(latest.get("price", 0.0) or 0.0)
            liquidity_score = max(0.0, min(15.0, (1.0 - min(spread / max(price, 1e-8), 0.05) / 0.05) * 15.0))
            if spread > float(self.settings.ai_max_spread_allowed):
                blocked_reasons.append("Spread demasiado alto")

            recent_news = self.database.list_news_events_since(
                since_iso=(datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
                symbol=symbol,
                limit=20,
            )
            if recent_news:
                top_news_influence = max(float(row.get("influence_score", 0.0) or 0.0) for row in recent_news)
                news_score = max(0.0, min(20.0, top_news_influence * 20.0))
            else:
                news_score = 0.0

            risk_score = max(
                0.0,
                min(
                    15.0,
                    15.0 - abs(float(latest.get("rsi", 50.0) or 50.0) - 50.0) / 3.5,
                ),
            )
            final_score = round(min(100.0, price_action_score + volume_score + liquidity_score + news_score + risk_score), 2)
            if final_score < 45:
                blocked_reasons.append("Score menor al mínimo")
            if final_score < 45:
                action = "AVOID"
            elif final_score < 60:
                action = "WATCH"
            elif final_score < 75:
                action = "BUY_SMALL"
            else:
                action = "BUY"

            metrics = self.calculate_average_cost(symbol=symbol, account_id=account_id)
            average_cost = float(metrics.get("average_cost", 0.0) or 0.0)
            min_sell_price = average_cost + float(self.settings.ai_fees_buffer) + float(self.settings.ai_slippage_buffer) + float(self.settings.ai_minimum_profit)
            reason = (
                f"price_action={price_action_score:.2f}; volume={volume_score:.2f}; liquidity={liquidity_score:.2f}; "
                f"news={news_score:.2f}; risk={risk_score:.2f}; final={final_score:.2f}; "
                f"volume_1m_current={volume_1m_current:.2f}; volume_1m_previous_closed={volume_1m_previous_closed:.2f}; "
                f"volume_5m_sum={volume_5m_sum:.2f}; volume_15m_sum={volume_15m_sum:.2f}; "
                f"avg_volume_1m_20={avg_volume_1m_20:.2f}; avg_volume_5m_20={avg_volume_5m_20:.2f}; "
                f"relative_volume_1m={relative_volume_1m:.2f}; relative_volume_5m={relative_volume_5m:.2f}; "
                f"dollar_volume_5m={dollar_volume_5m:.2f}; volume_24h_usd={volume_24h_usd:.2f}"
            )
            if blocked_reasons:
                reason += " | blocked_reason: " + " | ".join(dict.fromkeys(blocked_reasons))
            if average_cost > 0 and price < average_cost:
                action = "HOLD"
                reason += " | blocked_reason=below_average_cost"
                self.database.insert_decision_log(
                    {
                        "timestamp": self._now_iso(),
                        "symbol": symbol,
                        "decision": "HOLD",
                        "reason": "Venta automatica bloqueada por average cost",
                        "blocked_reason": "below_average_cost",
                        "raw_context_json": {"price": price, "average_cost": average_cost},
                    }
                )
            elif average_cost > 0 and price >= min_sell_price:
                action = "SELL_ALLOWED"

            features = {
                "price": price,
                "volume": volume_1m_current,
                "vwap": float(latest.get("vwap", price) or price),
                "rsi": float(latest.get("rsi", 50.0) or 50.0),
                "atr": float(latest.get("atr", 0.0) or 0.0),
                "spread": spread,
                "percent_change_1m": float(latest.get("percent_change_1m", 0.0) or 0.0),
                "percent_change_5m": float(latest.get("percent_change_5m", 0.0) or 0.0),
                "percent_change_15m": float(latest.get("percent_change_15m", 0.0) or 0.0),
                "price_action_score": price_action_score,
                "volume_score": volume_score,
                "liquidity_score": liquidity_score,
                "news_score": news_score,
                "risk_score": risk_score,
                "composite_score": final_score,
                "average_cost": average_cost,
                "volume_1m_current": volume_1m_current,
                "volume_1m_previous_closed": volume_1m_previous_closed,
                "volume_5m_sum": volume_5m_sum,
                "volume_15m_sum": volume_15m_sum,
                "avg_volume_1m_20": avg_volume_1m_20,
                "avg_volume_5m_20": avg_volume_5m_20,
                "relative_volume_1m": relative_volume_1m,
                "relative_volume_5m": relative_volume_5m,
                "dollar_volume_5m": dollar_volume_5m,
                "volume_24h_usd": volume_24h_usd,
            }

            openai_analysis: dict[str, Any] = {"summary": "worker_scan", "score": final_score}
            signal_trigger = final_score >= float(getattr(self.settings, "ai_openai_signal_trigger_score", 85.0) or 85.0)
            has_relevant_news = bool(recent_news) and news_score >= 10.0
            if (signal_trigger or has_relevant_news) and bool(self.settings.openai_api_key):
                self._openai_calls_today += 1
                self._last_openai_call_at = self._now_iso()
                openai_analysis = self.openai_analyzer.analyze_text(
                    symbol=symbol,
                    asset_type=asset_type,
                    text=(
                        f"Worker signal summary for {symbol}: action={action}, final_score={final_score:.2f}, "
                        f"price_action={price_action_score:.2f}, volume_score={volume_score:.2f}, news_score={news_score:.2f}."
                    ),
                    source="worker_signal",
                    context={"account_name": account_name, "reason": reason},
                )
            self.database.insert_signal(
                {
                    "timestamp": self._now_iso(),
                    "symbol": symbol,
                    "asset_type": asset_type,
                    "signal_type": action,
                    "confidence_score": final_score,
                    "model_version": "worker_scanner_v1",
                    "reason": reason,
                    "entry_price": price,
                    "suggested_limit_price": price,
                    "invalidation_price": max(price - float(latest.get("atr", 0.0) or 0.0), 0.0),
                    "take_profit_price": max(price + float(self.settings.ai_minimum_profit), min_sell_price if min_sell_price > 0 else price),
                    "risk_level": self._risk_label(100.0 - final_score),
                    "features_json": features,
                    "openai_analysis_json": openai_analysis,
                }
            )

            self.database.insert_decision_log(
                {
                    "timestamp": self._now_iso(),
                    "symbol": symbol,
                    "decision": action,
                    "reason": reason,
                    "blocked_reason": " | ".join(dict.fromkeys(blocked_reasons)),
                    "raw_context_json": {"final_score": final_score},
                }
            )

    def _labeler_cycle(self) -> None:
        now = datetime.now(timezone.utc)
        since = (now - timedelta(hours=2)).isoformat()
        signals = self.database.list_signals_since(since_iso=since)
        windows = [5, 15, 30, 60]
        for signal in signals:
            signal_id = int(signal.get("id", 0) or 0)
            if signal_id <= 0:
                continue
            symbol = str(signal.get("symbol", "")).upper()
            timestamp = str(signal.get("timestamp", ""))
            if not symbol or not timestamp:
                continue
            try:
                signal_time = datetime.fromisoformat(timestamp)
            except ValueError:
                continue

            entry_price = float(signal.get("entry_price", 0.0) or 0.0)
            if entry_price <= 0:
                continue

            outcome = self.database.get_signal_outcome(signal_id) or {}
            updates: dict[str, Any] = {}
            for window in windows:
                result_key = f"result_{window}m"
                if outcome.get(result_key):
                    continue
                if now < signal_time + timedelta(minutes=window):
                    continue
                rows = self.database.market_snapshots_between(
                    symbol=symbol,
                    start_iso=signal_time.isoformat(),
                    end_iso=(signal_time + timedelta(minutes=window)).isoformat(),
                )
                if not rows:
                    continue
                profits = [((float(row.get("high", entry_price) or entry_price) - entry_price) / entry_price) * 100.0 for row in rows]
                drawdowns = [((float(row.get("low", entry_price) or entry_price) - entry_price) / entry_price) * 100.0 for row in rows]
                max_profit = max(profits)
                max_drawdown = min(drawdowns)
                if max_profit >= 0.25:
                    result = "win"
                elif max_drawdown <= -0.25:
                    result = "loss"
                else:
                    result = "neutral"
                updates[f"max_profit_{window}m"] = round(max_profit, 6)
                updates[f"max_drawdown_{window}m"] = round(max_drawdown, 6)
                updates[result_key] = result
            if updates:
                self.database.upsert_signal_outcome(signal_id=signal_id, symbol=symbol, updates=updates)

    def _training_cycle(self) -> None:
        self._reset_daily_counters_if_needed()
        if self._training_interval_seconds() <= 0:
            self._last_training_cycle_at = f"{self._now_iso()} | modo manual"
            return
        evaluated = self.database.count_evaluated_outcomes()
        latest_training = self.database.latest_training_run() or {}
        latest_samples = int(latest_training.get("number_of_samples", 0) or 0)
        now_iso = self._now_iso()

        if evaluated < 200:
            self._last_training_cycle_at = f"{now_iso} | esperando {evaluated}/200 outcomes"
            return
        if evaluated <= max(self._last_auto_trained_outcomes, latest_samples):
            self._last_training_cycle_at = f"{now_iso} | sin datos nuevos para reentrenar ({evaluated})"
            return

        result = self.trainer.train_general_model()
        if bool(result.get("trained", False)):
            self._last_auto_trained_outcomes = evaluated
            self._last_training_cycle_at = f"{now_iso} | modelo {result.get('model_version', 'N/A')}"
            self.database.insert_decision_log(
                {
                    "timestamp": now_iso,
                    "symbol": "*",
                    "decision": "AUTO_TRAIN",
                    "reason": f"model={result.get('model_version', 'N/A')} samples={result.get('number_of_samples', 0)} outcomes={evaluated}",
                    "blocked_reason": "",
                    "raw_context_json": result,
                }
            )
            return

        self._last_training_cycle_at = f"{now_iso} | {result.get('reason', 'sin entrenamiento')}"

    def _training_interval_seconds(self) -> float:
        if bool(getattr(self.settings, "ai_dev_mode", False)):
            return 300.0
        mode = str(getattr(self.settings, "ai_auto_train_mode", "12h") or "12h").strip().lower()
        mapping = {
            "manual": 0.0,
            "6h": 6.0 * 3600.0,
            "12h": 12.0 * 3600.0,
            "24h": 24.0 * 3600.0,
            "dev": 300.0,
        }
        return float(mapping.get(mode, 12.0 * 3600.0))

    def _symbols_for_collection(self, account_name: str) -> list[dict[str, str]]:
        symbols: dict[str, dict[str, str]] = {}

        for asset in self.database.list_watchlist_assets(active_only=True):
            symbol = str(asset.get("symbol", "")).upper().strip()
            if not symbol:
                continue
            symbols[self._symbol_key(symbol)] = {
                "symbol": symbol,
                "asset_type": str(asset.get("asset_type", "stock")),
            }

        try:
            for position in self.broker.get_positions():
                symbol = str(position.get("symbol", "")).upper().strip()
                if not symbol:
                    continue
                symbols[self._symbol_key(symbol)] = {
                    "symbol": symbol,
                    "asset_type": "crypto" if "/" in symbol or symbol.endswith("USD") else "stock",
                }
                self._api_calls_today += 1
        except Exception as ex:
            self._last_api_error = str(ex)

        default_symbol = str(self.settings.default_symbol).upper().strip()
        if default_symbol:
            symbols.setdefault(
                self._symbol_key(default_symbol),
                {
                    "symbol": default_symbol,
                    "asset_type": "crypto" if "/" in default_symbol or default_symbol.endswith("USD") else "stock",
                },
            )

        return list(symbols.values())

    def _should_collect_symbol(self, symbol: str, asset_type: str) -> bool:
        key = self._symbol_key(symbol)
        now_monotonic = time.monotonic()
        previous = self._last_collected_at_by_symbol.get(key, 0.0)

        if asset_type.lower() == "crypto":
            interval = max(10.0, min(30.0, float(getattr(self.settings, "ai_crypto_collection_interval_seconds", 20) or 20)))
        else:
            is_open = self._is_stock_market_open_cached()
            open_interval = max(30.0, min(60.0, float(getattr(self.settings, "ai_stock_open_collection_interval_seconds", 45) or 45)))
            closed_interval = max(float(getattr(self.settings, "ai_stock_closed_collection_interval_seconds", 240) or 240), 60.0)
            interval = open_interval if is_open else closed_interval

        return (now_monotonic - previous) >= interval

    def _is_stock_market_open_cached(self) -> bool:
        now_monotonic = time.monotonic()
        if (now_monotonic - self._market_open_checked_at) <= 60.0:
            return self._market_open_cached
        try:
            clock = self.broker.get_clock()
            self._api_calls_today += 1
            self._market_open_cached = bool(clock.get("is_open", False))
            self._mark_api_recovered()
        except Exception as ex:
            self._last_api_error = str(ex)
            # On transient API failures, keep last known value to avoid noisy toggling.
        self._market_open_checked_at = now_monotonic
        return self._market_open_cached

    def _fetch_news_social_events(self, symbol: str, asset_type: str) -> list[dict[str, str]]:
        if asset_type == "crypto":
            events = self._fetch_cryptopanic_events(symbol)
            if events:
                return events
        return self._fetch_yahoo_finance_events(symbol)

    def _fetch_cryptopanic_events(self, symbol: str) -> list[dict[str, str]]:
        token = str(getattr(self.settings, "cryptopanic_api_key", "") or "").strip()
        if not token:
            return []
        query_currency = symbol.replace("/", "").replace("USD", "").upper().strip()
        if not query_currency:
            return []
        try:
            response = requests.get(
                "https://cryptopanic.com/api/v1/posts/",
                params={
                    "auth_token": token,
                    "currencies": query_currency,
                    "public": "true",
                    "kind": "news",
                    "filter": "hot",
                },
                timeout=20,
            )
            response.raise_for_status()
            self._api_calls_today += 1
            payload = response.json()
            self._mark_api_recovered()
            result: list[dict[str, str]] = []
            for row in payload.get("results", [])[:4]:
                result.append(
                    {
                        "source": "cryptopanic",
                        "title_or_text": str(row.get("title", "") or "").strip(),
                        "url": str(row.get("url", "") or "").strip(),
                        "author": str(row.get("domain", "") or "").strip(),
                    }
                )
            return result
        except Exception as ex:
            if not self._is_rate_limit_error(ex):
                self._last_api_error = str(ex)
            return []

    def _fetch_yahoo_finance_events(self, symbol: str) -> list[dict[str, str]]:
        key = self._symbol_key(symbol)
        now_monotonic = time.monotonic()
        cached = self._news_fetch_cache_by_symbol.get(key)
        if cached is not None:
            cached_at, cached_events = cached
            if (now_monotonic - cached_at) <= 1800.0:
                return cached_events

        cooldown_until = self._news_fetch_cooldown_until_by_symbol.get(key, 0.0)
        if now_monotonic < cooldown_until:
            return cached[1] if cached is not None else []

        feed_url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=US&lang=en-US"
        try:
            response = requests.get(
                feed_url,
                timeout=20,
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; AlpacaTradingBot/1.0)",
                    "Accept": "application/rss+xml,application/xml,text/xml;q=0.9,*/*;q=0.8",
                },
            )
            if response.status_code == 429:
                self._news_fetch_cooldown_until_by_symbol[key] = now_monotonic + 7200.0
                self.logger.warning("Yahoo RSS rate limited for %s. Cooling down for 2h.", symbol)
                return cached[1] if cached is not None else []
            response.raise_for_status()
            self._api_calls_today += 1
            root = ElementTree.fromstring(response.text)
            self._mark_api_recovered()
            result: list[dict[str, str]] = []
            for item in root.findall(".//item")[:5]:
                title = str(item.findtext("title", default="") or "").strip()
                link = str(item.findtext("link", default="") or "").strip()
                author = str(item.findtext("source", default="") or item.findtext("pubDate", default="") or "").strip()
                if not title:
                    continue
                result.append(
                    {
                        "source": "yahoo_finance_rss",
                        "title_or_text": title,
                        "url": link,
                        "author": author,
                    }
                )
            self._news_fetch_cache_by_symbol[key] = (now_monotonic, result)
            return result
        except Exception as ex:
            if self._is_rate_limit_error(ex):
                self._news_fetch_cooldown_until_by_symbol[key] = now_monotonic + 7200.0
                self.logger.warning("Yahoo RSS rate limited for %s. Using cached news if available.", symbol)
                return cached[1] if cached is not None else []
            self._last_api_error = str(ex)
            return []

    def _news_signature(self, symbol: str, source: str, title_or_text: str, url: str) -> str:
        key_base = f"{self._symbol_key(symbol)}|{source.strip().lower()}|{title_or_text.strip().lower()}|{url.strip().lower()}"
        return str(abs(hash(key_base)))

    def _is_duplicate_news_signature(self, signature: str) -> bool:
        now_monotonic = time.monotonic()
        self._seen_news_signatures = {
            key: seen_at
            for key, seen_at in self._seen_news_signatures.items()
            if (now_monotonic - seen_at) <= 21600.0
        }
        return signature in self._seen_news_signatures

    def _mark_news_signature_seen(self, signature: str) -> None:
        self._seen_news_signatures[signature] = time.monotonic()

    @staticmethod
    def _quick_news_assessment(text: str) -> dict[str, Any]:
        normalized = str(text or "").lower()
        importance = 20
        risk = 15
        sentiment = "neutral"
        event_type = "news"

        positive_terms = ["beats", "approval", "partnership", "upgrade", "record revenue", "surge", "breakout", "adoption"]
        negative_terms = ["downgrade", "investigation", "lawsuit", "hack", "breach", "default", "bankruptcy", "misses"]
        strong_terms = ["sec", "federal reserve", "earnings", "guidance", "etf", "liquidation", "delisting", "acquisition"]

        if any(term in normalized for term in positive_terms):
            sentiment = "positive"
            importance += 20
        if any(term in normalized for term in negative_terms):
            sentiment = "negative"
            importance += 20
            risk += 20
        if any(term in normalized for term in strong_terms):
            importance += 30
            risk += 10
            event_type = "macro_or_corporate"

        return {
            "sentiment": sentiment,
            "event_type": event_type,
            "importance_score": int(max(0, min(100, importance))),
            "risk_score": int(max(0, min(100, risk))),
        }

    def _reset_daily_counters_if_needed(self) -> None:
        today = datetime.now(timezone.utc).date().isoformat()
        if self._stats_day == today:
            return
        self._stats_day = today
        self._api_calls_today = 0
        self._openai_calls_today = 0

    def _blocked_buy_reason(self, symbol: str, account_id: int, features: dict[str, Any], account_name: str) -> str:
        funds = self.database.get_bot_funds(account_id) or {}
        runtime = self.database.get_runtime_settings(account_id) or {}
        if bool(runtime.get("kill_switch", 0)):
            return "Kill switch activo"
        if not bool(funds.get("enabled", 0)):
            return "Bot desactivado"
        if float(features.get("spread", 0.0) or 0.0) > float(self.settings.ai_max_spread_allowed):
            return "Spread demasiado alto"
        if float(features.get("volume", 0.0) or 0.0) < float(self.settings.ai_min_volume_required):
            return "Volumen inválido"
        if float(funds.get("available_capital", 0.0) or 0.0) <= 0:
            return "No hay fondos asignados"
        if float(funds.get("max_position_size", 0.0) or 0.0) <= 0:
            return "Max position size invalido"
        if not self.risk_manager.can_trade(current_daily_pnl=float(self.position_manager.journal.get_daily_realized_pnl())):
            return "Se excede max_daily_loss"
        if not bool(runtime.get("paper_trading", 1)) and not bool(runtime.get("live_trading_enabled", 0)):
            return "Live trading esta apagado"
        asset_type = "crypto" if "/" in symbol or symbol.endswith("USD") else "stock"
        supported = self._is_broker_supported(symbol=symbol, asset_type=asset_type)
        if not supported:
            return "Activo no soportado en Alpaca"
        return ""

    def _minimum_sell_price(self, average_cost: float) -> float:
        if average_cost <= 0:
            return 0.0
        return average_cost + float(self.settings.ai_fees_buffer) + float(self.settings.ai_slippage_buffer) + float(self.settings.ai_minimum_profit)

    def _is_sell_allowed(self, current_price: float, average_cost: float) -> bool:
        if average_cost <= 0:
            return False
        return current_price >= self._minimum_sell_price(average_cost)

    def _is_broker_supported(self, symbol: str, asset_type: str) -> bool:
        try:
            assets = self.broker.list_cryptos(status="active", only_tradable=True) if asset_type == "crypto" else self.broker.list_stocks(status="active", only_tradable=True)
            self._mark_api_recovered()
        except Exception:
            return False
        target = self._symbol_key(symbol)
        return any(self._symbol_key(str(asset.get("symbol", ""))) == target for asset in assets)

    def _mark_api_recovered(self) -> None:
        if self._last_api_error:
            self.logger.info("API recuperada. Limpiando ultimo error: %s", self._last_api_error)
            self._last_api_error = ""

    @staticmethod
    def _is_rate_limit_error(error: Exception) -> bool:
        message = str(error)
        return "429" in message or "Too Many Requests" in message

    @staticmethod
    def _pct_change(candles: list[dict[str, Any]], periods: int) -> float:
        if len(candles) < 2:
            return 0.0
        current = float(candles[-1].get("close", 0.0) or 0.0)
        lookback = float(candles[max(0, len(candles) - periods - 1)].get("close", 0.0) or 0.0)
        if lookback <= 0:
            return 0.0
        return ((current - lookback) / lookback) * 100.0

    @staticmethod
    def _calc_rsi(candles: list[dict[str, Any]], period: int = 14) -> float:
        if len(candles) <= period:
            return 50.0
        gains = 0.0
        losses = 0.0
        closes = [float(c.get("close", 0.0) or 0.0) for c in candles[-(period + 1):]]
        for previous, current in zip(closes, closes[1:]):
            delta = current - previous
            if delta >= 0:
                gains += delta
            else:
                losses += abs(delta)
        if losses == 0:
            return 100.0 if gains > 0 else 50.0
        rs = (gains / period) / (losses / period)
        return 100.0 - (100.0 / (1.0 + rs))

    @staticmethod
    def _calc_atr(candles: list[dict[str, Any]], period: int = 14) -> float:
        if len(candles) <= 1:
            return 0.0
        ranges: list[float] = []
        sample = candles[-period:]
        previous_close = float(sample[0].get("close", 0.0) or 0.0)
        for candle in sample[1:]:
            high = float(candle.get("high", 0.0) or 0.0)
            low = float(candle.get("low", 0.0) or 0.0)
            tr = max(high - low, abs(high - previous_close), abs(low - previous_close))
            ranges.append(tr)
            previous_close = float(candle.get("close", previous_close) or previous_close)
        return sum(ranges) / max(len(ranges), 1)

    @staticmethod
    def _risk_label(risk_score: float) -> str:
        if risk_score >= 70.0:
            return "high"
        if risk_score >= 40.0:
            return "medium"
        return "low"

    @staticmethod
    def _sentiment_to_float(sentiment: str) -> float:
        normalized = str(sentiment).lower().strip()
        if normalized == "positive":
            return 1.0
        if normalized == "negative":
            return -1.0
        return 0.0

    @staticmethod
    def _symbol_key(symbol: str) -> str:
        return str(symbol or "").upper().replace(" ", "").replace("/", "")

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()
