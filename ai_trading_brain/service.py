from __future__ import annotations

import calendar
import json
import math
from collections import deque
import os
import shutil
import subprocess
import threading
import time
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path
import resource
from typing import Any
from xml.etree import ElementTree

import requests

from ai_trading_brain.openai_analyzer import OpenAIAnalyzer
from ai_trading_brain.crypto_volume_manager import CryptoVolumeManager
from ai_trading_brain.scoring import compute_composite_score
from ai_trading_brain.workers import AutoTradingController, DataCollectorWorker, ModelTrainerWorker, NewsSocialCollectorWorker, OutcomeLabelerWorker, SignalScannerWorker
from database.manager import TradingBrainDatabase
from ml_model.model_registry import ModelRegistry
from ml_model.model_trainer import ModelTrainer
from ml_model.predictor import SignalPredictor
from runtime.db_writer import DatabaseWriterWorker
from runtime.alpaca_streams import AlpacaStreamManager
from runtime.thread_manager import ThreadManager


class NullStreamManager:
    def __init__(self) -> None:
        self.connected = False

    def start(self, account_name: str) -> None:
        _ = account_name
        self.connected = False

    def stop(self) -> None:
        self.connected = False

    def reconnect_now(self) -> None:
        self.connected = False

    def status_snapshot(self) -> dict[str, Any]:
        return {"status": "disabled", "connected": False, "provider": "binance"}


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
        self.trainer = ModelTrainer(database=self.database, registry=self.registry, logger=logger, settings=settings)
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
        self._diagnostic_signal_refresh_at_by_key: dict[str, float] = {}
        self._stream_bar_history_by_symbol: dict[str, deque[dict[str, Any]]] = {}
        self._global_crypto_cache_by_symbol: dict[str, dict[str, Any]] = {}
        self._focus_by_account: dict[str, dict[str, Any]] = {}
        self._recent_api_calls: deque[dict[str, Any]] = deque(maxlen=500)
        self._recent_errors: deque[dict[str, Any]] = deque(maxlen=500)
        self._last_health_snapshot: dict[str, Any] = {}
        self._health_lock = threading.Lock()
        self._health_stop_event = threading.Event()
        self._health_thread: threading.Thread | None = None
        self._health_started_at = 0.0
        self._last_ui_heartbeat = 0.0
        self._ui_queue_size = 0
        self._emergency_mode = False
        self._emergency_reason = ""
        self._thread_manager = ThreadManager()
        self._db_writer = DatabaseWriterWorker(logger=logger, thread_manager=self._thread_manager)
        self._cryptopanic_usage_lock = threading.Lock()
        self._cryptopanic_monthly_limit = max(int(getattr(settings, "cryptopanic_monthly_limit", 600) or 600), 1)
        self._cryptopanic_used_baseline = max(int(getattr(settings, "cryptopanic_used_this_month", 0) or 0), 0)
        self._cryptopanic_request_weekdays = self._parse_cryptopanic_request_days(
            str(getattr(settings, "cryptopanic_request_days", "mon,tue,wed,thu,fri") or "mon,tue,wed,thu,fri")
        )
        default_target = max(float(getattr(settings, "ai_target_profit_per_operation", 0.05) or 0.05), 0.0)
        self._ai_target_profit_per_operation_stocks = max(
            float(getattr(settings, "ai_target_profit_per_operation_stocks", default_target) or default_target),
            0.0,
        )
        self._ai_target_profit_per_operation_cryptos = max(
            float(getattr(settings, "ai_target_profit_per_operation_cryptos", default_target) or default_target),
            0.0,
        )
        self._ai_max_spread_allowed = max(float(getattr(settings, "ai_max_spread_allowed", 0.05) or 0.05), 0.0)
        self._global_volume_refresh_seconds = max(
            300,
            min(900, int(getattr(settings, "coingecko_global_volume_refresh_seconds", 600) or 600)),
        )
        self._default_focus_stocks_only = bool(getattr(settings, "ai_focus_stocks_only", False))
        self._default_focus_cryptos_only = bool(getattr(settings, "ai_focus_cryptos_only", False))
        self._default_focus_stocks_symbols = str(getattr(settings, "ai_focus_stocks_symbols", "") or "")
        self._default_focus_cryptos_symbols = str(getattr(settings, "ai_focus_cryptos_symbols", "") or "")
        self._cryptopanic_usage_path = Path(settings.ai_brain_db_path).resolve().parent / "cryptopanic_usage.json"
        self._cryptopanic_usage_data = self._load_cryptopanic_usage()
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
            thread_manager=self._thread_manager,
            role="collector",
        )
        self.signal_scanner_worker = SignalScannerWorker(
            name="SignalScannerWorker",
            loop_fn=self._scanner_cycle,
            sleep_seconds_fn=lambda: 20.0,
            logger=logger,
            thread_manager=self._thread_manager,
            role="scanner",
        )
        self.outcome_labeler_worker = OutcomeLabelerWorker(
            name="OutcomeLabelerWorker",
            loop_fn=self._labeler_cycle,
            sleep_seconds_fn=lambda: 30.0,
            logger=logger,
            thread_manager=self._thread_manager,
            role="labeler",
        )
        self.news_social_worker = NewsSocialCollectorWorker(
            name="NewsSocialCollectorWorker",
            loop_fn=self._news_social_cycle,
            sleep_seconds_fn=self._news_interval_seconds,
            logger=logger,
            thread_manager=self._thread_manager,
            role="news",
        )
        self.model_trainer_worker = ModelTrainerWorker(
            name="ModelTrainerWorker",
            loop_fn=self._training_cycle,
            sleep_seconds_fn=self._training_interval_seconds,
            logger=logger,
            thread_manager=self._thread_manager,
            role="trainer",
        )
        if str(getattr(self.broker, "provider", "alpaca") or "alpaca").lower() == "binance":
            self.stream_manager = NullStreamManager()
        else:
            self.stream_manager = AlpacaStreamManager(
                alpaca_endpoint=self.broker.endpoint,
                api_key=str(getattr(self.broker, "api_key", "") or ""),
                api_secret=str(getattr(self.broker, "api_secret", "") or ""),
                logger=logger,
                symbol_provider=self._stream_subscription_symbols,
                market_event_callback=self._handle_stream_market_event,
                news_event_callback=self._handle_stream_news_event,
                trade_update_callback=self._handle_stream_trade_update,
                paper_trading=bool(settings.paper_trading),
                thread_manager=self._thread_manager,
                stale_seconds=float(getattr(settings, "websocket_stale_seconds", 45) or 45),
                max_backoff_seconds=float(getattr(settings, "websocket_max_reconnect_backoff_seconds", 30) or 30),
            )
        self.volume_manager = CryptoVolumeManager(
            database=self.database,
            market_data=self.market_data,
            logger=self.logger,
            global_fetcher=self._fetch_global_crypto_market_data,
            websocket_connected=lambda: bool(self.stream_manager.connected),
            websocket_reconnect=self.reconnect_websocket,
            global_cache_seconds=int(getattr(self.settings, "coingecko_global_volume_refresh_seconds", 600) or 600),
        )
        self._install_database_write_queue()
        self._db_writer.start()
        self.initialize()

    def _install_database_write_queue(self) -> None:
        write_methods = {
            "upsert_account",
            "upsert_bot_funds",
            "upsert_runtime_settings",
            "upsert_watchlist_asset",
            "insert_market_snapshot",
            "insert_news_event",
            "insert_signal",
            "insert_trade",
            "upsert_trade_order",
            "upsert_position",
            "close_missing_positions",
            "insert_training_run",
            "insert_decision_log",
            "upsert_signal_outcome",
            "cleanup_old_data",
            "upsert_crypto_global_market_data",
            "insert_crypto_volume_record",
        }
        for method_name in write_methods:
            method = getattr(self.database, method_name, None)
            if method is None:
                continue
            wrapped = self._db_writer.wrap_method(method_name, method)
            setattr(self.database, method_name, wrapped)

    def _record_api_call(self, provider: str, action: str, symbol: str = "", result: str = "ok", error: str = "") -> None:
        self._recent_api_calls.append(
            {
                "timestamp": self._now_iso(),
                "provider": provider,
                "action": action,
                "symbol": symbol,
                "result": result,
                "error": error,
            }
        )

    def _record_error(self, worker: str, action: str, error: Exception | str) -> None:
        msg = str(error)
        self._recent_errors.append(
            {
                "timestamp": self._now_iso(),
                "worker": worker,
                "action": action,
                "error": msg,
            }
        )
        self._last_api_error = msg

    def update_ui_heartbeat(self, *, heartbeat_ts: float, queue_size: int) -> None:
        with self._health_lock:
            self._last_ui_heartbeat = float(heartbeat_ts)
            self._ui_queue_size = int(queue_size)

    def set_emergency_mode(self, enabled: bool, reason: str = "") -> None:
        self._emergency_mode = bool(enabled)
        self._emergency_reason = str(reason or "")
        if enabled:
            self.logger.warning("EMERGENCY MODE ACTIVADO: %s", self._emergency_reason)
        else:
            self.logger.info("EMERGENCY MODE desactivado")

    def reconnect_websocket(self) -> None:
        self.stream_manager.reconnect_now()

    @staticmethod
    def _normalize_text(value: Any) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        raw = raw.replace("_", " ").replace("-", " ")
        normalized = unicodedata.normalize("NFKD", raw)
        ascii_only = "".join(ch for ch in normalized if not unicodedata.combining(ch))
        return " ".join(ascii_only.lower().split())

    def _vpn_requirement_enabled(self) -> bool:
        return bool(getattr(self.settings, "require_nordvpn_before_trading", True))

    def _required_vpn_country(self) -> str:
        return str(getattr(self.settings, "required_vpn_country", "Dominican Republic") or "Dominican Republic").strip()

    def _vpn_status(self) -> dict[str, Any]:
        requirement_enabled = self._vpn_requirement_enabled()
        provider = str(getattr(self.settings, "required_vpn_provider", "NordVPN") or "NordVPN").strip() or "NordVPN"
        required_country = self._required_vpn_country()

        if not requirement_enabled:
            return {
                "enabled": False,
                "ready": True,
                "provider": provider,
                "required_country": required_country,
                "status": "DISABLED",
                "country": "",
                "reason": "VPN requirement disabled by configuration",
            }

        if provider.lower() != "nordvpn":
            return {
                "enabled": True,
                "ready": False,
                "provider": provider,
                "required_country": required_country,
                "status": "UNSUPPORTED_PROVIDER",
                "country": "",
                "reason": f"Unsupported VPN provider: {provider}",
            }

        nordvpn_bin = shutil.which("nordvpn")
        if not nordvpn_bin:
            return {
                "enabled": True,
                "ready": False,
                "provider": provider,
                "required_country": required_country,
                "status": "NORDVPN_NOT_INSTALLED",
                "country": "",
                "reason": "NordVPN CLI not found in PATH",
            }

        try:
            proc = subprocess.run(
                [nordvpn_bin, "status"],
                check=False,
                capture_output=True,
                text=True,
                timeout=8,
            )
        except Exception as ex:
            return {
                "enabled": True,
                "ready": False,
                "provider": provider,
                "required_country": required_country,
                "status": "STATUS_ERROR",
                "country": "",
                "reason": f"Unable to read NordVPN status: {ex}",
            }

        output = (proc.stdout or "") + "\n" + (proc.stderr or "")
        normalized_output = self._normalize_text(output)
        if "permission denied" in normalized_output and "groupadd nordvpn" in normalized_output:
            return {
                "enabled": True,
                "ready": False,
                "provider": provider,
                "required_country": required_country,
                "status": "PERMISSION_DENIED",
                "country": "",
                "reason": "NordVPN requires permissions: run 'sudo groupadd nordvpn' and 'sudo usermod -aG nordvpn $USER', then reboot",
            }
        parsed: dict[str, str] = {}
        for line in output.splitlines():
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            parsed[self._normalize_text(key)] = str(value or "").strip()

        status_text = str(parsed.get("status", "") or "").strip()
        country = str(parsed.get("country", "") or "").strip()
        connected = "connected" in self._normalize_text(status_text)
        if not connected:
            return {
                "enabled": True,
                "ready": False,
                "provider": provider,
                "required_country": required_country,
                "status": "DISCONNECTED",
                "country": country,
                "reason": f"NordVPN disconnected ({status_text or 'status unknown'})",
            }

        normalized_required = self._normalize_text(required_country)
        normalized_country = self._normalize_text(country)
        country_ok = normalized_required in normalized_country if normalized_country else False
        if not country_ok:
            return {
                "enabled": True,
                "ready": False,
                "provider": provider,
                "required_country": required_country,
                "status": "WRONG_COUNTRY",
                "country": country,
                "reason": f"VPN country must be {required_country}; current={country or 'unknown'}",
            }

        return {
            "enabled": True,
            "ready": True,
            "provider": provider,
            "required_country": required_country,
            "status": "READY",
            "country": country,
            "reason": "NordVPN connected to required country",
        }

    def _ensure_vpn_ready_for_trading(self, action: str) -> None:
        vpn = self._vpn_status()
        if bool(vpn.get("ready", False)):
            return
        reason = str(vpn.get("reason", "VPN requirement not met") or "VPN requirement not met")
        status = str(vpn.get("status", "VPN_BLOCKED") or "VPN_BLOCKED")
        raise ValueError(f"{action} blocked by VPN policy [{status}]: {reason}")

    def _start_health_monitor(self) -> None:
        if self._health_thread is not None and self._health_thread.is_alive():
            return
        self._health_stop_event.clear()
        self._health_started_at = time.time()
        self._thread_manager.register("HealthMonitorWorker", "health")
        self._health_thread = threading.Thread(target=self._health_monitor_loop, daemon=True, name="HealthMonitorWorker")
        self._health_thread.start()

    def _stop_health_monitor(self) -> None:
        self._health_stop_event.set()
        if self._health_thread is not None and self._health_thread.is_alive():
            self._health_thread.join(timeout=2.0)
        self._health_thread = None
        self._thread_manager.set_stopped("HealthMonitorWorker")

    def _health_monitor_loop(self) -> None:
        interval = max(int(getattr(self.settings, "health_monitor_interval_seconds", 30) or 30), 10)
        while not self._health_stop_event.is_set():
            try:
                snapshot = self._build_health_snapshot()
                with self._health_lock:
                    self._last_health_snapshot = snapshot
                if snapshot.get("app_status") == "ERROR":
                    self.set_emergency_mode(True, "Health monitor detecto estado ERROR")
                self._thread_manager.heartbeat("HealthMonitorWorker")
            except Exception as ex:
                self._thread_manager.set_error("HealthMonitorWorker", str(ex))
                self._record_error("HealthMonitorWorker", "loop", ex)
            self._health_stop_event.wait(timeout=float(interval))

    def _build_health_snapshot(self) -> dict[str, Any]:
        now = time.time()
        startup_grace_seconds = 45.0
        with self._health_lock:
            ui_heartbeat = float(self._last_ui_heartbeat or 0.0)
            ui_queue_size = int(self._ui_queue_size or 0)
        ui_age = (now - ui_heartbeat) if ui_heartbeat > 0 else float("inf")
        within_startup_grace = (self._health_started_at > 0.0) and ((now - self._health_started_at) < startup_grace_seconds)
        ws_status = self.stream_manager.status_snapshot()
        thread_summary = self._thread_manager.summary()
        rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        mem_mb = float(rss_kb) / 1024.0
        db_ok = True
        try:
            self.database.list_accounts()
        except Exception:
            db_ok = False
        cooldown = bool(self.broker.runtime_state.in_cooldown(self.broker.account_name))
        cooldown_remaining = float(self.broker.runtime_state.cooldown_remaining(self.broker.account_name))
        ws_stale = any(bool(row.get("stale", False)) for row in ws_status.get("streams", []))
        app_status = "OK"
        ui_warning = (ui_age > 8.0) and not within_startup_grace
        ui_error = (ui_age > 20.0) and not within_startup_grace
        if ui_warning or ui_queue_size > 5000 or not db_ok or thread_summary.get("error_threads", 0) > 0:
            app_status = "WARNING"
        if ui_error or ui_queue_size > 20000:
            app_status = "ERROR"
        return {
            "timestamp": self._now_iso(),
            "app_status": app_status,
            "ui_alive": (ui_age <= 8.0) or within_startup_grace,
            "ui_heartbeat_age_seconds": ui_age,
            "ui_queue_size": ui_queue_size,
            "websocket": ws_status,
            "alpaca_cooldown": cooldown,
            "alpaca_cooldown_remaining": cooldown_remaining,
            "db_status": "OK" if db_ok else "ERROR",
            "active_threads": thread_summary.get("active_threads", 0),
            "threads": thread_summary.get("threads", []),
            "memory_mb": mem_mb,
            "db_queue_size": int(self._db_writer.queue_size()),
            "last_api_error": self._last_api_error,
            "ws_stale": ws_stale,
            "emergency_mode": self._emergency_mode,
            "emergency_reason": self._emergency_reason,
        }

    def get_health_snapshot(self) -> dict[str, Any]:
        with self._health_lock:
            snapshot = dict(self._last_health_snapshot)
        if not snapshot:
            snapshot = self._build_health_snapshot()
            with self._health_lock:
                self._last_health_snapshot = snapshot
        return snapshot

    def export_crash_report(self) -> str:
        reports_dir = Path(__file__).resolve().parents[1] / "crash_reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        report_path = reports_dir / f"crash_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        log_tail: list[str] = []
        bot_log = Path(__file__).resolve().parents[1] / "logs" / "bot.log"
        if bot_log.exists():
            try:
                lines = bot_log.read_text(encoding="utf-8", errors="ignore").splitlines()
                log_tail = lines[-200:]
            except Exception:
                log_tail = []
        active_account = self.database.get_account_by_name(self._active_account_for_workers) if self._active_account_for_workers else None
        active_account_id = int(active_account["id"]) if active_account else 0
        payload = {
            "health": self.get_health_snapshot(),
            "threads": self._thread_manager.summary(),
            "last_error": self._last_api_error,
            "recent_api_calls": list(self._recent_api_calls)[-200:],
            "recent_errors": list(self._recent_errors)[-200:],
            "latest_signal": self.database.latest_signals(limit=1),
            "latest_trade": self.database.list_trades(account_id=active_account_id, limit=1) if active_account_id > 0 else [],
            "positions": self.database.list_positions(account_id=active_account_id) if active_account_id > 0 else [],
            "websocket_status": self.stream_manager.status_snapshot(),
            "log_tail": log_tail,
        }
        report_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        return str(report_path)

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
            existing_runtime = self.database.get_runtime_settings(account_id)
            self.database.upsert_runtime_settings(
                account_id=account_id,
                signal_only_mode=bool(self.settings.ai_signal_only_mode),
                paper_trading=bool(self.settings.paper_trading),
                live_trading_enabled=bool(self.settings.live_trading_enabled),
                manual_approval_required=bool(self.settings.manual_approval_required),
                kill_switch=False,
                auto_trade_stocks_enabled=bool(existing_runtime.get("auto_trade_stocks_enabled", 1)) if existing_runtime else True,
                auto_trade_cryptos_enabled=bool(existing_runtime.get("auto_trade_cryptos_enabled", 1)) if existing_runtime else True,
                scanner_decision_engine=str((existing_runtime or {}).get("scanner_decision_engine", "heuristic") or "heuristic"),
                futures_only_mode=bool((existing_runtime or {}).get("futures_only_mode", getattr(self.settings, "crypto_futures_only_mode", True))),
                futures_leverage=int((existing_runtime or {}).get("futures_leverage", getattr(self.settings, "crypto_futures_default_leverage", 1)) or 1),
                futures_require_technical=bool((existing_runtime or {}).get("futures_require_technical", getattr(self.settings, "ai_futures_require_technical", True))),
                futures_require_news=bool((existing_runtime or {}).get("futures_require_news", getattr(self.settings, "ai_futures_require_news", False))),
                futures_enable_long=bool((existing_runtime or {}).get("futures_enable_long", getattr(self.settings, "ai_futures_enable_long", True))),
                futures_enable_short=bool((existing_runtime or {}).get("futures_enable_short", getattr(self.settings, "ai_futures_enable_short", True))),
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
        auto_trade_stocks_enabled: bool,
        auto_trade_cryptos_enabled: bool,
        scanner_decision_engine: str = "heuristic",
        futures_only_mode: bool = True,
        futures_leverage: int = 1,
        futures_require_technical: bool = True,
        futures_require_news: bool = False,
        futures_enable_long: bool = True,
        futures_enable_short: bool = True,
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
            auto_trade_stocks_enabled=auto_trade_stocks_enabled,
            auto_trade_cryptos_enabled=auto_trade_cryptos_enabled,
            scanner_decision_engine=str(scanner_decision_engine or "heuristic"),
            futures_only_mode=bool(futures_only_mode),
            futures_leverage=max(int(futures_leverage or 1), 1),
            futures_require_technical=bool(futures_require_technical),
            futures_require_news=bool(futures_require_news),
            futures_enable_long=bool(futures_enable_long),
            futures_enable_short=bool(futures_enable_short),
        )
        self.auto_controller.signal_only_mode = bool(signal_only_mode)
        self.auto_controller.paper_trading = bool(paper_trading)
        self.auto_controller.live_trading_enabled = bool(live_trading_enabled)
        self.auto_controller.manual_approval_required = bool(manual_approval_required)

    def start_automation(self, account_name: str) -> dict[str, Any]:
        self.refresh_account_context(account_name)
        self._ensure_vpn_ready_for_trading("Automation start")
        with self._worker_lock:
            self._active_account_for_workers = account_name
        self.stream_manager.start(account_name)
        self.data_collector_worker.start()
        self.signal_scanner_worker.start()
        self.outcome_labeler_worker.start()
        self.news_social_worker.start()
        self.model_trainer_worker.start()
        self._start_health_monitor()
        self.logger.info("Workers automaticos iniciados para %s", account_name)
        return self.get_automation_status(account_name)

    def pause_automation(self) -> dict[str, Any]:
        self.stream_manager.stop()
        self.data_collector_worker.stop()
        self.signal_scanner_worker.stop()
        self.outcome_labeler_worker.stop()
        self.news_social_worker.stop()
        self.model_trainer_worker.stop()
        self._stop_health_monitor()
        self.logger.info("Workers automaticos en pausa")
        return self.get_automation_status(self._active_account_for_workers)

    def get_automation_status(self, account_name: str) -> dict[str, Any]:
        self._reset_daily_counters_if_needed()
        approved_model = self.registry.approved_version() or ""
        approved_model_available = bool(self.registry.approved_model_available())
        latest_model = self.registry.latest_version() or ""
        training_cycle_seconds = max(float(self._training_interval_seconds() or 0.0), 0.0)
        now_ts = time.time()
        last_training_ts = float(getattr(self.model_trainer_worker, "last_run_at", 0.0) or 0.0)
        last_training_attempt_ts = float(getattr(self.model_trainer_worker, "last_attempt_at", 0.0) or 0.0)
        training_started_ts = float(getattr(self.model_trainer_worker, "started_at", 0.0) or 0.0)
        progress_anchor_ts = max(last_training_ts, last_training_attempt_ts, training_started_ts)
        training_elapsed_seconds = max(now_ts - progress_anchor_ts, 0.0) if progress_anchor_ts > 0.0 else 0.0
        if training_cycle_seconds <= 0.0:
            training_progress_pct = 0.0
            training_remaining_seconds = 0.0
        else:
            training_progress_pct = min((training_elapsed_seconds / training_cycle_seconds) * 100.0, 100.0)
            training_remaining_seconds = max(training_cycle_seconds - training_elapsed_seconds, 0.0)

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
        with self._worker_lock:
            active_worker_account = str(self._active_account_for_workers or "").strip()
        scanner_engine = str((runtime or {}).get("scanner_decision_engine", "heuristic") or "heuristic").strip().lower()
        if scanner_engine not in {"heuristic", "model"}:
            scanner_engine = "heuristic"
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
            "active_worker_account": active_worker_account,
            "training_cycle_seconds": training_cycle_seconds,
            "training_elapsed_seconds": training_elapsed_seconds,
            "training_remaining_seconds": training_remaining_seconds,
            "training_progress_pct": training_progress_pct,
            "training_last_error": str(getattr(self.model_trainer_worker, "last_error", "") or ""),
            "model_current": (approved_model if approved_model and approved_model_available else "heuristic"),
            "model_latest_trained": latest_model or "none",
            "model_approved_paper": (approved_model if approved_model and approved_model_available else "manual_pending"),
            "model_approved_reference": approved_model or "",
            "model_approved_available": approved_model_available,
            "model_approved_live": "manual_required",
            "scanner_decision_engine": scanner_engine,
            "decision_actor": ("Modelo entrenado" if scanner_engine == "model" else "Heurística"),
            "auto_trade_stocks_enabled": bool(runtime.get("auto_trade_stocks_enabled", 1)) if runtime else True,
            "auto_trade_cryptos_enabled": bool(runtime.get("auto_trade_cryptos_enabled", 1)) if runtime else True,
            "futures_only_mode": bool(runtime.get("futures_only_mode", getattr(self.settings, "crypto_futures_only_mode", True))) if runtime else bool(getattr(self.settings, "crypto_futures_only_mode", True)),
            "futures_leverage": max(int(runtime.get("futures_leverage", getattr(self.settings, "crypto_futures_default_leverage", 1)) or 1), 1) if runtime else max(int(getattr(self.settings, "crypto_futures_default_leverage", 1) or 1), 1),
            "futures_require_technical": bool(runtime.get("futures_require_technical", getattr(self.settings, "ai_futures_require_technical", True))) if runtime else bool(getattr(self.settings, "ai_futures_require_technical", True)),
            "futures_require_news": bool(runtime.get("futures_require_news", getattr(self.settings, "ai_futures_require_news", False))) if runtime else bool(getattr(self.settings, "ai_futures_require_news", False)),
            "futures_enable_long": bool(runtime.get("futures_enable_long", getattr(self.settings, "ai_futures_enable_long", True))) if runtime else bool(getattr(self.settings, "ai_futures_enable_long", True)),
            "futures_enable_short": bool(runtime.get("futures_enable_short", getattr(self.settings, "ai_futures_enable_short", True))) if runtime else bool(getattr(self.settings, "ai_futures_enable_short", True)),
            "health": self.get_health_snapshot(),
            "threads": self._thread_manager.summary(),
            "websocket": self.stream_manager.status_snapshot(),
            "emergency_mode": self._emergency_mode,
            "emergency_reason": self._emergency_reason,
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
        vpn = self._vpn_status()
        return {
            "kill_switch": bool(runtime.get("kill_switch", 0)),
            "paper_trading": bool(runtime.get("paper_trading", 0)),
            "live_trading_enabled": bool(runtime.get("live_trading_enabled", 0)),
            "manual_approval_required": bool(runtime.get("manual_approval_required", 1)),
            "signal_only_mode": bool(runtime.get("signal_only_mode", 1)),
            "auto_trade_stocks_enabled": bool(runtime.get("auto_trade_stocks_enabled", 1)),
            "auto_trade_cryptos_enabled": bool(runtime.get("auto_trade_cryptos_enabled", 1)),
            "max_daily_loss": float(funds.get("max_daily_loss", 0.0) or 0.0),
            "max_position_size": float(funds.get("max_position_size", 0.0) or 0.0),
            "api_keys_ok": bool(str(getattr(self.broker, "api_key", "") or "") and str(getattr(self.broker, "api_secret", "") or "")),
            "openai_key_ok": bool(self.settings.openai_api_key),
            "vpn_required": bool(vpn.get("enabled", False)),
            "vpn_ready": bool(vpn.get("ready", False)),
            "vpn_provider": str(vpn.get("provider", "NordVPN") or "NordVPN"),
            "vpn_required_country": str(vpn.get("required_country", self._required_vpn_country()) or self._required_vpn_country()),
            "vpn_country": str(vpn.get("country", "") or ""),
            "vpn_status": str(vpn.get("status", "N/A") or "N/A"),
            "vpn_reason": str(vpn.get("reason", "") or ""),
            "recent_logs": self.database.latest_decision_logs(limit=15),
        }

    def validate_volume_data(self, symbol: str) -> dict[str, Any]:
        return self.volume_manager.validate_volume_data(symbol)

    @staticmethod
    def _parse_focus_symbols(raw: str) -> set[str]:
        return {
            str(token).upper().replace(" ", "")
            for token in str(raw or "").replace(";", ",").split(",")
            if str(token).strip()
        }

    def _focus_for_account(self, account_name: str) -> dict[str, Any]:
        key = str(account_name or "").strip().lower()
        existing = self._focus_by_account.get(key)
        if existing is not None:
            return existing
        payload = {
            "stocks_only": bool(self._default_focus_stocks_only),
            "cryptos_only": bool(self._default_focus_cryptos_only),
            "stocks_symbols_raw": self._default_focus_stocks_symbols,
            "cryptos_symbols_raw": self._default_focus_cryptos_symbols,
            "stocks_symbols": self._parse_focus_symbols(self._default_focus_stocks_symbols),
            "cryptos_symbols": self._parse_focus_symbols(self._default_focus_cryptos_symbols),
        }
        self._focus_by_account[key] = payload
        return payload

    def update_focus_symbols(
        self,
        *,
        account_name: str,
        focus_stocks_only: bool,
        focus_cryptos_only: bool,
        focus_stocks_symbols: str,
        focus_cryptos_symbols: str,
    ) -> None:
        key = str(account_name or "").strip().lower()
        payload = {
            "stocks_only": bool(focus_stocks_only),
            "cryptos_only": bool(focus_cryptos_only),
            "stocks_symbols_raw": str(focus_stocks_symbols or ""),
            "cryptos_symbols_raw": str(focus_cryptos_symbols or ""),
            "stocks_symbols": self._parse_focus_symbols(focus_stocks_symbols),
            "cryptos_symbols": self._parse_focus_symbols(focus_cryptos_symbols),
        }
        self._focus_by_account[key] = payload

    def get_focus_symbols(self, account_name: str) -> dict[str, Any]:
        payload = self._focus_for_account(account_name)
        return {
            "stocks_only": bool(payload.get("stocks_only", False)),
            "cryptos_only": bool(payload.get("cryptos_only", False)),
            "stocks_symbols_raw": str(payload.get("stocks_symbols_raw", "") or ""),
            "cryptos_symbols_raw": str(payload.get("cryptos_symbols_raw", "") or ""),
        }

    def _is_symbol_selected_in_focus(self, *, account_name: str, asset_type: str, symbol: str) -> bool:
        focus = self._focus_for_account(account_name)
        asset = str(asset_type or "").lower().strip()
        symbol_norm = str(symbol or "").upper().replace(" ", "")
        if not symbol_norm:
            return False
        if asset == "stock":
            if not bool(focus.get("stocks_only", False)):
                return False
            focus_stocks = set(focus.get("stocks_symbols", set()))
            return bool(focus_stocks) and symbol_norm in focus_stocks
        if asset == "crypto":
            if not bool(focus.get("cryptos_only", False)):
                return False
            focus_cryptos = set(focus.get("cryptos_symbols", set()))
            if not focus_cryptos:
                return False
            symbol_key = self._symbol_key(symbol_norm)
            return any(self._symbol_key(str(candidate)) == symbol_key for candidate in focus_cryptos)
        return False

    def _is_effective_auto_enabled_for_symbol(
        self,
        *,
        runtime: dict[str, Any],
        account_name: str,
        asset_type: str,
        symbol: str,
    ) -> bool:
        asset = str(asset_type or "").lower().strip()
        runtime_enabled = bool(runtime.get("auto_trade_cryptos_enabled", 1)) if asset == "crypto" else bool(runtime.get("auto_trade_stocks_enabled", 1))
        if runtime_enabled:
            return True
        # If asset-level auto toggle is off but user explicitly focused this symbol, keep it eligible.
        return self._is_symbol_selected_in_focus(account_name=account_name, asset_type=asset, symbol=symbol)

    @staticmethod
    def _crypto_base_symbol(symbol: str) -> str:
        normalized = str(symbol or "").upper().replace(" ", "")
        normalized = normalized.replace("/USDC", "/USD").replace("/USDT", "/USD")
        if "/" in normalized:
            return normalized.split("/", 1)[0]
        if normalized.endswith("USD") and len(normalized) > 3:
            return normalized[:-3]
        return normalized

    @staticmethod
    def _normalize_crypto_symbol_input(symbol: str) -> str:
        raw = str(symbol or "").upper().strip().replace(" ", "")
        if not raw:
            return ""

        alias_to_base = {
            "SOLANA": "SOL",
            "BITCOIN": "BTC",
            "ETHEREUM": "ETH",
            "RIPPLE": "XRP",
            "DOGECOIN": "DOGE",
            "CHAINLINK": "LINK",
            "AVALANCHE": "AVAX",
            "LITECOIN": "LTC",
        }
        base_candidates = {"BTC", "ETH", "SOL", "XRP", "DOGE", "LINK", "AVAX", "LTC", "PEPE", "BONK", "SHIB", "WIF"}

        raw = alias_to_base.get(raw, raw)
        if "/" in raw:
            base, quote = raw.split("/", 1)
            quote = "USD" if quote in {"USD", "USDT", "USDC"} else quote
            return f"{base}/{quote}"

        for quote in ("USDT", "USDC", "USD"):
            if raw.endswith(quote) and len(raw) > len(quote):
                return f"{raw[:-len(quote)]}/USD"

        if raw in base_candidates:
            return f"{raw}/USD"
        return raw

    @staticmethod
    def _coingecko_coin_id(base_symbol: str) -> str | None:
        mapping = {
            "BTC": "bitcoin",
            "ETH": "ethereum",
            "SOL": "solana",
            "XRP": "ripple",
            "DOGE": "dogecoin",
            "LINK": "chainlink",
            "AVAX": "avalanche-2",
            "LTC": "litecoin",
        }
        return mapping.get(str(base_symbol or "").upper().strip())

    @staticmethod
    def _normalize_global_source_label(source_value: Any) -> str:
        source = str(source_value or "").strip().lower()
        mapping = {
            "coingecko": "CoinGecko",
            "coinmarketcap": "CoinMarketCap",
            "coinbase": "Coinbase",
            "none": "CoinGecko",
            "": "CoinGecko",
        }
        return mapping.get(source, str(source_value or "CoinGecko"))

    @staticmethod
    def _parse_iso_timestamp(raw_value: str) -> datetime | None:
        raw = str(raw_value or "").strip()
        if not raw:
            return None
        normalized = raw.replace("Z", "+00:00") if raw.endswith("Z") else raw
        try:
            dt = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def _fetch_global_crypto_market_data(self, symbol: str) -> dict[str, Any]:
        base_symbol = self._crypto_base_symbol(symbol)
        cache_key = base_symbol
        now_monotonic = time.monotonic()
        cached = self._global_crypto_cache_by_symbol.get(cache_key)
        if cached is not None:
            fetched_at_mono = float(cached.get("_fetched_at_monotonic", 0.0) or 0.0)
            if (now_monotonic - fetched_at_mono) <= float(self._global_volume_refresh_seconds):
                return dict(cached)

        fresh_from_db = self.database.get_latest_crypto_global_market_data(
            symbol=base_symbol,
            source="coingecko",
            max_age_seconds=int(self._global_volume_refresh_seconds),
        )
        if fresh_from_db is not None:
            payload = {
                "symbol": base_symbol,
                "source": "coingecko",
                "current_price": float(fresh_from_db.get("current_price", 0.0) or 0.0),
                "total_volume": float(fresh_from_db.get("total_volume", 0.0) or 0.0),
                "market_cap": float(fresh_from_db.get("market_cap", 0.0) or 0.0),
                "price_change_percentage_24h": float(fresh_from_db.get("price_change_percentage_24h", 0.0) or 0.0),
                "fetched_at": str(fresh_from_db.get("fetched_at", "") or ""),
                "status": "OK",
                "_fetched_at_monotonic": now_monotonic,
            }
            self._global_crypto_cache_by_symbol[cache_key] = dict(payload)
            return payload

        coin_id = self._coingecko_coin_id(base_symbol)
        if not coin_id:
            payload = {
                "symbol": base_symbol,
                "source": "none",
                "current_price": 0.0,
                "total_volume": 0.0,
                "market_cap": 0.0,
                "price_change_percentage_24h": 0.0,
                "fetched_at": "",
                "status": "ERROR",
                "error": f"No CoinGecko mapping for {base_symbol}",
                "_fetched_at_monotonic": now_monotonic,
            }
            self._global_crypto_cache_by_symbol[cache_key] = dict(payload)
            return payload

        headers = {"Accept": "application/json"}
        api_key = str(getattr(self.settings, "coingecko_api_key", "") or "").strip()
        if api_key:
            headers["x-cg-pro-api-key"] = api_key
        timeout_seconds = int(getattr(self.settings, "http_timeout_news_seconds", 10) or 10)

        try:
            response = requests.get(
                "https://api.coingecko.com/api/v3/coins/markets",
                params={
                    "vs_currency": "usd",
                    "ids": coin_id,
                    "price_change_percentage": "24h",
                },
                headers=headers,
                timeout=timeout_seconds,
            )
            response.raise_for_status()
            rows = response.json()
            if not isinstance(rows, list) or not rows:
                raise ValueError(f"CoinGecko sin datos para {coin_id}")
            row = rows[0] or {}

            fetched_at = self._now_iso()
            payload = {
                "symbol": base_symbol,
                "source": "coingecko",
                "current_price": float(row.get("current_price", 0.0) or 0.0),
                "total_volume": float(row.get("total_volume", 0.0) or 0.0),
                "market_cap": float(row.get("market_cap", 0.0) or 0.0),
                "price_change_percentage_24h": float(row.get("price_change_percentage_24h_in_currency", row.get("price_change_percentage_24h", 0.0)) or 0.0),
                "fetched_at": fetched_at,
                "status": "OK",
                "_fetched_at_monotonic": now_monotonic,
            }
            self.database.upsert_crypto_global_market_data(
                symbol=base_symbol,
                source="coingecko",
                current_price=payload["current_price"],
                total_volume=payload["total_volume"],
                market_cap=payload["market_cap"],
                price_change_percentage_24h=payload["price_change_percentage_24h"],
                fetched_at=fetched_at,
            )
            self._global_crypto_cache_by_symbol[cache_key] = dict(payload)
            return payload
        except Exception as ex:
            stale = self.database.get_latest_crypto_global_market_data(
                symbol=base_symbol,
                source="coingecko",
                max_age_seconds=None,
            )
            if stale is not None:
                payload = {
                    "symbol": base_symbol,
                    "source": "coingecko",
                    "current_price": float(stale.get("current_price", 0.0) or 0.0),
                    "total_volume": float(stale.get("total_volume", 0.0) or 0.0),
                    "market_cap": float(stale.get("market_cap", 0.0) or 0.0),
                    "price_change_percentage_24h": float(stale.get("price_change_percentage_24h", 0.0) or 0.0),
                    "fetched_at": str(stale.get("fetched_at", "") or ""),
                    "status": "STALE",
                    "error": str(ex),
                    "_fetched_at_monotonic": now_monotonic,
                }
                self._global_crypto_cache_by_symbol[cache_key] = dict(payload)
                return payload

            payload = {
                "symbol": base_symbol,
                "source": "coingecko",
                "current_price": 0.0,
                "total_volume": 0.0,
                "market_cap": 0.0,
                "price_change_percentage_24h": 0.0,
                "fetched_at": "",
                "status": "ERROR",
                "error": str(ex),
                "_fetched_at_monotonic": now_monotonic,
            }
            self._global_crypto_cache_by_symbol[cache_key] = dict(payload)
            return payload

    def get_symbol_diagnostics(self, account_name: str, symbol: str, asset_type: str = "") -> dict[str, Any]:
        account = self.refresh_account_context(account_name)
        account_id = int(account["id"])
        runtime = self.database.get_runtime_settings(account_id) or {}
        account_name_key = str(account_name or "").strip().lower()
        input_symbol = str(symbol or "").upper().replace(" ", "").strip()
        if not input_symbol:
            raise ValueError("Simbolo invalido para diagnostico IA")

        inferred_asset_type = str(asset_type or "").lower().strip()
        if inferred_asset_type not in {"stock", "crypto"}:
            crypto_hint = self._normalize_crypto_symbol_input(input_symbol)
            inferred_asset_type = "crypto" if ("/" in input_symbol or input_symbol.endswith("USD") or input_symbol.endswith("USDT") or input_symbol.endswith("USDC") or "/" in crypto_hint) else "stock"

        symbol_norm = self._normalize_crypto_symbol_input(input_symbol) if inferred_asset_type == "crypto" else input_symbol
        symbol_key = self._symbol_key(symbol_norm)

        snapshots = self.database.latest_market_snapshots(symbol_norm, limit=240)
        latest_snapshot = snapshots[0] if snapshots else {}
        latest_signal: dict[str, Any] | None = None
        fallback_signal: dict[str, Any] | None = None
        fallback_signal_account = ""
        for row in self.database.latest_signals(limit=5000):
            if self._symbol_key(str(row.get("symbol", ""))) != symbol_key:
                continue
            if fallback_signal is None:
                fallback_signal = row
                fallback_features = row.get("features_json") or {}
                fallback_signal_account = str(fallback_features.get("account_name", "") or "").strip()
            features_row = row.get("features_json") or {}
            signal_account = str(features_row.get("account_name", "") or "").strip().lower()
            if signal_account and signal_account == account_name_key:
                latest_signal = row
                break
        if latest_signal is None and not account_name_key:
            latest_signal = fallback_signal

        latest_decision: dict[str, Any] | None = None
        fallback_decision: dict[str, Any] | None = None
        fallback_decision_account = ""
        for row in self.database.latest_decision_logs(limit=5000):
            if self._symbol_key(str(row.get("symbol", ""))) != symbol_key:
                continue
            row_payload = dict(row)
            raw_context = row_payload.get("raw_context_json")
            context_payload: dict[str, Any] = {}
            if isinstance(raw_context, dict):
                context_payload = raw_context
            elif isinstance(raw_context, str):
                try:
                    loaded = json.loads(raw_context)
                    context_payload = loaded if isinstance(loaded, dict) else {}
                except Exception:
                    context_payload = {}
            row_payload["raw_context_json"] = context_payload
            if fallback_decision is None:
                fallback_decision = row_payload
                fallback_decision_account = str(context_payload.get("account_name", "") or "").strip()
            decision_account = str(context_payload.get("account_name", "") or "").strip().lower()
            if decision_account and decision_account == account_name_key:
                latest_decision = row_payload
                break
        if latest_decision is None and not account_name_key:
            latest_decision = fallback_decision

        # If the diagnostic signal is stale for crypto, force a one-shot refresh so
        # the operator does not see an old AVOID while market data is already updated.
        signal_timestamp = self._parse_iso_timestamp(str((latest_signal or {}).get("timestamp", "") or ""))
        signal_age_seconds = float("inf")
        if signal_timestamp is not None:
            signal_age_seconds = max((datetime.now(timezone.utc) - signal_timestamp).total_seconds(), 0.0)
        should_refresh_diagnostic_signal = (
            inferred_asset_type == "crypto"
            and (
                latest_signal is None
                or signal_age_seconds > 120.0
            )
        )
        if should_refresh_diagnostic_signal:
            refresh_key = f"{account_name_key}:{symbol_key}"
            now_mono = time.monotonic()
            last_refresh = float(self._diagnostic_signal_refresh_at_by_key.get(refresh_key, 0.0) or 0.0)
            if (now_mono - last_refresh) > 20.0:
                self._diagnostic_signal_refresh_at_by_key[refresh_key] = now_mono
                try:
                    self.generate_signal(
                        symbol=symbol_norm,
                        asset_type=inferred_asset_type,
                        account_name=account_name,
                        text_context="",
                        source="diagnostic_auto_refresh",
                    )
                except Exception as ex:
                    self.logger.warning("Diagnostic auto-refresh signal failed for %s: %s", symbol_norm, ex)
                else:
                    for row in self.database.latest_signals(limit=200):
                        if self._symbol_key(str(row.get("symbol", ""))) != symbol_key:
                            continue
                        features_row = row.get("features_json") or {}
                        signal_account = str(features_row.get("account_name", "") or "").strip().lower()
                        if signal_account and signal_account == account_name_key:
                            latest_signal = row
                            break
                    for row in self.database.latest_decision_logs(limit=200):
                        if self._symbol_key(str(row.get("symbol", ""))) != symbol_key:
                            continue
                        row_payload = dict(row)
                        raw_context = row_payload.get("raw_context_json")
                        context_payload: dict[str, Any] = {}
                        if isinstance(raw_context, dict):
                            context_payload = raw_context
                        elif isinstance(raw_context, str):
                            try:
                                loaded = json.loads(raw_context)
                                context_payload = loaded if isinstance(loaded, dict) else {}
                            except Exception:
                                context_payload = {}
                        decision_account = str(context_payload.get("account_name", "") or "").strip().lower()
                        if decision_account and decision_account == account_name_key:
                            row_payload["raw_context_json"] = context_payload
                            latest_decision = row_payload
                            break

        live_candles_1m: list[dict[str, Any]] = []
        live_candles_15m: list[dict[str, Any]] = []
        live_quote: dict[str, Any] = {}
        live_price = 0.0
        try:
            live_candles_1m = self.market_data.get_candles(symbol=symbol_norm, interval="1m", limit=60)
        except Exception:
            live_candles_1m = []
        try:
            live_candles_15m = self.market_data.get_candles(symbol=symbol_norm, interval="15m", limit=96)
        except Exception:
            live_candles_15m = []
        try:
            live_quote = self.market_data.get_latest_quote(symbol_norm)
        except Exception:
            live_quote = {}
        try:
            live_price = float(self.market_data.get_last_price(symbol_norm) or 0.0)
        except Exception:
            live_price = 0.0

        snapshot_rows = list(reversed([
            {
                "open": float(row.get("open", row.get("close", 0.0)) or 0.0),
                "high": float(row.get("high", row.get("close", 0.0)) or 0.0),
                "low": float(row.get("low", row.get("close", 0.0)) or 0.0),
                "close": float(row.get("close", row.get("price", 0.0)) or 0.0),
                "volume": float(row.get("volume", 0.0) or 0.0),
                "timestamp": row.get("timestamp"),
            }
            for row in snapshots
        ]))
        snapshot_bars_1m = self._aggregate_snapshot_rows_to_minutes(snapshot_rows)

        latest_candle_1m = live_candles_1m[-1] if live_candles_1m else {}
        candle_count_1m = len(live_candles_1m)
        price = live_price if live_price > 0.0 else float(latest_snapshot.get("price", 0.0) or 0.0)
        spread = float(live_quote.get("spread", 0.0) or 0.0)
        spread_pct = float(live_quote.get("spread_pct", 0.0) or 0.0)
        if spread <= 0.0:
            spread = float(latest_snapshot.get("spread", 0.0) or 0.0)
        if spread_pct <= 0.0 and price > 0.0:
            spread_pct = (spread / price) * 100.0

        binance_pair_volume_1m_base = 0.0
        binance_pair_volume_5m_base = 0.0
        binance_pair_volume_15m_base = 0.0
        binance_pair_volume_1m_usd = 0.0
        binance_pair_volume_5m_usd = 0.0
        binance_pair_volume_15m_usd = 0.0
        binance_trade_volume_1m_base = 0.0
        binance_trade_volume_1m_usd = 0.0
        binance_trade_count_1m = 0
        binance_trade_window_seconds = 60
        trade_volume_status = "N/A"
        trade_volume_error = ""

        try:
            trades_1m = self.market_data.get_recent_trade_stats(symbol=symbol_norm, lookback_seconds=60, limit=5000)
            trades_5m = self.market_data.get_recent_trade_stats(symbol=symbol_norm, lookback_seconds=300, limit=5000)
            trades_15m = self.market_data.get_recent_trade_stats(symbol=symbol_norm, lookback_seconds=900, limit=5000)

            binance_trade_volume_1m_base = float(trades_1m.get("volume", 0.0) or 0.0)
            binance_trade_volume_1m_usd = float(trades_1m.get("volume_usd", 0.0) or 0.0)
            binance_trade_count_1m = int(trades_1m.get("count", 0) or 0)
            binance_trade_window_seconds = int(trades_1m.get("lookback_seconds", 60) or 60)

            binance_pair_volume_1m_base = binance_trade_volume_1m_base
            binance_pair_volume_5m_base = float(trades_5m.get("volume", 0.0) or 0.0)
            binance_pair_volume_15m_base = float(trades_15m.get("volume", 0.0) or 0.0)
            binance_pair_volume_1m_usd = binance_trade_volume_1m_usd
            binance_pair_volume_5m_usd = float(trades_5m.get("volume_usd", 0.0) or 0.0)
            binance_pair_volume_15m_usd = float(trades_15m.get("volume_usd", 0.0) or 0.0)
            trade_volume_status = "OK"
        except Exception as ex:
            trade_volume_status = "ERROR"
            trade_volume_error = str(ex)

        now_utc = datetime.now(timezone.utc)
        latest_candle_dt = self._parse_iso_timestamp(str(latest_candle_1m.get("timestamp", "") or ""))
        is_stale = latest_candle_dt is None or (now_utc - latest_candle_dt).total_seconds() > 180.0

        use_snapshot_volume_fallback = False

        volume_data_status = "OK" if trade_volume_status == "OK" else "ERROR"

        now_iso = self._now_iso()
        day_ago_iso = (now_utc - timedelta(hours=24)).isoformat()
        binance_pair_volume_24h_usd = 0.0
        if live_candles_15m:
            binance_pair_volume_24h_usd = sum(
                float(row.get("close", 0.0) or 0.0) * float(row.get("volume", 0.0) or 0.0)
                for row in live_candles_15m
            )

        global_volume_24h_usd = 0.0
        global_volume_source = "none"
        global_volume_status = "N/A"
        global_volume_warning = ""
        if inferred_asset_type == "crypto":
            global_row = self._fetch_global_crypto_market_data(symbol_norm)
            global_volume_24h_usd = float(global_row.get("total_volume", 0.0) or 0.0)
            global_volume_source = self._normalize_global_source_label(global_row.get("source", "coingecko"))
            global_volume_status = str(global_row.get("status", "ERROR") or "ERROR")
            base_symbol = self._crypto_base_symbol(symbol_norm)
            if base_symbol in {"SOL", "BTC", "ETH", "XRP"} and global_volume_24h_usd < 1_000_000.0:
                global_volume_warning = "Global volume seems incorrect or source is incomplete"

        volume_snapshot: dict[str, Any] | None = None
        volume_validation: dict[str, Any] | None = None
        if inferred_asset_type == "crypto":
            try:
                volume_snapshot = self.volume_manager.build_snapshot(
                    symbol=symbol_norm,
                    price=price,
                    binance_24h_volume=binance_pair_volume_24h_usd,
                )
                volume_validation = self.volume_manager.validate_volume_data(symbol_norm)
                binance_pair_volume_1m_base = float(volume_snapshot.get("local_volume_1m_base", volume_snapshot.get("local_volume_1m", binance_pair_volume_1m_base)) or 0.0)
                binance_pair_volume_5m_base = float(volume_snapshot.get("local_volume_5m_base", volume_snapshot.get("local_volume_5m", binance_pair_volume_5m_base)) or 0.0)
                binance_pair_volume_15m_base = float(volume_snapshot.get("local_volume_15m_base", volume_snapshot.get("local_volume_15m", binance_pair_volume_15m_base)) or 0.0)
                binance_pair_volume_1m_usd = float(volume_snapshot.get("local_volume_1m_usd", binance_pair_volume_1m_usd) or 0.0)
                binance_pair_volume_5m_usd = float(volume_snapshot.get("local_volume_5m_usd", binance_pair_volume_5m_usd) or 0.0)
                binance_pair_volume_15m_usd = float(volume_snapshot.get("local_volume_15m_usd", binance_pair_volume_15m_usd) or 0.0)
                binance_trade_volume_1m_base = float(volume_snapshot.get("local_volume_1m_base", binance_trade_volume_1m_base) or 0.0)
                binance_trade_volume_1m_usd = float(volume_snapshot.get("local_volume_1m_usd", binance_trade_volume_1m_usd) or 0.0)
                binance_trade_count_1m = int(volume_snapshot.get("trade_count_1m", binance_trade_count_1m) or 0)
                binance_trade_window_seconds = 60
                global_volume_24h_usd = float(volume_snapshot.get("global_volume_24h_usd", global_volume_24h_usd) or 0.0)
                global_volume_source = self._normalize_global_source_label(volume_snapshot.get("global_volume_source", global_volume_source))
                global_volume_status = str(volume_snapshot.get("global_volume_status", global_volume_status) or global_volume_status)
                volume_data_status = str(volume_snapshot.get("volume_status", volume_data_status) or volume_data_status)
                trade_volume_status = str(volume_snapshot.get("volume_status", trade_volume_status) or trade_volume_status)
                if str(volume_snapshot.get("error_message", "") or "").strip():
                    trade_volume_error = str(volume_snapshot.get("error_message", "") or "")
                volume_source = str(volume_snapshot.get("local_volume_source", volume_snapshot.get("volume_source", "binance_pair")) or "binance_pair")
                data_source = str(volume_snapshot.get("volume_source", "live_trades") or "live_trades")
            except Exception as ex:
                self.logger.warning("CryptoVolumeManager diagnostics failed for %s: %s", symbol_norm, ex)

        snapshot_timestamp = str(latest_candle_1m.get("timestamp", "") or latest_snapshot.get("timestamp", "") or "")
        if volume_snapshot is None:
            data_source = "rest_bars+trades fallback" if trade_volume_status == "OK" else "snapshot_fallback"
            volume_source = "rest_bars+trades fallback" if trade_volume_status == "OK" else "snapshot_fallback"
            fallback_latest_bar_age_seconds = 999999.0
            if latest_candle_dt is not None:
                fallback_latest_bar_age_seconds = max(0.0, (now_utc - latest_candle_dt).total_seconds())
            fallback_data_stale = bool(inferred_asset_type == "crypto" and fallback_latest_bar_age_seconds > 15.0)
            fallback_websocket_stale = bool(inferred_asset_type == "crypto")
            fallback_status = "FALLBACK_USED" if trade_volume_status == "OK" else "STALE"
            volume_data_status = fallback_status
            if volume_validation is None:
                fallback_reason = "CryptoVolumeManager snapshot unavailable"
                if trade_volume_error:
                    fallback_reason = f"{fallback_reason}: {trade_volume_error}"
                volume_validation = {
                    "is_valid": False,
                    "volume_valid_for_live_analysis": False,
                    "status": fallback_status,
                    "reason": fallback_reason,
                    "last_update": snapshot_timestamp,
                    "source": volume_source,
                    "data_stale": fallback_data_stale,
                    "websocket_stale": fallback_websocket_stale,
                    "latest_bar_age_seconds": fallback_latest_bar_age_seconds,
                    "volume_has_clear_unit": False,
                }
            if inferred_asset_type == "crypto":
                global_volume_source = self._normalize_global_source_label(global_volume_source)

        ai_max_spread_allowed = float(getattr(self, "_ai_max_spread_allowed", getattr(self.settings, "ai_max_spread_allowed", 0.05)) or 0.05)
        spread_check_ok = self._is_spread_allowed(asset_type=inferred_asset_type, spread=spread, price=price)
        spread_check_current = spread
        if inferred_asset_type == "crypto":
            spread_check_current = ((spread / max(price, 1e-8)) * 100.0) if price > 0 else float("inf")
        ai_min_volume_24h_usd = float(getattr(self.settings, "ai_min_volume_24h_usd", 100000.0) or 100000.0)
        ai_min_execution_confidence = float(getattr(self.settings, "ai_min_execution_confidence", 60.0) or 60.0)
        ai_target_profit = float(self._ai_target_profit_per_operation_value(inferred_asset_type))
        ai_fees_buffer = float(getattr(self.settings, "ai_fees_buffer", 0.02) or 0.02)
        ai_slippage_buffer = float(getattr(self.settings, "ai_slippage_buffer", 0.03) or 0.03)
        ai_minimum_profit = float(getattr(self.settings, "ai_minimum_profit", 0.05) or 0.05)
        funds = self.database.get_bot_funds(account_id) or {}
        available_capital = float(funds.get("available_capital", 0.0) or 0.0)
        max_position_size = float(funds.get("max_position_size", 0.0) or 0.0)
        dashboard = self.database.dashboard_summary(account_id)
        account_daily_pnl = float(dashboard.get("daily_pnl", 0.0) or 0.0)
        risk_daily_loss_ok = self.risk_manager.can_trade(current_daily_pnl=account_daily_pnl)
        try:
            broker_supported = self._is_broker_supported(symbol=symbol_norm, asset_type=inferred_asset_type)
        except Exception:
            broker_supported = False

        signal_limit_price = float((latest_signal or {}).get("suggested_limit_price", 0.0) or (latest_signal or {}).get("entry_price", 0.0) or 0.0)
        signal_take_profit = float((latest_signal or {}).get("take_profit_price", 0.0) or 0.0)
        expected_net_edge = signal_take_profit - signal_limit_price - ai_fees_buffer - ai_slippage_buffer

        blocked_reasons: list[str] = []
        raw_blocked = str((latest_decision or {}).get("blocked_reason", "") or "")
        if raw_blocked:
            blocked_reasons.extend([token.strip() for token in raw_blocked.split("|") if token.strip()])

        signal_reason = str((latest_signal or {}).get("reason", "") or "")
        marker = "blocked_reason:"
        if marker in signal_reason.lower():
            idx = signal_reason.lower().find(marker)
            tail = signal_reason[idx + len(marker):]
            blocked_reasons.extend([token.strip() for token in tail.split("|") if token.strip()])

        blocked_reasons_unique: list[str] = []
        seen: set[str] = set()
        for reason in blocked_reasons:
            normalized_reason = str(reason or "").strip()
            key = normalized_reason.lower()
            if "below_average_cost" in key:
                continue
            if "target ia no alcanzable ahora" in key:
                continue
            if key in seen:
                continue
            seen.add(key)
            blocked_reasons_unique.append(normalized_reason)

        signal_type = str((latest_signal or {}).get("signal_type", "") or "").upper().strip()
        allow_watch_as_buy_small = bool(getattr(self.settings, "ai_watch_as_buy_small", False))
        if allow_watch_as_buy_small and signal_type == "WATCH":
            signal_type = "BUY_SMALL"
        confidence = float((latest_signal or {}).get("confidence_score", 0.0) or 0.0)
        actual_signal_engine = self._trade_decision_engine_from_reason(signal_reason)
        requested_signal_engine = self._requested_decision_engine_from_reason(signal_reason)
        latest_signal_model_version = str((latest_signal or {}).get("model_version", "") or "").strip()
        current_approved_model = str(self.registry.approved_version() or "").strip()
        current_approved_model_available = bool(self.registry.approved_model_available())
        is_buy_signal = signal_type in {"BUY", "BUY_SMALL"}
        is_short_signal = signal_type == "SELL_SHORT"
        is_entry_signal = is_buy_signal or is_short_signal
        auto_enabled_for_asset = self._is_effective_auto_enabled_for_symbol(
            runtime=runtime,
            account_name=account_name,
            asset_type=inferred_asset_type,
            symbol=symbol_norm,
        )

        liquidity_check_state = "APPLIES"
        liquidity_check_ok = True
        liquidity_current = global_volume_24h_usd if inferred_asset_type == "crypto" else binance_pair_volume_24h_usd
        if inferred_asset_type == "crypto":
            if global_volume_status == "OK" and not global_volume_warning:
                liquidity_check_ok = global_volume_24h_usd >= ai_min_volume_24h_usd
            elif volume_data_status == "OK":
                liquidity_current = binance_pair_volume_24h_usd
                liquidity_check_ok = binance_pair_volume_24h_usd >= ai_min_volume_24h_usd
            elif global_volume_status in {"ERROR", "STALE"}:
                liquidity_check_state = "SKIPPED_GLOBAL_SOURCE"
            elif global_volume_warning:
                liquidity_check_state = "SKIPPED_SUSPECT_GLOBAL_VOLUME"
            else:
                liquidity_check_state = "SKIPPED_INSUFFICIENT_DATA"
        else:
            if volume_data_status != "OK":
                liquidity_check_state = "SKIPPED_INSUFFICIENT_DATA"
            else:
                liquidity_check_ok = binance_pair_volume_24h_usd >= ai_min_volume_24h_usd

        signal_available = latest_signal is not None

        checks = [
            {
                "name": "asset_auto_enabled",
                "ok": auto_enabled_for_asset,
                "current": auto_enabled_for_asset,
                "required": True,
            },
            {
                "name": "signal_only_disabled",
                "ok": not bool(runtime.get("signal_only_mode", 1)),
                "current": bool(runtime.get("signal_only_mode", 1)),
                "required": False,
            },
            {
                "name": "kill_switch_off",
                "ok": not bool(runtime.get("kill_switch", 0)),
                "current": bool(runtime.get("kill_switch", 0)),
                "required": True,
            },
            {
                "name": "buy_signal_available",
                "ok": is_entry_signal,
                "current": signal_type if signal_type else "NO_SIGNAL",
                "required": True,
            },
            {
                "name": "confidence_threshold",
                "ok": confidence >= ai_min_execution_confidence,
                "current": confidence,
                "required": ai_min_execution_confidence,
                "state": "APPLIES" if (signal_available and is_buy_signal) else ("SKIPPED_NO_SIGNAL" if not signal_available else "SKIPPED_NOT_BUY_SIGNAL"),
            },
            {
                "name": "spread_threshold",
                "ok": spread_check_ok,
                "current": spread_check_current,
                "required": ai_max_spread_allowed,
                "state": "APPLIES" if is_buy_signal else "SKIPPED_NOT_BUY_SIGNAL",
            },
            {
                "name": "volume_24h_threshold",
                "ok": liquidity_check_ok,
                "current": liquidity_current,
                "required": ai_min_volume_24h_usd,
                "state": liquidity_check_state,
            },
            {
                "name": "available_capital",
                "ok": available_capital > 0.0,
                "current": available_capital,
                "required": ">0",
                "state": "APPLIES" if is_buy_signal else "SKIPPED_NOT_BUY_SIGNAL",
            },
            {
                "name": "max_position_size",
                "ok": max_position_size > 0.0,
                "current": max_position_size,
                "required": ">0",
                "state": "APPLIES" if is_buy_signal else "SKIPPED_NOT_BUY_SIGNAL",
            },
            {
                "name": "risk_daily_loss",
                "ok": risk_daily_loss_ok,
                "current": account_daily_pnl,
                "required": f">{-1.0 * float(self.risk_manager.max_daily_loss):.6f}",
                "state": "APPLIES" if is_buy_signal else "SKIPPED_NOT_BUY_SIGNAL",
            },
            {
                "name": "broker_support",
                "ok": broker_supported,
                "current": broker_supported,
                "required": True,
                "state": "APPLIES" if is_buy_signal else "SKIPPED_NOT_BUY_SIGNAL",
            },
            {
                "name": "expected_net_edge",
                "ok": expected_net_edge > 0.0,
                "current": expected_net_edge,
                "required": ">0",
                "state": "APPLIES" if (signal_available and is_buy_signal and signal_limit_price > 0.0 and signal_take_profit > 0.0) else ("SKIPPED_NO_SIGNAL" if not signal_available else "SKIPPED_NO_TP"),
            },
        ]

        blocked_reasons_for_entry = list(blocked_reasons_unique)
        if not is_entry_signal:
            current_signal = signal_type if signal_type else "NO_SIGNAL"
            blocked_reasons_for_entry = [f"Señal actual no habilita compra ({current_signal})"]
            if requested_signal_engine == "model" and actual_signal_engine == "heuristic_fallback":
                if current_approved_model and not current_approved_model_available:
                    blocked_reasons_for_entry.append("Modo modelo con referencia aprobada rota: falta el archivo del modelo aprobado, usando heurística de respaldo")
                else:
                    blocked_reasons_for_entry.append("Modo modelo sin modelo aprobado activo: usando heurística de respaldo")

        recommendations: list[dict[str, Any]] = []
        if not auto_enabled_for_asset:
            recommendations.append(
                {
                    "setting": "auto_trade_asset",
                    "current": False,
                    "suggested": True,
                    "risk": "medio",
                    "reason": "La ejecucion automatica para este tipo de activo esta pausada.",
                }
            )
        if bool(runtime.get("signal_only_mode", 1)):
            recommendations.append(
                {
                    "setting": "signal_only_mode",
                    "current": True,
                    "suggested": False,
                    "risk": "alto",
                    "reason": "En modo solo señales no se envian ordenes automaticas.",
                }
            )
        if requested_signal_engine == "model" and actual_signal_engine == "heuristic_fallback":
            fallback_reason = (
                "La cuenta está en modo modelo, pero no hay modelo aprobado activo en este momento."
                if not current_approved_model
                else (
                    "La cuenta está en modo modelo, pero la referencia del modelo aprobado no tiene archivo disponible en disco; por eso cayó a heurística de respaldo."
                    if not current_approved_model_available
                    else "La cuenta está en modo modelo y sí hay un modelo aprobado activo, pero la última señal parece venir de un ciclo anterior o de respaldo heurístico."
                )
            )
            recommendations.append(
                {
                    "setting": "scanner_decision_engine",
                    "current": "model",
                    "suggested": (
                        "aprobar_modelo_o_usar_heuristica"
                        if not current_approved_model
                        else ("reaprobar_modelo_existente_o_aprobar_uno_nuevo" if not current_approved_model_available else "forzar_nueva_señal_con_modelo_aprobado")
                    ),
                    "risk": "bajo",
                    "reason": fallback_reason,
                }
            )
        if is_buy_signal and not spread_check_ok:
            recommendations.append(
                {
                    "setting": "AI_MAX_SPREAD_ALLOWED",
                    "current": ai_max_spread_allowed,
                    "suggested": round(max(spread_check_current * 1.15, ai_max_spread_allowed + 0.01), 6),
                    "risk": "alto",
                    "reason": "Subir spread permitido aumenta entradas en mercados mas caros y con peor fill.",
                }
            )
        if liquidity_check_state == "APPLIES" and not liquidity_check_ok:
            recommendations.append(
                {
                    "setting": "AI_MIN_VOLUME_24H_USD",
                    "current": ai_min_volume_24h_usd,
                    "suggested": round(max(1000.0, liquidity_current * 0.7), 2),
                    "risk": "medio-alto",
                    "reason": "Bajar liquidez minima permite operar activos menos liquidos con mayor slippage.",
                }
            )
        if is_buy_signal and confidence < ai_min_execution_confidence and confidence > 0:
            recommendations.append(
                {
                    "setting": "AI_MIN_EXECUTION_CONFIDENCE",
                    "current": ai_min_execution_confidence,
                    "suggested": round(max(45.0, confidence - 3.0), 2),
                    "risk": "alto",
                    "reason": "Reducir confianza minima aumenta frecuencia de entradas y falsos positivos.",
                }
            )
        if any("target ia no alcanzable ahora" in reason.lower() for reason in blocked_reasons_for_entry):
            recommendations.append(
                {
                    "setting": "AI_TARGET_PROFIT_PER_OPERATION_CRYPTOS" if inferred_asset_type == "crypto" else "AI_TARGET_PROFIT_PER_OPERATION_STOCKS",
                    "current": ai_target_profit,
                    "suggested": round(max(0.01, ai_target_profit * 0.7), 6),
                    "risk": "medio",
                    "reason": "Reducir target facilita entradas, pero baja ganancia esperada por operacion.",
                }
            )

        if latest_signal is None and latest_decision is None:
            other_account = fallback_signal_account or fallback_decision_account
            if other_account and other_account.lower().strip() != account_name_key:
                recommendations.append(
                    {
                        "setting": "active_scanner_account",
                        "current": account_name,
                        "suggested": other_account,
                        "risk": "bajo",
                        "reason": "Hay señales para este símbolo en otra cuenta activa. Cambia la cuenta activa de IA o inicia automatización en esta cuenta.",
                    }
                )

        entry_blockers: list[str] = []
        for item in checks:
            name = str(item.get("name", "check") or "check")
            state = str(item.get("state", "APPLIES") or "APPLIES")
            if state != "APPLIES":
                # Keep the root cause when there is no BUY signal; hide non-applicable checks.
                if name == "buy_signal_available" and not bool(item.get("ok", False)):
                    entry_blockers.append(f"{name}=FAIL")
                continue
            if not bool(item.get("ok", False)):
                entry_blockers.append(f"{name}=FAIL")
        for blocked in blocked_reasons_for_entry:
            if not is_entry_signal and "spread demasiado alto" in str(blocked).lower():
                continue
            text = str(blocked or "").strip()
            if text:
                entry_blockers.append(f"blocked_reason:{text}")

        return {
            "symbol": symbol_norm,
            "asset_type": inferred_asset_type,
            "account_name": account_name,
            "market": {
                "price": price,
                "spread": spread,
                "spread_pct": spread_pct,
                "binance_pair_volume_1m_base": binance_pair_volume_1m_base,
                "binance_pair_volume_5m_base": binance_pair_volume_5m_base,
                "binance_pair_volume_15m_base": binance_pair_volume_15m_base,
                "binance_pair_volume_1m_usd": binance_pair_volume_1m_usd,
                "binance_pair_volume_5m_usd": binance_pair_volume_5m_usd,
                "binance_pair_volume_15m_usd": binance_pair_volume_15m_usd,
                "alpaca_pair_volume_1m_base": binance_pair_volume_1m_base,
                "alpaca_pair_volume_5m_base": binance_pair_volume_5m_base,
                "alpaca_pair_volume_15m_base": binance_pair_volume_15m_base,
                "alpaca_pair_volume_1m_usd": binance_pair_volume_1m_usd,
                "alpaca_pair_volume_5m_usd": binance_pair_volume_5m_usd,
                "alpaca_pair_volume_15m_usd": binance_pair_volume_15m_usd,
                "binance_pair_volume_1m": binance_pair_volume_1m_base,
                "binance_pair_volume_5m": binance_pair_volume_5m_base,
                "binance_pair_volume_15m": binance_pair_volume_15m_base,
                "alpaca_pair_volume_1m": binance_pair_volume_1m_base,
                "alpaca_pair_volume_5m": binance_pair_volume_5m_base,
                "alpaca_pair_volume_15m": binance_pair_volume_15m_base,
                "volume_5m_minutes_used": min(5, max(candle_count_1m, 0)),
                "volume_15m_minutes_used": min(15, max(candle_count_1m, 0)),
                "volume_minutes_available": max(candle_count_1m, 0),
                "binance_trade_volume_1m_base": binance_trade_volume_1m_base,
                "binance_trade_volume_1m_usd": binance_trade_volume_1m_usd,
                "binance_trade_volume_1m": binance_trade_volume_1m_base,
                "binance_trade_count_1m": binance_trade_count_1m,
                "binance_trade_window_seconds": binance_trade_window_seconds,
                "alpaca_trade_volume_1m_base": binance_trade_volume_1m_base,
                "alpaca_trade_volume_1m_usd": binance_trade_volume_1m_usd,
                "alpaca_trade_volume_1m": binance_trade_volume_1m_base,
                "alpaca_trade_count_1m": binance_trade_count_1m,
                "alpaca_trade_window_seconds": binance_trade_window_seconds,
                "trade_volume_status": trade_volume_status,
                "trade_volume_error": trade_volume_error,
                "binance_pair_volume_24h_usd": binance_pair_volume_24h_usd,
                "alpaca_pair_volume_24h_usd": binance_pair_volume_24h_usd,
                "global_volume_24h_usd": global_volume_24h_usd,
                "volume_source": volume_source,
                "local_volume_source": volume_source,
                "global_volume_source": global_volume_source,
                "volume_data_status": volume_data_status,
                "volume_validation_status": str((volume_validation or {}).get("status", "UNKNOWN") or "UNKNOWN"),
                "volume_validation_reason": str((volume_validation or {}).get("reason", "") or ""),
                "volume_validation_source": str((volume_validation or {}).get("source", "unknown") or "unknown"),
                "volume_valid_for_live_analysis": bool((volume_validation or {}).get("volume_valid_for_live_analysis", False)),
                "data_stale": bool((volume_validation or {}).get("data_stale", False)),
                "websocket_stale": bool((volume_validation or {}).get("websocket_stale", False)),
                "latest_bar_age_seconds": float((volume_validation or {}).get("latest_bar_age_seconds", 999999.0) or 999999.0),
                "volume_has_clear_unit": bool((volume_validation or {}).get("volume_has_clear_unit", False)),
                "global_volume_status": global_volume_status,
                "global_volume_warning": global_volume_warning,
                "snapshot_timestamp": snapshot_timestamp,
                "data_source": data_source,
            },
            "runtime": {
                "signal_only_mode": bool(runtime.get("signal_only_mode", 1)),
                "paper_trading": bool(runtime.get("paper_trading", 1)),
                "live_trading_enabled": bool(runtime.get("live_trading_enabled", 0)),
                "manual_approval_required": bool(runtime.get("manual_approval_required", 1)),
                "kill_switch": bool(runtime.get("kill_switch", 0)),
                "auto_trade_stocks_enabled": bool(runtime.get("auto_trade_stocks_enabled", 1)),
                "auto_trade_cryptos_enabled": bool(runtime.get("auto_trade_cryptos_enabled", 1)),
            },
            "settings": {
                "AI_MAX_SPREAD_ALLOWED": ai_max_spread_allowed,
                "AI_MIN_VOLUME_24H_USD": ai_min_volume_24h_usd,
                "AI_MIN_EXECUTION_CONFIDENCE": ai_min_execution_confidence,
                "AI_TARGET_PROFIT_PER_OPERATION": ai_target_profit,
                "AI_TARGET_PROFIT_PER_OPERATION_STOCKS": float(self._ai_target_profit_per_operation_value("stock")),
                "AI_TARGET_PROFIT_PER_OPERATION_CRYPTOS": float(self._ai_target_profit_per_operation_value("crypto")),
                "AI_FEES_BUFFER": ai_fees_buffer,
                "AI_SLIPPAGE_BUFFER": ai_slippage_buffer,
                "AI_MINIMUM_PROFIT": ai_minimum_profit,
            },
            "latest_signal": latest_signal,
            "latest_decision": latest_decision,
            "diagnostic_model_info": {
                "current_approved_model": current_approved_model or "heuristic",
                "current_approved_model_available": current_approved_model_available,
                "latest_signal_model_version": latest_signal_model_version or "N/A",
                "latest_signal_engine": actual_signal_engine or "unknown",
                "latest_signal_requested_engine": requested_signal_engine or "unknown",
            },
            "blocked_reasons": blocked_reasons_for_entry,
            "entry_checks": checks,
            "can_enter_now": all(
                bool(item.get("ok", False))
                for item in checks
                if str(item.get("state", "APPLIES")) == "APPLIES"
            ),
            "entry_reason": "; ".join(entry_blockers) if entry_blockers else "Sin bloqueos activos detectados",
            "entry_blockers": entry_blockers,
            "latest_signal_text": "Sin señal registrada para este símbolo" if latest_signal is None else "ok",
            "latest_decision_text": "Sin decisión registrada para este símbolo" if latest_decision is None else "ok",
            "recommendations": recommendations,
        }

    def _is_auto_execution_paused_for_asset(
        self,
        *,
        runtime: dict[str, Any],
        asset_type: str,
        initiated_by: str,
        symbol: str,
        account_name: str,
    ) -> bool:
        if str(initiated_by or "").lower() not in {"bot_auto", "ai_auto", "automation"}:
            return False
        return not self._is_effective_auto_enabled_for_symbol(
            runtime=runtime,
            account_name=account_name,
            asset_type=asset_type,
            symbol=symbol,
        )

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
        features["account_name"] = account_name
        composite = compute_composite_score(features)
        features["composite_score"] = composite["score"]
        prediction = self.predictor.predict_signal(features)

        current_price = float(features["price"])
        average_cost = float(features["average_cost"])
        has_position = bool(features.get("existing_position", False))
        blocked_reason = self._blocked_buy_reason(symbol=symbol, account_id=account_id, features=features, account_name=account_name)
        signal_type = str(prediction["action"])
        reason = str(prediction["reason"])
        if bool(getattr(self.settings, "ai_watch_as_buy_small", False)) and signal_type == "WATCH":
            signal_type = "BUY_SMALL"
            reason = f"{reason} | WATCH adaptado a BUY_SMALL por AI_WATCH_AS_BUY_SMALL"
        target_profit_per_unit = self._target_profit_per_unit(asset_type=asset_type, price=current_price, account_id=account_id)
        target_take_profit = current_price + target_profit_per_unit
        if signal_type in {"BUY", "BUY_SMALL"}:
            recent_candles = self.market_data.get_candles(symbol=symbol, interval="1m", limit=20)
            recent_high = max((float(candle.get("high", current_price) or current_price) for candle in recent_candles), default=current_price)
            atr = float(features.get("atr", 0.0) or 0.0)
            spread_now = float(features.get("spread", 0.0) or 0.0)
            reachable_buffer = max(
                atr * 1.25,
                current_price * 0.002,
                spread_now * 2.0,
                target_profit_per_unit * 0.5,
            )

        if blocked_reason:
            signal_type = "AVOID" if signal_type in {"BUY", "BUY_SMALL"} else signal_type
            reason = f"{reason} | blocked_reason: {blocked_reason}"

        min_sell_price = self._minimum_sell_price(average_cost)
        if has_position and average_cost > 0 and not self._is_sell_allowed(current_price=current_price, average_cost=average_cost):
            signal_type = "HOLD"
            reason = "Precio debajo del average cost. Venta automatica bloqueada"
        elif has_position and average_cost > 0 and self._is_sell_allowed(current_price=current_price, average_cost=average_cost):
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
                "take_profit_price": max(suggested_limit + target_profit_per_unit, target_take_profit),
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
        has_position = bool(latest.get("existing_position", False))
        latest["protection_status"] = "HOLD" if (has_position and average_cost > 0 and not self._is_sell_allowed(current_price=current_price, average_cost=average_cost)) else "SELL_ALLOWED" if (has_position and average_cost > 0 and self._is_sell_allowed(current_price=current_price, average_cost=average_cost)) else "ACTIVE"
        return latest

    def update_ai_target_profit_per_share(self, value: float) -> None:
        # Backward-compatible wrapper.
        self.update_ai_target_profit_per_operation(value)

    def update_ai_target_profit_per_operation(self, value: float) -> None:
        target = max(float(value or 0.0), 0.0)
        self._ai_target_profit_per_operation_stocks = target
        self._ai_target_profit_per_operation_cryptos = target

    def update_ai_target_profit_per_share_by_asset(self, *, stock_value: float, crypto_value: float) -> None:
        # Backward-compatible wrapper.
        self.update_ai_target_profit_per_operation_by_asset(stock_value=stock_value, crypto_value=crypto_value)

    def update_ai_target_profit_per_operation_by_asset(self, *, stock_value: float, crypto_value: float) -> None:
        self._ai_target_profit_per_operation_stocks = max(float(stock_value or 0.0), 0.0)
        self._ai_target_profit_per_operation_cryptos = max(float(crypto_value or 0.0), 0.0)

    def update_ai_max_spread_allowed(self, value: float) -> None:
        self._ai_max_spread_allowed = max(float(value or 0.0), 0.0)

    def _ai_target_profit_per_share_value(self, asset_type: str = "") -> float:
        # Backward-compatible wrapper.
        return self._ai_target_profit_per_operation_value(asset_type)

    def _ai_target_profit_per_operation_value(self, asset_type: str = "") -> float:
        asset = str(asset_type or "").lower().strip()
        if asset == "crypto":
            return max(float(getattr(self, "_ai_target_profit_per_operation_cryptos", 0.05) or 0.05), 0.0)
        if asset == "stock":
            return max(float(getattr(self, "_ai_target_profit_per_operation_stocks", 0.05) or 0.05), 0.0)
        return max(float(getattr(self, "_ai_target_profit_per_operation_stocks", 0.05) or 0.05), 0.0)

    def _target_profit_per_unit(self, *, asset_type: str, price: float, account_id: int) -> float:
        operation_target = self._ai_target_profit_per_operation_value(asset_type)
        if price <= 0:
            return operation_target
        funds = self.database.get_bot_funds(account_id) or {}
        capital = min(float(funds.get("available_capital", 0.0) or 0.0), float(funds.get("max_position_size", 0.0) or 0.0))
        estimated_qty = max(capital / price, 1.0) if capital > 0 else 1.0
        return max(operation_target / estimated_qty, 0.0)

    def list_signals(self, limit: int = 25) -> list[dict[str, Any]]:
        return self.database.latest_signals(limit=limit)

    @staticmethod
    def _trade_decision_engine_from_reason(reason: Any) -> str:
        text = str(reason or "")
        marker = "decision_engine="
        if marker not in text:
            return "unknown"
        tail = text.split(marker, 1)[1]
        return str(tail.split(";", 1)[0] or "unknown").strip().lower()

    @staticmethod
    def _requested_decision_engine_from_reason(reason: Any) -> str:
        text = str(reason or "")
        marker = "requested_engine="
        if marker not in text:
            return "unknown"
        tail = text.split(marker, 1)[1]
        return str(tail.split(";", 1)[0] or "unknown").strip().lower()

    def list_history(self, account_name: str, limit: int | None = None, actor_filter: str = "all") -> list[dict[str, Any]]:
        account = self.refresh_account_context(account_name)
        trade_limit = int(limit) if limit is not None else 1000000
        rows = self.database.list_trades_enriched(int(account["id"]), limit=trade_limit)
        normalized_filter = str(actor_filter or "all").strip().lower()
        filtered: list[dict[str, Any]] = []
        for row in rows:
            initiated_by = str(row.get("initiated_by", "unknown") or "unknown").strip().lower()
            is_ai = initiated_by in {"bot_auto", "ai_auto", "automation"}
            if normalized_filter == "ia" and not is_ai:
                continue
            if normalized_filter == "normal" and is_ai:
                continue

            payload = dict(row)
            payload["decision_engine"] = self._trade_decision_engine_from_reason(payload.get("signal_reason", ""))
            payload["model_version_display"] = str(payload.get("signal_model_version", "") or "")
            filtered.append(payload)
        return filtered

    def get_scalping_board(self, limit_stocks: int = 6, limit_cryptos: int = 3) -> dict[str, list[dict[str, Any]]]:
        actionable = {"WATCH", "BUY_SMALL", "BUY", "SELL_ALLOWED", "SELL_SHORT"}
        signals = self.list_signals(limit=200)
        by_symbol: dict[str, dict[str, Any]] = {}
        for item in signals:
            symbol = str(item.get("symbol", "")).upper().strip()
            if not symbol:
                continue
            current = by_symbol.get(symbol)
            candidate_score = float(item.get("confidence_score", 0.0) or 0.0)
            current_score = float(current.get("confidence_score", 0.0) or 0.0) if current else -1.0
            if current is None or candidate_score > current_score:
                by_symbol[symbol] = item

        ranked = sorted(
            by_symbol.values(),
            key=lambda row: (float(row.get("confidence_score", 0.0) or 0.0), str(row.get("timestamp", ""))),
            reverse=True,
        )
        stock_rows = [row for row in ranked if str(row.get("asset_type", "")).lower() == "stock" and str(row.get("signal_type", "")).upper() in actionable]
        crypto_rows = [row for row in ranked if str(row.get("asset_type", "")).lower() == "crypto" and str(row.get("signal_type", "")).upper() in actionable]

        if not stock_rows:
            stock_rows = [row for row in ranked if str(row.get("asset_type", "")).lower() == "stock"]
        if not crypto_rows:
            crypto_rows = [row for row in ranked if str(row.get("asset_type", "")).lower() == "crypto"]

        return {
            "stocks": stock_rows[: max(int(limit_stocks), 1)],
            "cryptos": crypto_rows[: max(int(limit_cryptos), 1)],
        }

    def list_signal_recommendation_history(self, limit: int | None = None) -> list[dict[str, Any]]:
        signal_limit = max(int(limit) * 4, 80) if limit is not None else 1000000
        signals = self.list_signals(limit=signal_limit)
        rows: list[dict[str, Any]] = []
        for signal in signals:
            signal_id = int(signal.get("id", 0) or 0)
            if signal_id <= 0:
                continue
            outcome = self.database.get_signal_outcome(signal_id)
            if not outcome:
                continue
            generated_at_raw = str(signal.get("timestamp", "") or "")
            if not generated_at_raw:
                continue
            try:
                generated_at = datetime.fromisoformat(generated_at_raw)
            except ValueError:
                continue

            candidates: list[dict[str, Any]] = []
            for window in (5, 15, 30, 60):
                profit_key = f"max_profit_{window}m"
                drawdown_key = f"max_drawdown_{window}m"
                result_key = f"result_{window}m"
                if outcome.get(result_key) is None:
                    continue
                candidates.append(
                    {
                        "window": window,
                        "result": str(outcome.get(result_key, "neutral")),
                        "max_profit": float(outcome.get(profit_key, 0.0) or 0.0),
                        "max_drawdown": float(outcome.get(drawdown_key, 0.0) or 0.0),
                    }
                )
            if not candidates:
                continue

            best = max(candidates, key=lambda row: float(row.get("max_profit", 0.0) or 0.0))
            best_window = int(best["window"])
            exit_at = generated_at + timedelta(minutes=best_window)
            best_profit = float(best.get("max_profit", 0.0) or 0.0)
            worst_drawdown = min(float(item.get("max_drawdown", 0.0) or 0.0) for item in candidates)
            action = str(signal.get("signal_type", ""))
            hypothetical_pct = best_profit if action in {"BUY", "BUY_SMALL", "WATCH", "SELL_ALLOWED"} else worst_drawdown
            entry_price = float(signal.get("entry_price", 0.0) or 0.0)
            suggested_limit_price = float(signal.get("suggested_limit_price", 0.0) or 0.0)
            exit_limit_price = float(signal.get("take_profit_price", 0.0) or 0.0)
            rows.append(
                {
                    "symbol": str(signal.get("symbol", "N/A")),
                    "asset_type": str(signal.get("asset_type", "N/A")),
                    "action": action,
                    "generated_at": generated_at.isoformat(),
                    "entry_at": generated_at.isoformat(),
                    "entry_price": entry_price,
                    "entry_limit_price": suggested_limit_price,
                    "exit_limit_price": exit_limit_price,
                    "recommended_exit_at": exit_at.isoformat(),
                    "recommended_window_m": best_window,
                    "best_profit_pct": best_profit,
                    "worst_drawdown_pct": worst_drawdown,
                    "hypothetical_pnl_pct": hypothetical_pct,
                    "result": str(best.get("result", "neutral")),
                }
            )

        rows.sort(key=lambda row: str(row.get("generated_at", "")), reverse=True)
        return rows if limit is None else rows[: max(int(limit), 1)]

    def train_model(self) -> dict[str, Any]:
        evaluated = self.database.count_evaluated_outcomes()
        if evaluated < 200:
            return {
                "trained": False,
                "reason": "No hay suficientes outcomes reales para entrenar.",
                "number_of_samples": evaluated,
            }
        return self.trainer.train_general_model()

    def ensure_automation_running(self, account_name: str) -> dict[str, Any]:
        status = self.get_automation_status(account_name)
        running = all(status.get(key) == "Running" for key in ("collector", "scanner", "labeler", "news_social", "trainer"))
        if running:
            with self._worker_lock:
                active_account = str(self._active_account_for_workers or "").strip()
            if active_account == str(account_name or "").strip():
                return status
            # Workers are running but bound to another account; restart on requested account.
            self.pause_automation()
        return self.start_automation(account_name)

    def approve_latest_model(self) -> str:
        version = self.registry.latest_version()
        if not version:
            raise ValueError("No hay modelo para aprobar")
        return self.approve_model_version(version)

    def _training_run_by_model_version(self, version: str) -> dict[str, Any] | None:
        version_text = str(version or "").strip()
        if not version_text:
            return None
        for row in self.database.list_training_runs(limit=1000):
            if str(row.get("model_version", "") or "").strip() == version_text:
                return row
        return None

    def approve_model_version(self, version: str) -> str:
        if not version:
            raise ValueError("Version de modelo invalida")
        if version not in self.registry.available_versions():
            raise ValueError(f"Modelo no encontrado: {version}")
        training_run = self._training_run_by_model_version(version)
        if training_run is None:
            raise ValueError(
                f"No se puede aprobar {version}: no tiene métricas persistidas de entrenamiento para compararlo con modelos futuros"
            )
        self.registry.approve_model(version)
        self.database.insert_decision_log(
            {
                "timestamp": self._now_iso(),
                "symbol": "*",
                "decision": "MODEL_APPROVED",
                "reason": f"version={version}",
                "blocked_reason": "",
                "raw_context_json": {"version": version},
            }
        )
        return version

    def freeze_candidate_version(self, version: str) -> str:
        if not version:
            raise ValueError("Version de modelo invalida")
        self.registry.freeze_candidate(version)
        self.database.insert_decision_log(
            {
                "timestamp": self._now_iso(),
                "symbol": "*",
                "decision": "MODEL_FROZEN",
                "reason": f"version={version}",
                "blocked_reason": "",
                "raw_context_json": {"version": version},
            }
        )
        return version

    def clear_frozen_candidate(self) -> None:
        frozen = self.registry.frozen_candidate()
        self.registry.clear_frozen_candidate()
        self.database.insert_decision_log(
            {
                "timestamp": self._now_iso(),
                "symbol": "*",
                "decision": "MODEL_UNFROZEN",
                "reason": f"version={frozen or 'none'}",
                "blocked_reason": "",
                "raw_context_json": {"version": frozen},
            }
        )

    def delete_model_version(self, version: str) -> str:
        version_text = str(version or "").strip()
        if not version_text:
            raise ValueError("Version de modelo invalida")
        approved = self.registry.approved_version() or ""
        if version_text == approved:
            raise ValueError("No se puede eliminar el modelo actualmente aprobado")
        self.registry.delete_version(version_text)
        self.database.insert_decision_log(
            {
                "timestamp": self._now_iso(),
                "symbol": "*",
                "decision": "MODEL_DELETED",
                "reason": f"version={version_text}",
                "blocked_reason": "",
                "raw_context_json": {"version": version_text},
            }
        )
        return version_text

    def set_model_alias(self, version: str, alias: str) -> str:
        version_text = str(version or "").strip()
        if not version_text:
            raise ValueError("Version de modelo invalida")
        self.registry.set_alias(version_text, alias)
        alias_text = self.registry.get_alias(version_text)
        self.database.insert_decision_log(
            {
                "timestamp": self._now_iso(),
                "symbol": "*",
                "decision": "MODEL_ALIAS_SET",
                "reason": f"version={version_text}; alias={alias_text or 'cleared'}",
                "blocked_reason": "",
                "raw_context_json": {"version": version_text, "alias": alias_text},
            }
        )
        return alias_text

    def list_model_candidates(self, limit: int = 12) -> dict[str, Any]:
        approved = self.registry.approved_version() or ""
        approved_available = bool(self.registry.approved_model_available())
        latest = self.registry.latest_version() or ""
        frozen = self.registry.frozen_candidate() or ""
        available_versions = set(self.registry.available_versions())
        runs = self.database.list_training_runs(limit=max(int(limit), 1))
        rows: list[dict[str, Any]] = []
        for row in runs:
            version = str(row.get("model_version", "") or "")
            if not version or version not in available_versions:
                continue
            rows.append(
                {
                    **row,
                    "alias": self.registry.get_alias(version),
                    "is_approved": version == approved,
                    "is_latest": version == latest,
                    "is_frozen": version == frozen,
                }
            )
        return {
            "approved": approved,
            "approved_valid": bool(approved and approved_available and any(str(row.get("model_version", "") or "") == approved for row in rows)),
            "latest": latest,
            "frozen": frozen,
            "rows": rows,
        }

    def rollback_model(self) -> str | None:
        version = self.registry.rollback_to_previous()
        self.database.insert_decision_log(
            {
                "timestamp": self._now_iso(),
                "symbol": "*",
                "decision": "MODEL_ROLLBACK",
                "reason": f"version={version or 'sin_cambios'}",
                "blocked_reason": "" if version else "no_previous_version",
                "raw_context_json": {"version": version},
            }
        )
        return version

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
        executed_statuses = {
            "filled",
            "partially_filled",
            "done_for_day",
            "calculated",
        }
        for trade in reversed(trades):
            if self._symbol_key(str(trade.get("symbol", ""))) != symbol_key:
                continue
            status = str(trade.get("status", "") or "").strip().lower()
            qty = float(trade.get("qty", 0.0) or 0.0)
            filled_price = float(trade.get("filled_price", 0.0) or 0.0)
            limit_price = float(trade.get("limit_price", 0.0) or 0.0)
            # Ignore non-executed orders (pending/new/submitted) so they don't create fake positions.
            if filled_price <= 0.0 and status and status not in executed_statuses:
                continue
            price = filled_price if filled_price > 0.0 else limit_price
            if price <= 0.0:
                continue
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

    def place_limit_buy(
        self,
        signal_id: int,
        account_name: str,
        manual_approved: bool,
        initiated_by: str = "bot_auto",
    ) -> dict[str, Any]:
        account = self.refresh_account_context(account_name)
        account_id = int(account["id"])
        runtime = self.database.get_runtime_settings(account_id) or {}
        funds = self.database.get_bot_funds(account_id) or {}
        signal = next((item for item in self.database.latest_signals(limit=100) if int(item["id"]) == int(signal_id)), None)
        if signal is None:
            raise ValueError("Senal no encontrada")

        signal_type = str(signal.get("signal_type", "")).upper().strip()
        asset_type = str(signal.get("asset_type", "")).lower().strip()
        if self._emergency_mode:
            return {
                "status": "emergency_mode",
                "reason": self._emergency_reason or "Entradas pausadas por estabilidad",
            }
        allowed_buy_actions = {"BUY", "BUY_SMALL"}
        if bool(getattr(self.settings, "ai_watch_as_buy_small", False)):
            allowed_buy_actions.add("WATCH")
        if signal_type not in allowed_buy_actions:
            raise ValueError(f"IA no ejecuta compras para senales tipo {signal_type or 'N/A'}")

        if self._is_auto_execution_paused_for_asset(
            runtime=runtime,
            asset_type=asset_type,
            initiated_by=initiated_by,
            symbol=str(signal.get("symbol", "") or ""),
            account_name=account_name,
        ):
            return {
                "status": "paused_asset_type",
                "asset_type": asset_type,
                "reason": f"Auto trading pausado para {asset_type}",
            }

        confidence = float(signal.get("confidence_score", 0.0) or 0.0)
        min_confidence = float(getattr(self.settings, "ai_min_execution_confidence", 60.0) or 60.0)
        if confidence < min_confidence:
            raise ValueError(f"Confianza insuficiente para ejecutar compra ({confidence:.2f} < {min_confidence:.2f})")

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

        take_profit = float(signal.get("take_profit_price", 0.0) or 0.0)
        expected_net_edge = take_profit - limit_price - float(self.settings.ai_fees_buffer) - float(self.settings.ai_slippage_buffer)
        if expected_net_edge <= 0:
            raise ValueError(
                "Compra bloqueada: expectativa de ganancia neta no positiva (riesgo de loss esperado)"
            )

        qty = round(capital / limit_price, 6)
        if qty <= 0:
            raise ValueError("Cantidad calculada invalida")
        if bool(runtime.get("signal_only_mode", 1)):
            return {"status": "signal_only", "qty": qty, "limit_price": limit_price}

        self._ensure_vpn_ready_for_trading("Buy order")

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
                "initiated_by": initiated_by,
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

        immediate_exit = self._place_immediate_ai_target_exit(
            account_id=account_id,
            symbol=str(signal["symbol"]),
            asset_type=str(signal["asset_type"]),
            buy_order=order,
            requested_qty=float(qty),
            configured_take_profit=float(signal.get("take_profit_price", 0.0) or 0.0),
            initiated_by=initiated_by,
        )
        return {
            "status": "submitted",
            "order": order,
            "qty": qty,
            "limit_price": limit_price,
            "immediate_exit": immediate_exit,
        }

    @staticmethod
    def _runtime_futures_leverage(runtime: dict[str, Any], default_value: int) -> int:
        return max(int(runtime.get("futures_leverage", default_value) or default_value), 1)

    def place_limit_short(
        self,
        signal_id: int,
        account_name: str,
        manual_approved: bool,
        initiated_by: str = "bot_auto",
    ) -> dict[str, Any]:
        account = self.refresh_account_context(account_name)
        account_id = int(account["id"])
        runtime = self.database.get_runtime_settings(account_id) or {}
        funds = self.database.get_bot_funds(account_id) or {}
        signal = next((item for item in self.database.latest_signals(limit=200) if int(item["id"]) == int(signal_id)), None)
        if signal is None:
            raise ValueError("Senal no encontrada")

        signal_type = str(signal.get("signal_type", "")).upper().strip()
        asset_type = str(signal.get("asset_type", "")).lower().strip()
        if signal_type != "SELL_SHORT":
            raise ValueError(f"IA no ejecuta short para senales tipo {signal_type or 'N/A'}")
        if asset_type != "crypto":
            raise ValueError("Short automatico solo habilitado para cryptos en futures")
        if not bool(runtime.get("futures_enable_short", getattr(self.settings, "ai_futures_enable_short", True))):
            raise ValueError("Short deshabilitado en configuracion de futuros")

        if self._is_auto_execution_paused_for_asset(
            runtime=runtime,
            asset_type=asset_type,
            initiated_by=initiated_by,
            symbol=str(signal.get("symbol", "") or ""),
            account_name=account_name,
        ):
            return {
                "status": "paused_asset_type",
                "asset_type": asset_type,
                "reason": f"Auto trading pausado para {asset_type}",
            }

        confidence = float(signal.get("confidence_score", 0.0) or 0.0)
        min_confidence = float(getattr(self.settings, "ai_min_execution_confidence", 60.0) or 60.0)
        if confidence < min_confidence:
            raise ValueError(f"Confianza insuficiente para ejecutar short ({confidence:.2f} < {min_confidence:.2f})")
        if bool(runtime.get("kill_switch", 0)):
            raise ValueError("Kill switch activo")
        if bool(runtime.get("manual_approval_required", 1)) and not manual_approved:
            raise ValueError("Se requiere aprobacion manual")
        if not bool(runtime.get("paper_trading", 1)) and not bool(runtime.get("live_trading_enabled", 0)):
            raise ValueError("Trading live deshabilitado")

        leverage_default = int(getattr(self.settings, "crypto_futures_default_leverage", 1) or 1)
        leverage_max = max(int(getattr(self.settings, "crypto_futures_max_leverage", 20) or 20), 1)
        leverage = min(self._runtime_futures_leverage(runtime, leverage_default), leverage_max)

        capital = min(float(funds.get("available_capital", 0.0) or 0.0), float(funds.get("max_position_size", 0.0) or 0.0))
        if capital <= 0:
            raise ValueError("No hay capital disponible")
        limit_price = float(signal.get("suggested_limit_price", 0.0) or signal.get("entry_price", 0.0) or 0.0)
        if limit_price <= 0:
            raise ValueError("Precio limit invalido")

        qty = round((capital * float(leverage)) / limit_price, 6)
        if qty <= 0:
            raise ValueError("Cantidad calculada invalida")
        if bool(runtime.get("signal_only_mode", 1)):
            return {"status": "signal_only", "qty": qty, "limit_price": limit_price, "leverage": leverage}

        self._ensure_vpn_ready_for_trading("Short order")

        if hasattr(self.broker, "set_futures_leverage"):
            try:
                self.broker.set_futures_leverage(symbol=str(signal["symbol"]), leverage=leverage)
            except Exception as ex:
                self.logger.warning("No se pudo aplicar leverage %sx en %s: %s", leverage, signal["symbol"], ex)

        order = self.order_manager.create_limit_order(
            symbol=str(signal["symbol"]),
            qty=qty,
            side="sell",
            limit_price=limit_price,
            time_in_force="gtc",
        )
        self.database.insert_trade(
            {
                "timestamp": self._now_iso(),
                "account_id": account_id,
                "symbol": str(signal["symbol"]),
                "asset_type": str(signal["asset_type"]),
                "side": "sell",
                "order_type": "limit",
                "qty": qty,
                "limit_price": limit_price,
                "filled_price": float(order.get("filled_avg_price", 0.0) or 0.0),
                "fees": 0.0,
                "status": str(order.get("status", "submitted")),
                "initiated_by": initiated_by,
                "broker_order_id": str(order.get("id", "")),
                "signal_id": int(signal_id),
                "created_at": self._now_iso(),
            }
        )
        return {
            "status": "submitted",
            "order": order,
            "qty": qty,
            "limit_price": limit_price,
            "leverage": leverage,
        }

    def _place_immediate_ai_target_exit(
        self,
        account_id: int,
        symbol: str,
        asset_type: str,
        buy_order: dict[str, Any],
        requested_qty: float,
        configured_take_profit: float,
        initiated_by: str,
    ) -> dict[str, Any]:
        symbol_normalized = str(symbol).upper().strip()
        resolved_order = self._wait_ai_order_progress(buy_order=buy_order, requested_qty=requested_qty)
        filled_qty = float(resolved_order.get("filled_qty", 0.0) or 0.0)
        if filled_qty <= 0:
            return {
                "status": "pending_fill",
                "reason": "buy_order_not_filled_yet",
                "buy_order_id": str(resolved_order.get("id", "")),
            }

        existing = self._find_pending_sell_order(symbol=symbol_normalized)
        if existing is not None:
            return {
                "status": "pending_existing",
                "order_id": str(existing.get("id", "")),
                "limit_price": float(existing.get("limit_price", 0.0) or 0.0),
            }

        filled_price = float(
            resolved_order.get("filled_avg_price", 0.0)
            or resolved_order.get("avg_entry_price", 0.0)
            or buy_order.get("filled_avg_price", 0.0)
            or buy_order.get("avg_entry_price", 0.0)
            or buy_order.get("limit_price", 0.0)
            or 0.0
        )
        if filled_price <= 0:
            return {
                "status": "pending_price",
                "reason": "filled_price_unavailable",
                "filled_qty": filled_qty,
            }

        min_sell_price = self._minimum_sell_price(filled_price)
        target_price = float(configured_take_profit or 0.0)
        if target_price <= 0:
            target_price = min_sell_price
        limit_price = round(max(target_price, min_sell_price), 6)

        sell_order = self.order_manager.create_limit_order(
            symbol=symbol_normalized,
            qty=float(filled_qty),
            side="sell",
            limit_price=limit_price,
            time_in_force="gtc",
        )
        self.database.insert_trade(
            {
                "timestamp": self._now_iso(),
                "account_id": account_id,
                "symbol": symbol_normalized,
                "asset_type": str(asset_type),
                "side": "sell",
                "order_type": "limit",
                "qty": float(filled_qty),
                "limit_price": limit_price,
                "filled_price": float(sell_order.get("filled_avg_price", 0.0) or 0.0),
                "fees": 0.0,
                "status": str(sell_order.get("status", "submitted")),
                "initiated_by": f"{initiated_by}_target_immediate",
                "broker_order_id": str(sell_order.get("id", "")),
                "signal_id": None,
                "created_at": self._now_iso(),
            }
        )
        self.logger.info(
            "IA salida inmediata colocada %s qty=%.8f limit=%.8f",
            symbol_normalized,
            filled_qty,
            limit_price,
        )
        return {
            "status": "submitted",
            "order_id": str(sell_order.get("id", "")),
            "limit_price": limit_price,
            "qty": float(filled_qty),
        }

    def _wait_ai_order_progress(
        self,
        buy_order: dict[str, Any],
        requested_qty: float,
        max_attempts: int = 12,
        sleep_seconds: float = 0.35,
    ) -> dict[str, Any]:
        order_id = str(buy_order.get("id", "")).strip()
        current = dict(buy_order)
        if not order_id:
            return current

        for _ in range(max_attempts):
            status = str(current.get("status", "")).lower().strip()
            filled_qty = float(current.get("filled_qty", 0.0) or 0.0)
            if filled_qty >= (requested_qty - 1e-8) or status in {"filled", "canceled", "rejected", "expired"}:
                return current
            time.sleep(sleep_seconds)
            try:
                current = self.broker.get_order(order_id)
            except Exception:
                break
        return current

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
    def _aggregate_stream_bars(bars: list[dict[str, Any]], window: int) -> list[dict[str, Any]]:
        if window <= 0:
            return []

        aggregated: list[dict[str, Any]] = []
        chunk: list[dict[str, Any]] = []
        for bar in bars:
            chunk.append(bar)
            if len(chunk) < window:
                continue

            opens = float(chunk[0].get("open", chunk[0].get("close", 0.0)) or 0.0)
            closes = float(chunk[-1].get("close", chunk[-1].get("open", 0.0)) or 0.0)
            highs = [float(row.get("high", 0.0) or 0.0) for row in chunk]
            lows = [float(row.get("low", 0.0) or 0.0) for row in chunk]
            volumes = [float(row.get("volume", 0.0) or 0.0) for row in chunk]
            aggregated.append(
                {
                    "open": opens,
                    "high": max(highs) if highs else closes,
                    "low": min(lows) if lows else closes,
                    "close": closes,
                    "volume": sum(volumes),
                    "timestamp": chunk[-1].get("timestamp"),
                }
            )
            chunk = []

        if chunk:
            opens = float(chunk[0].get("open", chunk[0].get("close", 0.0)) or 0.0)
            closes = float(chunk[-1].get("close", chunk[-1].get("open", 0.0)) or 0.0)
            highs = [float(row.get("high", 0.0) or 0.0) for row in chunk]
            lows = [float(row.get("low", 0.0) or 0.0) for row in chunk]
            volumes = [float(row.get("volume", 0.0) or 0.0) for row in chunk]
            aggregated.append(
                {
                    "open": opens,
                    "high": max(highs) if highs else closes,
                    "low": min(lows) if lows else closes,
                    "close": closes,
                    "volume": sum(volumes),
                    "timestamp": chunk[-1].get("timestamp"),
                }
            )

        return aggregated

    def _aggregate_snapshot_rows_to_minutes(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        aggregated: list[dict[str, Any]] = []
        current_minute: datetime | None = None
        current_bar: dict[str, Any] | None = None

        for row in rows:
            ts = self._parse_iso_timestamp(str(row.get("timestamp", "") or ""))
            if ts is None:
                continue
            minute_key = ts.replace(second=0, microsecond=0)

            close_value = float(row.get("close", row.get("price", 0.0)) or 0.0)
            open_value = float(row.get("open", close_value) or close_value)
            high_value = float(row.get("high", close_value) or close_value)
            low_value = float(row.get("low", close_value) or close_value)
            volume_value = float(row.get("volume", 0.0) or 0.0)

            if current_minute is None or minute_key != current_minute:
                if current_bar is not None:
                    aggregated.append(current_bar)
                current_minute = minute_key
                current_bar = {
                    "open": open_value,
                    "high": high_value,
                    "low": low_value,
                    "close": close_value,
                    "volume": volume_value,
                    "timestamp": ts.isoformat(),
                }
                continue

            if current_bar is None:
                continue
            current_bar["high"] = max(float(current_bar.get("high", high_value) or high_value), high_value)
            current_bar["low"] = min(float(current_bar.get("low", low_value) or low_value), low_value)
            current_bar["close"] = close_value
            current_bar["volume"] = max(float(current_bar.get("volume", 0.0) or 0.0), volume_value)
            current_bar["timestamp"] = ts.isoformat()

        if current_bar is not None:
            aggregated.append(current_bar)

        return aggregated

    def _stream_market_context(self, symbol: str, account_id: int, asset_type: str) -> dict[str, Any] | None:
        snapshots = self.database.latest_market_snapshots(symbol=symbol, limit=180)
        if not snapshots:
            return None

        snapshot_rows = list(reversed([
            {
                "open": float(row.get("open", row.get("close", 0.0)) or 0.0),
                "high": float(row.get("high", row.get("close", 0.0)) or 0.0),
                "low": float(row.get("low", row.get("close", 0.0)) or 0.0),
                "close": float(row.get("close", row.get("price", 0.0)) or 0.0),
                "volume": float(row.get("volume", 0.0) or 0.0),
                "timestamp": row.get("timestamp"),
            }
            for row in snapshots
        ]))
        bars_1m = self._aggregate_snapshot_rows_to_minutes(snapshot_rows)
        bars_5m = self._aggregate_stream_bars(bars_1m, 5)
        bars_15m = self._aggregate_stream_bars(bars_1m, 15)
        latest = bars_1m[-1] if bars_1m else {}
        price = float(latest.get("close", 0.0) or 0.0)
        quote = self.market_data.runtime_state.get_quote(self.market_data.account_name, symbol, ttl_seconds=30.0)
        if quote is None:
            quote = {}
        if price <= 0.0:
            price = float(self.market_data.get_last_price(symbol))
        if not quote:
            try:
                quote = self.market_data.get_latest_quote(symbol)
            except Exception:
                quote = {}

        return {
            "asset_type": asset_type,
            "price": price,
            "quote": quote,
            "candles_1m": bars_1m,
            "candles_5m": bars_5m,
            "candles_15m": bars_15m,
            "latest_snapshot": snapshots[0],
            "account_id": account_id,
        }

    def place_limit_sell_if_allowed(
        self,
        symbol: str,
        account_name: str,
        manual_approved: bool = True,
        initiated_by: str = "bot_auto",
    ) -> dict[str, Any]:
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

        asset_type = str(position.get("asset_type", "")).lower().strip()
        if self._is_auto_execution_paused_for_asset(
            runtime=runtime,
            asset_type=asset_type,
            initiated_by=initiated_by,
            symbol=str(position.get("symbol", "") or ""),
            account_name=account_name,
        ):
            return {
                "status": "paused_asset_type",
                "asset_type": asset_type,
                "reason": f"Auto trading pausado para {asset_type}",
            }

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

        self._ensure_vpn_ready_for_trading("Sell order")

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
                "initiated_by": initiated_by,
                "broker_order_id": str(order.get("id", "")),
                "signal_id": None,
                "created_at": self._now_iso(),
            }
        )
        return {"status": "submitted", "order": order, "limit_price": limit_price}

    def _build_features(self, symbol: str, asset_type: str, account_id: int) -> dict[str, Any]:
        context = self._stream_market_context(symbol=symbol, account_id=account_id, asset_type=asset_type)
        if context is None:
            candles_1m = self.market_data.get_candles(symbol=symbol, interval="1m", limit=60)
            candles_5m = self.market_data.get_candles(symbol=symbol, interval="5m", limit=60)
            candles_15m = self.market_data.get_candles(symbol=symbol, interval="15m", limit=60)
            price = float(self.market_data.get_last_price(symbol))
            quote = self.market_data.get_latest_quote(symbol)
        else:
            candles_1m = context["candles_1m"]
            candles_5m = context["candles_5m"]
            candles_15m = context["candles_15m"]
            price = float(context["price"])
            quote = context["quote"]
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
        if self.stream_manager.connected:
            return
        if self.broker.runtime_state.in_cooldown(self.broker.account_name):
            self._last_collector_cycle_at = f"{self._now_iso()} | alpaca cooldown"
            return
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
        if self.stream_manager.connected:
            return
        if self.broker.runtime_state.in_cooldown(self.broker.account_name):
            self._last_news_cycle_at = f"{self._now_iso()} | alpaca cooldown"
            return
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
        runtime = self.database.get_runtime_settings(account_id) or {}
        scanner_engine = str(runtime.get("scanner_decision_engine", "heuristic") or "heuristic").strip().lower()
        if scanner_engine not in {"heuristic", "model"}:
            scanner_engine = "heuristic"
        futures_only_mode = bool(runtime.get("futures_only_mode", getattr(self.settings, "crypto_futures_only_mode", True)))
        futures_require_technical = bool(runtime.get("futures_require_technical", getattr(self.settings, "ai_futures_require_technical", True)))
        futures_require_news = bool(runtime.get("futures_require_news", getattr(self.settings, "ai_futures_require_news", False)))
        futures_enable_long = bool(runtime.get("futures_enable_long", getattr(self.settings, "ai_futures_enable_long", True))) and bool(
            getattr(self.settings, "ai_futures_enable_long", True)
        )
        futures_enable_short = bool(runtime.get("futures_enable_short", getattr(self.settings, "ai_futures_enable_short", True))) and bool(
            getattr(self.settings, "ai_futures_enable_short", True)
        )
        focus = self._focus_for_account(account_name)
        focus_stocks_only = bool(focus.get("stocks_only", False))
        focus_cryptos_only = bool(focus.get("cryptos_only", False))
        focus_stocks = set(focus.get("stocks_symbols", set()))
        focus_cryptos = set(focus.get("cryptos_symbols", set()))
        symbols = self._symbols_for_collection(account_name)
        for item in symbols:
            symbol = str(item.get("symbol", "")).upper()
            asset_type = str(item.get("asset_type", "stock"))

            if futures_only_mode and str(asset_type).lower() != "crypto":
                continue

            if not self._is_effective_auto_enabled_for_symbol(
                runtime=runtime,
                account_name=account_name,
                asset_type=asset_type,
                symbol=symbol,
            ):
                continue

            if asset_type.lower() == "stock" and focus_stocks_only:
                if focus_stocks and symbol not in focus_stocks:
                    continue
                if not focus_stocks:
                    continue

            if asset_type.lower() == "crypto" and focus_cryptos_only:
                symbol_key = self._symbol_key(symbol)
                focus_keys = {self._symbol_key(s) for s in focus_cryptos}
                if focus_keys and symbol_key not in focus_keys:
                    continue
                if not focus_keys:
                    continue

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

            volume_1m_current = 0.0
            volume_1m_previous_closed = 0.0
            volume_1m_current_usd = 0.0
            volume_5m_sum_usd = 0.0
            volume_15m_sum_usd = 0.0

            snapshot_rows = list(reversed([
                {
                    "open": float(row.get("open", row.get("close", 0.0)) or 0.0),
                    "high": float(row.get("high", row.get("close", 0.0)) or 0.0),
                    "low": float(row.get("low", row.get("close", 0.0)) or 0.0),
                    "close": float(row.get("close", row.get("price", 0.0)) or 0.0),
                    "volume": float(row.get("volume", 0.0) or 0.0),
                    "timestamp": row.get("timestamp"),
                }
                for row in snapshots
            ]))
            minute_bars = self._aggregate_snapshot_rows_to_minutes(snapshot_rows)
            minute_bars_desc = list(reversed(minute_bars))

            closed_1m = [float(row.get("volume", 0.0) or 0.0) for row in minute_bars_desc[1:61]]
            volume_5m_sum = sum(closed_1m[:5])
            volume_15m_sum = sum(closed_1m[:15])
            volume_5m_sum_usd = latest_price * volume_5m_sum
            volume_15m_sum_usd = latest_price * volume_15m_sum
            avg_volume_1m_20 = sum(closed_1m[:20]) / max(len(closed_1m[:20]), 1)

            trade_volume_status = "OK"
            trade_volume_error = ""
            trade_count_1m = 0
            try:
                trades_1m = self.market_data.get_recent_trade_stats(symbol=symbol, lookback_seconds=60, limit=5000)
                trades_5m = self.market_data.get_recent_trade_stats(symbol=symbol, lookback_seconds=300, limit=5000)
                trades_15m = self.market_data.get_recent_trade_stats(symbol=symbol, lookback_seconds=900, limit=5000)
                volume_1m_current = float(trades_1m.get("volume", 0.0) or 0.0)
                volume_1m_current_usd = float(trades_1m.get("volume_usd", 0.0) or 0.0)
                trade_count_1m = int(trades_1m.get("count", 0) or 0)
                volume_1m_previous_closed = volume_1m_current
                volume_5m_sum = float(trades_5m.get("volume", 0.0) or 0.0)
                volume_15m_sum = float(trades_15m.get("volume", 0.0) or 0.0)
                volume_5m_sum_usd = float(trades_5m.get("volume_usd", 0.0) or 0.0)
                volume_15m_sum_usd = float(trades_15m.get("volume_usd", 0.0) or 0.0)
            except Exception as ex:
                trade_volume_status = "ERROR"
                trade_volume_error = str(ex)

            candles_5m = self._aggregate_stream_bars(minute_bars, 5)
            candle_5m_volumes = [float(candle.get("volume", 0.0) or 0.0) for candle in candles_5m]
            avg_volume_5m_20 = sum(candle_5m_volumes[-20:]) / max(len(candle_5m_volumes[-20:]), 1)

            relative_volume_1m = (volume_1m_previous_closed / avg_volume_1m_20) if avg_volume_1m_20 > 0 else 0.0
            relative_volume_5m = (volume_5m_sum / avg_volume_5m_20) if avg_volume_5m_20 > 0 else 0.0
            dollar_volume_5m = volume_5m_sum_usd if volume_5m_sum_usd > 0.0 else latest_price * volume_5m_sum

            now_iso = self._now_iso()
            day_ago_iso = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
            snapshots_24h = self.database.market_snapshots_between(symbol=symbol, start_iso=day_ago_iso, end_iso=now_iso)
            if snapshots_24h:
                volume_24h_usd = sum(float(row.get("price", 0.0) or 0.0) * float(row.get("volume", 0.0) or 0.0) for row in snapshots_24h)
            else:
                volume_24h_usd = 0.0

            global_volume_24h_usd = 0.0
            global_volume_status = "N/A"
            global_volume_warning = ""
            if asset_type.lower() == "crypto":
                global_row = self._fetch_global_crypto_market_data(symbol)
                global_volume_24h_usd = float(global_row.get("total_volume", 0.0) or 0.0)
                global_volume_status = str(global_row.get("status", "ERROR") or "ERROR")
                base_symbol = self._crypto_base_symbol(symbol)
                if base_symbol in {"SOL", "BTC", "ETH", "XRP"} and global_volume_24h_usd < 1_000_000.0:
                    global_volume_warning = "Global volume seems incorrect or source is incomplete"

            recent_trade_volume = 0.0
            recent_trade_count = 0
            recent_trade_window_seconds = 0
            recent_trade_error = trade_volume_error
            if trade_volume_status == "OK":
                recent_trade_volume = volume_1m_current
                recent_trade_count = trade_count_1m
                recent_trade_window_seconds = 60

            blocked_reasons: list[str] = []
            volume_validation = self.volume_manager.validate_volume_data(symbol) if asset_type.lower() == "crypto" else {
                "is_valid": True,
                "status": "OK",
                "reason": "",
            }
            if volume_1m_current <= 0 and volume_5m_sum <= 0 and volume_15m_sum <= 0:
                if recent_trade_volume > 0.0 or recent_trade_count > 0:
                    blocked_reasons.append(f"Volumen barras en cero; trades recientes detectados ({recent_trade_count} trades / {recent_trade_window_seconds}s)")
                else:
                    blocked_reasons.append("Volumen real inválido")
            if trade_volume_status != "OK":
                blocked_reasons.append("No se pudo consultar volumen real de trades")
            if not bool(volume_validation.get("is_valid", False)):
                blocked_reasons.append(f"Volumen no válido: {volume_validation.get('reason', 'sin detalle')}")

            min_volume_24h_usd = float(getattr(self.settings, "ai_min_volume_24h_usd", 100000.0) or 100000.0)
            effective_volume_24h_usd = volume_24h_usd
            if asset_type.lower() == "crypto" and global_volume_status == "OK" and not global_volume_warning:
                effective_volume_24h_usd = global_volume_24h_usd
            if effective_volume_24h_usd > 0 and effective_volume_24h_usd < min_volume_24h_usd:
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
                            "volume_1m_usd": volume_1m_current_usd,
                            "volume_5m_usd": volume_5m_sum_usd,
                            "volume_15m_usd": volume_15m_sum_usd,
                            "recent_trade_volume": recent_trade_volume,
                            "recent_trade_count": recent_trade_count,
                            "recent_trade_window_seconds": recent_trade_window_seconds,
                            "recent_trade_error": recent_trade_error,
                            "trade_volume_status": trade_volume_status,
                            "volume_validation": volume_validation,
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
            if not self._is_spread_allowed(asset_type=asset_type, spread=spread, price=price):
                blocked_reasons.append("Spread demasiado alto")

            recent_news = self.database.list_news_events_since(
                since_iso=(datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
                symbol=symbol,
                limit=20,
            )
            news_sentiment_weighted = 0.0
            news_weight_sum = 0.0
            if recent_news:
                top_news_influence = max(float(row.get("influence_score", 0.0) or 0.0) for row in recent_news)
                news_score = max(0.0, min(20.0, top_news_influence * 20.0))
                for row in recent_news:
                    influence = max(float(row.get("influence_score", 0.0) or 0.0), 0.0)
                    sentiment = float(row.get("sentiment_score", 0.0) or 0.0)
                    news_sentiment_weighted += (sentiment * influence)
                    news_weight_sum += influence
            else:
                news_score = 0.0
            news_bias = (news_sentiment_weighted / news_weight_sum) if news_weight_sum > 0 else 0.0

            rsi_now = float(latest.get("rsi", 50.0) or 50.0)
            pct1 = float(latest.get("percent_change_1m", 0.0) or 0.0)
            pct5 = float(latest.get("percent_change_5m", 0.0) or 0.0)
            pct15 = float(latest.get("percent_change_15m", 0.0) or 0.0)
            vwap_now = float(latest.get("vwap", price) or price)
            bullish_technical = (pct1 > 0 and pct5 > 0) or (price >= vwap_now and rsi_now >= 52.0 and pct15 > -0.15)
            bearish_technical = (pct1 < 0 and pct5 < 0) or (price < vwap_now and rsi_now <= 48.0 and pct15 < 0.15)
            bullish_news = news_bias >= 0.10
            bearish_news = news_bias <= -0.10
            long_confirmed = (bullish_technical if futures_require_technical else True) and (bullish_news if futures_require_news else True)
            short_confirmed = (bearish_technical if futures_require_technical else True) and (bearish_news if futures_require_news else True)

            risk_score = max(
                0.0,
                min(
                    15.0,
                    15.0 - abs(float(latest.get("rsi", 50.0) or 50.0) - 50.0) / 3.5,
                ),
            )
            final_score = round(min(100.0, price_action_score + volume_score + liquidity_score + news_score + risk_score), 2)
            target_profit_per_unit = self._target_profit_per_unit(asset_type=asset_type, price=price, account_id=account_id)
            target_take_profit = price + target_profit_per_unit
            recent_high_15m = max((float(row.get("high", price) or price) for row in snapshots[:15]), default=price)
            atr_now = float(latest.get("atr", 0.0) or 0.0)
            reachable_buffer = max(
                atr_now * 1.25,
                price * 0.002,
                spread * 2.0,
                target_profit_per_unit * 0.5,
            )

            model_version = "worker_scanner_v1"
            actual_decision_engine = scanner_engine
            if scanner_engine == "model":
                predictor_features = {
                    "asset_type": asset_type,
                    "price": price,
                    "volume": volume_1m_current,
                    "vwap": float(latest.get("vwap", price) or price),
                    "rsi": float(latest.get("rsi", 50.0) or 50.0),
                    "atr": float(latest.get("atr", 0.0) or 0.0),
                    "spread": spread,
                    "percent_change_1m": float(latest.get("percent_change_1m", 0.0) or 0.0),
                    "percent_change_5m": float(latest.get("percent_change_5m", 0.0) or 0.0),
                    "percent_change_15m": float(latest.get("percent_change_15m", 0.0) or 0.0),
                    "price_above_vwap": 1.0 if price >= float(latest.get("vwap", price) or price) else 0.0,
                    "volume_spike_score": max(relative_volume_1m, relative_volume_5m),
                    "news_sentiment_score": 0.0,
                    "news_importance_score": min(news_score / 20.0, 1.0),
                    "news_risk_score": min(max((100.0 - final_score) / 100.0, 0.0), 1.0),
                    "market_session": "crypto" if asset_type.lower() == "crypto" else "regular",
                    "existing_position": False,
                    "distance_from_average_cost": 0.0,
                    "liquidity_score": (liquidity_score / 15.0) if liquidity_score > 0 else 0.0,
                    "composite_score": final_score,
                }
                try:
                    prediction = self.predictor.predict_signal(predictor_features)
                    action = str(prediction.get("action", "WATCH") or "WATCH")
                    final_score = float(prediction.get("confidence_score", final_score) or final_score)
                    model_version = str(prediction.get("model_version", "general_model_heuristic") or "general_model_heuristic")
                    actual_decision_engine = str(prediction.get("decision_engine", scanner_engine) or scanner_engine).strip().lower()
                except Exception as ex:
                    self._record_error("SignalScannerWorker", "predict_signal", ex)
                    scanner_engine = "heuristic"
                    actual_decision_engine = "heuristic"
                    blocked_reasons.append("Fallback a heuristica por error de modelo")
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
            else:
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

            watch_resolution_note = ""
            if asset_type.lower() == "crypto":
                directional_action = action
                if final_score >= 60.0:
                    if long_confirmed and futures_enable_long and not short_confirmed:
                        directional_action = "BUY_SMALL" if final_score < 75.0 else "BUY"
                    elif short_confirmed and futures_enable_short and not long_confirmed:
                        directional_action = "SELL_SHORT"
                    else:
                        directional_action = "WATCH"
                        if futures_require_technical and not (bullish_technical or bearish_technical):
                            blocked_reasons.append("Sin confirmacion tecnica para long/short")
                        if futures_require_news and not (bullish_news or bearish_news):
                            blocked_reasons.append("Sin confirmacion de noticias para long/short")
                else:
                    directional_action = "AVOID" if final_score < 45.0 else "WATCH"
                action = directional_action

            # Evidence-based WATCH resolution (IA + technical + news + CryptoPanic when available).
            # This avoids arbitrary conversions and allows directional outcome when evidence is strong.
            if action == "WATCH" and asset_type.lower() == "crypto" and bool(getattr(self.settings, "ai_watch_as_buy_small", False)):
                min_exec_confidence = float(getattr(self.settings, "ai_min_execution_confidence", 60.0) or 60.0)
                min_evidence = max(int(getattr(self.settings, "ai_watch_resolution_min_evidence", 2) or 2), 1)
                min_edge = max(int(getattr(self.settings, "ai_watch_resolution_min_edge", 1) or 1), 0)
                min_volume_1m_usd = float(getattr(self.settings, "ai_watch_resolution_min_volume_1m_usd", 1000.0) or 1000.0)
                tie_news_bias = float(getattr(self.settings, "ai_watch_resolution_tie_news_bias", 0.12) or 0.12)
                cryptopanic_rows = [
                    row for row in recent_news
                    if "cryptopanic" in str(row.get("source", "") or "").strip().lower()
                ]
                cp_weight = 0.0
                cp_sent_weighted = 0.0
                for row in cryptopanic_rows:
                    influence = max(float(row.get("influence_score", 0.0) or 0.0), 0.0)
                    sentiment = float(row.get("sentiment_score", 0.0) or 0.0)
                    cp_weight += influence
                    cp_sent_weighted += (sentiment * influence)
                cp_bias = (cp_sent_weighted / cp_weight) if cp_weight > 0.0 else 0.0
                cp_available = bool(cryptopanic_rows)

                long_evidence = 0
                short_evidence = 0
                if bullish_technical:
                    long_evidence += 2
                if bearish_technical:
                    short_evidence += 2
                if pct1 > 0 and pct5 > 0:
                    long_evidence += 1
                if pct1 < 0 and pct5 < 0:
                    short_evidence += 1
                if bullish_news:
                    long_evidence += 1
                if bearish_news:
                    short_evidence += 1
                if news_bias >= 0.10:
                    long_evidence += 1
                if news_bias <= -0.10:
                    short_evidence += 1
                if cp_available and cp_bias >= 0.05:
                    long_evidence += 1
                if cp_available and cp_bias <= -0.05:
                    short_evidence += 1

                volume_ok = (trade_volume_status in {"OK", "FALLBACK_USED"}) and (volume_1m_current_usd >= min_volume_1m_usd)
                score_ok = final_score >= min_exec_confidence

                if score_ok and volume_ok:
                    long_wins = long_evidence >= (short_evidence + min_edge)
                    short_wins = short_evidence >= (long_evidence + min_edge)

                    # Technical/news tie-breaker when evidence count is equal.
                    if (not long_wins and not short_wins) and long_evidence == short_evidence:
                        if news_bias >= tie_news_bias:
                            long_wins = True
                        elif news_bias <= (-1.0 * tie_news_bias):
                            short_wins = True

                    if futures_enable_long and long_evidence >= min_evidence and long_wins:
                        action = "BUY_SMALL"
                        watch_resolution_note = (
                            f"watch_to_buy_small(score={final_score:.2f}; long_ev={long_evidence}; short_ev={short_evidence}; "
                            f"cp_available={int(cp_available)}; cp_bias={cp_bias:.3f}; min_ev={min_evidence}; min_edge={min_edge})"
                        )
                    elif futures_enable_short and short_evidence >= min_evidence and short_wins:
                        action = "SELL_SHORT"
                        watch_resolution_note = (
                            f"watch_to_sell_short(score={final_score:.2f}; long_ev={long_evidence}; short_ev={short_evidence}; "
                            f"cp_available={int(cp_available)}; cp_bias={cp_bias:.3f}; min_ev={min_evidence}; min_edge={min_edge})"
                        )

            metrics = self.calculate_average_cost(symbol=symbol, account_id=account_id)
            average_cost = float(metrics.get("average_cost", 0.0) or 0.0)
            quantity_owned = float(metrics.get("quantity_owned", 0.0) or 0.0)
            min_sell_price = average_cost + float(self.settings.ai_fees_buffer) + float(self.settings.ai_slippage_buffer) + float(self.settings.ai_minimum_profit)
            reason = (
                f"price_action={price_action_score:.2f}; volume={volume_score:.2f}; liquidity={liquidity_score:.2f}; "
                f"news={news_score:.2f}; risk={risk_score:.2f}; final={final_score:.2f}; "
                f"news_bias={news_bias:.4f}; bullish_technical={int(bullish_technical)}; bearish_technical={int(bearish_technical)}; "
                f"bullish_news={int(bullish_news)}; bearish_news={int(bearish_news)}; "
                f"decision_engine={actual_decision_engine}; requested_engine={scanner_engine}; "
                f"volume_1m_current={volume_1m_current:.2f}; volume_1m_previous_closed={volume_1m_previous_closed:.2f}; "
                f"volume_5m_sum={volume_5m_sum:.2f}; volume_15m_sum={volume_15m_sum:.2f}; "
                f"volume_1m_usd={volume_1m_current_usd:.2f}; volume_5m_usd={volume_5m_sum_usd:.2f}; volume_15m_usd={volume_15m_sum_usd:.2f}; "
                f"avg_volume_1m_20={avg_volume_1m_20:.2f}; avg_volume_5m_20={avg_volume_5m_20:.2f}; "
                f"relative_volume_1m={relative_volume_1m:.2f}; relative_volume_5m={relative_volume_5m:.2f}; "
                f"dollar_volume_5m={dollar_volume_5m:.2f}; volume_24h_usd={volume_24h_usd:.2f}"
            )
            if asset_type.lower() == "crypto":
                reason += f"; global_volume_24h_usd={global_volume_24h_usd:.2f}; global_volume_status={global_volume_status}"
                if global_volume_warning:
                    reason += f"; global_volume_warning={global_volume_warning}"
            if watch_resolution_note:
                reason += f"; {watch_resolution_note}"
            if blocked_reasons:
                reason += " | blocked_reason: " + " | ".join(dict.fromkeys(blocked_reasons))
            if quantity_owned > 0 and average_cost > 0 and price < average_cost:
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
            elif quantity_owned > 0 and average_cost > 0 and price >= min_sell_price:
                action = "SELL_ALLOWED"

            features = {
                "account_name": account_name,
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
                "news_bias": news_bias,
                "bullish_technical": 1 if bullish_technical else 0,
                "bearish_technical": 1 if bearish_technical else 0,
                "bullish_news": 1 if bullish_news else 0,
                "bearish_news": 1 if bearish_news else 0,
                "risk_score": risk_score,
                "composite_score": final_score,
                "average_cost": average_cost,
                "volume_1m_current": volume_1m_current,
                "volume_1m_previous_closed": volume_1m_previous_closed,
                "volume_5m_sum": volume_5m_sum,
                "volume_15m_sum": volume_15m_sum,
                "volume_1m_usd": volume_1m_current_usd,
                "volume_5m_usd": volume_5m_sum_usd,
                "volume_15m_usd": volume_15m_sum_usd,
                "avg_volume_1m_20": avg_volume_1m_20,
                "avg_volume_5m_20": avg_volume_5m_20,
                "relative_volume_1m": relative_volume_1m,
                "relative_volume_5m": relative_volume_5m,
                "dollar_volume_5m": dollar_volume_5m,
                "volume_24h_usd": volume_24h_usd,
                "global_volume_24h_usd": global_volume_24h_usd,
                "global_volume_status": global_volume_status,
                "global_volume_warning": global_volume_warning,
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
            new_signal_id = self.database.insert_signal(
                {
                    "timestamp": self._now_iso(),
                    "symbol": symbol,
                    "asset_type": asset_type,
                    "signal_type": action,
                    "confidence_score": final_score,
                    "model_version": model_version,
                    "reason": reason,
                    "entry_price": price,
                    "suggested_limit_price": price,
                    "invalidation_price": max(price - float(latest.get("atr", 0.0) or 0.0), 0.0),
                    "take_profit_price": max(price + target_profit_per_unit, min_sell_price if min_sell_price > 0 else price + target_profit_per_unit),
                    "risk_level": self._risk_label(100.0 - final_score),
                    "features_json": features,
                    "openai_analysis_json": openai_analysis,
                }
            )

            # Auto-execute: if BUY/BUY_SMALL place the order automatically
            if action in {"BUY", "BUY_SMALL"} and new_signal_id > 0:
                try:
                    buy_result = self.place_limit_buy(
                        signal_id=new_signal_id,
                        account_name=account_name,
                        manual_approved=True,
                        initiated_by="bot_auto",
                    )
                    buy_status = str(buy_result.get("status", "unknown") or "unknown")
                    self.database.insert_decision_log(
                        {
                            "timestamp": self._now_iso(),
                            "symbol": symbol,
                            "decision": "AUTO_BUY_SUBMITTED",
                            "reason": f"status={buy_status}; signal_id={new_signal_id}",
                            "blocked_reason": "" if buy_status == "submitted" else buy_status,
                            "raw_context_json": {
                                "account_name": account_name,
                                "asset_type": asset_type,
                                "signal_id": new_signal_id,
                                "result": buy_result,
                            },
                        }
                    )
                    self.logger.info(
                        "Auto-ejecucion IA | %s | %s | score=%.2f | result=%s",
                        symbol, action, final_score, buy_status,
                    )
                except Exception as ex:
                    err_text = str(ex or "unknown_error")
                    self.database.insert_decision_log(
                        {
                            "timestamp": self._now_iso(),
                            "symbol": symbol,
                            "decision": "AUTO_BUY_BLOCKED",
                            "reason": f"signal_id={new_signal_id}",
                            "blocked_reason": err_text,
                            "raw_context_json": {
                                "account_name": account_name,
                                "asset_type": asset_type,
                                "signal_id": new_signal_id,
                            },
                        }
                    )
                    self.logger.info("Auto-ejecucion bloqueada para %s: %s", symbol, ex)

            if action == "SELL_SHORT" and new_signal_id > 0:
                try:
                    short_result = self.place_limit_short(
                        signal_id=new_signal_id,
                        account_name=account_name,
                        manual_approved=True,
                        initiated_by="bot_auto",
                    )
                    short_status = str(short_result.get("status", "unknown") or "unknown")
                    self.database.insert_decision_log(
                        {
                            "timestamp": self._now_iso(),
                            "symbol": symbol,
                            "decision": "AUTO_SHORT_SUBMITTED",
                            "reason": f"status={short_status}; signal_id={new_signal_id}",
                            "blocked_reason": "" if short_status == "submitted" else short_status,
                            "raw_context_json": {
                                "account_name": account_name,
                                "asset_type": asset_type,
                                "signal_id": new_signal_id,
                                "result": short_result,
                            },
                        }
                    )
                    self.logger.info(
                        "Auto-ejecucion IA SHORT | %s | score=%.2f | result=%s",
                        symbol,
                        final_score,
                        short_status,
                    )
                except Exception as ex:
                    err_text = str(ex or "unknown_error")
                    self.database.insert_decision_log(
                        {
                            "timestamp": self._now_iso(),
                            "symbol": symbol,
                            "decision": "AUTO_SHORT_BLOCKED",
                            "reason": f"signal_id={new_signal_id}",
                            "blocked_reason": err_text,
                            "raw_context_json": {
                                "account_name": account_name,
                                "asset_type": asset_type,
                                "signal_id": new_signal_id,
                            },
                        }
                    )
                    self.logger.info("Auto-short bloqueado para %s: %s", symbol, ex)

            self.database.insert_decision_log(
                {
                    "timestamp": self._now_iso(),
                    "symbol": symbol,
                    "decision": action,
                    "reason": reason,
                    "blocked_reason": " | ".join(dict.fromkeys(blocked_reasons)),
                    "raw_context_json": {
                        "final_score": final_score,
                        "account_name": account_name,
                        "asset_type": asset_type,
                    },
                }
            )

    def _labeler_cycle(self) -> None:
        now = datetime.now(timezone.utc)
        since = (now - timedelta(hours=2)).isoformat()
        signals = self.database.list_signals_since(since_iso=since)
        windows = [5, 15, 30, 60]
        profit_threshold_pct = float(getattr(self.settings, "ai_outcome_win_profit_pct", 0.25) or 0.25)
        loss_threshold_pct = abs(float(getattr(self.settings, "ai_outcome_loss_drawdown_pct", 0.25) or 0.25))
        for signal in signals:
            signal_id = int(signal.get("id", 0) or 0)
            if signal_id <= 0:
                continue
            symbol = str(signal.get("symbol", "")).upper()
            asset_type = str(signal.get("asset_type", ""))
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
            updates: dict[str, Any] = {
                "asset_type": asset_type,
                "entry_price": entry_price,
                "timestamp_signal": signal_time.isoformat(),
            }
            take_profit_price = float(signal.get("take_profit_price", 0.0) or 0.0)
            target_profit_pct = ((take_profit_price - entry_price) / entry_price) * 100.0 if take_profit_price > 0 else 0.0
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
                price_after = float(rows[-1].get("close", entry_price) or entry_price)
                max_profit = max(profits)
                max_drawdown = min(drawdowns)

                is_win = (max_profit >= target_profit_pct and target_profit_pct > 0.0) or (max_profit >= profit_threshold_pct)
                is_loss = max_drawdown <= (-loss_threshold_pct)
                if is_win:
                    result = "win"
                elif is_loss:
                    result = "loss"
                else:
                    result = "neutral"
                updates[f"price_after_{window}m"] = round(price_after, 6)
                updates[f"max_profit_{window}m"] = round(max_profit, 6)
                updates[f"max_drawdown_{window}m"] = round(max_drawdown, 6)
                updates[result_key] = result

            merged = dict(outcome)
            merged.update(updates)
            final_label = str(merged.get("result_15m") or merged.get("result_30m") or "").strip().lower()
            if final_label in {"win", "loss", "neutral"}:
                updates["final_label"] = final_label
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

    def _stream_subscription_symbols(self, account_name: str) -> dict[str, list[str]]:
        symbols = self._symbols_for_collection(account_name)
        focus = self._focus_for_account(account_name)
        focus_stocks = set(focus.get("stocks_symbols", set()))
        focus_cryptos = set(focus.get("cryptos_symbols", set()))
        stock_symbols: list[str] = []
        crypto_symbols: list[str] = []
        seen: set[str] = set()
        for item in symbols:
            symbol = str(item.get("symbol", "")).upper().strip()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            asset_type = str(item.get("asset_type", "stock")).lower().strip()
            if asset_type == "crypto" or "/" in symbol or symbol.endswith("USD"):
                crypto_symbols.append(symbol)
            else:
                stock_symbols.append(symbol)

        default_symbol = str(self.settings.default_symbol).upper().strip()
        default_is_crypto = bool("/" in default_symbol or default_symbol.endswith("USD"))
        allow_default = True
        if default_is_crypto and focus_cryptos:
            allow_default = any(self._symbol_key(default_symbol) == self._symbol_key(candidate) for candidate in focus_cryptos)
        if not default_is_crypto and focus_stocks:
            allow_default = default_symbol in focus_stocks
        if default_symbol and default_symbol not in seen and allow_default:
            if "/" in default_symbol or default_symbol.endswith("USD"):
                crypto_symbols.append(default_symbol)
            else:
                stock_symbols.append(default_symbol)

        return {"stocks": stock_symbols, "crypto": crypto_symbols}

    def _symbols_for_collection(self, account_name: str) -> list[dict[str, str]]:
        symbols: dict[str, dict[str, str]] = {}
        is_binance_provider = str(getattr(self.broker, "provider", "") or "").strip().lower() == "binance"
        focus = self._focus_for_account(account_name)
        focus_stocks_only = bool(focus.get("stocks_only", False))
        focus_cryptos_only = bool(focus.get("cryptos_only", False))
        focus_stocks = {str(token).upper().strip() for token in set(focus.get("stocks_symbols", set())) if str(token).strip()}
        focus_cryptos = {
            self._normalize_crypto_symbol_input(str(token))
            for token in set(focus.get("cryptos_symbols", set()))
            if str(token).strip()
        }
        focus_cryptos = {token for token in focus_cryptos if token}

        for asset in self.database.list_watchlist_assets(active_only=True):
            symbol = str(asset.get("symbol", "")).upper().strip()
            if not symbol:
                continue
            asset_type = str(asset.get("asset_type", "stock") or "stock").lower().strip()
            if is_binance_provider and asset_type != "crypto":
                continue
            symbols[self._symbol_key(symbol)] = {
                "symbol": symbol,
                "asset_type": asset_type,
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
        if is_binance_provider and not ("/" in default_symbol or default_symbol.endswith("USD") or default_symbol.endswith("USDT") or default_symbol.endswith("USDC")):
            default_symbol = "SOL/USD"
        if default_symbol:
            symbols.setdefault(
                self._symbol_key(default_symbol),
                {
                    "symbol": default_symbol,
                    "asset_type": "crypto" if "/" in default_symbol or default_symbol.endswith("USD") else "stock",
                },
            )

        # If user defined a focus list, force evaluation to those symbols for that asset type.
        # This keeps collector/scanner aligned with "Enfoque de evaluacion IA".
        if focus_stocks:
            focus_stock_keys = {self._symbol_key(item) for item in focus_stocks}
            symbols = {
                key: value
                for key, value in symbols.items()
                if not (str(value.get("asset_type", "")).lower().strip() == "stock")
                or key in focus_stock_keys
            }

        if focus_cryptos:
            focus_crypto_keys = {self._symbol_key(item) for item in focus_cryptos}
            symbols = {
                key: value
                for key, value in symbols.items()
                if not (str(value.get("asset_type", "")).lower().strip() == "crypto")
                or key in focus_crypto_keys
            }
            for symbol in sorted(focus_cryptos):
                symbols.setdefault(
                    self._symbol_key(symbol),
                    {
                        "symbol": symbol,
                        "asset_type": "crypto",
                    },
                )

        if focus_stocks_only:
            symbols = {
                key: value
                for key, value in symbols.items()
                if str(value.get("asset_type", "")).lower().strip() == "stock"
            }
        if focus_cryptos_only:
            symbols = {
                key: value
                for key, value in symbols.items()
                if str(value.get("asset_type", "")).lower().strip() == "crypto"
            }

        return list(symbols.values())

    def _handle_stream_market_event(self, message_type: str, payload: dict[str, Any]) -> None:
        try:
            self.volume_manager.handle_websocket_event(message_type, payload)
        except Exception as ex:
            self.logger.warning("Volume manager WS update failed: %s", ex)

        symbol = str(payload.get("S", "")).upper().strip()
        if not symbol:
            return

        asset_type = "crypto" if "/" in symbol or symbol.endswith("USD") else "stock"
        price = float(payload.get("p", payload.get("c", 0.0)) or 0.0)
        if message_type == "q":
            bid = float(payload.get("bp", 0.0) or 0.0)
            ask = float(payload.get("ap", 0.0) or 0.0)
            midpoint = (bid + ask) / 2.0 if bid > 0.0 and ask > 0.0 else price
            quote_payload = {
                "bid": bid,
                "ask": ask,
                "bid_size": payload.get("bs", 0),
                "ask_size": payload.get("as", 0),
                "timestamp": payload.get("t"),
                "spread": max(ask - bid, 0.0),
                "spread_pct": ((max(ask - bid, 0.0) / midpoint) * 100.0) if midpoint > 0 else 0.0,
            }
            self.market_data.runtime_state.set_quote(self.market_data.account_name, symbol, quote_payload)
            if midpoint > 0.0:
                self.market_data.update_latest_price(symbol, midpoint)
            return

        if message_type == "t" and price > 0.0:
            self.market_data.update_latest_price(symbol, price)
            return

        if message_type not in {"b", "u", "d"}:
            return

        bar = {
            "open": float(payload.get("o", price) or price),
            "high": float(payload.get("h", price) or price),
            "low": float(payload.get("l", price) or price),
            "close": float(payload.get("c", price) or price),
            "volume": float(payload.get("v", 0.0) or 0.0),
            "timestamp": payload.get("t"),
        }
        history = self._stream_bar_history_by_symbol.setdefault(self._symbol_key(symbol), deque(maxlen=120))
        history.append(bar)
        history_list = list(history)
        close_price = float(bar["close"])
        self.market_data.update_latest_price(symbol, close_price)
        quote = self.market_data.runtime_state.get_quote(self.market_data.account_name, symbol, ttl_seconds=30.0) or {}
        snapshot = {
            "timestamp": self._now_iso(),
            "symbol": symbol,
            "asset_type": asset_type,
            "price": close_price,
            "open": float(bar["open"]),
            "high": float(bar["high"]),
            "low": float(bar["low"]),
            "close": close_price,
            "volume": float(bar["volume"]),
            "vwap": float(self.market_data.calculate_vwap(history_list)) if history_list else close_price,
            "rsi": self._calc_rsi(history_list),
            "atr": self._calc_atr(history_list),
            "spread": float(quote.get("spread", 0.0) or 0.0),
            "percent_change_1m": self._pct_change(history_list, 1),
            "percent_change_5m": self._pct_change(history_list, 5),
            "percent_change_15m": self._pct_change(history_list, 15),
            "source": "stream",
        }
        self.database.insert_market_snapshot(snapshot)
        self._last_collected_at_by_symbol[self._symbol_key(symbol)] = time.monotonic()

    def _handle_stream_news_event(self, _: str, payload: dict[str, Any]) -> None:
        headline = str(payload.get("headline", "") or payload.get("summary", "") or "").strip()
        if not headline:
            return

        source = str(payload.get("source", "news") or "news").strip()
        url = str(payload.get("url", "") or "").strip()
        author = str(payload.get("author", "") or "").strip()
        symbols = [str(symbol or "").upper().strip() for symbol in payload.get("symbols", []) if str(symbol or "").strip()]
        if not symbols:
            symbols = [str(self.settings.default_symbol).upper().strip()]

        now_iso = self._now_iso()
        for symbol in symbols:
            if not symbol:
                continue
            signature = self._news_signature(symbol=symbol, source=source, title_or_text=headline, url=url)
            if self._is_duplicate_news_signature(signature):
                continue

            quick = self._quick_news_assessment(headline)
            strong_event = quick["importance_score"] >= float(getattr(self.settings, "ai_openai_news_trigger_importance", 65.0) or 65.0) or quick["risk_score"] >= 70
            analysis: dict[str, Any] = {
                "symbol": symbol,
                "asset_type": "crypto" if "/" in symbol or symbol.endswith("USD") else "stock",
                "sentiment": quick["sentiment"],
                "event_type": quick["event_type"],
                "importance_score": quick["importance_score"],
                "risk_score": quick["risk_score"],
                "summary": headline[:180],
                "possible_market_impact": "Potential impact detected" if strong_event else "Low impact",
                "action_bias": "neutral",
            }

            if strong_event and bool(self.settings.openai_api_key):
                self._openai_calls_today += 1
                self._last_openai_call_at = now_iso
                analysis = self.openai_analyzer.analyze_text(
                    symbol=symbol,
                    asset_type=str(analysis["asset_type"]),
                    text=headline,
                    source=source,
                    author=author,
                    context={"account_name": self._active_account_for_workers, "origin": "news_stream"},
                )

            self.database.insert_news_event(
                {
                    "timestamp": now_iso,
                    "symbol": symbol,
                    "asset_type": str(analysis.get("asset_type", "stock")),
                    "source": source,
                    "title_or_text": headline,
                    "url": url,
                    "author": author,
                    "influence_score": float(analysis.get("importance_score", 0.0) or 0.0) / 100.0,
                    "sentiment_score": self._sentiment_to_float(str(analysis.get("sentiment", "neutral"))),
                    "ai_summary": str(analysis.get("summary", "")),
                    "ai_classification": str(analysis.get("event_type", "other")),
                    "raw_payload": analysis,
                }
            )
            self._mark_news_signature_seen(signature)

            if strong_event:
                self.database.insert_decision_log(
                    {
                        "timestamp": now_iso,
                        "symbol": symbol,
                        "decision": "NEWS",
                        "reason": f"source={source}; importance={analysis.get('importance_score', 0)}; risk={analysis.get('risk_score', 0)}",
                        "blocked_reason": "",
                        "raw_context_json": {"title": headline, "source": source, "url": url},
                    }
                )

    def _handle_stream_trade_update(self, _: str, payload: dict[str, Any]) -> None:
        event = str(payload.get("event", "")).lower().strip()
        order = payload.get("order", {})
        if not isinstance(order, dict):
            return

        account_name = self._active_account_for_workers
        if not account_name:
            return

        account = self.refresh_account_context(account_name)
        account_id = int(account["id"])
        symbol = str(order.get("symbol", "") or "").upper().strip()
        if not symbol:
            return

        order_payload = {
            "timestamp": str(payload.get("timestamp", order.get("updated_at", self._now_iso()))),
            "account_id": account_id,
            "symbol": symbol,
            "asset_type": "crypto" if "/" in symbol or symbol.endswith("USD") else "stock",
            "side": str(order.get("side", "") or "unknown").lower().strip(),
            "order_type": str(order.get("order_type", "") or "market").lower().strip(),
            "qty": float(order.get("qty", order.get("filled_qty", 0.0)) or 0.0),
            "limit_price": float(order.get("limit_price", 0.0) or 0.0),
            "filled_price": float(order.get("filled_avg_price", payload.get("price", 0.0)) or payload.get("price", 0.0) or 0.0),
            "fees": 0.0,
            "status": str(order.get("status", event or "unknown") or event or "unknown"),
            "initiated_by": "trade_updates",
            "broker_order_id": str(order.get("id", "") or ""),
            "signal_id": None,
            "created_at": str(order.get("created_at", payload.get("timestamp", self._now_iso()))),
        }
        if not order_payload["broker_order_id"]:
            return

        self.database.upsert_trade_order(order_payload)
        self.broker.runtime_state.invalidate_orders(self.broker.account_name)
        self.broker.runtime_state.invalidate_positions(self.broker.account_name)

        if event in {"fill", "partial_fill", "canceled", "expired", "rejected", "replaced", "done_for_day", "pending_cancel", "order_cancel_rejected", "order_replace_rejected"}:
            try:
                self.position_manager.synchronize_open_positions()
            except Exception as ex:
                self._last_api_error = str(ex)

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
        def _merge_events(*groups: list[dict[str, str]]) -> list[dict[str, str]]:
            merged: list[dict[str, str]] = []
            seen: set[str] = set()
            for events in groups:
                for row in events:
                    source = str(row.get("source", "") or "").strip().lower()
                    title = str(row.get("title_or_text", "") or "").strip().lower()
                    url = str(row.get("url", "") or "").strip().lower()
                    key = f"{source}|{title}|{url}"
                    if not title or key in seen:
                        continue
                    seen.add(key)
                    merged.append(row)
            return merged[:10]

        if asset_type == "crypto":
            events = self._fetch_cryptopanic_events(symbol)
            if events:
                return events

        stock_news = self._fetch_alpaca_news_events(symbol)
        yahoo_news = self._fetch_yahoo_finance_events(symbol)
        return _merge_events(stock_news, yahoo_news)

    def _fetch_alpaca_news_events(self, symbol: str) -> list[dict[str, str]]:
        if str(getattr(self.broker, "provider", "") or "").strip().lower() == "binance":
            return []
        key = self._symbol_key(symbol)
        cache_key = f"alpaca_news:{key}"
        cooldown_key = f"alpaca_news:{key}"
        now_monotonic = time.monotonic()

        cached = self._news_fetch_cache_by_symbol.get(cache_key)
        if cached is not None:
            cached_at, cached_events = cached
            if (now_monotonic - cached_at) <= 900.0:
                return cached_events

        cooldown_until = self._news_fetch_cooldown_until_by_symbol.get(cooldown_key, 0.0)
        if now_monotonic < cooldown_until:
            return cached[1] if cached is not None else []

        api_key = str(getattr(self.settings, "alpaca_api_key", "") or "").strip()
        api_secret = str(getattr(self.settings, "alpaca_api_secret", "") or "").strip()
        if not api_key or not api_secret:
            return []

        try:
            response = requests.get(
                "https://data.alpaca.markets/v1beta1/news",
                params={"symbols": symbol.upper().replace(" ", ""), "limit": "8"},
                timeout=int(getattr(self.settings, "http_timeout_alpaca_seconds", 10) or 10),
                headers={
                    "APCA-API-KEY-ID": api_key,
                    "APCA-API-SECRET-KEY": api_secret,
                    "Accept": "application/json",
                },
            )
            if response.status_code == 429:
                self._news_fetch_cooldown_until_by_symbol[cooldown_key] = now_monotonic + 900.0
                self.logger.warning("Alpaca News rate limited for %s. Cooling down for 15m.", symbol)
                return cached[1] if cached is not None else []

            response.raise_for_status()
            self._api_calls_today += 1
            payload = response.json()
            self._mark_api_recovered()

            result: list[dict[str, str]] = []
            for item in payload.get("news", [])[:8]:
                title = str(item.get("headline", "") or item.get("summary", "") or "").strip()
                if not title:
                    continue
                source = item.get("source") or {}
                author = str(item.get("author", "") or source.get("name", "") or source.get("domain", "") or "").strip()
                url = str(item.get("url", "") or "").strip()
                result.append(
                    {
                        "source": "alpaca_news",
                        "title_or_text": title,
                        "url": url,
                        "author": author,
                    }
                )

            self._news_fetch_cache_by_symbol[cache_key] = (now_monotonic, result)
            return result
        except Exception as ex:
            if self._is_rate_limit_error(ex):
                self._news_fetch_cooldown_until_by_symbol[cooldown_key] = now_monotonic + 900.0
                self.logger.warning("Alpaca News rate limited for %s. Using cached news if available.", symbol)
                return cached[1] if cached is not None else []
            self._last_api_error = str(ex)
            return []

    def _fetch_cryptopanic_events(self, symbol: str) -> list[dict[str, str]]:
        token = str(getattr(self.settings, "cryptopanic_api_key", "") or "").strip()
        if not token:
            return []
        query_currency = symbol.replace("/", "").replace("USD", "").upper().strip()
        if not query_currency:
            return []

        quota = self.get_cryptopanic_quota_status()
        if not bool(quota.get("allowed_today", False)):
            return []

        key = f"cryptopanic:{self._symbol_key(symbol)}"
        now_monotonic = time.monotonic()
        cached = self._news_fetch_cache_by_symbol.get(key)
        cache_seconds = max(int(getattr(self.settings, "cryptopanic_cache_seconds", 1800) or 1800), 60)
        if cached is not None:
            cached_at, cached_events = cached
            if (now_monotonic - cached_at) <= float(cache_seconds):
                return cached_events

        today_used = int(quota.get("today_used", 0) or 0)
        today_budget = int(quota.get("today_budget", 0) or 0)
        if today_used >= today_budget:
            return []

        try:
            response = requests.get(
                "https://cryptopanic.com/api/growth_weekly/v2/posts/",
                params={
                    "auth_token": token,
                    "currencies": query_currency,
                    "public": "true",
                    "kind": "news",
                    "filter": "hot",
                    "regions": "en",
                },
                timeout=int(getattr(self.settings, "http_timeout_cryptopanic_seconds", 10) or 10),
            )
            response.raise_for_status()
            self._api_calls_today += 1
            payload = response.json()
            self._record_cryptopanic_request()
            self._mark_api_recovered()
            result: list[dict[str, str]] = []
            for row in payload.get("results", [])[:4]:
                source = row.get("source") or {}
                title = str(row.get("title", "") or "").strip()
                url = str(row.get("original_url", "") or row.get("url", "") or "").strip()
                author = str(source.get("domain", "") or source.get("title", "") or row.get("author", "") or "").strip()
                if not title:
                    continue
                result.append(
                    {
                        "source": "cryptopanic",
                        "title_or_text": title,
                        "url": url,
                        "author": author,
                    }
                )
            self._news_fetch_cache_by_symbol[key] = (now_monotonic, result)
            return result
        except Exception as ex:
            if not self._is_rate_limit_error(ex):
                self._last_api_error = str(ex)
            return []

    def update_cryptopanic_quota_settings(self, monthly_limit: int, used_baseline: int, request_days: str) -> None:
        self._cryptopanic_monthly_limit = max(int(monthly_limit or 1), 1)
        self._cryptopanic_used_baseline = max(int(used_baseline or 0), 0)
        self._cryptopanic_request_weekdays = self._parse_cryptopanic_request_days(request_days)

    def reset_cryptopanic_month_usage(self, used_baseline: int = 0) -> dict[str, Any]:
        now_utc = datetime.now(timezone.utc)
        month_key = now_utc.strftime("%Y-%m")
        baseline = max(int(used_baseline or 0), 0)
        with self._cryptopanic_usage_lock:
            self._cryptopanic_used_baseline = baseline
            months = self._cryptopanic_usage_data.setdefault("months", {})
            months[month_key] = {"count": 0, "days": {}}
            self._save_cryptopanic_usage()
        return self.get_cryptopanic_quota_status()

    def get_cryptopanic_quota_status(self) -> dict[str, Any]:
        now_utc = datetime.now(timezone.utc)
        month_key = now_utc.strftime("%Y-%m")
        tracked_used = self._cryptopanic_tracked_used_month(month_key)
        used_total = min(self._cryptopanic_monthly_limit, self._cryptopanic_used_baseline + tracked_used)
        remaining_total = max(self._cryptopanic_monthly_limit - used_total, 0)

        active_days_remaining = self._cryptopanic_active_days_remaining(now_utc)
        is_today_active = now_utc.weekday() in self._cryptopanic_request_weekdays
        today_budget = self._cryptopanic_day_budget(now_utc)
        today_used = self._cryptopanic_tracked_used_day(month_key, now_utc.strftime("%Y-%m-%d"))

        return {
            "month_key": month_key,
            "monthly_limit": self._cryptopanic_monthly_limit,
            "used_total": used_total,
            "remaining_total": remaining_total,
            "used_baseline": self._cryptopanic_used_baseline,
            "used_tracked": tracked_used,
            "request_days": self._cryptopanic_request_days_text(),
            "active_days_remaining": active_days_remaining,
            "today_active": is_today_active,
            "today_budget": today_budget,
            "today_used": today_used,
            "allowed_today": bool(is_today_active and remaining_total > 0 and today_budget > today_used),
        }

    def _parse_cryptopanic_request_days(self, request_days: str) -> set[int]:
        aliases = {
            "mon": 0,
            "monday": 0,
            "lun": 0,
            "tue": 1,
            "tuesday": 1,
            "mar": 1,
            "wed": 2,
            "wednesday": 2,
            "mie": 2,
            "mié": 2,
            "thu": 3,
            "thursday": 3,
            "jue": 3,
            "fri": 4,
            "friday": 4,
            "vie": 4,
            "sat": 5,
            "saturday": 5,
            "sab": 5,
            "sáb": 5,
            "sun": 6,
            "sunday": 6,
            "dom": 6,
        }
        result: set[int] = set()
        for raw in str(request_days or "").replace(";", ",").split(","):
            token = raw.strip().lower()
            if not token:
                continue
            if token.isdigit():
                idx = int(token)
                if 0 <= idx <= 6:
                    result.add(idx)
                continue
            if token in aliases:
                result.add(aliases[token])
        if not result:
            return {0, 1, 2, 3, 4}
        return result

    def _cryptopanic_request_days_text(self) -> str:
        names = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
        return ",".join(names[idx] for idx in sorted(self._cryptopanic_request_weekdays))

    def _load_cryptopanic_usage(self) -> dict[str, Any]:
        if not self._cryptopanic_usage_path.exists():
            return {"months": {}}
        try:
            payload = json.loads(self._cryptopanic_usage_path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                return {"months": {}}
            if "months" not in payload or not isinstance(payload.get("months"), dict):
                payload["months"] = {}
            return payload
        except Exception:
            return {"months": {}}

    def _save_cryptopanic_usage(self) -> None:
        try:
            self._cryptopanic_usage_path.parent.mkdir(parents=True, exist_ok=True)
            self._cryptopanic_usage_path.write_text(
                json.dumps(self._cryptopanic_usage_data, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
        except Exception as ex:
            self.logger.warning("No se pudo guardar uso de CryptoPanic: %s", ex)

    def _record_cryptopanic_request(self) -> None:
        now_utc = datetime.now(timezone.utc)
        month_key = now_utc.strftime("%Y-%m")
        day_key = now_utc.strftime("%Y-%m-%d")
        with self._cryptopanic_usage_lock:
            months = self._cryptopanic_usage_data.setdefault("months", {})
            month_data = months.setdefault(month_key, {"count": 0, "days": {}})
            month_data["count"] = int(month_data.get("count", 0) or 0) + 1
            day_map = month_data.setdefault("days", {})
            day_map[day_key] = int(day_map.get(day_key, 0) or 0) + 1
            self._save_cryptopanic_usage()

    def _cryptopanic_tracked_used_month(self, month_key: str) -> int:
        with self._cryptopanic_usage_lock:
            months = self._cryptopanic_usage_data.get("months", {})
            month_data = months.get(month_key, {})
            return int(month_data.get("count", 0) or 0)

    def _cryptopanic_tracked_used_day(self, month_key: str, day_key: str) -> int:
        with self._cryptopanic_usage_lock:
            months = self._cryptopanic_usage_data.get("months", {})
            month_data = months.get(month_key, {})
            day_map = month_data.get("days", {}) if isinstance(month_data.get("days", {}), dict) else {}
            return int(day_map.get(day_key, 0) or 0)

    def _cryptopanic_active_days_remaining(self, now_utc: datetime) -> int:
        year = now_utc.year
        month = now_utc.month
        _, days_in_month = calendar.monthrange(year, month)
        count = 0
        for day in range(now_utc.day, days_in_month + 1):
            dt = datetime(year, month, day, tzinfo=timezone.utc)
            if dt.weekday() in self._cryptopanic_request_weekdays:
                count += 1
        return count

    def _cryptopanic_day_budget(self, now_utc: datetime) -> int:
        if now_utc.weekday() not in self._cryptopanic_request_weekdays:
            return 0

        month_key = now_utc.strftime("%Y-%m")
        tracked_used = self._cryptopanic_tracked_used_month(month_key)
        used_total = min(self._cryptopanic_monthly_limit, self._cryptopanic_used_baseline + tracked_used)
        remaining_total = max(self._cryptopanic_monthly_limit - used_total, 0)
        if remaining_total <= 0:
            return 0

        year = now_utc.year
        month = now_utc.month
        _, days_in_month = calendar.monthrange(year, month)
        active_dates = [
            datetime(year, month, day, tzinfo=timezone.utc)
            for day in range(now_utc.day, days_in_month + 1)
            if datetime(year, month, day, tzinfo=timezone.utc).weekday() in self._cryptopanic_request_weekdays
        ]
        if not active_dates:
            return 0

        total_active = len(active_dates)
        idx = next((i for i, dt in enumerate(active_dates) if dt.date() == now_utc.date()), 0)
        prev_target = math.floor((remaining_total * idx) / total_active)
        current_target = math.floor((remaining_total * (idx + 1)) / total_active)
        daily_budget = current_target - prev_target
        return max(int(daily_budget), 0)

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
                timeout=int(getattr(self.settings, "http_timeout_news_seconds", 10) or 10),
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
        if float(funds.get("available_capital", 0.0) or 0.0) <= 0:
            return "No hay fondos asignados"
        if float(funds.get("max_position_size", 0.0) or 0.0) <= 0:
            return "Max position size invalido"
        price = float(features.get("price", 0.0) or 0.0)
        spread = float(features.get("spread", 0.0) or 0.0)
        asset_type = "crypto" if "/" in symbol or symbol.endswith("USD") else "stock"
        if not self._is_spread_allowed(asset_type=asset_type, spread=spread, price=price):
            return "Spread demasiado alto"
        if asset_type != "crypto":
            if float(features.get("volume", 0.0) or 0.0) < float(self.settings.ai_min_volume_required):
                return "Volumen inválido"
        dashboard = self.database.dashboard_summary(account_id)
        account_daily_pnl = float(dashboard.get("daily_pnl", 0.0) or 0.0)
        if not self.risk_manager.can_trade(current_daily_pnl=account_daily_pnl):
            return "Se excede max_daily_loss"
        if not bool(runtime.get("paper_trading", 1)) and not bool(runtime.get("live_trading_enabled", 0)):
            return "Live trading esta apagado"
        supported = self._is_broker_supported(symbol=symbol, asset_type=asset_type)
        if not supported:
            return "Activo no soportado en Alpaca"
        return ""

    def _is_spread_allowed(self, *, asset_type: str, spread: float, price: float) -> bool:
        max_allowed = float(getattr(self, "_ai_max_spread_allowed", getattr(self.settings, "ai_max_spread_allowed", 0.05)) or 0.05)
        spread_value = float(spread or 0.0)
        if str(asset_type or "").lower() == "crypto":
            spread_value = ((spread_value / max(price, 1e-8)) * 100.0) if price > 0 else float("inf")
        return spread_value <= max_allowed

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
        normalized = str(symbol or "").upper().replace(" ", "").replace("/", "")
        for quote in ("USDC", "USDT"):
            if normalized.endswith(quote) and len(normalized) > len(quote):
                return f"{normalized[:-len(quote)]}USD"
        return normalized

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()
