import json
import os
import queue
import shutil
import subprocess
import sys
import threading
import tkinter as tk
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any

import requests

from broker.broker_client import AlpacaBrokerClient
from config import settings
from data.market_data import MarketDataService
from data.nyse_calendar import NyseCalendarService
from orders.order_manager import OrderManager
from portfolio.position_manager import PositionManager, PositionSnapshot
from scheduling.market_open_scheduler import MarketOpenScheduler
from risk.risk_manager import RiskManager
from strategies.scalping_strategy import ScalpingStrategy
from runtime.alpaca_state import AlpacaRuntimeState
from utils.power_inhibit import PowerInhibitor, start_power_inhibitor


class BotControlWindow:
    def __init__(
        self,
        broker: Any,
        market_data: Any,
        risk_manager: RiskManager,
        strategy: ScalpingStrategy,
        order_manager: Any,
        position_manager: PositionManager,
        scheduler: MarketOpenScheduler,
        ai_trading_brain: Any,
        logger: Any,
    ) -> None:
        self.broker = broker
        self.market_data = market_data
        self.risk_manager = risk_manager
        self.strategy = strategy
        self.order_manager = order_manager
        self.position_manager = position_manager
        self.scheduler = scheduler
        self.ai_trading_brain = ai_trading_brain
        self.logger = logger
        self.nyse_calendar = NyseCalendarService(logger=logger)

        self.root = tk.Tk()
        self.root.title("Trading Bot Control")
        self._fit_window_to_screen(self.root, preferred_width=1080, preferred_height=720, min_width=860, min_height=620)
        self._ui_compact_mode = bool(self.root.winfo_screenheight() < 900)
        self._ui_thread_ident = threading.get_ident()
        self._root_after_original = self.root.after
        self._ui_after_queue: queue.PriorityQueue[tuple[float, int, Any, tuple[Any, ...]]] = queue.PriorityQueue()
        self._ui_after_counter = 0
        self._ui_max_log_lines = max(int(getattr(settings, "ui_max_log_lines", 1000) or 1000), 200)
        self.root.after = self._thread_safe_after  # type: ignore[method-assign]

        self.account_profiles = settings.account_profiles()

        default_account = next(iter(self.account_profiles.keys()), "Demo")
        initial_account = default_account if default_account in self.account_profiles else next(iter(self.account_profiles.keys()), "Demo")

        self.account_var = tk.StringVar(value=initial_account)
        self.market_kind_var = tk.StringVar(value="Stocks")
        self.account_mode_var = tk.StringVar(value="Modo activo: N/A")
        self.balance_var = tk.StringVar(value="Saldos: no consultados")
        self.accounts_overview_var = tk.StringVar(value="Cuentas: cargando resumen...")
        self.nyse_status_var = tk.StringVar(value="NYSE: cargando estado...")
        self.nyse_next_var = tk.StringVar(value="Proxima apertura NYSE: calculando...")
        self.nyse_early_var = tk.StringVar(value="Proximo cierre temprano NYSE: calculando...")
        self.nyse_days_var = tk.StringVar(value="")
        self.stock_count_var = tk.StringVar(value="Activos cargados: 0")
        self.stock_var = tk.StringVar(value=settings.default_symbol)
        self.capital_var = tk.StringVar(value=str(settings.default_trade_capital))
        self.target_profit_var = tk.StringVar(value=str(settings.target_profit_per_share))
        self._target_profit_cached = float(settings.target_profit_per_share)
        self.interval_var = tk.StringVar(value=settings.default_interval)
        self.daily_pnl_var = tk.StringVar(value="0")
        self.cancel_order_var = tk.StringVar()
        self.history_delete_var = tk.StringVar()
        self.status_var = tk.StringVar(value="Listo")
        self.app_health_var = tk.StringVar(value="App status: N/A")
        self.ws_health_var = tk.StringVar(value="WebSocket: N/A")
        self.api_health_var = tk.StringVar(value="Broker/API: N/A")
        self.vpn_badge_var = tk.StringVar(value="VPN: esperando conexión...")
        self._order_alias_map: dict[str, str] = {}

        self._monitor_in_flight = False
        self._watch_tabs: dict[str, dict[str, Any]] = {}
        self._schedule_tabs: dict[str, dict[str, Any]] = {}
        self._watch_lock = threading.Lock()
        self._watch_counter = 0
        self._account_runtimes: dict[str, dict[str, Any]] = {}
        self._account_runtime_states: dict[str, AlpacaRuntimeState] = {}
        self._account_monitor_cooldown_until: dict[str, float] = {}
        self._account_watch_http_cooldown_until: dict[str, float] = {}
        self._ai_watch_sync_in_flight = False
        self._ai_watch_last_sync_ts = 0.0
        self._watch_state_path = Path(__file__).resolve().parents[1] / "watch_tabs_state.json"
        self._ai_ui_state_path = Path(__file__).resolve().parents[1] / "ai_ui_state.json"
        self._ui_heartbeat_path = Path(__file__).resolve().parents[1] / "runtime" / f"ui_heartbeat_{os.getpid()}.json"
        self._ui_heartbeat_interval_ms = max(int(float(os.getenv("UI_HEARTBEAT_INTERVAL_MS", "2000") or 2000)), 500)
        self._ui_heartbeat_timeout_seconds = max(float(os.getenv("UI_HEARTBEAT_TIMEOUT_SECONDS", "90") or 90), 20.0)
        self._emergency_stale_checks_required = max(int(float(os.getenv("EMERGENCY_STALE_CHECKS_REQUIRED", "3") or 3)), 1)
        self._emergency_min_uptime_before_restart_seconds = max(
            float(os.getenv("EMERGENCY_MIN_UPTIME_BEFORE_RESTART_SECONDS", "45") or 45),
            5.0,
        )
        self._emergency_helper_enabled = str(os.getenv("ENABLE_EMERGENCY_HELPER", "true") or "true").strip().lower() == "true"
        self._history_tab_frame: ttk.Frame | None = None
        self._power_inhibitor: PowerInhibitor | None = None
        self._emergency_close_process: subprocess.Popen[str] | None = None
        self._emergency_helper_log_path = Path(__file__).resolve().parents[1] / "runtime" / "emergency_helper.log"
        self._emergency_helper_backoff_until = 0.0
        self._is_closing = False
        self._startup_restore_done = False
        self._network_degraded = False
        self._monitor_success_streak = 0
        self._monitor_failure_streak = 0
        self._vpn_ever_ready = False
        self._vpn_forced_pause_active = False
        self._vpn_bootstrap_in_flight = False
        self._vpn_guard_in_flight = False
        self._vpn_last_ready_state: bool | None = None
        self._vpn_last_connect_attempt_ts = 0.0
        self._tailscale_disconnect_in_flight = False
        self._tailscale_last_disconnect_attempt_ts = 0.0
        self._transient_log_last_ts: dict[str, float] = {}
        self._latest_ai_signal_id: int | None = None
        self.ai_window: tk.Toplevel | None = None
        self.ai_diagnostics_window: tk.Toplevel | None = None
        self.ai_history_window: tk.Toplevel | None = None
        self.ai_history_tree: ttk.Treeview | None = None
        self.ai_history_account_filter_var = tk.StringVar(value="Todas")
        self.ai_history_side_filter_var = tk.StringVar(value="Todos")
        self.ai_history_engine_filter_var = tk.StringVar(value="Todos")
        self.ai_history_status_filter_var = tk.StringVar(value="Todos")
        self._ai_automation_account = initial_account
        default_diag_market = "Cryptos" if ("/" in str(settings.default_symbol).upper() or str(settings.default_symbol).upper().endswith("USD")) else "Stocks"
        self.ai_diag_market_kind_var = tk.StringVar(value=default_diag_market)
        self.ai_diag_symbol_var = tk.StringVar(value=str(settings.default_symbol))
        self.ai_diag_asset_type_var = tk.StringVar(value="crypto" if default_diag_market == "Cryptos" else "stock")
        self.ai_diag_account_var = tk.StringVar(value=initial_account)
        self.ai_diag_max_spread_var = tk.StringVar(value=str(getattr(settings, "ai_max_spread_allowed", 0.05)))
        self.ai_diag_min_volume_var = tk.StringVar(value=str(getattr(settings, "ai_min_volume_24h_usd", 100000.0)))
        self.ai_diag_min_confidence_var = tk.StringVar(value=str(getattr(settings, "ai_min_execution_confidence", 60.0)))
        self.ai_diag_target_profit_var = tk.StringVar(value=str(getattr(settings, "ai_target_profit_per_operation", 0.05)))
        self.ai_diag_target_profit_stocks_var = tk.StringVar(value=str(getattr(settings, "ai_target_profit_per_operation_stocks", getattr(settings, "ai_target_profit_per_operation", 0.05))))
        self.ai_diag_target_profit_cryptos_var = tk.StringVar(value=str(getattr(settings, "ai_target_profit_per_operation_cryptos", getattr(settings, "ai_target_profit_per_operation", 0.05))))
        self.ai_diag_fees_var = tk.StringVar(value=str(getattr(settings, "ai_fees_buffer", 0.02)))
        self.ai_diag_slippage_var = tk.StringVar(value=str(getattr(settings, "ai_slippage_buffer", 0.03)))
        self.ai_diag_min_profit_var = tk.StringVar(value=str(getattr(settings, "ai_minimum_profit", 0.05)))
        self.ai_diag_signal_only_var = tk.IntVar(value=1 if settings.ai_signal_only_mode else 0)
        self.ai_diag_auto_stocks_var = tk.IntVar(value=1)
        self.ai_diag_auto_cryptos_var = tk.IntVar(value=1)
        self.ai_diag_details_text: tk.Text | None = None
        self.ai_diag_reco_text: tk.Text | None = None
        self.ai_max_capital_var = tk.StringVar(value=str(settings.ai_default_max_capital_assigned))
        self.ai_max_position_var = tk.StringVar(value=str(settings.ai_default_max_position_size))
        self.ai_max_daily_loss_var = tk.StringVar(value=str(settings.ai_default_max_daily_loss))
        self.ai_target_profit_var = tk.StringVar(value=str(getattr(settings, "ai_target_profit_per_operation", 0.05)))
        self.ai_target_profit_stocks_var = tk.StringVar(value=str(getattr(settings, "ai_target_profit_per_operation_stocks", getattr(settings, "ai_target_profit_per_operation", 0.05))))
        self.ai_target_profit_cryptos_var = tk.StringVar(value=str(getattr(settings, "ai_target_profit_per_operation_cryptos", getattr(settings, "ai_target_profit_per_operation", 0.05))))
        self.ai_bot_enabled_var = tk.IntVar(value=1)
        self.ai_signal_only_var = tk.IntVar(value=1 if settings.ai_signal_only_mode else 0)
        self.ai_paper_trading_var = tk.IntVar(value=1 if settings.paper_trading else 0)
        self.ai_live_enabled_var = tk.IntVar(value=1 if settings.live_trading_enabled else 0)
        self.ai_manual_approval_var = tk.IntVar(value=1 if settings.manual_approval_required else 0)
        self.ai_kill_switch_var = tk.IntVar(value=0)
        self.ai_auto_trade_stocks_var = tk.IntVar(value=1)
        self.ai_auto_trade_cryptos_var = tk.IntVar(value=1)
        self.ai_decision_engine_var = tk.StringVar(value="heuristic")
        self.ai_futures_only_mode_var = tk.IntVar(value=1 if bool(getattr(settings, "crypto_futures_only_mode", True)) else 0)
        self.ai_futures_leverage_var = tk.StringVar(value=str(max(int(getattr(settings, "crypto_futures_default_leverage", 1) or 1), 1)))
        self.ai_futures_require_technical_var = tk.IntVar(value=1 if bool(getattr(settings, "ai_futures_require_technical", True)) else 0)
        self.ai_futures_require_news_var = tk.IntVar(value=1 if bool(getattr(settings, "ai_futures_require_news", False)) else 0)
        self.ai_futures_enable_long_var = tk.IntVar(value=1 if bool(getattr(settings, "ai_futures_enable_long", True)) else 0)
        self.ai_futures_enable_short_var = tk.IntVar(value=1 if bool(getattr(settings, "ai_futures_enable_short", True)) else 0)
        self.ai_pending_status_var = tk.StringVar(value="IA pendiente: --")
        self.ai_focus_stocks_only_var = tk.IntVar(value=1 if bool(getattr(settings, "ai_focus_stocks_only", False)) else 0)
        self.ai_focus_cryptos_only_var = tk.IntVar(value=1 if bool(getattr(settings, "ai_focus_cryptos_only", False)) else 0)
        self.ai_focus_stocks_symbols_var = tk.StringVar(value=str(getattr(settings, "ai_focus_stocks_symbols", "") or ""))
        self.ai_focus_cryptos_symbols_var = tk.StringVar(value=str(getattr(settings, "ai_focus_cryptos_symbols", "") or ""))
        self.ai_focus_stock_pick_var = tk.StringVar(value="")
        self.ai_focus_crypto_pick_var = tk.StringVar(value="")
        self.ai_focus_stock_values: list[str] = []
        self.ai_focus_crypto_values: list[str] = []
        self.ai_focus_stock_selected: list[str] = []
        self.ai_focus_crypto_selected: list[str] = []
        self.ai_focus_stocks_selected_var = tk.StringVar(value="(ninguno)")
        self.ai_focus_cryptos_selected_var = tk.StringVar(value="(ninguno)")
        self.ai_header_stocks_var = tk.StringVar(value="Stocks: --")
        self.ai_header_cryptos_var = tk.StringVar(value="Cryptos: --")
        self.ai_badge_stocks_var = tk.StringVar(value="Ejecucion Stocks: --")
        self.ai_badge_cryptos_var = tk.StringVar(value="Ejecucion Cryptos: --")
        self.ai_badge_learning_var = tk.StringVar(value="Aprendizaje IA: --")
        self.ai_model_reco_var = tk.StringVar(value="Semaforo modelo: N/A")
        self.ai_model_selected_var = tk.StringVar(value="")
        self.ai_model_alias_var = tk.StringVar(value="")
        self._ai_model_versions_values: list[str] = []
        self._ai_model_version_map: dict[str, str] = {}
        self._ai_news_autofill_text = ""
        self.ai_crypto_monitor_text: tk.Text | None = None

        # Config panel variables
        self.config_capital_var = tk.StringVar(value=str(settings.default_trade_capital))
        self.config_risk_pct_var = tk.StringVar(value=str(settings.risk_per_trade_pct))
        self.config_max_daily_loss_var = tk.StringVar(value=str(settings.max_daily_loss))
        self.config_max_open_pos_var = tk.StringVar(value=str(settings.max_open_positions))
        self.config_stock_allow_hold_var = tk.IntVar(value=1 if bool(getattr(settings, "stock_allow_hold", True)) else 0)
        self.config_stock_allow_stop_loss_var = tk.IntVar(value=1 if bool(getattr(settings, "stock_allow_stop_loss", False)) else 0)
        self.config_stock_no_auto_sell_below_avg_cost_var = tk.IntVar(
            value=1 if bool(getattr(settings, "stock_no_auto_sell_below_avg_cost", True)) else 0
        )
        self.config_stock_auto_sell_profit_pct_var = tk.StringVar(value=str(getattr(settings, "stock_auto_sell_profit_pct", 0.5)))
        self.config_btc_sl_var = tk.StringVar(value=str(settings.crypto_btc_eth_stop_loss_pct))
        self.config_btc_tp1_var = tk.StringVar(value=str(settings.crypto_btc_eth_tp1_pct))
        self.config_btc_tp2_var = tk.StringVar(value=str(settings.crypto_btc_eth_tp2_pct))
        self.config_btc_max_tp_var = tk.StringVar(value=str(settings.crypto_btc_eth_max_tp_pct))
        self.config_alt_sl_var = tk.StringVar(value=str(settings.crypto_alt_stop_loss_pct))
        self.config_alt_tp1_var = tk.StringVar(value=str(settings.crypto_alt_tp1_pct))
        self.config_alt_tp2_var = tk.StringVar(value=str(settings.crypto_alt_tp2_pct))
        self.config_alt_max_tp_var = tk.StringVar(value=str(settings.crypto_alt_max_tp_pct))
        self.config_meme_sl_var = tk.StringVar(value=str(settings.crypto_meme_stop_loss_pct))
        self.config_meme_tp1_var = tk.StringVar(value=str(settings.crypto_meme_tp1_pct))
        self.config_meme_tp2_var = tk.StringVar(value=str(settings.crypto_meme_tp2_pct))
        self.config_meme_max_tp_var = tk.StringVar(value=str(settings.crypto_meme_max_tp_pct))
        self.config_ai_target_profit_var = tk.StringVar(value=str(getattr(settings, "ai_target_profit_per_operation", 0.05)))
        self.config_ai_target_profit_stocks_var = tk.StringVar(value=str(getattr(settings, "ai_target_profit_per_operation_stocks", getattr(settings, "ai_target_profit_per_operation", 0.05))))
        self.config_ai_target_profit_cryptos_var = tk.StringVar(value=str(getattr(settings, "ai_target_profit_per_operation_cryptos", getattr(settings, "ai_target_profit_per_operation", 0.05))))
        self.config_cp_monthly_limit_var = tk.StringVar(value=str(getattr(settings, "cryptopanic_monthly_limit", 600)))
        self.config_cp_used_baseline_var = tk.StringVar(value=str(getattr(settings, "cryptopanic_used_this_month", 0)))
        self.config_cp_used_var = tk.StringVar(value="0")
        self.config_cp_remaining_var = tk.StringVar(value="0")
        self.config_cp_today_budget_var = tk.StringVar(value="0")
        self.config_cp_today_used_var = tk.StringVar(value="0")
        self.config_cp_days_visible_var = tk.StringVar(value="mon,tue,wed,thu,fri")
        self.config_cp_today_active_var = tk.StringVar(value="No")
        self.config_cp_active_days_remaining_var = tk.StringVar(value="0")
        self.config_cp_request_days_vars: dict[int, tk.IntVar] = {idx: tk.IntVar(value=0) for idx in range(7)}
        self._ai_crypto_monitor_refresh_in_flight = False
        self._ai_runtime_refresh_in_flight = False

        self._load_ai_ui_state()

        self._build_ui()
        self._build_ai_window()
        self._power_inhibitor = start_power_inhibitor(self.logger)
        if self._emergency_helper_enabled:
            self._start_emergency_close_helper()
        self._apply_selected_account(update_status=False, require_credentials=False)
        threading.Thread(target=self._refresh_accounts_header_summary, daemon=True).start()
        threading.Thread(target=self._refresh_ai_views, daemon=True).start()

    def _build_ui(self) -> None:
        container = ttk.Frame(self.root, padding=16)
        container.pack(fill="both", expand=True)

        header_row = ttk.Frame(container)
        header_row.pack(fill="x", pady=(0, 10))

        header = ttk.Label(
            header_row,
            text="Panel de control del bot",
            font=("TkDefaultFont", 14, "bold"),
        )
        header.pack(side="left", anchor="w")
        self.force_close_button = tk.Button(
            header_row,
            text="Forzar cierre",
            command=self._force_close,
            fg="white",
            bg="#8a1c1c",
            activebackground="#6b1414",
            relief="raised",
            bd=1,
            padx=10,
            pady=3,
        )
        self.force_close_button.pack(side="right", padx=(10, 0))
        ttk.Label(
            header_row,
            textvariable=self.accounts_overview_var,
            foreground="#1d5f2a",
            justify="right",
        ).pack(side="right", anchor="e")

        nyse_row = ttk.Frame(container)
        nyse_row.pack(fill="x", pady=(0, 8))
        ttk.Label(nyse_row, textvariable=self.nyse_status_var, foreground="#113a6b").pack(side="left")
        ttk.Label(nyse_row, text=" | ", foreground="#666").pack(side="left", padx=(8, 8))
        ttk.Label(nyse_row, textvariable=self.nyse_next_var, foreground="#113a6b").pack(side="left")

        nyse_early_row = ttk.Frame(container)
        nyse_early_row.pack(fill="x", pady=(0, 8))
        ttk.Label(nyse_early_row, textvariable=self.nyse_early_var, foreground="#8a4d00").pack(side="left")

        nyse_actions_row = ttk.Frame(container)
        nyse_actions_row.pack(fill="x", pady=(0, 8))
        ttk.Label(nyse_actions_row, textvariable=self.nyse_days_var, foreground="#444").pack(side="left")
        self.nyse_refresh_button = ttk.Button(
            nyse_actions_row,
            text="Actualizar NYSE",
            command=lambda: self._run_async(self._view_nyse_calendar),
        )
        self.nyse_refresh_button.pack(side="right")

        account_row = ttk.Frame(container)
        account_row.pack(fill="x", pady=(0, 8))

        ttk.Label(account_row, text="Cuenta").grid(row=0, column=0, sticky="w")
        self.account_combo = ttk.Combobox(
            account_row,
            textvariable=self.account_var,
            values=list(self.account_profiles.keys()),
            width=12,
            state="readonly",
        )
        self.account_combo.grid(row=1, column=0, padx=(0, 10), sticky="w")
        self.account_combo.bind("<<ComboboxSelected>>", lambda _event: self._on_main_account_selected())

        self.switch_account_button = ttk.Button(
            account_row,
            text="Cambiar cuenta",
            command=lambda: self._run_async(self._switch_account),
        )
        self.switch_account_button.grid(row=1, column=1, padx=(0, 12), sticky="w")

        ttk.Label(account_row, textvariable=self.account_mode_var).grid(row=1, column=2, sticky="w")
        ttk.Label(account_row, textvariable=self.balance_var, foreground="#1d5f2a").grid(
            row=1,
            column=3,
            padx=(12, 0),
            sticky="w",
        )

        config_row = ttk.Frame(container)
        config_row.pack(fill="x", pady=(0, 8))

        ttk.Label(config_row, text="Activo").grid(row=0, column=0, sticky="w")
        self.stock_combo = ttk.Combobox(
            config_row,
            textvariable=self.stock_var,
            values=[],
            width=18,
            state="normal",
        )
        self.stock_combo.grid(row=1, column=0, padx=(0, 10), sticky="w")

        ttk.Label(config_row, text="Mercado").grid(row=0, column=1, sticky="w")
        self.market_kind_combo = ttk.Combobox(
            config_row,
            textvariable=self.market_kind_var,
            values=["Stocks", "Cryptos"],
            width=12,
            state="readonly",
        )
        self.market_kind_combo.grid(row=1, column=1, padx=(0, 10), sticky="w")
        self.market_kind_combo.bind(
            "<<ComboboxSelected>>",
            lambda _event: self._run_async(self._refresh_stock_selector),
        )

        self.refresh_stocks_button = ttk.Button(
            config_row,
            text="Actualizar activos",
            command=lambda: self._run_async(self._refresh_stock_selector),
        )
        self.refresh_stocks_button.grid(row=1, column=2, padx=(0, 10), sticky="w")

        ttk.Label(config_row, textvariable=self.stock_count_var).grid(row=0, column=2, sticky="w")

        ttk.Label(config_row, text="Capital/Compra").grid(row=0, column=3, sticky="w")
        ttk.Entry(config_row, textvariable=self.capital_var, width=12).grid(
            row=1,
            column=3,
            padx=(0, 10),
            sticky="w",
        )

        ttk.Label(config_row, text="Target USD total").grid(row=0, column=4, sticky="w")
        ttk.Entry(config_row, textvariable=self.target_profit_var, width=12).grid(
            row=1,
            column=4,
            padx=(0, 10),
            sticky="w",
        )

        ttk.Label(config_row, text="Interval").grid(row=0, column=5, sticky="w")
        ttk.Entry(config_row, textvariable=self.interval_var, width=10).grid(
            row=1,
            column=5,
            padx=(0, 10),
            sticky="w",
        )

        ttk.Label(config_row, text="PnL diario").grid(row=0, column=6, sticky="w")
        ttk.Entry(config_row, textvariable=self.daily_pnl_var, width=12).grid(
            row=1,
            column=6,
            padx=(0, 10),
            sticky="w",
        )

        action_row = ttk.Frame(container)
        action_row.pack(fill="x", pady=(4, 8))

        self.account_button = ttk.Button(
            action_row,
            text="Ver cuenta",
            command=lambda: self._run_async(self._view_account),
        )
        self.account_button.pack(side="left")

        self.stock_price_button = ttk.Button(
            action_row,
            text="Ver precio activo",
            command=lambda: self._run_async(self._view_stock_price),
        )
        self.stock_price_button.pack(side="left", padx=(8, 0))

        self.strategy_button = ttk.Button(
            action_row,
            text="Ejecutar estrategia",
            command=self._start_entry_watch,
        )
        self.strategy_button.pack(side="left", padx=(8, 0))

        self.cancel_entry_watch_button = ttk.Button(
            action_row,
            text="Cerrar pestaña activa",
            command=self._request_close_selected_watch_tab,
        )
        self.cancel_entry_watch_button.pack(side="left", padx=(8, 0))

        self.schedule_button = ttk.Button(
            action_row,
            text="Programar apertura",
            command=lambda: self._run_async(self._schedule_selected_stock),
        )
        self.schedule_button.pack(side="left", padx=(8, 0))

        self.manual_sell_button = ttk.Button(
            action_row,
            text="Venta manual",
            command=lambda: self._run_async(self._manual_sell_selected_stock),
        )
        self.manual_sell_button.pack(side="left", padx=(8, 0))

        self.dashboard_button = ttk.Button(
            action_row,
            text="Dashboard",
            command=lambda: self._run_async(self._refresh_dashboard),
        )
        self.dashboard_button.pack(side="left", padx=(8, 0))

        self.schedules_button = ttk.Button(
            action_row,
            text="Ver programadas",
            command=lambda: self._run_async(self._view_schedules),
        )
        self.schedules_button.pack(side="left", padx=(8, 0))

        self.orders_button = ttk.Button(
            action_row,
            text="Ver ordenes",
            command=lambda: self._run_async(self._view_orders),
        )
        self.orders_button.pack(side="left", padx=(8, 0))

        self.cancel_pending_button = ttk.Button(
            action_row,
            text="Cancelar pendientes",
            command=lambda: self._run_async(self._cancel_all_pending_orders),
        )
        self.cancel_pending_button.pack(side="left", padx=(8, 0))

        self.history_button = ttk.Button(
            action_row,
            text="Historial Bot normal",
            command=lambda: self._run_async(self._view_history),
        )
        self.history_button.pack(side="left", padx=(8, 0))

        self.stocks_button = ttk.Button(
            action_row,
            text="Ver activos",
            command=lambda: self._run_async(self._view_stocks),
        )
        self.stocks_button.pack(side="left", padx=(8, 0))

        self.all_cryptos_button = ttk.Button(
            action_row,
            text="Ver todas cryptos",
            command=lambda: self._run_async(self._view_all_cryptos),
        )
        self.all_cryptos_button.pack(side="left", padx=(8, 0))

        self.ai_window_button = ttk.Button(
            action_row,
            text="Ventana IA",
            command=self._show_ai_window,
        )
        self.ai_window_button.pack(side="left", padx=(8, 0))

        safety_row = ttk.Frame(container)
        safety_row.pack(fill="x", pady=(2, 8))
        ttk.Button(safety_row, text="Pause Bot", command=lambda: self._run_async(self._pause_bot)).pack(side="left")
        ttk.Button(safety_row, text="Resume Bot", command=lambda: self._run_async(self._resume_bot)).pack(side="left", padx=(6, 0))
        ttk.Button(safety_row, text="Emergency Stop", command=lambda: self._run_async(self._emergency_stop)).pack(side="left", padx=(6, 0))
        ttk.Button(safety_row, text="Reconnect WebSocket", command=lambda: self._run_async(self._reconnect_websocket)).pack(side="left", padx=(6, 0))
        ttk.Button(safety_row, text="Sync Positions", command=lambda: self._run_async(self._sync_positions_now)).pack(side="left", padx=(6, 0))
        ttk.Button(safety_row, text="Clear UI Logs", command=self._clear_ui_logs).pack(side="left", padx=(6, 0))
        ttk.Button(safety_row, text="Export Crash Report", command=lambda: self._run_async(self._export_crash_report)).pack(side="left", padx=(6, 0))

        ttk.Separator(container).pack(fill="x", pady=12)
        ttk.Label(container, textvariable=self.status_var, foreground="#555").pack(anchor="w")
        ttk.Label(container, textvariable=self.app_health_var, foreground="#1f4d7a").pack(anchor="w")
        ttk.Label(container, textvariable=self.ws_health_var, foreground="#1f4d7a").pack(anchor="w")
        ttk.Label(container, textvariable=self.api_health_var, foreground="#1f4d7a").pack(anchor="w")
        self.vpn_badge_label = tk.Label(
            container,
            textvariable=self.vpn_badge_var,
            fg="white",
            bg="#9a6700",
            padx=10,
            pady=4,
            relief="ridge",
            bd=1,
        )
        self.vpn_badge_label.pack(anchor="w", pady=(4, 0))

        self.log_notebook = ttk.Notebook(container)
        self.log_notebook.pack(fill="both", expand=True, pady=(8, 0))

        general_frame = ttk.Frame(self.log_notebook)
        self.log_notebook.add(general_frame, text="General")
        self._general_tab = general_frame

        self.output = tk.Text(general_frame, height=20, wrap="word")
        self.output.pack(fill="both", expand=True)
        self.output.configure(state="disabled")

        # Config Tab
        self._build_config_tab()

    def _mousewheel_steps(self, event: Any) -> int:
        delta = int(getattr(event, "delta", 0) or 0)
        if delta != 0:
            return int(-delta / 120)
        num = int(getattr(event, "num", 0) or 0)
        if num == 4:
            return -1
        if num == 5:
            return 1
        return 0

    def _scroll_target_under_mouse(self, event: Any) -> Any:
        target = self.root.winfo_containing(int(getattr(event, "x_root", 0) or 0), int(getattr(event, "y_root", 0) or 0))
        if target is None:
            target = getattr(event, "widget", None)
        while target is not None:
            if isinstance(target, (tk.Text, tk.Listbox, tk.Canvas)):
                return target
            target = getattr(target, "master", None)
        return None

    def _on_scoped_mousewheel(self, event: Any, *, fallback_canvas: tk.Canvas) -> str | None:
        steps = self._mousewheel_steps(event)
        if steps == 0:
            return None
        target = self._scroll_target_under_mouse(event)
        if target is None:
            target = fallback_canvas
        try:
            target.yview_scroll(steps, "units")
            return "break"
        except Exception:
            return None

    def _build_config_tab(self) -> None:
        """Construye tab de configuración de riesgo y parámetros de trading"""
        config_frame = ttk.Frame(self.log_notebook)
        self.log_notebook.add(config_frame, text="⚙️ Configuración")

        # Canvas + scrollbar para scroll vertical
        canvas = tk.Canvas(config_frame)
        scrollbar = ttk.Scrollbar(config_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)

        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        # GENERAL SETTINGS
        general_lf = ttk.LabelFrame(scrollable_frame, text="Parámetros Generales", padding=10)
        general_lf.pack(fill="x", padx=8, pady=6)

        ttk.Label(general_lf, text="Capital por trade ($)").grid(row=0, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(general_lf, textvariable=self.config_capital_var, width=15).grid(row=0, column=1, padx=4, pady=4)

        ttk.Label(general_lf, text="Riesgo por trade (%)").grid(row=1, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(general_lf, textvariable=self.config_risk_pct_var, width=15).grid(row=1, column=1, padx=4, pady=4)

        ttk.Label(general_lf, text="Pérdida máxima diaria ($)").grid(row=2, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(general_lf, textvariable=self.config_max_daily_loss_var, width=15).grid(row=2, column=1, padx=4, pady=4)

        ttk.Label(general_lf, text="Máx posiciones abiertas").grid(row=3, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(general_lf, textvariable=self.config_max_open_pos_var, width=15).grid(row=3, column=1, padx=4, pady=4)
        ttk.Label(general_lf, text="Target IA Stocks $/operación").grid(row=3, column=2, sticky="w", padx=4, pady=4)
        ttk.Entry(general_lf, textvariable=self.config_ai_target_profit_stocks_var, width=12).grid(row=3, column=3, padx=4, pady=4, sticky="w")
        ttk.Label(general_lf, text="Target IA Cryptos $/operación").grid(row=3, column=4, sticky="w", padx=4, pady=4)
        ttk.Entry(general_lf, textvariable=self.config_ai_target_profit_cryptos_var, width=12).grid(row=3, column=5, padx=4, pady=4, sticky="w")

        ttk.Label(general_lf, text="Cuenta IA (fondos automáticos)").grid(row=4, column=0, sticky="w", padx=4, pady=4)
        self.config_ai_account_combo = ttk.Combobox(
            general_lf,
            textvariable=self.account_var,
            values=list(self.account_profiles.keys()),
            width=15,
            state="readonly",
        )
        self.config_ai_account_combo.grid(row=4, column=1, padx=4, pady=4, sticky="w")
        self.config_ai_account_combo.bind("<<ComboboxSelected>>", lambda _event: self._on_config_ai_account_selected())
        ttk.Button(
            general_lf,
            text="Aplicar cuenta IA",
            command=lambda: self._run_async(self._apply_config_account_for_ai),
        ).grid(row=4, column=2, padx=6, pady=4, sticky="w")

        ttk.Label(
            general_lf,
            text="La IA usará esta cuenta para fondos y operaciones automáticas (paper/live según configuración).",
            foreground="#555",
        ).grid(row=5, column=0, columnspan=3, sticky="w", padx=4, pady=(0, 4))

        ttk.Label(general_lf, text="Pausas por mercado (sin parar aprendizaje)").grid(
            row=6, column=0, sticky="w", padx=4, pady=(6, 4)
        )
        ttk.Checkbutton(general_lf, text="Operar Stocks", variable=self.ai_auto_trade_stocks_var).grid(
            row=6, column=1, sticky="w", padx=4, pady=(6, 4)
        )
        ttk.Checkbutton(general_lf, text="Operar Cryptos", variable=self.ai_auto_trade_cryptos_var).grid(
            row=6, column=2, sticky="w", padx=4, pady=(6, 4)
        )
        ttk.Button(
            general_lf,
            text="Guardar pausas IA",
            command=lambda: self._run_async(self._save_ai_runtime_controls),
        ).grid(row=7, column=1, sticky="w", padx=4, pady=(0, 4))

        quota_lf = ttk.LabelFrame(scrollable_frame, text="Cuota mensual CryptoPanic", padding=10)
        quota_lf.pack(fill="x", padx=8, pady=6)

        ttk.Label(quota_lf, text="Límite mensual").grid(row=0, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(quota_lf, textvariable=self.config_cp_monthly_limit_var, width=12).grid(row=0, column=1, sticky="w", padx=4, pady=4)

        ttk.Label(quota_lf, text="Usado inicial del mes").grid(row=0, column=2, sticky="w", padx=4, pady=4)
        ttk.Entry(quota_lf, textvariable=self.config_cp_used_baseline_var, width=12).grid(row=0, column=3, sticky="w", padx=4, pady=4)

        ttk.Label(quota_lf, text="Días permitidos").grid(row=1, column=0, sticky="w", padx=4, pady=4)
        day_labels = ["Lun", "Mar", "Mie", "Jue", "Vie", "Sab", "Dom"]
        for idx, day_label in enumerate(day_labels):
            ttk.Checkbutton(quota_lf, text=day_label, variable=self.config_cp_request_days_vars[idx]).grid(
                row=1,
                column=1 + idx,
                sticky="w",
                padx=2,
                pady=2,
            )

        ttk.Label(quota_lf, text="Usado total mes").grid(row=2, column=0, sticky="w", padx=4, pady=4)
        ttk.Label(quota_lf, textvariable=self.config_cp_used_var, foreground="#8a1c1c").grid(row=2, column=1, sticky="w", padx=4, pady=4)
        ttk.Label(quota_lf, text="Restante mes").grid(row=2, column=2, sticky="w", padx=4, pady=4)
        ttk.Label(quota_lf, textvariable=self.config_cp_remaining_var, foreground="#1f5f2a").grid(row=2, column=3, sticky="w", padx=4, pady=4)
        ttk.Label(quota_lf, text="Presupuesto hoy").grid(row=3, column=0, sticky="w", padx=4, pady=4)
        ttk.Label(quota_lf, textvariable=self.config_cp_today_budget_var).grid(row=3, column=1, sticky="w", padx=4, pady=4)
        ttk.Label(quota_lf, text="Usado hoy").grid(row=3, column=2, sticky="w", padx=4, pady=4)
        ttk.Label(quota_lf, textvariable=self.config_cp_today_used_var).grid(row=3, column=3, sticky="w", padx=4, pady=4)
        ttk.Button(
            quota_lf,
            text="Actualizar cuota",
            command=self._refresh_cryptopanic_quota_display_async,
        ).grid(row=3, column=4, sticky="w", padx=6, pady=4)

        ttk.Label(quota_lf, text="Días habilitados API").grid(row=4, column=0, sticky="w", padx=4, pady=4)
        ttk.Label(quota_lf, textvariable=self.config_cp_days_visible_var).grid(row=4, column=1, columnspan=3, sticky="w", padx=4, pady=4)
        ttk.Label(quota_lf, text="Hoy habilitado").grid(row=5, column=0, sticky="w", padx=4, pady=4)
        ttk.Label(quota_lf, textvariable=self.config_cp_today_active_var).grid(row=5, column=1, sticky="w", padx=4, pady=4)
        ttk.Label(quota_lf, text="Días activos restantes").grid(row=5, column=2, sticky="w", padx=4, pady=4)
        ttk.Label(quota_lf, textvariable=self.config_cp_active_days_remaining_var).grid(row=5, column=3, sticky="w", padx=4, pady=4)
        ttk.Button(
            quota_lf,
            text="Reset usado del mes",
            command=self._reset_cryptopanic_month_usage,
        ).grid(row=5, column=4, sticky="w", padx=6, pady=4)

        stock_lf = ttk.LabelFrame(scrollable_frame, text="📈 Configuración Stocks", padding=10)
        stock_lf.pack(fill="x", padx=8, pady=6)

        ttk.Checkbutton(stock_lf, text="Permitir HOLD en stocks", variable=self.config_stock_allow_hold_var).grid(
            row=0,
            column=0,
            sticky="w",
            padx=4,
            pady=4,
        )
        ttk.Checkbutton(stock_lf, text="Permitir stop loss automático", variable=self.config_stock_allow_stop_loss_var).grid(
            row=1,
            column=0,
            sticky="w",
            padx=4,
            pady=4,
        )
        ttk.Checkbutton(
            stock_lf,
            text="No vender automático por debajo del average cost",
            variable=self.config_stock_no_auto_sell_below_avg_cost_var,
        ).grid(
            row=2,
            column=0,
            sticky="w",
            padx=4,
            pady=4,
        )
        ttk.Label(stock_lf, text="Take Profit automático stocks (%)").grid(row=3, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(stock_lf, textvariable=self.config_stock_auto_sell_profit_pct_var, width=12).grid(
            row=3,
            column=1,
            sticky="w",
            padx=4,
            pady=4,
        )

        # TIER 1: BTC/ETH
        tier1_lf = ttk.LabelFrame(scrollable_frame, text="🟡 Tier 1 (BTC/ETH) - Principales", padding=10)
        tier1_lf.pack(fill="x", padx=8, pady=6)

        ttk.Label(tier1_lf, text="Stop Loss (%)").grid(row=0, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(tier1_lf, textvariable=self.config_btc_sl_var, width=12).grid(row=0, column=1, padx=4, pady=4)

        ttk.Label(tier1_lf, text="Target 1 (%)").grid(row=1, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(tier1_lf, textvariable=self.config_btc_tp1_var, width=12).grid(row=1, column=1, padx=4, pady=4)

        ttk.Label(tier1_lf, text="Target 2 (%)").grid(row=2, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(tier1_lf, textvariable=self.config_btc_tp2_var, width=12).grid(row=2, column=1, padx=4, pady=4)

        ttk.Label(tier1_lf, text="Target Máximo (%)").grid(row=3, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(tier1_lf, textvariable=self.config_btc_max_tp_var, width=12).grid(row=3, column=1, padx=4, pady=4)

        # TIER 2: ALT-COINS
        tier2_lf = ttk.LabelFrame(scrollable_frame, text="🟠 Tier 2 (ALT-coins) - SOL, XRP, LINK, etc", padding=10)
        tier2_lf.pack(fill="x", padx=8, pady=6)

        ttk.Label(tier2_lf, text="Stop Loss (%)").grid(row=0, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(tier2_lf, textvariable=self.config_alt_sl_var, width=12).grid(row=0, column=1, padx=4, pady=4)

        ttk.Label(tier2_lf, text="Target 1 (%)").grid(row=1, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(tier2_lf, textvariable=self.config_alt_tp1_var, width=12).grid(row=1, column=1, padx=4, pady=4)

        ttk.Label(tier2_lf, text="Target 2 (%)").grid(row=2, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(tier2_lf, textvariable=self.config_alt_tp2_var, width=12).grid(row=2, column=1, padx=4, pady=4)

        ttk.Label(tier2_lf, text="Target Máximo (%)").grid(row=3, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(tier2_lf, textvariable=self.config_alt_max_tp_var, width=12).grid(row=3, column=1, padx=4, pady=4)

        # TIER 3: MEME COINS
        tier3_lf = ttk.LabelFrame(scrollable_frame, text="🔴 Tier 3 (MEME) - PEPE, BONK, SHIB, etc", padding=10)
        tier3_lf.pack(fill="x", padx=8, pady=6)

        ttk.Label(tier3_lf, text="Stop Loss (%)").grid(row=0, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(tier3_lf, textvariable=self.config_meme_sl_var, width=12).grid(row=0, column=1, padx=4, pady=4)

        ttk.Label(tier3_lf, text="Target 1 (%)").grid(row=1, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(tier3_lf, textvariable=self.config_meme_tp1_var, width=12).grid(row=1, column=1, padx=4, pady=4)

        ttk.Label(tier3_lf, text="Target 2 (%)").grid(row=2, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(tier3_lf, textvariable=self.config_meme_tp2_var, width=12).grid(row=2, column=1, padx=4, pady=4)

        ttk.Label(tier3_lf, text="Target Máximo (%)").grid(row=3, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(tier3_lf, textvariable=self.config_meme_max_tp_var, width=12).grid(row=3, column=1, padx=4, pady=4)

        # Buttons
        buttons_frame = ttk.Frame(scrollable_frame)
        buttons_frame.pack(fill="x", padx=8, pady=12)

        ttk.Button(
            buttons_frame,
            text="💾 Guardar configuración",
            command=lambda: self._run_async(self._save_config)
        ).pack(side="left", padx=4)

        ttk.Button(
            buttons_frame,
            text="🔄 Recargar valores",
            command=lambda: self._refresh_config_values()
        ).pack(side="left", padx=4)

        self._apply_request_days_from_text(str(getattr(settings, "cryptopanic_request_days", "mon,tue,wed,thu,fri") or "mon,tue,wed,thu,fri"))
        self._refresh_cryptopanic_quota_display_async()

        # Pack canvas and scrollbar
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

    def _refresh_config_values(self) -> None:
        """Recarga los valores actuales desde settings"""
        self.config_capital_var.set(str(settings.default_trade_capital))
        self.config_risk_pct_var.set(str(settings.risk_per_trade_pct))
        self.config_max_daily_loss_var.set(str(settings.max_daily_loss))
        self.config_max_open_pos_var.set(str(settings.max_open_positions))
        self.config_stock_allow_hold_var.set(1 if bool(getattr(settings, "stock_allow_hold", True)) else 0)
        self.config_stock_allow_stop_loss_var.set(1 if bool(getattr(settings, "stock_allow_stop_loss", False)) else 0)
        self.config_stock_no_auto_sell_below_avg_cost_var.set(
            1 if bool(getattr(settings, "stock_no_auto_sell_below_avg_cost", True)) else 0
        )
        self.config_stock_auto_sell_profit_pct_var.set(str(getattr(settings, "stock_auto_sell_profit_pct", 0.5)))
        self.config_btc_sl_var.set(str(settings.crypto_btc_eth_stop_loss_pct))
        self.config_btc_tp1_var.set(str(settings.crypto_btc_eth_tp1_pct))
        self.config_btc_tp2_var.set(str(settings.crypto_btc_eth_tp2_pct))
        self.config_btc_max_tp_var.set(str(settings.crypto_btc_eth_max_tp_pct))
        self.config_alt_sl_var.set(str(settings.crypto_alt_stop_loss_pct))
        self.config_alt_tp1_var.set(str(settings.crypto_alt_tp1_pct))
        self.config_alt_tp2_var.set(str(settings.crypto_alt_tp2_pct))
        self.config_alt_max_tp_var.set(str(settings.crypto_alt_max_tp_pct))
        self.config_meme_sl_var.set(str(settings.crypto_meme_stop_loss_pct))
        self.config_meme_tp1_var.set(str(settings.crypto_meme_tp1_pct))
        self.config_meme_tp2_var.set(str(settings.crypto_meme_tp2_pct))
        self.config_meme_max_tp_var.set(str(settings.crypto_meme_max_tp_pct))
        self.config_ai_target_profit_var.set(str(getattr(settings, "ai_target_profit_per_operation", 0.05)))
        self.config_ai_target_profit_stocks_var.set(str(getattr(settings, "ai_target_profit_per_operation_stocks", getattr(settings, "ai_target_profit_per_operation", 0.05))))
        self.config_ai_target_profit_cryptos_var.set(str(getattr(settings, "ai_target_profit_per_operation_cryptos", getattr(settings, "ai_target_profit_per_operation", 0.05))))
        self.config_cp_monthly_limit_var.set(str(getattr(settings, "cryptopanic_monthly_limit", 600)))
        self.config_cp_used_baseline_var.set(str(getattr(settings, "cryptopanic_used_this_month", 0)))
        self._apply_request_days_from_text(str(getattr(settings, "cryptopanic_request_days", "mon,tue,wed,thu,fri") or "mon,tue,wed,thu,fri"))
        self._refresh_cryptopanic_quota_display_async()
        self._set_output("✅ Valores recargados desde configuración actual")

    def _apply_request_days_from_text(self, request_days_text: str) -> None:
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
        selected: set[int] = set()
        for raw in str(request_days_text or "").replace(";", ",").split(","):
            token = raw.strip().lower()
            if not token:
                continue
            if token.isdigit():
                idx = int(token)
                if 0 <= idx <= 6:
                    selected.add(idx)
                continue
            if token in aliases:
                selected.add(aliases[token])
        if not selected:
            selected = {0, 1, 2, 3, 4}
        for idx, var in self.config_cp_request_days_vars.items():
            var.set(1 if idx in selected else 0)

    def _request_days_to_text(self) -> str:
        names = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
        selected = [names[idx] for idx, var in self.config_cp_request_days_vars.items() if bool(var.get())]
        if not selected:
            selected = ["mon", "tue", "wed", "thu", "fri"]
        return ",".join(selected)

    def _refresh_cryptopanic_quota_display(self) -> None:
        try:
            status = self.ai_trading_brain.get_cryptopanic_quota_status()
        except Exception as ex:
            self.logger.warning("No se pudo refrescar cuota CryptoPanic: %s", ex)
            return
        self._safe_after(0, self.config_cp_used_var.set, str(status.get("used_total", 0)))
        self._safe_after(0, self.config_cp_remaining_var.set, str(status.get("remaining_total", 0)))
        self._safe_after(0, self.config_cp_today_budget_var.set, str(status.get("today_budget", 0)))
        self._safe_after(0, self.config_cp_today_used_var.set, str(status.get("today_used", 0)))
        self._safe_after(0, self.config_cp_days_visible_var.set, str(status.get("request_days", "mon,tue,wed,thu,fri")))
        self._safe_after(0, self.config_cp_today_active_var.set, "Si" if bool(status.get("today_active", False)) else "No")
        self._safe_after(0, self.config_cp_active_days_remaining_var.set, str(status.get("active_days_remaining", 0)))

    def _refresh_cryptopanic_quota_display_async(self) -> None:
        threading.Thread(target=self._refresh_cryptopanic_quota_display, daemon=True).start()

    def _reset_cryptopanic_month_usage(self) -> None:
        if not messagebox.askyesno(
            "Reset CryptoPanic",
            "Esto reiniciara el usado del mes para comenzar desde cero. Deseas continuar?",
        ):
            return
        try:
            self.config_cp_used_baseline_var.set("0")
            self.ai_trading_brain.reset_cryptopanic_month_usage(used_baseline=0)
            self._save_config()
            self._refresh_cryptopanic_quota_display_async()
            self._set_output("✅ Uso mensual CryptoPanic reiniciado a 0 y guardado en .env", focus_general=False)
        except Exception as ex:
            self._show_error(f"No se pudo resetear cuota CryptoPanic: {ex}", False)

    def _apply_config_account_for_ai(self) -> None:
        """Aplica la cuenta seleccionada como cuenta activa para trading/fondos IA."""
        selected = self.account_var.get().strip()
        if not selected:
            self.root.after(0, self._show_error, "Selecciona una cuenta para IA.")
            return

        self._ai_automation_account = selected
        if hasattr(self, "ai_diag_account_var"):
            self.ai_diag_account_var.set(selected)

        self._apply_selected_account(update_status=True, require_credentials=True)
        self._save_ai_ui_state()

        # Load runtime controls for the selected account so IA funds view matches the active account.
        try:
            self._load_ai_runtime_controls()
        except Exception as ex:
            self.logger.warning("No se pudo refrescar controles IA para %s: %s", selected, ex)

        self.root.after(
            0,
            self._show_success,
            f"Cuenta IA aplicada: {selected}. Los fondos automáticos ahora salen de esta cuenta.",
            False,
        )

    def _on_main_account_selected(self) -> None:
        selected = self.account_var.get().strip()
        if hasattr(self, "ai_diag_account_var") and selected:
            self.ai_diag_account_var.set(selected)
        self._save_ai_ui_state()

    def _on_config_ai_account_selected(self) -> None:
        self._on_main_account_selected()
        self._run_async(self._apply_config_account_for_ai)

    def _save_config(self) -> None:
        """Guarda los cambios en la configuración al archivo .env"""
        from pathlib import Path
        import os
        import re

        try:
            # Recolectar valores
            values = {
                "DEFAULT_TRADE_CAPITAL": self.config_capital_var.get(),
                "BOT_RISK_PER_TRADE_PCT": self.config_risk_pct_var.get(),
                "BOT_MAX_DAILY_LOSS": self.config_max_daily_loss_var.get(),
                "MAX_OPEN_POSITIONS": self.config_max_open_pos_var.get(),
                "STOCK_ALLOW_HOLD": "True" if bool(self.config_stock_allow_hold_var.get()) else "False",
                "STOCK_ALLOW_STOP_LOSS": "True" if bool(self.config_stock_allow_stop_loss_var.get()) else "False",
                "STOCK_NO_AUTO_SELL_BELOW_AVG_COST": "True" if bool(self.config_stock_no_auto_sell_below_avg_cost_var.get()) else "False",
                "STOCK_AUTO_SELL_PROFIT_PCT": self.config_stock_auto_sell_profit_pct_var.get(),
                "CRYPTO_BTC_ETH_STOP_LOSS_PCT": self.config_btc_sl_var.get(),
                "CRYPTO_BTC_ETH_TP1_PCT": self.config_btc_tp1_var.get(),
                "CRYPTO_BTC_ETH_TP2_PCT": self.config_btc_tp2_var.get(),
                "CRYPTO_BTC_ETH_MAX_TP_PCT": self.config_btc_max_tp_var.get(),
                "CRYPTO_ALT_STOP_LOSS_PCT": self.config_alt_sl_var.get(),
                "CRYPTO_ALT_TP1_PCT": self.config_alt_tp1_var.get(),
                "CRYPTO_ALT_TP2_PCT": self.config_alt_tp2_var.get(),
                "CRYPTO_ALT_MAX_TP_PCT": self.config_alt_max_tp_var.get(),
                "CRYPTO_MEME_STOP_LOSS_PCT": self.config_meme_sl_var.get(),
                "CRYPTO_MEME_TP1_PCT": self.config_meme_tp1_var.get(),
                "CRYPTO_MEME_TP2_PCT": self.config_meme_tp2_var.get(),
                "CRYPTO_MEME_MAX_TP_PCT": self.config_meme_max_tp_var.get(),
                "AI_TARGET_PROFIT_PER_OPERATION": self.config_ai_target_profit_stocks_var.get(),
                "AI_TARGET_PROFIT_PER_OPERATION_STOCKS": self.config_ai_target_profit_stocks_var.get(),
                "AI_TARGET_PROFIT_PER_OPERATION_CRYPTOS": self.config_ai_target_profit_cryptos_var.get(),
                "CRYPTOPANIC_MONTHLY_LIMIT": self.config_cp_monthly_limit_var.get(),
                "CRYPTOPANIC_USED_THIS_MONTH": self.config_cp_used_baseline_var.get(),
                "CRYPTOPANIC_REQUEST_DAYS": self._request_days_to_text(),
            }

            # Validar que sean números
            numeric_keys = {
                "DEFAULT_TRADE_CAPITAL",
                "BOT_RISK_PER_TRADE_PCT",
                "BOT_MAX_DAILY_LOSS",
                "MAX_OPEN_POSITIONS",
                "STOCK_AUTO_SELL_PROFIT_PCT",
                "CRYPTO_BTC_ETH_STOP_LOSS_PCT",
                "CRYPTO_BTC_ETH_TP1_PCT",
                "CRYPTO_BTC_ETH_TP2_PCT",
                "CRYPTO_BTC_ETH_MAX_TP_PCT",
                "CRYPTO_ALT_STOP_LOSS_PCT",
                "CRYPTO_ALT_TP1_PCT",
                "CRYPTO_ALT_TP2_PCT",
                "CRYPTO_ALT_MAX_TP_PCT",
                "CRYPTO_MEME_STOP_LOSS_PCT",
                "CRYPTO_MEME_TP1_PCT",
                "CRYPTO_MEME_TP2_PCT",
                "CRYPTO_MEME_MAX_TP_PCT",
                "AI_TARGET_PROFIT_PER_OPERATION",
                "AI_TARGET_PROFIT_PER_OPERATION_STOCKS",
                "AI_TARGET_PROFIT_PER_OPERATION_CRYPTOS",
                "CRYPTOPANIC_MONTHLY_LIMIT",
                "CRYPTOPANIC_USED_THIS_MONTH",
            }
            for key, value in values.items():
                if key not in numeric_keys:
                    continue
                try:
                    float(value)
                except ValueError:
                    self.root.after(0, self._show_error, f"❌ {key} debe ser un número válido, recibido: {value}")
                    return

            # Leer .env actual
            env_path = Path(__file__).resolve().parents[1] / ".env"
            env_content = ""
            if env_path.exists():
                with open(env_path, "r") as f:
                    env_content = f.read()

            # Actualizar valores en el contenido
            for key, value in values.items():
                pattern = f"^{key}=.*$"
                if re.search(pattern, env_content, re.MULTILINE):
                    env_content = re.sub(pattern, f"{key}={value}", env_content, flags=re.MULTILINE)
                else:
                    env_content += f"\n{key}={value}"

            # Guardar
            with open(env_path, "w") as f:
                f.write(env_content)

            self.ai_trading_brain.update_cryptopanic_quota_settings(
                monthly_limit=int(float(values.get("CRYPTOPANIC_MONTHLY_LIMIT", "600") or 600)),
                used_baseline=int(float(values.get("CRYPTOPANIC_USED_THIS_MONTH", "0") or 0)),
                request_days=str(values.get("CRYPTOPANIC_REQUEST_DAYS", "mon,tue,wed,thu,fri")),
            )
            try:
                settings.max_open_positions = max(int(float(values.get("MAX_OPEN_POSITIONS", settings.max_open_positions) or settings.max_open_positions)), 1)
            except Exception:
                pass
            target_stock = float(values.get("AI_TARGET_PROFIT_PER_OPERATION_STOCKS", values.get("AI_TARGET_PROFIT_PER_OPERATION", "0.05")) or 0.05)
            target_crypto = float(values.get("AI_TARGET_PROFIT_PER_OPERATION_CRYPTOS", values.get("AI_TARGET_PROFIT_PER_OPERATION", "0.05")) or 0.05)
            self.ai_trading_brain.update_ai_target_profit_per_operation_by_asset(
                stock_value=target_stock,
                crypto_value=target_crypto,
            )
            self.ai_target_profit_var.set(str(target_stock))
            self.ai_target_profit_stocks_var.set(str(target_stock))
            self.ai_target_profit_cryptos_var.set(str(target_crypto))
            self._refresh_cryptopanic_quota_display()

            self.root.after(0, self._show_success, "✅ Configuración guardada correctamente en .env\n⚠️ Reinicia el bot para aplicar cambios")
            self._set_output("✅ Configuración guardada en .env. Reinicia bot para aplicar.", focus_general=False)

        except Exception as ex:
            self.logger.exception("Error guardando configuración: %s", ex)
            self.root.after(0, self._show_error, f"❌ Error guardando configuración: {ex}")

    def _build_ai_window(self) -> None:
        window = tk.Toplevel(self.root)
        window.title("IA")
        self._fit_window_to_screen(window, preferred_width=1220, preferred_height=920, min_width=920, min_height=680)

        header = ttk.Frame(window, padding=(12, 10))
        header.pack(fill="x")
        ttk.Label(header, text="IA - Estrategia y Entrenamiento", font=("TkDefaultFont", 12, "bold")).pack(side="left")
        ttk.Button(header, text="Refrescar IA", command=lambda: self._run_async(self._refresh_ai_views)).pack(side="right")

        header_board = ttk.Frame(window, padding=(12, 0, 12, 8))
        header_board.pack(fill="x")
        ttk.Label(header_board, textvariable=self.ai_header_stocks_var, foreground="#113a6b").pack(anchor="w")
        ttk.Label(header_board, textvariable=self.ai_header_cryptos_var, foreground="#2f5d1f").pack(anchor="w")

        badges_row = ttk.Frame(window, padding=(12, 0, 12, 8))
        badges_row.pack(fill="x")

        self.ai_badge_stocks_button = tk.Button(
            badges_row,
            textvariable=self.ai_badge_stocks_var,
            font=("TkDefaultFont", 11, "bold"),
            bg="#1f5f2a",
            fg="#ffffff",
            padx=10,
            pady=6,
            activebackground="#1f5f2a",
            activeforeground="#ffffff",
            relief="raised",
            bd=1,
            cursor="hand2",
            command=lambda: self._run_async(self._toggle_ai_stocks_execution),
        )
        self.ai_badge_stocks_button.pack(side="left", padx=(0, 8))

        self.ai_badge_cryptos_button = tk.Button(
            badges_row,
            textvariable=self.ai_badge_cryptos_var,
            font=("TkDefaultFont", 11, "bold"),
            bg="#1f5f2a",
            fg="#ffffff",
            padx=10,
            pady=6,
            activebackground="#1f5f2a",
            activeforeground="#ffffff",
            relief="raised",
            bd=1,
            cursor="hand2",
            command=lambda: self._run_async(self._toggle_ai_cryptos_execution),
        )
        self.ai_badge_cryptos_button.pack(side="left", padx=(0, 8))

        self.ai_badge_learning_button = tk.Button(
            badges_row,
            textvariable=self.ai_badge_learning_var,
            font=("TkDefaultFont", 11, "bold"),
            bg="#9a6700",
            fg="#ffffff",
            padx=10,
            pady=6,
            activebackground="#9a6700",
            activeforeground="#ffffff",
            relief="raised",
            bd=1,
            cursor="hand2",
            command=lambda: self._run_async(self._toggle_ai_learning_automation),
        )
        self.ai_badge_learning_button.pack(side="left")

        scroll_host = ttk.Frame(window)
        scroll_host.pack(fill="both", expand=True)
        scroll_canvas = tk.Canvas(scroll_host, highlightthickness=0)
        scroll_bar = ttk.Scrollbar(scroll_host, orient="vertical", command=scroll_canvas.yview)
        body = ttk.Frame(scroll_canvas, padding=(12, 0, 12, 12))
        body_window = scroll_canvas.create_window((0, 0), window=body, anchor="nw")

        def _on_ai_body_configure(_event: Any) -> None:
            scroll_canvas.configure(scrollregion=scroll_canvas.bbox("all"))

        def _on_ai_canvas_configure(event: Any) -> None:
            scroll_canvas.itemconfigure(body_window, width=event.width)

        body.bind("<Configure>", _on_ai_body_configure)
        scroll_canvas.bind("<Configure>", _on_ai_canvas_configure)
        scroll_canvas.configure(yscrollcommand=scroll_bar.set)

        window.bind("<MouseWheel>", lambda event: self._on_scoped_mousewheel(event, fallback_canvas=scroll_canvas), add="+")
        window.bind("<Button-4>", lambda event: self._on_scoped_mousewheel(event, fallback_canvas=scroll_canvas), add="+")
        window.bind("<Button-5>", lambda event: self._on_scoped_mousewheel(event, fallback_canvas=scroll_canvas), add="+")

        scroll_canvas.pack(side="left", fill="both", expand=True)
        scroll_bar.pack(side="right", fill="y")

        mode_row = ttk.LabelFrame(body, text="Control automático")
        mode_row.pack(fill="x", pady=(0, 8))
        ttk.Button(mode_row, text="Iniciar bot automático", command=lambda: self._run_async(self._start_ai_automation)).pack(
            side="left", padx=(8, 8), pady=6
        )
        ttk.Button(mode_row, text="Pausar bot", command=lambda: self._run_async(self._pause_ai_automation)).pack(
            side="left", padx=(0, 8), pady=6
        )
        ttk.Checkbutton(mode_row, text="Solo señales", variable=self.ai_signal_only_var).pack(side="left", padx=(0, 8))
        ttk.Checkbutton(mode_row, text="Paper trading", variable=self.ai_paper_trading_var).pack(side="left", padx=(0, 8))
        ttk.Checkbutton(mode_row, text="Live trading (bloqueado)", variable=self.ai_live_enabled_var, state="disabled").pack(side="left", padx=(0, 8))
        ttk.Checkbutton(mode_row, text="Auto Stocks", variable=self.ai_auto_trade_stocks_var).pack(side="left", padx=(0, 8))
        ttk.Checkbutton(mode_row, text="Auto Cryptos", variable=self.ai_auto_trade_cryptos_var).pack(side="left", padx=(0, 8))
        ttk.Label(mode_row, text="Decisiones IA:").pack(side="left", padx=(8, 4))
        ttk.Combobox(
            mode_row,
            textvariable=self.ai_decision_engine_var,
            values=["heuristic", "model"],
            state="readonly",
            width=10,
        ).pack(side="left", padx=(0, 8))
        ttk.Button(mode_row, text="Config Futuros", command=self._open_futures_risk_window).pack(side="left", padx=(0, 8))
        ttk.Label(mode_row, text="(Aquí cambias X, LONG y SHORT)", foreground="#666").pack(side="left", padx=(0, 8))
        ttk.Label(mode_row, textvariable=self.ai_pending_status_var, foreground="#1d5f2a").pack(side="left", padx=(4, 8))
        ttk.Button(mode_row, text="Guardar modo", command=lambda: self._run_async(self._save_ai_runtime_controls)).pack(side="right", padx=(8, 8))

        focus_row = ttk.LabelFrame(body, text="Enfoque de evaluación IA")
        focus_row.pack(fill="x", pady=(0, 8))
        ttk.Checkbutton(focus_row, text="Solo stocks seleccionados", variable=self.ai_focus_stocks_only_var).grid(
            row=0,
            column=0,
            sticky="w",
            padx=8,
            pady=6,
        )
        ttk.Label(focus_row, text="Stock:").grid(row=0, column=1, sticky="w", padx=(8, 4), pady=6)
        self.ai_focus_stock_combo = ttk.Combobox(
            focus_row,
            textvariable=self.ai_focus_stock_pick_var,
            values=self.ai_focus_stock_values,
            width=20,
            state="readonly",
        )
        self.ai_focus_stock_combo.grid(row=0, column=2, sticky="w", padx=4, pady=6)
        ttk.Button(focus_row, text="Agregar", command=self._add_focus_stock).grid(row=0, column=3, sticky="w", padx=4, pady=6)
        ttk.Button(focus_row, text="Quitar", command=self._remove_focus_stock).grid(row=0, column=4, sticky="w", padx=4, pady=6)
        ttk.Button(focus_row, text="Limpiar", command=self._clear_focus_stocks).grid(row=0, column=5, sticky="w", padx=4, pady=6)
        ttk.Button(focus_row, text="Actualizar lista", command=self._refresh_ai_focus_symbol_options_async).grid(row=0, column=6, sticky="w", padx=4, pady=6)
        ttk.Label(focus_row, text="Seleccionados:").grid(row=1, column=1, sticky="w", padx=(8, 4), pady=(0, 6))
        ttk.Label(focus_row, textvariable=self.ai_focus_stocks_selected_var, foreground="#1d5f2a").grid(row=1, column=2, columnspan=5, sticky="w", padx=4, pady=(0, 6))
        ttk.Checkbutton(focus_row, text="Solo cryptos seleccionadas", variable=self.ai_focus_cryptos_only_var).grid(
            row=2,
            column=0,
            sticky="w",
            padx=8,
            pady=6,
        )
        ttk.Label(focus_row, text="Crypto:").grid(row=2, column=1, sticky="w", padx=(8, 4), pady=6)
        self.ai_focus_crypto_combo = ttk.Combobox(
            focus_row,
            textvariable=self.ai_focus_crypto_pick_var,
            values=self.ai_focus_crypto_values,
            width=20,
            state="readonly",
        )
        self.ai_focus_crypto_combo.grid(row=2, column=2, sticky="w", padx=4, pady=6)
        ttk.Button(focus_row, text="Agregar", command=self._add_focus_crypto).grid(row=2, column=3, sticky="w", padx=4, pady=6)
        ttk.Button(focus_row, text="Quitar", command=self._remove_focus_crypto).grid(row=2, column=4, sticky="w", padx=4, pady=6)
        ttk.Button(focus_row, text="Limpiar", command=self._clear_focus_cryptos).grid(row=2, column=5, sticky="w", padx=4, pady=6)
        ttk.Label(focus_row, text="Seleccionados:").grid(row=3, column=1, sticky="w", padx=(8, 4), pady=(0, 6))
        ttk.Label(focus_row, textvariable=self.ai_focus_cryptos_selected_var, foreground="#1d5f2a").grid(row=3, column=2, columnspan=5, sticky="w", padx=4, pady=(0, 6))
        ttk.Button(
            focus_row,
            text="Guardar enfoque",
            command=lambda: self._run_async(self._save_ai_focus_controls),
        ).grid(row=0, column=7, rowspan=4, sticky="ns", padx=(10, 8), pady=6)
        ttk.Label(
            focus_row,
            text="Usa los menús desplegables y botones para elegir símbolos específicos (sin escribir manual).",
            foreground="#666",
        ).grid(row=4, column=0, columnspan=8, sticky="w", padx=8, pady=(0, 6))

        signals_body = ttk.LabelFrame(body, text="Top activos y señales IA")
        signals_body.pack(fill="both", expand=False, pady=(0, 8))
        self._build_ai_signals_controls(signals_body)

        crypto_monitor_body = ttk.LabelFrame(body, text="Monitoreo Crypto IA")
        crypto_monitor_body.pack(fill="both", expand=False, pady=(0, 8))
        crypto_toolbar = ttk.Frame(crypto_monitor_body)
        crypto_toolbar.pack(fill="x", pady=(0, 6))
        ttk.Label(
            crypto_toolbar,
            text="Vista rápida por crypto: señal IA, dirección (LONG/SHORT) y ajuste mínimo para habilitar entrada.",
        ).pack(side="left")
        ttk.Button(
            crypto_toolbar,
            text="Refrescar Monitoreo Crypto",
            command=self._refresh_ai_crypto_monitor_view_async,
        ).pack(side="right")
        monitor_height = 8 if self._ui_compact_mode else 12
        self.ai_crypto_monitor_text = tk.Text(crypto_monitor_body, height=monitor_height, wrap="word")
        self.ai_crypto_monitor_text.pack(fill="both", expand=True)
        self.ai_crypto_monitor_text.configure(state="disabled")

        self.ai_runtime_text = self._build_ai_section(body, "Estado del bot automático", 9)

        self.ai_model_text = self._build_ai_section(body, "Entrenamiento de modelo", 16)
        model_toolbar = ttk.Frame(self.ai_model_text.master)
        model_toolbar.pack(fill="x", pady=(6, 0))
        model_toolbar_row1 = ttk.Frame(model_toolbar)
        model_toolbar_row1.pack(fill="x", pady=(0, 4))
        model_toolbar_row2 = ttk.Frame(model_toolbar)
        model_toolbar_row2.pack(fill="x")
        ttk.Label(model_toolbar_row1, text="El entrenamiento del modelo es automático y continuo.").pack(side="left")
        self.ai_model_version_combo = ttk.Combobox(
            model_toolbar_row1,
            textvariable=self.ai_model_selected_var,
            values=self._ai_model_versions_values,
            width=36,
            state="readonly",
        )
        self.ai_model_version_combo.pack(side="left", padx=(8, 0))
        self.ai_model_version_combo.bind("<<ComboboxSelected>>", lambda _event: self._refresh_ai_model_view())
        ttk.Entry(model_toolbar_row1, textvariable=self.ai_model_alias_var, width=22).pack(side="left", padx=(8, 0))
        ttk.Button(model_toolbar_row1, text="Guardar nombre", command=lambda: self._run_async(self._save_ai_model_alias)).pack(side="left", padx=(8, 0))
        ttk.Button(model_toolbar_row1, text="Refrescar candidatos", command=lambda: self._run_async(self._refresh_ai_model_candidates)).pack(side="left", padx=(8, 0))
        self.ai_model_semaphore_label = tk.Label(
            model_toolbar_row1,
            textvariable=self.ai_model_reco_var,
            fg="white",
            bg="#6e7781",
            padx=10,
            pady=3,
            relief="raised",
            bd=1,
        )
        self.ai_model_semaphore_label.pack(side="right", padx=(8, 0))
        ttk.Button(model_toolbar_row2, text="Aprobar esta version", command=lambda: self._run_async(self._approve_ai_model_selected)).pack(side="left", padx=(0, 8))
        ttk.Button(model_toolbar_row2, text="Congelar candidato", command=lambda: self._run_async(self._freeze_ai_model_selected)).pack(side="left", padx=(0, 8))
        ttk.Button(model_toolbar_row2, text="Descongelar", command=lambda: self._run_async(self._unfreeze_ai_model_candidate)).pack(side="left", padx=(0, 8))
        ttk.Button(model_toolbar_row2, text="Eliminar version", command=lambda: self._run_async(self._delete_ai_model_selected)).pack(side="left", padx=(0, 8))
        ttk.Button(model_toolbar_row2, text="Aprobar modelo nuevo", command=lambda: self._run_async(self._approve_ai_model)).pack(side="left", padx=(0, 8))
        ttk.Button(model_toolbar_row2, text="Volver al anterior", command=lambda: self._run_async(self._rollback_ai_model)).pack(side="left")

        window.protocol("WM_DELETE_WINDOW", self._on_close_ai_window)
        self.ai_window = window
        self._load_focus_selected_from_vars()
        self._refresh_ai_focus_symbol_options_async()
        self.root.after(1500, self._ai_runtime_loop)

    def _show_ai_window(self) -> None:
        if self.ai_window is None or not self.ai_window.winfo_exists():
            self._build_ai_window()
        if self.ai_window is not None:
            self.ai_window.deiconify()
            self.ai_window.lift()
            self.ai_window.focus_force()

    def _on_close_ai_window(self) -> None:
        if self.ai_window is not None and self.ai_window.winfo_exists():
            self.ai_window.withdraw()

    def _show_ai_diagnostics_window(self) -> None:
        diag_account = self.ai_diag_account_var.get().strip()
        if (not diag_account) or (diag_account not in self.account_profiles):
            self.ai_diag_account_var.set(self.account_var.get().strip())
        if self.ai_diagnostics_window is not None and self.ai_diagnostics_window.winfo_exists():
            self.ai_diagnostics_window.deiconify()
            self.ai_diagnostics_window.lift()
            self.ai_diagnostics_window.focus_force()
            self._refresh_ai_diagnostics_async()
            return

        window = tk.Toplevel(self.root)
        window.title("Diagnóstico IA y Control de Riesgo")
        self._fit_window_to_screen(window, preferred_width=1240, preferred_height=900, min_width=920, min_height=680)

        header = ttk.Frame(window, padding=(12, 10))
        header.pack(fill="x")
        ttk.Label(header, text="Diagnóstico IA por símbolo", font=("TkDefaultFont", 12, "bold")).pack(side="left")
        ttk.Label(header, text=f"Cuenta UI activa: {self.account_var.get().strip()}", foreground="#444").pack(side="right")

        scroll_host = ttk.Frame(window)
        scroll_host.pack(fill="both", expand=True)
        scroll_canvas = tk.Canvas(scroll_host, highlightthickness=0)
        scroll_bar = ttk.Scrollbar(scroll_host, orient="vertical", command=scroll_canvas.yview)
        content = ttk.Frame(scroll_canvas, padding=(0, 0, 0, 8))
        content_window = scroll_canvas.create_window((0, 0), window=content, anchor="nw")

        def _on_diag_content_configure(_event: Any) -> None:
            scroll_canvas.configure(scrollregion=scroll_canvas.bbox("all"))

        def _on_diag_canvas_configure(event: Any) -> None:
            scroll_canvas.itemconfigure(content_window, width=event.width)

        content.bind("<Configure>", _on_diag_content_configure)
        scroll_canvas.bind("<Configure>", _on_diag_canvas_configure)
        scroll_canvas.configure(yscrollcommand=scroll_bar.set)

        window.bind("<MouseWheel>", lambda event: self._on_scoped_mousewheel(event, fallback_canvas=scroll_canvas), add="+")
        window.bind("<Button-4>", lambda event: self._on_scoped_mousewheel(event, fallback_canvas=scroll_canvas), add="+")
        window.bind("<Button-5>", lambda event: self._on_scoped_mousewheel(event, fallback_canvas=scroll_canvas), add="+")

        scroll_canvas.pack(side="left", fill="both", expand=True)
        scroll_bar.pack(side="right", fill="y")

        controls = ttk.LabelFrame(content, text="Entrada y acciones", padding=10)
        controls.pack(fill="x", padx=12, pady=(0, 8))
        ttk.Label(controls, text="Cuenta").grid(row=0, column=0, sticky="w", padx=4, pady=4)
        self.ai_diag_account_combo = ttk.Combobox(
            controls,
            textvariable=self.ai_diag_account_var,
            values=list(self.account_profiles.keys()),
            state="readonly",
            width=14,
        )
        self.ai_diag_account_combo.grid(row=0, column=1, sticky="w", padx=4, pady=4)
        self.ai_diag_account_combo.bind(
            "<<ComboboxSelected>>",
            lambda _event: self._on_ai_diag_account_selected(),
        )

        ttk.Label(controls, text="Mercado").grid(row=0, column=2, sticky="w", padx=4, pady=4)
        self.ai_diag_market_combo = ttk.Combobox(
            controls,
            textvariable=self.ai_diag_market_kind_var,
            values=["Stocks", "Cryptos"],
            state="readonly",
            width=12,
        )
        self.ai_diag_market_combo.grid(row=0, column=3, sticky="w", padx=4, pady=4)
        self.ai_diag_market_combo.bind(
            "<<ComboboxSelected>>",
            lambda _event: self._on_ai_diag_market_selected(),
        )

        ttk.Label(controls, text="Símbolo").grid(row=0, column=4, sticky="w", padx=4, pady=4)
        self.ai_diag_symbol_combo = ttk.Combobox(
            controls,
            textvariable=self.ai_diag_symbol_var,
            values=[],
            state="readonly",
            width=18,
        )
        self.ai_diag_symbol_combo.grid(row=0, column=5, sticky="w", padx=4, pady=4)
        self.ai_diag_symbol_combo.bind(
            "<<ComboboxSelected>>",
            lambda _event: self._on_ai_diag_symbol_selected(),
        )

        ttk.Button(controls, text="Actualizar activos", command=self._refresh_ai_diagnostic_symbols_async).grid(row=0, column=6, sticky="w", padx=8, pady=4)
        ttk.Button(controls, text="Refrescar diagnóstico", command=self._refresh_ai_diagnostics_async).grid(row=0, column=7, sticky="w", padx=8, pady=4)
        ttk.Button(controls, text="Guardar settings (.env)", command=lambda: self._run_async(self._save_ai_diagnostics_settings)).grid(row=0, column=8, sticky="w", padx=8, pady=4)
        ttk.Button(controls, text="Aplicar runtime IA", command=lambda: self._run_async(self._apply_ai_diagnostics_runtime_controls)).grid(row=0, column=9, sticky="w", padx=8, pady=4)
        ttk.Button(controls, text="Guardar + aplicar", command=lambda: self._run_async(self._save_and_apply_ai_diagnostics)).grid(row=0, column=10, sticky="w", padx=8, pady=4)

        settings_frame = ttk.LabelFrame(content, text="Settings IA editables", padding=10)
        settings_frame.pack(fill="x", padx=12, pady=(0, 8))
        ttk.Label(settings_frame, text="AI_MAX_SPREAD_ALLOWED").grid(row=0, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(settings_frame, textvariable=self.ai_diag_max_spread_var, width=12).grid(row=0, column=1, sticky="w", padx=4, pady=4)
        ttk.Label(settings_frame, text="AI_MIN_VOLUME_24H_USD").grid(row=0, column=2, sticky="w", padx=4, pady=4)
        ttk.Entry(settings_frame, textvariable=self.ai_diag_min_volume_var, width=14).grid(row=0, column=3, sticky="w", padx=4, pady=4)
        ttk.Label(settings_frame, text="AI_MIN_EXECUTION_CONFIDENCE").grid(row=0, column=4, sticky="w", padx=4, pady=4)
        ttk.Entry(settings_frame, textvariable=self.ai_diag_min_confidence_var, width=12).grid(row=0, column=5, sticky="w", padx=4, pady=4)
        ttk.Label(settings_frame, text="AI_TARGET_PROFIT_PER_OPERATION_STOCKS").grid(row=1, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(settings_frame, textvariable=self.ai_diag_target_profit_stocks_var, width=12).grid(row=1, column=1, sticky="w", padx=4, pady=4)
        ttk.Label(settings_frame, text="AI_TARGET_PROFIT_PER_OPERATION_CRYPTOS").grid(row=1, column=2, sticky="w", padx=4, pady=4)
        ttk.Entry(settings_frame, textvariable=self.ai_diag_target_profit_cryptos_var, width=12).grid(row=1, column=3, sticky="w", padx=4, pady=4)
        ttk.Label(settings_frame, text="AI_FEES_BUFFER").grid(row=1, column=4, sticky="w", padx=4, pady=4)
        ttk.Entry(settings_frame, textvariable=self.ai_diag_fees_var, width=12).grid(row=1, column=5, sticky="w", padx=4, pady=4)
        ttk.Label(settings_frame, text="AI_SLIPPAGE_BUFFER").grid(row=1, column=6, sticky="w", padx=4, pady=4)
        ttk.Entry(settings_frame, textvariable=self.ai_diag_slippage_var, width=12).grid(row=1, column=7, sticky="w", padx=4, pady=4)
        ttk.Label(settings_frame, text="AI_MINIMUM_PROFIT").grid(row=1, column=8, sticky="w", padx=4, pady=4)
        ttk.Entry(settings_frame, textvariable=self.ai_diag_min_profit_var, width=12).grid(row=1, column=9, sticky="w", padx=4, pady=4)
        ttk.Checkbutton(settings_frame, text="signal_only_mode", variable=self.ai_diag_signal_only_var).grid(row=2, column=0, sticky="w", padx=4, pady=4)
        ttk.Checkbutton(settings_frame, text="auto_trade_stocks_enabled", variable=self.ai_diag_auto_stocks_var).grid(row=2, column=1, sticky="w", padx=4, pady=4)
        ttk.Checkbutton(settings_frame, text="auto_trade_cryptos_enabled", variable=self.ai_diag_auto_cryptos_var).grid(row=2, column=2, sticky="w", padx=4, pady=4)
        ttk.Label(
            settings_frame,
            text="Nota: cambios en .env requieren reinicio para aplicarse globalmente.",
            foreground="#555",
        ).grid(row=3, column=0, columnspan=8, sticky="w", padx=4, pady=(2, 0))

        body = ttk.Frame(content, padding=(12, 0, 12, 12))
        body.pack(fill="both", expand=True)

        left = ttk.LabelFrame(body, text="Estado actual que ve la IA", padding=8)
        left.pack(side="left", fill="both", expand=True, padx=(0, 6))
        right = ttk.LabelFrame(body, text="Recomendaciones de settings y riesgo", padding=8)
        right.pack(side="left", fill="both", expand=True, padx=(6, 0))

        self.ai_diag_details_text = tk.Text(left, height=30, wrap="word")
        self.ai_diag_details_text.pack(fill="both", expand=True)
        self.ai_diag_details_text.configure(state="disabled")

        self.ai_diag_reco_text = tk.Text(right, height=30, wrap="word")
        self.ai_diag_reco_text.pack(fill="both", expand=True)
        self.ai_diag_reco_text.configure(state="disabled")

        window.protocol("WM_DELETE_WINDOW", lambda: window.withdraw())
        self.ai_diagnostics_window = window
        self._refresh_ai_diagnostic_symbols_async()
        self._refresh_ai_diagnostics_async()

    def _on_ai_diag_account_selected(self) -> None:
        self._save_ai_ui_state()
        self._refresh_ai_diagnostics_async()

    def _on_ai_diag_market_selected(self) -> None:
        self._save_ai_ui_state()
        self._refresh_ai_diagnostic_symbols_async()

    def _on_ai_diag_symbol_selected(self) -> None:
        self._save_ai_ui_state()
        self._refresh_ai_diagnostics_async()

    def _refresh_ai_diagnostics_async(self) -> None:
        threading.Thread(target=self._refresh_ai_diagnostics, daemon=True).start()

    def _refresh_ai_diagnostic_symbols_async(self) -> None:
        threading.Thread(target=self._refresh_ai_diagnostic_symbols, daemon=True).start()

    def _refresh_ai_diagnostic_symbols(self) -> None:
        selected_market = self.ai_diag_market_kind_var.get().strip() if hasattr(self, "ai_diag_market_kind_var") else "Stocks"
        try:
            if selected_market == "Cryptos":
                assets = self.broker.list_cryptos(status="active", only_tradable=True)
            else:
                assets = self.broker.list_stocks(status="active", only_tradable=True)
            symbols = sorted(
                {
                    str(asset.get("symbol", "")).upper()
                    for asset in assets
                    if str(asset.get("symbol", "")).strip()
                }
            )
        except Exception as ex:
            self.logger.warning("No se pudo actualizar activos en diagnostico IA: %s", ex)
            symbols = []

        if not symbols:
            current = self.ai_diag_symbol_var.get().strip().upper()
            if current:
                symbols = [current]

        self._safe_after(0, self._apply_ai_diagnostic_symbols, selected_market, symbols)

    def _apply_ai_diagnostic_symbols(self, selected_market: str, symbols: list[str]) -> None:
        self.ai_diag_asset_type_var.set("crypto" if selected_market == "Cryptos" else "stock")
        combo = getattr(self, "ai_diag_symbol_combo", None)
        if combo is None:
            return

        combo.configure(values=symbols)
        if not symbols:
            return

        current = self.ai_diag_symbol_var.get().strip().upper()
        if current in symbols:
            self.ai_diag_symbol_var.set(current)
            return
        self.ai_diag_symbol_var.set(symbols[0])

    def _refresh_ai_diagnostics(self) -> None:
        symbol = self.ai_diag_symbol_var.get().strip() if hasattr(self, "ai_diag_symbol_var") else ""
        if not symbol:
            self._safe_after(0, self._show_error, "Ingresa un símbolo para diagnóstico IA.", False)
            return
        account_name = self.ai_diag_account_var.get().strip() if hasattr(self, "ai_diag_account_var") else self.account_var.get().strip()
        if (not account_name) or (account_name not in self.account_profiles):
            account_name = self.account_var.get().strip()
            if hasattr(self, "ai_diag_account_var"):
                self.ai_diag_account_var.set(account_name)
        asset_type = "crypto" if self.ai_diag_market_kind_var.get().strip() == "Cryptos" else "stock"
        try:
            payload = self.ai_trading_brain.get_symbol_diagnostics(
                account_name=account_name,
                symbol=symbol,
                asset_type=asset_type,
            )
            self._safe_after(0, self._render_ai_diagnostics_payload, payload)
        except Exception as ex:
            self._safe_after(0, self._show_error, f"No se pudo obtener diagnóstico IA: {ex}", False)

    def _render_ai_diagnostics_payload(self, payload: dict[str, Any]) -> None:
        if self.ai_diagnostics_window is None or not self.ai_diagnostics_window.winfo_exists():
            return

        market = payload.get("market", {}) or {}
        runtime = payload.get("runtime", {}) or {}
        settings_payload = payload.get("settings", {}) or {}
        latest_signal = payload.get("latest_signal") or {}
        latest_decision = payload.get("latest_decision") or {}
        diagnostic_model_info = payload.get("diagnostic_model_info", {}) or {}
        checks = payload.get("entry_checks", []) or []
        blocked_reasons = payload.get("blocked_reasons", []) or []
        recommendations = payload.get("recommendations", []) or []
        entry_blockers = payload.get("entry_blockers", []) or []

        volume_15m_value = float(
            market.get(
                "binance_pair_volume_15m_base",
                market.get("alpaca_pair_volume_15m_base", market.get("alpaca_pair_volume_15m", 0.0)),
            )
            or 0.0
        )
        symbol_text = str(payload.get("symbol", "") or "").upper().strip()
        base_unit = symbol_text.split("/", 1)[0] if "/" in symbol_text else "BASE"
        volume_status_text = str(market.get("volume_data_status", "N/A") or "N/A").upper()
        market_title = "Mercado (actual):"
        if volume_status_text == "STALE":
            market_title = "Mercado: último dato disponible / STALE"

        def _fmt_recent_volume(value: Any, minutes_window: int) -> str:
            try:
                parsed = float(value or 0.0)
            except (TypeError, ValueError):
                parsed = 0.0
            if parsed > 0.0:
                return f"{parsed:.4f}"
            if minutes_window in {1, 5} and volume_15m_value > 0.0:
                return f"{parsed:.4f} (sin trades en {minutes_window}m; hubo {volume_15m_value:.4f} en 15m)"
            return f"{parsed:.4f} (sin trades en {minutes_window}m)"

        # Do not overwrite editable controls on refresh; keep operator manual input intact
        # until the user explicitly saves/applies settings.
        _ = runtime
        _ = settings_payload
        latest_decision_blocked_reason = ""
        if latest_decision:
            latest_decision_blocked_reason = str(latest_decision.get("blocked_reason", "") or "").strip()

        details_lines = [
            f"Cuenta: {payload.get('account_name', 'N/A')}",
            f"Símbolo: {payload.get('symbol', 'N/A')} | Tipo: {payload.get('asset_type', 'N/A')}",
            f"Dirección señal/operación: {payload.get('signal_direction', 'N/A')}",
            f"Puede entrar ahora: {'SI' if bool(payload.get('can_enter_now', False)) else 'NO'}",
            f"Razón exacta: {payload.get('entry_reason', 'N/A')}",
            "",
            market_title,
            f"- Fuente: {market.get('data_source', 'N/A')}",
            f"- Precio: {float(market.get('price', 0.0) or 0.0):.8f}",
            f"- Spread: {float(market.get('spread', 0.0) or 0.0):.8f}",
            f"- Spread %: {float(market.get('spread_pct', 0.0) or 0.0):.4f}%",
            f"- Volumen Binance 1m base: {_fmt_recent_volume(market.get('binance_pair_volume_1m_base', market.get('alpaca_pair_volume_1m_base', market.get('alpaca_pair_volume_1m', 0.0))), 1)} {base_unit}",
            f"- Volumen Binance 1m USD: {float(market.get('binance_pair_volume_1m_usd', market.get('alpaca_pair_volume_1m_usd', 0.0)) or 0.0):.4f}",
            f"- Volumen Binance 5m base: {_fmt_recent_volume(market.get('binance_pair_volume_5m_base', market.get('alpaca_pair_volume_5m_base', market.get('alpaca_pair_volume_5m', 0.0))), 5)} {base_unit}",
            f"- Volumen Binance 5m USD: {float(market.get('binance_pair_volume_5m_usd', market.get('alpaca_pair_volume_5m_usd', 0.0)) or 0.0):.4f}",
            f"- Volumen Binance 15m base: {_fmt_recent_volume(market.get('binance_pair_volume_15m_base', market.get('alpaca_pair_volume_15m_base', market.get('alpaca_pair_volume_15m', 0.0))), 15)} {base_unit}",
            f"- Volumen Binance 15m USD: {float(market.get('binance_pair_volume_15m_usd', market.get('alpaca_pair_volume_15m_usd', 0.0)) or 0.0):.4f}",
            f"- Minutos usados para 5m/15m: {int(market.get('volume_5m_minutes_used', 0) or 0)}/{int(market.get('volume_15m_minutes_used', 0) or 0)} (disponibles={int(market.get('volume_minutes_available', 0) or 0)})",
            f"- Volumen trades reales 1m base: {float(market.get('binance_trade_volume_1m_base', market.get('alpaca_trade_volume_1m_base', market.get('alpaca_trade_volume_1m', 0.0))) or 0.0):.4f} {base_unit}",
            f"- Volumen trades reales 1m USD: {float(market.get('binance_trade_volume_1m_usd', market.get('alpaca_trade_volume_1m_usd', 0.0)) or 0.0):.4f}",
            f"- Conteo trades reales 1m: {int(market.get('binance_trade_count_1m', market.get('alpaca_trade_count_1m', 0)) or 0)}",
            f"- Ventana trades reales: {int(market.get('binance_trade_window_seconds', market.get('alpaca_trade_window_seconds', 60)) or 60)}s",
            f"- Estado volumen por trades: {market.get('trade_volume_status', 'N/A')}",
            f"- Error volumen por trades: {market.get('trade_volume_error', '') or 'N/A'}",
            f"- Volumen 24h USD Binance: {float(market.get('binance_pair_volume_24h_usd', market.get('alpaca_pair_volume_24h_usd', 0.0)) or 0.0):.2f}",
            f"- Volumen 24h USD Global: {float(market.get('global_volume_24h_usd', 0.0) or 0.0):.2f}",
            f"- Fuente local Binance: {market.get('local_volume_source', market.get('volume_source', 'N/A'))}",
            f"- Fuente volumen global: {market.get('global_volume_source', 'CoinGecko')}",
            f"- Estado datos: {market.get('volume_data_status', 'N/A')}",
            f"- DATA_STALE: {bool(market.get('data_stale', False))}",
            f"- WebSocket stale: {bool(market.get('websocket_stale', False))}",
            f"- Latest bar age (s): {float(market.get('latest_bar_age_seconds', 0.0) or 0.0):.2f}",
            f"- Unidad volumen clara: {bool(market.get('volume_has_clear_unit', False))}",
            f"- volume_valid_for_live_analysis: {bool(market.get('volume_valid_for_live_analysis', False))}",
            f"- Validación volumen: {market.get('volume_validation_status', 'N/A')}",
            f"- Motivo validación: {market.get('volume_validation_reason', '') or 'N/A'}",
            f"- Fuente validación: {market.get('volume_validation_source', 'N/A')}",
            f"- Estado volumen global: {market.get('global_volume_status', 'N/A')}",
            f"- Warning volumen: {market.get('global_volume_warning', '') or 'N/A'}",
            f"- Snapshot ts: {market.get('snapshot_timestamp', 'N/A') or 'N/A'}",
            "",
            "Última señal IA:",
            f"- Tipo: {latest_signal.get('signal_type', 'SIN DATOS') if latest_signal else 'SIN DATOS'}",
            f"- Score/confianza: {float(latest_signal.get('confidence_score', 0.0) or 0.0):.2f}" if latest_signal else "- Score/confianza: SIN DATOS",
            f"- Modelo actual aprobado: {diagnostic_model_info.get('current_approved_model', 'heuristic')}"
            f"{' (archivo faltante)' if not bool(diagnostic_model_info.get('current_approved_model_available', True)) and str(diagnostic_model_info.get('current_approved_model', '')) not in {'', 'heuristic'} else ''}",
            f"- Modelo usado por última señal: {diagnostic_model_info.get('latest_signal_model_version', 'N/A')}",
            f"- Motor real usado: {diagnostic_model_info.get('latest_signal_engine', 'unknown')}",
            f"- Motor solicitado: {diagnostic_model_info.get('latest_signal_requested_engine', 'unknown')}",
            f"- Razón: {latest_signal.get('reason', '') or payload.get('latest_signal_text', 'Sin señal registrada para este símbolo')}" if latest_signal else f"- Razón: {payload.get('latest_signal_text', 'Sin señal registrada para este símbolo')}",
            "",
            "Última decisión IA:",
            f"- Decisión: {latest_decision.get('decision', 'SIN DATOS') if latest_decision else 'SIN DATOS'}",
            f"- Blocked reason: {latest_decision_blocked_reason or 'N/A'}" if latest_decision else f"- Blocked reason: {payload.get('latest_decision_text', 'Sin decisión registrada para este símbolo')}",
            f"- Razón: {latest_decision.get('reason', '') or payload.get('latest_decision_text', 'Sin decisión registrada para este símbolo')}" if latest_decision else f"- Razón: {payload.get('latest_decision_text', 'Sin decisión registrada para este símbolo')}",
            "",
            "Razones bloqueantes (exactas):",
        ]
        if entry_blockers:
            for blocker in entry_blockers:
                details_lines.append(f"- {blocker}")
        else:
            details_lines.append("- Sin bloqueos")
        details_lines.append("")
        details_lines.append("Motivos de bloqueo detectados:")
        if blocked_reasons:
            for reason in blocked_reasons:
                details_lines.append(f"- {reason}")
        else:
            details_lines.append("- Ninguno")

        reco_lines = ["Recomendaciones de settings (con riesgo):", ""]
        if not recommendations:
            reco_lines.append("- No hay recomendaciones automáticas para este símbolo en este momento.")
        else:
            for row in recommendations:
                reco_lines.extend(
                    [
                        f"- Setting: {row.get('setting', 'N/A')}",
                        f"  actual={row.get('current')} | sugerido={row.get('suggested')}",
                        f"  riesgo={row.get('risk', 'N/A')} | motivo={row.get('reason', 'N/A')}",
                        "",
                    ]
                )

        if self.ai_diag_details_text is not None:
            self._set_diagnostics_details_with_direction_color(
                details_lines=details_lines,
                direction=str(payload.get("signal_direction", "N/A") or "N/A"),
            )
        if self.ai_diag_reco_text is not None:
            self._set_text_widget(self.ai_diag_reco_text, "\n".join(reco_lines))

    def _save_ai_ui_state(self) -> None:
        existing: dict[str, Any] = {}
        if self._ai_ui_state_path.exists():
            try:
                raw_existing = json.loads(self._ai_ui_state_path.read_text(encoding="utf-8"))
                if isinstance(raw_existing, dict):
                    existing = raw_existing
            except Exception:
                existing = {}

        account_name = str(self.account_var.get() or "").strip()
        focus_by_account = existing.get("ai_focus_by_account", {})
        if not isinstance(focus_by_account, dict):
            focus_by_account = {}
        diag_by_account = existing.get("ai_diag_by_account", {})
        if not isinstance(diag_by_account, dict):
            diag_by_account = {}
        if account_name:
            focus_by_account[account_name] = {
                "stocks_only": bool(self.ai_focus_stocks_only_var.get()),
                "cryptos_only": bool(self.ai_focus_cryptos_only_var.get()),
                "stocks_symbols": str(self.ai_focus_stocks_symbols_var.get() or "").strip(),
                "cryptos_symbols": str(self.ai_focus_cryptos_symbols_var.get() or "").strip(),
            }
            diag_by_account[account_name] = {
                "signal_only": bool(self.ai_diag_signal_only_var.get()),
                "auto_stocks": bool(self.ai_diag_auto_stocks_var.get()),
                "auto_cryptos": bool(self.ai_diag_auto_cryptos_var.get()),
            }

        payload = {
            "account": account_name,
            "ai_automation_account": str(getattr(self, "_ai_automation_account", "") or account_name).strip(),
            "ai_focus": {
                "stocks_only": bool(self.ai_focus_stocks_only_var.get()),
                "cryptos_only": bool(self.ai_focus_cryptos_only_var.get()),
                "stocks_symbols": str(self.ai_focus_stocks_symbols_var.get() or "").strip(),
                "cryptos_symbols": str(self.ai_focus_cryptos_symbols_var.get() or "").strip(),
            },
            "ai_focus_by_account": focus_by_account,
            "ai_diag_by_account": diag_by_account,
            "ai_diag": {
                "account": str(self.ai_diag_account_var.get() or "").strip(),
                "market": str(self.ai_diag_market_kind_var.get() or "").strip(),
                "symbol": str(self.ai_diag_symbol_var.get() or "").strip().upper(),
                "max_spread": str(self.ai_diag_max_spread_var.get() or "").strip(),
                "min_volume": str(self.ai_diag_min_volume_var.get() or "").strip(),
                "min_confidence": str(self.ai_diag_min_confidence_var.get() or "").strip(),
                "target_stocks": str(self.ai_diag_target_profit_stocks_var.get() or "").strip(),
                "target_cryptos": str(self.ai_diag_target_profit_cryptos_var.get() or "").strip(),
                "fees": str(self.ai_diag_fees_var.get() or "").strip(),
                "slippage": str(self.ai_diag_slippage_var.get() or "").strip(),
                "minimum_profit": str(self.ai_diag_min_profit_var.get() or "").strip(),
                "signal_only": bool(self.ai_diag_signal_only_var.get()),
                "auto_stocks": bool(self.ai_diag_auto_stocks_var.get()),
                "auto_cryptos": bool(self.ai_diag_auto_cryptos_var.get()),
            },
        }
        self._ai_ui_state_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    def _load_ai_ui_state(self) -> None:
        if not self._ai_ui_state_path.exists():
            return
        try:
            raw = json.loads(self._ai_ui_state_path.read_text(encoding="utf-8"))
        except Exception:
            return
        if not isinstance(raw, dict):
            return

        self._ai_ui_state_cache = raw

        try:
            selected_account = str(raw.get("account", "") or "").strip()
            if selected_account and selected_account in self.account_profiles:
                self.account_var.set(selected_account)
            automation_account = str(raw.get("ai_automation_account", "") or "").strip()
            if automation_account and automation_account in self.account_profiles:
                self._ai_automation_account = automation_account

            focus_by_account = raw.get("ai_focus_by_account", {}) if isinstance(raw.get("ai_focus_by_account", {}), dict) else {}
            focus = focus_by_account.get(selected_account, {}) if selected_account and isinstance(focus_by_account.get(selected_account, {}), dict) else {}
            if not focus:
                focus = raw.get("ai_focus", {}) if isinstance(raw.get("ai_focus", {}), dict) else {}
            self.ai_focus_stocks_only_var.set(1 if bool(focus.get("stocks_only", self.ai_focus_stocks_only_var.get())) else 0)
            self.ai_focus_cryptos_only_var.set(1 if bool(focus.get("cryptos_only", self.ai_focus_cryptos_only_var.get())) else 0)
            self.ai_focus_stocks_symbols_var.set(str(focus.get("stocks_symbols", self.ai_focus_stocks_symbols_var.get()) or ""))
            self.ai_focus_cryptos_symbols_var.set(str(focus.get("cryptos_symbols", self.ai_focus_cryptos_symbols_var.get()) or ""))
            self.ai_focus_stock_selected = self._parse_symbols_csv(self.ai_focus_stocks_symbols_var.get())
            self.ai_focus_crypto_selected = self._parse_symbols_csv(self.ai_focus_cryptos_symbols_var.get())
            self._sync_focus_symbol_vars_from_selected()

            diag = raw.get("ai_diag", {}) if isinstance(raw.get("ai_diag", {}), dict) else {}
            diag_by_account = raw.get("ai_diag_by_account", {}) if isinstance(raw.get("ai_diag_by_account", {}), dict) else {}
            selected_diag = diag_by_account.get(selected_account, {}) if selected_account and isinstance(diag_by_account.get(selected_account, {}), dict) else {}
            diag_account = str(diag.get("account", self.ai_diag_account_var.get()) or self.ai_diag_account_var.get()).strip()
            if diag_account and diag_account in self.account_profiles:
                self.ai_diag_account_var.set(diag_account)
            else:
                self.ai_diag_account_var.set(selected_account or self.account_var.get().strip())
            self.ai_diag_market_kind_var.set(str(diag.get("market", self.ai_diag_market_kind_var.get()) or self.ai_diag_market_kind_var.get()))
            self.ai_diag_symbol_var.set(str(diag.get("symbol", self.ai_diag_symbol_var.get()) or self.ai_diag_symbol_var.get()).upper())
            self.ai_diag_max_spread_var.set(str(diag.get("max_spread", self.ai_diag_max_spread_var.get()) or self.ai_diag_max_spread_var.get()))
            self.ai_diag_min_volume_var.set(str(diag.get("min_volume", self.ai_diag_min_volume_var.get()) or self.ai_diag_min_volume_var.get()))
            self.ai_diag_min_confidence_var.set(str(diag.get("min_confidence", self.ai_diag_min_confidence_var.get()) or self.ai_diag_min_confidence_var.get()))
            self.ai_diag_target_profit_stocks_var.set(str(diag.get("target_stocks", self.ai_diag_target_profit_stocks_var.get()) or self.ai_diag_target_profit_stocks_var.get()))
            self.ai_diag_target_profit_cryptos_var.set(str(diag.get("target_cryptos", self.ai_diag_target_profit_cryptos_var.get()) or self.ai_diag_target_profit_cryptos_var.get()))
            self.ai_diag_fees_var.set(str(diag.get("fees", self.ai_diag_fees_var.get()) or self.ai_diag_fees_var.get()))
            self.ai_diag_slippage_var.set(str(diag.get("slippage", self.ai_diag_slippage_var.get()) or self.ai_diag_slippage_var.get()))
            self.ai_diag_min_profit_var.set(str(diag.get("minimum_profit", self.ai_diag_min_profit_var.get()) or self.ai_diag_min_profit_var.get()))
            self.ai_diag_signal_only_var.set(1 if bool(selected_diag.get("signal_only", diag.get("signal_only", self.ai_diag_signal_only_var.get()))) else 0)
            self.ai_diag_auto_stocks_var.set(1 if bool(selected_diag.get("auto_stocks", diag.get("auto_stocks", self.ai_diag_auto_stocks_var.get()))) else 0)
            self.ai_diag_auto_cryptos_var.set(1 if bool(selected_diag.get("auto_cryptos", diag.get("auto_cryptos", self.ai_diag_auto_cryptos_var.get()))) else 0)
        except Exception:
            return

    def _write_env_values(self, values: dict[str, str]) -> None:
        import re

        env_path = Path(__file__).resolve().parents[1] / ".env"
        env_content = env_path.read_text(encoding="utf-8") if env_path.exists() else ""
        for key, value in values.items():
            pattern = rf"^{re.escape(key)}=.*$"
            new_line = f"{key}={value}"
            if re.search(pattern, env_content, re.MULTILINE):
                env_content = re.sub(pattern, new_line, env_content, flags=re.MULTILINE)
            else:
                if env_content and not env_content.endswith("\n"):
                    env_content += "\n"
                env_content += new_line
        env_path.write_text(env_content, encoding="utf-8")

    def _save_ai_diagnostics_settings(self) -> None:
        def _safe_assign(obj: Any, name: str, value: Any) -> None:
            try:
                setattr(obj, name, value)
                return
            except Exception:
                pass
            try:
                object.__setattr__(obj, name, value)
            except Exception:
                pass

        values = {
            "AI_MAX_SPREAD_ALLOWED": self.ai_diag_max_spread_var.get().strip(),
            "AI_MIN_VOLUME_24H_USD": self.ai_diag_min_volume_var.get().strip(),
            "AI_MIN_EXECUTION_CONFIDENCE": self.ai_diag_min_confidence_var.get().strip(),
            "AI_TARGET_PROFIT_PER_OPERATION": self.ai_diag_target_profit_stocks_var.get().strip(),
            "AI_TARGET_PROFIT_PER_OPERATION_STOCKS": self.ai_diag_target_profit_stocks_var.get().strip(),
            "AI_TARGET_PROFIT_PER_OPERATION_CRYPTOS": self.ai_diag_target_profit_cryptos_var.get().strip(),
            "AI_FEES_BUFFER": self.ai_diag_fees_var.get().strip(),
            "AI_SLIPPAGE_BUFFER": self.ai_diag_slippage_var.get().strip(),
            "AI_MINIMUM_PROFIT": self.ai_diag_min_profit_var.get().strip(),
            "AI_SIGNAL_ONLY_MODE": "true" if bool(self.ai_diag_signal_only_var.get()) else "false",
        }

        numeric_keys = {
            "AI_MAX_SPREAD_ALLOWED",
            "AI_MIN_VOLUME_24H_USD",
            "AI_MIN_EXECUTION_CONFIDENCE",
            "AI_TARGET_PROFIT_PER_OPERATION",
            "AI_TARGET_PROFIT_PER_OPERATION_STOCKS",
            "AI_TARGET_PROFIT_PER_OPERATION_CRYPTOS",
            "AI_FEES_BUFFER",
            "AI_SLIPPAGE_BUFFER",
            "AI_MINIMUM_PROFIT",
        }
        for key in numeric_keys:
            try:
                float(values[key])
            except ValueError as ex:
                raise ValueError(f"{key} debe ser numérico") from ex

        numeric_values = {key: float(values[key]) for key in numeric_keys}

        self._write_env_values(values)

        # Keep runtime diagnostics aligned with the just-saved values without requiring app restart.
        _safe_assign(settings, "ai_max_spread_allowed", numeric_values["AI_MAX_SPREAD_ALLOWED"])
        _safe_assign(settings, "ai_min_volume_24h_usd", numeric_values["AI_MIN_VOLUME_24H_USD"])
        _safe_assign(settings, "ai_min_execution_confidence", numeric_values["AI_MIN_EXECUTION_CONFIDENCE"])
        _safe_assign(settings, "ai_target_profit_per_operation", numeric_values["AI_TARGET_PROFIT_PER_OPERATION"])
        _safe_assign(settings, "ai_target_profit_per_operation_stocks", numeric_values["AI_TARGET_PROFIT_PER_OPERATION_STOCKS"])
        _safe_assign(settings, "ai_target_profit_per_operation_cryptos", numeric_values["AI_TARGET_PROFIT_PER_OPERATION_CRYPTOS"])
        _safe_assign(settings, "ai_fees_buffer", numeric_values["AI_FEES_BUFFER"])
        _safe_assign(settings, "ai_slippage_buffer", numeric_values["AI_SLIPPAGE_BUFFER"])
        _safe_assign(settings, "ai_minimum_profit", numeric_values["AI_MINIMUM_PROFIT"])
        _safe_assign(settings, "ai_signal_only_mode", bool(self.ai_diag_signal_only_var.get()))

        brain_settings = getattr(self.ai_trading_brain, "settings", None)
        if brain_settings is not None and brain_settings is not settings:
            _safe_assign(brain_settings, "ai_max_spread_allowed", numeric_values["AI_MAX_SPREAD_ALLOWED"])
            _safe_assign(brain_settings, "ai_min_volume_24h_usd", numeric_values["AI_MIN_VOLUME_24H_USD"])
            _safe_assign(brain_settings, "ai_min_execution_confidence", numeric_values["AI_MIN_EXECUTION_CONFIDENCE"])
            _safe_assign(brain_settings, "ai_target_profit_per_operation", numeric_values["AI_TARGET_PROFIT_PER_OPERATION"])
            _safe_assign(brain_settings, "ai_target_profit_per_operation_stocks", numeric_values["AI_TARGET_PROFIT_PER_OPERATION_STOCKS"])
            _safe_assign(brain_settings, "ai_target_profit_per_operation_cryptos", numeric_values["AI_TARGET_PROFIT_PER_OPERATION_CRYPTOS"])
            _safe_assign(brain_settings, "ai_fees_buffer", numeric_values["AI_FEES_BUFFER"])
            _safe_assign(brain_settings, "ai_slippage_buffer", numeric_values["AI_SLIPPAGE_BUFFER"])
            _safe_assign(brain_settings, "ai_minimum_profit", numeric_values["AI_MINIMUM_PROFIT"])
            _safe_assign(brain_settings, "ai_signal_only_mode", bool(self.ai_diag_signal_only_var.get()))

        update_spread_fn = getattr(self.ai_trading_brain, "update_ai_max_spread_allowed", None)
        if callable(update_spread_fn):
            update_spread_fn(numeric_values["AI_MAX_SPREAD_ALLOWED"])

        target_stock = float(values["AI_TARGET_PROFIT_PER_OPERATION_STOCKS"] or values["AI_TARGET_PROFIT_PER_OPERATION"] or 0.05)
        target_crypto = float(values["AI_TARGET_PROFIT_PER_OPERATION_CRYPTOS"] or values["AI_TARGET_PROFIT_PER_OPERATION"] or 0.05)
        self.ai_trading_brain.update_ai_target_profit_per_operation_by_asset(
            stock_value=target_stock,
            crypto_value=target_crypto,
        )
        self.ai_target_profit_var.set(str(target_stock))
        self.ai_target_profit_stocks_var.set(str(target_stock))
        self.ai_target_profit_cryptos_var.set(str(target_crypto))
        self._save_ai_ui_state()
        changes_text = (
            "Settings IA aplicados:\n"
            f"- AI_MAX_SPREAD_ALLOWED={numeric_values['AI_MAX_SPREAD_ALLOWED']}\n"
            f"- AI_MIN_VOLUME_24H_USD={numeric_values['AI_MIN_VOLUME_24H_USD']}\n"
            f"- AI_MIN_EXECUTION_CONFIDENCE={numeric_values['AI_MIN_EXECUTION_CONFIDENCE']}\n"
            f"- AI_TARGET_PROFIT_PER_OPERATION_STOCKS={target_stock}\n"
            f"- AI_TARGET_PROFIT_PER_OPERATION_CRYPTOS={target_crypto}\n"
            f"- AI_FEES_BUFFER={numeric_values['AI_FEES_BUFFER']}\n"
            f"- AI_SLIPPAGE_BUFFER={numeric_values['AI_SLIPPAGE_BUFFER']}\n"
            f"- AI_MINIMUM_PROFIT={numeric_values['AI_MINIMUM_PROFIT']}\n"
            f"- AI_SIGNAL_ONLY_MODE={'true' if bool(self.ai_diag_signal_only_var.get()) else 'false'}"
        )
        self._show_success(changes_text, False)

    def _apply_ai_diagnostics_runtime_controls(self) -> None:
        self.ai_signal_only_var.set(1 if bool(self.ai_diag_signal_only_var.get()) else 0)
        self.ai_auto_trade_stocks_var.set(1 if bool(self.ai_diag_auto_stocks_var.get()) else 0)
        self.ai_auto_trade_cryptos_var.set(1 if bool(self.ai_diag_auto_cryptos_var.get()) else 0)
        self.ai_target_profit_var.set(self.ai_diag_target_profit_stocks_var.get().strip())
        self.ai_target_profit_stocks_var.set(self.ai_diag_target_profit_stocks_var.get().strip())
        self.ai_target_profit_cryptos_var.set(self.ai_diag_target_profit_cryptos_var.get().strip())
        self._save_ai_ui_state()
        self._persist_ai_runtime_controls(show_message=False)
        self._show_success("Controles runtime IA aplicados a la cuenta activa.", False)

    def _save_and_apply_ai_diagnostics(self) -> None:
        self._save_ai_diagnostics_settings()
        self._apply_ai_diagnostics_runtime_controls()
        self._refresh_ai_diagnostics_async()

    def _ai_runtime_loop(self) -> None:
        if self.ai_window is not None and self.ai_window.winfo_exists() and str(self.ai_window.state()) != "withdrawn":
            account_name = self._ai_automation_account_name()
            if account_name:
                threading.Thread(target=self._ensure_ai_automation_running, args=(account_name,), daemon=True).start()
                self._sync_ai_watch_tabs_from_recent_trades_async()
            self._refresh_ai_runtime_view_async()
        self.root.after(5000, self._ai_runtime_loop)

    def _refresh_ai_runtime_view_async(self) -> None:
        if self._ai_runtime_refresh_in_flight:
            return
        self._ai_runtime_refresh_in_flight = True

        def worker() -> None:
            status: dict[str, Any] | None = None
            try:
                status = self.ai_trading_brain.get_automation_status(self._ai_automation_account_name())
            except Exception as ex:
                if self._should_emit_transient_log("ai-runtime-refresh", min_interval_seconds=30.0):
                    self.logger.warning("No se pudo refrescar runtime IA: %s", ex)
            finally:
                self._ai_runtime_refresh_in_flight = False

            if status is not None:
                self._safe_after(0, self._refresh_ai_runtime_view, status)

        threading.Thread(target=worker, daemon=True).start()

    def _ai_automation_account_name(self) -> str:
        account_name = str(getattr(self, "_ai_automation_account", "") or "").strip()
        if account_name:
            return account_name
        return self.account_var.get().strip()

    @staticmethod
    def _is_ai_trade_origin(initiated_by: str) -> bool:
        origin = str(initiated_by or "").strip().lower()
        return origin in {"bot_auto", "ai_auto", "automation"}

    def _sync_ai_watch_tabs_from_recent_trades_async(self) -> None:
        now = time.monotonic()
        if self._ai_watch_sync_in_flight:
            return
        if (now - float(self._ai_watch_last_sync_ts or 0.0)) < 8.0:
            return
        self._ai_watch_sync_in_flight = True

        def worker() -> None:
            candidates: list[dict[str, Any]] = []
            try:
                account_names = list((self.account_profiles or {}).keys())
                if not account_names:
                    current = self.account_var.get().strip()
                    if current:
                        account_names = [current]

                terminal_statuses = {"canceled", "rejected", "expired"}
                for account_name in account_names:
                    if not account_name:
                        continue
                    try:
                        rows = self.ai_trading_brain.list_history(account_name, limit=30)
                    except Exception:
                        continue
                    for row in rows:
                        side = str(row.get("side", "") or "").strip().lower()
                        if side != "buy":
                            continue
                        initiated_by = str(row.get("initiated_by", "") or "").strip().lower()
                        if not self._is_ai_trade_origin(initiated_by):
                            continue
                        status = str(row.get("status", "") or "").strip().lower()
                        if status in terminal_statuses:
                            continue
                        symbol = str(row.get("symbol", "") or "").strip().upper()
                        if not symbol:
                            continue
                        asset_type = str(row.get("asset_type", "") or "").strip().lower()
                        if asset_type not in {"stock", "crypto"}:
                            asset_type = self._asset_type_for_symbol(symbol)
                        candidates.append(
                            {
                                "account": account_name,
                                "symbol": symbol,
                                "asset_type": asset_type,
                                "status": status,
                            }
                        )
                self._safe_after(0, self._apply_ai_trade_watch_candidates, candidates)
            finally:
                self._ai_watch_last_sync_ts = time.monotonic()
                self._ai_watch_sync_in_flight = False

        threading.Thread(target=worker, daemon=True).start()

    def _apply_ai_trade_watch_candidates(self, candidates: list[dict[str, Any]]) -> None:
        if self._is_closing:
            return
        unique_keys: set[tuple[str, str]] = set()
        for item in candidates:
            symbol = str(item.get("symbol", "") or "").strip().upper()
            account = str(item.get("account", "") or "").strip()
            asset_type = str(item.get("asset_type", "stock") or "stock").strip().lower()
            if not symbol or not account:
                continue
            unique_key = (self._symbol_key(symbol), account)
            if unique_key in unique_keys:
                continue
            unique_keys.add(unique_key)

            if self._watch_tab_exists_for_symbol(symbol=symbol, account=account, bot_actor="ia"):
                continue

            try:
                runtime = self._get_account_runtime(account)
                broker = runtime.get("broker")
                position_manager = runtime.get("position_manager")
                has_position = self._find_open_position_by_symbol(symbol, broker=broker) is not None
                has_pending_buy = bool(position_manager._has_pending_buy_order(symbol)) if position_manager is not None else False
            except Exception:
                has_position = False
                has_pending_buy = False

            if not has_position and not has_pending_buy:
                continue

            watch_id = self._create_watch_tab(
                symbol=symbol,
                asset_type=asset_type,
                start_mode="tracking",
                account_name=account,
                bot_actor="ia",
            )
            self._watch_log(
                watch_id,
                f"Pestaña IA creada automáticamente para {symbol}. Estado broker: {'posicion abierta' if has_position else 'orden BUY pendiente'}.",
            )
            self._start_position_tracking(watch_id=watch_id, symbol=symbol, entry_price_hint=0.0)

    def _ensure_ai_automation_running(self, account_name: str) -> None:
        if not account_name:
            return
        try:
            security = self.ai_trading_brain.get_security_state(account_name)
            self._safe_after(0, self._apply_vpn_security_state, security)
            if bool(security.get("vpn_required", False)) and not bool(security.get("vpn_ready", False)):
                return
            self.ai_trading_brain.ensure_automation_running(account_name)
        except Exception as ex:
            self.logger.warning("No se pudo iniciar la automatizacion IA automaticamente: %s", ex)

    def _start_vpn_bootstrap_async(self) -> None:
        if self._vpn_bootstrap_in_flight or self._is_closing:
            return
        self._vpn_bootstrap_in_flight = True
        self.vpn_badge_var.set("VPN: esperando conexión...")

        def worker() -> None:
            try:
                provider = str(getattr(settings, "required_vpn_provider", "WireGuard") or "WireGuard").strip().lower()
                if provider in {"wireguard", "proton wireguard", "protonvpn wireguard"}:
                    conn_name = str(getattr(settings, "required_vpn_connection_name", "") or "").strip()
                    if conn_name:
                        nmcli_bin = shutil.which("nmcli")
                        if nmcli_bin:
                            up_proc = subprocess.run(
                                [nmcli_bin, "connection", "up", conn_name],
                                check=False,
                                capture_output=True,
                                text=True,
                                timeout=20,
                            )
                            if up_proc.returncode != 0:
                                up_proc = subprocess.run(
                                    ["sudo", "-n", nmcli_bin, "connection", "up", conn_name],
                                    check=False,
                                    capture_output=True,
                                    text=True,
                                    timeout=20,
                                )
                            if up_proc.returncode != 0:
                                err = (up_proc.stderr or up_proc.stdout or "").strip()
                                self.logger.warning("WireGuard bootstrap failed rc=%s: %s", up_proc.returncode, err)
                        else:
                            self.logger.warning("No se encontro nmcli para activar WireGuard.")

                    account_name = self._ai_automation_account_name()
                    if account_name:
                        security = self.ai_trading_brain.get_security_state(account_name)
                        self._safe_after(0, self._apply_vpn_security_state, security)
                    return

                account_name = self._ai_automation_account_name()
                if account_name:
                    security = self.ai_trading_brain.get_security_state(account_name)
                    self._safe_after(0, self._apply_vpn_security_state, security)
            finally:
                self._vpn_bootstrap_in_flight = False

        threading.Thread(target=worker, daemon=True).start()

    def _attempt_vpn_reconnect_async(self) -> None:
        if self._vpn_bootstrap_in_flight or self._is_closing:
            return
        now_mono = time.monotonic()
        if (now_mono - float(self._vpn_last_connect_attempt_ts or 0.0)) < 20.0:
            return
        self._vpn_last_connect_attempt_ts = now_mono
        self._start_vpn_bootstrap_async()

    def _vpn_guard_loop(self) -> None:
        if self._is_closing:
            return
        if self._vpn_guard_in_flight:
            self.root.after(5000, self._vpn_guard_loop)
            return
        self._vpn_guard_in_flight = True

        def worker() -> None:
            try:
                account_name = self._ai_automation_account_name()
                if not account_name:
                    return
                security = self.ai_trading_brain.get_security_state(account_name)
                self._safe_after(0, self._apply_vpn_security_state, security)
                if bool(security.get("vpn_required", False)) and not bool(security.get("vpn_ready", False)):
                    self._safe_after(0, self._attempt_vpn_reconnect_async)
            except Exception as ex:
                self.logger.warning("VPN guard check failed: %s", ex)
            finally:
                self._vpn_guard_in_flight = False

        threading.Thread(target=worker, daemon=True).start()
        self.root.after(5000, self._vpn_guard_loop)

    def _apply_vpn_security_state(self, security: dict[str, Any]) -> None:
        vpn_required = bool(security.get("vpn_required", False))
        vpn_ready = bool(security.get("vpn_ready", False))
        vpn_reason = str(security.get("vpn_reason", "") or "")
        vpn_provider = str(security.get("vpn_provider", "") or "").strip().lower()
        was_ready = self._vpn_last_ready_state
        self._vpn_last_ready_state = vpn_ready

        if not vpn_required:
            self.vpn_badge_var.set("VPN: no requerida")
            self.vpn_badge_label.configure(bg="#1f5f2a")
            return

        if vpn_ready:
            self._vpn_ever_ready = True
            country = str(security.get("vpn_country", "") or "").strip()
            provider_label = str(security.get("vpn_provider", "VPN") or "VPN").strip()
            self.vpn_badge_var.set(f"VPN CONECTADA: {country or provider_label}")
            self.vpn_badge_label.configure(bg="#1f5f2a")
            if vpn_provider == "protonvpn":
                self._ensure_tailscale_down_async()
            transitioned_to_ready = (was_ready is not True)
            if self._vpn_forced_pause_active:
                account_name = self._ai_automation_account_name()
                if account_name:
                    threading.Thread(target=self._ensure_ai_automation_running, args=(account_name,), daemon=True).start()
                self._vpn_forced_pause_active = False
            elif transitioned_to_ready:
                account_name = self._ai_automation_account_name()
                if account_name:
                    threading.Thread(target=self._ensure_ai_automation_running, args=(account_name,), daemon=True).start()
            return

        if self._vpn_ever_ready:
            self.vpn_badge_var.set("VPN CAIDA")
            self.vpn_badge_label.configure(bg="#8a1c1c")
        else:
            self.vpn_badge_var.set("ESPERANDO CONEXION VPN")
            self.vpn_badge_label.configure(bg="#9a6700")

        if vpn_reason:
            self.status_var.set(vpn_reason)

        if not self._vpn_forced_pause_active:
            try:
                self.ai_trading_brain.pause_automation()
                self._vpn_forced_pause_active = True
            except Exception:
                pass

    def _ensure_tailscale_down_async(self) -> None:
        if self._is_closing or self._tailscale_disconnect_in_flight:
            return
        now_mono = time.monotonic()
        if (now_mono - float(self._tailscale_last_disconnect_attempt_ts or 0.0)) < 30.0:
            return
        self._tailscale_last_disconnect_attempt_ts = now_mono
        self._tailscale_disconnect_in_flight = True

        def worker() -> None:
            try:
                tailscale_bin = shutil.which("tailscale")
                if not tailscale_bin:
                    return

                status_proc = subprocess.run(
                    [tailscale_bin, "status", "--json"],
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=6,
                )
                if status_proc.returncode != 0:
                    return

                backend_state = ""
                try:
                    payload = json.loads(str(status_proc.stdout or "{}"))
                    backend_state = str(payload.get("BackendState", "") or "").strip().lower()
                except Exception:
                    backend_state = ""

                if backend_state not in {"running", "starting"}:
                    return

                down_proc = subprocess.run(
                    [tailscale_bin, "down"],
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                if down_proc.returncode == 0:
                    self.logger.info("Tailscale desactivado automaticamente porque ProtonVPN esta conectado.")
                    return

                err = str((down_proc.stderr or down_proc.stdout or "")).strip()
                if err:
                    self.logger.warning("No se pudo desactivar Tailscale automaticamente: %s", err)
                else:
                    self.logger.warning("No se pudo desactivar Tailscale automaticamente (rc=%s).", down_proc.returncode)
            except Exception as ex:
                self.logger.warning("Error al intentar desactivar Tailscale: %s", ex)
            finally:
                self._tailscale_disconnect_in_flight = False

        threading.Thread(target=worker, daemon=True).start()

    def run(self) -> None:
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        try:
            self.root.deiconify()
            self.root.lift()
            self.root.focus_force()
            self.root.attributes("-topmost", True)
            self._safe_after(400, self.root.attributes, "-topmost", False)
        except Exception:
            pass
        self._root_after_original(50, self._drain_ui_after_queue)
        if self._emergency_helper_enabled:
            self._root_after_original(4000, self._ensure_emergency_helper_alive)
        self._write_ui_heartbeat(state="running")
        self._root_after_original(self._ui_heartbeat_interval_ms, self._ui_heartbeat_loop)
        self.root.after(250, self._initial_restore_startup)
        self.root.after(350, self._start_vpn_bootstrap_async)
        self.root.after(1000, self._vpn_guard_loop)
        self.root.after(1000, self._monitor_positions_loop)
        self.root.after(1000, lambda: self._run_async(self._refresh_stock_selector))
        account_name = self._ai_automation_account_name()
        if account_name:
            self.root.after(1400, lambda: threading.Thread(target=self._ensure_ai_automation_running, args=(account_name,), daemon=True).start())
        self.root.after(1200, lambda: threading.Thread(target=self._refresh_nyse_status, daemon=True).start())
        self.root.after(60000, self._nyse_status_loop)
        self.root.after(5000, self._health_refresh_loop)
        self.root.mainloop()

    def _initial_restore_startup(self) -> None:
        if self._is_closing or self._startup_restore_done:
            return
        self._startup_restore_done = True
        try:
            self._restore_watch_tabs()
            self._restore_open_positions_tabs()
        except Exception as ex:
            self.logger.warning("No se pudo completar restauracion inicial de pestañas: %s", ex)

    def _on_close(self) -> None:
        self._is_closing = True
        self._save_ai_ui_state()
        self._write_ui_heartbeat(state="closing")
        with self._watch_lock:
            contexts = list(self._watch_tabs.values())
        for context in contexts:
            stop_event = context.get("stop_event")
            if stop_event is not None:
                stop_event.set()
            track_stop_event = context.get("track_stop_event")
            if track_stop_event is not None:
                track_stop_event.set()
        if self._power_inhibitor is not None:
            self._power_inhibitor.stop()
            self._power_inhibitor = None
        if self._emergency_close_process is not None:
            try:
                self._emergency_close_process.terminate()
            except Exception:
                pass
            self._emergency_close_process = None
        if self.ai_window is not None and self.ai_window.winfo_exists():
            self.ai_window.destroy()
            self.ai_window = None
        if self.ai_diagnostics_window is not None and self.ai_diagnostics_window.winfo_exists():
            self.ai_diagnostics_window.destroy()
            self.ai_diagnostics_window = None
        try:
            self._ui_heartbeat_path.unlink(missing_ok=True)
        except Exception:
            pass
        self.root.destroy()

    def _force_close(self) -> None:
        self.logger.warning("Forzando cierre de la aplicacion por solicitud del usuario.")
        try:
            self._on_close()
        except Exception:
            pass
        os._exit(0)

    def _start_emergency_close_helper(self) -> None:
        if not self._emergency_helper_enabled:
            return
        if self._is_closing:
            return
        if self._emergency_close_process is not None and self._emergency_close_process.poll() is None:
            return
        helper_path = Path(__file__).resolve().parents[1] / "runtime" / "emergency_close_helper.py"
        if not helper_path.exists():
            return
        main_path = Path(__file__).resolve().parents[1] / "main.py"
        python_executable = str(Path(sys.executable).absolute())
        self._write_ui_heartbeat(state="running")
        log_handle: Any | None = None
        try:
            self._emergency_helper_log_path.parent.mkdir(parents=True, exist_ok=True)
            log_handle = self._emergency_helper_log_path.open("a", encoding="utf-8")
            self._emergency_close_process = subprocess.Popen(
                [
                    python_executable,
                    str(helper_path),
                    str(os.getpid()),
                    str(self._ui_heartbeat_path),
                    str(self._ui_heartbeat_timeout_seconds),
                    python_executable,
                    str(main_path),
                    str(main_path.parent),
                    "1",
                    str(self._emergency_stale_checks_required),
                    str(self._emergency_min_uptime_before_restart_seconds),
                ],
                stdout=subprocess.DEVNULL,
                stderr=log_handle,
                text=True,
            )
            self.logger.info("Helper de emergencia iniciado (pid=%s)", self._emergency_close_process.pid)
            self._emergency_helper_backoff_until = 0.0
        except Exception as ex:
            self._emergency_helper_backoff_until = time.monotonic() + 8.0
            self.logger.warning("No se pudo iniciar helper de cierre de emergencia: %s", ex)
        finally:
            if log_handle is not None:
                try:
                    log_handle.close()
                except Exception:
                    pass

    def _ensure_emergency_helper_alive(self) -> None:
        if not self._emergency_helper_enabled:
            return
        if self._is_closing:
            return
        now = time.monotonic()
        if now < float(self._emergency_helper_backoff_until or 0.0):
            self._root_after_original(4000, self._ensure_emergency_helper_alive)
            return

        needs_restart = self._emergency_close_process is None
        if not needs_restart and self._emergency_close_process is not None:
            needs_restart = self._emergency_close_process.poll() is not None

        if needs_restart:
            self.logger.warning("Helper de emergencia inactivo. Relanzando...")
            self._start_emergency_close_helper()

        self._root_after_original(4000, self._ensure_emergency_helper_alive)

    def _write_ui_heartbeat(self, state: str = "running") -> None:
        try:
            self._ui_heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "pid": os.getpid(),
                "state": state,
                "updated_at": time.time(),
            }
            self._ui_heartbeat_path.write_text(json.dumps(payload), encoding="utf-8")
        except Exception:
            # Heartbeat failure must never block UI execution.
            return

    def _ui_heartbeat_loop(self) -> None:
        if self._is_closing:
            return
        self._write_ui_heartbeat(state="running")
        try:
            self.ai_trading_brain.update_ui_heartbeat(
                heartbeat_ts=time.time(),
                queue_size=int(self._ui_after_queue.qsize()),
            )
        except Exception:
            pass
        self._root_after_original(self._ui_heartbeat_interval_ms, self._ui_heartbeat_loop)

    def _health_refresh_loop(self) -> None:
        if self._is_closing:
            return
        try:
            health = self.ai_trading_brain.get_health_snapshot()
            self.app_health_var.set(
                f"App status: {health.get('app_status', 'N/A')} | memory={float(health.get('memory_mb', 0.0) or 0.0):.1f}MB | ui_queue={int(health.get('ui_queue_size', 0) or 0)} | db_queue={int(health.get('db_queue_size', 0) or 0)}"
            )
            ws = health.get("websocket", {}) if isinstance(health.get("websocket", {}), dict) else {}
            streams = ws.get("streams", []) if isinstance(ws.get("streams", []), list) else []
            stale_count = sum(1 for row in streams if bool((row or {}).get("stale", False)))
            self.ws_health_var.set(
                f"WebSocket: {'OK' if bool(ws.get('connected', False)) else 'DOWN'} | stale_streams={stale_count} | streams={len(streams)}"
            )
            self.api_health_var.set(
                f"Broker/API: {'COOLDOWN' if bool(health.get('alpaca_cooldown', False)) else 'OK'} | threads={int(health.get('active_threads', 0) or 0)}"
            )
        except Exception as ex:
            self.logger.warning("No se pudo refrescar health monitor en UI: %s", ex)
        self.root.after(5000, self._health_refresh_loop)

    def _drain_ui_after_queue(self) -> None:
        if self._is_closing:
            return
        now = time.monotonic()
        deferred: list[tuple[float, int, Any, tuple[Any, ...]]] = []
        while True:
            try:
                run_at, order, callback, args = self._ui_after_queue.get_nowait()
            except queue.Empty:
                break
            if run_at > now:
                deferred.append((run_at, order, callback, args))
                continue
            try:
                callback(*args)
            except Exception as ex:
                self.logger.warning("UI callback error: %s", ex)
        for item in deferred:
            self._ui_after_queue.put(item)
        self._root_after_original(50, self._drain_ui_after_queue)

    def _thread_safe_after(self, delay_ms: int, callback: Any, *args: Any) -> None:
        if self._is_closing:
            return
        if threading.get_ident() == self._ui_thread_ident:
            self._root_after_original(delay_ms, callback, *args)
            return
        self._ui_after_counter += 1
        run_at = time.monotonic() + max(float(delay_ms), 0.0) / 1000.0
        self._ui_after_queue.put((run_at, self._ui_after_counter, callback, args))

    def _safe_after(self, delay_ms: int, callback: Any, *args: Any) -> None:
        if self._is_closing:
            return
        try:
            self._thread_safe_after(delay_ms, callback, *args)
        except (RuntimeError, tk.TclError):
            return

    def _nyse_status_loop(self) -> None:
        threading.Thread(target=self._refresh_nyse_status, daemon=True).start()
        self.root.after(60000, self._nyse_status_loop)

    def _run_async(self, fn: Any) -> None:
        action_name = getattr(fn, "__name__", "accion")
        self.status_var.set(f"Procesando: {action_name}...")
        self._set_output(f"Ejecutando {action_name}...", focus_general=False)
        threading.Thread(target=self._run_action, args=(fn,), daemon=True).start()

    def _run_action(self, fn: Any) -> None:
        try:
            fn()
            self._mark_network_recovered()
        except requests.exceptions.HTTPError as ex:
            status = ex.response.status_code if ex.response is not None else "N/A"
            detail = ex.response.text if ex.response is not None else ""

            if status == 401:
                selected = self.account_var.get().strip()
                self.root.after(
                    0,
                    self._show_error,
                    (
                        "HTTP 401 Unauthorized.\n"
                        f"Cuenta seleccionada: {selected}\n"
                        "Revisa endpoint, API Key y Secret de esa cuenta.\n"
                        "Si compartiste tus claves, regeneralas en Alpaca.\n\n"
                        f"Detalle API:\n{detail}"
                    ),
                )
                return

            if status == 429:
                self.root.after(
                    0,
                    self._show_error,
                    (
                        "HTTP 429 Too Many Requests.\n"
                        "Se alcanzo el limite temporal de consultas del proveedor de datos.\n"
                        "Espera unos segundos y vuelve a intentar."
                    ),
                )
                return

            if isinstance(status, int) and status >= 500:
                self._mark_network_degraded(f"Servidor/API no disponible (HTTP {status}). Reintentando automaticamente...")
                return

            self.root.after(0, self._show_error, f"HTTP {status}: {detail}")
        except requests.exceptions.RequestException as ex:
            self._mark_network_degraded(f"Sin conexion de red ({ex.__class__.__name__}). Reintentando automaticamente...")
        except Exception as ex:
            self.logger.exception("Error en accion de UI")
            self.root.after(0, self._show_error, str(ex))

    def _pause_bot(self) -> None:
        self.ai_trading_brain.pause_automation()
        self.ai_trading_brain.set_emergency_mode(True, "Pausa manual del usuario")
        self.root.after(0, self._show_success, "Bot pausado. Nuevas entradas bloqueadas.", False)

    def _resume_bot(self) -> None:
        account_name = self.account_var.get().strip()
        self.ai_trading_brain.set_emergency_mode(False, "")
        self.ai_trading_brain.ensure_automation_running(account_name)
        self.root.after(0, self._show_success, "Bot reanudado.", False)

    def _emergency_stop(self) -> None:
        self.ai_trading_brain.set_emergency_mode(True, "Emergency stop manual")
        self.ai_trading_brain.pause_automation()
        self.root.after(0, self._show_error, "EMERGENCY STOP activado. Entradas pausadas.", False)

    def _reconnect_websocket(self) -> None:
        self.ai_trading_brain.reconnect_websocket()
        self.root.after(0, self._show_success, "Reconexión de WebSocket solicitada.", False)

    def _sync_positions_now(self) -> None:
        account_name = self.account_var.get().strip()
        self.ai_trading_brain.sync_positions(account_name)
        self.root.after(0, self._show_success, "Sincronización de posiciones completada.", False)

    def _export_crash_report(self) -> None:
        report_path = self.ai_trading_brain.export_crash_report()
        self.root.after(0, self._show_success, f"Crash report exportado: {report_path}", False)

    def _clear_ui_logs(self) -> None:
        self.output.configure(state="normal")
        self.output.delete("1.0", tk.END)
        self.output.configure(state="disabled")

    def _mark_network_degraded(self, message: str) -> None:
        if not self._network_degraded:
            self._network_degraded = True
            self.root.after(0, self.status_var.set, "Conexion de red perdida. Reintentando...")
            self.root.after(0, self._set_output, message, False)
            self.logger.warning(message)

    def _mark_network_recovered(self) -> None:
        if self._network_degraded:
            self._network_degraded = False
            self.root.after(0, self.status_var.set, "Conexion restaurada")
            self.logger.info("Conexion restaurada. Reanudando flujos automaticamente.")

    def _should_emit_transient_log(self, key: str, min_interval_seconds: float = 30.0) -> bool:
        now = time.monotonic()
        last = float(self._transient_log_last_ts.get(key, 0.0) or 0.0)
        if (now - last) >= float(min_interval_seconds):
            self._transient_log_last_ts[key] = now
            return True
        return False

    def _view_account(self) -> None:
        account = self.broker.get_account()
        currency = account.get("currency", "USD")
        self.root.after(
            0,
            self._update_balance_summary,
            account.get("cash", "N/A"),
            account.get("buying_power", "N/A"),
            currency,
        )
        mode = self.account_mode_var.get().replace("Modo activo: ", "")
        output = (
            "CUENTA ACTIVA\n"
            "===========\n"
            f"Modo: {mode}\n"
            f"Estado: {account.get('status', 'N/A')}\n"
            f"Cash: {account.get('cash', 'N/A')} {currency}\n"
            f"Buying Power: {account.get('buying_power', 'N/A')} {currency}\n"
            f"Equity: {account.get('equity', 'N/A')} {currency}"
        )
        self.root.after(0, self._show_success, output)

    def _view_stock_price(self) -> None:
        symbol = self._selected_symbol_for_market()
        price = self.market_data.get_last_price(symbol)
        self.root.after(0, self._show_success, f"{symbol}: {price}")

    def _view_nyse_calendar(self) -> None:
        overview = self._refresh_nyse_status(show_output=False)
        upcoming_lines = []
        for item in overview.get("upcoming", []):
            status = "ABIERTO" if item.is_open_day else "CERRADO"
            extra = f" | {item.early_close_note}" if item.is_early_close else ""
            upcoming_lines.append(f"{item.day.isoformat()} | {status} | {item.reason}{extra}")

        now_et = overview.get("now_et")
        now_text = now_et.strftime("%Y-%m-%d %H:%M:%S ET") if now_et else "N/A"
        output = (
            f"Fuente oficial NYSE: {overview.get('source', '')}\n"
            f"Hora ET: {now_text}\n"
            f"Mercado NYSE ahora: {'ABIERTO' if overview.get('is_open_now') else 'CERRADO'}\n"
            f"Manana ({overview.get('tomorrow').day.isoformat()}): "
            f"{'ABIERTO' if overview.get('tomorrow').is_open_day else 'CERRADO'} - {overview.get('tomorrow').reason}\n"
            f"Proxima apertura: {overview.get('next_open_day').isoformat()}\n\n"
            "Proximos dias:\n"
            + "\n".join(upcoming_lines)
        )
        self.root.after(0, self._show_success, output)

    def _refresh_nyse_status(self, show_output: bool = False) -> dict[str, Any]:
        try:
            overview = self.nyse_calendar.get_market_overview(days_ahead=6)
            self.root.after(0, self._apply_nyse_status_overview, overview)
            if show_output:
                self.root.after(0, self._show_success, "Estado NYSE actualizado")
            return overview
        except Exception as ex:
            self.logger.warning("No se pudo refrescar estado NYSE: %s", ex)
            self.root.after(0, self.nyse_status_var.set, "NYSE: error consultando fuente oficial")
            self.root.after(0, self.nyse_next_var.set, "Reintento automatico en 60s")
            self.root.after(0, self.nyse_days_var.set, "Proximos dias NYSE -> sin datos")
            return {}

    def _apply_nyse_status_overview(self, overview: dict[str, Any]) -> None:
        now_et = overview.get("now_et")
        now_text = now_et.strftime("%H:%M:%S ET") if now_et else "N/A"
        is_open_now = bool(overview.get("is_open_now", False))
        self.nyse_status_var.set(f"NYSE ahora: {'ABIERTO' if is_open_now else 'CERRADO'} ({now_text})")

        tomorrow = overview.get("tomorrow")
        next_open_day = overview.get("next_open_day")
        if tomorrow is not None:
            tomorrow_status = "ABIERTO" if tomorrow.is_open_day else "CERRADO"
            tomorrow_text = f"Manana {tomorrow.day.isoformat()}: {tomorrow_status}"
        else:
            tomorrow_text = "Manana: N/A"

        if next_open_day is not None:
            self.nyse_next_var.set(f"{tomorrow_text} | Proxima apertura: {next_open_day.isoformat()}")
        else:
            self.nyse_next_var.set(tomorrow_text)

        next_early_close_day = overview.get("next_early_close_day")
        if next_early_close_day is not None:
            self.nyse_early_var.set(f"Proximo cierre temprano NYSE: {next_early_close_day.isoformat()} (1:00 p.m. ET)")
        else:
            self.nyse_early_var.set("Proximo cierre temprano NYSE: no detectado en calendario cargado")

        upcoming = overview.get("upcoming", [])
        chips = []
        for item in upcoming:
            day_label = item.day.strftime("%a %d")
            marker = "E" if item.is_early_close else ("O" if item.is_open_day else "X")
            chips.append(f"{day_label}:{marker}")
        self.nyse_days_var.set("Proximos dias NYSE -> " + " | ".join(chips))

    def _switch_account(self) -> None:
        self._apply_selected_account(update_status=True, require_credentials=True)
        self._save_ai_ui_state()
        self.root.after(0, self._restore_open_positions_tabs)
        self.root.after(0, lambda: self._run_async(self._view_account))
        self.root.after(0, lambda: threading.Thread(target=self._refresh_accounts_header_summary, daemon=True).start())
        self.root.after(0, lambda: self._run_async(self._refresh_stock_selector))
        account_name = self.account_var.get().strip()
        if account_name:
            self.root.after(0, lambda: threading.Thread(target=self._ensure_ai_automation_running, args=(account_name,), daemon=True).start())
        self.root.after(0, lambda: self._run_async(self._refresh_ai_views))

    def _manual_sell_selected_stock(self) -> None:
        symbol = self._selected_symbol_for_market()
        try:
            result = self.position_manager.manual_sell(symbol)
        except ValueError as ex:
            self.root.after(0, self._show_error, str(ex))
            return

        output = (
            "VENTA MANUAL\n"
            "===========\n"
            f"Activo: {result.get('symbol', symbol)}\n"
            f"Accion: {result.get('action', 'N/A')}\n"
            f"Motivo: {result.get('reason', 'manual_sell')}\n"
            f"Qty: {result.get('qty', 'N/A')}\n"
            f"Trigger: {result.get('trigger_price', 'N/A')}\n"
            f"Ejecucion: {result.get('exit_price', 'N/A')}\n"
            f"PnL realizado: {result.get('realized_pnl', 'N/A')}\n"
            f"Trade ID: {result.get('trade_id', 'N/A')}"
        )
        self.root.after(0, self._show_success, output)

    def _schedule_selected_stock(self) -> None:
        symbol = self._selected_symbol_for_market()
        try:
            capital = float(self.capital_var.get().strip() or str(settings.default_trade_capital))
            target_profit = float(self.target_profit_var.get().strip() or str(settings.target_profit_per_share))
        except ValueError:
            self.root.after(0, self._show_error, "Capital o target invalido")
            return

        schedule = self.scheduler.schedule_trade(
            symbol=symbol,
            capital=capital,
            target_profit_per_share=target_profit,
            note="programado_para_apertura",
        )
        output = (
            "PROGRAMACION CREADA\n"
            "===================\n"
            f"ID: {schedule.id}\n"
            f"Activo: {schedule.symbol}\n"
            f"Capital: {schedule.capital:.2f}\n"
            f"Target total USD: {schedule.target_profit_per_share:.4f}\n"
            f"Estado: {schedule.status}\n"
            f"Nota: {schedule.note}"
        )
        self.root.after(0, self._show_success, output)

    def _view_schedules(self) -> None:
        schedules = self.scheduler.list_schedules()
        self.root.after(0, self._sync_schedule_tabs, schedules)
        pending = sum(1 for schedule in schedules if schedule.status == "pending")
        self.root.after(
            0,
            self._show_success,
            f"Programaciones cargadas: {len(schedules)} | pendientes: {pending}.",
        )

    def _sync_schedule_tabs(self, schedules: list[Any]) -> None:
        active_schedules = [schedule for schedule in schedules if schedule.status == "pending"]
        active_ids = {schedule.id for schedule in active_schedules}

        # Close tabs that no longer exist.
        for schedule_id in list(self._schedule_tabs.keys()):
            if schedule_id not in active_ids:
                self._close_schedule_tab(schedule_id)

        for schedule in active_schedules:
            if schedule.id not in self._schedule_tabs:
                self._create_schedule_tab(schedule)
            self._update_schedule_tab(schedule)

    def _create_schedule_tab(self, schedule: Any) -> None:
        frame = ttk.Frame(self.log_notebook)
        header = ttk.Frame(frame)
        header.pack(fill="x", pady=(0, 6))

        status_var = tk.StringVar(value="Cargando...")
        title = f"Programada: {schedule.symbol}"
        ttk.Label(
            header,
            text=f"ID: {schedule.id} | Activo: {schedule.symbol}",
            font=("TkDefaultFont", 10, "bold"),
        ).pack(side="left")
        ttk.Label(header, textvariable=status_var, foreground="#444").pack(side="left", padx=(12, 0))

        ttk.Button(
            header,
            text="Cancelar programacion",
            command=lambda sid=schedule.id: self._run_async(lambda: self._cancel_schedule_by_id(sid)),
        ).pack(side="right", padx=(8, 0))
        ttk.Button(
            header,
            text="Cerrar",
            command=lambda sid=schedule.id: self._close_schedule_tab(sid),
        ).pack(side="right")

        text = tk.Text(frame, height=18, wrap="word")
        text.pack(fill="both", expand=True)
        text.configure(state="disabled")

        self.log_notebook.add(frame, text=title)

        self._schedule_tabs[schedule.id] = {
            "frame": frame,
            "text": text,
            "status_var": status_var,
        }

    def _update_schedule_tab(self, schedule: Any) -> None:
        context = self._schedule_tabs.get(schedule.id)
        if context is None:
            return

        text_widget = context.get("text")
        status_var = context.get("status_var")
        if text_widget is None or status_var is None:
            return

        status_var.set(f"Estado: {schedule.status}")
        payload_text = (
            "DETALLE PROGRAMACION\n"
            "====================\n"
            f"ID: {schedule.id}\n"
            f"Activo: {schedule.symbol}\n"
            f"Capital: {schedule.capital:.2f}\n"
            f"Target total USD: {schedule.target_profit_per_share:.4f}\n"
            f"Estado: {schedule.status}\n"
            f"Nota: {schedule.note}\n"
            f"Ultimo error: {schedule.last_error or 'N/A'}\n"
            f"Creada: {schedule.created_at or 'N/A'}\n"
            f"Ejecutada: {schedule.executed_at or 'N/A'}\n"
            f"Trade ID: {schedule.trade_id or 'N/A'}"
        )
        text_widget.configure(state="normal")
        text_widget.delete("1.0", tk.END)
        text_widget.insert(tk.END, payload_text)
        text_widget.configure(state="disabled")

    def _close_schedule_tab(self, schedule_id: str) -> None:
        context = self._schedule_tabs.get(schedule_id)
        if context is None:
            return

        frame = context.get("frame")
        if frame is not None:
            try:
                self.log_notebook.forget(frame)
            except tk.TclError:
                pass
            frame.destroy()

        self._schedule_tabs.pop(schedule_id, None)

    def _cancel_schedule_by_id(self, schedule_id: str) -> None:
        cancelled = self.scheduler.cancel_schedule(schedule_id)
        if not cancelled:
            self.root.after(0, self._show_error, f"No se pudo cancelar la programacion: {schedule_id}")
            return

        schedules = self.scheduler.list_schedules()
        self.root.after(0, self._sync_schedule_tabs, schedules)
        self.root.after(0, self._show_success, f"Programacion cancelada: {schedule_id}")

    def _delete_history_line(self) -> None:
        target_id = self.history_delete_var.get().strip()
        if not target_id:
            self.root.after(0, self._show_error, "Debes indicar un ID de historial (trade_id o schedule_id)")
            return

        removed_trade_records = self.position_manager.journal.delete_trade(target_id)
        removed_schedule = self.scheduler.delete_schedule(target_id)
        self.history_delete_var.set("")

        if removed_trade_records == 0 and not removed_schedule:
            self.root.after(0, self._show_error, f"No se encontro una linea de historial con ID: {target_id}")
            return

        summary = (
            f"Historial actualizado. ID={target_id} | "
            f"registros_trade_borrados={removed_trade_records} | "
            f"programacion_borrada={'si' if removed_schedule else 'no'}"
        )
        self.root.after(0, self._show_success, summary)

    def _delete_trade_by_id(self, trade_id: str) -> None:
        removed = self.position_manager.journal.delete_trade(trade_id)
        if removed <= 0:
            self.root.after(0, self._show_error, f"No se pudo borrar trade_id={trade_id}", False)
            return

        self.root.after(0, self._show_success, f"Trade borrado: {trade_id} (registros={removed})", False)
        # Refresh in worker thread to avoid blocking UI when history is large.
        self._view_history()

    def _render_history_trades_tab(self, closed_trades: list[dict[str, Any]]) -> None:
        def _fmt_ts(value: Any) -> str:
            raw = str(value or "").strip()
            if not raw:
                return "N/A"
            text = raw.replace("Z", "+00:00")
            try:
                parsed = datetime.fromisoformat(text)
            except ValueError:
                return raw
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            local_dt = parsed.astimezone()
            return local_dt.strftime("%Y-%m-%d %H:%M:%S %Z")

        selected_tab = self.log_notebook.select()
        if self._history_tab_frame is not None:
            try:
                self.log_notebook.forget(self._history_tab_frame)
            except tk.TclError:
                pass
            self._history_tab_frame.destroy()

        frame = ttk.Frame(self.log_notebook)
        self._history_tab_frame = frame
        self.log_notebook.add(frame, text="Historial Trades")

        # Keep user-selected tab stable during refresh.
        if selected_tab:
            try:
                self.log_notebook.select(selected_tab)
            except tk.TclError:
                self.log_notebook.select(frame)
        else:
            self.log_notebook.select(frame)

        header = ttk.Frame(frame)
        header.pack(fill="x", pady=(0, 6))
        display_limit = 200
        display_trades = closed_trades[:display_limit]

        ttk.Label(
            header,
            text=(
                f"Trades cerrados: {len(closed_trades)} | mostrando: {len(display_trades)} "
                f"(cada fila tiene boton Borrar)"
            ),
            font=("TkDefaultFont", 10, "bold"),
        ).pack(side="left")

        body = ttk.Frame(frame)
        body.pack(fill="both", expand=True)

        canvas = tk.Canvas(body, highlightthickness=0)
        scrollbar = ttk.Scrollbar(body, orient="vertical", command=canvas.yview)
        list_frame = ttk.Frame(canvas)
        list_window = canvas.create_window((0, 0), window=list_frame, anchor="nw")

        def on_configure(_event: Any) -> None:
            canvas.configure(scrollregion=canvas.bbox("all"))

        def on_canvas_configure(event: Any) -> None:
            canvas.itemconfigure(list_window, width=event.width)

        list_frame.bind("<Configure>", on_configure)
        canvas.bind("<Configure>", on_canvas_configure)
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        if not closed_trades:
            ttk.Label(list_frame, text="No hay trades cerrados.").pack(anchor="w", padx=8, pady=8)
            return

        if len(closed_trades) > display_limit:
            ttk.Label(
                list_frame,
                text=(
                    f"Mostrando los {display_limit} trades mas recientes de {len(closed_trades)}. "
                    "Borra algunos para reducir carga."
                ),
                foreground="#555",
            ).pack(anchor="w", padx=8, pady=(6, 4))

        for trade in display_trades:
            row = ttk.Frame(list_frame)
            row.pack(fill="x", padx=6, pady=4)
            row_top = ttk.Frame(row)
            row_top.pack(fill="x")
            row_bottom = ttk.Frame(row)
            row_bottom.pack(fill="x", pady=(2, 0))

            symbol = str(trade.get("symbol", "N/A"))
            qty = float(trade.get("qty", 0.0) or 0.0)
            entry = float(trade.get("entry_price", 0.0) or 0.0)
            exit_price = float(trade.get("exit_price", 0.0) or 0.0)
            entry_time = _fmt_ts(trade.get("entry_time", ""))
            exit_time = _fmt_ts(trade.get("exit_time", ""))
            entry_origin = str(trade.get("entry_origin", "AUTOMATICA"))
            exit_origin = str(trade.get("exit_origin", "AUTOMATICA"))
            pnl = float(trade.get("realized_pnl", 0.0) or 0.0)
            result = str(trade.get("result", "N/A"))
            trade_id = str(trade.get("trade_id", ""))
            short_id = trade_id[:12]
            outcome = "GANO" if pnl > 0 else "PERDIO" if pnl < 0 else "TABLAS"
            outcome_fg, outcome_bg = (
                ("#0f5132", "#d1e7dd")
                if pnl > 0
                else ("#842029", "#f8d7da")
                if pnl < 0
                else ("#41464b", "#e2e3e5")
            )

            top_text = f"{symbol} | entrada={entry_time} | salida={exit_time} | qty={qty:.4f} | id={short_id}"
            bottom_prefix = (
                f"origen_entrada={entry_origin} | origen_salida={exit_origin} | "
                f"entry={entry:.4f} | exit={exit_price:.4f} | pnl={pnl:.4f} | RESULTADO="
            )

            ttk.Label(row_top, text=top_text).pack(side="left", anchor="w")
            ttk.Button(
                row_top,
                text="Borrar",
                command=lambda tid=trade_id: self._run_async(lambda: self._delete_trade_by_id(tid)),
            ).pack(side="right")

            ttk.Label(row_bottom, text=bottom_prefix).pack(side="left", anchor="w")
            tk.Label(
                row_bottom,
                text=outcome,
                fg=outcome_fg,
                bg=outcome_bg,
                padx=8,
                pady=1,
                font=("TkDefaultFont", 9, "bold"),
                relief="flat",
            ).pack(side="left", padx=(0, 4))
            ttk.Label(row_bottom, text=f" | {result}").pack(side="left", anchor="w")

    def _view_history(self) -> None:
        schedules = self.scheduler.list_schedules()
        cancelled_schedules = [
            {
                "id": schedule.id,
                "symbol": schedule.symbol,
                "capital": schedule.capital,
                "target_profit_per_share": schedule.target_profit_per_share,
                "status": schedule.status,
                "note": schedule.note,
                "last_error": schedule.last_error,
                "created_at": schedule.created_at,
                "executed_at": schedule.executed_at,
            }
            for schedule in schedules
            if schedule.status == "cancelled"
        ]

        records = self.position_manager.journal.load_records()
        entries_by_trade_id: dict[str, dict[str, Any]] = {}
        closed_trades: list[dict[str, Any]] = []

        for record in records:
            trade_id = str(record.get("trade_id", ""))
            if not trade_id:
                continue
            if record.get("record_type") == "entry":
                entries_by_trade_id[trade_id] = record
                continue
            if record.get("record_type") != "exit":
                continue

            entry = entries_by_trade_id.get(trade_id, {})
            closed_trades.append(
                {
                    "trade_id": trade_id,
                    "symbol": record.get("symbol") or entry.get("symbol"),
                    "entry_time": entry.get("entry_time", ""),
                    "entry_price": float(entry.get("entry_price", 0.0) or 0.0),
                    "exit_time": record.get("exit_time", ""),
                    "exit_price": float(record.get("exit_price", 0.0) or 0.0),
                    "qty": float(record.get("qty", entry.get("qty", 0.0)) or 0.0),
                    "reason_buy": entry.get("reason_buy", ""),
                    "reason_sell": record.get("reason_sell", ""),
                    "entry_origin": self._classify_entry_origin(entry.get("reason_buy", "")),
                    "exit_origin": self._classify_exit_origin(record.get("reason_sell", "")),
                    "realized_pnl": float(record.get("realized_pnl", 0.0) or 0.0),
                    "result": (
                        "GANANCIA"
                        if float(record.get("realized_pnl", 0.0) or 0.0) > 0
                        else "PERDIDA"
                        if float(record.get("realized_pnl", 0.0) or 0.0) < 0
                        else "TABLAS"
                    ),
                }
            )

        closed_trades.sort(key=lambda item: str(item.get("exit_time", "")), reverse=True)
        self.root.after(0, self._render_history_trades_tab, closed_trades)

        total_realized = sum(float(item.get("realized_pnl", 0.0) or 0.0) for item in closed_trades)
        winners = sum(1 for item in closed_trades if float(item.get("realized_pnl", 0.0) or 0.0) > 0)
        losers = sum(1 for item in closed_trades if float(item.get("realized_pnl", 0.0) or 0.0) < 0)
        breakeven = sum(1 for item in closed_trades if float(item.get("realized_pnl", 0.0) or 0.0) == 0)

        lines: list[str] = []
        lines.append("HISTORIAL")
        lines.append("=")
        lines.append(
            f"Trades cerrados: {len(closed_trades)} | Ganadores: {winners} | Perdedores: {losers} | Tablas: {breakeven}"
        )
        lines.append(f"PnL realizado total: {total_realized:.4f}")
        lines.append(f"Programaciones canceladas: {len(cancelled_schedules)}")
        lines.append("Usa la pestaña 'Historial Trades' para borrar cada trade con su botón.")

        lines.append("")
        lines.append("PROGRAMACIONES CANCELADAS")
        lines.append("-")
        if not cancelled_schedules:
            lines.append("Sin programaciones canceladas.")
        else:
            sched_header = (
                f"{'SYMBOL':<12} {'CAPITAL':>12} {'TARGET_USD':>10} {'CREATED_AT':<20} {'NOTA':<24} {'ERROR':<28} {'SCHED_ID':<12}"
            )
            lines.append(sched_header)
            lines.append("-" * len(sched_header))
            for schedule in cancelled_schedules[:120]:
                symbol = str(schedule.get("symbol", "N/A"))[:12]
                capital = float(schedule.get("capital", 0.0) or 0.0)
                target = float(schedule.get("target_profit_per_share", 0.0) or 0.0)
                created_at = str(schedule.get("created_at", ""))[:20]
                note = str(schedule.get("note", ""))[:24]
                last_error = str(schedule.get("last_error", ""))[:28]
                schedule_id = str(schedule.get("id", ""))[:12]
                lines.append(
                    f"{symbol:<12} {capital:>12.2f} {target:>10.4f} {created_at:<20} {note:<24} {last_error:<28} {schedule_id:<12}"
                )

            if len(cancelled_schedules) > 120:
                lines.append(f"... y {len(cancelled_schedules) - 120} programacion(es) mas")

        self.root.after(0, self._show_success, "\n".join(lines), False)

    @staticmethod
    def _classify_entry_origin(reason_buy: Any) -> str:
        text = str(reason_buy or "").lower()
        manual_terms = ("manual", "manual_entry_now", "user", "boton")
        if any(term in text for term in manual_terms):
            return "MANUAL"
        return "AUTOMATICA"

    @staticmethod
    def _classify_exit_origin(reason_sell: Any) -> str:
        text = str(reason_sell or "").lower()
        manual_terms = ("manual", "manual_sell", "user", "boton")
        if any(term in text for term in manual_terms):
            return "MANUAL"
        return "AUTOMATICA"

    def _refresh_dashboard(self, show_output: bool = True) -> None:
        self._apply_runtime_settings()
        dashboard = self.position_manager.get_dashboard_snapshot()
        scheduled_count = len(self.scheduler.get_pending_schedules())
        watched_count = len(self.stock_combo.cget("values"))
        output = self._format_dashboard(dashboard, watched_count, scheduled_count)
        if show_output:
            self.root.after(0, self._show_success, output)

    def _refresh_stock_selector(self) -> None:
        selected_market = self.market_kind_var.get().strip()
        try:
            if selected_market == "Cryptos":
                assets = self.broker.list_cryptos(status="active", only_tradable=True)
            else:
                assets = self.broker.list_stocks(status="active", only_tradable=True)
            symbols = sorted(
                {
                    str(asset.get("symbol", "")).upper()
                    for asset in assets
                    if str(asset.get("symbol", "")).strip()
                }
            )
        except Exception as ex:
            self.logger.warning("No se pudo actualizar la lista de activos: %s", ex)
            symbols = []
        self.root.after(0, self._apply_stock_symbols, selected_market, symbols)

    def _apply_stock_symbols(self, selected_market: str, symbols: list[str]) -> None:
        label = "Cryptos" if selected_market == "Cryptos" else "Stocks"
        if not symbols:
            self.stock_count_var.set(f"{label} cargados: 0")
            self.stock_combo.configure(values=[])
            return

        current = self.stock_var.get().strip().upper()
        self.stock_combo.configure(values=symbols)
        self.stock_count_var.set(f"{label} cargados: {len(symbols)}")

        if current and current in symbols:
            self.stock_var.set(current)
            return

        self.stock_var.set(symbols[0])

    def _start_entry_watch(self) -> None:
        symbol = self._selected_symbol_for_market()
        asset_type = self._asset_type_for_selection(symbol)
        market_label = "Cryptos" if asset_type == "crypto" else "Stocks"

        with self._watch_lock:
            existing = next(
                (
                    watch_id
                    for watch_id, ctx in self._watch_tabs.items()
                    if ctx.get("symbol") == symbol
                    and ctx.get("asset_type") == asset_type
                    and ctx.get("account") == self.account_var.get().strip()
                    and ctx.get("active")
                ),
                None,
            )

        if existing:
            self._select_watch_tab(existing)
            self._show_success(
                f"Ya existe una busqueda activa para {symbol} ({market_label}) en {self.account_var.get().strip()}."
            )
            return

        watch_id = self._create_watch_tab(symbol=symbol, asset_type=asset_type, start_mode="waiting")
        self.status_var.set(f"Buscando entrada para {symbol} ({market_label})...")
        self._show_success(
            f"Modo espera iniciado para {symbol} ({market_label}) en pestaña independiente."
        )

        threading.Thread(
            target=self._entry_watch_loop,
            args=(watch_id, symbol, asset_type),
            daemon=True,
        ).start()

    def _entry_watch_loop(self, watch_id: str, symbol: str, asset_type: str) -> None:
        try:
            while not self._watch_should_stop(watch_id):
                try:
                    runtime = self._runtime_for_watch(watch_id)
                    result = self._attempt_strategy_entry(watch_id=watch_id, symbol=symbol, asset_type=asset_type, runtime=runtime)
                    self._mark_network_recovered()
                except requests.exceptions.RequestException as ex:
                    wait_seconds = max(settings.position_monitor_interval_seconds, 5)
                    self._mark_network_degraded(
                        f"Internet caido durante busqueda de entrada ({symbol}). Reintentando en {wait_seconds}s..."
                    )
                    self._watch_log(
                        watch_id,
                        f"Sin conexion en busqueda de entrada ({ex.__class__.__name__}). Reintentando en {wait_seconds}s.",
                    )
                    self._set_watch_status(watch_id, f"Sin conexion. Reintentando en {wait_seconds}s...")
                    time.sleep(wait_seconds)
                    continue
                if result.get("action") == "buy":
                    self._watch_log(watch_id, result.get("message", "Entrada ejecutada"))
                    self.root.after(0, self._show_success, f"Entrada ejecutada para {symbol}", False)
                    self._set_watch_stop_reason(watch_id, "strategy_buy")
                    self._start_position_tracking(
                        watch_id=watch_id,
                        symbol=symbol,
                        entry_price_hint=float(result.get("entry_price", 0.0) or 0.0),
                    )
                    self._mark_watch_finished(watch_id)
                    return

                status_message = result.get("status", "Esperando mejor momento de entrada...")
                self._watch_log(watch_id, status_message)
                self._set_watch_status(watch_id, status_message)
                self.root.after(0, self.status_var.set, status_message)
                time.sleep(max(settings.position_monitor_interval_seconds, 5))

            stop_reason = self._get_watch_stop_reason(watch_id)
            if stop_reason == "entered_now":
                self._watch_log(watch_id, f"Busqueda finalizada para {symbol}: entrada inmediata ejecutada.")
                self._set_watch_status(watch_id, "Entrada ejecutada - monitoreando PnL")
            elif stop_reason == "strategy_buy":
                self._watch_log(watch_id, f"Busqueda finalizada para {symbol}: entrada por señal ejecutada.")
                self._set_watch_status(watch_id, "Entrada por señal - monitoreando PnL")
            elif stop_reason == "manual_close":
                self._watch_log(watch_id, f"Busqueda cerrada manualmente para {symbol}.")
                self._set_watch_status(watch_id, "Pestaña cerrada manualmente")
            else:
                self._watch_log(
                    watch_id,
                    f"Busqueda cancelada manualmente para {symbol} ({'Cryptos' if asset_type == 'crypto' else 'Stocks'}).",
                )
                self._set_watch_status(watch_id, "Cancelada manualmente")
        except Exception as ex:
            self._watch_log(watch_id, f"Error en busqueda: {ex}")
            self._set_watch_status(watch_id, f"Error: {ex}")
            self.root.after(0, self._show_error, f"Busqueda de entrada fallida: {ex}", False)
        finally:
            self._mark_watch_finished(watch_id)

    def _attempt_strategy_entry(self, watch_id: str, symbol: str, asset_type: str, runtime: dict[str, Any]) -> dict[str, str]:
        market_data = runtime["market_data"]
        position_manager = runtime["position_manager"]
        watch_target_profit = self._watch_target_profit_value(watch_id, position_manager=position_manager)

        candles_1m = market_data.get_candles(symbol=symbol, interval="1m", limit=50)
        candles_5m = market_data.get_candles(symbol=symbol, interval="5m", limit=50)
        latest_price = market_data.get_last_price(symbol)
        quote = market_data.get_latest_quote(symbol)
        vwap = market_data.calculate_vwap(candles_1m)
        spread_pct = float(quote.get("spread_pct", 0.0) or 0.0)

        signal = self.strategy.generate_signal(
            symbol=symbol,
            candles_1m=candles_1m,
            candles_5m=candles_5m,
            current_price=latest_price,
            spread_pct=spread_pct,
            vwap=vwap,
            asset_type=asset_type,
        )

        if signal.action != "buy":
            return {
                "action": "hold",
                "status": f"Esperando entrada {symbol}: {signal.reason} | precio={latest_price:.4f}",
            }

        open_result = self._try_open_position(
            symbol=symbol,
            latest_price=latest_price,
            spread_pct=spread_pct,
            reason=f"entry_watch: {signal.reason}",
            wait_prefix=f"Esperando entrada {symbol}",
            runtime=runtime,
            watch_id=watch_id,
            target_profit_per_share=watch_target_profit,
        )
        if open_result.get("action") != "buy":
            return open_result

        qty = float(open_result.get("qty", 0.0) or 0.0)
        capital_used = float(open_result.get("capital_used", 0.0) or 0.0)
        trade_result = open_result.get("trade_result", {})
        entry_price = float(trade_result.get("entry_price", latest_price) or latest_price)
        target_profit_per_share = float(trade_result.get("target_profit_per_share", watch_target_profit) or watch_target_profit)
        target_price = entry_price + target_profit_per_share
        _, target_details_text = self._format_watch_target_details(
            configured_target=watch_target_profit,
            effective_target_per_share=target_profit_per_share,
            qty=qty,
        )
        immediate_exit = trade_result.get("immediate_exit") or {}
        actual_limit_price = float(immediate_exit.get("limit_price", 0.0) or 0.0)

        output = (
            f"Signal: {signal.action}\n"
            f"Reason: {signal.reason}\n"
            f"Details: {signal.details}\n"
            f"Price: {latest_price}\n"
            f"VWAP: {vwap:.2f}\n"
            f"Spread%: {spread_pct:.2f}\n"
            f"Capital usado: {capital_used:.2f}\n"
            f"Qty: {qty}\n"
            f"Entry: {entry_price}\n"
            f"{target_details_text}\n"
            f"Target teorico: {target_price:.4f}\n"
            f"Limit real broker: {(f'{actual_limit_price:.4f}' if actual_limit_price > 0 else 'pendiente / no creado')}\n"
            f"Trade ID: {trade_result.get('trade_id', 'N/A')}"
        )
        return {"action": "buy", "message": output}

    def _try_open_position(
        self,
        symbol: str,
        latest_price: float,
        spread_pct: float,
        reason: str,
        wait_prefix: str,
        runtime: dict[str, Any],
        watch_id: str | None = None,
        target_profit_per_share: float | None = None,
    ) -> dict[str, Any]:
        broker = runtime["broker"]
        position_manager = runtime["position_manager"]
        can_open, reason_text = position_manager.can_open_new_trade(symbol)
        if not can_open:
            return {
                "action": "wait",
                "status": f"{wait_prefix}: {reason_text}",
            }

        if watch_id is not None:
            try:
                trade_capital = float(self._planned_rebuy_capital_amount(watch_id))
            except Exception:
                return {"action": "wait", "status": "Capital de recompra invalido"}
        else:
            try:
                trade_capital = float(self.capital_var.get().strip() or str(settings.default_trade_capital))
            except ValueError:
                return {"action": "wait", "status": "Capital por compra invalido"}

        if trade_capital <= 0:
            return {"action": "wait", "status": "Capital por compra debe ser mayor que cero"}

        if watch_id is None:
            account = broker.get_account()
            cash_available = float(account.get("cash", 0.0) or 0.0)
            if trade_capital > cash_available:
                return {
                    "action": "wait",
                    "status": (
                        f"{wait_prefix}: capital ({trade_capital:.2f}) excede cash ({cash_available:.2f})"
                    ),
                }

        qty = round(trade_capital / latest_price, 4)
        if qty <= 0:
            return {"action": "wait", "status": "Cantidad calculada invalida"}

        try:
            current_daily_pnl = float(self.daily_pnl_var.get().strip() or "0")
        except ValueError:
            return {"action": "wait", "status": "PnL diario invalido"}

        if not self.risk_manager.can_trade(current_daily_pnl=current_daily_pnl):
            return {"action": "wait", "status": "Bloqueado por limite de perdida diaria"}

        configured_target_delta = float(target_profit_per_share or self._safe_target_profit_value())
        if watch_id is not None:
            configured_target_delta = self._watch_target_profit_value(watch_id, position_manager=position_manager)
        actor = "normal"
        if watch_id is not None:
            with self._watch_lock:
                context = self._watch_tabs.get(watch_id)
                if context is not None:
                    actor = str(context.get("bot_actor", "normal") or "normal").strip().lower()
        try:
            if watch_id is not None and actor == "ia":
                trade_result = position_manager.open_position_limit(
                    symbol=symbol,
                    qty=qty,
                    reason=reason,
                    spread_pct=spread_pct,
                    target_profit_per_share=configured_target_delta,
                )
            else:
                trade_result = position_manager.open_position(
                    symbol=symbol,
                    qty=qty,
                    reason=reason,
                    spread_pct=spread_pct,
                    target_profit_per_share=configured_target_delta,
                )
        except ValueError as ex:
            return {"action": "wait", "status": f"{wait_prefix}: {ex}"}
        return {
            "action": "buy",
            "trade_result": trade_result,
            "qty": qty,
            "entry_price": float(trade_result.get("entry_price", latest_price) or latest_price),
            "capital_used": float(trade_result.get("entry_cost", trade_capital) or trade_capital),
        }

    def _enter_market_now(self, watch_id: str) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
            if context is None:
                return
            if bool(context.get("enter_now_in_flight", False)):
                status = "Entrada inmediata ya en proceso para esta pestaña."
                self._watch_log(watch_id, status)
                self._set_watch_status(watch_id, status)
                return
            context["enter_now_in_flight"] = True

        self._set_watch_buy_now_enabled(watch_id, enabled=False)
        self._set_watch_status(watch_id, "Entrada inmediata en proceso...")
        threading.Thread(target=self._enter_market_now_worker, args=(watch_id,), daemon=True).start()

    def _attempt_auto_rebuy_sequence(self, watch_id: str, symbol: str, max_attempts: int = 8, wait_seconds: float = 1.0) -> bool:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
            baseline_success_count = int((context or {}).get("auto_rebuy_success_count", 0) or 0)

        for attempt in range(1, max_attempts + 1):
            with self._watch_lock:
                context = self._watch_tabs.get(watch_id)
            if context is None:
                return False

            self._watch_log(watch_id, f"Recompra inmediata {symbol}: intento {attempt}/{max_attempts}.")
            self._enter_market_now_worker(
                watch_id=watch_id,
                fallback_to_wait_on_fail=False,
                source="auto_rebuy",
            )

            with self._watch_lock:
                context_after = self._watch_tabs.get(watch_id)
                current_success_count = int((context_after or {}).get("auto_rebuy_success_count", 0) or 0)
                last_http_status = int((context_after or {}).get("rebuy_last_http_status", 0) or 0)
                retry_after_seconds = float((context_after or {}).get("rebuy_retry_after_seconds", 0.0) or 0.0)
                cooldown_until = float((context_after or {}).get("rebuy_cooldown_until", 0.0) or 0.0)
            if current_success_count > baseline_success_count:
                self._watch_log(watch_id, f"Recompra inmediata confirmada para {symbol} en intento {attempt}.")
                return True

            try:
                runtime = self._runtime_for_watch(watch_id)
                broker = runtime["broker"]
                if self._find_open_position_by_symbol(symbol, broker=broker) is not None:
                    self._watch_log(
                        watch_id,
                        f"Posicion abierta detectada para {symbol}, pero sin compra nueva de recompra en intento {attempt}.",
                    )
                    return True
            except Exception:
                pass

            if attempt < max_attempts:
                delay_seconds = float(wait_seconds)
                remaining_cooldown = max(cooldown_until - time.monotonic(), 0.0)
                if remaining_cooldown > 0:
                    delay_seconds = max(delay_seconds, remaining_cooldown)
                if last_http_status == 429:
                    backoff_seconds = min(12.0, float(wait_seconds) * (2 ** max(attempt - 1, 0)))
                    delay_seconds = max(float(wait_seconds), retry_after_seconds, backoff_seconds)
                    if remaining_cooldown > 0:
                        delay_seconds = max(delay_seconds, remaining_cooldown)
                    self._watch_log(
                        watch_id,
                        f"Rate limit detectado en recompra ({symbol}). Esperando {delay_seconds:.1f}s antes del siguiente intento.",
                    )
                elif last_http_status in {503, 504}:
                    delay_seconds = max(float(wait_seconds), 2.0)
                time.sleep(delay_seconds)

        self._watch_log(watch_id, f"Recompra inmediata no se pudo ejecutar para {symbol}. Volviendo a modo espera.")
        self._resume_waiting_entry(watch_id=watch_id, symbol=symbol)
        return False

    def _toggle_auto_rebuy(self, watch_id: str) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
            if context is None:
                return
            enabled = not bool(context.get("auto_rebuy", False))
            context["auto_rebuy"] = enabled

        self._apply_auto_rebuy_button_state(watch_id, enabled)
        self._refresh_auto_rebuy_capital_display(watch_id)
        state_label = "ENCENDIDO" if enabled else "APAGADO"
        capital_text = self._planned_rebuy_capital_text(watch_id)
        self._watch_log(watch_id, f"Recompra inmediata tras venta: {state_label} | {capital_text}")
        self._set_watch_status(watch_id, f"Recompra inmediata: {state_label}")
        self._save_watch_tabs_state()

    def _apply_auto_rebuy_button_state(self, watch_id: str, enabled: bool) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
        if context is None:
            return

        button = context.get("auto_rebuy_button")
        if button is None:
            return

        if enabled:
            button.configure(
                text="Recompra inmediata ON",
                bg="#0b8f3a",
                activebackground="#18a34a",
                fg="white",
                activeforeground="white",
            )
        else:
            button.configure(
                text="Recompra inmediata OFF",
                bg="#8b1e1e",
                activebackground="#b82d2d",
                fg="white",
                activeforeground="white",
            )

    def _planned_rebuy_capital_text(self, watch_id: str) -> str:
        try:
            planned = self._planned_rebuy_capital_amount(watch_id)
        except Exception:
            planned = float(settings.default_trade_capital)
        return f"capital recompra planificado: {planned:.2f}"

    def _save_watch_trade_config(self, watch_id: str) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
        if context is None:
            return

        symbol = str(context.get("symbol", "") or "").strip().upper()
        actor = str(context.get("bot_actor", "normal") or "normal").strip().lower()
        if actor != "ia":
            self._watch_log(watch_id, "Guardar configuracion disponible solo para pestañas de Bot IA.")
            return

        try:
            target_value = float(self._watch_target_profit_value(watch_id))
        except Exception:
            target_value = float(self._safe_target_profit_value())
        try:
            capital_value = float(self._planned_rebuy_capital_amount(watch_id))
        except Exception:
            capital_value = float(settings.default_trade_capital)

        self._save_watch_tabs_state()
        self._watch_log(
            watch_id,
            f"Configuración IA guardada para {symbol}: target={target_value:.4f} | capital_siguiente_compra={capital_value:.2f}",
        )
        self._set_watch_status(
            watch_id,
            f"Configuración IA guardada (target={target_value:.4f}, capital={capital_value:.2f})",
        )

    def _watch_target_profit_value(
        self,
        watch_id: str,
        position_manager: PositionManager | None = None,
    ) -> float:
        fallback = float(self._safe_target_profit_value())
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
            if context is None:
                return fallback
            target_var = context.get("target_profit_var")
            cached = float(context.get("target_profit_cached", fallback) or fallback)

        value = cached
        if target_var is not None and threading.get_ident() == self._ui_thread_ident:
            try:
                value = float(target_var.get().strip())
            except (ValueError, RuntimeError, tk.TclError):
                value = cached

        if value <= 0:
            value = fallback

        with self._watch_lock:
            current = self._watch_tabs.get(watch_id)
            if current is not None:
                current["target_profit_cached"] = value
        return value

    @staticmethod
    def _format_watch_target_details(
        *,
        configured_target: float,
        effective_target_per_share: float,
        qty: float,
    ) -> tuple[str, str]:
        configured_target = float(configured_target or 0.0)
        effective_target_per_share = float(effective_target_per_share or 0.0)
        qty = float(qty or 0.0)
        effective_total = effective_target_per_share * qty if qty > 0 else 0.0

        short_text = f"target_bruto={configured_target:.4f} | ganancia_total_esperada={effective_total:.4f}"
        long_text = (
            f"Target bruto configurado: {configured_target:.4f}\n"
            f"Ganancia total esperada en el limit: {effective_total:.4f}"
        )
        return short_text, long_text

    def _planned_rebuy_capital_amount(self, watch_id: str) -> float:
        runtime = self._runtime_for_watch(watch_id)
        broker = runtime["broker"]
        now_monotonic = time.monotonic()
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
            rebuy_capital_var = context.get("rebuy_capital_var") if context is not None else None
            cached_cash_ts = float((context or {}).get("cached_cash_ts", 0.0) or 0.0)
            cached_cash = float((context or {}).get("cached_cash", 0.0) or 0.0)
            cached_rebuy_capital = float((context or {}).get("rebuy_capital_cached", settings.default_trade_capital) or settings.default_trade_capital)
        trade_capital = cached_rebuy_capital
        if rebuy_capital_var is not None and threading.get_ident() == self._ui_thread_ident:
            try:
                trade_capital = float(rebuy_capital_var.get().strip())
            except ValueError:
                trade_capital = float(settings.default_trade_capital)

        if (now_monotonic - cached_cash_ts) <= 6.0 and cached_cash >= 0.0:
            cash_available = cached_cash
        else:
            account = broker.get_account()
            cash_available = float(account.get("cash", 0.0) or 0.0)
            with self._watch_lock:
                current = self._watch_tabs.get(watch_id)
                if current is not None:
                    current["cached_cash"] = cash_available
                    current["cached_cash_ts"] = now_monotonic
        return max(min(trade_capital, cash_available), 0.0)

    def _refresh_auto_rebuy_capital_display(self, watch_id: str) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
        if context is None:
            return
        capital_var = context.get("auto_rebuy_capital_var")
        if capital_var is None:
            return
        actor = str(context.get("bot_actor", "normal") or "normal").strip().lower()
        if actor == "ia":
            self._safe_after(0, capital_var.set, "Modo IA: análisis continuo")
            return
        enabled = self._is_auto_rebuy_enabled(watch_id)
        if not enabled:
            self._safe_after(0, capital_var.set, "Recompra: OFF")
            return
        self._safe_after(0, capital_var.set, f"Recompra: {self._planned_rebuy_capital_text(watch_id)}")

    def _set_current_buy_capital_display(self, watch_id: str, capital_used: float) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
        if context is None:
            return
        capital_var = context.get("current_buy_capital_var")
        if capital_var is None:
            return
        self._safe_after(0, capital_var.set, f"Compra actual: {float(capital_used or 0.0):.2f}")

    def _is_auto_rebuy_enabled(self, watch_id: str) -> bool:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
            if context is None:
                return False
            return bool(context.get("auto_rebuy", False))

    def _pause_ia_watch_tab(self, watch_id: str) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
            if context is None:
                return
            actor = str(context.get("bot_actor", "normal") or "normal").strip().lower()
            if actor != "ia":
                return
            symbol = str(context.get("symbol", "") or "").upper()
            stop_event = context.get("stop_event")
            if stop_event is None:
                stop_event = threading.Event()
                context["stop_event"] = stop_event
            stop_event.set()
            context["active"] = False
            context["mode"] = "waiting"
            context["stop_reason"] = "ia_manual_pause"

        self._save_watch_tabs_state()
        self._refresh_watch_buy_now_state(watch_id)
        self._refresh_watch_sell_now_state(watch_id)
        self._watch_log(watch_id, f"IA pausada manualmente para {symbol}.")
        self._set_watch_status(watch_id, f"IA en pausa para {symbol}. Usa 'Reanudar IA' para continuar.")

    def _resume_ia_watch_tab(self, watch_id: str) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
            if context is None:
                return
            actor = str(context.get("bot_actor", "normal") or "normal").strip().lower()
            if actor != "ia":
                return
            symbol = str(context.get("symbol", "") or "").upper()
        if not symbol:
            return
        self._resume_waiting_entry(watch_id=watch_id, symbol=symbol)

    def _disable_ia_account_from_watch(self, watch_id: str) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
            if context is None:
                return
            account_name = str(context.get("account", "") or "").strip()
            actor = str(context.get("bot_actor", "normal") or "normal").strip().lower()
            symbol = str(context.get("symbol", "") or "").upper()

        if actor != "ia":
            self.root.after(0, self._show_error, "El apagado por cuenta aplica solo a pestañas IA.", False)
            return
        if not account_name:
            self.root.after(0, self._show_error, "No se pudo identificar la cuenta de la pestaña IA.", False)
            return

        account = self.ai_trading_brain.refresh_account_context(account_name)
        account_id = int(account["id"])
        runtime = self.ai_trading_brain.database.get_runtime_settings(account_id) or {}
        funds = self.ai_trading_brain.database.get_bot_funds(account_id) or {}

        self.ai_trading_brain.update_runtime_controls(
            account_name=account_name,
            max_capital_assigned=float(funds.get("max_capital_assigned", 0.0) or 0.0),
            max_position_size=float(funds.get("max_position_size", 0.0) or 0.0),
            max_daily_loss=float(funds.get("max_daily_loss", 0.0) or 0.0),
            enabled=False,
            signal_only_mode=bool(runtime.get("signal_only_mode", 1)),
            paper_trading=bool(runtime.get("paper_trading", 1)),
            live_trading_enabled=bool(runtime.get("live_trading_enabled", 0)),
            manual_approval_required=bool(runtime.get("manual_approval_required", 1)),
            kill_switch=True,
            auto_trade_stocks_enabled=False,
            auto_trade_cryptos_enabled=False,
            scanner_decision_engine=str(runtime.get("scanner_decision_engine", "heuristic") or "heuristic"),
            futures_only_mode=bool(runtime.get("futures_only_mode", getattr(settings, "crypto_futures_only_mode", True))),
            futures_leverage=max(int(runtime.get("futures_leverage", getattr(settings, "crypto_futures_default_leverage", 1)) or 1), 1),
            futures_require_technical=bool(runtime.get("futures_require_technical", getattr(settings, "ai_futures_require_technical", True))),
            futures_require_news=bool(runtime.get("futures_require_news", getattr(settings, "ai_futures_require_news", False))),
            futures_enable_long=bool(runtime.get("futures_enable_long", getattr(settings, "ai_futures_enable_long", True))),
            futures_enable_short=bool(runtime.get("futures_enable_short", getattr(settings, "ai_futures_enable_short", True))),
        )

        self._pause_ia_watch_tab(watch_id)
        if self.account_var.get().strip() == account_name:
            status = self.ai_trading_brain.get_automation_status(account_name)
            self._refresh_ai_runtime_view(status=status)
        self.root.after(
            0,
            self._show_success,
            f"IA apagada solo para la cuenta {account_name} (símbolo {symbol}). Otras cuentas siguen activas.",
            False,
        )

    def _sell_market_now(self, watch_id: str) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
            if context is None:
                return
            if bool(context.get("sell_now_in_flight", False)):
                status = "Venta inmediata ya en proceso para esta pestaña."
                self._watch_log(watch_id, status)
                self._set_watch_status(watch_id, status)
                return
            context["sell_now_in_flight"] = True

        self._set_watch_sell_now_enabled(watch_id, enabled=False)
        self._set_watch_status(watch_id, "Venta inmediata en proceso...")
        threading.Thread(target=self._sell_market_now_worker, args=(watch_id,), daemon=True).start()

    def _cancel_symbol_limits(self, watch_id: str) -> None:
        threading.Thread(target=self._cancel_symbol_limits_worker, args=(watch_id,), daemon=True).start()

    def _cancel_symbol_limits_worker(self, watch_id: str) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
        if context is None:
            return
        runtime = self._runtime_for_watch(watch_id)
        order_manager = runtime["order_manager"]

        symbol = str(context.get("symbol", "")).upper()
        if not symbol:
            return

        combined: dict[str, dict[str, Any]] = {}
        for status in ("open", "all"):
            for order in order_manager.review_orders(status=status, limit=200):
                order_id = str(order.get("id", "")).strip()
                if not order_id:
                    continue
                combined[order_id] = order

        terminal_statuses = {"filled", "canceled", "rejected", "expired", "replaced"}
        target_key = self._symbol_key(symbol)
        limits = [
            order
            for order in combined.values()
            if self._symbol_key(str(order.get("symbol", ""))) == target_key
            and str(order.get("type", "")).lower().strip() == "limit"
            and str(order.get("status", "")).lower().strip() not in terminal_statuses
        ]

        if not limits:
            msg = f"No hay ordenes LIMIT activas para {symbol}."
            self._watch_log(watch_id, msg)
            self._set_watch_status(watch_id, msg)
            self.root.after(0, self._show_success, msg, False)
            return

        cancelled = 0
        failed = 0
        for order in limits:
            order_id = str(order.get("id", "")).strip()
            if not order_id:
                continue
            try:
                if order_manager.cancel_order(order_id):
                    cancelled += 1
                else:
                    failed += 1
            except Exception:
                failed += 1

        msg = (
            f"Cancelar LIMIT {symbol} -> encontradas={len(limits)} | "
            f"canceladas={cancelled} | fallidas={failed}"
        )
        self._watch_log(watch_id, msg)
        self._set_watch_status(watch_id, msg)
        self.root.after(0, self._show_success, msg, False)

    def _sell_market_now_worker(self, watch_id: str) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
        if context is None:
            return
        runtime = self._runtime_for_watch(watch_id)
        position_manager = runtime["position_manager"]
        broker = runtime["broker"]

        symbol = str(context.get("symbol", "")).upper()
        actor = str(context.get("bot_actor", "normal") or "normal").strip().lower()
        if not symbol:
            return

        try:
            result = position_manager.manual_sell(symbol)
            cancelled_limits = int(result.get("cancelled_limit_orders", 0) or 0)
            self._watch_log(
                watch_id,
                (
                    f"Venta manual ejecutada en {symbol} | limits_cancelados={cancelled_limits} | trigger={result.get('trigger_price', 'N/A')} "
                    f"exec={result.get('exit_price', 'N/A')} pnl={result.get('realized_pnl', 'N/A')}"
                ),
            )
            self._set_watch_status(watch_id, f"Venta manual enviada para {symbol}. Confirmando cierre...")
            self.root.after(0, self._show_success, f"Venta manual ejecutada para {symbol}.", False)

            if self._wait_position_closed(symbol=symbol, max_attempts=8, sleep_seconds=0.5, broker=broker):
                if actor == "ia":
                    self._resume_waiting_entry(watch_id=watch_id, symbol=symbol)
                elif self._is_auto_rebuy_enabled(watch_id):
                    self._resume_waiting_entry(watch_id=watch_id, symbol=symbol)
                else:
                    self._pause_watch_after_position_closed(
                        watch_id=watch_id,
                        symbol=symbol,
                        status_message=f"Venta confirmada para {symbol}. Recompra OFF: pestaña en pausa.",
                    )
            else:
                self._watch_log(watch_id, f"Venta enviada en {symbol}, aun pendiente de confirmacion en broker.")
                self._set_watch_status(watch_id, f"Venta enviada en {symbol}; esperando confirmacion...")
        except Exception as ex:
            status = f"Venta manual fallida para {symbol}: {ex}"
            self._watch_log(watch_id, status)
            self._set_watch_status(watch_id, status)
            self.root.after(0, self._show_error, status, False)
        finally:
            with self._watch_lock:
                current_context = self._watch_tabs.get(watch_id)
                if current_context is not None:
                    current_context["sell_now_in_flight"] = False
            self._refresh_watch_sell_now_state(watch_id)

    def _enter_market_now_worker(
        self,
        watch_id: str,
        fallback_to_wait_on_fail: bool = False,
        source: str = "manual",
    ) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
        if context is None:
            return
        runtime = self._runtime_for_watch(watch_id)
        market_data = runtime["market_data"]
        position_manager = runtime["position_manager"]

        symbol = str(context.get("symbol", "")).upper()
        asset_type = str(context.get("asset_type", "stock"))
        market_label = "Cryptos" if asset_type == "crypto" else "Stocks"

        try:
            latest_price = market_data.get_last_price(symbol)
            quote = market_data.get_latest_quote(symbol)
            spread_pct = float(quote.get("spread_pct", 0.0) or 0.0)
            watch_target_profit = self._watch_target_profit_value(watch_id, position_manager=position_manager)
            result = self._try_open_position(
                symbol=symbol,
                latest_price=latest_price,
                spread_pct=spread_pct,
                reason="manual_entry_now",
                wait_prefix=f"Entrada inmediata {symbol}",
                runtime=runtime,
                watch_id=watch_id,
                target_profit_per_share=watch_target_profit,
            )

            if result.get("action") != "buy":
                status = str(result.get("status", "No se pudo ejecutar entrada inmediata"))
                self._watch_log(watch_id, status)
                self._set_watch_status(watch_id, status)
                self.root.after(0, self.status_var.set, status)
                if fallback_to_wait_on_fail:
                    self._resume_waiting_entry(watch_id=watch_id, symbol=symbol)
                return

            trade_result = result.get("trade_result", {})
            qty = float(result.get("qty", 0.0) or 0.0)
            capital_used = float(result.get("capital_used", 0.0) or 0.0)
            entry_price = float(trade_result.get("entry_price", latest_price) or latest_price)
            target_profit_per_share = float(trade_result.get("target_profit_per_share", watch_target_profit) or watch_target_profit)
            target_price = entry_price + target_profit_per_share
            _, target_details_text = self._format_watch_target_details(
                configured_target=watch_target_profit,
                effective_target_per_share=target_profit_per_share,
                qty=qty,
            )
            immediate_exit = trade_result.get("immediate_exit") or {}
            actual_limit_price = float(immediate_exit.get("limit_price", 0.0) or 0.0)
            if actual_limit_price > 0:
                with self._watch_lock:
                    current_context = self._watch_tabs.get(watch_id)
                    if current_context is not None:
                        current_context["last_known_limit_price"] = actual_limit_price
            prefix = "Reentrada inmediata ejecutada" if source == "auto_rebuy" else "Entrada inmediata ejecutada"
            message = (
                f"{prefix}\n"
                f"Activo: {symbol} ({market_label})\n"
                f"Price: {latest_price}\n"
                f"Spread%: {spread_pct:.2f}\n"
                f"Capital usado: {capital_used:.2f}\n"
                f"Qty: {qty}\n"
                f"Entry: {entry_price}\n"
                f"{target_details_text}\n"
                f"Target teorico: {target_price:.4f}\n"
                f"Limit real broker: {(f'{actual_limit_price:.4f}' if actual_limit_price > 0 else 'pendiente / no creado')}\n"
                f"Trade ID: {trade_result.get('trade_id', 'N/A')}\n"
                "La gestion automatica de la posicion sigue activa."
            )
            self._watch_log(watch_id, message)
            self._set_current_buy_capital_display(watch_id, capital_used)
            self._set_watch_buy_now_enabled(watch_id, enabled=False)
            self._set_watch_status(watch_id, "Entrada inmediata ejecutada")
            self.root.after(0, self._show_success, message, False)
            if source == "auto_rebuy":
                with self._watch_lock:
                    current_context = self._watch_tabs.get(watch_id)
                    if current_context is not None:
                        current_context["auto_rebuy_success_count"] = int(current_context.get("auto_rebuy_success_count", 0) or 0) + 1
                        current_context["rebuy_last_http_status"] = 0
                        current_context["rebuy_retry_after_seconds"] = 0.0
                        current_context["rebuy_cooldown_until"] = 0.0
            self._set_watch_stop_reason(watch_id, "entered_now")
            self._start_position_tracking(
                watch_id=watch_id,
                symbol=symbol,
                entry_price_hint=float(trade_result.get("entry_price", latest_price) or latest_price),
            )
            self._mark_watch_finished(watch_id)

            stop_event = context.get("stop_event")
            if stop_event is not None:
                stop_event.set()
        except requests.exceptions.HTTPError as ex:
            status_code = ex.response.status_code if ex.response is not None else "N/A"
            detail = ex.response.text if ex.response is not None else ""
            retry_after_seconds = 0.0
            if ex.response is not None:
                raw_retry_after = str(ex.response.headers.get("Retry-After", "")).strip()
                if raw_retry_after:
                    try:
                        retry_after_seconds = max(float(raw_retry_after), 0.0)
                    except ValueError:
                        retry_after_seconds = 0.0
            if status_code == 429 and retry_after_seconds <= 0.0:
                retry_after_seconds = 1.5
            cooldown_until = time.monotonic() + max(retry_after_seconds, 1.5 if status_code == 429 else 0.0)
            with self._watch_lock:
                current_context = self._watch_tabs.get(watch_id)
                if current_context is not None:
                    current_context["rebuy_last_http_status"] = int(status_code) if str(status_code).isdigit() else 0
                    current_context["rebuy_retry_after_seconds"] = float(retry_after_seconds)
                    current_context["rebuy_cooldown_until"] = float(cooldown_until)
            status = f"Entrada inmediata fallida (HTTP {status_code}): {detail or str(ex)}"
            self._watch_log(watch_id, status)
            self._set_watch_status(watch_id, status)
            self.root.after(0, self._show_error, status, False)
            if fallback_to_wait_on_fail:
                self._resume_waiting_entry(watch_id=watch_id, symbol=symbol)
        except Exception as ex:
            status = f"Entrada inmediata fallida: {ex}"
            self._watch_log(watch_id, status)
            self._set_watch_status(watch_id, status)
            self.root.after(0, self._show_error, status, False)
            if fallback_to_wait_on_fail:
                self._resume_waiting_entry(watch_id=watch_id, symbol=symbol)
        finally:
            with self._watch_lock:
                current_context = self._watch_tabs.get(watch_id)
                if current_context is not None:
                    current_context["enter_now_in_flight"] = False
            if source == "manual":
                self._refresh_watch_buy_now_state(watch_id)

    def _create_watch_tab(
        self,
        symbol: str,
        asset_type: str,
        start_mode: str = "waiting",
        account_name: str | None = None,
        rebuy_capital_value: float | None = None,
        target_profit_value: float | None = None,
        bot_actor: str = "normal",
    ) -> str:
        with self._watch_lock:
            self._watch_counter += 1
            watch_id = f"watch-{self._watch_counter}"

        account_name = account_name or self.account_var.get().strip()
        market_label = "Cryptos" if asset_type == "crypto" else "Stocks"
        runtime = self._get_account_runtime(account_name)

        frame = ttk.Frame(self.log_notebook)
        header = ttk.Frame(frame)
        header.pack(fill="x", pady=(0, 6))

        title_row = ttk.Frame(header)
        title_row.pack(fill="x")

        status_row_top = ttk.Frame(header)
        status_row_top.pack(fill="x", pady=(2, 0))

        status_row_bottom = ttk.Frame(header)
        status_row_bottom.pack(fill="x", pady=(2, 0))

        controls_row = ttk.Frame(header)
        controls_row.pack(fill="x", pady=(4, 0))

        if rebuy_capital_value is None:
            rebuy_capital_value = float(settings.default_trade_capital)
        if target_profit_value is None:
            target_profit_value = float(self._safe_target_profit_value())

        actor_key = str(bot_actor or "normal").strip().lower()
        if actor_key not in {"ia", "normal"}:
            actor_key = "normal"
        actor_label = "Bot IA" if actor_key == "ia" else "Bot normal"

        status_var = tk.StringVar(value="Iniciando...")
        status_detail_var = tk.StringVar(value="")
        actor_var = tk.StringVar(value=f"Operador: {actor_label}")
        auto_rebuy_capital_var = tk.StringVar(value=("Modo IA: análisis continuo" if actor_key == "ia" else "Recompra: OFF"))
        current_buy_capital_var = tk.StringVar(value="Compra actual: N/A")
        rebuy_capital_var = tk.StringVar(value=f"{float(rebuy_capital_value):.2f}")
        target_profit_var = tk.StringVar(value=f"{float(target_profit_value):.4f}")
        ttk.Label(
            title_row,
            text=f"Cuenta: {account_name} | Mercado: {market_label} | Activo: {symbol}",
            font=("TkDefaultFont", 10, "bold"),
        ).pack(side="left")
        ttk.Label(status_row_top, textvariable=status_var, foreground="#444").pack(side="left", anchor="w")
        ttk.Label(status_row_bottom, textvariable=status_detail_var, foreground="#444").pack(side="left", padx=(0, 12), anchor="w")
        ttk.Label(status_row_bottom, textvariable=actor_var, foreground="#1d5f2a").pack(side="left", padx=(0, 12))
        ttk.Label(status_row_bottom, textvariable=auto_rebuy_capital_var, foreground="#0b5cab").pack(side="left", padx=(0, 12))
        ttk.Label(status_row_bottom, textvariable=current_buy_capital_var, foreground="#8a4d00").pack(side="left")
        buy_now_button = ttk.Button(
            controls_row,
            text="Comprar ahora",
            command=lambda wid=watch_id: self._enter_market_now(wid),
        )
        if actor_key != "ia":
            buy_now_button.pack(side="left", padx=(0, 8))
        auto_rebuy_button = tk.Button(
            controls_row,
            text="Recompra inmediata OFF",
            command=lambda wid=watch_id: self._toggle_auto_rebuy(wid),
            bg="#8b1e1e",
            activebackground="#b82d2d",
            fg="white",
            activeforeground="white",
            relief="raised",
            bd=1,
            padx=8,
            pady=2,
        )
        if actor_key != "ia":
            auto_rebuy_button.pack(side="left", padx=(0, 8))
        ttk.Label(controls_row, text="Target USD total").pack(side="left", padx=(0, 4))
        ttk.Entry(controls_row, textvariable=target_profit_var, width=10).pack(side="left", padx=(0, 8))
        capital_label = "Capital a usar" if actor_key == "ia" else "Capital recompra"
        ttk.Label(controls_row, text=capital_label).pack(side="left", padx=(0, 4))
        ttk.Entry(controls_row, textvariable=rebuy_capital_var, width=10).pack(side="left", padx=(0, 8))
        sell_now_button = ttk.Button(
            controls_row,
            text="Vender ahora",
            command=lambda wid=watch_id: self._sell_market_now(wid),
        )
        if actor_key != "ia":
            sell_now_button.pack(side="left", padx=(0, 8))
            ttk.Button(
                controls_row,
                text="Cancelar limites",
                command=lambda wid=watch_id: self._cancel_symbol_limits(wid),
            ).pack(side="left", padx=(0, 8))
        else:
            sell_now_button.pack(side="left", padx=(0, 8))
            ttk.Button(
                controls_row,
                text="Pausar IA",
                command=lambda wid=watch_id: self._pause_ia_watch_tab(wid),
            ).pack(side="left", padx=(0, 8))
            ttk.Button(
                controls_row,
                text="Reanudar IA",
                command=lambda wid=watch_id: self._resume_ia_watch_tab(wid),
            ).pack(side="left", padx=(0, 8))
            ttk.Button(
                controls_row,
                text="Apagar IA",
                command=lambda wid=watch_id: self._run_async(lambda: self._disable_ia_account_from_watch(wid)),
            ).pack(side="left", padx=(0, 8))
            ttk.Button(
                controls_row,
                text="Guardar",
                command=lambda wid=watch_id: self._save_watch_trade_config(wid),
            ).pack(side="left", padx=(0, 8))
        ttk.Button(
            controls_row,
            text="Cerrar",
            command=lambda wid=watch_id: self._request_close_watch_tab(wid),
        ).pack(side="left")

        text = tk.Text(frame, height=18, wrap="word")
        text.pack(fill="both", expand=True)
        text.configure(state="disabled")

        self.log_notebook.add(frame, text=f"[{actor_label}] {symbol} | {account_name}")
        self.log_notebook.select(frame)

        context = {
            "id": watch_id,
            "frame": frame,
            "text": text,
            "status_var": status_var,
            "status_detail_var": status_detail_var,
            "actor_var": actor_var,
            "bot_actor": actor_key,
            "symbol": symbol,
            "asset_type": asset_type,
            "account": account_name,
            "runtime": runtime,
            "active": start_mode == "waiting",
            "stop_event": threading.Event(),
            "track_stop_event": threading.Event(),
            "tracking_active": False,
            "stop_reason": "",
            "mode": start_mode,
            "auto_rebuy": False if actor_key == "ia" else False,
            "buy_now_button": (buy_now_button if actor_key != "ia" else None),
            "sell_now_button": (sell_now_button if actor_key != "ia" else None),
            "auto_rebuy_button": (auto_rebuy_button if actor_key != "ia" else None),
            "auto_rebuy_capital_var": auto_rebuy_capital_var,
            "rebuy_capital_var": rebuy_capital_var,
            "rebuy_capital_cached": float(rebuy_capital_value),
            "target_profit_var": target_profit_var,
            "target_profit_cached": float(target_profit_value),
            "current_buy_capital_var": current_buy_capital_var,
            "last_known_limit_price": 0.0,
            "enter_now_in_flight": False,
            "sell_now_in_flight": False,
            "auto_rebuy_success_count": 0,
            "rebuy_last_http_status": 0,
            "rebuy_retry_after_seconds": 0.0,
            "rebuy_cooldown_until": 0.0,
            "last_limit_repair_ts": 0.0,
            "limit_repair_backoff_seconds": 0.0,
            "limit_repair_cooldown_until": 0.0,
            "last_limit_repair_http_status": 0,
            "last_limit_repair_http_status_ts": 0.0,
            "last_limit_lookup_http_status": 0,
            "last_limit_lookup_http_status_ts": 0.0,
            "monitor_http_backoff_seconds": 0.0,
            "monitor_http_cooldown_until": 0.0,
            "last_monitor_http_status": 0,
            "last_monitor_http_status_ts": 0.0,
            "last_shared_watch_cooldown_log_ts": 0.0,
            "last_live_pnl_snapshot": None,
            "last_live_pnl_log_ts": 0.0,
            "last_watch_log_message": "",
            "last_watch_log_ts": 0.0,
        }
        if start_mode != "waiting":
            context["stop_event"].set()
        with self._watch_lock:
            self._watch_tabs[watch_id] = context

        def _on_target_change(*_args: Any) -> None:
            try:
                parsed = float(target_profit_var.get().strip())
            except (TypeError, ValueError, RuntimeError, tk.TclError):
                return
            with self._watch_lock:
                current = self._watch_tabs.get(watch_id)
                if current is not None and parsed > 0:
                    current["target_profit_cached"] = parsed

        def _on_rebuy_capital_change(*_args: Any) -> None:
            try:
                parsed = float(rebuy_capital_var.get().strip())
            except (TypeError, ValueError, RuntimeError, tk.TclError):
                return
            with self._watch_lock:
                current = self._watch_tabs.get(watch_id)
                if current is not None and parsed >= 0:
                    current["rebuy_capital_cached"] = parsed

        target_profit_var.trace_add("write", _on_target_change)
        rebuy_capital_var.trace_add("write", _on_rebuy_capital_change)
        self._watch_log(watch_id, f"Pestaña creada ({actor_label}). Esperando señal para {symbol}...")
        self._set_watch_buy_now_enabled(watch_id, enabled=(start_mode == "waiting"))
        self._set_watch_sell_now_enabled(watch_id, enabled=False)
        self._refresh_auto_rebuy_capital_display(watch_id)
        self._save_watch_tabs_state()
        return watch_id

    def _request_close_selected_watch_tab(self) -> None:
        selected_tab = self.log_notebook.select()
        if not selected_tab:
            self._show_success("No hay pestaña seleccionada.", False)
            return

        for watch_id, context in list(self._watch_tabs.items()):
            if str(context.get("frame")) == str(selected_tab):
                self._request_close_watch_tab(watch_id)
                return

        self._show_success("La pestaña activa no es una busqueda en ejecucion.", False)

    def _request_close_watch_tab(self, watch_id: str) -> None:
        threading.Thread(target=self._close_watch_tab_with_liquidation, args=(watch_id,), daemon=True).start()

    def _close_watch_tab_with_liquidation(self, watch_id: str) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
        if context is None:
            return
        runtime = self._runtime_for_watch(watch_id)
        broker = runtime["broker"]
        position_manager = runtime["position_manager"]

        symbol = str(context.get("symbol", "")).upper()
        try:
            position = self._find_open_position_by_symbol(symbol, broker=broker)
        except Exception as ex:
            self._watch_log(watch_id, f"No se pudo verificar posicion en broker ({ex}). Cerrando pestaña por solicitud.")
            self.root.after(
                0,
                self._finalize_close_watch_tab,
                watch_id,
                "manual_close",
                "Pestaña cerrada (verificacion broker no disponible)",
            )
            return

        # Requested behavior: if there is no open position, close the tab immediately.
        if position is None:
            self._watch_log(watch_id, f"No hay posicion abierta para {symbol}. Cerrando pestaña.")
            self.root.after(
                0,
                self._finalize_close_watch_tab,
                watch_id,
                "manual_close",
                "Pestaña cerrada (sin posicion abierta)",
            )
            return

        self.root.after(0, self.status_var.set, f"Cerrando posicion {symbol} antes de cerrar pestaña...")
        try:
            position_manager.manual_sell(symbol)
        except Exception as ex:
            # If the position disappeared meanwhile, allow closing the tab.
            if self._find_open_position_by_symbol(symbol, broker=broker) is None:
                self._watch_log(watch_id, f"{symbol} ya estaba cerrada en broker. Cerrando pestaña.")
                self.root.after(
                    0,
                    self._finalize_close_watch_tab,
                    watch_id,
                    "manual_close",
                    "Pestaña cerrada (sin posicion abierta)",
                )
                return
            self._watch_log(watch_id, f"No se pudo cerrar posicion {symbol}: {ex}")
            self.root.after(
                0,
                self._show_error,
                f"No se pudo cerrar posicion {symbol}. La pestaña sigue abierta. Error: {ex}",
                False,
            )
            return

        if not self._wait_position_closed(symbol=symbol, max_attempts=8, sleep_seconds=0.5, broker=broker):
            self._watch_log(watch_id, f"Venta enviada para {symbol}, pero no confirmada aun en broker.")
            self.root.after(
                0,
                self._show_error,
                (
                    f"Venta de {symbol} no confirmada aun en broker. "
                    "No cierro la pestaña para evitar perder monitoreo."
                ),
                False,
            )
            return

        self._watch_log(watch_id, f"Venta confirmada en broker para {symbol}. Cerrando pestaña.")

        self.root.after(0, self._finalize_close_watch_tab, watch_id, "manual_close", "Pestaña cerrada (venta confirmada)")

    def _wait_position_closed(
        self,
        symbol: str,
        max_attempts: int = 8,
        sleep_seconds: float = 0.5,
        broker: Any | None = None,
    ) -> bool:
        for _ in range(max_attempts):
            if self._find_open_position_by_symbol(symbol, broker=broker) is None:
                return True
            time.sleep(sleep_seconds)
        return False

    def _finalize_close_watch_tab(
        self,
        watch_id: str,
        stop_reason: str,
        status_message: str,
        persist_state: bool = True,
    ) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
        if context is None:
            return

        account_name = str(context.get("account", "")).strip()

        stop_event = context.get("stop_event")
        if stop_event is not None:
            self._set_watch_stop_reason(watch_id, stop_reason)
            stop_event.set()

        track_stop_event = context.get("track_stop_event")
        if track_stop_event is not None:
            track_stop_event.set()

        frame = context.get("frame")
        if frame is not None:
            try:
                self.log_notebook.forget(frame)
            except tk.TclError:
                pass
            frame.destroy()

        with self._watch_lock:
            self._watch_tabs.pop(watch_id, None)
        if persist_state:
            replace_accounts = {account_name} if account_name else None
            self._save_watch_tabs_state(replace_accounts=replace_accounts)
        self.status_var.set(status_message)

    def _cancel_all_watch_tabs(self) -> None:
        with self._watch_lock:
            watch_ids = list(self._watch_tabs.keys())
        for watch_id in watch_ids:
            self.root.after(
                0,
                self._finalize_close_watch_tab,
                watch_id,
                "account_switch",
                "Pestañas cerradas por cambio de cuenta",
                False,
            )

    def _watch_should_stop(self, watch_id: str) -> bool:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
        if context is None:
            return True
        stop_event = context.get("stop_event")
        return bool(stop_event is not None and stop_event.is_set())

    def _set_watch_stop_reason(self, watch_id: str, reason: str) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
            if context is not None:
                context["stop_reason"] = reason

    def _get_watch_stop_reason(self, watch_id: str) -> str:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
            if context is None:
                return ""
            return str(context.get("stop_reason", ""))

    def _mark_watch_finished(self, watch_id: str) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
        if context is None:
            return
        context["active"] = False
        self._refresh_watch_buy_now_state(watch_id)
        self._save_watch_tabs_state()

    def _set_watch_buy_now_enabled(self, watch_id: str, enabled: bool) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
        if context is None:
            return
        button = context.get("buy_now_button")
        if button is None:
            return
        self._safe_after(0, lambda: button.configure(state=("normal" if enabled else "disabled")))

    def _set_watch_sell_now_enabled(self, watch_id: str, enabled: bool) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
        if context is None:
            return
        button = context.get("sell_now_button")
        if button is None:
            return
        self._safe_after(0, lambda: button.configure(state=("normal" if enabled else "disabled")))

    def _refresh_watch_buy_now_state(self, watch_id: str) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
        if context is None:
            return

        symbol = str(context.get("symbol", "")).upper()
        runtime = context.get("runtime")
        if not symbol or runtime is None:
            self._set_watch_buy_now_enabled(watch_id, enabled=True)
            return

        broker = runtime.get("broker")
        position_manager = runtime.get("position_manager")
        if broker is None or position_manager is None:
            self._set_watch_buy_now_enabled(watch_id, enabled=True)
            return

        try:
            has_position = self._find_open_position_by_symbol(symbol, broker=broker) is not None
        except Exception:
            has_position = False

        try:
            has_pending_buy = bool(position_manager._has_pending_buy_order(symbol))
        except Exception:
            has_pending_buy = False

        self._set_watch_buy_now_enabled(watch_id, enabled=not (has_position or has_pending_buy))

    def _refresh_watch_sell_now_state(self, watch_id: str) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
        if context is None:
            return

        if bool(context.get("sell_now_in_flight", False)):
            self._set_watch_sell_now_enabled(watch_id, enabled=False)
            return

        symbol = str(context.get("symbol", "")).upper()
        runtime = context.get("runtime")
        if not symbol or runtime is None:
            self._set_watch_sell_now_enabled(watch_id, enabled=False)
            return

        broker = runtime.get("broker")
        if broker is None:
            self._set_watch_sell_now_enabled(watch_id, enabled=False)
            return

        try:
            has_position = self._find_open_position_by_symbol(symbol, broker=broker) is not None
        except Exception:
            has_position = False

        self._set_watch_sell_now_enabled(watch_id, enabled=has_position)

    def _pause_watch_after_position_closed(self, watch_id: str, symbol: str, status_message: str) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
            if context is None:
                return

            stop_event = context.get("stop_event")
            if stop_event is None:
                stop_event = threading.Event()
                context["stop_event"] = stop_event
            stop_event.set()
            context["active"] = False
            context["mode"] = "waiting"
            context["stop_reason"] = "paused_after_close"

        self._save_watch_tabs_state()
        self._refresh_watch_buy_now_state(watch_id)
        self._refresh_watch_sell_now_state(watch_id)
        self._watch_log(watch_id, f"{symbol} cerrado. Recompra OFF: pestaña en pausa, sin compras automáticas.")
        self._set_watch_status(watch_id, status_message)

    def _watch_log(self, watch_id: str, message: str) -> None:
        now = time.monotonic()
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
            if context is None:
                return
            if str(message).startswith("PnL en vivo "):
                last_message = str(context.get("last_watch_log_message", ""))
                last_ts = float(context.get("last_watch_log_ts", 0.0) or 0.0)
                if last_message == message and (now - last_ts) < 180.0:
                    return
            context["last_watch_log_message"] = message
            context["last_watch_log_ts"] = now
        timestamp = datetime.now().strftime("%H:%M:%S")
        self._safe_after(0, self._append_watch_log, watch_id, f"[{timestamp}] {message}")

    def _append_watch_log(self, watch_id: str, line: str) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
        if context is None:
            return
        text_widget = context.get("text")
        if text_widget is None:
            return
        text_widget.configure(state="normal")
        text_widget.insert(tk.END, line + "\n")
        try:
            line_count = int(float(text_widget.index("end-1c").split(".")[0]))
            extra = line_count - self._ui_max_log_lines
            if extra > 0:
                text_widget.delete("1.0", f"{extra + 1}.0")
        except Exception:
            pass
        text_widget.see(tk.END)
        text_widget.configure(state="disabled")

    def _set_watch_status(self, watch_id: str, message: str) -> None:
        self._safe_after(0, self._apply_watch_status, watch_id, message)

    def _apply_watch_status(self, watch_id: str, message: str) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
        if context is None:
            return
        status_var = context.get("status_var")
        status_detail_var = context.get("status_detail_var")
        main_text = str(message or "")
        detail_text = ""

        if main_text.startswith("PnL en vivo "):
            parts = main_text.split(" | ")
            if len(parts) > 4:
                main_text = " | ".join(parts[:4])
                detail_text = " | ".join(parts[4:])
        elif len(main_text) > 110 and " | " in main_text:
            parts = main_text.split(" | ")
            midpoint = max(1, len(parts) // 2)
            main_text = " | ".join(parts[:midpoint])
            detail_text = " | ".join(parts[midpoint:])

        if status_var is not None:
            status_var.set(main_text)
        if status_detail_var is not None:
            status_detail_var.set(detail_text)

    def _select_watch_tab(self, watch_id: str) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
        if context is None:
            return
        frame = context.get("frame")
        if frame is not None:
            self.log_notebook.select(frame)

    def _start_position_tracking(self, watch_id: str, symbol: str, entry_price_hint: float) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
            if context is None:
                return
            if context.get("tracking_active"):
                return
            context["tracking_active"] = True
            context["mode"] = "tracking"
            context["last_live_pnl_snapshot"] = None
            context["last_live_pnl_log_ts"] = 0.0

        self._set_watch_buy_now_enabled(watch_id, enabled=False)
        self._save_watch_tabs_state()

        threading.Thread(
            target=self._position_tracking_loop,
            args=(watch_id, symbol, entry_price_hint),
            daemon=True,
        ).start()

    def _should_emit_live_pnl_update(
        self,
        watch_id: str,
        *,
        side: str,
        leverage: float,
        state: str,
        current_price: float,
        pnl: float,
        pnl_pct: float,
        qty: float,
        avg_entry_price: float,
        target_price: float,
    ) -> bool:
        now = time.monotonic()
        heartbeat_seconds = 180.0
        snapshot = {
            "side": str(side or "long").lower(),
            "leverage": round(float(leverage or 1.0), 2),
            "state": state,
            "current_price": round(current_price, 4),
            "pnl": round(pnl, 4),
            "pnl_pct": round(pnl_pct, 2),
            "qty": round(qty, 4),
            "entry": round(avg_entry_price, 4),
            "target": round(target_price, 4),
        }

        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
            if context is None:
                return False
            previous = context.get("last_live_pnl_snapshot")
            last_ts = float(context.get("last_live_pnl_log_ts", 0.0) or 0.0)
            should_emit = previous != snapshot or (now - last_ts) >= heartbeat_seconds
            if should_emit:
                context["last_live_pnl_snapshot"] = snapshot
                context["last_live_pnl_log_ts"] = now

        return should_emit

    def _resume_waiting_entry(self, watch_id: str, symbol: str) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
            if context is None:
                return

            stop_event = context.get("stop_event")
            if stop_event is None:
                stop_event = threading.Event()
                context["stop_event"] = stop_event

            already_waiting = bool(context.get("active", False)) and not bool(stop_event.is_set())
            if already_waiting:
                self._set_watch_status(watch_id, f"Esperando señal de entrada para {symbol}...")
                self._watch_log(watch_id, f"{symbol} ya esta en modo espera de señal.")
                return

            # Ensure entry loop can run again on this tab.
            stop_event.clear()
            context["active"] = True
            context["mode"] = "waiting"
            context["stop_reason"] = ""
            asset_type = str(context.get("asset_type", "stock"))

        self._save_watch_tabs_state()
        self._refresh_watch_buy_now_state(watch_id)
        self._refresh_auto_rebuy_capital_display(watch_id)
        self._watch_log(watch_id, f"Posicion cerrada en {symbol}. Reanudando busqueda automatica de entrada. {self._planned_rebuy_capital_text(watch_id)}")
        self._set_watch_status(watch_id, f"Esperando nueva entrada para {symbol}...")
        threading.Thread(
            target=self._entry_watch_loop,
            args=(watch_id, symbol, asset_type),
            daemon=True,
        ).start()

    def _position_tracking_loop(self, watch_id: str, symbol: str, entry_price_hint: float) -> None:
        had_open_position = False
        reentry_started = False
        fast_poll_seconds = 1.0
        try:
            while True:
                with self._watch_lock:
                    context = self._watch_tabs.get(watch_id)
                if context is None:
                    return

                account_name = str((context or {}).get("account", "") or "")
                now_monotonic = time.monotonic()
                shared_cooldown_until = float(self._account_watch_http_cooldown_until.get(account_name, 0.0) or 0.0)
                if account_name and shared_cooldown_until > now_monotonic:
                    remaining = shared_cooldown_until - now_monotonic
                    with self._watch_lock:
                        current_context = self._watch_tabs.get(watch_id)
                        if current_context is not None:
                            last_shared_log_ts = float(current_context.get("last_shared_watch_cooldown_log_ts", 0.0) or 0.0)
                            should_log_shared = (now_monotonic - last_shared_log_ts) >= 15.0
                            current_context["last_shared_watch_cooldown_log_ts"] = now_monotonic
                        else:
                            should_log_shared = True
                    if should_log_shared:
                        self._watch_log(
                            watch_id,
                            f"Cuenta {account_name} en cooldown compartido por rate limit. Esperando {remaining:.1f}s.",
                        )
                    self._set_watch_status(watch_id, f"Cuenta en cooldown por rate limit. Reintentando en {remaining:.1f}s...")
                    time.sleep(min(max(remaining, 0.5), 5.0))
                    continue

                track_stop_event = context.get("track_stop_event")
                if track_stop_event is not None and track_stop_event.is_set():
                    return

                try:
                    runtime = self._runtime_for_watch(watch_id)
                    broker = runtime["broker"]
                    market_data = runtime["market_data"]
                    position_manager = runtime["position_manager"]
                    position = self._find_open_position_by_symbol(symbol, broker=broker)
                    with self._watch_lock:
                        current_context = self._watch_tabs.get(watch_id)
                        if current_context is not None:
                            current_context["monitor_http_backoff_seconds"] = 0.0
                            current_context["monitor_http_cooldown_until"] = 0.0
                    self._mark_network_recovered()
                except requests.exceptions.HTTPError as ex:
                    status = ex.response.status_code if ex.response is not None else "N/A"
                    retry_after_seconds = 0.0
                    if ex.response is not None:
                        raw_retry_after = str(ex.response.headers.get("Retry-After", "")).strip()
                        if raw_retry_after:
                            try:
                                retry_after_seconds = max(float(raw_retry_after), 0.0)
                            except ValueError:
                                retry_after_seconds = 0.0

                    with self._watch_lock:
                        current_context = self._watch_tabs.get(watch_id)
                        current_backoff = float((current_context or {}).get("monitor_http_backoff_seconds", 0.0) or 0.0)

                    if status == 429:
                        wait_seconds = max(retry_after_seconds, current_backoff * 2.0 if current_backoff > 0 else 2.0)
                        wait_seconds = min(wait_seconds, 45.0)
                        status_message = f"Rate limit del broker (HTTP 429). Reintentando en {wait_seconds:.1f}s..."
                    elif status in {503, 504}:
                        wait_seconds = max(retry_after_seconds, current_backoff * 2.0 if current_backoff > 0 else 5.0)
                        wait_seconds = min(wait_seconds, 60.0)
                        status_message = f"Broker/API temporalmente no disponible (HTTP {status}). Reintentando en {wait_seconds:.1f}s..."
                    else:
                        wait_seconds = max(retry_after_seconds, 8.0)
                        wait_seconds = min(wait_seconds, 30.0)
                        status_message = f"Broker/API no disponible (HTTP {status}). Reintentando en {wait_seconds:.1f}s..."

                    now_monotonic = time.monotonic()
                    if account_name:
                        current_account_cooldown_until = float(self._account_watch_http_cooldown_until.get(account_name, 0.0) or 0.0)
                        self._account_watch_http_cooldown_until[account_name] = max(
                            current_account_cooldown_until,
                            now_monotonic + float(wait_seconds),
                        )
                    with self._watch_lock:
                        current_context = self._watch_tabs.get(watch_id)
                        if current_context is not None:
                            current_context["monitor_http_backoff_seconds"] = float(wait_seconds)
                            current_context["monitor_http_cooldown_until"] = now_monotonic + float(wait_seconds)
                            last_status_code = int(current_context.get("last_monitor_http_status", 0) or 0)
                            last_status_ts = float(current_context.get("last_monitor_http_status_ts", 0.0) or 0.0)
                            should_log_status = (last_status_code != int(status) if str(status).isdigit() else True) or ((now_monotonic - last_status_ts) >= 15.0)
                            current_context["last_monitor_http_status"] = int(status) if str(status).isdigit() else 0
                            current_context["last_monitor_http_status_ts"] = now_monotonic
                        else:
                            should_log_status = True

                    self._mark_network_degraded(status_message)
                    if should_log_status:
                        self._watch_log(
                            watch_id,
                            f"Monitoreo degradado ({symbol}) HTTP {status}. Reintentando en {wait_seconds:.1f}s.",
                        )
                    self._set_watch_status(watch_id, status_message)
                    time.sleep(wait_seconds)
                    continue
                except requests.exceptions.RequestException as ex:
                    wait_seconds = max(settings.position_monitor_interval_seconds, 5)
                    self._mark_network_degraded(
                        f"Internet caido durante monitoreo de posicion ({symbol}). Reintentando en {wait_seconds}s..."
                    )
                    if self._should_emit_transient_log(f"watch-net-{watch_id}", min_interval_seconds=90.0):
                        self._watch_log(
                            watch_id,
                            f"Sin conexion en monitoreo ({ex.__class__.__name__}). Reintentando en {wait_seconds}s.",
                        )
                    self._set_watch_status(watch_id, f"Sin conexion. Reintentando en {wait_seconds}s...")
                    time.sleep(wait_seconds)
                    continue

                if position is None:
                    if had_open_position:
                        actor = str((context or {}).get("bot_actor", "normal") or "normal").strip().lower()
                        self._watch_log(
                            watch_id,
                            f"Posicion {symbol} ya no esta abierta. Monitoreo sigue activo hasta cierre manual.",
                        )
                        if not reentry_started:
                            reentry_started = True
                            if actor == "ia":
                                self._watch_log(
                                    watch_id,
                                    f"Posición IA cerrada para {symbol}. Reanudando análisis de nueva entrada.",
                                )
                                self._resume_waiting_entry(watch_id=watch_id, symbol=symbol)
                                return
                            if self._is_auto_rebuy_enabled(watch_id):
                                self._watch_log(
                                    watch_id,
                                    f"Recompra inmediata activa para {symbol}. Intentando compra market inmediata.",
                                )
                                self._set_watch_status(watch_id, f"Recomprando {symbol} en market...")
                                rebuy_ok = self._attempt_auto_rebuy_sequence(watch_id=watch_id, symbol=symbol)
                                if rebuy_ok:
                                    # Keep this tracker alive across chained rebuys.
                                    had_open_position = True
                                    reentry_started = False
                                    time.sleep(fast_poll_seconds)
                                    continue
                            else:
                                self._pause_watch_after_position_closed(
                                    watch_id=watch_id,
                                    symbol=symbol,
                                    status_message=f"Sin posición abierta en {symbol}. Recompra OFF: pestaña en pausa.",
                                )
                            return
                    self._refresh_watch_buy_now_state(watch_id)
                    self._refresh_watch_sell_now_state(watch_id)
                    self._set_watch_status(watch_id, f"Sin posicion abierta en {symbol}.")
                    had_open_position = False
                    time.sleep(fast_poll_seconds)
                    continue

                qty = float(position.get("qty", 0.0) or 0.0)
                avg_entry_price = float(position.get("avg_entry_price", 0.0) or 0.0)
                if avg_entry_price <= 0:
                    avg_entry_price = entry_price_hint
                side = str(position.get("side", "long") or "long").lower().strip()
                if side not in {"long", "short"}:
                    side = "long"
                side_label = "SHORT" if side == "short" else "LONG"
                leverage = float(position.get("leverage", 1.0) or 1.0)
                provider = str(getattr(broker, "provider", "") or "").lower().strip()
                if provider == "binance" and leverage <= 1.0 and hasattr(broker, "get_binance_position_risk"):
                    try:
                        risk_row = broker.get_binance_position_risk(symbol)
                        risk_leverage = float((risk_row or {}).get("leverage", 0.0) or 0.0)
                        if risk_leverage > 0.0:
                            leverage = risk_leverage
                    except Exception:
                        pass
                if leverage <= 0:
                    leverage = 1.0
                current_price = float(market_data.get_last_price(symbol))
                if side == "short":
                    pnl = (avg_entry_price - current_price) * qty
                    pnl_pct = ((avg_entry_price / current_price) - 1.0) * 100.0 if avg_entry_price > 0 and current_price > 0 else 0.0
                else:
                    pnl = (current_price - avg_entry_price) * qty
                    pnl_pct = ((current_price / avg_entry_price) - 1.0) * 100.0 if avg_entry_price > 0 else 0.0
                state = "GANANDO" if pnl > 0 else "PERDIENDO" if pnl < 0 else "EQUILIBRIO"
                configured_target = self._watch_target_profit_value(watch_id, position_manager=position_manager)
                target_profit_per_share = float(position_manager.get_target_profit_per_share_for_symbol(symbol))
                target_price = avg_entry_price - target_profit_per_share if side == "short" else avg_entry_price + target_profit_per_share
                target_details_short, _ = self._format_watch_target_details(
                    configured_target=configured_target,
                    effective_target_per_share=target_profit_per_share,
                    qty=qty,
                )
                pending_sell_order = None
                order_lookup_failed = False
                try:
                    pending_sell_order = position_manager.get_pending_sell_order(symbol, suppress_errors=False)
                except requests.exceptions.HTTPError as order_ex:
                    status = order_ex.response.status_code if order_ex.response is not None else None
                    retry_after_seconds = 0.0
                    if order_ex.response is not None:
                        raw_retry_after = str(order_ex.response.headers.get("Retry-After", "")).strip()
                        if raw_retry_after:
                            try:
                                retry_after_seconds = max(float(raw_retry_after), 0.0)
                            except ValueError:
                                retry_after_seconds = 0.0

                    now_monotonic = time.monotonic()
                    with self._watch_lock:
                        current_context = self._watch_tabs.get(watch_id)
                        current_repair_backoff = float((current_context or {}).get("limit_repair_backoff_seconds", 0.0) or 0.0)

                    if status == 429:
                        block_seconds = max(retry_after_seconds, current_repair_backoff * 2.0 if current_repair_backoff > 0 else 4.0)
                        block_seconds = min(block_seconds, 90.0)
                    elif status == 403:
                        block_seconds = max(retry_after_seconds, 180.0)
                    elif status in {503, 504}:
                        block_seconds = max(retry_after_seconds, current_repair_backoff * 2.0 if current_repair_backoff > 0 else 6.0)
                        block_seconds = min(block_seconds, 90.0)
                    else:
                        block_seconds = max(retry_after_seconds, 8.0)
                        block_seconds = min(block_seconds, 60.0)

                    with self._watch_lock:
                        current_context = self._watch_tabs.get(watch_id)
                        if current_context is not None:
                            current_context["limit_repair_backoff_seconds"] = float(block_seconds)
                            current_context["limit_repair_cooldown_until"] = now_monotonic + float(block_seconds)
                            last_lookup_http_status = int(current_context.get("last_limit_lookup_http_status", 0) or 0)
                            last_lookup_http_status_ts = float(current_context.get("last_limit_lookup_http_status_ts", 0.0) or 0.0)
                            should_log_lookup = (last_lookup_http_status != int(status) if status is not None else True) or ((now_monotonic - last_lookup_http_status_ts) >= 20.0)
                            current_context["last_limit_lookup_http_status"] = int(status) if status is not None else 0
                            current_context["last_limit_lookup_http_status_ts"] = now_monotonic
                        else:
                            should_log_lookup = True

                    if account_name:
                        current_account_cooldown_until = float(self._account_watch_http_cooldown_until.get(account_name, 0.0) or 0.0)
                        self._account_watch_http_cooldown_until[account_name] = max(
                            current_account_cooldown_until,
                            now_monotonic + float(block_seconds),
                        )

                    if should_log_lookup:
                        self._watch_log(
                            watch_id,
                            f"Consulta de ordenes limitada para {symbol} (HTTP {status or 'N/A'}). Repair pausado {block_seconds:.1f}s.",
                        )
                    order_lookup_failed = True
                except requests.exceptions.RequestException as order_ex:
                    self._watch_log(
                        watch_id,
                        f"No se pudo consultar ordenes abiertas para {symbol}: {order_ex.__class__.__name__}",
                    )
                    order_lookup_failed = True
                effective_limit = float((pending_sell_order or {}).get("limit_price", 0.0) or 0.0)
                if effective_limit <= 0:
                    now_monotonic = time.monotonic()
                    with self._watch_lock:
                        current_context = self._watch_tabs.get(watch_id)
                        if current_context is not None:
                            effective_limit = float(current_context.get("last_known_limit_price", 0.0) or 0.0)
                            last_repair_ts = float(current_context.get("last_limit_repair_ts", 0.0) or 0.0)
                            repair_cooldown_until = float(current_context.get("limit_repair_cooldown_until", 0.0) or 0.0)
                        else:
                            last_repair_ts = 0.0
                            repair_cooldown_until = 0.0

                    if (
                        not order_lookup_failed
                        and
                        now_monotonic >= repair_cooldown_until
                        and (now_monotonic - last_repair_ts) >= 6.0
                        and qty > 0
                        and avg_entry_price > 0
                    ):
                        with self._watch_lock:
                            current_context = self._watch_tabs.get(watch_id)
                            if current_context is not None:
                                current_context["last_limit_repair_ts"] = now_monotonic
                        try:
                            repair_result = position_manager._place_immediate_target_exit(
                                symbol=symbol,
                                qty=qty,
                                avg_entry_price=avg_entry_price,
                                current_price=current_price,
                                reason_sell="repair_missing_limit_from_tracking",
                                target_profit_per_share=configured_target,
                            )
                            repaired_limit = float((repair_result or {}).get("limit_price", 0.0) or 0.0)
                            if repaired_limit > 0:
                                effective_limit = repaired_limit
                                with self._watch_lock:
                                    current_context = self._watch_tabs.get(watch_id)
                                    if current_context is not None:
                                        current_context["last_known_limit_price"] = repaired_limit
                                        current_context["limit_repair_backoff_seconds"] = 0.0
                                        current_context["limit_repair_cooldown_until"] = 0.0
                                self._watch_log(watch_id, f"Limit de salida reparado para {symbol}: {repaired_limit:.4f}")
                        except requests.exceptions.HTTPError as repair_ex:
                            status = repair_ex.response.status_code if repair_ex.response is not None else None
                            retry_after_seconds = 0.0
                            if repair_ex.response is not None:
                                raw_retry_after = str(repair_ex.response.headers.get("Retry-After", "")).strip()
                                if raw_retry_after:
                                    try:
                                        retry_after_seconds = max(float(raw_retry_after), 0.0)
                                    except ValueError:
                                        retry_after_seconds = 0.0

                            with self._watch_lock:
                                current_context = self._watch_tabs.get(watch_id)
                                current_backoff = float((current_context or {}).get("limit_repair_backoff_seconds", 0.0) or 0.0)

                            if status == 429:
                                next_backoff = max(retry_after_seconds, current_backoff * 2.0 if current_backoff > 0 else 4.0)
                                next_backoff = min(next_backoff, 90.0)
                            elif status in {503, 504}:
                                next_backoff = max(retry_after_seconds, current_backoff * 2.0 if current_backoff > 0 else 6.0)
                                next_backoff = min(next_backoff, 90.0)
                            elif status == 403:
                                next_backoff = max(retry_after_seconds, 180.0)
                            else:
                                next_backoff = max(retry_after_seconds, 8.0)
                                next_backoff = min(next_backoff, 60.0)

                            cooldown_until = time.monotonic() + float(next_backoff)
                            with self._watch_lock:
                                current_context = self._watch_tabs.get(watch_id)
                                if current_context is not None:
                                    current_context["limit_repair_backoff_seconds"] = float(next_backoff)
                                    current_context["limit_repair_cooldown_until"] = float(cooldown_until)
                                    last_repair_http_status = int(current_context.get("last_limit_repair_http_status", 0) or 0)
                                    last_repair_http_status_ts = float(current_context.get("last_limit_repair_http_status_ts", 0.0) or 0.0)
                                    should_log_repair_status = (last_repair_http_status != int(status) if status is not None else True) or ((time.monotonic() - last_repair_http_status_ts) >= 20.0)
                                    current_context["last_limit_repair_http_status"] = int(status) if status is not None else 0
                                    current_context["last_limit_repair_http_status_ts"] = time.monotonic()
                                else:
                                    should_log_repair_status = True

                            if should_log_repair_status:
                                self._watch_log(
                                    watch_id,
                                    f"Repair limit en cooldown para {symbol} por HTTP {status or 'N/A'}: esperando {next_backoff:.1f}s.",
                                )
                        except Exception as repair_ex:
                            if self._should_emit_transient_log(f"watch-repair-{watch_id}", min_interval_seconds=120.0):
                                self._watch_log(watch_id, f"No se pudo reparar limit para {symbol}: {repair_ex}")
                elif effective_limit > 0:
                    with self._watch_lock:
                        current_context = self._watch_tabs.get(watch_id)
                        if current_context is not None:
                            current_context["last_known_limit_price"] = effective_limit

                line = (
                    f"PnL en vivo {symbol} | side={side_label} | lev={leverage:.0f}x | entry={avg_entry_price:.4f} | current={current_price:.4f} | "
                    f"target_teorico={target_price:.4f} ({target_details_short}) | "
                    f"limit_real={(f'{effective_limit:.4f}' if effective_limit > 0 else 'N/A')} | "
                    f"qty={qty:.4f} | pnl={pnl:.4f} ({pnl_pct:.2f}%) | estado={state}"
                )
                if self._should_emit_live_pnl_update(
                    watch_id,
                    side=side,
                    leverage=leverage,
                    state=state,
                    current_price=current_price,
                    pnl=pnl,
                    pnl_pct=pnl_pct,
                    qty=qty,
                    avg_entry_price=avg_entry_price,
                    target_price=target_price,
                ):
                    self._watch_log(watch_id, line)
                    self._set_watch_status(watch_id, line)
                self._refresh_watch_buy_now_state(watch_id)
                self._refresh_watch_sell_now_state(watch_id)
                had_open_position = True
                reentry_started = False
                time.sleep(fast_poll_seconds)
        finally:
            with self._watch_lock:
                context = self._watch_tabs.get(watch_id)
                if context is not None:
                    context["tracking_active"] = False

    def _save_watch_tabs_state(self, replace_accounts: set[str] | None = None) -> None:
        with self._watch_lock:
            payload = []
            seen_keys: set[tuple[str, str]] = set()
            for context in self._watch_tabs.values():
                symbol = str(context.get("symbol", "")).strip().upper()
                account = str(context.get("account", "")).strip()
                if not symbol or not account:
                    continue
                unique_key = (self._symbol_key(symbol), account)
                if unique_key in seen_keys:
                    continue
                seen_keys.add(unique_key)
                payload.append(
                    {
                        "symbol": symbol,
                        "asset_type": str(context.get("asset_type", "stock")),
                        "account": account,
                        "bot_actor": str(context.get("bot_actor", "normal") or "normal"),
                        "mode": str(context.get("mode", "waiting")),
                        "auto_rebuy": bool(context.get("auto_rebuy", False)),
                        "rebuy_capital": str(float(context.get("rebuy_capital_cached", settings.default_trade_capital) or settings.default_trade_capital)),
                        "target_profit_per_share": str(float(context.get("target_profit_cached", self._safe_target_profit_value()) or self._safe_target_profit_value())),
                    }
                )

        if replace_accounts is None:
            replace_accounts = {
                str(item.get("account", "")).strip()
                for item in payload
                if str(item.get("account", "")).strip()
            }

        existing_payload = []
        if self._watch_state_path.exists():
            try:
                raw_existing = json.loads(self._watch_state_path.read_text(encoding="utf-8"))
                if isinstance(raw_existing, list):
                    existing_payload = [item for item in raw_existing if isinstance(item, dict)]
            except Exception:
                existing_payload = []

        merged_payload = []
        merged_keys: set[tuple[str, str]] = set()
        for item in existing_payload:
            account = str(item.get("account", "")).strip()
            if account in replace_accounts:
                continue
            symbol = str(item.get("symbol", "")).strip().upper()
            if not symbol or not account:
                continue
            unique_key = (self._symbol_key(symbol), account)
            if unique_key in merged_keys:
                continue
            merged_keys.add(unique_key)
            merged_payload.append(item)
        for item in payload:
            account = str(item.get("account", "")).strip()
            symbol = str(item.get("symbol", "")).strip().upper()
            unique_key = (self._symbol_key(symbol), account)
            if unique_key in merged_keys:
                continue
            merged_payload.append(item)

        self._watch_state_path.write_text(
            json.dumps(merged_payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    def _restore_watch_tabs(self) -> None:
        if not self._watch_state_path.exists():
            return

        try:
            raw = json.loads(self._watch_state_path.read_text(encoding="utf-8"))
        except Exception:
            return

        if not isinstance(raw, list):
            return

        restored = 0
        skipped = 0
        cleaned_payload: list[dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict):
                continue

            symbol = str(item.get("symbol", "")).strip().upper()
            asset_type = str(item.get("asset_type", "stock")).strip().lower()
            account = str(item.get("account", "")).strip()
            mode = str(item.get("mode", "waiting")).strip().lower()
            bot_actor = str(item.get("bot_actor", "normal") or "normal").strip().lower()
            auto_rebuy = bool(item.get("auto_rebuy", False))
            rebuy_capital = float(item.get("rebuy_capital", settings.default_trade_capital) or settings.default_trade_capital)
            target_profit_per_share = float(item.get("target_profit_per_share", self._safe_target_profit_value()) or self._safe_target_profit_value())

            if not symbol or not account:
                continue
            if mode not in {"waiting", "tracking"}:
                mode = "waiting"
            profile = self.account_profiles.get(account)
            if not profile:
                skipped += 1
                self.logger.warning(
                    "Pestana guardada ignorada para cuenta no configurada: %s (%s)",
                    account,
                    symbol,
                )
                continue

            endpoint = str(profile.get("endpoint", "")).strip()
            api_key = str(profile.get("key", "")).strip()
            api_secret = str(profile.get("secret", "")).strip()
            if not endpoint or not api_key or not api_secret:
                skipped += 1
                self.logger.warning(
                    "Pestana guardada ignorada para cuenta sin credenciales: %s (%s)",
                    account,
                    symbol,
                )
                continue

            if self._watch_tab_exists_for_symbol(symbol=symbol, account=account, bot_actor=bot_actor):
                cleaned_payload.append(
                    {
                        "symbol": symbol,
                        "asset_type": asset_type,
                        "account": account,
                        "bot_actor": bot_actor,
                        "mode": mode,
                        "auto_rebuy": (False if bot_actor == "ia" else auto_rebuy),
                        "rebuy_capital": rebuy_capital,
                        "target_profit_per_share": target_profit_per_share,
                    }
                )
                continue

            watch_id = self._create_watch_tab(
                symbol=symbol,
                asset_type=asset_type,
                start_mode=mode,
                account_name=account,
                rebuy_capital_value=rebuy_capital,
                target_profit_value=target_profit_per_share,
                bot_actor=bot_actor,
            )
            cleaned_payload.append(
                {
                    "symbol": symbol,
                    "asset_type": asset_type,
                    "account": account,
                    "bot_actor": bot_actor,
                    "mode": mode,
                    "auto_rebuy": (False if bot_actor == "ia" else auto_rebuy),
                    "rebuy_capital": rebuy_capital,
                    "target_profit_per_share": target_profit_per_share,
                }
            )
            with self._watch_lock:
                context = self._watch_tabs.get(watch_id)
                if context is not None:
                    context["auto_rebuy"] = (False if bot_actor == "ia" else auto_rebuy)
            self._apply_auto_rebuy_button_state(watch_id, (False if bot_actor == "ia" else auto_rebuy))

            has_open_position = False
            try:
                runtime = self._runtime_for_watch(watch_id)
                has_open_position = self._find_open_position_by_symbol(symbol, broker=runtime.get("broker")) is not None
            except Exception as ex:
                self.logger.warning("No se pudo verificar posicion abierta para pestana restaurada %s: %s", symbol, ex)

            restored += 1
            if mode == "waiting" and not has_open_position:
                with self._watch_lock:
                    context = self._watch_tabs.get(watch_id)
                    if context is not None:
                        context["active"] = False
                        stop_event = context.get("stop_event")
                        if stop_event is not None:
                            stop_event.set()
                self._watch_log(watch_id, "Pestaña restaurada en pausa para evitar compras automaticas.")
                self._set_watch_status(watch_id, "Entrada restaurada en pausa")
            else:
                if mode == "waiting" and has_open_position:
                    self._watch_log(watch_id, "Pestaña restaurada: posición abierta detectada. Activando monitoreo en vivo.")
                else:
                    self._watch_log(watch_id, "Pestaña restaurada tras reinicio. Reanudando monitoreo en vivo.")
                self._start_position_tracking(watch_id=watch_id, symbol=symbol, entry_price_hint=0.0)

        self._watch_state_path.write_text(
            json.dumps(cleaned_payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        if restored > 0:
            self.status_var.set(f"Se restauraron {restored} pestaña(s) de seguimiento")
        if skipped > 0 and restored <= 0:
            self.status_var.set(f"Se ignoraron {skipped} pestaña(s) guardadas sin cuenta valida")

    def _restore_open_positions_tabs(self) -> None:
        created = 0
        resumed = 0
        account_names = list((self.account_profiles or {}).keys())
        if not account_names:
            current = self.account_var.get().strip()
            if current:
                account_names = [current]

        for account_name in account_names:
            if not account_name:
                continue
            try:
                runtime = self._get_account_runtime(account_name)
                broker = runtime.get("broker")
                positions = broker.get_positions() if broker is not None else []
            except Exception as ex:
                self.logger.warning(
                    "No se pudieron recuperar posiciones abiertas para restaurar pestañas en %s: %s",
                    account_name,
                    ex,
                )
                continue

            for position in positions:
                symbol = str(position.get("symbol", "")).strip().upper()
                if not symbol:
                    continue
                existing_watch_id = self._find_watch_id_for_symbol(symbol=symbol, account=account_name)
                if existing_watch_id:
                    with self._watch_lock:
                        existing_context = self._watch_tabs.get(existing_watch_id)
                        existing_mode = str(existing_context.get("mode", "waiting")).strip().lower() if existing_context else "waiting"
                    if existing_mode != "tracking":
                        self._watch_log(existing_watch_id, "Posición abierta detectada tras reinicio. Reanudando monitoreo en vivo.")
                        self._start_position_tracking(watch_id=existing_watch_id, symbol=symbol, entry_price_hint=0.0)
                        resumed += 1
                    continue

                asset_type = self._asset_type_for_symbol(symbol)
                watch_id = self._create_watch_tab(
                    symbol=symbol,
                    asset_type=asset_type,
                    start_mode="tracking",
                    account_name=account_name,
                )
                self._watch_log(watch_id, "Pestaña creada automaticamente desde posicion abierta en broker.")
                self._start_position_tracking(watch_id=watch_id, symbol=symbol, entry_price_hint=0.0)
                created += 1

        if created > 0 or resumed > 0:
            self.status_var.set(
                f"Pestañas desde posiciones abiertas: nuevas={created} | monitoreo_reanudado={resumed}"
            )

    def _refresh_accounts_header_summary(self) -> None:
        account_names = list((self.account_profiles or {}).keys())
        if not account_names:
            current = self.account_var.get().strip()
            if current:
                account_names = [current]

        parts: list[str] = []
        for account_name in account_names:
            if not account_name:
                continue
            try:
                runtime = self._get_account_runtime(account_name)
                broker = runtime.get("broker")
                if broker is None:
                    continue
                account = broker.get_account()
                cash = self._safe_float(account.get("cash", 0.0))
                buying_power = self._safe_float(account.get("buying_power", 0.0))
                equity = self._safe_float(account.get("equity", 0.0))
                last_equity = self._safe_float(account.get("last_equity", 0.0))
                pnl_total = equity - last_equity
                parts.append(
                    f"{account_name} cash={cash:,.2f} bp={buying_power:,.2f} pnl={pnl_total:+,.2f}"
                )
            except Exception:
                parts.append(f"{account_name} sin datos")

        text = " | ".join(parts) if parts else "Sin cuentas activas"
        self._safe_after(0, self.accounts_overview_var.set, f"Cuentas -> {text}")

    @staticmethod
    def _safe_float(value: Any) -> float:
        try:
            return float(value or 0.0)
        except (TypeError, ValueError):
            return 0.0

    def _find_watch_id_for_symbol(self, symbol: str, account: str, bot_actor: str | None = None) -> str | None:
        target = self._symbol_key(symbol)
        actor_filter = str(bot_actor or "").strip().lower()
        with self._watch_lock:
            for watch_id, context in self._watch_tabs.items():
                current_symbol = self._symbol_key(str(context.get("symbol", "")))
                current_account = str(context.get("account", "")).strip()
                current_actor = str(context.get("bot_actor", "normal") or "normal").strip().lower()
                if current_symbol == target and current_account == account:
                    if actor_filter and current_actor != actor_filter:
                        continue
                    return str(watch_id)
        return None

    def _watch_tab_exists_for_symbol(self, symbol: str, account: str, bot_actor: str | None = None) -> bool:
        target = self._symbol_key(symbol)
        actor_filter = str(bot_actor or "").strip().lower()
        with self._watch_lock:
            for context in self._watch_tabs.values():
                current_symbol = self._symbol_key(str(context.get("symbol", "")))
                current_account = str(context.get("account", "")).strip()
                current_actor = str(context.get("bot_actor", "normal") or "normal").strip().lower()
                if current_symbol == target and current_account == account:
                    if actor_filter and current_actor != actor_filter:
                        continue
                    return True
        return False

    def _find_open_position_by_symbol(self, symbol: str, broker: Any | None = None) -> dict[str, Any] | None:
        target = self._symbol_key(symbol)
        active_broker = broker or self.broker
        try:
            positions = active_broker.get_positions(force_refresh=True)
        except TypeError:
            positions = active_broker.get_positions()
        for position in positions:
            current = self._symbol_key(str(position.get("symbol", "")))
            if current == target:
                return position
        return None

    @staticmethod
    def _symbol_key(symbol: str) -> str:
        return str(symbol or "").upper().replace(" ", "").replace("/", "")

    def _view_orders(self) -> None:
        # Show actionable orders first: open orders are the ones that can still be cancelled/managed.
        open_orders = self.order_manager.review_orders(status="open", limit=200)
        combined_open: dict[str, dict[str, Any]] = {}
        for order in open_orders:
            order_id = str(order.get("id", "")).strip()
            if not order_id:
                continue
            combined_open[order_id] = order

        pending_orders = list(combined_open.values())

        # Optional fallback for rare API inconsistencies: include recent non-terminal orders only.
        hidden_old = 0
        terminal_statuses = {"filled", "canceled", "rejected", "expired", "replaced"}
        try:
            open_ids = set(combined_open.keys())
            for order in self.order_manager.review_orders(status="all", limit=200):
                order_id = str(order.get("id", "")).strip()
                if not order_id or order_id in open_ids:
                    continue
                status = str(order.get("status", "")).lower().strip()
                if status not in terminal_statuses:
                    hidden_old += 1
        except Exception:
            hidden_old = 0

        if not pending_orders:
            combined_all: dict[str, dict[str, Any]] = {}
            for order in self.order_manager.review_orders(status="all", limit=200):
                order_id = str(order.get("id", "")).strip()
                if not order_id:
                    continue
                combined_all[order_id] = order
            pending_orders = [
                order
                for order in combined_all.values()
                if str(order.get("status", "")).lower().strip() not in terminal_statuses
            ]

        positions_by_symbol: dict[str, float] = {}
        try:
            for position in self.broker.get_positions():
                symbol = self._symbol_key(str(position.get("symbol", "")))
                if not symbol:
                    continue
                positions_by_symbol[symbol] = float(position.get("avg_entry_price", 0.0) or 0.0)
        except Exception:
            positions_by_symbol = {}

        trimmed = [
            {
                "id": order.get("id"),
                "symbol": order.get("symbol"),
                "asset_type": self._asset_type_for_symbol(str(order.get("symbol", ""))),
                "type": order.get("type"),
                "side": order.get("side"),
                "qty": order.get("qty"),
                "filled_qty": order.get("filled_qty"),
                "limit_price": order.get("limit_price"),
                "entry_price": float(
                    order.get("filled_avg_price")
                    or positions_by_symbol.get(self._symbol_key(str(order.get("symbol", ""))), 0.0)
                    or 0.0
                ),
                "notional": order.get("notional"),
                "time_in_force": order.get("time_in_force"),
                "status": order.get("status"),
                "submitted_at": order.get("submitted_at"),
                "updated_at": order.get("updated_at"),
                "cancel_requested_at": order.get("cancel_requested_at"),
                "raw_reject_reason": order.get("reject_reason"),
                "why_not_filled": self._infer_order_wait_reason(order),
            }
            for order in pending_orders
        ]
        if not trimmed:
            self._order_alias_map = {}
            self.root.after(0, self._show_success, "No hay ordenes pendientes/cancelables.")
            return

        # Build short aliases (O1, O2, ...) to avoid exposing long UUIDs in the UI.
        alias_map: dict[str, str] = {}
        for index, order in enumerate(trimmed, start=1):
            alias = f"O{index}"
            real_id = str(order.get("id", "")).strip()
            if real_id:
                alias_map[alias] = real_id
            order["alias"] = alias
        self._order_alias_map = alias_map

        lines: list[str] = []
        lines.append("ORDENES PENDIENTES")
        lines.append("=================")
        lines.append(f"Cuenta activa: {self.account_var.get().strip()}")
        lines.append(f"Total pendientes: {len(trimmed)}")
        lines.append("Filtro aplicado: solo ordenes abiertas/cancelables")
        if hidden_old > 0:
            lines.append(f"Ordenes viejas/no-cancelables ocultadas: {hidden_old}")
        lines.append("")
        header = (
            f"{'OID':<4} {'SYMBOL':<12} {'SIDE':<6} {'QTY':>10} {'FILLED':>10} {'ENTRY':>12} {'LIMIT':>12} "
            f"{'STATUS':<20} {'TIF':<6} {'MOTIVO':<40}"
        )
        lines.append(header)
        lines.append("-" * len(header))

        for order in trimmed:
            alias = str(order.get("alias", ""))[:4]
            symbol = str(order.get("symbol", "N/A"))[:12]
            side = str(order.get("side", "N/A"))[:6]
            qty = float(order.get("qty", 0.0) or 0.0)
            filled_qty = float(order.get("filled_qty", 0.0) or 0.0)
            entry_price = float(order.get("entry_price", 0.0) or 0.0)
            entry_text = "-" if entry_price <= 0 else f"{entry_price:.6f}"
            raw_limit = order.get("limit_price")
            limit_text = "-"
            if raw_limit not in (None, ""):
                try:
                    limit_text = f"{float(raw_limit):.6f}"
                except (TypeError, ValueError):
                    limit_text = str(raw_limit)
            status = str(order.get("status", "N/A"))[:20]
            tif = str(order.get("time_in_force", "N/A"))[:6]
            reason = str(order.get("why_not_filled", "N/A"))[:40]
            lines.append(
                f"{alias:<4} {symbol:<12} {side:<6} {qty:>10.4f} {filled_qty:>10.4f} {entry_text:>12} {limit_text:>12} "
                f"{status:<20} {tif:<6} {reason:<40}"
            )

        lines.append("")
        lines.append("IDs cortos de orden (usa estos en Cancelar orden):")
        for order in trimmed:
            lines.append(f"- {order.get('alias', 'N/A')} | {order.get('symbol', 'N/A')}")

        self.root.after(0, self._show_success, "\n".join(lines))

    def _cancel_all_pending_orders(self) -> None:
        open_orders = self.order_manager.review_orders(status="open", limit=200)
        combined_open: dict[str, dict[str, Any]] = {}
        for order in open_orders:
            order_id = str(order.get("id", "")).strip()
            if not order_id:
                continue
            combined_open[order_id] = order

        pending_orders = list(combined_open.values())
        if not pending_orders:
            self.root.after(0, self._show_success, "No hay ordenes pendientes para cancelar.")
            return

        cancelled_ids: list[str] = []
        failed: list[dict[str, str]] = []
        for order in pending_orders:
            order_id = str(order.get("id", "")).strip()
            if not order_id:
                continue
            try:
                ok = self.order_manager.cancel_order(order_id)
                if ok:
                    cancelled_ids.append(order_id)
                else:
                    failed.append({"id": order_id, "reason": "cancelacion no confirmada"})
            except Exception as ex:
                failed.append({"id": order_id, "reason": str(ex)})

        lines: list[str] = []
        lines.append("CANCELACION MASIVA")
        lines.append("=================")
        lines.append(f"Pendientes encontradas: {len(pending_orders)}")
        lines.append(f"Canceladas: {len(cancelled_ids)}")
        lines.append(f"Fallidas: {len(failed)}")

        lines.append("")
        lines.append("ORDENES CANCELADAS")
        lines.append("------------------")
        if cancelled_ids:
            for order_id in cancelled_ids:
                lines.append(f"- {order_id}")
        else:
            lines.append("Ninguna")

        lines.append("")
        lines.append("ERRORES")
        lines.append("-------")
        if failed:
            for item in failed:
                lines.append(f"- {item.get('id', 'N/A')}: {item.get('reason', 'sin detalle')}")
        else:
            lines.append("Sin errores")

        self.root.after(0, self._show_success, "\n".join(lines))

    @staticmethod
    def _infer_order_wait_reason(order: dict[str, Any]) -> str:
        status = str(order.get("status", "")).lower()
        reject_reason = str(order.get("reject_reason", "") or "").strip()
        if reject_reason:
            return f"Rechazada por broker: {reject_reason}"

        filled_qty = float(order.get("filled_qty", 0.0) or 0.0)
        qty = float(order.get("qty", 0.0) or 0.0)
        if status == "partially_filled":
            return f"Parcialmente ejecutada ({filled_qty:.8f}/{qty:.8f}); falta liquidez para completar"
        if status in {"new", "accepted", "pending_new", "accepted_for_bidding"}:
            return "En cola del broker/mercado; aun sin contraparte suficiente o enrutamiento pendiente"
        if status in {"pending_replace", "stopped", "calculated"}:
            return "Orden en estado intermedio del broker; esperando transicion a filled/canceled"
        return "Esperando ejecucion en mercado"

    def _view_stocks(self) -> None:
        selected_market = self.market_kind_var.get().strip()
        if selected_market == "Cryptos":
            assets = self.broker.list_cryptos(status="active", only_tradable=True)
        else:
            assets = self.broker.list_stocks(status="active", only_tradable=True)
        sorted_stocks = sorted(assets, key=lambda item: str(item.get("symbol", "")))

        lines = []
        for asset in sorted_stocks:
            symbol = asset.get("symbol", "N/A")
            name = asset.get("name", "N/A")
            exchange = asset.get("exchange", "N/A")
            tradable = asset.get("tradable", False)
            asset_class = asset.get("asset_class", "N/A")
            lines.append(f"{symbol} | {name} | {exchange} | class={asset_class} | tradable={tradable}")

        label = "Cryptos" if selected_market == "Cryptos" else "Stocks"
        output = f"{label} tradables: {len(sorted_stocks)}\n\n" + "\n".join(lines)
        self.root.after(0, self._show_success, output)

    def _view_all_cryptos(self) -> None:
        assets = self.broker.list_all_cryptos()
        sorted_assets = sorted(assets, key=lambda item: str(item.get("symbol", "")))

        lines = []
        for asset in sorted_assets:
            symbol = asset.get("symbol", "N/A")
            name = asset.get("name", "N/A")
            exchange = asset.get("exchange", "N/A")
            tradable = asset.get("tradable", False)
            status = asset.get("status", "N/A")
            lines.append(
                f"{symbol} | {name} | {exchange} | status={status} | tradable={tradable}"
            )

        output = f"Todas las cryptos listadas en Alpaca (active+inactive): {len(sorted_assets)}\n\n" + "\n".join(lines)
        self.root.after(0, self._show_success, output)

    def _cancel_order(self) -> None:
        user_input = self.cancel_order_var.get().strip()
        if not user_input:
            self.root.after(0, self._show_error, "Debes indicar un Order ID")
            return

        alias = user_input.upper()
        order_id = self._order_alias_map.get(alias, user_input)

        if alias.startswith("O") and alias[1:].isdigit() and alias not in self._order_alias_map:
            self.root.after(
                0,
                self._show_error,
                "ID corto no encontrado. Pulsa 'Ver ordenes' para refrescar aliases (O1, O2, ...).",
            )
            return

        if not order_id or ("-" not in order_id and alias not in self._order_alias_map):
            self.root.after(
                0,
                self._show_error,
                "Order ID invalido. Usa un ID corto (O1, O2, ...) o un UUID completo.",
            )
            return

        cancelled = self.order_manager.cancel_order(order_id)
        if not cancelled:
            self.root.after(0, self._show_error, "No se pudo cancelar la orden")
            return

        shown_id = user_input.upper() if user_input.upper() in self._order_alias_map else order_id
        self.root.after(0, self._show_success, f"Orden cancelada: {shown_id}")

    def _build_ai_trading_brain_tab(self) -> None:
        frame = ttk.Frame(self.log_notebook)
        self.log_notebook.add(frame, text="AI Trading Brain")
        self._ai_tab = frame

        header = ttk.Frame(frame)
        header.pack(fill="x", pady=(0, 8))
        ttk.Label(header, text="AI Trading Brain", font=("TkDefaultFont", 12, "bold")).pack(side="left")
        ttk.Button(header, text="Refrescar IA", command=lambda: self._run_async(self._refresh_ai_views)).pack(side="right")

        self.ai_dashboard_text = self._build_ai_section(frame, "Dashboard", 7)
        funds_body = ttk.LabelFrame(frame, text="Fondos")
        funds_body.pack(fill="x", pady=(0, 8))
        self._build_ai_funds_controls(funds_body)

        signals_body = ttk.LabelFrame(frame, text="Señales")
        signals_body.pack(fill="both", expand=False, pady=(0, 8))
        self._build_ai_signals_controls(signals_body)

        self.ai_history_text = self._build_ai_section(frame, "Historial", 8)
        self.ai_model_text = self._build_ai_section(frame, "Modelo local", 12)
        model_toolbar = ttk.Frame(self.ai_model_text.master)
        model_toolbar.pack(fill="x", pady=(6, 0))
        model_toolbar_row1 = ttk.Frame(model_toolbar)
        model_toolbar_row1.pack(fill="x", pady=(0, 4))
        model_toolbar_row2 = ttk.Frame(model_toolbar)
        model_toolbar_row2.pack(fill="x")
        ttk.Button(model_toolbar_row1, text="Entrenar modelo", command=lambda: self._run_async(self._train_ai_model)).pack(side="left")
        self.ai_model_version_combo_tab = ttk.Combobox(
            model_toolbar_row1,
            textvariable=self.ai_model_selected_var,
            values=self._ai_model_versions_values,
            width=36,
            state="readonly",
        )
        self.ai_model_version_combo_tab.pack(side="left", padx=(8, 0))
        self.ai_model_version_combo_tab.bind("<<ComboboxSelected>>", lambda _event: self._refresh_ai_model_view())
        ttk.Entry(model_toolbar_row1, textvariable=self.ai_model_alias_var, width=22).pack(side="left", padx=(8, 0))
        ttk.Button(model_toolbar_row1, text="Guardar nombre", command=lambda: self._run_async(self._save_ai_model_alias)).pack(side="left", padx=(8, 0))
        ttk.Button(model_toolbar_row1, text="Refrescar candidatos", command=lambda: self._run_async(self._refresh_ai_model_candidates)).pack(side="left", padx=(8, 0))
        ttk.Button(model_toolbar_row2, text="Aprobar esta version", command=lambda: self._run_async(self._approve_ai_model_selected)).pack(side="left", padx=(0, 8))
        ttk.Button(model_toolbar_row2, text="Congelar candidato", command=lambda: self._run_async(self._freeze_ai_model_selected)).pack(side="left", padx=(0, 8))
        ttk.Button(model_toolbar_row2, text="Descongelar", command=lambda: self._run_async(self._unfreeze_ai_model_candidate)).pack(side="left", padx=(0, 8))
        ttk.Button(model_toolbar_row2, text="Eliminar version", command=lambda: self._run_async(self._delete_ai_model_selected)).pack(side="left", padx=(0, 8))
        ttk.Button(model_toolbar_row2, text="Aprobar modelo nuevo", command=lambda: self._run_async(self._approve_ai_model)).pack(side="left", padx=(0, 8))
        ttk.Button(model_toolbar_row2, text="Volver al anterior", command=lambda: self._run_async(self._rollback_ai_model)).pack(side="left")
        self.ai_security_text = self._build_ai_section(frame, "Seguridad", 7)

        monitor_container = ttk.LabelFrame(frame, text="Monitoreo Crypto IA")
        monitor_container.pack(fill="both", expand=False, pady=(0, 8))
        monitor_toolbar = ttk.Frame(monitor_container)
        monitor_toolbar.pack(fill="x", pady=(0, 6))
        ttk.Label(
            monitor_toolbar,
            text="Vista rápida por crypto: última señal, bloqueo exacto y ajuste mínimo para pasar de WATCH a BUY.",
        ).pack(side="left")
        ttk.Button(
            monitor_toolbar,
            text="Refrescar Monitoreo Crypto",
            command=lambda: self._run_async(self._refresh_ai_crypto_monitor_view),
        ).pack(side="right")
        monitor_height = 8 if self._ui_compact_mode else 12
        self.ai_crypto_monitor_text = tk.Text(monitor_container, height=monitor_height, wrap="word")
        self.ai_crypto_monitor_text.pack(fill="both", expand=True)
        self.ai_crypto_monitor_text.configure(state="disabled")

    def _build_ai_section(self, parent: Any, title: str, height: int) -> tk.Text:
        container = ttk.LabelFrame(parent, text=title)
        container.pack(fill="both", expand=False, pady=(0, 8))
        effective_height = max(5, int(height * 0.65)) if self._ui_compact_mode else height
        widget = tk.Text(container, height=effective_height, wrap="word")
        widget.pack(fill="both", expand=True)
        widget.configure(state="disabled")
        return widget

    def _build_ai_funds_controls(self, parent: Any) -> None:
        ttk.Label(parent, text="Capital maximo").grid(row=0, column=0, sticky="w")
        ttk.Entry(parent, textvariable=self.ai_max_capital_var, width=12).grid(row=1, column=0, padx=(0, 8), sticky="w")
        ttk.Label(parent, text="Max por posicion").grid(row=0, column=1, sticky="w")
        ttk.Entry(parent, textvariable=self.ai_max_position_var, width=12).grid(row=1, column=1, padx=(0, 8), sticky="w")
        ttk.Label(parent, text="Perdida diaria").grid(row=0, column=2, sticky="w")
        ttk.Entry(parent, textvariable=self.ai_max_daily_loss_var, width=12).grid(row=1, column=2, padx=(0, 8), sticky="w")
        ttk.Label(parent, text="Target IA Stocks $/operación").grid(row=0, column=3, sticky="w")
        ttk.Entry(parent, textvariable=self.ai_target_profit_stocks_var, width=12).grid(row=1, column=3, padx=(0, 8), sticky="w")
        ttk.Label(parent, text="Target IA Cryptos $/operación").grid(row=0, column=4, sticky="w")
        ttk.Entry(parent, textvariable=self.ai_target_profit_cryptos_var, width=12).grid(row=1, column=4, padx=(0, 8), sticky="w")
        ttk.Checkbutton(parent, text="Bot activo", variable=self.ai_bot_enabled_var).grid(row=0, column=5, sticky="w")
        ttk.Checkbutton(parent, text="Solo señales", variable=self.ai_signal_only_var).grid(row=1, column=5, sticky="w")
        ttk.Checkbutton(parent, text="Paper trading", variable=self.ai_paper_trading_var).grid(row=0, column=6, sticky="w")
        ttk.Checkbutton(parent, text="Live habilitado", variable=self.ai_live_enabled_var).grid(row=1, column=6, sticky="w")
        ttk.Checkbutton(parent, text="Aprobacion manual", variable=self.ai_manual_approval_var).grid(row=0, column=7, sticky="w")
        ttk.Checkbutton(parent, text="Kill switch", variable=self.ai_kill_switch_var).grid(row=1, column=7, sticky="w")
        ttk.Button(parent, text="Guardar fondos", command=lambda: self._run_async(self._save_ai_runtime_controls)).grid(row=1, column=8, padx=(8, 0), sticky="w")

    def _build_ai_signals_controls(self, parent: Any) -> None:
        toolbar = ttk.Frame(parent)
        toolbar.pack(fill="x", pady=(0, 6))
        ttk.Label(toolbar, text="Las señales y el análisis de texto se generan automáticamente en segundo plano.").pack(side="left")
        ttk.Button(toolbar, text="Diagnóstico IA", command=self._show_ai_diagnostics_window).pack(side="left", padx=(8, 0))
        ttk.Button(toolbar, text="Generar señal IA", command=lambda: self._run_async(self._generate_ai_signal)).pack(side="left", padx=(8, 0))
        ttk.Button(toolbar, text="Analizar texto", command=lambda: self._run_async(self._analyze_ai_text)).pack(side="left", padx=(8, 0))
        ttk.Button(toolbar, text="Comprar limit", command=lambda: self._run_async(self._execute_ai_limit_buy)).pack(side="left", padx=(8, 0))
        ttk.Button(toolbar, text="Evaluar venta", command=lambda: self._run_async(self._execute_ai_sell_check)).pack(side="left", padx=(8, 0))
        ttk.Button(toolbar, text="Historial", command=lambda: self._run_async(self._show_ai_signal_history)).pack(side="left", padx=(8, 0))
        signal_height = 7 if self._ui_compact_mode else 10
        self.ai_signal_text = tk.Text(parent, height=signal_height, wrap="word")
        self.ai_signal_text.pack(fill="both", expand=True)
        self.ai_signal_text.configure(state="disabled")
        ttk.Label(parent, text="Texto/noticia para OpenAI Analyzer").pack(anchor="w", pady=(6, 0))
        self.ai_news_input = tk.Text(parent, height=4, wrap="word")
        self.ai_news_input.pack(fill="x", expand=False)
        self._ai_news_autofill_text = ""

    def _refresh_ai_views(self) -> None:
        account_name = self._ai_automation_account_name()
        if account_name:
            self._ensure_ai_automation_running(account_name)
        try:
            self._load_ai_runtime_controls()
        except Exception:
            # Keep IA window usable even if account/API is temporarily unavailable.
            pass

        if hasattr(self, "ai_runtime_text"):
            self._refresh_ai_runtime_view()

        if hasattr(self, "ai_model_text"):
            self._refresh_ai_model_view()
        if hasattr(self, "ai_signal_text"):
            self._refresh_ai_signals_view()
        if getattr(self, "ai_crypto_monitor_text", None) is not None:
            self._refresh_ai_crypto_monitor_view_async()

    def _refresh_ai_crypto_monitor_view_async(self) -> None:
        if self._ai_crypto_monitor_refresh_in_flight:
            return
        self._ai_crypto_monitor_refresh_in_flight = True

        def worker() -> None:
            try:
                self._refresh_ai_crypto_monitor_view()
            finally:
                self._ai_crypto_monitor_refresh_in_flight = False

        threading.Thread(target=worker, daemon=True).start()

    def _start_ai_automation(self) -> None:
        account_name = self.account_var.get().strip()
        self._ai_automation_account = account_name
        self._save_ai_ui_state()
        self._persist_ai_runtime_controls(show_message=False)
        status = self.ai_trading_brain.start_automation(account_name=account_name)
        self._refresh_ai_runtime_view(status=status)
        self.root.after(0, self._show_success, "Bot automático IA iniciado.", False)

    def _refresh_ai_model_view_with_status(self, status: dict[str, Any]) -> None:
        if not hasattr(self, "ai_model_text"):
            return
        training = self.ai_trading_brain.database.latest_training_run() or {}
        if not training:
            self._set_text_widget(
                self.ai_model_text,
                "Modelo no entrenado todavía.\nNo hay suficientes datos para mostrar métricas.",
            )
            return

        training_cycle_seconds = float(status.get("training_cycle_seconds", 0.0) or 0.0)
        training_elapsed_seconds = float(status.get("training_elapsed_seconds", 0.0) or 0.0)
        training_remaining_seconds = float(status.get("training_remaining_seconds", 0.0) or 0.0)
        training_progress_pct = float(status.get("training_progress_pct", 0.0) or 0.0)
        training_last_error = str(status.get("training_last_error", "") or "").strip()

        candidates_payload = self.ai_trading_brain.list_model_candidates(limit=20)
        approved = str(candidates_payload.get("approved", "") or "")
        approved_valid = bool(candidates_payload.get("approved_valid", False))
        latest = str(candidates_payload.get("latest", "") or "")
        frozen = str(candidates_payload.get("frozen", "") or "")
        candidate_rows = list(candidates_payload.get("rows", []))

        selected_version = ""
        try:
            selected_version = self._selected_ai_model_version()
        except Exception:
            selected_version = ""
        if not selected_version and candidate_rows:
            selected_version = str(candidate_rows[0].get("model_version", "") or "")

        selected_row = None
        for row in candidate_rows:
            if str(row.get("model_version", "") or "") == selected_version:
                selected_row = row
                break
        if selected_row is None:
            selected_row = candidate_rows[0] if candidate_rows else training
            selected_version = str(selected_row.get("model_version", latest or approved or "") or "")

        self.ai_model_selected_var.set(selected_version)
        self.ai_model_alias_var.set(self._model_alias_for_version(selected_version))

        lines = [
            f"Aprobado para paper: {approved or 'N/A'}",
            f"Aprobado para live: {frozen or 'N/A'}",
            f"Ultimo entrenado: {latest or 'N/A'}",
            f"Modelo actual: {selected_version or 'N/A'}",
            f"Progreso entrenamiento: {training_progress_pct:.1f}%",
            f"Tiempo transcurrido: {training_elapsed_seconds:.0f}s",
            f"Tiempo restante: {training_remaining_seconds:.0f}s",
            f"Ciclo entrenamiento: {training_cycle_seconds:.0f}s",
            f"Ultimo error: {training_last_error or 'N/A'}",
            "",
            "Candidatos recientes:",
        ]

        if not candidate_rows:
            lines.append("  - Sin candidatos registrados")
        else:
            for row in candidate_rows[:8]:
                lines.append(
                    f"  - {row.get('model_version', 'N/A')} | acc={float(row.get('accuracy', 0.0) or 0.0):.3f} | "
                    f"win={float(row.get('win_rate', 0.0) or 0.0):.3f} | approved_paper={bool(row.get('approved_for_paper', 0))}"
                )

        self._set_text_widget(self.ai_model_text, "\n".join(lines))

    def _pause_ai_automation(self) -> None:
        status = self.ai_trading_brain.pause_automation()
        self._refresh_ai_runtime_view(status=status)
        self.root.after(0, self._show_success, "Bot automático IA en pausa.", False)

    def _persist_ai_runtime_controls(self, show_message: bool = True) -> None:
        account_name = self._ai_automation_account_name()
        if not account_name:
            account_name = self.account_var.get().strip()
        self.ai_trading_brain.update_runtime_controls(
            account_name=account_name,
            max_capital_assigned=float(self.ai_max_capital_var.get().strip() or 0.0),
            max_position_size=float(self.ai_max_position_var.get().strip() or 0.0),
            max_daily_loss=float(self.ai_max_daily_loss_var.get().strip() or 0.0),
            enabled=bool(self.ai_bot_enabled_var.get()),
            signal_only_mode=bool(self.ai_signal_only_var.get()),
            paper_trading=bool(self.ai_paper_trading_var.get()),
            live_trading_enabled=False,
            manual_approval_required=bool(self.ai_manual_approval_var.get()),
            kill_switch=bool(self.ai_kill_switch_var.get()),
            auto_trade_stocks_enabled=bool(self.ai_auto_trade_stocks_var.get()),
            auto_trade_cryptos_enabled=bool(self.ai_auto_trade_cryptos_var.get()),
            scanner_decision_engine=str(self.ai_decision_engine_var.get() or "heuristic"),
            futures_only_mode=bool(self.ai_futures_only_mode_var.get()),
            futures_leverage=max(int(float(self.ai_futures_leverage_var.get().strip() or "1")), 1),
            futures_require_technical=bool(self.ai_futures_require_technical_var.get()),
            futures_require_news=bool(self.ai_futures_require_news_var.get()),
            futures_enable_long=bool(self.ai_futures_enable_long_var.get()),
            futures_enable_short=bool(self.ai_futures_enable_short_var.get()),
        )
        target_stock = float(self.ai_target_profit_stocks_var.get().strip() or self.ai_target_profit_var.get().strip() or 0.0)
        target_crypto = float(self.ai_target_profit_cryptos_var.get().strip() or self.ai_target_profit_var.get().strip() or 0.0)
        self.ai_trading_brain.update_ai_target_profit_per_operation_by_asset(
            stock_value=target_stock,
            crypto_value=target_crypto,
        )
        self.ai_target_profit_var.set(str(target_stock))
        self._refresh_ai_views()
        if show_message:
            self.root.after(0, self._show_success, f"AI Trading Brain actualizado para cuenta: {account_name}", False)

    def _persist_ai_focus_controls(self, show_message: bool = True) -> None:
        account_name = self.account_var.get().strip()
        self._sync_focus_symbol_vars_from_selected()
        self.ai_trading_brain.update_focus_symbols(
            account_name=account_name,
            focus_stocks_only=bool(self.ai_focus_stocks_only_var.get()),
            focus_cryptos_only=bool(self.ai_focus_cryptos_only_var.get()),
            focus_stocks_symbols=self.ai_focus_stocks_symbols_var.get().strip(),
            focus_cryptos_symbols=self.ai_focus_cryptos_symbols_var.get().strip(),
        )
        self._write_env_values(
            {
                "AI_FOCUS_STOCKS_ONLY": "true" if bool(self.ai_focus_stocks_only_var.get()) else "false",
                "AI_FOCUS_CRYPTOS_ONLY": "true" if bool(self.ai_focus_cryptos_only_var.get()) else "false",
                "AI_FOCUS_STOCKS_SYMBOLS": self.ai_focus_stocks_symbols_var.get().strip(),
                "AI_FOCUS_CRYPTOS_SYMBOLS": self.ai_focus_cryptos_symbols_var.get().strip(),
            }
        )
        self._save_ai_ui_state()
        if show_message:
            self.root.after(0, self._show_success, f"Enfoque IA guardado y aplicado para cuenta: {account_name}", False)

    def _save_ai_focus_controls(self) -> None:
        self._persist_ai_focus_controls(show_message=True)

    @staticmethod
    def _parse_symbols_csv(raw: str) -> list[str]:
        symbols = [
            str(token).upper().replace(" ", "")
            for token in str(raw or "").replace(";", ",").split(",")
            if str(token).strip()
        ]
        unique: list[str] = []
        for symbol in symbols:
            if symbol not in unique:
                unique.append(symbol)
        return unique

    def _sync_focus_symbol_vars_from_selected(self) -> None:
        self.ai_focus_stocks_symbols_var.set(",".join(self.ai_focus_stock_selected))
        self.ai_focus_cryptos_symbols_var.set(",".join(self.ai_focus_crypto_selected))
        self.ai_focus_stocks_selected_var.set(", ".join(self.ai_focus_stock_selected) if self.ai_focus_stock_selected else "(ninguno)")
        self.ai_focus_cryptos_selected_var.set(", ".join(self.ai_focus_crypto_selected) if self.ai_focus_crypto_selected else "(ninguno)")

    def _load_focus_selected_from_vars(self) -> None:
        self.ai_focus_stock_selected = self._parse_symbols_csv(self.ai_focus_stocks_symbols_var.get())
        self.ai_focus_crypto_selected = self._parse_symbols_csv(self.ai_focus_cryptos_symbols_var.get())
        self._sync_focus_symbol_vars_from_selected()

    def _refresh_ai_focus_symbol_options_async(self) -> None:
        threading.Thread(target=self._refresh_ai_focus_symbol_options, daemon=True).start()

    def _refresh_ai_focus_symbol_options(self) -> None:
        try:
            stock_assets = self.broker.list_stocks(status="active", only_tradable=True)
            stock_values = sorted(
                {
                    str(asset.get("symbol", "")).upper()
                    for asset in stock_assets
                    if str(asset.get("symbol", "")).strip()
                }
            )
        except Exception as ex:
            self.logger.warning("No se pudo refrescar lista de stocks para enfoque IA: %s", ex)
            stock_values = []

        try:
            crypto_assets = self.broker.list_cryptos(status="active", only_tradable=True)
            crypto_values = sorted(
                {
                    str(asset.get("symbol", "")).upper()
                    for asset in crypto_assets
                    if str(asset.get("symbol", "")).strip()
                }
            )
        except Exception as ex:
            self.logger.warning("No se pudo refrescar lista de cryptos para enfoque IA: %s", ex)
            crypto_values = []

        self._safe_after(0, self._apply_ai_focus_symbol_options, stock_values, crypto_values)

    def _apply_ai_focus_symbol_options(self, stock_values: list[str], crypto_values: list[str]) -> None:
        self.ai_focus_stock_values = stock_values
        self.ai_focus_crypto_values = crypto_values

        stock_combo = getattr(self, "ai_focus_stock_combo", None)
        if stock_combo is not None:
            stock_combo.configure(values=self.ai_focus_stock_values)
            if self.ai_focus_stock_values and self.ai_focus_stock_pick_var.get().strip().upper() not in self.ai_focus_stock_values:
                self.ai_focus_stock_pick_var.set(self.ai_focus_stock_values[0])

        crypto_combo = getattr(self, "ai_focus_crypto_combo", None)
        if crypto_combo is not None:
            crypto_combo.configure(values=self.ai_focus_crypto_values)
            if self.ai_focus_crypto_values and self.ai_focus_crypto_pick_var.get().strip().upper() not in self.ai_focus_crypto_values:
                self.ai_focus_crypto_pick_var.set(self.ai_focus_crypto_values[0])

    def _add_focus_stock(self) -> None:
        symbol = self.ai_focus_stock_pick_var.get().strip().upper()
        if not symbol:
            return
        if symbol not in self.ai_focus_stock_selected:
            self.ai_focus_stock_selected.append(symbol)
        self._sync_focus_symbol_vars_from_selected()
        self._persist_ai_focus_controls(show_message=False)

    def _remove_focus_stock(self) -> None:
        symbol = self.ai_focus_stock_pick_var.get().strip().upper()
        if symbol in self.ai_focus_stock_selected:
            self.ai_focus_stock_selected.remove(symbol)
        self._sync_focus_symbol_vars_from_selected()
        self._persist_ai_focus_controls(show_message=False)

    def _clear_focus_stocks(self) -> None:
        self.ai_focus_stock_selected = []
        self._sync_focus_symbol_vars_from_selected()
        self._persist_ai_focus_controls(show_message=False)

    def _add_focus_crypto(self) -> None:
        symbol = self.ai_focus_crypto_pick_var.get().strip().upper()
        if not symbol:
            return
        if symbol not in self.ai_focus_crypto_selected:
            self.ai_focus_crypto_selected.append(symbol)
        self._sync_focus_symbol_vars_from_selected()
        self._persist_ai_focus_controls(show_message=False)

    def _remove_focus_crypto(self) -> None:
        symbol = self.ai_focus_crypto_pick_var.get().strip().upper()
        if symbol in self.ai_focus_crypto_selected:
            self.ai_focus_crypto_selected.remove(symbol)
        self._sync_focus_symbol_vars_from_selected()
        self._persist_ai_focus_controls(show_message=False)

    def _clear_focus_cryptos(self) -> None:
        self.ai_focus_crypto_selected = []
        self._sync_focus_symbol_vars_from_selected()
        self._persist_ai_focus_controls(show_message=False)

    def _toggle_ai_stocks_execution(self) -> None:
        account_name = self._ai_automation_account_name()
        if not account_name:
            account_name = self.account_var.get().strip()
        current = bool(self.ai_auto_trade_stocks_var.get())
        self.ai_auto_trade_stocks_var.set(0 if current else 1)
        self._persist_ai_runtime_controls(show_message=False)
        state = "ON" if not current else "PAUSADA"
        self.root.after(0, self._show_success, f"Ejecución Stocks ({account_name}): {state}", False)

    def _toggle_ai_cryptos_execution(self) -> None:
        account_name = self._ai_automation_account_name()
        if not account_name:
            account_name = self.account_var.get().strip()
        current = bool(self.ai_auto_trade_cryptos_var.get())
        self.ai_auto_trade_cryptos_var.set(0 if current else 1)
        self._persist_ai_runtime_controls(show_message=False)
        state = "ON" if not current else "PAUSADA"
        self.root.after(0, self._show_success, f"Ejecución Cryptos ({account_name}): {state}", False)

    def _toggle_ai_learning_automation(self) -> None:
        account_name = self._ai_automation_account_name()
        status = self.ai_trading_brain.get_automation_status(account_name)
        learning_running = all(
            status.get(key, "Stopped") == "Running"
            for key in ("collector", "scanner", "labeler", "news_social", "trainer")
        )

        if learning_running:
            status = self.ai_trading_brain.pause_automation()
            message = "Aprendizaje IA: PAUSED"
        else:
            self._ai_automation_account = account_name
            self._persist_ai_runtime_controls(show_message=False)
            self._save_ai_ui_state()
            status = self.ai_trading_brain.start_automation(account_name=account_name)
            message = "Aprendizaje IA: RUNNING"

        self._refresh_ai_runtime_view(status=status)
        self.root.after(0, self._show_success, message, False)

    def _refresh_ai_runtime_view(self, status: dict[str, Any] | None = None) -> None:
        if not hasattr(self, "ai_runtime_text"):
            return
        if status is None:
            status = self.ai_trading_brain.get_automation_status(self._ai_automation_account_name())

        def _fmt_runtime_time(value: Any) -> str:
            raw = str(value or "").strip()
            if not raw:
                return "N/A"

            suffix = ""
            timestamp_part = raw
            if " | " in raw:
                timestamp_part, suffix = raw.split(" | ", 1)

            try:
                parsed = datetime.fromisoformat(timestamp_part)
            except ValueError:
                return raw

            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)

            local_dt = parsed.astimezone()
            utc_dt = parsed.astimezone(timezone.utc)
            base = f"{local_dt.strftime('%Y-%m-%d %H:%M:%S %Z')} | {utc_dt.strftime('%H:%M:%S UTC')}"
            if suffix:
                return f"{base} | {suffix}"
            return base

        last_signal = status.get("last_signal") or {}
        last_signal_text = (
            f"{last_signal.get('symbol', 'N/A')} | {last_signal.get('signal_type', 'N/A')} | "
            f"score={float(last_signal.get('confidence_score', 0.0) or 0.0):.2f}"
            if last_signal
            else "N/A"
        )

        stocks_on = bool(status.get("auto_trade_stocks_enabled", True))
        cryptos_on = bool(status.get("auto_trade_cryptos_enabled", True))
        learning_running = all(
            status.get(key, "Stopped") == "Running"
            for key in ("collector", "scanner", "labeler", "news_social", "trainer")
        )

        self.ai_badge_stocks_var.set(f"Ejecucion Stocks: {'ON' if stocks_on else 'PAUSADA'}")
        self.ai_badge_cryptos_var.set(f"Ejecucion Cryptos: {'ON' if cryptos_on else 'PAUSADA'}")
        self.ai_badge_learning_var.set(f"Aprendizaje IA: {'RUNNING' if learning_running else 'PAUSED'}")

        if hasattr(self, "ai_badge_stocks_button"):
            color = "#1f5f2a" if stocks_on else "#8a1c1c"
            self.ai_badge_stocks_button.configure(bg=color, activebackground=color)
        if hasattr(self, "ai_badge_cryptos_button"):
            color = "#1f5f2a" if cryptos_on else "#8a1c1c"
            self.ai_badge_cryptos_button.configure(bg=color, activebackground=color)
        if hasattr(self, "ai_badge_learning_button"):
            color = "#1f5f2a" if learning_running else "#9a6700"
            self.ai_badge_learning_button.configure(bg=color, activebackground=color)

        scanner_state = str(status.get("scanner", "Stopped") or "Stopped")
        data_tick = _fmt_runtime_time(status.get("last_data_update", "N/A"))
        self.ai_pending_status_var.set(f"IA pendiente: {'SI' if scanner_state == 'Running' else 'NO'} | tick={data_tick}")

        lines = [
            f"Cuenta scanner IA activa: {status.get('active_worker_account', 'N/A') or 'N/A'}",
            f"Motor de decisión scanner: {status.get('scanner_decision_engine', 'heuristic')}",
            f"Actor decisiones: {status.get('decision_actor', 'Heurística')}",
            f"Máximo posiciones abiertas activas: {int(status.get('max_open_positions', getattr(settings, 'max_open_positions', 5)) or getattr(settings, 'max_open_positions', 5))}",
            (
                "Futuros: "
                f"solo_futuros={'SI' if bool(status.get('futures_only_mode', True)) else 'NO'} | "
                f"leverage_config_nuevas_entradas={int(status.get('futures_leverage', 1) or 1)}x | "
                f"LONG={'ON' if bool(status.get('futures_enable_long', True)) else 'OFF'} | "
                f"SHORT={'ON' if bool(status.get('futures_enable_short', True)) else 'OFF'}"
            ),
            f"Leverage broker posiciones abiertas: {', '.join(list(status.get('broker_open_positions_leverage', []) or [])[:8]) or 'N/A'}",
            f"Cryptos en estudio ahora: {int(status.get('scanner_cryptos_count', 0) or 0)}",
            f"Vista rápida cryptos: {', '.join(list(status.get('scanner_cryptos_preview', []) or [])[:12]) or 'N/A'}",
            f"Pensamiento IA (últimas decisiones): {' || '.join(list(status.get('scanner_thinking_preview', []) or [])[:3]) or 'N/A'}",
            f"Estado del DataCollector: {status.get('collector', 'Stopped')}",
            f"Estado del SignalScanner: {status.get('scanner', 'Stopped')}",
            f"Estado del OutcomeLabeler: {status.get('labeler', 'Stopped')}",
            f"Estado del News/Social Collector: {status.get('news_social', 'Stopped')}",
            f"Estado del ModelTrainer: {status.get('trainer', 'Stopped')}",
            f"Última actualización de datos: {_fmt_runtime_time(status.get('last_data_update', 'N/A'))}",
            f"Última actualización de news/social: {_fmt_runtime_time(status.get('last_news_update', 'N/A'))}",
            f"Última actualización de entrenamiento: {_fmt_runtime_time(status.get('last_training_update', 'N/A'))}",
            f"Snapshots guardados hoy: {status.get('snapshots_today', 0)}",
            f"Señales generadas hoy: {status.get('signals_today', 0)}",
            f"Señales evaluadas hoy: {status.get('evaluated_outcomes_today', 0)}",
            f"Eventos news/social hoy: {status.get('news_events_today', 0)}",
            f"Outcomes evaluados: {status.get('evaluated_outcomes', 0)}",
            f"Modelo actual: {status.get('model_current', 'N/A')}",
            f"Modelo aprobado para paper: {status.get('model_approved_paper', 'manual_pending')}",
            f"Modelo aprobado para live: {status.get('model_approved_live', 'manual_required')}",
            f"Última señal: {last_signal_text}",
            f"Última llamada OpenAI: {status.get('last_openai_call', 'N/A') or 'N/A'}",
            f"Último error de API: {status.get('last_api_error', '') or 'Ninguno'}",
            f"Modo actual: {status.get('mode', 'Solo señales')}",
            f"Auto trading stocks: {'ON' if bool(status.get('auto_trade_stocks_enabled', True)) else 'PAUSADO'}",
            f"Auto trading cryptos: {'ON' if bool(status.get('auto_trade_cryptos_enabled', True)) else 'PAUSADO'}",
            f"OpenAI calls usadas hoy: {status.get('openai_calls_today', 0)}",
            f"API calls usadas hoy: {status.get('api_calls_today', 0)}",
        ]
        self._set_text_widget(self.ai_runtime_text, "\n".join(lines))

    def _load_ai_runtime_controls(self) -> None:
        account_name = self.account_var.get().strip()
        dashboard = self.ai_trading_brain.get_dashboard(account_name)
        funds = dashboard.get("funds") or {}
        runtime = dashboard.get("runtime") or {}
        self.ai_max_capital_var.set(str(funds.get("max_capital_assigned", settings.ai_default_max_capital_assigned)))
        self.ai_max_position_var.set(str(funds.get("max_position_size", settings.ai_default_max_position_size)))
        self.ai_max_daily_loss_var.set(str(funds.get("max_daily_loss", settings.ai_default_max_daily_loss)))
        stock_target = float(getattr(self.ai_trading_brain, "_ai_target_profit_per_operation_stocks", getattr(settings, "ai_target_profit_per_operation_stocks", getattr(settings, "ai_target_profit_per_operation", 0.05))))
        crypto_target = float(getattr(self.ai_trading_brain, "_ai_target_profit_per_operation_cryptos", getattr(settings, "ai_target_profit_per_operation_cryptos", getattr(settings, "ai_target_profit_per_operation", 0.05))))
        self.ai_target_profit_var.set(str(stock_target))
        self.ai_target_profit_stocks_var.set(str(stock_target))
        self.ai_target_profit_cryptos_var.set(str(crypto_target))
        self.ai_bot_enabled_var.set(1 if bool(funds.get("enabled", 1)) else 0)
        self.ai_signal_only_var.set(1 if bool(runtime.get("signal_only_mode", settings.ai_signal_only_mode)) else 0)
        self.ai_paper_trading_var.set(1 if bool(runtime.get("paper_trading", settings.paper_trading)) else 0)
        self.ai_live_enabled_var.set(1 if bool(runtime.get("live_trading_enabled", settings.live_trading_enabled)) else 0)
        self.ai_manual_approval_var.set(1 if bool(runtime.get("manual_approval_required", settings.manual_approval_required)) else 0)
        self.ai_kill_switch_var.set(1 if bool(runtime.get("kill_switch", 0)) else 0)
        self.ai_auto_trade_stocks_var.set(1 if bool(runtime.get("auto_trade_stocks_enabled", 1)) else 0)
        self.ai_auto_trade_cryptos_var.set(1 if bool(runtime.get("auto_trade_cryptos_enabled", 1)) else 0)
        self.ai_futures_only_mode_var.set(1 if bool(runtime.get("futures_only_mode", getattr(settings, "crypto_futures_only_mode", True))) else 0)
        self.ai_futures_leverage_var.set(str(max(int(runtime.get("futures_leverage", getattr(settings, "crypto_futures_default_leverage", 1)) or 1), 1)))
        self.ai_futures_require_technical_var.set(1 if bool(runtime.get("futures_require_technical", getattr(settings, "ai_futures_require_technical", True))) else 0)
        self.ai_futures_require_news_var.set(1 if bool(runtime.get("futures_require_news", getattr(settings, "ai_futures_require_news", False))) else 0)
        self.ai_futures_enable_long_var.set(1 if bool(runtime.get("futures_enable_long", getattr(settings, "ai_futures_enable_long", True))) else 0)
        self.ai_futures_enable_short_var.set(1 if bool(runtime.get("futures_enable_short", getattr(settings, "ai_futures_enable_short", True))) else 0)
        self.ai_diag_signal_only_var.set(self.ai_signal_only_var.get())
        self.ai_diag_auto_stocks_var.set(self.ai_auto_trade_stocks_var.get())
        self.ai_diag_auto_cryptos_var.set(self.ai_auto_trade_cryptos_var.get())
        engine = str(runtime.get("scanner_decision_engine", "heuristic") or "heuristic").strip().lower()
        self.ai_decision_engine_var.set(engine if engine in {"heuristic", "model"} else "heuristic")
        focus = self.ai_trading_brain.get_focus_symbols(account_name)
        self.ai_focus_stocks_only_var.set(1 if bool(focus.get("stocks_only", False)) else 0)
        self.ai_focus_cryptos_only_var.set(1 if bool(focus.get("cryptos_only", False)) else 0)
        self.ai_focus_stocks_symbols_var.set(str(focus.get("stocks_symbols_raw", "") or ""))
        self.ai_focus_cryptos_symbols_var.set(str(focus.get("cryptos_symbols_raw", "") or ""))

        persisted_focus: dict[str, Any] = {}
        cached_state = getattr(self, "_ai_ui_state_cache", {})
        if isinstance(cached_state, dict):
            by_account = cached_state.get("ai_focus_by_account", {})
            if isinstance(by_account, dict):
                candidate = by_account.get(account_name, {})
                if isinstance(candidate, dict):
                    persisted_focus = candidate
        if persisted_focus:
            self.ai_focus_stocks_only_var.set(1 if bool(persisted_focus.get("stocks_only", self.ai_focus_stocks_only_var.get())) else 0)
            self.ai_focus_cryptos_only_var.set(1 if bool(persisted_focus.get("cryptos_only", self.ai_focus_cryptos_only_var.get())) else 0)
            self.ai_focus_stocks_symbols_var.set(str(persisted_focus.get("stocks_symbols", self.ai_focus_stocks_symbols_var.get()) or ""))
            self.ai_focus_cryptos_symbols_var.set(str(persisted_focus.get("cryptos_symbols", self.ai_focus_cryptos_symbols_var.get()) or ""))

        self._load_focus_selected_from_vars()
        self._refresh_ai_focus_symbol_options_async()

    def _save_ai_runtime_controls(self) -> None:
        self._persist_ai_runtime_controls(show_message=True)

    def _open_futures_risk_window(self) -> None:
        window = tk.Toplevel(self.root)
        window.title("Configuracion de Futuros")
        window.transient(self.ai_window if self.ai_window is not None else self.root)
        window.grab_set()
        window.resizable(False, False)

        body = ttk.Frame(window, padding=12)
        body.pack(fill="both", expand=True)

        ttk.Label(body, text="Riesgo y dirección para futuros", font=("TkDefaultFont", 11, "bold")).grid(
            row=0,
            column=0,
            columnspan=2,
            sticky="w",
            pady=(0, 8),
        )
        ttk.Checkbutton(body, text="Solo futuros (ignorar stocks)", variable=self.ai_futures_only_mode_var).grid(
            row=1,
            column=0,
            columnspan=2,
            sticky="w",
            pady=3,
        )
        ttk.Label(body, text="Apalancamiento (1x, 2x, 3x...)").grid(row=2, column=0, sticky="w", pady=3)
        ttk.Entry(body, textvariable=self.ai_futures_leverage_var, width=10).grid(row=2, column=1, sticky="w", pady=3)

        ttk.Checkbutton(body, text="Permitir LONG", variable=self.ai_futures_enable_long_var).grid(
            row=3,
            column=0,
            columnspan=2,
            sticky="w",
            pady=3,
        )
        ttk.Checkbutton(body, text="Permitir SHORT", variable=self.ai_futures_enable_short_var).grid(
            row=4,
            column=0,
            columnspan=2,
            sticky="w",
            pady=3,
        )
        ttk.Checkbutton(body, text="Exigir confirmacion tecnica", variable=self.ai_futures_require_technical_var).grid(
            row=5,
            column=0,
            columnspan=2,
            sticky="w",
            pady=3,
        )
        ttk.Checkbutton(body, text="Exigir confirmacion de noticias", variable=self.ai_futures_require_news_var).grid(
            row=6,
            column=0,
            columnspan=2,
            sticky="w",
            pady=3,
        )

        ttk.Label(
            body,
            text="El scanner combina tecnico + noticias para decidir long/short segun estas reglas.",
            foreground="#666",
        ).grid(row=7, column=0, columnspan=2, sticky="w", pady=(8, 4))

        actions = ttk.Frame(body)
        actions.grid(row=8, column=0, columnspan=2, sticky="e", pady=(8, 0))
        ttk.Button(actions, text="Cancelar", command=window.destroy).pack(side="right", padx=(8, 0))
        ttk.Button(
            actions,
            text="Guardar",
            command=lambda: self._save_futures_risk_window(window),
        ).pack(side="right")

    def _save_futures_risk_window(self, window: tk.Toplevel) -> None:
        try:
            leverage = max(int(float(self.ai_futures_leverage_var.get().strip() or "1")), 1)
        except ValueError:
            self._show_error("Apalancamiento invalido. Usa valores como 1, 2, 3...")
            return

        self.ai_futures_leverage_var.set(str(leverage))
        self._write_env_values(
            {
                "CRYPTO_FUTURES_ONLY_MODE": "true" if bool(self.ai_futures_only_mode_var.get()) else "false",
                "CRYPTO_FUTURES_DEFAULT_LEVERAGE": str(leverage),
                "AI_FUTURES_REQUIRE_TECHNICAL": "true" if bool(self.ai_futures_require_technical_var.get()) else "false",
                "AI_FUTURES_REQUIRE_NEWS": "true" if bool(self.ai_futures_require_news_var.get()) else "false",
                "AI_FUTURES_ENABLE_LONG": "true" if bool(self.ai_futures_enable_long_var.get()) else "false",
                "AI_FUTURES_ENABLE_SHORT": "true" if bool(self.ai_futures_enable_short_var.get()) else "false",
            }
        )
        self._persist_ai_runtime_controls(show_message=False)
        window.destroy()
        self._show_success("Configuracion de futuros guardada y aplicada.", False)

    def _refresh_ai_dashboard_view(self) -> None:
        account_name = self.account_var.get().strip()
        dashboard = self.ai_trading_brain.get_dashboard(account_name)
        funds = dashboard.get("funds") or {}
        training = dashboard.get("training") or {}
        lines = [
            f"Capital total asignado: {float(funds.get('max_capital_assigned', 0.0) or 0.0):.2f}",
            f"Capital disponible: {float(funds.get('available_capital', 0.0) or 0.0):.2f}",
            f"Capital usado: {float(funds.get('capital_used', 0.0) or 0.0):.2f}",
            f"Ganancia/Perdida del dia: {float(dashboard.get('daily_pnl', 0.0) or 0.0):.2f}",
            f"Ganancia/Perdida total: {float(dashboard.get('total_pnl', 0.0) or 0.0):.2f}",
            f"Posiciones abiertas: {dashboard.get('open_positions', 0)}",
            f"Posiciones en HOLD: {dashboard.get('hold_positions', 0)}",
            f"Senales activas: {dashboard.get('signals_active', 0)}",
            f"Trades ganadores: {dashboard.get('winners', 0)}",
            f"Trades perdedores: {dashboard.get('losers', 0)}",
            f"Win rate: {float(dashboard.get('win_rate', 0.0) or 0.0):.2f}%",
            f"Modelo actual: {training.get('model_version', self.ai_trading_brain.registry.approved_version() or 'heuristic')}",
        ]
        self._set_text_widget(self.ai_dashboard_text, "\n".join(lines))

    def _refresh_ai_signals_view(self) -> None:
        signals = self.ai_trading_brain.list_signals(limit=60)
        if not signals:
            self._set_text_widget(self.ai_signal_text, "Sin señales generadas.")
            self._auto_fill_ai_news_input()
            return

        # Keep one best signal per symbol and rank by confidence score descending.
        by_symbol: dict[str, dict[str, Any]] = {}
        for item in signals:
            symbol = str(item.get("symbol", "N/A"))
            current = by_symbol.get(symbol)
            candidate_score = float(item.get("confidence_score", 0.0) or 0.0)
            current_score = float(current.get("confidence_score", 0.0) or 0.0) if current else -1.0
            if current is None or candidate_score > current_score:
                by_symbol[symbol] = item

        ranked = sorted(
            by_symbol.values(),
            key=lambda row: (
                float(row.get("confidence_score", 0.0) or 0.0),
                str(row.get("timestamp", "")),
            ),
            reverse=True,
        )

        best = ranked[0]
        best_action = str(best.get("signal_type", "")).upper()
        actionable = {"WATCH", "BUY_SMALL", "BUY", "SELL_ALLOWED"}
        best_timestamp = str(best.get("timestamp", "N/A") or "N/A")
        best_openai = best.get("openai_analysis_json") or {}
        board = self.ai_trading_brain.get_scalping_board(limit_stocks=6, limit_cryptos=3)
        stocks_board = board.get("stocks", [])
        cryptos_board = board.get("cryptos", [])

        stock_names = " | ".join(str(item.get("symbol", "N/A")) for item in stocks_board)
        crypto_names = " | ".join(str(item.get("symbol", "N/A")) for item in cryptos_board)
        self.ai_header_stocks_var.set(f"Stocks: {stock_names or '--'}")
        self.ai_header_cryptos_var.set(f"Cryptos: {crypto_names or '--'}")

        if best_action in actionable:
            self._latest_ai_signal_id = int(best.get("id"))
            lines = [
                f"Mejor activo ahora: {best.get('symbol', 'N/A')}",
                f"Generada: {best_timestamp}",
                f"Tipo: {best.get('asset_type', 'N/A')}",
                f"Score: {float(best.get('features_json', {}).get('composite_score', 0.0) or 0.0):.2f}",
                f"Accion: {best.get('signal_type', 'N/A')}",
                f"Razon: {best.get('reason', 'N/A')}",
                f"Precio actual: {float(best.get('entry_price', 0.0) or 0.0):.6f}",
                f"Entrada sugerida: {float(best.get('suggested_limit_price', 0.0) or 0.0):.6f}",
                f"Take profit sugerido: {float(best.get('take_profit_price', 0.0) or 0.0):.6f}",
                f"Average cost: {float(best.get('average_cost', 0.0) or 0.0):.6f}",
                f"Estado de proteccion: {best.get('protection_status', 'N/A')}",
            ]
            if best_openai:
                lines.extend(
                    [
                        f"OpenAI sentiment: {best_openai.get('sentiment', 'N/A')}",
                        f"OpenAI event_type: {best_openai.get('event_type', 'N/A')}",
                        f"OpenAI summary: {best_openai.get('summary', '') or 'N/A'}",
                    ]
                )
            lines.extend(["", "Mejores señales (arriba = mayor score):"])
        else:
            self._latest_ai_signal_id = None
            lines = [
                "No hay activo apto ahora",
                f"Ultima señal apta evaluada: {best_timestamp}",
                "",
                "Mejores señales (arriba = mayor score):",
            ]
        for item in ranked[:10]:
            signal_ts = self._format_iso_local_text(item.get("timestamp", "N/A"))
            lines.append(
                f"- {signal_ts} | {item.get('symbol', 'N/A')} | {item.get('signal_type', 'N/A')} | conf={float(item.get('confidence_score', 0.0) or 0.0):.2f}"
            )

        recent_news = self.ai_trading_brain.database.list_news_events_since(
            since_iso=(datetime.now(timezone.utc) - timedelta(hours=24)).isoformat(),
            limit=8,
        )
        lines.extend(["", "Textos/noticias para OpenAI Analyzer (ultimas 24h):"])
        if not recent_news:
            lines.append("- Sin noticias/textos recientes analizados")
        else:
            for event in recent_news[:8]:
                event_ts = self._format_iso_local_text(event.get("timestamp", "N/A"))
                lines.append(
                    "- "
                    f"{event_ts} | {event.get('symbol', 'N/A')} | {event.get('source', 'N/A')} | "
                    f"sent={float(event.get('sentiment_score', 0.0) or 0.0):.2f} | infl={float(event.get('influence_score', 0.0) or 0.0):.2f}"
                )
                text_preview = str(event.get("title_or_text", "") or "").strip()
                summary = str(event.get("ai_summary", "") or "").strip()
                if text_preview:
                    lines.append(f"  texto: {text_preview[:140]}")
                if summary:
                    lines.append(f"  resumen: {summary[:180]}")
        self._set_text_widget(self.ai_signal_text, "\n".join(lines))
        self._auto_fill_ai_news_input()

    def _auto_fill_ai_news_input(self) -> None:
        if not hasattr(self, "ai_news_input"):
            return

        symbol = self._selected_symbol_for_market()
        since_iso = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

        recent = self.ai_trading_brain.database.list_news_events_since(
            since_iso=since_iso,
            symbol=symbol,
            limit=1,
        )
        if not recent:
            recent = self.ai_trading_brain.database.list_news_events_since(
                since_iso=since_iso,
                limit=1,
            )
        if not recent:
            return

        latest = recent[0]
        source = str(latest.get("source", "news") or "news")
        event_symbol = str(latest.get("symbol", symbol) or symbol)
        body = str(latest.get("title_or_text", "") or "").strip()
        if not body:
            body = str(latest.get("ai_summary", "") or "").strip()
        if not body:
            return

        autofill_text = f"[{event_symbol} | {source}] {body}"
        current_text = self.ai_news_input.get("1.0", tk.END).strip()
        should_replace = (not current_text) or (self._ai_news_autofill_text and current_text == self._ai_news_autofill_text)
        if not should_replace:
            return

        self.ai_news_input.delete("1.0", tk.END)
        self.ai_news_input.insert("1.0", autofill_text)
        self._ai_news_autofill_text = autofill_text

    def _refresh_ai_crypto_monitor_view(self) -> None:
        widget = getattr(self, "ai_crypto_monitor_text", None)
        if widget is None:
            return

        account_name = self.account_var.get().strip()
        if not account_name:
            self._set_text_widget(widget, "Selecciona una cuenta para ver el monitoreo crypto IA.")
            return

        try:
            watchlist = self.ai_trading_brain.database.list_watchlist_assets(active_only=True)
        except Exception as ex:
            self._set_text_widget(widget, f"No se pudo leer watchlist crypto: {ex}")
            return

        crypto_symbols = sorted(
            {
                str(row.get("symbol", "")).upper()
                for row in watchlist
                if str(row.get("symbol", "")).strip() and str(row.get("asset_type", "")).lower() == "crypto"
            }
        )

        if not crypto_symbols:
            self._set_text_widget(widget, "No hay cryptos activas en la watchlist para monitoreo IA.")
            return

        lines: list[str] = []
        lines.append(f"Cuenta: {account_name}")
        lines.append(f"Cryptos monitoreadas: {', '.join(crypto_symbols)}")
        lines.append("")

        for index, symbol in enumerate(crypto_symbols, start=1):
            try:
                payload = self.ai_trading_brain.get_symbol_diagnostics(
                    account_name=account_name,
                    symbol=symbol,
                    asset_type="crypto",
                )
            except Exception as ex:
                lines.append(f"{index}. {symbol}")
                lines.append("1. Ultima señal: N/A")
                lines.append(f"2. Ultimo bloqueo exacto: error_diagnostico:{ex}")
                lines.append("3. Ajuste minimo para pasar de WATCH a BUY: No disponible por error de diagnostico")
                lines.append("")
                continue

            latest_signal = payload.get("latest_signal") or {}
            signal_type = str(latest_signal.get("signal_type", "N/A") or "N/A")
            confidence = float(latest_signal.get("confidence_score", 0.0) or 0.0)
            signal_reason = str(latest_signal.get("reason", "") or payload.get("latest_signal_text", "Sin señal"))
            signal_reason = signal_reason.replace("\n", " ").strip()
            direction = str(payload.get("signal_direction", "N/A") or "N/A")
            runtime_info = payload.get("runtime", {}) or {}
            short_enabled = bool(runtime_info.get("futures_enable_short", False))
            long_enabled = bool(runtime_info.get("futures_enable_long", True))

            latest_decision = payload.get("latest_decision") or {}
            exact_block = str(latest_decision.get("blocked_reason", "") or "").strip()
            if not exact_block:
                blockers = payload.get("entry_blockers", []) or []
                exact_block = str(blockers[0]) if blockers else "Sin bloqueo activo"

            can_enter_now = bool(payload.get("can_enter_now", False))
            recommendations = payload.get("recommendations", []) or []
            if can_enter_now and signal_type in {"BUY", "BUY_SMALL", "SELL_SHORT"}:
                min_adjustment = "Ninguno: ya puede entrar a operar."
            elif recommendations:
                first = recommendations[0]
                setting = str(first.get("setting", "N/A") or "N/A")
                current = first.get("current", "N/A")
                suggested = first.get("suggested", "N/A")
                min_adjustment = f"{setting}: {current} -> {suggested}"
            else:
                min_adjustment = "Sin ajuste sugerido automatico; revisar spread, volumen y bloqueos actuales."

            if signal_type == "AVOID" and direction == "N/A":
                direction = "SIN_ENTRADA"
            if signal_type == "SELL_SHORT" and not short_enabled:
                exact_block = "SHORT deshabilitado en runtime (futures_enable_short=OFF)"
                min_adjustment = "futures_enable_short: OFF -> ON"
            if signal_type in {"BUY", "BUY_SMALL"} and not long_enabled:
                exact_block = "LONG deshabilitado en runtime (futures_enable_long=OFF)"
                min_adjustment = "futures_enable_long: OFF -> ON"

            lines.append(f"{index}. {symbol}")
            lines.append(f"1. Ultima señal: {signal_type} | dirección={direction} | confianza={confidence:.2f} | razon={signal_reason[:180]}")
            lines.append(f"2. Ultimo bloqueo exacto: {exact_block}")
            lines.append(f"3. Ajuste minimo para habilitar entrada LONG/SHORT: {min_adjustment}")
            lines.append("")

        self._set_text_widget(widget, "\n".join(lines).strip())

    def _show_ai_signal_history(self) -> None:
        if self.ai_history_window is not None and self.ai_history_window.winfo_exists():
            self.ai_history_window.deiconify()
            self.ai_history_window.lift()
            self.ai_history_window.focus_force()
            self._refresh_ai_history_window()
            return

        window = tk.Toplevel(self.root)
        window.title("Historial de operaciones IA")
        self._fit_window_to_screen(window, preferred_width=1280, preferred_height=760, min_width=980, min_height=620)

        container = ttk.Frame(window, padding=12)
        container.pack(fill="both", expand=True)

        filters = ttk.LabelFrame(container, text="Filtros")
        filters.pack(fill="x", pady=(0, 8))
        ttk.Label(filters, text="Cuenta").pack(side="left", padx=(8, 4), pady=8)
        account_combo = ttk.Combobox(filters, textvariable=self.ai_history_account_filter_var, state="readonly", width=18)
        account_combo.pack(side="left", padx=(0, 8), pady=8)
        ttk.Label(filters, text="Lado").pack(side="left", padx=(8, 4), pady=8)
        side_combo = ttk.Combobox(filters, textvariable=self.ai_history_side_filter_var, state="readonly", width=10)
        side_combo.pack(side="left", padx=(0, 8), pady=8)
        ttk.Label(filters, text="Motor").pack(side="left", padx=(8, 4), pady=8)
        engine_combo = ttk.Combobox(filters, textvariable=self.ai_history_engine_filter_var, state="readonly", width=12)
        engine_combo.pack(side="left", padx=(0, 8), pady=8)
        ttk.Label(filters, text="Status").pack(side="left", padx=(8, 4), pady=8)
        status_combo = ttk.Combobox(filters, textvariable=self.ai_history_status_filter_var, state="readonly", width=14)
        status_combo.pack(side="left", padx=(0, 8), pady=8)
        ttk.Button(filters, text="Refrescar", command=lambda: self._run_async(self._refresh_ai_history_window)).pack(side="right", padx=(8, 8), pady=8)

        columns = ("timestamp", "account", "symbol", "side", "status", "engine", "model", "qty", "filled", "order")
        tree = ttk.Treeview(container, columns=columns, show="headings", height=22)
        headings = {
            "timestamp": "Fecha",
            "account": "Cuenta",
            "symbol": "Símbolo",
            "side": "Lado",
            "status": "Status",
            "engine": "Motor",
            "model": "Modelo",
            "qty": "Qty",
            "filled": "Filled",
            "order": "Order ID",
        }
        widths = {
            "timestamp": 150,
            "account": 120,
            "symbol": 90,
            "side": 70,
            "status": 100,
            "engine": 90,
            "model": 170,
            "qty": 90,
            "filled": 90,
            "order": 210,
        }
        for column in columns:
            tree.heading(column, text=headings[column])
            tree.column(column, width=widths[column], anchor="w")

        scrollbar = ttk.Scrollbar(container, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)
        tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        account_combo.bind("<<ComboboxSelected>>", lambda _event: self._refresh_ai_history_window())
        side_combo.bind("<<ComboboxSelected>>", lambda _event: self._refresh_ai_history_window())
        engine_combo.bind("<<ComboboxSelected>>", lambda _event: self._refresh_ai_history_window())
        status_combo.bind("<<ComboboxSelected>>", lambda _event: self._refresh_ai_history_window())

        self.ai_history_window = window
        self.ai_history_tree = tree
        window.protocol("WM_DELETE_WINDOW", self._on_close_ai_history_window)
        self._refresh_ai_history_window()
        return

    def _on_close_ai_history_window(self) -> None:
        if self.ai_history_window is not None and self.ai_history_window.winfo_exists():
            self.ai_history_window.withdraw()

    def _refresh_ai_history_window(self) -> None:
        tree = self.ai_history_tree
        if tree is None:
            return

        account_names = list((self.account_profiles or {}).keys())
        selected_account = str(self.ai_history_account_filter_var.get() or "Todas")
        selected_side = str(self.ai_history_side_filter_var.get() or "Todos")
        selected_engine = str(self.ai_history_engine_filter_var.get() or "Todos")
        selected_status = str(self.ai_history_status_filter_var.get() or "Todos")

        operations: list[dict[str, Any]] = []
        for account_name in account_names:
            try:
                rows = self.ai_trading_brain.list_history(account_name, limit=300, actor_filter="ia")
            except Exception:
                continue
            for row in rows:
                payload = dict(row)
                payload["account_name"] = account_name
                operations.append(payload)

        account_values = ["Todas"] + sorted({str(item.get("account_name", "")) for item in operations if str(item.get("account_name", ""))})
        side_values = ["Todos"] + sorted({str(item.get("side", "")) for item in operations if str(item.get("side", ""))})
        engine_values = ["Todos"] + sorted({str(item.get("decision_engine", "unknown") or "unknown") for item in operations})
        status_values = ["Todos"] + sorted({str(item.get("status", "")) for item in operations if str(item.get("status", ""))})

        for widget, values, current in (
            (self.ai_history_window.winfo_children()[0].winfo_children()[0].winfo_children()[1], account_values, selected_account),
            (self.ai_history_window.winfo_children()[0].winfo_children()[0].winfo_children()[3], side_values, selected_side),
            (self.ai_history_window.winfo_children()[0].winfo_children()[0].winfo_children()[5], engine_values, selected_engine),
            (self.ai_history_window.winfo_children()[0].winfo_children()[0].winfo_children()[7], status_values, selected_status),
        ):
            widget.configure(values=values)
            if current not in values:
                if widget is self.ai_history_window.winfo_children()[0].winfo_children()[0].winfo_children()[1]:
                    self.ai_history_account_filter_var.set("Todas")
                elif widget is self.ai_history_window.winfo_children()[0].winfo_children()[0].winfo_children()[3]:
                    self.ai_history_side_filter_var.set("Todos")
                elif widget is self.ai_history_window.winfo_children()[0].winfo_children()[0].winfo_children()[5]:
                    self.ai_history_engine_filter_var.set("Todos")
                else:
                    self.ai_history_status_filter_var.set("Todos")

        filtered = []
        for item in operations:
            if self.ai_history_account_filter_var.get() != "Todas" and str(item.get("account_name", "")) != self.ai_history_account_filter_var.get():
                continue
            if self.ai_history_side_filter_var.get() != "Todos" and str(item.get("side", "")) != self.ai_history_side_filter_var.get():
                continue
            if self.ai_history_engine_filter_var.get() != "Todos" and str(item.get("decision_engine", "unknown") or "unknown") != self.ai_history_engine_filter_var.get():
                continue
            if self.ai_history_status_filter_var.get() != "Todos" and str(item.get("status", "")) != self.ai_history_status_filter_var.get():
                continue
            filtered.append(item)

        for item_id in tree.get_children():
            tree.delete(item_id)

        for item in filtered:
            tree.insert(
                "",
                tk.END,
                values=(
                    str(item.get("timestamp", "N/A")),
                    str(item.get("account_name", "N/A")),
                    str(item.get("symbol", "N/A")),
                    str(item.get("side", "N/A")).upper(),
                    str(item.get("status", "N/A")),
                    str(item.get("decision_engine", "unknown") or "unknown"),
                    str(item.get("model_version_display", "") or "N/A"),
                    f"{float(item.get('qty', 0.0) or 0.0):.6f}",
                    f"{float(item.get('filled_price', 0.0) or 0.0):.6f}",
                    str(item.get("broker_order_id", "N/A")),
                ),
            )

        def _fmt_ts(value: Any) -> str:
            raw = str(value or "")
            if not raw:
                return "N/A"
            try:
                parsed = datetime.fromisoformat(raw)
            except ValueError:
                return raw
            return parsed.strftime("%Y-%m-%d %H:%M:%S")

        operations: list[dict[str, Any]] = []
        account_profiles = self.account_profiles or {}

        generic_names = {"paper", "real", "cuenta actual"}
        canonical_by_fingerprint: dict[tuple[str, str, str], str] = {}
        for profile_name, profile_payload in account_profiles.items():
            fingerprint = (
                str(profile_payload.get("endpoint", "") or "").strip(),
                str(profile_payload.get("key", "") or "").strip(),
                str(profile_payload.get("secret", "") or "").strip(),
            )
            if not all(fingerprint):
                continue
            current = canonical_by_fingerprint.get(fingerprint)
            if current is None:
                canonical_by_fingerprint[fingerprint] = profile_name
                continue
            current_is_generic = str(current).strip().lower() in generic_names
            candidate_is_generic = str(profile_name).strip().lower() in generic_names
            if current_is_generic and not candidate_is_generic:
                canonical_by_fingerprint[fingerprint] = profile_name

        for account_name in account_profiles.keys():
            try:
                rows = self.ai_trading_brain.list_history(account_name, limit=80, actor_filter="ia")
            except Exception:
                continue

            profile = account_profiles.get(account_name, {})
            mode = str(profile.get("mode", "N/A"))
            fingerprint = (
                str(profile.get("endpoint", "") or "").strip(),
                str(profile.get("key", "") or "").strip(),
                str(profile.get("secret", "") or "").strip(),
            )
            display_account_name = canonical_by_fingerprint.get(fingerprint, account_name)
            for row in rows:
                side = str(row.get("side", "")).lower().strip()
                if side not in {"buy", "sell"}:
                    continue
                operations.append(
                    {
                        "account_name": display_account_name,
                        "mode": mode,
                        "timestamp": str(row.get("timestamp", "")),
                        "symbol": str(row.get("symbol", "N/A")),
                        "asset_type": str(row.get("asset_type", "N/A")),
                        "side": side.upper(),
                        "qty": float(row.get("qty", 0.0) or 0.0),
                        "limit_price": float(row.get("limit_price", 0.0) or 0.0),
                        "filled_price": float(row.get("filled_price", 0.0) or 0.0),
                        "status": str(row.get("status", "N/A")),
                        "initiated_by": str(row.get("initiated_by", "unknown") or "unknown"),
                        "decision_engine": str(row.get("decision_engine", "unknown") or "unknown"),
                        "model_version_display": str(row.get("model_version_display", "") or ""),
                        "broker_order_id": str(row.get("broker_order_id", "") or "N/A"),
                        "signal_id": row.get("signal_id", "N/A"),
                    }
                )

        operations.sort(key=lambda item: str(item.get("timestamp", "")), reverse=True)
        operations = operations[:120]

        if not operations:
            self._set_text_widget(
                self.ai_signal_text,
                "Sin operaciones reales registradas todavia (paper/live).\n"
                "Este panel ahora muestra solo ordenes enviadas al broker (BUY/SELL), no WATCH/RETENIDO hipotetico.",
            )
            return

        lines: list[str] = []
        lines.append("HISTORIAL DE OPERACIONES REALES (PAPER/LIVE)")
        lines.append("=")
        lines.append(f"Total mostrado: {len(operations)}")
        lines.append("Solo incluye BUY/SELL enviados al broker; no incluye WATCH/RETENIDO hipotetico.")
        lines.append("")

        for item in operations:
            lines.append(
                f"{_fmt_ts(item.get('timestamp'))} | cuenta={item.get('account_name', 'N/A')} ({item.get('mode', 'N/A')}) | "
                f"{item.get('symbol', 'N/A')} ({item.get('asset_type', 'N/A')}) | side={item.get('side', 'N/A')} | "
                f"qty={float(item.get('qty', 0.0) or 0.0):.6f} | status={item.get('status', 'N/A')} | "
                f"origen={item.get('initiated_by', 'unknown')} | motor={item.get('decision_engine', 'unknown')} | modelo={item.get('model_version_display', 'N/A') or 'N/A'}"
            )
            lines.append(
                f"  limit={float(item.get('limit_price', 0.0) or 0.0):.6f} | "
                f"filled={float(item.get('filled_price', 0.0) or 0.0):.6f} | "
                f"signal={item.get('signal_id', 'N/A')} | order={item.get('broker_order_id', 'N/A')}"
            )
            lines.append("")

        self._set_text_widget(self.ai_signal_text, "\n".join(lines))

    def _refresh_ai_history_view(self) -> None:
        history = self.ai_trading_brain.list_history(self.account_var.get().strip(), limit=40, actor_filter="ia")
        if not history:
            self._set_text_widget(self.ai_history_text, "Sin trades IA registrados.")
            return
        lines = []
        for item in history:
            engine = str(item.get('decision_engine', 'unknown') or 'unknown')
            model_version = str(item.get('model_version_display', '') or '')
            model_text = model_version if model_version else 'N/A'
            lines.append(
                f"{item.get('timestamp', 'N/A')} | {item.get('symbol', 'N/A')} | {item.get('side', 'N/A')} | qty={float(item.get('qty', 0.0) or 0.0):.6f} | filled={float(item.get('filled_price', 0.0) or 0.0):.6f} | status={item.get('status', 'N/A')} | motor={engine} | modelo={model_text} | origen={item.get('initiated_by', 'unknown')}"
            )
        self._set_text_widget(self.ai_history_text, "\n".join(lines))

    def _refresh_ai_model_view(self, status: dict[str, Any] | None = None) -> None:
        if not hasattr(self, "ai_model_text"):
            return
        training = self.ai_trading_brain.database.latest_training_run() or {}
        if not training:
            self._set_text_widget(
                self.ai_model_text,
                "Modelo no entrenado todavía.\nNo hay suficientes datos para mostrar métricas.",
            )
            return
        if status is None:
            status = self.ai_trading_brain.get_automation_status(self.account_var.get().strip())
        training_cycle_seconds = float(status.get("training_cycle_seconds", 0.0) or 0.0)
        training_elapsed_seconds = float(status.get("training_elapsed_seconds", 0.0) or 0.0)
        training_remaining_seconds = float(status.get("training_remaining_seconds", 0.0) or 0.0)
        training_progress_pct = float(status.get("training_progress_pct", 0.0) or 0.0)
        training_last_error = str(status.get("training_last_error", "") or "").strip()

        candidates_payload = self.ai_trading_brain.list_model_candidates(limit=20)
        approved = str(candidates_payload.get("approved", "") or "")
        approved_valid = bool(candidates_payload.get("approved_valid", False))
        latest = str(candidates_payload.get("latest", "") or "")
        frozen = str(candidates_payload.get("frozen", "") or "")
        candidate_rows = list(candidates_payload.get("rows", []))

        selected_version = ""
        try:
            selected_version = self._selected_ai_model_version()
        except Exception:
            selected_version = ""
        if not selected_version and candidate_rows:
            selected_version = str(candidate_rows[0].get("model_version", "") or "")

        selected_row = None
        for row in candidate_rows:
            if str(row.get("model_version", "") or "") == selected_version:
                selected_row = row
                break
        if selected_row is None:
            selected_row = training
            selected_version = str(selected_row.get("model_version", latest or approved or "") or "")

        pending_version = ""
        if frozen and frozen != approved:
            pending_version = frozen
        elif latest and latest != approved:
            pending_version = latest
        pending_candidate = bool(pending_version)

        approved_run = self._training_run_by_version(approved) if approved else None
        pending_run = self._training_run_by_version(pending_version) if pending_candidate else None
        evaluation_row = pending_run or selected_row or training
        evaluation_version = str((evaluation_row or {}).get("model_version", "") or selected_version or pending_version or "N/A")
        recommendation = self._build_model_approval_recommendation(
            latest_run=evaluation_row,
            approved_run=approved_run,
            has_pending=bool(evaluation_version and evaluation_version != approved and evaluation_version != "N/A"),
        )
        governance_event = self._latest_model_governance_event()
        ranked_candidates = sorted(
            candidate_rows,
            key=lambda row: self._score_model_candidate(row),
            reverse=True,
        )
        best_candidate = self._best_approvable_candidate(ranked_candidates, approved_run)
        if best_candidate is not None:
            best_version = str(best_candidate.get("model_version", "") or "")
            if best_version and evaluation_version and best_version != evaluation_version:
                rec_action = str(recommendation.get("action", "esperar")).lower()
                if rec_action == "aprobar":
                    recommendation = {
                        "action": "evaluar",
                        "reason": (
                            f"Existe un candidato mejor ({best_version}) que el evaluado ({evaluation_version}); "
                            "revisar y aprobar solo el mejor."
                        ),
                        "comparison": recommendation.get("comparison", "N/A"),
                    }
        selected_score = self._score_model_candidate(selected_row or training)
        approved_score = self._score_model_candidate(approved_run) if approved_run is not None else 0.0
        approved_reference_exists = bool(approved)
        approved_baseline_missing = approved_reference_exists and approved_run is None
        selected_better_than_approved = False
        if approved_run is None:
            selected_better_than_approved = True
        elif selected_version == approved:
            selected_better_than_approved = False
        else:
            selected_better_than_approved = selected_score > approved_score

        recommended_action = str(recommendation.get("action", "esperar")).lower()
        if selected_version and selected_version == approved and approved_valid:
            recommendation_line = "RECOMENDACION: MODELO APROBADO ACTUAL"
            self._apply_model_recommendation_semaphore("VERDE", "#1f7a1f")
        elif recommended_action == "aprobar":
            recommendation_line = "RECOMENDACION: APROBAR modelo candidato"
            self._apply_model_recommendation_semaphore("VERDE", "#1f7a1f")
        elif recommended_action == "evaluar":
            recommendation_line = "RECOMENDACION: REVISAR manualmente antes de aprobar"
            self._apply_model_recommendation_semaphore("AMARILLO", "#9a6700")
        else:
            recommendation_line = "RECOMENDACION: ESPERAR siguiente ciclo"
            self._apply_model_recommendation_semaphore("ROJO", "#8a1c1c")

        current_version = approved if approved_valid else (latest or "sin_aprobado_valido")
        selected_state = (
            "APROBADO"
            if selected_version == approved and approved_valid
            else ("APROBADO INVALIDO" if selected_version == approved and not approved_valid else ("CONGELADO" if selected_version == frozen else ("LATEST" if selected_version == latest else "CANDIDATO")))
        )
        selected_alias = str((selected_row or {}).get("alias", "") or "").strip()
        self.ai_model_alias_var.set(selected_alias)
        cycle_text = self._format_duration_hhmmss(training_cycle_seconds)
        elapsed_text = self._format_duration_hhmmss(training_elapsed_seconds)
        remaining_text = self._format_duration_hhmmss(training_remaining_seconds)
        selected_training_status = "FINALIZADO" if selected_version else "N/A"
        approved_score_text = f"{approved_score:.3f}" if approved_run is not None else "N/A (sin baseline histórico)"
        approved_pf_text = f"{float((approved_run or {}).get('profit_factor', 0.0) or 0.0):.4f}" if approved_run is not None else "N/A"
        approved_dd_text = f"{float((approved_run or {}).get('max_drawdown', 0.0) or 0.0):.4f}" if approved_run is not None else "N/A"
        approved_recall_text = f"{float((approved_run or {}).get('recall', 0.0) or 0.0):.4f}" if approved_run is not None else "N/A"
        approved_precision_text = f"{float((approved_run or {}).get('precision', 0.0) or 0.0):.4f}" if approved_run is not None else "N/A"
        lines = [
            f"Version actual: {current_version}",
            f"Version seleccionada: {selected_version or 'N/A'}",
            f"Nombre personalizado: {selected_alias or 'N/A'}",
            f"Estado seleccion: {selected_state}",
            f"Entrenamiento del modelo seleccionado: {selected_training_status}",
            f"Ultima fecha de entrenamiento: {self._format_iso_local_text((selected_row or training).get('timestamp', 'N/A'))}",
            f"Modelo candidato pendiente: {'SI' if pending_candidate else 'NO'}",
            f"Version candidato: {pending_version if pending_candidate else 'N/A'}",
            f"Candidato congelado: {frozen or 'N/A'}",
            "",
            "Ciclo actual del trainer automatico (no del modelo seleccionado):",
            f"Duracion configurada del ciclo: {cycle_text}",
            f"Tiempo transcurrido del ciclo actual: {elapsed_text} ({training_progress_pct:.1f}%)",
            f"Tiempo restante estimado del ciclo actual: {remaining_text}",
            f"Ultimo error del trainer: {training_last_error or 'Ninguno'}",
            f"Cantidad de senales usadas: {(selected_row or training).get('number_of_samples', 0)}",
            f"Win rate: {float((selected_row or training).get('win_rate', 0.0) or 0.0):.4f}",
            f"Accuracy: {float((selected_row or training).get('accuracy', 0.0) or 0.0):.4f}",
            f"Precision: {float((selected_row or training).get('precision', 0.0) or 0.0):.4f}",
            f"Recall: {float((selected_row or training).get('recall', 0.0) or 0.0):.4f}",
            f"Profit factor: {float((selected_row or training).get('profit_factor', 0.0) or 0.0):.4f}",
            f"Max drawdown: {float((selected_row or training).get('max_drawdown', 0.0) or 0.0):.4f}",
            f"Puntaje compuesto: {self._score_model_candidate(selected_row or training):.3f}",
            "",
            recommendation_line,
            f"Motivo: {recommendation.get('reason', 'sin recomendacion')}",
            f"Candidato evaluado para recomendacion: {evaluation_version}",
            f"¿Es mejor que el aprobado actual?: {'SI' if selected_better_than_approved else 'NO'}",
            f"Score seleccionado vs aprobado: {selected_score:.3f} vs {approved_score_text}",
            f"Profit factor seleccionado vs aprobado: {float((selected_row or training).get('profit_factor', 0.0) or 0.0):.4f} vs {approved_pf_text}",
            f"Drawdown seleccionado vs aprobado: {float((selected_row or training).get('max_drawdown', 0.0) or 0.0):.4f} vs {approved_dd_text}",
            f"Recall seleccionado vs aprobado: {float((selected_row or training).get('recall', 0.0) or 0.0):.4f} vs {approved_recall_text}",
            f"Precision seleccionado vs aprobado: {float((selected_row or training).get('precision', 0.0) or 0.0):.4f} vs {approved_precision_text}",
            f"Comparativa vs aprobado: {recommendation.get('comparison', 'N/A')}",
            f"Ultimo evento de aprobacion/rollback: {governance_event}",
            f"Mejor candidato actual: {self._describe_best_candidate(best_candidate)}",
            "",
            "Candidatos recientes (fecha | version | estado | semaforo | score | sugerencia):",
        ]
        if approved_baseline_missing:
            lines.insert(1, "Advertencia: la versión aprobada no tiene baseline histórico en la base de entrenamiento.")
        if approved and not approved_valid:
            lines.insert(1, f"Advertencia: la versión aprobada referenciada ({approved}) es inválida para comparar o usar.")

        for row in candidate_rows[:10]:
            version = str(row.get("model_version", "") or "")
            alias = str(row.get("alias", "") or "").strip()
            ts = self._format_iso_local_text(row.get("timestamp", "N/A"))
            is_approved = bool(row.get("is_approved", False))
            is_frozen = bool(row.get("is_frozen", False))
            is_selected = version == selected_version
            guidance = self._candidate_keep_delete_guidance(row=row, approved_version=approved, best_candidate=best_candidate)
            if is_approved:
                state = "APROBADO"
                sem = "VERDE"
            else:
                rec = self._build_model_approval_recommendation(
                    latest_run=row,
                    approved_run=approved_run,
                    has_pending=True,
                )
                action = str(rec.get("action", "esperar")).lower()
                sem = "VERDE" if action == "aprobar" else ("AMARILLO" if action == "evaluar" else "ROJO")
                if guidance.startswith("ELIMINAR"):
                    sem = "ROJO"
                state = "CONGELADO" if is_frozen else "CANDIDATO"
            score = self._score_model_candidate(row)
            prefix = ">" if is_selected else "-"
            label = f"{version} ({alias})" if alias else version
            lines.append(f"{prefix} {ts} | {label} | {state} | {sem} | {score:.3f} | {guidance}")

        lines.extend(
            [
                "",
            "Modo:",
            "- Entrenamiento automático continuo",
            "- Aprobación manual opcional",
            "- Rollback manual opcional con fecha/hora visible",
            ]
        )
        self._set_text_widget(self.ai_model_text, "\n".join(lines))
        self._refresh_ai_model_candidates()

    def _refresh_ai_model_candidates(self) -> None:
        payload = self.ai_trading_brain.list_model_candidates(limit=20)
        rows = list(payload.get("rows", []))
        frozen = str(payload.get("frozen", "") or "")
        latest = str(payload.get("latest", "") or "")

        values: list[str] = []
        mapping: dict[str, str] = {}
        for row in rows:
            version = str(row.get("model_version", "") or "")
            if not version:
                continue
            alias = str(row.get("alias", "") or "").strip()
            ts = self._format_iso_local_text(row.get("timestamp", "N/A"))
            label = f"{alias} | {version} | {ts}" if alias else f"{version} | {ts}"
            values.append(label)
            mapping[label] = version

        self._ai_model_versions_values = values
        self._ai_model_version_map = mapping
        if hasattr(self, "ai_model_version_combo"):
            self.ai_model_version_combo.configure(values=values)
        if hasattr(self, "ai_model_version_combo_tab"):
            self.ai_model_version_combo_tab.configure(values=values)

        current_value = self.ai_model_selected_var.get().strip()
        if current_value in values:
            return

        target_version = frozen or latest
        if target_version:
            for label, version in mapping.items():
                if version == target_version:
                    self.ai_model_selected_var.set(label)
                    return
        if values:
            self.ai_model_selected_var.set(values[0])
        else:
            self.ai_model_selected_var.set("")

    def _selected_ai_model_version(self) -> str:
        selected = self.ai_model_selected_var.get().strip()
        if not selected:
            raise ValueError("No hay version seleccionada")
        return self._ai_model_version_map.get(selected, selected.split("|", 1)[0].strip())

    def _approve_ai_model_selected(self) -> None:
        version = self._selected_ai_model_version()
        approved = self.ai_trading_brain.approve_model_version(version)
        self._refresh_ai_views()
        self.root.after(0, self._show_success, f"Modelo aprobado: {approved}", False)

    def _freeze_ai_model_selected(self) -> None:
        version = self._selected_ai_model_version()
        frozen = self.ai_trading_brain.freeze_candidate_version(version)
        self._refresh_ai_views()
        self.root.after(0, self._show_success, f"Candidato congelado: {frozen}", False)

    def _unfreeze_ai_model_candidate(self) -> None:
        self.ai_trading_brain.clear_frozen_candidate()
        self._refresh_ai_views()
        self.root.after(0, self._show_success, "Candidato descongelado", False)

    def _delete_ai_model_selected(self) -> None:
        version = self._selected_ai_model_version()
        if not messagebox.askyesno(
            "Eliminar modelo",
            f"Deseas eliminar la version {version}? Esta accion no se puede deshacer.",
        ):
            return
        deleted = self.ai_trading_brain.delete_model_version(version)
        self._refresh_ai_views()
        self.root.after(0, self._show_success, f"Modelo eliminado: {deleted}", False)

    def _save_ai_model_alias(self) -> None:
        version = self._selected_ai_model_version()
        alias = self.ai_model_alias_var.get().strip()
        saved = self.ai_trading_brain.set_model_alias(version, alias)
        self._refresh_ai_views()
        if saved:
            self.root.after(0, self._show_success, f"Nombre guardado para {version}: {saved}", False)
        else:
            self.root.after(0, self._show_success, f"Nombre personalizado eliminado para {version}", False)

    def _apply_model_recommendation_semaphore(self, status_name: str, color: str) -> None:
        self.ai_model_reco_var.set(f"Semaforo modelo: {status_name}")
        if hasattr(self, "ai_model_semaphore_label"):
            self.ai_model_semaphore_label.configure(bg=color, activebackground=color)

    def _format_iso_local_text(self, value: Any) -> str:
        raw = str(value or "")
        if not raw:
            return "N/A"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return raw
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        local_dt = parsed.astimezone()
        return local_dt.strftime("%Y-%m-%d %H:%M:%S %Z")

    def _format_duration_hhmmss(self, seconds_value: float) -> str:
        seconds = max(int(seconds_value or 0), 0)
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"

    def _training_run_by_version(self, model_version: str) -> dict[str, Any] | None:
        if not model_version:
            return None
        for row in self.ai_trading_brain.database.list_training_runs(limit=300):
            if str(row.get("model_version", "")) == str(model_version):
                return row
        return None

    def _build_model_approval_recommendation(
        self,
        latest_run: dict[str, Any],
        approved_run: dict[str, Any] | None,
        has_pending: bool,
    ) -> dict[str, str]:
        if not has_pending:
            return {
                "action": "esperar",
                "reason": "No hay modelo nuevo pendiente de aprobacion.",
                "comparison": "N/A",
            }

        candidate_accuracy = float(latest_run.get("accuracy", 0.0) or 0.0)
        candidate_precision = float(latest_run.get("precision", 0.0) or 0.0)
        candidate_recall = float(latest_run.get("recall", 0.0) or 0.0)
        candidate_win_rate = float(latest_run.get("win_rate", 0.0) or 0.0)
        candidate_profit_factor = float(latest_run.get("profit_factor", 0.0) or 0.0)
        candidate_drawdown = float(latest_run.get("max_drawdown", 0.0) or 0.0)

        if approved_run is None:
            if (
                candidate_accuracy >= 0.55
                and candidate_precision >= 0.50
                and candidate_recall >= 0.45
                and candidate_profit_factor >= 1.00
            ):
                return {
                    "action": "aprobar",
                    "reason": "No hay modelo aprobado y el candidato cumple umbrales minimos.",
                    "comparison": "Sin baseline previo",
                }
            return {
                "action": "esperar",
                "reason": "No hay baseline aprobado y el candidato no cumple umbrales minimos.",
                "comparison": "Sin baseline previo",
            }

        approved_accuracy = float(approved_run.get("accuracy", 0.0) or 0.0)
        approved_precision = float(approved_run.get("precision", 0.0) or 0.0)
        approved_recall = float(approved_run.get("recall", 0.0) or 0.0)
        approved_win_rate = float(approved_run.get("win_rate", 0.0) or 0.0)
        approved_profit_factor = float(approved_run.get("profit_factor", 0.0) or 0.0)
        approved_drawdown = float(approved_run.get("max_drawdown", 0.0) or 0.0)

        d_acc = candidate_accuracy - approved_accuracy
        d_pre = candidate_precision - approved_precision
        d_rec = candidate_recall - approved_recall
        d_win = candidate_win_rate - approved_win_rate
        d_pf = candidate_profit_factor - approved_profit_factor
        d_dd = candidate_drawdown - approved_drawdown

        improvements = 0
        if d_acc >= 0.005:
            improvements += 1
        if d_pre >= 0.01:
            improvements += 1
        if d_rec >= 0.01:
            improvements += 1
        if d_win >= 0.01:
            improvements += 1
        if d_pf >= 0.05:
            improvements += 1
        if d_dd <= -0.02:
            improvements += 1

        high_risk = candidate_profit_factor < 0.95 or candidate_drawdown > (approved_drawdown * 1.2 + 0.02)
        comparison = (
            f"acc {d_acc:+.4f}, prec {d_pre:+.4f}, recall {d_rec:+.4f}, "
            f"win {d_win:+.4f}, pf {d_pf:+.4f}, dd {d_dd:+.4f}"
        )

        if high_risk:
            return {
                "action": "esperar",
                "reason": "El candidato aumenta riesgo (profit factor bajo o drawdown mayor).",
                "comparison": comparison,
            }
        if improvements >= 3:
            return {
                "action": "aprobar",
                "reason": f"Mejora suficiente frente al aprobado ({improvements}/6 metricas).",
                "comparison": comparison,
            }
        if improvements == 2:
            return {
                "action": "evaluar",
                "reason": "Mejora parcial; conviene validar un ciclo mas o revisar manualmente.",
                "comparison": comparison,
            }
        return {
            "action": "esperar",
            "reason": "No mejora de forma consistente frente al modelo aprobado.",
            "comparison": comparison,
        }

    def _score_model_candidate(self, row: dict[str, Any]) -> float:
        accuracy = float(row.get("accuracy", 0.0) or 0.0)
        precision = float(row.get("precision", 0.0) or 0.0)
        recall = float(row.get("recall", 0.0) or 0.0)
        win_rate = float(row.get("win_rate", 0.0) or 0.0)
        profit_factor = float(row.get("profit_factor", 0.0) or 0.0)
        drawdown = abs(float(row.get("max_drawdown", 0.0) or 0.0))

        pf_score = min(profit_factor / 40.0, 1.0)
        drawdown_score = max(0.0, 1.0 - min(drawdown / 0.25, 1.0))
        return (
            accuracy * 0.18
            + precision * 0.17
            + recall * 0.17
            + win_rate * 0.13
            + pf_score * 0.25
            + drawdown_score * 0.10
        )

    def _describe_best_candidate(self, row: dict[str, Any] | None) -> str:
        if not row:
            return "Ninguno claramente superior al aprobado"
        version = str(row.get("model_version", "") or "")
        alias = str(row.get("alias", "") or "").strip()
        score = self._score_model_candidate(row)
        label = f"{version} ({alias})" if alias else version
        return f"{label} | score={score:.3f} | pf={float(row.get('profit_factor', 0.0) or 0.0):.4f}"

    def _best_approvable_candidate(
        self,
        ranked_candidates: list[dict[str, Any]],
        approved_run: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        for row in ranked_candidates:
            rec = self._build_model_approval_recommendation(
                latest_run=row,
                approved_run=approved_run,
                has_pending=True,
            )
            if str(rec.get("action", "esperar")).lower() in {"aprobar", "evaluar"}:
                return row
        return None

    def _candidate_keep_delete_guidance(
        self,
        row: dict[str, Any],
        approved_version: str,
        best_candidate: dict[str, Any] | None,
    ) -> str:
        version = str(row.get("model_version", "") or "")
        if version == approved_version:
            return "CONSERVAR (aprobado)"
        if best_candidate is not None and version == str(best_candidate.get("model_version", "") or ""):
            return "CONSERVAR (mejor)"
        rec = self._build_model_approval_recommendation(
            latest_run=row,
            approved_run=self._training_run_by_version(approved_version) if approved_version else None,
            has_pending=True,
        )
        if str(rec.get("action", "esperar")).lower() == "esperar":
            return "ELIMINAR"
        profit_factor = float(row.get("profit_factor", 0.0) or 0.0)
        recall = float(row.get("recall", 0.0) or 0.0)
        drawdown = abs(float(row.get("max_drawdown", 0.0) or 0.0))
        if profit_factor < 1.0 or recall < 0.50 or drawdown > 0.18:
            return "ELIMINAR"
        return "REVISAR"

    def _latest_model_governance_event(self) -> str:
        rows = self.ai_trading_brain.database.latest_decision_logs(limit=80)
        for row in rows:
            decision = str(row.get("decision", "") or "").upper().strip()
            if decision not in {"MODEL_APPROVED", "MODEL_ROLLBACK", "MODEL_FROZEN", "MODEL_UNFROZEN"}:
                continue
            ts = self._format_iso_local_text(row.get("timestamp", "N/A"))
            reason = str(row.get("reason", "") or "").strip()
            return f"{ts} | {decision} | {reason or 'sin detalle'}"
        return "N/A"

    def _refresh_ai_security_view(self) -> None:
        account_name = self._ai_automation_account_name()
        security = self.ai_trading_brain.get_security_state(account_name)
        live_status = "ON" if security.get("live_trading_enabled") else "OFF"
        if security.get("paper_trading") and not security.get("live_trading_enabled"):
            live_status = "OFF (normal en paper)"
        lines = [
            f"Cuenta seguridad IA: {account_name or 'N/A'}",
            f"Kill switch: {'ON' if security.get('kill_switch') else 'OFF'}",
            f"Trading real/live activado: {live_status}",
            f"Paper trading activado: {'ON' if security.get('paper_trading') else 'OFF'}",
            f"VPN requerida: {'SI' if security.get('vpn_required') else 'NO'}",
            f"VPN lista para operar: {'SI' if security.get('vpn_ready') else 'NO'}",
            f"Proveedor VPN: {security.get('vpn_provider', 'N/A')}",
            f"Pais VPN requerido: {security.get('vpn_required_country', 'Dominican Republic')}",
            f"Pais VPN actual: {security.get('vpn_country', 'N/A') or 'N/A'}",
            f"Estado VPN: {security.get('vpn_status', 'N/A')}",
            f"Motivo VPN: {security.get('vpn_reason', 'N/A') or 'N/A'}",
            f"Auto trading stocks: {'ON' if security.get('auto_trade_stocks_enabled') else 'PAUSADO'}",
            f"Auto trading cryptos: {'ON' if security.get('auto_trade_cryptos_enabled') else 'PAUSADO'}",
            f"Modo solo señales: {'ON' if security.get('signal_only_mode') else 'OFF'}",
            f"Aprobacion manual: {'ON' if security.get('manual_approval_required') else 'OFF'}",
            f"Perdida maxima diaria: {float(security.get('max_daily_loss', 0.0) or 0.0):.2f}",
            f"Capital maximo por posicion: {float(security.get('max_position_size', 0.0) or 0.0):.2f}",
            f"API Alpaca OK: {'SI' if security.get('api_keys_ok') else 'NO'}",
            f"API OpenAI OK: {'SI' if security.get('openai_key_ok') else 'NO'}",
            "",
            "Logs recientes:",
        ]
        for item in security.get("recent_logs", [])[:10]:
            lines.append(
                f"- {item.get('timestamp', 'N/A')} | {item.get('symbol', 'N/A')} | {item.get('decision', 'N/A')} | blocked={item.get('blocked_reason', '')}"
            )
        self._set_text_widget(self.ai_security_text, "\n".join(lines))

    def _generate_ai_signal(self) -> None:
        symbol = self._selected_symbol_for_market()
        asset_type = self._asset_type_for_selection(symbol)
        text_context = self.ai_news_input.get("1.0", tk.END).strip()
        signal = self.ai_trading_brain.generate_signal(
            symbol=symbol,
            asset_type=asset_type,
            account_name=self.account_var.get().strip(),
            text_context=text_context,
            source="ui_manual_signal",
        )
        self._latest_ai_signal_id = int(signal.get("id"))
        self._refresh_ai_views()
        self.root.after(0, self._show_success, f"Señal IA generada para {symbol}: {signal.get('signal_type', 'N/A')}", False)

    def _analyze_ai_text(self) -> None:
        symbol = self._selected_symbol_for_market()
        asset_type = self._asset_type_for_selection(symbol)
        text_context = self.ai_news_input.get("1.0", tk.END).strip()
        analysis = self.ai_trading_brain.analyze_text(
            symbol=symbol,
            asset_type=asset_type,
            text=text_context,
            source="ui_manual_text",
            account_name=self.account_var.get().strip(),
        )
        lines = [
            f"Sentiment: {analysis.get('sentiment', 'neutral')}",
            f"Event type: {analysis.get('event_type', 'other')}",
            f"Importance: {analysis.get('importance_score', 0)}",
            f"Risk: {analysis.get('risk_score', 0)}",
            f"Summary: {analysis.get('summary', '')}",
            f"Impact: {analysis.get('possible_market_impact', '')}",
            f"Bias: {analysis.get('action_bias', 'neutral')}",
        ]
        self._set_text_widget(self.ai_signal_text, "\n".join(lines))

    def _execute_ai_limit_buy(self) -> None:
        if self._latest_ai_signal_id is None:
            raise ValueError("No hay señal IA seleccionada")
        result = self.ai_trading_brain.place_limit_buy(
            signal_id=self._latest_ai_signal_id,
            account_name=self.account_var.get().strip(),
            manual_approved=bool(self.ai_manual_approval_var.get()),
            initiated_by="user_manual",
        )
        self._refresh_ai_views()
        self.root.after(0, self._show_success, f"Compra IA: {result}", False)

    def _execute_ai_sell_check(self) -> None:
        symbol = self._selected_symbol_for_market()
        result = self.ai_trading_brain.place_limit_sell_if_allowed(
            symbol=symbol,
            account_name=self.account_var.get().strip(),
            manual_approved=bool(self.ai_manual_approval_var.get()),
            initiated_by="user_manual",
        )
        self._refresh_ai_views()
        self.root.after(0, self._show_success, f"Venta IA: {result}", False)

    def _train_ai_model(self) -> None:
        self._refresh_ai_views()
        self.root.after(0, self._show_success, "El entrenamiento IA ahora es automático y continuo en segundo plano.", False)

    def _approve_ai_model(self) -> None:
        version = self.ai_trading_brain.approve_latest_model()
        self._refresh_ai_views()
        self.root.after(0, self._show_success, f"Modelo aprobado: {version}", False)

    def _rollback_ai_model(self) -> None:
        version = self.ai_trading_brain.rollback_model()
        self._refresh_ai_views()
        self.root.after(0, self._show_success, f"Rollback de modelo: {version or 'sin cambios'}", False)

    def _set_text_widget(self, widget: Any, text: str) -> None:
        def _apply() -> None:
            if self._is_closing or widget is None:
                return
            try:
                if hasattr(widget, "winfo_exists") and not widget.winfo_exists():
                    return
                widget.configure(state="normal")
                widget.delete("1.0", tk.END)
                widget.insert(tk.END, text)
                widget.configure(state="disabled")
            except (RuntimeError, tk.TclError):
                return

        if threading.get_ident() == self._ui_thread_ident:
            _apply()
            return
        self._safe_after(0, _apply)

    def _set_diagnostics_details_with_direction_color(self, details_lines: list[str], direction: str) -> None:
        widget = self.ai_diag_details_text
        if widget is None:
            return

        direction_key = str(direction or "").upper().strip()
        color = "#d4a017"
        if direction_key == "LONG":
            color = "#1a8f3c"
        elif direction_key == "SHORT":
            color = "#c62828"
        elif direction_key in {"HOLD", "WATCH"}:
            color = "#d4a017"

        def _apply() -> None:
            if self._is_closing or widget is None:
                return
            try:
                if hasattr(widget, "winfo_exists") and not widget.winfo_exists():
                    return
                widget.configure(state="normal")
                widget.delete("1.0", tk.END)
                widget.tag_configure("diag_direction", foreground=color)
                widget.tag_configure("diag_can_enter_yes", foreground="#1a8f3c")
                widget.tag_configure("diag_can_enter_no", foreground="#c62828")
                for line in details_lines:
                    if line.startswith("Dirección señal/operación:"):
                        widget.insert(tk.END, line + "\n", ("diag_direction",))
                    elif line.startswith("Puede entrar ahora:"):
                        normalized = line.upper()
                        if "SI" in normalized:
                            widget.insert(tk.END, line + "\n", ("diag_can_enter_yes",))
                        elif "NO" in normalized:
                            widget.insert(tk.END, line + "\n", ("diag_can_enter_no",))
                        else:
                            widget.insert(tk.END, line + "\n")
                    else:
                        widget.insert(tk.END, line + "\n")
                widget.configure(state="disabled")
            except (RuntimeError, tk.TclError):
                return

        if threading.get_ident() == self._ui_thread_ident:
            _apply()
            return
        self._safe_after(0, _apply)

    def _get_account_runtime(self, account_name: str) -> dict[str, Any]:
        runtime = self._account_runtimes.get(account_name)
        if runtime is not None:
            self._sync_runtime_target(runtime["position_manager"])
            return runtime

        profile = self.account_profiles.get(account_name)
        if not profile:
            raise ValueError(f"Cuenta no soportada para runtime: {account_name}")

        runtime_state = self._account_runtime_states.get(account_name)
        if runtime_state is None:
            runtime_state = AlpacaRuntimeState()
            self._account_runtime_states[account_name] = runtime_state

        broker = AlpacaBrokerClient(
            endpoint=str(profile.get("endpoint", "")),
            api_key=str(profile.get("key", "")),
            api_secret=str(profile.get("secret", "")),
            logger=self.logger,
            account_name=account_name,
            runtime_state=runtime_state,
        )
        market_data = MarketDataService(logger=self.logger, account_name=account_name, runtime_state=runtime_state)
        market_data.set_connection(
            endpoint=str(profile.get("endpoint", "")),
            api_key=str(profile.get("key", "")),
            api_secret=str(profile.get("secret", "")),
        )
        order_manager = OrderManager(broker=broker, logger=self.logger)
        position_manager = PositionManager(
            broker=broker,
            market_data=market_data,
            order_manager=order_manager,
            logger=self.logger,
            settings=settings,
        )
        self._sync_runtime_target(position_manager)
        runtime = {
            "broker": broker,
            "market_data": market_data,
            "order_manager": order_manager,
            "position_manager": position_manager,
        }
        self._account_runtimes[account_name] = runtime
        return runtime

    def _runtime_for_watch(self, watch_id: str) -> dict[str, Any]:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
        if context is None:
            raise ValueError(f"Watch no encontrado: {watch_id}")

        runtime = context.get("runtime")
        if runtime is None:
            runtime = self._get_account_runtime(str(context.get("account", "")).strip())
            with self._watch_lock:
                current = self._watch_tabs.get(watch_id)
                if current is not None:
                    current["runtime"] = runtime
        return runtime

    def _sync_runtime_target(self, position_manager: PositionManager) -> None:
        target_profit = self._safe_target_profit_value()
        position_manager.set_target_profit_per_share(target_profit)

    def _safe_target_profit_value(self) -> float:
        fallback = float(getattr(self, "_target_profit_cached", settings.target_profit_per_share))
        if threading.get_ident() != self._ui_thread_ident:
            return fallback
        try:
            value = float(self.target_profit_var.get().strip() or str(settings.target_profit_per_share))
            self._target_profit_cached = value
            return value
        except (ValueError, RuntimeError, tk.TclError):
            return fallback

    def _show_success(self, message: str, focus_general: bool = True) -> None:
        self.status_var.set("Operacion completada")
        self._set_output(message, focus_general=focus_general)

    def _show_error(self, message: str, focus_general: bool = True) -> None:
        self.status_var.set("Operacion con error")
        self._set_output(message, focus_general=focus_general)

    def _focus_general_tab(self) -> None:
        tab = getattr(self, "_general_tab", None)
        if tab is None:
            return
        try:
            self.log_notebook.select(tab)
        except tk.TclError:
            return

    def _set_output(self, message: str, focus_general: bool = True) -> None:
        if focus_general:
            self._focus_general_tab()
        self.output.configure(state="normal")
        self.output.insert(tk.END, str(message).strip() + "\n")
        try:
            line_count = int(float(self.output.index("end-1c").split(".")[0]))
            extra = line_count - self._ui_max_log_lines
            if extra > 0:
                self.output.delete("1.0", f"{extra + 1}.0")
        except Exception:
            pass
        self.output.see(tk.END)
        self.output.configure(state="disabled")

    def _set_buttons_state(self, state: str) -> None:
        self.account_button.configure(state=state)
        self.stock_price_button.configure(state=state)
        self.strategy_button.configure(state=state)
        self.schedule_button.configure(state=state)
        self.manual_sell_button.configure(state=state)
        self.dashboard_button.configure(state=state)
        self.schedules_button.configure(state=state)
        self.orders_button.configure(state=state)
        self.cancel_pending_button.configure(state=state)
        self.history_button.configure(state=state)
        self.stocks_button.configure(state=state)
        self.all_cryptos_button.configure(state=state)
        self.switch_account_button.configure(state=state)
        self.refresh_stocks_button.configure(state=state)
        self.cancel_entry_watch_button.configure(state=state)
        self.nyse_refresh_button.configure(state=state)

    @staticmethod
    def _fit_window_to_screen(
        window: tk.Misc,
        *,
        preferred_width: int,
        preferred_height: int,
        min_width: int,
        min_height: int,
    ) -> None:
        screen_w = max(int(window.winfo_screenwidth() or preferred_width), preferred_width)
        screen_h = max(int(window.winfo_screenheight() or preferred_height), preferred_height)

        usable_w = max(720, screen_w - 80)
        usable_h = max(560, screen_h - 120)

        width = min(int(preferred_width), usable_w)
        height = min(int(preferred_height), usable_h)

        min_w = max(640, min(int(min_width), width))
        min_h = max(520, min(int(min_height), height))

        try:
            window.minsize(min_w, min_h)
        except Exception:
            pass

        x = max((screen_w - width) // 2, 0)
        y = max((screen_h - height) // 2, 0)
        try:
            window.geometry(f"{width}x{height}+{x}+{y}")
        except Exception:
            window.geometry(f"{width}x{height}")

    def _apply_selected_account(self, update_status: bool, require_credentials: bool) -> None:
        selected = self.account_var.get().strip()
        profile = self.account_profiles.get(selected)
        if not profile:
            fallback = next(iter(self.account_profiles.keys()), "")
            if not fallback:
                raise ValueError("No hay cuentas configuradas")
            self.account_var.set(fallback)
            selected = fallback
            profile = self.account_profiles.get(selected)
            if not profile:
                raise ValueError("Cuenta no soportada")

        endpoint = profile.get("endpoint", "")
        api_key = profile.get("key", "")
        api_secret = profile.get("secret", "")

        if not endpoint or not api_key or not api_secret:
            if require_credentials:
                raise ValueError(f"Faltan credenciales de la cuenta {selected}. Revisa variables de entorno.")
            self.account_mode_var.set(f"Modo activo: {profile.get('mode', 'N/A')}")
            self.status_var.set(f"Cuenta {selected} sin credenciales")
            return

        self.broker.set_connection(endpoint=endpoint, api_key=api_key, api_secret=api_secret)
        if hasattr(self.market_data, "set_connection"):
            self.market_data.set_connection(endpoint=endpoint, api_key=api_key, api_secret=api_secret)
        self.account_mode_var.set(f"Modo activo: {profile.get('mode', 'N/A')}")
        try:
            recovery = self.position_manager.synchronize_open_positions()
        except requests.exceptions.HTTPError as ex:
            response = getattr(ex, "response", None)
            status = getattr(response, "status_code", None)
            if status == 401:
                message = (
                    f"Cuenta {selected} rechazada por Alpaca (401 Unauthorized). "
                    "Revisa endpoint, API Key y Secret."
                )
                self.logger.warning(message)
                self.status_var.set(message)
                self.balance_var.set("Saldos: credenciales invalidas o sin acceso")
                if update_status:
                    self.root.after(0, self._show_error, message, False)
                return
            raise
        except requests.exceptions.RequestException as ex:
            message = f"No se pudo validar la cuenta {selected}: {ex}"
            self.logger.warning(message)
            self.status_var.set(message)
            self.balance_var.set("Saldos: no disponibles")
            if update_status:
                self.root.after(0, self._show_error, message, False)
            return

        if update_status:
            self.status_var.set(
                (
                    f"Cuenta activa: {selected} | abiertas={recovery.get('open_positions', 0)} "
                    f"sincronizadas={recovery.get('synced', 0)}"
                )
            )

    def _update_balance_summary(self, cash: Any, buying_power: Any, currency: str) -> None:
        self.balance_var.set(f"Saldos | Cash: {cash} {currency} | Buying Power: {buying_power} {currency}")

    def _apply_runtime_settings(self) -> None:
        target_profit = self._safe_target_profit_value()
        self.position_manager.set_target_profit_per_share(target_profit)
        try:
            current_max_open = max(int(float(self.config_max_open_pos_var.get().strip() or str(settings.max_open_positions))), 1)
            settings.max_open_positions = current_max_open
        except Exception:
            pass

    def _auto_manage_all_account_positions(self) -> list[dict[str, Any]]:
        actions: list[dict[str, Any]] = []
        account_names = list((self.account_profiles or {}).keys())
        if not account_names:
            account_names = [self.account_var.get().strip()] if self.account_var.get().strip() else []
        now_monotonic = time.monotonic()

        for account_name in account_names:
            if not account_name:
                continue
            cooldown_until = float(self._account_monitor_cooldown_until.get(account_name, 0.0) or 0.0)
            if cooldown_until > now_monotonic:
                continue
            runtime = self._get_account_runtime(account_name)
            position_manager = runtime["position_manager"]
            try:
                recovery = position_manager.synchronize_open_positions()
                self.logger.info(
                    "Monitoreo multi-cuenta | %s | abiertas=%s sincronizadas=%s ya_linkeadas=%s reparadas=%s",
                    account_name,
                    recovery.get("open_positions", 0),
                    recovery.get("synced", 0),
                    recovery.get("already_linked", 0),
                    recovery.get("repaired_limits", 0),
                )
                account_actions = position_manager.auto_manage_positions()
                for action in account_actions:
                    action["account_name"] = account_name
                actions.extend(account_actions)
                self._account_monitor_cooldown_until[account_name] = 0.0
            except requests.exceptions.HTTPError as ex:
                status = ex.response.status_code if ex.response is not None else None
                retry_after_seconds = 0.0
                if ex.response is not None:
                    raw_retry_after = str(ex.response.headers.get("Retry-After", "")).strip()
                    if raw_retry_after:
                        try:
                            retry_after_seconds = max(float(raw_retry_after), 0.0)
                        except ValueError:
                            retry_after_seconds = 0.0

                if status == 429:
                    wait_seconds = max(retry_after_seconds, 2.0)
                    self._account_monitor_cooldown_until[account_name] = time.monotonic() + float(wait_seconds)
                    self.logger.warning(
                        "Monitoreo %s en cooldown por rate limit (HTTP 429). Esperando %.1fs.",
                        account_name,
                        wait_seconds,
                    )
                elif status in {503, 504}:
                    wait_seconds = max(retry_after_seconds, 5.0)
                    self._account_monitor_cooldown_until[account_name] = time.monotonic() + float(wait_seconds)
                    self.logger.warning(
                        "Monitoreo %s en cooldown por API temporalmente no disponible (HTTP %s). Esperando %.1fs.",
                        account_name,
                        status,
                        wait_seconds,
                    )
                else:
                    self.logger.warning("Monitoreo fallido para cuenta %s: %s", account_name, ex)
            except Exception as ex:
                self.logger.warning("Monitoreo fallido para cuenta %s: %s", account_name, ex)
        return actions

    def _monitor_positions_loop(self) -> None:
        if not self._monitor_in_flight:
            self._monitor_in_flight = True

            def worker() -> None:
                try:
                    self._apply_runtime_settings()
                    schedule_actions = self.scheduler.process_pending_schedules()
                    actions = self._auto_manage_all_account_positions()
                    self._monitor_failure_streak = 0
                    self._monitor_success_streak += 1
                    # Avoid network status flapping on single successful cycle.
                    if self._monitor_success_streak >= 3:
                        self._mark_network_recovered()
                    message = self._format_monitor_result(schedule_actions, actions)
                    self.root.after(0, self._on_monitor_result, message)
                except requests.exceptions.RequestException as ex:
                    self._monitor_success_streak = 0
                    self._monitor_failure_streak += 1
                    self._mark_network_degraded(
                        f"Sin conexion durante monitoreo general ({ex.__class__.__name__}). Reintentando automaticamente..."
                    )
                    log_interval = 300.0 if ex.__class__.__name__ == "ReadTimeout" else 120.0
                    if self._should_emit_transient_log("general-monitor-network", min_interval_seconds=log_interval):
                        self.logger.warning(
                            "Sin conexion durante monitoreo general (%s). Reintentando automaticamente...",
                            ex.__class__.__name__,
                        )
                except Exception as ex:
                    self.root.after(0, self._show_error, f"Monitoreo fallido: {ex}")
                finally:
                    self._monitor_in_flight = False

            threading.Thread(target=worker, daemon=True).start()

        self.root.after(settings.position_monitor_interval_seconds * 1000, self._monitor_positions_loop)

    def _on_monitor_result(self, message: str) -> None:
        if message:
            self.status_var.set(message)
        self._sync_ai_watch_tabs_from_recent_trades_async()
        threading.Thread(target=self._refresh_accounts_header_summary, daemon=True).start()
        threading.Thread(target=lambda: self._refresh_dashboard(show_output=False), daemon=True).start()

    def _format_monitor_result(self, schedule_actions: list[dict[str, Any]], actions: list[dict[str, Any]]) -> str:
        buy_actions = [action for action in schedule_actions if action.get("action") == "BUY"]
        sell_actions = [action for action in actions if action.get("action") == "SELL"]
        limit_actions = [
            action
            for action in actions
            if action.get("action") in {"LIMIT_SELL_PLACED", "LIMIT_SELL_PENDING"}
        ]
        if not buy_actions and not sell_actions:
            if limit_actions:
                latest_limit = limit_actions[-1]
                return (
                    "Monitoreo: señal de venta LIMIT enviada "
                    f"{latest_limit.get('symbol', 'N/A')} precio={latest_limit.get('limit_price', 'N/A')} "
                    f"order_id={latest_limit.get('order_id', 'N/A')} "
                    f"estado={latest_limit.get('action', 'N/A')}"
                )
            return "Monitoreo completado: sin compras/ventas automáticas"
        if sell_actions:
            latest_sell = sell_actions[-1]
            symbol = str(latest_sell.get("symbol", "N/A"))
            reason = str(latest_sell.get("reason", "N/A"))
            trigger_price = latest_sell.get("trigger_price")
            exit_price = latest_sell.get("exit_price")
            return (
                f"Monitoreo: {len(buy_actions)} compra(s), {len(sell_actions)} venta(s). "
                f"Ultima venta {symbol} motivo={reason} trigger={trigger_price} exec={exit_price}"
            )
        if limit_actions:
            latest_limit = limit_actions[-1]
            return (
                f"Monitoreo: {len(buy_actions)} compra(s), {len(limit_actions)} venta(s) LIMIT enviada(s). "
                f"Ultima {latest_limit.get('symbol', 'N/A')} @ {latest_limit.get('limit_price', 'N/A')} "
                f"(order_id={latest_limit.get('order_id', 'N/A')})"
            )
        return f"Monitoreo completado: {len(buy_actions)} compra(s) y {len(sell_actions)} venta(s) automática(s)"

    def _format_dashboard(self, dashboard: dict[str, Any], watched_count: int, scheduled_count: int) -> str:
        positions: list[PositionSnapshot] = dashboard.get("positions", [])
        pending_orders = dashboard.get("pending_orders", [])

        lines = [
            f"Modo: {dashboard.get('mode', 'N/A')}",
            f"Posiciones abiertas: {dashboard.get('open_positions', 0)}",
            f"Posiciones en ganancia: {dashboard.get('profit_positions', 0)}",
            f"Posiciones en perdida: {dashboard.get('loss_positions', 0)}",
            f"Posiciones en HOLD: {dashboard.get('hold_positions', 0)}",
            f"Ganancia realizada hoy: {dashboard.get('realized_today', 0.0):.2f}",
            f"Ganancia/Pérdida flotante: {dashboard.get('floating_pnl', 0.0):.2f}",
            f"Trades cerrados con ganancia hoy: {dashboard.get('closed_winners_today', 0)}",
            f"Activos monitoreados: {watched_count}",
            f"Activos programados para apertura: {scheduled_count}",
            f"Órdenes pendientes: {len(pending_orders)}",
            f"Minutos hasta cierre: {dashboard.get('minutes_to_close', 0)}",
            "",
            "Posiciones abiertas:",
        ]

        if not positions:
            lines.append("  - No hay posiciones abiertas")
        else:
            for position in positions:
                side_text = "SHORT" if str(position.side).lower().strip() == "short" else "LONG"
                leverage_value = max(float(getattr(position, "leverage", 1.0) or 1.0), 1.0)
                lines.append(
                    "  - "
                    f"{position.symbol} | side={side_text} | lev={leverage_value:.0f}x | qty={position.qty} | entry={position.avg_entry_price:.2f} | "
                    f"current={position.current_price:.2f} | pnl={position.unrealized_pl:.2f} | "
                    f"state={position.state} | pnl/share={position.pnl_per_share:.2f} | "
                    f"age={position.duration_seconds/60.0:.1f}m"
                )

        lines.append("")
        lines.append("Órdenes pendientes:")
        if not pending_orders:
            lines.append("  - Ninguna")
        else:
            for order in pending_orders:
                lines.append(
                    f"  - {order.get('symbol', 'N/A')} | side={order.get('side', 'N/A')} | qty={order.get('qty', 'N/A')} | id={order.get('id', 'N/A')}"
                )

        return "\n".join(lines)

    @staticmethod
    def _asset_type_for_symbol(symbol: str) -> str:
        normalized = symbol.upper().replace(" ", "")
        return "crypto" if "/" in normalized or normalized.endswith("USD") else "stock"

    def _asset_type_for_selection(self, symbol: str) -> str:
        selected_market = self.market_kind_var.get().strip()
        if selected_market == "Cryptos":
            return "crypto"
        if selected_market == "Stocks":
            return "stock"
        return self._asset_type_for_symbol(symbol)

    def _selected_symbol_for_market(self) -> str:
        symbol = self.stock_var.get().strip().upper() or settings.default_symbol
        selected_market = self.market_kind_var.get().strip()
        if selected_market != "Cryptos":
            return symbol

        normalized = symbol.replace(" ", "")
        normalized = normalized.replace("/USDT", "/USD")
        if normalized.endswith("USDT") and "/" not in normalized:
            normalized = normalized[:-4] + "USD"
        if not normalized:
            return normalized
        if "/" in normalized or normalized.endswith("USD"):
            return normalized
        return f"{normalized}USD"
