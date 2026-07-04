import json
import threading
import tkinter as tk
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tkinter import ttk
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
        self.root.geometry("1080x720")
        self.root.minsize(1080, 720)

        self.account_profiles = settings.account_profiles()

        default_account = "Paper" if "paper" in settings.alpaca_endpoint.lower() else "Real"
        if default_account in self.account_profiles:
            initial_account = default_account
        else:
            initial_account = next(iter(self.account_profiles.keys()), "Paper")

        self.account_var = tk.StringVar(value=initial_account)
        self.market_kind_var = tk.StringVar(value="Stocks")
        self.account_mode_var = tk.StringVar(value="Modo activo: N/A")
        self.balance_var = tk.StringVar(value="Saldos: no consultados")
        self.nyse_status_var = tk.StringVar(value="NYSE: cargando estado...")
        self.nyse_next_var = tk.StringVar(value="Proxima apertura NYSE: calculando...")
        self.nyse_early_var = tk.StringVar(value="Proximo cierre temprano NYSE: calculando...")
        self.nyse_days_var = tk.StringVar(value="")
        self.stock_count_var = tk.StringVar(value="Activos cargados: 0")
        self.stock_var = tk.StringVar(value=settings.default_symbol)
        self.capital_var = tk.StringVar(value=str(settings.default_trade_capital))
        self.target_profit_var = tk.StringVar(value=str(settings.target_profit_per_share))
        self.interval_var = tk.StringVar(value=settings.default_interval)
        self.daily_pnl_var = tk.StringVar(value="0")
        self.cancel_order_var = tk.StringVar()
        self.history_delete_var = tk.StringVar()
        self.status_var = tk.StringVar(value="Listo")
        self._order_alias_map: dict[str, str] = {}

        self._monitor_in_flight = False
        self._watch_tabs: dict[str, dict[str, Any]] = {}
        self._schedule_tabs: dict[str, dict[str, Any]] = {}
        self._watch_lock = threading.Lock()
        self._watch_counter = 0
        self._account_runtimes: dict[str, dict[str, Any]] = {}
        self._watch_state_path = Path(__file__).resolve().parents[1] / "watch_tabs_state.json"
        self._history_tab_frame: ttk.Frame | None = None
        self._power_inhibitor: PowerInhibitor | None = None
        self._network_degraded = False
        self._latest_ai_signal_id: int | None = None
        self.ai_window: tk.Toplevel | None = None
        self.ai_max_capital_var = tk.StringVar(value=str(settings.ai_default_max_capital_assigned))
        self.ai_max_position_var = tk.StringVar(value=str(settings.ai_default_max_position_size))
        self.ai_max_daily_loss_var = tk.StringVar(value=str(settings.ai_default_max_daily_loss))
        self.ai_bot_enabled_var = tk.IntVar(value=1)
        self.ai_signal_only_var = tk.IntVar(value=1 if settings.ai_signal_only_mode else 0)
        self.ai_paper_trading_var = tk.IntVar(value=1 if settings.paper_trading else 0)
        self.ai_live_enabled_var = tk.IntVar(value=1 if settings.live_trading_enabled else 0)
        self.ai_manual_approval_var = tk.IntVar(value=1 if settings.manual_approval_required else 0)
        self.ai_kill_switch_var = tk.IntVar(value=0)
        self.ai_header_stocks_var = tk.StringVar(value="Stocks: --")
        self.ai_header_cryptos_var = tk.StringVar(value="Cryptos: --")

        self._build_ui()
        self._build_ai_window()
        self._power_inhibitor = start_power_inhibitor(self.logger)
        self._apply_selected_account(update_status=False, require_credentials=False)
        self._restore_watch_tabs()
        self._restore_open_positions_tabs()
        self._refresh_ai_views()

    def _build_ui(self) -> None:
        container = ttk.Frame(self.root, padding=16)
        container.pack(fill="both", expand=True)

        header = ttk.Label(
            container,
            text="Panel de control del bot",
            font=("TkDefaultFont", 14, "bold"),
        )
        header.pack(anchor="w", pady=(0, 10))

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

        ttk.Label(config_row, text="Target $/acc").grid(row=0, column=4, sticky="w")
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
            text="Historial",
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

        ttk.Separator(container).pack(fill="x", pady=12)
        ttk.Label(container, textvariable=self.status_var, foreground="#555").pack(anchor="w")

        self.log_notebook = ttk.Notebook(container)
        self.log_notebook.pack(fill="both", expand=True, pady=(8, 0))

        general_frame = ttk.Frame(self.log_notebook)
        self.log_notebook.add(general_frame, text="General")
        self._general_tab = general_frame

        self.output = tk.Text(general_frame, height=20, wrap="word")
        self.output.pack(fill="both", expand=True)
        self.output.configure(state="disabled")

    def _build_ai_window(self) -> None:
        window = tk.Toplevel(self.root)
        window.title("IA")
        window.geometry("860x760")
        window.minsize(760, 640)

        header = ttk.Frame(window, padding=(12, 10))
        header.pack(fill="x")
        ttk.Label(header, text="IA - Estrategia y Entrenamiento", font=("TkDefaultFont", 12, "bold")).pack(side="left")
        ttk.Button(header, text="Refrescar IA", command=lambda: self._run_async(self._refresh_ai_views)).pack(side="right")

        header_board = ttk.Frame(window, padding=(12, 0, 12, 8))
        header_board.pack(fill="x")
        ttk.Label(header_board, textvariable=self.ai_header_stocks_var, foreground="#113a6b").pack(anchor="w")
        ttk.Label(header_board, textvariable=self.ai_header_cryptos_var, foreground="#2f5d1f").pack(anchor="w")

        body = ttk.Frame(window, padding=(12, 0, 12, 12))
        body.pack(fill="both", expand=True)

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
        ttk.Button(mode_row, text="Guardar modo", command=lambda: self._run_async(self._save_ai_runtime_controls)).pack(side="right", padx=(8, 8))

        signals_body = ttk.LabelFrame(body, text="Top activos y señales IA")
        signals_body.pack(fill="both", expand=False, pady=(0, 8))
        self._build_ai_signals_controls(signals_body)

        self.ai_runtime_text = self._build_ai_section(body, "Estado del bot automático", 9)

        self.ai_model_text = self._build_ai_section(body, "Entrenamiento de modelo", 10)
        model_toolbar = ttk.Frame(self.ai_model_text.master)
        model_toolbar.pack(fill="x", pady=(6, 0))
        ttk.Label(model_toolbar, text="El entrenamiento del modelo es automático y continuo.").pack(side="left")
        ttk.Button(model_toolbar, text="Aprobar modelo nuevo", command=lambda: self._run_async(self._approve_ai_model)).pack(side="left", padx=(8, 0))
        ttk.Button(model_toolbar, text="Volver al anterior", command=lambda: self._run_async(self._rollback_ai_model)).pack(side="left", padx=(8, 0))

        window.protocol("WM_DELETE_WINDOW", self._on_close_ai_window)
        self.ai_window = window
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

    def _ai_runtime_loop(self) -> None:
        if self.ai_window is not None and self.ai_window.winfo_exists() and str(self.ai_window.state()) != "withdrawn":
            threading.Thread(target=self._ensure_ai_automation_running, daemon=True).start()
            self._run_async(self._refresh_ai_runtime_view)
        self.root.after(5000, self._ai_runtime_loop)

    def _ensure_ai_automation_running(self) -> None:
        account_name = self.account_var.get().strip()
        if not account_name:
            return
        try:
            self.ai_trading_brain.ensure_automation_running(account_name)
        except Exception as ex:
            self.logger.warning("No se pudo iniciar la automatizacion IA automaticamente: %s", ex)

    def run(self) -> None:
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.after(1000, self._monitor_positions_loop)
        self.root.after(1000, lambda: self._run_async(self._refresh_stock_selector))
        self.root.after(1400, lambda: threading.Thread(target=self._ensure_ai_automation_running, daemon=True).start())
        self.root.after(1200, lambda: threading.Thread(target=self._refresh_nyse_status, daemon=True).start())
        self.root.after(60000, self._nyse_status_loop)
        self.root.mainloop()

    def _on_close(self) -> None:
        if self._power_inhibitor is not None:
            self._power_inhibitor.stop()
            self._power_inhibitor = None
        if self.ai_window is not None and self.ai_window.winfo_exists():
            self.ai_window.destroy()
            self.ai_window = None
        self.root.destroy()

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
            raise

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
        self.root.after(0, self._restore_open_positions_tabs)
        self.root.after(0, lambda: self._run_async(self._view_account))
        self.root.after(0, lambda: self._run_async(self._refresh_stock_selector))
        self.root.after(0, lambda: threading.Thread(target=self._ensure_ai_automation_running, daemon=True).start())
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
            f"Target por accion: {schedule.target_profit_per_share:.4f}\n"
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
            f"Target por accion: {schedule.target_profit_per_share:.4f}\n"
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

            symbol = str(trade.get("symbol", "N/A"))
            qty = float(trade.get("qty", 0.0) or 0.0)
            entry = float(trade.get("entry_price", 0.0) or 0.0)
            exit_price = float(trade.get("exit_price", 0.0) or 0.0)
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

            info_prefix = (
                f"{symbol} | qty={qty:.4f} | entry={entry:.4f} | exit={exit_price:.4f} | "
                f"pnl={pnl:.4f} | RESULTADO="
            )
            info_suffix = f" | {result} | id={short_id}"

            ttk.Label(row, text=info_prefix).pack(side="left", anchor="w")
            tk.Label(
                row,
                text=outcome,
                fg=outcome_fg,
                bg=outcome_bg,
                padx=8,
                pady=1,
                font=("TkDefaultFont", 9, "bold"),
                relief="flat",
            ).pack(side="left", padx=(0, 4))
            ttk.Label(row, text=info_suffix).pack(side="left", anchor="w")
            ttk.Button(
                row,
                text="Borrar",
                command=lambda tid=trade_id: self._run_async(lambda: self._delete_trade_by_id(tid)),
            ).pack(side="right")

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
                f"{'SYMBOL':<12} {'CAPITAL':>12} {'TARGET':>10} {'CREATED_AT':<20} {'NOTA':<24} {'ERROR':<28} {'SCHED_ID':<12}"
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
                    result = self._attempt_strategy_entry(symbol=symbol, asset_type=asset_type, runtime=runtime)
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

    def _attempt_strategy_entry(self, symbol: str, asset_type: str, runtime: dict[str, Any]) -> dict[str, str]:
        market_data = runtime["market_data"]
        position_manager = runtime["position_manager"]
        self._sync_runtime_target(position_manager)

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
        )
        if open_result.get("action") != "buy":
            return open_result

        qty = float(open_result.get("qty", 0.0) or 0.0)
        trade_result = open_result.get("trade_result", {})
        entry_price = float(trade_result.get("entry_price", latest_price) or latest_price)
        target_profit_per_share = float(position_manager.target_profit_per_share)
        target_price = entry_price + target_profit_per_share

        output = (
            f"Signal: {signal.action}\n"
            f"Reason: {signal.reason}\n"
            f"Details: {signal.details}\n"
            f"Price: {latest_price}\n"
            f"VWAP: {vwap:.2f}\n"
            f"Spread%: {spread_pct:.2f}\n"
            f"Qty: {qty}\n"
            f"Entry: {entry_price}\n"
            f"Target $/acc configurado: {target_profit_per_share:.4f}\n"
            f"Target price: {target_price:.4f}\n"
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
    ) -> dict[str, Any]:
        broker = runtime["broker"]
        position_manager = runtime["position_manager"]
        can_open, reason_text = position_manager.can_open_new_trade(symbol)
        if not can_open:
            return {
                "action": "wait",
                "status": f"{wait_prefix}: {reason_text}",
            }

        try:
            trade_capital = float(self.capital_var.get().strip() or str(settings.default_trade_capital))
        except ValueError:
            return {"action": "wait", "status": "Capital por compra invalido"}

        if trade_capital <= 0:
            return {"action": "wait", "status": "Capital por compra debe ser mayor que cero"}

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

        self._sync_runtime_target(position_manager)
        trade_result = position_manager.open_position(
            symbol=symbol,
            qty=qty,
            reason=reason,
            spread_pct=spread_pct,
        )
        return {
            "action": "buy",
            "trade_result": trade_result,
            "qty": qty,
            "entry_price": float(trade_result.get("entry_price", latest_price) or latest_price),
        }

    def _enter_market_now(self, watch_id: str) -> None:
        threading.Thread(target=self._enter_market_now_worker, args=(watch_id,), daemon=True).start()

    def _toggle_auto_rebuy(self, watch_id: str) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
            if context is None:
                return
            enabled = not bool(context.get("auto_rebuy", False))
            context["auto_rebuy"] = enabled

        self._apply_auto_rebuy_button_state(watch_id, enabled)
        state_label = "ENCENDIDO" if enabled else "APAGADO"
        self._watch_log(watch_id, f"Recompra inmediata tras venta: {state_label}")
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

    def _is_auto_rebuy_enabled(self, watch_id: str) -> bool:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
            if context is None:
                return False
            return bool(context.get("auto_rebuy", False))

    def _sell_market_now(self, watch_id: str) -> None:
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
        if not symbol:
            return

        try:
            result = position_manager.manual_sell(symbol)
            self._watch_log(
                watch_id,
                (
                    f"Venta manual ejecutada en {symbol} | trigger={result.get('trigger_price', 'N/A')} "
                    f"exec={result.get('exit_price', 'N/A')} pnl={result.get('realized_pnl', 'N/A')}"
                ),
            )
            self._set_watch_status(watch_id, f"Venta manual enviada para {symbol}. Confirmando cierre...")
            self.root.after(0, self._show_success, f"Venta manual ejecutada para {symbol}.", False)

            if self._wait_position_closed(symbol=symbol, max_attempts=8, sleep_seconds=0.5, broker=broker):
                self._resume_waiting_entry(watch_id=watch_id, symbol=symbol)
            else:
                self._watch_log(watch_id, f"Venta enviada en {symbol}, aun pendiente de confirmacion en broker.")
                self._set_watch_status(watch_id, f"Venta enviada en {symbol}; esperando confirmacion...")
        except Exception as ex:
            status = f"Venta manual fallida para {symbol}: {ex}"
            self._watch_log(watch_id, status)
            self._set_watch_status(watch_id, status)
            self.root.after(0, self._show_error, status, False)

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
            result = self._try_open_position(
                symbol=symbol,
                latest_price=latest_price,
                spread_pct=spread_pct,
                reason="manual_entry_now",
                wait_prefix=f"Entrada inmediata {symbol}",
                runtime=runtime,
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
            entry_price = float(trade_result.get("entry_price", latest_price) or latest_price)
            target_profit_per_share = float(position_manager.target_profit_per_share)
            target_price = entry_price + target_profit_per_share
            prefix = "Reentrada inmediata ejecutada" if source == "auto_rebuy" else "Entrada inmediata ejecutada"
            message = (
                f"{prefix}\n"
                f"Activo: {symbol} ({market_label})\n"
                f"Price: {latest_price}\n"
                f"Spread%: {spread_pct:.2f}\n"
                f"Qty: {qty}\n"
                f"Entry: {entry_price}\n"
                f"Target $/acc configurado: {target_profit_per_share:.4f}\n"
                f"Target price: {target_price:.4f}\n"
                f"Trade ID: {trade_result.get('trade_id', 'N/A')}\n"
                "La gestion automatica de la posicion sigue activa."
            )
            self._watch_log(watch_id, message)
            self._set_watch_status(watch_id, "Entrada inmediata ejecutada")
            self.root.after(0, self._show_success, message, False)
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

    def _create_watch_tab(
        self,
        symbol: str,
        asset_type: str,
        start_mode: str = "waiting",
        account_name: str | None = None,
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

        info_row = ttk.Frame(header)
        info_row.pack(fill="x")

        controls_row = ttk.Frame(header)
        controls_row.pack(fill="x", pady=(4, 0))

        status_var = tk.StringVar(value="Iniciando...")
        ttk.Label(
            info_row,
            text=f"Cuenta: {account_name} | Mercado: {market_label} | Activo: {symbol}",
            font=("TkDefaultFont", 10, "bold"),
        ).pack(side="left")
        ttk.Label(info_row, textvariable=status_var, foreground="#444").pack(side="left", padx=(12, 0))
        ttk.Button(
            controls_row,
            text="Comprar ahora",
            command=lambda wid=watch_id: self._enter_market_now(wid),
        ).pack(side="left", padx=(0, 8))
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
        auto_rebuy_button.pack(side="left", padx=(0, 8))
        ttk.Button(
            controls_row,
            text="Vender ahora",
            command=lambda wid=watch_id: self._sell_market_now(wid),
        ).pack(side="left", padx=(0, 8))
        ttk.Button(
            controls_row,
            text="Cancelar limites",
            command=lambda wid=watch_id: self._cancel_symbol_limits(wid),
        ).pack(side="left", padx=(0, 8))
        ttk.Button(
            controls_row,
            text="Cerrar",
            command=lambda wid=watch_id: self._request_close_watch_tab(wid),
        ).pack(side="left")

        text = tk.Text(frame, height=18, wrap="word")
        text.pack(fill="both", expand=True)
        text.configure(state="disabled")

        self.log_notebook.add(frame, text=f"{symbol} | {account_name}")
        self.log_notebook.select(frame)

        context = {
            "id": watch_id,
            "frame": frame,
            "text": text,
            "status_var": status_var,
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
            "auto_rebuy": False,
            "auto_rebuy_button": auto_rebuy_button,
            "last_live_pnl_snapshot": None,
            "last_live_pnl_log_ts": 0.0,
            "last_watch_log_message": "",
            "last_watch_log_ts": 0.0,
        }
        if start_mode != "waiting":
            context["stop_event"].set()
        with self._watch_lock:
            self._watch_tabs[watch_id] = context
        self._watch_log(watch_id, f"Pestaña creada. Esperando señal para {symbol}...")
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
        self._save_watch_tabs_state()

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
        self.root.after(0, self._append_watch_log, watch_id, f"[{timestamp}] {message}")

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
        text_widget.see(tk.END)
        text_widget.configure(state="disabled")

    def _set_watch_status(self, watch_id: str, message: str) -> None:
        self.root.after(0, self._apply_watch_status, watch_id, message)

    def _apply_watch_status(self, watch_id: str, message: str) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
        if context is None:
            return
        status_var = context.get("status_var")
        if status_var is not None:
            status_var.set(message)

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
        self._watch_log(watch_id, f"Posicion cerrada en {symbol}. Reanudando busqueda automatica de entrada.")
        self._set_watch_status(watch_id, f"Esperando nueva entrada para {symbol}...")
        threading.Thread(
            target=self._entry_watch_loop,
            args=(watch_id, symbol, asset_type),
            daemon=True,
        ).start()

    def _position_tracking_loop(self, watch_id: str, symbol: str, entry_price_hint: float) -> None:
        had_open_position = False
        reentry_started = False
        try:
            while True:
                with self._watch_lock:
                    context = self._watch_tabs.get(watch_id)
                if context is None:
                    return

                track_stop_event = context.get("track_stop_event")
                if track_stop_event is not None and track_stop_event.is_set():
                    return

                try:
                    runtime = self._runtime_for_watch(watch_id)
                    broker = runtime["broker"]
                    market_data = runtime["market_data"]
                    position_manager = runtime["position_manager"]
                    position = self._find_open_position_by_symbol(symbol, broker=broker)
                    self._mark_network_recovered()
                except requests.exceptions.RequestException as ex:
                    wait_seconds = max(settings.position_monitor_interval_seconds, 5)
                    self._mark_network_degraded(
                        f"Internet caido durante monitoreo de posicion ({symbol}). Reintentando en {wait_seconds}s..."
                    )
                    self._watch_log(
                        watch_id,
                        f"Sin conexion en monitoreo ({ex.__class__.__name__}). Reintentando en {wait_seconds}s.",
                    )
                    self._set_watch_status(watch_id, f"Sin conexion. Reintentando en {wait_seconds}s...")
                    time.sleep(wait_seconds)
                    continue

                if position is None:
                    if had_open_position:
                        self._watch_log(
                            watch_id,
                            f"Posicion {symbol} ya no esta abierta. Monitoreo sigue activo hasta cierre manual.",
                        )
                        if not reentry_started:
                            reentry_started = True
                            if self._is_auto_rebuy_enabled(watch_id):
                                self._watch_log(
                                    watch_id,
                                    f"Recompra inmediata activa para {symbol}. Intentando compra market inmediata.",
                                )
                                self._set_watch_status(watch_id, f"Recomprando {symbol} en market...")
                                self._enter_market_now_worker(
                                    watch_id=watch_id,
                                    fallback_to_wait_on_fail=True,
                                    source="auto_rebuy",
                                )
                            else:
                                self._resume_waiting_entry(watch_id=watch_id, symbol=symbol)
                            return
                    self._set_watch_status(watch_id, f"Sin posicion abierta en {symbol}.")
                    had_open_position = False
                    time.sleep(max(settings.position_monitor_interval_seconds, 5))
                    continue

                qty = float(position.get("qty", 0.0) or 0.0)
                avg_entry_price = float(position.get("avg_entry_price", 0.0) or 0.0)
                if avg_entry_price <= 0:
                    avg_entry_price = entry_price_hint
                current_price = float(market_data.get_last_price(symbol))
                pnl = (current_price - avg_entry_price) * qty
                pnl_pct = ((current_price / avg_entry_price) - 1.0) * 100.0 if avg_entry_price > 0 else 0.0
                state = "GANANDO" if pnl > 0 else "PERDIENDO" if pnl < 0 else "EQUILIBRIO"
                target_profit_per_share = float(position_manager.target_profit_per_share)
                target_price = avg_entry_price + target_profit_per_share

                line = (
                    f"PnL en vivo {symbol} | entry={avg_entry_price:.4f} | current={current_price:.4f} | "
                    f"target={target_price:.4f} (cfg={target_profit_per_share:.4f}) | "
                    f"qty={qty:.4f} | pnl={pnl:.4f} ({pnl_pct:.2f}%) | estado={state}"
                )
                if self._should_emit_live_pnl_update(
                    watch_id,
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
                had_open_position = True
                reentry_started = False
                time.sleep(max(settings.position_monitor_interval_seconds, 5))
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
                        "mode": str(context.get("mode", "waiting")),
                        "auto_rebuy": bool(context.get("auto_rebuy", False)),
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
            merged_keys.add(unique_key)
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
            auto_rebuy = bool(item.get("auto_rebuy", False))

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

            if self._watch_tab_exists_for_symbol(symbol=symbol, account=account):
                cleaned_payload.append(
                    {
                        "symbol": symbol,
                        "asset_type": asset_type,
                        "account": account,
                        "mode": mode,
                        "auto_rebuy": auto_rebuy,
                    }
                )
                continue

            watch_id = self._create_watch_tab(
                symbol=symbol,
                asset_type=asset_type,
                start_mode=mode,
                account_name=account,
            )
            cleaned_payload.append(
                {
                    "symbol": symbol,
                    "asset_type": asset_type,
                    "account": account,
                    "mode": mode,
                    "auto_rebuy": auto_rebuy,
                }
            )
            with self._watch_lock:
                context = self._watch_tabs.get(watch_id)
                if context is not None:
                    context["auto_rebuy"] = auto_rebuy
            self._apply_auto_rebuy_button_state(watch_id, auto_rebuy)
            restored += 1
            if mode == "waiting":
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
        current_account = self.account_var.get().strip()
        try:
            positions = self.broker.get_positions()
        except Exception as ex:
            self.logger.warning("No se pudieron recuperar posiciones abiertas para restaurar pestañas: %s", ex)
            return

        created = 0
        for position in positions:
            symbol = str(position.get("symbol", "")).strip().upper()
            if not symbol:
                continue
            if self._watch_tab_exists_for_symbol(symbol=symbol, account=current_account):
                continue

            asset_type = self._asset_type_for_symbol(symbol)
            watch_id = self._create_watch_tab(
                symbol=symbol,
                asset_type=asset_type,
                start_mode="tracking",
                account_name=current_account,
            )
            self._watch_log(watch_id, "Pestaña creada automaticamente desde posicion abierta en broker.")
            self._start_position_tracking(watch_id=watch_id, symbol=symbol, entry_price_hint=0.0)
            created += 1

        if created > 0:
            self.status_var.set(f"Se restauraron {created} pestaña(s) desde posiciones abiertas")

    def _watch_tab_exists_for_symbol(self, symbol: str, account: str) -> bool:
        target = self._symbol_key(symbol)
        with self._watch_lock:
            for context in self._watch_tabs.values():
                current_symbol = self._symbol_key(str(context.get("symbol", "")))
                current_account = str(context.get("account", "")).strip()
                if current_symbol == target and current_account == account:
                    return True
        return False

    def _find_open_position_by_symbol(self, symbol: str, broker: Any | None = None) -> dict[str, Any] | None:
        target = self._symbol_key(symbol)
        active_broker = broker or self.broker
        for position in active_broker.get_positions():
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
        self.ai_model_text = self._build_ai_section(frame, "Modelo local", 7)
        model_toolbar = ttk.Frame(self.ai_model_text.master)
        model_toolbar.pack(fill="x", pady=(6, 0))
        ttk.Button(model_toolbar, text="Entrenar modelo", command=lambda: self._run_async(self._train_ai_model)).pack(side="left")
        ttk.Button(model_toolbar, text="Aprobar modelo nuevo", command=lambda: self._run_async(self._approve_ai_model)).pack(side="left", padx=(8, 0))
        ttk.Button(model_toolbar, text="Volver al anterior", command=lambda: self._run_async(self._rollback_ai_model)).pack(side="left", padx=(8, 0))
        self.ai_security_text = self._build_ai_section(frame, "Seguridad", 7)

    def _build_ai_section(self, parent: Any, title: str, height: int) -> tk.Text:
        container = ttk.LabelFrame(parent, text=title)
        container.pack(fill="both", expand=False, pady=(0, 8))
        widget = tk.Text(container, height=height, wrap="word")
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
        ttk.Checkbutton(parent, text="Bot activo", variable=self.ai_bot_enabled_var).grid(row=0, column=3, sticky="w")
        ttk.Checkbutton(parent, text="Solo señales", variable=self.ai_signal_only_var).grid(row=1, column=3, sticky="w")
        ttk.Checkbutton(parent, text="Paper trading", variable=self.ai_paper_trading_var).grid(row=0, column=4, sticky="w")
        ttk.Checkbutton(parent, text="Live habilitado", variable=self.ai_live_enabled_var).grid(row=1, column=4, sticky="w")
        ttk.Checkbutton(parent, text="Aprobacion manual", variable=self.ai_manual_approval_var).grid(row=0, column=5, sticky="w")
        ttk.Checkbutton(parent, text="Kill switch", variable=self.ai_kill_switch_var).grid(row=1, column=5, sticky="w")
        ttk.Button(parent, text="Guardar fondos", command=lambda: self._run_async(self._save_ai_runtime_controls)).grid(row=1, column=6, padx=(8, 0), sticky="w")

    def _build_ai_signals_controls(self, parent: Any) -> None:
        toolbar = ttk.Frame(parent)
        toolbar.pack(fill="x", pady=(0, 6))
        ttk.Label(toolbar, text="Las señales y el análisis de texto se generan automáticamente en segundo plano.").pack(side="left")
        ttk.Button(toolbar, text="Comprar limit", command=lambda: self._run_async(self._execute_ai_limit_buy)).pack(side="left", padx=(8, 0))
        ttk.Button(toolbar, text="Evaluar venta", command=lambda: self._run_async(self._execute_ai_sell_check)).pack(side="left", padx=(8, 0))
        ttk.Button(toolbar, text="Historial", command=lambda: self._run_async(self._show_ai_signal_history)).pack(side="left", padx=(8, 0))
        self.ai_signal_text = tk.Text(parent, height=10, wrap="word")
        self.ai_signal_text.pack(fill="both", expand=True)
        self.ai_signal_text.configure(state="disabled")
        ttk.Label(parent, text="Texto/noticia para OpenAI Analyzer").pack(anchor="w", pady=(6, 0))
        self.ai_news_input = tk.Text(parent, height=4, wrap="word")
        self.ai_news_input.pack(fill="x", expand=False)

    def _refresh_ai_views(self) -> None:
        self._ensure_ai_automation_running()
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

    def _start_ai_automation(self) -> None:
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
        )
        status = self.ai_trading_brain.start_automation(account_name=account_name)
        self._refresh_ai_runtime_view(status=status)
        self.root.after(0, self._show_success, "Bot automático IA iniciado.", False)

    def _pause_ai_automation(self) -> None:
        status = self.ai_trading_brain.pause_automation()
        self._refresh_ai_runtime_view(status=status)
        self.root.after(0, self._show_success, "Bot automático IA en pausa.", False)

    def _refresh_ai_runtime_view(self, status: dict[str, Any] | None = None) -> None:
        if not hasattr(self, "ai_runtime_text"):
            return
        if status is None:
            status = self.ai_trading_brain.get_automation_status(self.account_var.get().strip())
        last_signal = status.get("last_signal") or {}
        last_signal_text = (
            f"{last_signal.get('symbol', 'N/A')} | {last_signal.get('signal_type', 'N/A')} | "
            f"score={float(last_signal.get('confidence_score', 0.0) or 0.0):.2f}"
            if last_signal
            else "N/A"
        )
        lines = [
            f"Estado del DataCollector: {status.get('collector', 'Stopped')}",
            f"Estado del SignalScanner: {status.get('scanner', 'Stopped')}",
            f"Estado del OutcomeLabeler: {status.get('labeler', 'Stopped')}",
            f"Estado del News/Social Collector: {status.get('news_social', 'Stopped')}",
            f"Estado del ModelTrainer: {status.get('trainer', 'Stopped')}",
            f"Última actualización de datos: {status.get('last_data_update', 'N/A') or 'N/A'}",
            f"Última actualización de news/social: {status.get('last_news_update', 'N/A') or 'N/A'}",
            f"Última actualización de entrenamiento: {status.get('last_training_update', 'N/A') or 'N/A'}",
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
        self.ai_bot_enabled_var.set(1 if bool(funds.get("enabled", 1)) else 0)
        self.ai_signal_only_var.set(1 if bool(runtime.get("signal_only_mode", settings.ai_signal_only_mode)) else 0)
        self.ai_paper_trading_var.set(1 if bool(runtime.get("paper_trading", settings.paper_trading)) else 0)
        self.ai_live_enabled_var.set(1 if bool(runtime.get("live_trading_enabled", settings.live_trading_enabled)) else 0)
        self.ai_manual_approval_var.set(1 if bool(runtime.get("manual_approval_required", settings.manual_approval_required)) else 0)
        self.ai_kill_switch_var.set(1 if bool(runtime.get("kill_switch", 0)) else 0)

    def _save_ai_runtime_controls(self) -> None:
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
        )
        self._refresh_ai_views()
        self.root.after(0, self._show_success, "AI Trading Brain actualizado.", False)

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
            lines.append(
                f"- {item.get('timestamp', 'N/A')} | {item.get('symbol', 'N/A')} | {item.get('signal_type', 'N/A')} | conf={float(item.get('confidence_score', 0.0) or 0.0):.2f}"
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
                lines.append(
                    "- "
                    f"{event.get('timestamp', 'N/A')} | {event.get('symbol', 'N/A')} | {event.get('source', 'N/A')} | "
                    f"sent={float(event.get('sentiment_score', 0.0) or 0.0):.2f} | infl={float(event.get('influence_score', 0.0) or 0.0):.2f}"
                )
                text_preview = str(event.get("title_or_text", "") or "").strip()
                summary = str(event.get("ai_summary", "") or "").strip()
                if text_preview:
                    lines.append(f"  texto: {text_preview[:140]}")
                if summary:
                    lines.append(f"  resumen: {summary[:180]}")
        self._set_text_widget(self.ai_signal_text, "\n".join(lines))

    def _show_ai_signal_history(self) -> None:
        rows = self.ai_trading_brain.list_signal_recommendation_history(limit=60)
        if not rows:
            self._set_text_widget(self.ai_signal_text, "Historial IA sin señales evaluadas todavía.")
            return

        def _fmt_price(value: Any) -> str:
            number = float(value or 0.0)
            return f"{number:.6f}" if number > 0 else "N/A"

        def _fmt_ts(value: Any) -> str:
            raw = str(value or "")
            if not raw:
                return "N/A"
            try:
                parsed = datetime.fromisoformat(raw)
            except ValueError:
                return raw
            return parsed.strftime("%Y-%m-%d %H:%M:%S")

        now_utc = datetime.now(timezone.utc)
        prepared: list[dict[str, Any]] = []
        for row in rows:
            pnl = float(row.get("hypothetical_pnl_pct", 0.0) or 0.0)
            generated_raw = str(row.get("generated_at", "") or "")
            generated_dt: datetime | None = None
            try:
                generated_dt = datetime.fromisoformat(generated_raw)
            except ValueError:
                generated_dt = None

            days_retained = 0
            if generated_dt is not None:
                days_retained = max((now_utc - generated_dt).days, 0)

            if pnl > 0:
                status_label = "VENTA"
                color_tag = "ai_hist_green"
                priority = 2
            else:
                if days_retained >= 5:
                    status_label = "RETENIDO 5D"
                    color_tag = "ai_hist_red"
                    priority = 0
                else:
                    status_label = "RETENIDO"
                    color_tag = "ai_hist_yellow"
                    priority = 1

            prepared.append(
                {
                    "row": row,
                    "status_label": status_label,
                    "color_tag": color_tag,
                    "priority": priority,
                    "days_retained": days_retained,
                }
            )

        prepared.sort(key=lambda item: (int(item["priority"]), str(item["row"].get("generated_at", ""))), reverse=False)

        self.ai_signal_text.configure(state="normal")
        self.ai_signal_text.delete("1.0", tk.END)
        self.ai_signal_text.tag_configure("ai_hist_red", foreground="#b00020")
        self.ai_signal_text.tag_configure("ai_hist_yellow", foreground="#a07000")
        self.ai_signal_text.tag_configure("ai_hist_green", foreground="#1b7a1b")
        self.ai_signal_text.insert(tk.END, "Historial de recomendaciones IA (entrada/salida y PnL hipotético):\n\n")

        for item in prepared:
            row = item["row"]
            color_tag = str(item["color_tag"])
            status_label = str(item["status_label"])
            days_retained = int(item["days_retained"])
            self.ai_signal_text.insert(
                tk.END,
                f"{_fmt_ts(row.get('generated_at'))} | {row.get('symbol', 'N/A')} ({row.get('asset_type', 'N/A')}) | accion={row.get('action', 'N/A')}\n",
            )
            self.ai_signal_text.insert(
                tk.END,
                f"  entrada: {_fmt_ts(row.get('entry_at'))} | precio entrada: {_fmt_price(row.get('entry_price'))} | limite compra: {_fmt_price(row.get('entry_limit_price'))}\n",
            )
            self.ai_signal_text.insert(
                tk.END,
                f"  salida sugerida: {_fmt_ts(row.get('recommended_exit_at'))} ({row.get('recommended_window_m', 0)}m) | limite salida: {_fmt_price(row.get('exit_limit_price'))}\n",
            )
            self.ai_signal_text.insert(
                tk.END,
                f"  mejor +{float(row.get('best_profit_pct', 0.0) or 0.0):.3f}% | peor {float(row.get('worst_drawdown_pct', 0.0) or 0.0):.3f}% | hipotético {float(row.get('hypothetical_pnl_pct', 0.0) or 0.0):.3f}% | resultado={row.get('result', 'N/A')}\n",
            )
            retained_note = f" ({days_retained}d)" if status_label.startswith("RETENIDO") else ""
            self.ai_signal_text.insert(tk.END, f"  estado: {status_label}{retained_note}\n", color_tag)
            self.ai_signal_text.insert(tk.END, "\n")

        self.ai_signal_text.configure(state="disabled")

    def _refresh_ai_history_view(self) -> None:
        history = self.ai_trading_brain.list_history(self.account_var.get().strip(), limit=40)
        if not history:
            self._set_text_widget(self.ai_history_text, "Sin trades IA registrados.")
            return
        lines = []
        for item in history:
            lines.append(
                f"{item.get('timestamp', 'N/A')} | {item.get('symbol', 'N/A')} | {item.get('side', 'N/A')} | qty={float(item.get('qty', 0.0) or 0.0):.6f} | filled={float(item.get('filled_price', 0.0) or 0.0):.6f} | status={item.get('status', 'N/A')} | signal={item.get('signal_id', 'N/A')}"
            )
        self._set_text_widget(self.ai_history_text, "\n".join(lines))

    def _refresh_ai_model_view(self) -> None:
        if not hasattr(self, "ai_model_text"):
            return
        training = self.ai_trading_brain.database.latest_training_run() or {}
        if not training:
            self._set_text_widget(
                self.ai_model_text,
                "Modelo no entrenado todavía.\nNo hay suficientes datos para mostrar métricas.",
            )
            return
        approved = self.ai_trading_brain.registry.approved_version() or ""
        latest = self.ai_trading_brain.registry.latest_version() or ""
        lines = [
            f"Version actual: {approved or latest or 'heuristic'}",
            f"Ultima fecha de entrenamiento: {training.get('timestamp', 'N/A')}",
            f"Cantidad de senales usadas: {training.get('number_of_samples', 0)}",
            f"Win rate: {float(training.get('win_rate', 0.0) or 0.0):.4f}",
            f"Accuracy: {float(training.get('accuracy', 0.0) or 0.0):.4f}",
            f"Precision: {float(training.get('precision', 0.0) or 0.0):.4f}",
            f"Recall: {float(training.get('recall', 0.0) or 0.0):.4f}",
            f"Profit factor: {float(training.get('profit_factor', 0.0) or 0.0):.4f}",
            f"Max drawdown: {float(training.get('max_drawdown', 0.0) or 0.0):.4f}",
            "",
            "Modo:",
            "- Entrenamiento automático continuo",
            "- Aprobación manual opcional",
            "- Rollback manual opcional",
        ]
        self._set_text_widget(self.ai_model_text, "\n".join(lines))

    def _refresh_ai_security_view(self) -> None:
        security = self.ai_trading_brain.get_security_state(self.account_var.get().strip())
        lines = [
            f"Kill switch: {'ON' if security.get('kill_switch') else 'OFF'}",
            f"Trading real activado: {'ON' if security.get('live_trading_enabled') else 'OFF'}",
            f"Paper trading activado: {'ON' if security.get('paper_trading') else 'OFF'}",
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
        )
        self._refresh_ai_views()
        self.root.after(0, self._show_success, f"Compra IA: {result}", False)

    def _execute_ai_sell_check(self) -> None:
        symbol = self._selected_symbol_for_market()
        result = self.ai_trading_brain.place_limit_sell_if_allowed(
            symbol=symbol,
            account_name=self.account_var.get().strip(),
            manual_approved=bool(self.ai_manual_approval_var.get()),
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
        widget.configure(state="normal")
        widget.delete("1.0", tk.END)
        widget.insert(tk.END, text)
        widget.configure(state="disabled")

    def _get_account_runtime(self, account_name: str) -> dict[str, Any]:
        runtime = self._account_runtimes.get(account_name)
        if runtime is not None:
            self._sync_runtime_target(runtime["position_manager"])
            return runtime

        profile = self.account_profiles.get(account_name)
        if not profile:
            raise ValueError(f"Cuenta no soportada para runtime: {account_name}")

        broker = AlpacaBrokerClient(
            endpoint=str(profile.get("endpoint", "")),
            api_key=str(profile.get("key", "")),
            api_secret=str(profile.get("secret", "")),
            logger=self.logger,
        )
        market_data = MarketDataService(logger=self.logger)
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
        self._sync_runtime_target(runtime["position_manager"])
        return runtime

    def _sync_runtime_target(self, position_manager: PositionManager) -> None:
        try:
            target_profit = float(self.target_profit_var.get().strip() or str(settings.target_profit_per_share))
        except ValueError:
            target_profit = float(settings.target_profit_per_share)
        position_manager.set_target_profit_per_share(target_profit)

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
        self.output.delete("1.0", tk.END)
        self.output.insert(tk.END, message)
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

    def _apply_selected_account(self, update_status: bool, require_credentials: bool) -> None:
        selected = self.account_var.get().strip()
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
        try:
            target_profit = float(self.target_profit_var.get().strip() or str(settings.target_profit_per_share))
        except ValueError:
            target_profit = float(settings.target_profit_per_share)
        self.position_manager.set_target_profit_per_share(target_profit)

    def _monitor_positions_loop(self) -> None:
        if not self._monitor_in_flight:
            self._monitor_in_flight = True

            def worker() -> None:
                try:
                    self._apply_runtime_settings()
                    schedule_actions = self.scheduler.process_pending_schedules()
                    actions = self.position_manager.auto_manage_positions()
                    self._mark_network_recovered()
                    message = self._format_monitor_result(schedule_actions, actions)
                    self.root.after(0, self._on_monitor_result, message)
                except requests.exceptions.RequestException as ex:
                    self._mark_network_degraded(
                        f"Sin conexion durante monitoreo general ({ex.__class__.__name__}). Reintentando automaticamente..."
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
                lines.append(
                    "  - "
                    f"{position.symbol} | qty={position.qty} | entry={position.avg_entry_price:.2f} | "
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
