import json
import threading
import tkinter as tk
import time
from datetime import datetime
from pathlib import Path
from tkinter import ttk
from typing import Any

import requests

from config import settings
from data.nyse_calendar import NyseCalendarService
from portfolio.position_manager import PositionManager, PositionSnapshot
from scheduling.market_open_scheduler import MarketOpenScheduler
from risk.risk_manager import RiskManager
from strategies.scalping_strategy import ScalpingStrategy


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
        logger: Any,
    ) -> None:
        self.broker = broker
        self.market_data = market_data
        self.risk_manager = risk_manager
        self.strategy = strategy
        self.order_manager = order_manager
        self.position_manager = position_manager
        self.scheduler = scheduler
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
        self._watch_state_path = Path(__file__).resolve().parents[1] / "watch_tabs_state.json"
        self._history_tab_frame: ttk.Frame | None = None

        self._build_ui()
        self._apply_selected_account(update_status=False, require_credentials=False)
        self._restore_watch_tabs()
        self._restore_open_positions_tabs()

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

    def run(self) -> None:
        self.root.after(1000, self._monitor_positions_loop)
        self.root.after(1000, lambda: self._run_async(self._refresh_stock_selector))
        self.root.after(1200, lambda: threading.Thread(target=self._refresh_nyse_status, daemon=True).start())
        self.root.after(60000, self._nyse_status_loop)
        self.root.mainloop()

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

            self.root.after(0, self._show_error, f"HTTP {status}: {detail}")
        except Exception as ex:
            self.logger.exception("Error en accion de UI")
            self.root.after(0, self._show_error, str(ex))

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
        self._cancel_all_watch_tabs()
        self._apply_selected_account(update_status=True, require_credentials=True)
        self._restore_open_positions_tabs()
        self._view_account()
        self._refresh_stock_selector()

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

            info = (
                f"{symbol} | qty={qty:.4f} | entry={entry:.4f} | exit={exit_price:.4f} | "
                f"pnl={pnl:.4f} | {result} | id={short_id}"
            )
            ttk.Label(row, text=info).pack(side="left", anchor="w")
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
                    "result": "GANANCIA" if float(record.get("realized_pnl", 0.0) or 0.0) > 0 else "PERDIDA",
                }
            )

        closed_trades.sort(key=lambda item: str(item.get("exit_time", "")), reverse=True)
        self.root.after(0, self._render_history_trades_tab, closed_trades)

        total_realized = sum(float(item.get("realized_pnl", 0.0) or 0.0) for item in closed_trades)
        winners = sum(1 for item in closed_trades if float(item.get("realized_pnl", 0.0) or 0.0) > 0)
        losers = sum(1 for item in closed_trades if float(item.get("realized_pnl", 0.0) or 0.0) <= 0)

        lines: list[str] = []
        lines.append("HISTORIAL")
        lines.append("=")
        lines.append(f"Trades cerrados: {len(closed_trades)} | Ganadores: {winners} | Perdedores: {losers}")
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
                result = self._attempt_strategy_entry(symbol=symbol, asset_type=asset_type)
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

    def _attempt_strategy_entry(self, symbol: str, asset_type: str) -> dict[str, str]:
        self._apply_runtime_settings()

        candles_1m = self.market_data.get_candles(symbol=symbol, interval="1m", limit=50)
        candles_5m = self.market_data.get_candles(symbol=symbol, interval="5m", limit=50)
        latest_price = self.market_data.get_last_price(symbol)
        quote = self.market_data.get_latest_quote(symbol)
        vwap = self.market_data.calculate_vwap(candles_1m)
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
        )
        if open_result.get("action") != "buy":
            return open_result

        qty = float(open_result.get("qty", 0.0) or 0.0)
        trade_result = open_result.get("trade_result", {})
        entry_price = float(trade_result.get("entry_price", latest_price) or latest_price)
        target_profit_per_share = float(self.position_manager.target_profit_per_share)
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
    ) -> dict[str, Any]:
        can_open, reason_text = self.position_manager.can_open_new_trade(symbol)
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

        account = self.broker.get_account()
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

        trade_result = self.position_manager.open_position(
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

        symbol = str(context.get("symbol", "")).upper()
        if not symbol:
            return

        combined: dict[str, dict[str, Any]] = {}
        for status in ("open", "all"):
            for order in self.order_manager.review_orders(status=status, limit=200):
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
                if self.order_manager.cancel_order(order_id):
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

        symbol = str(context.get("symbol", "")).upper()
        if not symbol:
            return

        try:
            result = self.position_manager.manual_sell(symbol)
            self._watch_log(
                watch_id,
                (
                    f"Venta manual ejecutada en {symbol} | trigger={result.get('trigger_price', 'N/A')} "
                    f"exec={result.get('exit_price', 'N/A')} pnl={result.get('realized_pnl', 'N/A')}"
                ),
            )
            self._set_watch_status(watch_id, f"Venta manual enviada para {symbol}. Confirmando cierre...")
            self.root.after(0, self._show_success, f"Venta manual ejecutada para {symbol}.", False)

            if self._wait_position_closed(symbol=symbol, max_attempts=8, sleep_seconds=0.5):
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

        symbol = str(context.get("symbol", "")).upper()
        asset_type = str(context.get("asset_type", "stock"))
        market_label = "Cryptos" if asset_type == "crypto" else "Stocks"

        try:
            latest_price = self.market_data.get_last_price(symbol)
            quote = self.market_data.get_latest_quote(symbol)
            spread_pct = float(quote.get("spread_pct", 0.0) or 0.0)
            result = self._try_open_position(
                symbol=symbol,
                latest_price=latest_price,
                spread_pct=spread_pct,
                reason="manual_entry_now",
                wait_prefix=f"Entrada inmediata {symbol}",
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
            target_profit_per_share = float(self.position_manager.target_profit_per_share)
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

    def _create_watch_tab(self, symbol: str, asset_type: str, start_mode: str = "waiting") -> str:
        with self._watch_lock:
            self._watch_counter += 1
            watch_id = f"watch-{self._watch_counter}"

        account_name = self.account_var.get().strip()
        market_label = "Cryptos" if asset_type == "crypto" else "Stocks"

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
            "active": start_mode == "waiting",
            "stop_event": threading.Event(),
            "track_stop_event": threading.Event(),
            "tracking_active": False,
            "stop_reason": "",
            "mode": start_mode,
            "auto_rebuy": False,
            "auto_rebuy_button": auto_rebuy_button,
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

        symbol = str(context.get("symbol", "")).upper()
        try:
            position = self._find_open_position_by_symbol(symbol)
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
            self.position_manager.manual_sell(symbol)
        except Exception as ex:
            # If the position disappeared meanwhile, allow closing the tab.
            if self._find_open_position_by_symbol(symbol) is None:
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

        if not self._wait_position_closed(symbol=symbol, max_attempts=8, sleep_seconds=0.5):
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

    def _wait_position_closed(self, symbol: str, max_attempts: int = 8, sleep_seconds: float = 0.5) -> bool:
        for _ in range(max_attempts):
            if self._find_open_position_by_symbol(symbol) is None:
                return True
            time.sleep(sleep_seconds)
        return False

    def _finalize_close_watch_tab(self, watch_id: str, stop_reason: str, status_message: str) -> None:
        with self._watch_lock:
            context = self._watch_tabs.get(watch_id)
        if context is None:
            return

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
        self._save_watch_tabs_state()
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

        self._save_watch_tabs_state()

        threading.Thread(
            target=self._position_tracking_loop,
            args=(watch_id, symbol, entry_price_hint),
            daemon=True,
        ).start()

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

                position = self._find_open_position_by_symbol(symbol)
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
                current_price = float(self.market_data.get_last_price(symbol))
                pnl = (current_price - avg_entry_price) * qty
                pnl_pct = ((current_price / avg_entry_price) - 1.0) * 100.0 if avg_entry_price > 0 else 0.0
                state = "GANANDO" if pnl > 0 else "PERDIENDO" if pnl < 0 else "EQUILIBRIO"
                target_profit_per_share = float(self.position_manager.target_profit_per_share)
                target_price = avg_entry_price + target_profit_per_share

                line = (
                    f"PnL en vivo {symbol} | entry={avg_entry_price:.4f} | current={current_price:.4f} | "
                    f"target={target_price:.4f} (cfg={target_profit_per_share:.4f}) | "
                    f"qty={qty:.4f} | pnl={pnl:.4f} ({pnl_pct:.2f}%) | estado={state}"
                )
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

    def _save_watch_tabs_state(self) -> None:
        with self._watch_lock:
            payload = [
                {
                    "symbol": str(context.get("symbol", "")),
                    "asset_type": str(context.get("asset_type", "stock")),
                    "account": str(context.get("account", "")),
                    "mode": str(context.get("mode", "waiting")),
                    "auto_rebuy": bool(context.get("auto_rebuy", False)),
                }
                for context in self._watch_tabs.values()
            ]

        self._watch_state_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    def _restore_watch_tabs(self) -> None:
        if not self._watch_state_path.exists():
            return

        try:
            raw = json.loads(self._watch_state_path.read_text(encoding="utf-8"))
        except Exception:
            return

        if not isinstance(raw, list):
            return

        current_account = self.account_var.get().strip()
        restored = 0
        for item in raw:
            if not isinstance(item, dict):
                continue

            symbol = str(item.get("symbol", "")).strip().upper()
            asset_type = str(item.get("asset_type", "stock")).strip().lower()
            account = str(item.get("account", "")).strip()
            mode = str(item.get("mode", "waiting")).strip().lower()
            auto_rebuy = bool(item.get("auto_rebuy", False))

            if not symbol or account != current_account:
                continue
            if mode not in {"waiting", "tracking"}:
                mode = "waiting"

            watch_id = self._create_watch_tab(symbol=symbol, asset_type=asset_type, start_mode=mode)
            with self._watch_lock:
                context = self._watch_tabs.get(watch_id)
                if context is not None:
                    context["auto_rebuy"] = auto_rebuy
            self._apply_auto_rebuy_button_state(watch_id, auto_rebuy)
            restored += 1
            if mode == "waiting":
                self._watch_log(watch_id, "Pestaña restaurada tras reinicio. Reanudando busqueda de entrada.")
                threading.Thread(
                    target=self._entry_watch_loop,
                    args=(watch_id, symbol, asset_type),
                    daemon=True,
                ).start()
            else:
                self._watch_log(watch_id, "Pestaña restaurada tras reinicio. Reanudando monitoreo en vivo.")
                self._start_position_tracking(watch_id=watch_id, symbol=symbol, entry_price_hint=0.0)

        if restored > 0:
            self.status_var.set(f"Se restauraron {restored} pestaña(s) de seguimiento")

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
            watch_id = self._create_watch_tab(symbol=symbol, asset_type=asset_type, start_mode="tracking")
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

    def _find_open_position_by_symbol(self, symbol: str) -> dict[str, Any] | None:
        target = self._symbol_key(symbol)
        for position in self.broker.get_positions():
            current = self._symbol_key(str(position.get("symbol", "")))
            if current == target:
                return position
        return None

    @staticmethod
    def _symbol_key(symbol: str) -> str:
        return str(symbol or "").upper().replace(" ", "").replace("/", "")

    def _view_orders(self) -> None:
        # Merge open + recent all to surface edge statuses like done_for_day that still represent active intent.
        combined: dict[str, dict[str, Any]] = {}
        for status in ("open", "all"):
            for order in self.order_manager.review_orders(status=status, limit=200):
                order_id = str(order.get("id", "")).strip()
                if not order_id:
                    continue
                combined[order_id] = order

        terminal_statuses = {"filled", "canceled", "rejected", "expired", "replaced"}
        pending_orders = [
            order
            for order in combined.values()
            if str(order.get("status", "")).lower().strip() not in terminal_statuses
        ]

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
        lines.append("")
        header = (
            f"{'OID':<4} {'SYMBOL':<12} {'SIDE':<6} {'QTY':>10} {'FILLED':>10} {'LIMIT':>12} "
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
                f"{alias:<4} {symbol:<12} {side:<6} {qty:>10.4f} {filled_qty:>10.4f} {limit_text:>12} "
                f"{status:<20} {tif:<6} {reason:<40}"
            )

        lines.append("")
        lines.append("IDs cortos de orden (usa estos en Cancelar orden):")
        for order in trimmed:
            lines.append(f"- {order.get('alias', 'N/A')} | {order.get('symbol', 'N/A')}")

        self.root.after(0, self._show_success, "\n".join(lines))

    def _cancel_all_pending_orders(self) -> None:
        combined: dict[str, dict[str, Any]] = {}
        for status in ("open", "all"):
            for order in self.order_manager.review_orders(status=status, limit=200):
                order_id = str(order.get("id", "")).strip()
                if not order_id:
                    continue
                combined[order_id] = order

        terminal_statuses = {"filled", "canceled", "rejected", "expired", "replaced"}
        pending_orders = [
            order
            for order in combined.values()
            if str(order.get("status", "")).lower().strip() not in terminal_statuses
        ]
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
        recovery = self.position_manager.synchronize_open_positions()
        self.account_mode_var.set(f"Modo activo: {profile.get('mode', 'N/A')}")
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
                    message = self._format_monitor_result(schedule_actions, actions)
                    self.root.after(0, self._on_monitor_result, message)
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
