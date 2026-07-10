from __future__ import annotations

import json
import os
import threading
import time
import tkinter as tk
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any


class ModelTrainerWindow:
    def __init__(self, ai_trading_brain: Any, account_profiles: dict[str, dict[str, str]], logger: Any) -> None:
        self.ai_trading_brain = ai_trading_brain
        self.account_profiles = dict(account_profiles)
        self.logger = logger

        initial_account = next(iter(self.account_profiles.keys()), "")
        self.root = tk.Tk()
        self.root.title("Model Trainer")
        self.root.geometry("1380x1040")
        self.root.minsize(1220, 900)

        self.account_var = tk.StringVar(value=initial_account)
        self.status_var = tk.StringVar(value="Listo")
        self.summary_var = tk.StringVar(value="Estado general: cargando...")
        self.model_var = tk.StringVar(value="Modelo actual: cargando...")
        self.training_gate_var = tk.StringVar(value="Entrenamiento: evaluando readiness...")
        self.help_var = tk.StringVar(
            value=(
                "Flujo recomendado: 1) deja activos los workers, 2) espera suficientes outcomes, "
                "3) compara candidatos, 4) aprueba solo el mejor."
            )
        )
        self.selected_version_var = tk.StringVar(value="")
        self.selected_summary_var = tk.StringVar(value="Selecciona un modelo para ver qué aprendió y cómo compara.")
        self.selected_compare_var = tk.StringVar(value="Comparativa: N/A")
        self.focus_status_var = tk.StringVar(value="Monedas foco: cargando...")
        self._refresh_in_flight = False
        self._candidate_rows_by_version: dict[str, dict[str, Any]] = {}
        self._latest_candidates_payload: dict[str, Any] = {}
        self._latest_status: dict[str, Any] = {}
        self._worker_value_vars: dict[str, tk.StringVar] = {}
        self._worker_note_vars: dict[str, tk.StringVar] = {}
        self._status_labels: dict[str, tk.Label] = {}
        self._detail_scroll_hold_until = 0.0
        self._detail_rendered_version = ""
        self._suppress_candidate_select_event = False
        self._focus_crypto_values: list[str] = []
        self._focus_crypto_selected: list[str] = []
        self._ai_ui_state_path = Path(__file__).resolve().parents[1] / "ai_ui_state.json"

        self._build_ui()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.after(250, self._auto_start)
        self.root.after(400, self._refresh_focus_cryptos_async)
        self.root.after(1000, self._refresh_async)

    def _build_ui(self) -> None:
        container = ttk.Frame(self.root, padding=14)
        container.pack(fill="both", expand=True)

        header = ttk.Frame(container)
        header.pack(fill="x", pady=(0, 10))
        ttk.Label(header, text="Model Trainer", font=("TkDefaultFont", 15, "bold")).pack(side="left")
        ttk.Label(header, textvariable=self.status_var, foreground="#1f4d7a").pack(side="right")

        top = ttk.LabelFrame(container, text="Control del entrenador", padding=10)
        top.pack(fill="x", pady=(0, 10))
        for column in range(6):
            top.columnconfigure(column, weight=0)
        top.columnconfigure(6, weight=1)

        ttk.Label(top, text="Cuenta de datos").grid(row=0, column=0, sticky="w", padx=4, pady=4)
        self.account_combo = ttk.Combobox(
            top,
            textvariable=self.account_var,
            values=list(self.account_profiles.keys()),
            state="readonly",
            width=18,
        )
        self.account_combo.grid(row=0, column=1, sticky="w", padx=4, pady=4)
        self.account_combo.bind("<<ComboboxSelected>>", self._on_account_changed)

        self.start_button = ttk.Button(top, text="Activar procesos automáticos", command=self._start_workers_async)
        self.start_button.grid(row=0, column=2, padx=4, pady=4)
        self.pause_button = ttk.Button(top, text="Pausar procesos", command=self._pause_workers_async)
        self.pause_button.grid(row=0, column=3, padx=4, pady=4)
        self.train_now_button = ttk.Button(top, text="Entrenar ahora", command=self._train_now_async)
        self.train_now_button.grid(row=0, column=4, padx=4, pady=4)
        ttk.Button(top, text="Refrescar", command=self._refresh_async).grid(row=0, column=5, padx=4, pady=4)

        ttk.Label(top, textvariable=self.summary_var, foreground="#444").grid(row=1, column=0, columnspan=7, sticky="w", padx=4, pady=(8, 2))
        ttk.Label(top, textvariable=self.model_var, foreground="#1f4d7a").grid(row=2, column=0, columnspan=7, sticky="w", padx=4, pady=2)
        ttk.Label(top, textvariable=self.training_gate_var, foreground="#8a4d00").grid(row=3, column=0, columnspan=7, sticky="w", padx=4, pady=2)
        ttk.Label(top, textvariable=self.help_var, foreground="#555").grid(row=4, column=0, columnspan=7, sticky="w", padx=4, pady=(2, 0))

        focus_frame = ttk.LabelFrame(container, text="Monedas a estudiar y operar", padding=10)
        focus_frame.pack(fill="x", pady=(0, 10))
        focus_frame.columnconfigure(0, weight=1)
        focus_frame.columnconfigure(2, weight=1)

        ttk.Label(
            focus_frame,
            text=(
                "Selecciona aquí las criptomonedas permitidas. El scanner, las señales, el trading y noticias/social "
                "se limitarán a esta lista cuando guardes."
            ),
            foreground="#555",
            wraplength=1200,
        ).grid(row=0, column=0, columnspan=3, sticky="w", padx=4, pady=(0, 8))

        left_focus = ttk.Frame(focus_frame)
        left_focus.grid(row=1, column=0, sticky="nsew", padx=(4, 6), pady=2)
        ttk.Label(left_focus, text="Monedas disponibles").pack(anchor="w")
        self.focus_available_listbox = tk.Listbox(left_focus, selectmode="extended", exportselection=False, height=8)
        self.focus_available_listbox.pack(fill="both", expand=True, pady=(4, 0))

        middle_focus = ttk.Frame(focus_frame)
        middle_focus.grid(row=1, column=1, sticky="ns", padx=4, pady=2)
        ttk.Button(middle_focus, text="Agregar ->", command=self._add_focus_cryptos).pack(fill="x", pady=(0, 6))
        ttk.Button(middle_focus, text="<- Quitar", command=self._remove_focus_cryptos).pack(fill="x", pady=(0, 6))
        ttk.Button(middle_focus, text="Agregar todas", command=self._add_all_focus_cryptos).pack(fill="x", pady=(0, 6))
        ttk.Button(middle_focus, text="Limpiar", command=self._clear_focus_cryptos).pack(fill="x")

        right_focus = ttk.Frame(focus_frame)
        right_focus.grid(row=1, column=2, sticky="nsew", padx=(6, 4), pady=2)
        ttk.Label(right_focus, text="Monedas seleccionadas").pack(anchor="w")
        self.focus_selected_listbox = tk.Listbox(right_focus, selectmode="extended", exportselection=False, height=8)
        self.focus_selected_listbox.pack(fill="both", expand=True, pady=(4, 0))

        actions_focus = ttk.Frame(focus_frame)
        actions_focus.grid(row=2, column=0, columnspan=3, sticky="ew", padx=4, pady=(8, 0))
        ttk.Button(actions_focus, text="Actualizar lista", command=self._refresh_focus_cryptos_async).pack(side="left", padx=(0, 6))
        ttk.Button(actions_focus, text="Guardar monedas activas", command=self._save_focus_cryptos_async).pack(side="left")
        ttk.Label(actions_focus, textvariable=self.focus_status_var, foreground="#1f4d7a").pack(side="right")

        workers_frame = ttk.LabelFrame(container, text="Procesos del trainer", padding=10)
        workers_frame.pack(fill="x", pady=(0, 10))
        worker_specs = [
            ("collector", "Recolector", "Descarga snapshots y mercado para construir dataset."),
            ("labeler", "Etiquetador", "Convierte señales pasadas en outcomes win/loss/neutral."),
            ("news_social", "Noticias/Social", "Añade contexto textual y sentimiento al dataset."),
            ("trainer", "Entrenador", "Reentrena y guarda nuevas versiones del modelo."),
            ("scanner", "Scanner", "Evalúa señales; en Model Trainer normalmente no aplica."),
        ]
        for col, (key, title, note) in enumerate(worker_specs):
            card = ttk.Frame(workers_frame, padding=(8, 6))
            card.grid(row=0, column=col, sticky="nsew", padx=6, pady=2)
            workers_frame.columnconfigure(col, weight=1)
            ttk.Label(card, text=title, font=("TkDefaultFont", 10, "bold")).pack(anchor="w")
            value_var = tk.StringVar(value="Cargando...")
            note_var = tk.StringVar(value=note)
            self._worker_value_vars[key] = value_var
            self._worker_note_vars[key] = note_var
            status_label = tk.Label(card, textvariable=value_var, anchor="w", fg="#555")
            status_label.pack(anchor="w", pady=(4, 2), fill="x")
            self._status_labels[key] = status_label
            ttk.Label(card, textvariable=note_var, wraplength=220, foreground="#666").pack(anchor="w", fill="x")

        middle = ttk.PanedWindow(container, orient="horizontal")
        middle.pack(fill="both", expand=True)

        left = ttk.LabelFrame(middle, text="Modelos disponibles", padding=8)
        right = ttk.LabelFrame(middle, text="Detalle del modelo seleccionado", padding=8)
        middle.add(left, weight=2)
        middle.add(right, weight=2)

        ttk.Label(
            left,
            text="Selecciona un modelo para ver qué aprendió y cómo compara contra el aprobado y los demás.",
            foreground="#555",
        ).pack(anchor="w", pady=(0, 6))
        ttk.Label(
            left,
            text=(
                "Leyenda: score = ranking compuesto | acc = accuracy | win = win rate | "
                "pf = profit factor | dd = drawdown"
            ),
            foreground="#666",
            wraplength=620,
        ).pack(anchor="w", pady=(0, 8))

        columns = ("state", "score", "accuracy", "win_rate", "profit_factor", "drawdown", "samples")
        self.candidates_tree = ttk.Treeview(left, columns=columns, show="tree headings", height=12)
        self.candidates_tree.heading("#0", text="Modelo")
        self.candidates_tree.heading("state", text="Estado")
        self.candidates_tree.heading("score", text="Score")
        self.candidates_tree.heading("accuracy", text="Acc")
        self.candidates_tree.heading("win_rate", text="Win")
        self.candidates_tree.heading("profit_factor", text="PF")
        self.candidates_tree.heading("drawdown", text="DD")
        self.candidates_tree.heading("samples", text="Muestras")
        self.candidates_tree.column("#0", width=280, stretch=True)
        self.candidates_tree.column("state", width=110, anchor="center", stretch=False)
        self.candidates_tree.column("score", width=70, anchor="e", stretch=False)
        self.candidates_tree.column("accuracy", width=70, anchor="e", stretch=False)
        self.candidates_tree.column("win_rate", width=70, anchor="e", stretch=False)
        self.candidates_tree.column("profit_factor", width=70, anchor="e", stretch=False)
        self.candidates_tree.column("drawdown", width=70, anchor="e", stretch=False)
        self.candidates_tree.column("samples", width=90, anchor="e", stretch=False)
        tree_scroll = ttk.Scrollbar(left, orient="vertical", command=self.candidates_tree.yview)
        self.candidates_tree.configure(yscrollcommand=tree_scroll.set)
        self.candidates_tree.pack(side="left", fill="both", expand=True)
        tree_scroll.pack(side="left", fill="y")
        self.candidates_tree.bind("<<TreeviewSelect>>", self._on_candidate_selected)

        candidate_buttons = ttk.Frame(left)
        candidate_buttons.pack(fill="x", pady=(8, 0), side="bottom")
        ttk.Button(candidate_buttons, text="Aprobar seleccionado", command=self._approve_selected_async).pack(side="left", padx=(0, 6))
        ttk.Button(candidate_buttons, text="Congelar candidato", command=self._freeze_selected_async).pack(side="left", padx=(0, 6))
        ttk.Button(candidate_buttons, text="Descongelar", command=self._unfreeze_async).pack(side="left", padx=(0, 6))
        ttk.Button(candidate_buttons, text="Eliminar", command=self._delete_selected_async).pack(side="left")

        ttk.Label(
            right,
            text="Aquí ves qué aprendió el modelo seleccionado y cómo compara contra el aprobado.",
            foreground="#555",
            wraplength=520,
        ).pack(anchor="w", pady=(0, 6))
        ttk.Label(right, textvariable=self.selected_summary_var, foreground="#1f4d7a", wraplength=520).pack(anchor="w")
        ttk.Label(right, textvariable=self.selected_compare_var, foreground="#555", wraplength=520).pack(anchor="w", pady=(4, 8))
        detail_frame = ttk.Frame(right)
        detail_frame.pack(fill="both", expand=True)
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical")
        self.detail_text = tk.Text(detail_frame, wrap="word", height=16, yscrollcommand=detail_scroll.set)
        detail_scroll.configure(command=self.detail_text.yview)
        self.detail_text.pack(side="left", fill="both", expand=True)
        detail_scroll.pack(side="left", fill="y")
        self.detail_text.configure(state="disabled")
        for sequence in ("<MouseWheel>", "<Button-4>", "<Button-5>", "<ButtonPress-1>", "<KeyPress>", "<FocusIn>"):
            self.detail_text.bind(sequence, self._on_detail_user_interaction, add="+")
        detail_scroll.bind("<ButtonPress-1>", self._on_detail_user_interaction, add="+")

        bottom = ttk.LabelFrame(container, text="Actividad y estado operativo", padding=8)
        bottom.pack(fill="both", expand=True, pady=(10, 0))
        activity_frame = ttk.Frame(bottom)
        activity_frame.pack(fill="both", expand=True)
        activity_scroll = ttk.Scrollbar(activity_frame, orient="vertical")
        self.activity_text = tk.Text(activity_frame, wrap="word", height=22, yscrollcommand=activity_scroll.set)
        activity_scroll.configure(command=self.activity_text.yview)
        self.activity_text.pack(side="left", fill="both", expand=True)
        activity_scroll.pack(side="left", fill="y")
        self.activity_text.configure(state="disabled")

    def run(self) -> None:
        self.root.mainloop()

    def _on_close(self) -> None:
        try:
            self.ai_trading_brain.pause_automation()
        except Exception:
            pass
        self.root.destroy()

    def _set_activity(self, text: str) -> None:
        current_yview = self.activity_text.yview()
        self.activity_text.configure(state="normal")
        self.activity_text.delete("1.0", tk.END)
        self.activity_text.insert(tk.END, text)
        self.activity_text.configure(state="disabled")
        if current_yview:
            self.activity_text.yview_moveto(current_yview[0])

    def _set_detail(self, text: str, *, force: bool = False) -> None:
        if not force and time.monotonic() < self._detail_scroll_hold_until:
            return
        current_yview = self.detail_text.yview()
        self.detail_text.configure(state="normal")
        self.detail_text.delete("1.0", tk.END)
        self.detail_text.insert(tk.END, text)
        self.detail_text.configure(state="disabled")
        if current_yview:
            self.detail_text.yview_moveto(current_yview[0])

    def _on_detail_user_interaction(self, _event: Any = None) -> None:
        self._detail_scroll_hold_until = time.monotonic() + 12.0

    def _on_candidate_selected(self, _event: Any = None) -> None:
        if self._suppress_candidate_select_event:
            return
        selection = self.candidates_tree.selection()
        if not selection:
            self.selected_version_var.set("")
            self.selected_summary_var.set("Selecciona un modelo para ver qué aprendió y cómo compara.")
            self.selected_compare_var.set("Comparativa: N/A")
            self._set_detail("Sin modelo seleccionado.")
            return
        self.selected_version_var.set(str(selection[0]).strip())
        self._render_selected_model_detail(force=True)

    def _auto_start(self) -> None:
        self._start_workers_async()

    def _on_account_changed(self, _event: Any = None) -> None:
        self._refresh_focus_cryptos_async()
        self._refresh_async()

    @staticmethod
    def _parse_symbols_csv(raw: str) -> list[str]:
        ordered: list[str] = []
        seen: set[str] = set()
        for token in str(raw or "").replace(";", ",").split(","):
            symbol = str(token or "").strip().upper()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            ordered.append(symbol)
        return ordered

    def _refresh_focus_cryptos_async(self) -> None:
        account_name = self.account_var.get().strip()
        if not account_name:
            return
        threading.Thread(target=self._refresh_focus_cryptos, args=(account_name,), daemon=True).start()

    def _refresh_focus_cryptos(self, account_name: str) -> None:
        try:
            available: set[str] = set()
            for asset in self.ai_trading_brain.database.list_watchlist_assets(active_only=True):
                symbol = str(asset.get("symbol", "") or "").strip().upper()
                asset_type = str(asset.get("asset_type", "stock") or "stock").strip().lower()
                if symbol and asset_type == "crypto":
                    available.add(symbol)
            try:
                for asset in self.ai_trading_brain.broker.list_cryptos(status="active", only_tradable=True):
                    symbol = str(asset.get("symbol", "") or "").strip().upper()
                    if symbol:
                        available.add(symbol)
            except Exception:
                pass

            selected = self._load_focus_cryptos_from_shared_state(account_name)
            if not selected:
                focus = self.ai_trading_brain.get_focus_symbols(account_name)
                selected = self._parse_symbols_csv(str(focus.get("cryptos_symbols_raw", "") or ""))
            self._sync_focus_into_backend(account_name, selected)
            self.root.after(0, self._apply_focus_cryptos_state, sorted(available), selected)
        except Exception as ex:
            self.root.after(0, self.focus_status_var.set, f"Error cargando monedas: {ex}")

    def _sync_focus_into_backend(self, account_name: str, selected: list[str]) -> None:
        if not account_name or not selected:
            return
        self.ai_trading_brain.update_focus_symbols(
            account_name=account_name,
            focus_stocks_only=False,
            focus_cryptos_only=True,
            focus_stocks_symbols="",
            focus_cryptos_symbols=",".join(selected),
        )

    def _load_focus_cryptos_from_shared_state(self, account_name: str) -> list[str]:
        if not self._ai_ui_state_path.exists():
            return []
        try:
            raw = json.loads(self._ai_ui_state_path.read_text(encoding="utf-8"))
        except Exception:
            return []
        if not isinstance(raw, dict):
            return []

        selected_account = str(account_name or "").strip()
        focus_by_account = raw.get("ai_focus_by_account", {}) if isinstance(raw.get("ai_focus_by_account", {}), dict) else {}
        focus = focus_by_account.get(selected_account, {}) if selected_account and isinstance(focus_by_account.get(selected_account, {}), dict) else {}
        if not focus:
            focus = raw.get("ai_focus", {}) if isinstance(raw.get("ai_focus", {}), dict) else {}
        if not isinstance(focus, dict):
            return []
        return self._parse_symbols_csv(str(focus.get("cryptos_symbols", "") or focus.get("cryptos_symbols_raw", "") or ""))

    def _apply_focus_cryptos_state(self, available: list[str], selected: list[str]) -> None:
        self._focus_crypto_values = list(available)
        self._focus_crypto_selected = [symbol for symbol in selected if symbol]

        self.focus_available_listbox.delete(0, tk.END)
        for symbol in self._focus_crypto_values:
            self.focus_available_listbox.insert(tk.END, symbol)

        self.focus_selected_listbox.delete(0, tk.END)
        for symbol in self._focus_crypto_selected:
            self.focus_selected_listbox.insert(tk.END, symbol)

        selected_count = len(self._focus_crypto_selected)
        self.focus_status_var.set(f"Monedas activas: {selected_count}")

    def _add_focus_cryptos(self) -> None:
        indices = self.focus_available_listbox.curselection()
        for index in indices:
            symbol = str(self.focus_available_listbox.get(index) or "").strip().upper()
            if symbol and symbol not in self._focus_crypto_selected:
                self._focus_crypto_selected.append(symbol)
        self._focus_crypto_selected.sort()
        self._apply_focus_cryptos_state(self._focus_crypto_values, self._focus_crypto_selected)

    def _remove_focus_cryptos(self) -> None:
        indices = self.focus_selected_listbox.curselection()
        remove_set = {str(self.focus_selected_listbox.get(index) or "").strip().upper() for index in indices}
        self._focus_crypto_selected = [symbol for symbol in self._focus_crypto_selected if symbol not in remove_set]
        self._apply_focus_cryptos_state(self._focus_crypto_values, self._focus_crypto_selected)

    def _add_all_focus_cryptos(self) -> None:
        self._focus_crypto_selected = sorted(set(self._focus_crypto_values))
        self._apply_focus_cryptos_state(self._focus_crypto_values, self._focus_crypto_selected)

    def _clear_focus_cryptos(self) -> None:
        self._focus_crypto_selected = []
        self._apply_focus_cryptos_state(self._focus_crypto_values, self._focus_crypto_selected)

    def _save_focus_cryptos_async(self) -> None:
        threading.Thread(target=self._save_focus_cryptos, daemon=True).start()

    def _save_focus_cryptos(self) -> None:
        account_name = self.account_var.get().strip()
        if not account_name:
            self.root.after(0, self.focus_status_var.set, "Selecciona una cuenta primero")
            return
        if not self._focus_crypto_selected:
            self.root.after(0, lambda: messagebox.showwarning("Model Trainer", "Selecciona al menos una moneda para operar."))
            return
        try:
            self.ai_trading_brain.update_focus_symbols(
                account_name=account_name,
                focus_stocks_only=False,
                focus_cryptos_only=True,
                focus_stocks_symbols="",
                focus_cryptos_symbols=",".join(self._focus_crypto_selected),
            )
            self._save_shared_focus_state(account_name)
            self.root.after(0, self.focus_status_var.set, f"Guardado: {len(self._focus_crypto_selected)} monedas activas")
            self.root.after(0, self.status_var.set, "Enfoque guardado: el bot operará solo monedas seleccionadas")
            self.root.after(0, self._refresh_async)
        except Exception as ex:
            self.root.after(0, self.focus_status_var.set, f"Error guardando enfoque: {ex}")

    def _save_shared_focus_state(self, account_name: str) -> None:
        try:
            raw_existing: dict[str, Any] = {}
            if self._ai_ui_state_path.exists():
                loaded = json.loads(self._ai_ui_state_path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    raw_existing = loaded
        except Exception:
            raw_existing = {}

        focus_by_account = raw_existing.get("ai_focus_by_account", {})
        if not isinstance(focus_by_account, dict):
            focus_by_account = {}
        if account_name:
            focus_by_account[account_name] = {
                "stocks_only": False,
                "cryptos_only": True,
                "stocks_symbols": "",
                "cryptos_symbols": ",".join(self._focus_crypto_selected),
            }

        payload = dict(raw_existing)
        payload["account"] = account_name
        payload["ai_focus"] = {
            "stocks_only": False,
            "cryptos_only": True,
            "stocks_symbols": "",
            "cryptos_symbols": ",".join(self._focus_crypto_selected),
        }
        payload["ai_focus_by_account"] = focus_by_account
        self._ai_ui_state_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    def _start_workers_async(self) -> None:
        account_name = self.account_var.get().strip()
        threading.Thread(target=self._start_workers, args=(account_name,), daemon=True).start()

    def _start_workers(self, account_name: str) -> None:
        try:
            selected = self._load_focus_cryptos_from_shared_state(account_name)
            self._sync_focus_into_backend(account_name, selected)
            status = self.ai_trading_brain.start_automation(account_name)
            self.root.after(0, self.status_var.set, "Procesos automáticos activos")
            self.root.after(0, self._update_from_status, status)
        except Exception as ex:
            self.logger.warning("No se pudo iniciar Model Trainer: %s", ex)
            self.root.after(0, self.status_var.set, f"Error iniciando: {ex}")

    def _pause_workers_async(self) -> None:
        threading.Thread(target=self._pause_workers, daemon=True).start()

    def _pause_workers(self) -> None:
        try:
            status = self.ai_trading_brain.pause_automation()
            self.root.after(0, self.status_var.set, "Procesos en pausa")
            self.root.after(0, self._update_from_status, status)
        except Exception as ex:
            self.logger.warning("No se pudo pausar Model Trainer: %s", ex)
            self.root.after(0, self.status_var.set, f"Error pausa: {ex}")

    def _train_now_async(self) -> None:
        threading.Thread(target=self._train_now, daemon=True).start()

    def _train_now(self) -> None:
        try:
            result = self.ai_trading_brain.train_model()
            if bool(result.get("trained", False)):
                self.root.after(0, self.status_var.set, f"Modelo creado: {result.get('model_version', 'N/A')}")
            else:
                self.root.after(0, self.status_var.set, str(result.get("reason", "Sin entrenamiento")))
            self.root.after(0, self._refresh_async)
        except Exception as ex:
            self.logger.warning("Fallo entrenando modelo: %s", ex)
            self.root.after(0, self.status_var.set, f"Error entrenamiento: {ex}")

    def _approve_selected_async(self) -> None:
        self._approve_selected()

    def _approve_selected(self) -> None:
        version = self.selected_version_var.get().strip()
        if not version:
            self.root.after(0, lambda: messagebox.showerror("Model Trainer", "Selecciona un candidato primero."))
            return
        if not messagebox.askyesno("Aprobar modelo", f"Aprobar {version} como modelo actual?"):
            return
        try:
            approved = self.ai_trading_brain.approve_model_version(version)
            self.root.after(0, self.status_var.set, f"Aprobado: {approved}")
            self.root.after(0, self._refresh_async)
        except Exception as ex:
            self.logger.warning("No se pudo aprobar modelo %s: %s", version, ex)
            self.root.after(0, self.status_var.set, f"Error aprobando: {ex}")

    def _freeze_selected_async(self) -> None:
        self._freeze_selected()

    def _freeze_selected(self) -> None:
        version = self.selected_version_var.get().strip()
        if not version:
            self.root.after(0, lambda: messagebox.showerror("Model Trainer", "Selecciona un candidato primero."))
            return
        try:
            frozen = self.ai_trading_brain.freeze_candidate_version(version)
            self.root.after(0, self.status_var.set, f"Congelado: {frozen}")
            self.root.after(0, self._refresh_async)
        except Exception as ex:
            self.logger.warning("No se pudo congelar modelo %s: %s", version, ex)
            self.root.after(0, self.status_var.set, f"Error congelando: {ex}")

    def _unfreeze_async(self) -> None:
        self._unfreeze()

    def _unfreeze(self) -> None:
        try:
            self.ai_trading_brain.clear_frozen_candidate()
            self.root.after(0, self.status_var.set, "Candidato descongelado")
            self.root.after(0, self._refresh_async)
        except Exception as ex:
            self.logger.warning("No se pudo descongelar candidato: %s", ex)
            self.root.after(0, self.status_var.set, f"Error descongelando: {ex}")

    def _delete_selected_async(self) -> None:
        self._delete_selected()

    def _delete_selected(self) -> None:
        version = self.selected_version_var.get().strip()
        if not version:
            self.root.after(0, lambda: messagebox.showerror("Model Trainer", "Selecciona un candidato primero."))
            return
        if not messagebox.askyesno("Eliminar candidato", f"Eliminar {version}? Esta acción no se puede deshacer."):
            return
        try:
            deleted = self.ai_trading_brain.delete_model_version(version)
            self.root.after(0, self.status_var.set, f"Eliminado: {deleted}")
            self.root.after(0, self._refresh_async)
        except Exception as ex:
            self.logger.warning("No se pudo eliminar modelo %s: %s", version, ex)
            self.root.after(0, self.status_var.set, f"Error eliminando: {ex}")

    def _refresh_async(self) -> None:
        if self._refresh_in_flight:
            return
        self._refresh_in_flight = True
        account_name = self.account_var.get().strip()
        threading.Thread(target=self._refresh, args=(account_name,), daemon=True).start()

    def _refresh(self, account_name: str) -> None:
        try:
            selected = self._load_focus_cryptos_from_shared_state(account_name)
            self._sync_focus_into_backend(account_name, selected)
            status = self.ai_trading_brain.get_automation_status(account_name)
            candidates = self.ai_trading_brain.list_model_candidates(limit=50)
            self.root.after(0, self._update_from_status, status)
            self.root.after(0, self._update_candidates, candidates)
        except Exception as ex:
            self.logger.warning("No se pudo refrescar Model Trainer: %s", ex)
            self.root.after(0, self.status_var.set, f"Error refrescando: {ex}")
        finally:
            self._refresh_in_flight = False
            self.root.after(5000, self._refresh_async)

    def _update_from_status(self, status: dict[str, Any]) -> None:
        self._latest_status = dict(status)
        enough_outcomes = bool(status.get("training_enough_outcomes", False))
        ready_to_train = bool(status.get("training_ready", False))
        outcomes_total = int(status.get("evaluated_outcomes", 0) or 0)
        min_required = int(status.get("training_min_outcomes_required", 200) or 200)
        new_outcomes = int(status.get("training_new_outcomes", 0) or 0)
        readiness_reason = str(status.get("training_ready_reason", "") or "")
        running_workers = sum(1 for key in ("collector", "labeler", "news_social", "trainer") if str(status.get(key, "")).strip().lower() == "running")
        scanner_engine = str(status.get("scanner_decision_engine", "heuristic") or "heuristic")
        runtime_role = str(status.get("runtime_role", "training") or "training")

        self.summary_var.set(
            (
                f"Cuenta: {self.account_var.get().strip() or 'N/A'} | "
                f"rol={runtime_role} | motor de decisión={scanner_engine} | "
                f"workers activos={running_workers}/4"
            )
        )
        self.model_var.set(
            f"Modelo aprobado: {status.get('model_approved_reference', 'manual_pending') or 'manual_pending'} | "
            f"último entrenado: {status.get('model_latest_trained', 'none')} | "
            f"modelo activo actual: {status.get('model_current', 'heuristic')}"
        )
        self.training_gate_var.set(
            f"Outcomes evaluados: {outcomes_total}/{min_required} | nuevos: {new_outcomes} | "
            f"suficientes={'SI' if enough_outcomes else 'NO'} | listo para entrenar={'SI' if ready_to_train else 'NO'} | {readiness_reason}"
        )
        self.train_now_button.configure(state=("normal" if ready_to_train else "disabled"))
        self.start_button.configure(state=("disabled" if running_workers >= 3 else "normal"))
        self.pause_button.configure(state=("normal" if running_workers > 0 else "disabled"))

        for key in ("collector", "labeler", "news_social", "trainer", "scanner"):
            raw_status = str(status.get(key, "N/A") or "N/A")
            human_status, note, color = self._friendly_worker_status(key, raw_status, runtime_role)
            self._worker_value_vars[key].set(human_status)
            self._worker_note_vars[key].set(note)
            label = self._status_labels.get(key)
            if label is not None:
                label.configure(fg=color)

        scanner_preview = list(status.get("scanner_thinking_preview", []) or [])
        scanner_symbols = list(status.get("scanner_cryptos_preview", []) or status.get("scanner_symbols_preview", []) or [])
        lines = [
            "Qué está pasando ahora:",
            f"- Esta app corre con rol '{runtime_role}'. Por eso scanner puede aparecer desactivado: no es un fallo, es normal en Model Trainer.",
            f"- Motor de decisión configurado: {scanner_engine}.",
            f"- Última actualización de datos: {status.get('last_data_update', 'N/A')}",
            f"- Última actualización de entrenamiento: {status.get('last_training_update', 'N/A')}",
            f"- Progreso estimado del ciclo de training: {float(status.get('training_progress_pct', 0.0) or 0.0):.1f}%",
            f"- Último error del trainer: {status.get('training_last_error', '') or 'Ninguno'}",
            f"- Snapshots hoy: {status.get('snapshots_today', 0)} | señales hoy: {status.get('signals_today', 0)} | noticias hoy: {status.get('news_events_today', 0)}",
            f"- Outcomes evaluados: {status.get('evaluated_outcomes', 0)} | nuevos desde último entrenamiento: {new_outcomes}",
            f"- Estado de entrenamiento automático: {readiness_reason or 'N/A'}",
            "",
            "Cómo usar esta ventana:",
            "1. Activa procesos automáticos para recolectar datos y etiquetar outcomes.",
            "2. Espera a que la línea de outcomes diga que ya está listo para entrenar.",
            "3. Revisa la tabla de modelos; el mejor candidato no siempre es el último entrenado.",
            "4. Haz clic en un modelo y mira el panel derecho antes de aprobarlo.",
        ]
        if scanner_symbols:
            lines.append("")
            lines.append(f"Universo de símbolos monitorizados: {', '.join(scanner_symbols[:12])}")
        if scanner_preview:
            lines.append("Último thinking del scanner:")
            lines.extend(f"- {item}" for item in scanner_preview)
        self._set_activity("\n".join(lines))

    def _update_candidates(self, payload: dict[str, Any]) -> None:
        self._latest_candidates_payload = dict(payload)
        self._candidate_rows_by_version.clear()
        self._suppress_candidate_select_event = True
        try:
            for item in self.candidates_tree.get_children():
                self.candidates_tree.delete(item)
            rows = list(payload.get("rows", []))
            approved_version = str(payload.get("approved", "") or "")
            approved_row = self._find_row_by_version(rows, approved_version)
            ranked = sorted(rows, key=self._candidate_score, reverse=True)
            for row in rows:
                version = str(row.get("model_version", "") or "")
                if not version:
                    continue
                self._candidate_rows_by_version[version] = row
                state_text = self._candidate_state_text(row)
                score = self._candidate_score(row)
                label = version
                alias = str(row.get("alias", "") or "").strip()
                if alias:
                    label = f"{alias} | {version}"
                self.candidates_tree.insert(
                    "",
                    "end",
                    iid=version,
                    text=label,
                    values=(
                        state_text,
                        f"{score:.3f}",
                        f"{float(row.get('accuracy', 0.0) or 0.0):.3f}",
                        f"{float(row.get('win_rate', 0.0) or 0.0):.3f}",
                        f"{float(row.get('profit_factor', 0.0) or 0.0):.3f}",
                        f"{float(row.get('max_drawdown', 0.0) or 0.0):.3f}",
                        f"{int(row.get('number_of_samples', 0) or 0)}",
                    ),
                )

            selected_version = self.selected_version_var.get().strip()
            if selected_version and selected_version in self._candidate_rows_by_version:
                self.candidates_tree.selection_set(selected_version)
            elif ranked:
                best_version = str(ranked[0].get("model_version", "") or "")
                self.selected_version_var.set(best_version)
                self.candidates_tree.selection_set(best_version)
            else:
                self.selected_version_var.set("")
        finally:
            self._suppress_candidate_select_event = False

        self._render_selected_model_detail(approved_row=approved_row, ranked_rows=ranked)

    def _friendly_worker_status(self, key: str, raw_status: str, runtime_role: str) -> tuple[str, str, str]:
        status_key = str(raw_status or "N/A").strip().lower()
        role_key = str(runtime_role or "training").strip().lower()
        if status_key == "running":
            return "Activo", "Está trabajando ahora mismo.", "#1f7a1f"
        if status_key == "stopped":
            return "Pausado", "Puede activarse desde el botón superior.", "#9a6700"
        if key == "scanner" and status_key == "disabled" and role_key == "training":
            return "No aplica", "El scanner no se usa en Model Trainer; aquí se recolecta y se entrena.", "#666666"
        if status_key == "disabled":
            return "Desactivado", "Está deshabilitado por configuración o por el rol actual.", "#8a1c1c"
        return raw_status or "N/A", "Estado reportado por backend.", "#555555"

    def _candidate_score(self, row: dict[str, Any]) -> float:
        accuracy = float(row.get("accuracy", 0.0) or 0.0)
        precision = float(row.get("precision", 0.0) or 0.0)
        recall = float(row.get("recall", 0.0) or 0.0)
        win_rate = float(row.get("win_rate", 0.0) or 0.0)
        profit_factor = float(row.get("profit_factor", 0.0) or 0.0)
        drawdown = abs(float(row.get("max_drawdown", 0.0) or 0.0))
        pf_score = min(profit_factor / 4.0, 1.0)
        dd_score = max(0.0, 1.0 - min(drawdown / 0.25, 1.0))
        return (
            accuracy * 0.20
            + precision * 0.18
            + recall * 0.17
            + win_rate * 0.15
            + pf_score * 0.20
            + dd_score * 0.10
        )

    def _candidate_state_text(self, row: dict[str, Any]) -> str:
        states: list[str] = []
        if bool(row.get("is_approved", False)):
            states.append("Aprobado")
        if bool(row.get("is_latest", False)):
            states.append("Último")
        if bool(row.get("is_frozen", False)):
            states.append("Congelado")
        return ", ".join(states) if states else "Candidato"

    @staticmethod
    def _find_row_by_version(rows: list[dict[str, Any]], version: str) -> dict[str, Any] | None:
        version_key = str(version or "")
        for row in rows:
            if str(row.get("model_version", "") or "") == version_key:
                return row
        return None

    def _model_rank_text(self, version: str, ranked_rows: list[dict[str, Any]]) -> str:
        if not version:
            return "N/A"
        for idx, row in enumerate(ranked_rows, start=1):
            if str(row.get("model_version", "") or "") == version:
                return f"#{idx} de {len(ranked_rows)}"
        return "N/A"

    def _build_learned_summary(self, row: dict[str, Any]) -> tuple[str, str]:
        precision = float(row.get("precision", 0.0) or 0.0)
        recall = float(row.get("recall", 0.0) or 0.0)
        win_rate = float(row.get("win_rate", 0.0) or 0.0)
        profit_factor = float(row.get("profit_factor", 0.0) or 0.0)
        drawdown = abs(float(row.get("max_drawdown", 0.0) or 0.0))

        if precision >= 0.60:
            entries = "filtra entradas con alta precisión"
        elif precision >= 0.50:
            entries = "filtra entradas con precisión media"
        else:
            entries = "todavía tiene poco criterio al filtrar entradas"

        if recall >= 0.60:
            opportunities = "captura gran parte de las oportunidades"
        elif recall >= 0.45:
            opportunities = "captura oportunidades de forma balanceada"
        else:
            opportunities = "deja pasar bastantes oportunidades"

        if profit_factor >= 1.20 and drawdown <= 0.12:
            profile = "perfil rentable con riesgo controlado"
        elif profit_factor >= 1.0 and drawdown <= 0.18:
            profile = "perfil aceptable pero aún mejorable"
        else:
            profile = "perfil arriesgado o con retorno insuficiente"

        behavior = f"Aprendió a {entries} y a {opportunities}."
        risk = f"Su comportamiento histórico sugiere un {profile} (win={win_rate:.3f}, pf={profit_factor:.3f}, dd={drawdown:.3f})."
        return behavior, risk

    def _render_selected_model_detail(
        self,
        approved_row: dict[str, Any] | None = None,
        ranked_rows: list[dict[str, Any]] | None = None,
        force: bool = False,
    ) -> None:
        version = self.selected_version_var.get().strip()
        if not version:
            self.selected_summary_var.set("Selecciona un modelo para ver qué aprendió y cómo compara.")
            self.selected_compare_var.set("Comparativa: N/A")
            self._set_detail("Sin modelo seleccionado.", force=True)
            self._detail_rendered_version = ""
            return

        if not force and version == self._detail_rendered_version and time.monotonic() < self._detail_scroll_hold_until:
            return

        row = self._candidate_rows_by_version.get(version)
        if row is None:
            self.selected_summary_var.set("Modelo seleccionado no disponible en la lista actual.")
            self.selected_compare_var.set("Comparativa: N/A")
            self._set_detail("No se encontró información para el modelo seleccionado.", force=True)
            self._detail_rendered_version = version
            return

        if ranked_rows is None:
            ranked_rows = sorted(self._candidate_rows_by_version.values(), key=self._candidate_score, reverse=True)
        if approved_row is None:
            approved_version = str(self._latest_candidates_payload.get("approved", "") or "")
            approved_row = self._candidate_rows_by_version.get(approved_version)

        alias = str(row.get("alias", "") or "").strip()
        title = f"{alias} | {version}" if alias else version
        state_text = self._candidate_state_text(row)
        rank_text = self._model_rank_text(version, ranked_rows)
        learned_summary, risk_summary = self._build_learned_summary(row)
        self.selected_summary_var.set(f"{title} | estado={state_text} | ranking={rank_text}")

        compare_text = "Sin baseline aprobado para comparar."
        detail_lines = [
            f"Modelo: {title}",
            f"Estado: {state_text}",
            f"Ranking entre candidatos: {rank_text}",
            f"Entrenado en: {row.get('timestamp', 'N/A')}",
            f"Muestras usadas: {int(row.get('number_of_samples', 0) or 0)}",
            f"Outcomes reales incluidos: {int(row.get('trained_with_outcomes_count', row.get('number_of_samples', 0)) or 0)}",
            f"Tipo de etiqueta: {row.get('label_type', 'N/A')}",
            "",
            "Qué aprendió este modelo:",
            f"- {learned_summary}",
            f"- {risk_summary}",
            "",
            "Métricas:",
            f"- Accuracy: {float(row.get('accuracy', 0.0) or 0.0):.3f}",
            f"- Precision: {float(row.get('precision', 0.0) or 0.0):.3f}",
            f"- Recall: {float(row.get('recall', 0.0) or 0.0):.3f}",
            f"- Win rate: {float(row.get('win_rate', 0.0) or 0.0):.3f}",
            f"- Profit factor: {float(row.get('profit_factor', 0.0) or 0.0):.3f}",
            f"- Max drawdown: {float(row.get('max_drawdown', 0.0) or 0.0):.3f}",
            f"- Score compuesto: {self._candidate_score(row):.3f}",
        ]

        if approved_row is not None and str(approved_row.get('model_version', '') or '') != version:
            delta_acc = float(row.get('accuracy', 0.0) or 0.0) - float(approved_row.get('accuracy', 0.0) or 0.0)
            delta_win = float(row.get('win_rate', 0.0) or 0.0) - float(approved_row.get('win_rate', 0.0) or 0.0)
            delta_pf = float(row.get('profit_factor', 0.0) or 0.0) - float(approved_row.get('profit_factor', 0.0) or 0.0)
            delta_dd = float(row.get('max_drawdown', 0.0) or 0.0) - float(approved_row.get('max_drawdown', 0.0) or 0.0)
            delta_score = self._candidate_score(row) - self._candidate_score(approved_row)
            compare_text = (
                f"Vs aprobado: score {delta_score:+.3f}, acc {delta_acc:+.3f}, "
                f"win {delta_win:+.3f}, pf {delta_pf:+.3f}, dd {delta_dd:+.3f}"
            )
            detail_lines.extend(
                [
                    "",
                    "Comparado con el modelo aprobado:",
                    f"- Score compuesto: {delta_score:+.3f}",
                    f"- Accuracy: {delta_acc:+.3f}",
                    f"- Win rate: {delta_win:+.3f}",
                    f"- Profit factor: {delta_pf:+.3f}",
                    f"- Drawdown: {delta_dd:+.3f} (negativo es mejor)",
                ]
            )
        elif approved_row is not None:
            compare_text = "Este es el modelo actualmente aprobado."

        top_rows = ranked_rows[:5]
        if top_rows:
            detail_lines.extend(["", "Top candidatos actuales:"])
            for idx, top_row in enumerate(top_rows, start=1):
                top_version = str(top_row.get('model_version', '') or '')
                top_alias = str(top_row.get('alias', '') or '').strip()
                top_label = f"{top_alias} | {top_version}" if top_alias else top_version
                detail_lines.append(
                    f"- #{idx} {top_label} | score={self._candidate_score(top_row):.3f} | "
                    f"acc={float(top_row.get('accuracy', 0.0) or 0.0):.3f} | "
                    f"pf={float(top_row.get('profit_factor', 0.0) or 0.0):.3f}"
                )

        self.selected_compare_var.set(compare_text)
        self._set_detail("\n".join(detail_lines), force=force)
        self._detail_rendered_version = version
