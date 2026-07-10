from __future__ import annotations

import csv
import sqlite3
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, simpledialog, ttk


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "ai_trading_brain.sqlite"
DEFAULT_LIMIT = 200

PRESET_QUERIES: dict[str, str] = {
    "Ultimas senales": f"""
SELECT id, timestamp, symbol, asset_type, signal_type, confidence_score,
       model_version, take_profit_price
FROM signals
ORDER BY timestamp DESC
LIMIT {DEFAULT_LIMIT}
""".strip(),
    "Outcomes evaluados": f"""
SELECT s.timestamp, s.symbol, s.signal_type, s.confidence_score,
       o.final_label, o.result_5m, o.result_15m, o.result_30m,
       o.max_profit_15m, o.max_drawdown_15m
FROM signals s
JOIN signal_outcomes o ON o.signal_id = s.id
WHERE o.final_label IS NOT NULL
   OR o.result_5m IS NOT NULL
   OR o.result_15m IS NOT NULL
   OR o.result_30m IS NOT NULL
ORDER BY s.timestamp DESC
LIMIT {DEFAULT_LIMIT}
""".strip(),
    "Entrenamientos": f"""
SELECT id, timestamp, model_version, asset_scope, number_of_samples,
       accuracy, precision, recall, win_rate, profit_factor,
       max_drawdown, approved_for_paper, approved_for_live, label_type
FROM model_training_runs
ORDER BY timestamp DESC
LIMIT {DEFAULT_LIMIT}
""".strip(),
    "Conteo por etiqueta": """
SELECT COALESCE(final_label, 'pendiente') AS final_label, COUNT(*) AS total
FROM signal_outcomes
GROUP BY COALESCE(final_label, 'pendiente')
ORDER BY total DESC
""".strip(),
    "Esquema": """
SELECT name AS table_name, type, sql
FROM sqlite_master
WHERE type IN ('table', 'view')
ORDER BY name
""".strip(),
}


class DatabaseViewerApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Visor grafico AI Trading Brain")
        self.root.geometry("1460x900")
        self.root.minsize(1100, 720)

        self.connection = sqlite3.connect(DB_PATH)
        self.connection.row_factory = sqlite3.Row
        self.current_rows: list[dict[str, object]] = []
        self.current_columns: list[str] = []
        self.summary_vars = {
            "signals": tk.StringVar(value="signals: -"),
            "outcomes": tk.StringVar(value="outcomes: -"),
            "training_runs": tk.StringVar(value="training runs: -"),
            "evaluated": tk.StringVar(value="evaluados: -"),
            "db_path": tk.StringVar(value=f"base: {DB_PATH}"),
            "status": tk.StringVar(value="Listo"),
        }
        self.limit_var = tk.StringVar(value=str(DEFAULT_LIMIT))
        self.preset_var = tk.StringVar(value="Outcomes evaluados")

        self._build_ui()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

        self.refresh_summary()
        self.load_preset("Outcomes evaluados")

    def _build_ui(self) -> None:
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(2, weight=1)

        header = ttk.Frame(self.root, padding=(18, 16, 18, 10))
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)

        title = ttk.Label(
            header,
            text="Visor grafico de datos del bot",
            font=("TkDefaultFont", 16, "bold"),
        )
        title.grid(row=0, column=0, sticky="w")

        subtitle = ttk.Label(
            header,
            text="Explora senales, resultados y entrenamientos guardados en SQLite.",
        )
        subtitle.grid(row=1, column=0, sticky="w", pady=(4, 0))

        summary = ttk.Frame(self.root, padding=(18, 0, 18, 12))
        summary.grid(row=1, column=0, sticky="ew")
        for index in range(5):
            summary.columnconfigure(index, weight=1)

        ttk.Label(summary, textvariable=self.summary_vars["signals"], relief="groove", padding=10).grid(
            row=0, column=0, sticky="ew", padx=(0, 8)
        )
        ttk.Label(summary, textvariable=self.summary_vars["outcomes"], relief="groove", padding=10).grid(
            row=0, column=1, sticky="ew", padx=(0, 8)
        )
        ttk.Label(summary, textvariable=self.summary_vars["training_runs"], relief="groove", padding=10).grid(
            row=0, column=2, sticky="ew", padx=(0, 8)
        )
        ttk.Label(summary, textvariable=self.summary_vars["evaluated"], relief="groove", padding=10).grid(
            row=0, column=3, sticky="ew", padx=(0, 8)
        )
        ttk.Label(summary, textvariable=self.summary_vars["db_path"], relief="groove", padding=10).grid(
            row=0, column=4, sticky="ew"
        )

        content = ttk.Panedwindow(self.root, orient="horizontal")
        content.grid(row=2, column=0, sticky="nsew", padx=18, pady=(0, 18))

        left = ttk.Frame(content, padding=14)
        left.columnconfigure(0, weight=1)
        content.add(left, weight=0)

        ttk.Label(left, text="Consultas rapidas", font=("TkDefaultFont", 11, "bold")).grid(
            row=0, column=0, sticky="w"
        )
        ttk.Label(
            left,
            text="Selecciona una vista o escribe SQL abajo para inspeccionar los datos.",
            wraplength=250,
        ).grid(row=1, column=0, sticky="w", pady=(6, 14))

        button_row = 2
        for name in PRESET_QUERIES:
            ttk.Button(left, text=name, command=lambda value=name: self.load_preset(value)).grid(
                row=button_row, column=0, sticky="ew", pady=(0, 8)
            )
            button_row += 1

        ttk.Separator(left).grid(row=button_row, column=0, sticky="ew", pady=12)
        button_row += 1

        ttk.Button(left, text="Refrescar resumen", command=self.refresh_summary).grid(
            row=button_row, column=0, sticky="ew", pady=(0, 8)
        )
        button_row += 1
        ttk.Button(left, text="Exportar vista CSV", command=self.export_current_view).grid(
            row=button_row, column=0, sticky="ew"
        )
        button_row += 1
        ttk.Button(left, text="Borrar todos los datos", command=self.reset_all_data).grid(
            row=button_row, column=0, sticky="ew", pady=(8, 0)
        )

        right = ttk.Frame(content, padding=0)
        right.columnconfigure(0, weight=1)
        right.rowconfigure(2, weight=1)
        right.rowconfigure(3, weight=1)
        content.add(right, weight=1)

        controls = ttk.Frame(right, padding=(14, 14, 14, 10))
        controls.grid(row=0, column=0, sticky="ew")
        controls.columnconfigure(1, weight=1)
        controls.columnconfigure(3, weight=1)

        ttk.Label(controls, text="Vista:").grid(row=0, column=0, sticky="w")
        preset_box = ttk.Combobox(
            controls,
            textvariable=self.preset_var,
            values=list(PRESET_QUERIES.keys()),
            state="readonly",
        )
        preset_box.grid(row=0, column=1, sticky="ew", padx=(8, 12))
        preset_box.bind("<<ComboboxSelected>>", lambda _event: self.load_preset(self.preset_var.get()))

        ttk.Label(controls, text="Limite:").grid(row=0, column=2, sticky="w")
        ttk.Entry(controls, textvariable=self.limit_var, width=10).grid(row=0, column=3, sticky="w", padx=(8, 12))

        ttk.Button(controls, text="Ejecutar SQL", command=self.run_current_sql).grid(row=0, column=4, sticky="e")

        editor_frame = ttk.Frame(right, padding=(14, 0, 14, 10))
        editor_frame.grid(row=1, column=0, sticky="ew")
        editor_frame.columnconfigure(0, weight=1)

        self.sql_text = tk.Text(editor_frame, height=8, wrap="word")
        self.sql_text.grid(row=0, column=0, sticky="ew")
        sql_scroll = ttk.Scrollbar(editor_frame, orient="vertical", command=self.sql_text.yview)
        sql_scroll.grid(row=0, column=1, sticky="ns")
        self.sql_text.configure(yscrollcommand=sql_scroll.set)

        table_frame = ttk.Frame(right, padding=(14, 0, 14, 10))
        table_frame.grid(row=2, column=0, sticky="nsew")
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(0, weight=1)

        self.table = ttk.Treeview(table_frame, show="headings")
        self.table.grid(row=0, column=0, sticky="nsew")
        self.table.bind("<<TreeviewSelect>>", self.on_row_selected)

        yscroll = ttk.Scrollbar(table_frame, orient="vertical", command=self.table.yview)
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll = ttk.Scrollbar(table_frame, orient="horizontal", command=self.table.xview)
        xscroll.grid(row=1, column=0, sticky="ew")
        self.table.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)

        details_frame = ttk.LabelFrame(right, text="Detalle de fila", padding=14)
        details_frame.grid(row=3, column=0, sticky="nsew", padx=14, pady=(0, 14))
        details_frame.columnconfigure(0, weight=1)
        details_frame.rowconfigure(0, weight=1)

        self.details_text = tk.Text(details_frame, height=12, wrap="word")
        self.details_text.grid(row=0, column=0, sticky="nsew")
        details_scroll = ttk.Scrollbar(details_frame, orient="vertical", command=self.details_text.yview)
        details_scroll.grid(row=0, column=1, sticky="ns")
        self.details_text.configure(yscrollcommand=details_scroll.set)

        status = ttk.Label(self.root, textvariable=self.summary_vars["status"], anchor="w", padding=(18, 0, 18, 12))
        status.grid(row=3, column=0, sticky="ew")

    def refresh_summary(self) -> None:
        try:
            counts = {
                "signals": self._fetch_value("SELECT COUNT(*) FROM signals"),
                "outcomes": self._fetch_value("SELECT COUNT(*) FROM signal_outcomes"),
                "training_runs": self._fetch_value("SELECT COUNT(*) FROM model_training_runs"),
                "evaluated": self._fetch_value(
                    "SELECT COUNT(*) FROM signal_outcomes WHERE final_label IS NOT NULL"
                ),
            }
        except sqlite3.Error as exc:
            messagebox.showerror("Error SQLite", str(exc))
            self.summary_vars["status"].set(f"Error al leer resumen: {exc}")
            return

        self.summary_vars["signals"].set(f"signals: {counts['signals']}")
        self.summary_vars["outcomes"].set(f"outcomes: {counts['outcomes']}")
        self.summary_vars["training_runs"].set(f"training runs: {counts['training_runs']}")
        self.summary_vars["evaluated"].set(f"evaluados: {counts['evaluated']}")
        self.summary_vars["status"].set("Resumen actualizado")

    def load_preset(self, preset_name: str) -> None:
        self.preset_var.set(preset_name)
        sql = PRESET_QUERIES[preset_name]
        sql = self._apply_limit_override(sql)
        self.sql_text.delete("1.0", tk.END)
        self.sql_text.insert("1.0", sql)
        self.run_current_sql()

    def run_current_sql(self) -> None:
        sql = self.sql_text.get("1.0", tk.END).strip()
        if not sql:
            messagebox.showwarning("Consulta vacia", "Escribe una consulta SQL primero.")
            return

        try:
            cursor = self.connection.execute(sql)
            rows = cursor.fetchall()
        except sqlite3.Error as exc:
            messagebox.showerror("Error SQL", str(exc))
            self.summary_vars["status"].set(f"Consulta fallida: {exc}")
            return

        columns = [description[0] for description in cursor.description] if cursor.description else []
        self.current_rows = [dict(row) for row in rows]
        self.current_columns = columns
        self._render_table(columns, self.current_rows)
        self._render_details(None)
        self.summary_vars["status"].set(f"Consulta lista: {len(self.current_rows)} filas")

    def export_current_view(self) -> None:
        if not self.current_rows or not self.current_columns:
            messagebox.showinfo("Nada para exportar", "Ejecuta una consulta antes de exportar.")
            return

        output_path = filedialog.asksaveasfilename(
            title="Guardar CSV",
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv")],
            initialfile="ai_trading_brain_export.csv",
        )
        if not output_path:
            return

        try:
            with open(output_path, "w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=self.current_columns)
                writer.writeheader()
                writer.writerows(self.current_rows)
        except OSError as exc:
            messagebox.showerror("Error al exportar", str(exc))
            self.summary_vars["status"].set(f"No se pudo exportar: {exc}")
            return

        self.summary_vars["status"].set(f"CSV exportado en {output_path}")

    def reset_all_data(self) -> None:
        confirmed = messagebox.askyesno(
            "Borrar datos",
            "Esto eliminará señales, outcomes, noticias, snapshots, entrenamientos, posiciones y logs.\n\n"
            "La estructura de la base se conserva.\n\n¿Deseas continuar?",
            icon="warning",
        )
        if not confirmed:
            return

        token = simpledialog.askstring(
            "Confirmación final",
            "Escribe BORRAR para confirmar:",
            parent=self.root,
        )
        if str(token or "").strip().upper() != "BORRAR":
            self.summary_vars["status"].set("Borrado cancelado por el usuario")
            return

        tables_to_clear = [
            "signal_outcomes",
            "trades",
            "positions",
            "signals",
            "market_snapshots",
            "news_social_events",
            "model_training_runs",
            "bot_decision_logs",
            "crypto_volume_records",
            "crypto_global_market_data",
        ]

        try:
            with self.connection:
                for table_name in tables_to_clear:
                    self.connection.execute(f"DELETE FROM {table_name}")
            self.current_rows = []
            self.current_columns = []
            self._render_table([], [])
            self._render_details(None)
            self.refresh_summary()
            self.summary_vars["status"].set("Datos borrados. Listo para comenzar con datos actuales.")
        except sqlite3.Error as exc:
            messagebox.showerror("Error SQLite", str(exc))
            self.summary_vars["status"].set(f"Error al borrar datos: {exc}")

    def on_row_selected(self, _event: object) -> None:
        selection = self.table.selection()
        if not selection:
            self._render_details(None)
            return

        item_index = int(selection[0])
        if 0 <= item_index < len(self.current_rows):
            self._render_details(self.current_rows[item_index])

    def _render_table(self, columns: list[str], rows: list[dict[str, object]]) -> None:
        self.table.delete(*self.table.get_children())
        self.table["columns"] = columns

        for column in columns:
            self.table.heading(column, text=column)
            self.table.column(column, width=150, minwidth=100, anchor="w", stretch=True)

        for index, row in enumerate(rows):
            values = [self._format_cell(row.get(column)) for column in columns]
            self.table.insert("", "end", iid=str(index), values=values)

    def _render_details(self, row: dict[str, object] | None) -> None:
        self.details_text.delete("1.0", tk.END)
        if row is None:
            self.details_text.insert("1.0", "Selecciona una fila para ver el detalle completo.")
            return

        lines = [f"{key}: {value}" for key, value in row.items()]
        self.details_text.insert("1.0", "\n".join(lines))

    def _fetch_value(self, sql: str) -> int:
        row = self.connection.execute(sql).fetchone()
        return int(row[0]) if row else 0

    def _apply_limit_override(self, sql: str) -> str:
        try:
            limit_value = max(1, int(self.limit_var.get().strip()))
        except ValueError:
            limit_value = DEFAULT_LIMIT
            self.limit_var.set(str(DEFAULT_LIMIT))

        stripped = sql.rstrip()
        if "LIMIT" not in stripped.upper():
            return stripped

        parts = stripped.rsplit("LIMIT", 1)
        return f"{parts[0]}LIMIT {limit_value}"

    @staticmethod
    def _format_cell(value: object) -> object:
        if value is None:
            return ""
        text = str(value)
        if len(text) > 120:
            return f"{text[:117]}..."
        return text

    def _on_close(self) -> None:
        try:
            self.connection.close()
        finally:
            self.root.destroy()


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"No se encontro la base de datos: {DB_PATH}")

    root = tk.Tk()
    ttk.Style(root).theme_use("clam")
    DatabaseViewerApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
