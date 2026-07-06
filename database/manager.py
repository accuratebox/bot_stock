from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
import threading
from typing import Any, Iterator


class TradingBrainDatabase:
    def __init__(self, db_path: str) -> None:
        self.path = Path(db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._db_lock = threading.RLock()
        self._init_schema()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        with self._db_lock:
            connection = sqlite3.connect(self.path, timeout=5.0)
            connection.execute("PRAGMA journal_mode=WAL;")
            connection.execute("PRAGMA busy_timeout=5000;")
            connection.row_factory = sqlite3.Row
            try:
                yield connection
                connection.commit()
            finally:
                connection.close()

    def _init_schema(self) -> None:
        statements = [
            """
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                broker TEXT NOT NULL,
                account_name TEXT NOT NULL UNIQUE,
                account_type TEXT NOT NULL,
                is_paper INTEGER NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS bot_funds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER NOT NULL UNIQUE,
                max_capital_assigned REAL NOT NULL,
                available_capital REAL NOT NULL,
                capital_used REAL NOT NULL,
                max_position_size REAL NOT NULL,
                max_daily_loss REAL NOT NULL,
                enabled INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(account_id) REFERENCES accounts(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS assets_watchlist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL UNIQUE,
                asset_type TEXT NOT NULL,
                broker_supported INTEGER NOT NULL,
                active INTEGER NOT NULL,
                min_volume REAL NOT NULL,
                max_spread_allowed REAL NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS market_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                symbol TEXT NOT NULL,
                asset_type TEXT NOT NULL,
                price REAL NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                vwap REAL NOT NULL,
                rsi REAL NOT NULL,
                atr REAL NOT NULL,
                spread REAL NOT NULL,
                percent_change_1m REAL NOT NULL,
                percent_change_5m REAL NOT NULL,
                percent_change_15m REAL NOT NULL,
                source TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS news_social_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                symbol TEXT NOT NULL,
                asset_type TEXT NOT NULL,
                source TEXT NOT NULL,
                title_or_text TEXT NOT NULL,
                url TEXT NOT NULL,
                author TEXT NOT NULL,
                influence_score REAL NOT NULL,
                sentiment_score REAL NOT NULL,
                ai_summary TEXT NOT NULL,
                ai_classification TEXT NOT NULL,
                raw_payload TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                symbol TEXT NOT NULL,
                asset_type TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                confidence_score REAL NOT NULL,
                model_version TEXT NOT NULL,
                reason TEXT NOT NULL,
                entry_price REAL NOT NULL,
                suggested_limit_price REAL NOT NULL,
                invalidation_price REAL NOT NULL,
                take_profit_price REAL NOT NULL,
                risk_level TEXT NOT NULL,
                features_json TEXT NOT NULL,
                openai_analysis_json TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                account_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                asset_type TEXT NOT NULL,
                side TEXT NOT NULL,
                order_type TEXT NOT NULL,
                qty REAL NOT NULL,
                limit_price REAL NOT NULL,
                filled_price REAL NOT NULL,
                fees REAL NOT NULL,
                status TEXT NOT NULL,
                initiated_by TEXT NOT NULL DEFAULT 'unknown',
                broker_order_id TEXT NOT NULL,
                signal_id INTEGER,
                created_at TEXT NOT NULL,
                FOREIGN KEY(account_id) REFERENCES accounts(id),
                FOREIGN KEY(signal_id) REFERENCES signals(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                asset_type TEXT NOT NULL,
                qty REAL NOT NULL,
                total_cost_basis REAL NOT NULL,
                average_cost REAL NOT NULL,
                current_price REAL NOT NULL,
                unrealized_pnl REAL NOT NULL,
                realized_pnl REAL NOT NULL,
                status TEXT NOT NULL,
                last_updated TEXT NOT NULL,
                UNIQUE(account_id, symbol),
                FOREIGN KEY(account_id) REFERENCES accounts(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS model_training_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                model_version TEXT NOT NULL,
                asset_scope TEXT NOT NULL,
                dataset_start TEXT NOT NULL,
                dataset_end TEXT NOT NULL,
                number_of_samples INTEGER NOT NULL,
                trained_with_outcomes_count INTEGER NOT NULL DEFAULT 0,
                label_type TEXT NOT NULL DEFAULT 'result_15m_fallback_30m',
                accuracy REAL NOT NULL,
                precision REAL NOT NULL,
                recall REAL NOT NULL,
                win_rate REAL NOT NULL,
                profit_factor REAL NOT NULL,
                max_drawdown REAL NOT NULL,
                approved_for_paper INTEGER NOT NULL DEFAULT 0,
                approved_for_live INTEGER NOT NULL,
                notes TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS bot_decision_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                symbol TEXT NOT NULL,
                decision TEXT NOT NULL,
                reason TEXT NOT NULL,
                blocked_reason TEXT NOT NULL,
                raw_context_json TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS ai_runtime_settings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER NOT NULL UNIQUE,
                signal_only_mode INTEGER NOT NULL,
                paper_trading INTEGER NOT NULL,
                live_trading_enabled INTEGER NOT NULL,
                manual_approval_required INTEGER NOT NULL,
                kill_switch INTEGER NOT NULL,
                auto_trade_stocks_enabled INTEGER NOT NULL DEFAULT 1,
                auto_trade_cryptos_enabled INTEGER NOT NULL DEFAULT 1,
                scanner_decision_engine TEXT NOT NULL DEFAULT 'heuristic',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(account_id) REFERENCES accounts(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS signal_outcomes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INTEGER NOT NULL UNIQUE,
                symbol TEXT NOT NULL,
                asset_type TEXT,
                entry_price REAL,
                timestamp_signal TEXT,
                evaluated_at TEXT NOT NULL,
                price_after_5m REAL,
                max_profit_5m REAL,
                max_drawdown_5m REAL,
                result_5m TEXT,
                price_after_15m REAL,
                max_profit_15m REAL,
                max_drawdown_15m REAL,
                result_15m TEXT,
                price_after_30m REAL,
                max_profit_30m REAL,
                max_drawdown_30m REAL,
                result_30m TEXT,
                price_after_60m REAL,
                max_profit_60m REAL,
                max_drawdown_60m REAL,
                result_60m TEXT,
                final_label TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(signal_id) REFERENCES signals(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS crypto_global_market_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                source TEXT NOT NULL,
                current_price REAL NOT NULL,
                total_volume REAL NOT NULL,
                market_cap REAL NOT NULL,
                price_change_percentage_24h REAL NOT NULL,
                fetched_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(symbol, source)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS crypto_volume_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                symbol TEXT NOT NULL,
                normalized_symbol TEXT NOT NULL,
                price REAL NOT NULL,
                local_volume_1m REAL NOT NULL,
                local_volume_5m REAL NOT NULL,
                local_volume_15m REAL NOT NULL,
                local_volume_1m_usd REAL NOT NULL DEFAULT 0,
                local_volume_5m_usd REAL NOT NULL DEFAULT 0,
                local_volume_15m_usd REAL NOT NULL DEFAULT 0,
                local_volume_unit TEXT NOT NULL DEFAULT 'UNKNOWN',
                volume_has_clear_unit INTEGER NOT NULL DEFAULT 0,
                trade_count_1m INTEGER NOT NULL,
                trade_count_5m INTEGER NOT NULL,
                trade_count_15m INTEGER NOT NULL,
                global_volume_24h_usd REAL NOT NULL,
                alpaca_24h_volume REAL NOT NULL,
                volume_source TEXT NOT NULL,
                global_volume_source TEXT NOT NULL,
                volume_status TEXT NOT NULL,
                latest_bar_age_seconds REAL NOT NULL DEFAULT 999999,
                data_stale INTEGER NOT NULL DEFAULT 1,
                websocket_stale INTEGER NOT NULL DEFAULT 1,
                volume_valid_for_live_analysis INTEGER NOT NULL DEFAULT 0,
                error_message TEXT NOT NULL,
                last_update TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
        ]
        with self.connect() as connection:
            for statement in statements:
                connection.execute(statement)
            self._ensure_schema_migrations(connection)

    def _ensure_schema_migrations(self, connection: sqlite3.Connection) -> None:
        self._ensure_column_exists(connection, "signal_outcomes", "asset_type", "TEXT")
        self._ensure_column_exists(connection, "signal_outcomes", "entry_price", "REAL")
        self._ensure_column_exists(connection, "signal_outcomes", "timestamp_signal", "TEXT")
        self._ensure_column_exists(connection, "signal_outcomes", "price_after_5m", "REAL")
        self._ensure_column_exists(connection, "signal_outcomes", "price_after_15m", "REAL")
        self._ensure_column_exists(connection, "signal_outcomes", "price_after_30m", "REAL")
        self._ensure_column_exists(connection, "signal_outcomes", "price_after_60m", "REAL")
        self._ensure_column_exists(connection, "signal_outcomes", "final_label", "TEXT")

        self._ensure_column_exists(connection, "model_training_runs", "trained_with_outcomes_count", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column_exists(connection, "model_training_runs", "label_type", "TEXT NOT NULL DEFAULT 'result_15m_fallback_30m'")
        self._ensure_column_exists(connection, "model_training_runs", "approved_for_paper", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column_exists(connection, "ai_runtime_settings", "auto_trade_stocks_enabled", "INTEGER NOT NULL DEFAULT 1")
        self._ensure_column_exists(connection, "ai_runtime_settings", "auto_trade_cryptos_enabled", "INTEGER NOT NULL DEFAULT 1")
        self._ensure_column_exists(connection, "ai_runtime_settings", "scanner_decision_engine", "TEXT NOT NULL DEFAULT 'heuristic'")
        self._ensure_column_exists(connection, "trades", "initiated_by", "TEXT NOT NULL DEFAULT 'unknown'")
        self._ensure_column_exists(connection, "crypto_volume_records", "local_volume_1m_usd", "REAL NOT NULL DEFAULT 0")
        self._ensure_column_exists(connection, "crypto_volume_records", "local_volume_5m_usd", "REAL NOT NULL DEFAULT 0")
        self._ensure_column_exists(connection, "crypto_volume_records", "local_volume_15m_usd", "REAL NOT NULL DEFAULT 0")
        self._ensure_column_exists(connection, "crypto_volume_records", "local_volume_unit", "TEXT NOT NULL DEFAULT 'UNKNOWN'")
        self._ensure_column_exists(connection, "crypto_volume_records", "volume_has_clear_unit", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column_exists(connection, "crypto_volume_records", "latest_bar_age_seconds", "REAL NOT NULL DEFAULT 999999")
        self._ensure_column_exists(connection, "crypto_volume_records", "data_stale", "INTEGER NOT NULL DEFAULT 1")
        self._ensure_column_exists(connection, "crypto_volume_records", "websocket_stale", "INTEGER NOT NULL DEFAULT 1")
        self._ensure_column_exists(connection, "crypto_volume_records", "volume_valid_for_live_analysis", "INTEGER NOT NULL DEFAULT 0")

    def _ensure_column_exists(
        self,
        connection: sqlite3.Connection,
        table_name: str,
        column_name: str,
        column_definition: str,
    ) -> None:
        rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        existing_columns = {str(row[1]) for row in rows}
        if column_name in existing_columns:
            return
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}")

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def upsert_account(
        self,
        broker: str,
        account_name: str,
        account_type: str,
        is_paper: bool,
        status: str,
    ) -> int:
        now = self._now_iso()
        with self.connect() as connection:
            existing = connection.execute(
                "SELECT id FROM accounts WHERE account_name = ?",
                (account_name,),
            ).fetchone()
            if existing is None:
                cursor = connection.execute(
                    """
                    INSERT INTO accounts (
                        broker, account_name, account_type, is_paper, status, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (broker, account_name, account_type, 1 if is_paper else 0, status, now, now),
                )
                return int(cursor.lastrowid)

            account_id = int(existing["id"])
            connection.execute(
                """
                UPDATE accounts
                SET broker = ?, account_type = ?, is_paper = ?, status = ?, updated_at = ?
                WHERE id = ?
                """,
                (broker, account_type, 1 if is_paper else 0, status, now, account_id),
            )
            return account_id

    def get_account_by_name(self, account_name: str) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM accounts WHERE account_name = ?",
                (account_name,),
            ).fetchone()
            return dict(row) if row is not None else None

    def list_accounts(self) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute("SELECT * FROM accounts ORDER BY account_name").fetchall()
            return [dict(row) for row in rows]

    def upsert_bot_funds(
        self,
        account_id: int,
        max_capital_assigned: float,
        available_capital: float,
        capital_used: float,
        max_position_size: float,
        max_daily_loss: float,
        enabled: bool,
    ) -> None:
        now = self._now_iso()
        with self.connect() as connection:
            existing = connection.execute(
                "SELECT id, created_at FROM bot_funds WHERE account_id = ?",
                (account_id,),
            ).fetchone()
            if existing is None:
                connection.execute(
                    """
                    INSERT INTO bot_funds (
                        account_id, max_capital_assigned, available_capital, capital_used,
                        max_position_size, max_daily_loss, enabled, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        account_id,
                        max_capital_assigned,
                        available_capital,
                        capital_used,
                        max_position_size,
                        max_daily_loss,
                        1 if enabled else 0,
                        now,
                        now,
                    ),
                )
                return

            connection.execute(
                """
                UPDATE bot_funds
                SET max_capital_assigned = ?, available_capital = ?, capital_used = ?,
                    max_position_size = ?, max_daily_loss = ?, enabled = ?, updated_at = ?
                WHERE account_id = ?
                """,
                (
                    max_capital_assigned,
                    available_capital,
                    capital_used,
                    max_position_size,
                    max_daily_loss,
                    1 if enabled else 0,
                    now,
                    account_id,
                ),
            )

    def get_bot_funds(self, account_id: int) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM bot_funds WHERE account_id = ?",
                (account_id,),
            ).fetchone()
            return dict(row) if row is not None else None

    def upsert_runtime_settings(
        self,
        account_id: int,
        signal_only_mode: bool,
        paper_trading: bool,
        live_trading_enabled: bool,
        manual_approval_required: bool,
        kill_switch: bool,
        auto_trade_stocks_enabled: bool,
        auto_trade_cryptos_enabled: bool,
        scanner_decision_engine: str = "heuristic",
    ) -> None:
        now = self._now_iso()
        with self.connect() as connection:
            row = connection.execute(
                "SELECT id FROM ai_runtime_settings WHERE account_id = ?",
                (account_id,),
            ).fetchone()
            engine = str(scanner_decision_engine or "heuristic").strip().lower()
            if engine not in {"heuristic", "model"}:
                engine = "heuristic"
            payload = (
                1 if signal_only_mode else 0,
                1 if paper_trading else 0,
                1 if live_trading_enabled else 0,
                1 if manual_approval_required else 0,
                1 if kill_switch else 0,
                1 if auto_trade_stocks_enabled else 0,
                1 if auto_trade_cryptos_enabled else 0,
                engine,
                now,
            )
            if row is None:
                connection.execute(
                    """
                    INSERT INTO ai_runtime_settings (
                        account_id, signal_only_mode, paper_trading, live_trading_enabled,
                        manual_approval_required, kill_switch, auto_trade_stocks_enabled,
                        auto_trade_cryptos_enabled, scanner_decision_engine, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (account_id, *payload, now),
                )
                return

            connection.execute(
                """
                UPDATE ai_runtime_settings
                SET signal_only_mode = ?, paper_trading = ?, live_trading_enabled = ?,
                    manual_approval_required = ?, kill_switch = ?,
                    auto_trade_stocks_enabled = ?, auto_trade_cryptos_enabled = ?, scanner_decision_engine = ?,
                    updated_at = ?
                WHERE account_id = ?
                """,
                (*payload, account_id),
            )

    def get_runtime_settings(self, account_id: int) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM ai_runtime_settings WHERE account_id = ?",
                (account_id,),
            ).fetchone()
            return dict(row) if row is not None else None

    def upsert_watchlist_asset(
        self,
        symbol: str,
        asset_type: str,
        broker_supported: bool,
        active: bool,
        min_volume: float,
        max_spread_allowed: float,
    ) -> None:
        now = self._now_iso()
        with self.connect() as connection:
            row = connection.execute(
                "SELECT id FROM assets_watchlist WHERE symbol = ?",
                (symbol,),
            ).fetchone()
            if row is None:
                connection.execute(
                    """
                    INSERT INTO assets_watchlist (
                        symbol, asset_type, broker_supported, active, min_volume,
                        max_spread_allowed, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        symbol,
                        asset_type,
                        1 if broker_supported else 0,
                        1 if active else 0,
                        min_volume,
                        max_spread_allowed,
                        now,
                        now,
                    ),
                )
                return

            connection.execute(
                """
                UPDATE assets_watchlist
                SET asset_type = ?, broker_supported = ?, active = ?, min_volume = ?,
                    max_spread_allowed = ?, updated_at = ?
                WHERE symbol = ?
                """,
                (
                    asset_type,
                    1 if broker_supported else 0,
                    1 if active else 0,
                    min_volume,
                    max_spread_allowed,
                    now,
                    symbol,
                ),
            )

    def list_watchlist_assets(self, active_only: bool = True) -> list[dict[str, Any]]:
        query = "SELECT * FROM assets_watchlist"
        params: tuple[Any, ...] = ()
        if active_only:
            query += " WHERE active = 1"
        query += " ORDER BY symbol"
        with self.connect() as connection:
            rows = connection.execute(query, params).fetchall()
            return [dict(row) for row in rows]

    def insert_market_snapshot(self, payload: dict[str, Any]) -> int:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO market_snapshots (
                    timestamp, symbol, asset_type, price, open, high, low, close, volume,
                    vwap, rsi, atr, spread, percent_change_1m, percent_change_5m,
                    percent_change_15m, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["timestamp"],
                    payload["symbol"],
                    payload["asset_type"],
                    payload["price"],
                    payload["open"],
                    payload["high"],
                    payload["low"],
                    payload["close"],
                    payload["volume"],
                    payload["vwap"],
                    payload["rsi"],
                    payload["atr"],
                    payload["spread"],
                    payload["percent_change_1m"],
                    payload["percent_change_5m"],
                    payload["percent_change_15m"],
                    payload["source"],
                ),
            )
            return int(cursor.lastrowid)

    def insert_news_event(self, payload: dict[str, Any]) -> int:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO news_social_events (
                    timestamp, symbol, asset_type, source, title_or_text, url, author,
                    influence_score, sentiment_score, ai_summary, ai_classification, raw_payload
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["timestamp"],
                    payload["symbol"],
                    payload["asset_type"],
                    payload["source"],
                    payload["title_or_text"],
                    payload["url"],
                    payload["author"],
                    payload["influence_score"],
                    payload["sentiment_score"],
                    payload["ai_summary"],
                    payload["ai_classification"],
                    json.dumps(payload["raw_payload"], ensure_ascii=False),
                ),
            )
            return int(cursor.lastrowid)

    def insert_signal(self, payload: dict[str, Any]) -> int:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO signals (
                    timestamp, symbol, asset_type, signal_type, confidence_score, model_version,
                    reason, entry_price, suggested_limit_price, invalidation_price,
                    take_profit_price, risk_level, features_json, openai_analysis_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["timestamp"],
                    payload["symbol"],
                    payload["asset_type"],
                    payload["signal_type"],
                    payload["confidence_score"],
                    payload["model_version"],
                    payload["reason"],
                    payload["entry_price"],
                    payload["suggested_limit_price"],
                    payload["invalidation_price"],
                    payload["take_profit_price"],
                    payload["risk_level"],
                    json.dumps(payload["features_json"], ensure_ascii=False),
                    json.dumps(payload["openai_analysis_json"], ensure_ascii=False),
                ),
            )
            return int(cursor.lastrowid)

    def insert_trade(self, payload: dict[str, Any]) -> int:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO trades (
                    timestamp, account_id, symbol, asset_type, side, order_type, qty, limit_price,
                    filled_price, fees, status, initiated_by, broker_order_id, signal_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["timestamp"],
                    payload["account_id"],
                    payload["symbol"],
                    payload["asset_type"],
                    payload["side"],
                    payload["order_type"],
                    payload["qty"],
                    payload["limit_price"],
                    payload["filled_price"],
                    payload["fees"],
                    payload["status"],
                    payload.get("initiated_by", "unknown"),
                    payload["broker_order_id"],
                    payload.get("signal_id"),
                    payload.get("created_at", payload["timestamp"]),
                ),
            )
            return int(cursor.lastrowid)

    def list_trades(self, account_id: int, limit: int = 200) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM trades WHERE account_id = ? ORDER BY timestamp DESC LIMIT ?",
                (account_id, limit),
            ).fetchall()
            return [dict(row) for row in rows]

    def list_trades_enriched(self, account_id: int, limit: int = 200) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    t.*,
                    s.model_version AS signal_model_version,
                    s.reason AS signal_reason,
                    s.signal_type AS signal_type_from_signal,
                    s.confidence_score AS signal_confidence_score
                FROM trades t
                LEFT JOIN signals s ON s.id = COALESCE(
                    t.signal_id,
                    (
                        SELECT tb.signal_id
                        FROM trades tb
                        WHERE tb.account_id = t.account_id
                          AND tb.symbol = t.symbol
                          AND lower(tb.side) = 'buy'
                          AND tb.signal_id IS NOT NULL
                          AND tb.timestamp <= t.timestamp
                        ORDER BY tb.timestamp DESC
                        LIMIT 1
                    )
                )
                WHERE t.account_id = ?
                ORDER BY t.timestamp DESC
                LIMIT ?
                """,
                (account_id, limit),
            ).fetchall()
            return [dict(row) for row in rows]

    def upsert_trade_order(self, payload: dict[str, Any]) -> int:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT id FROM trades WHERE account_id = ? AND broker_order_id = ?",
                (payload["account_id"], payload["broker_order_id"]),
            ).fetchone()
            if row is None:
                cursor = connection.execute(
                    """
                    INSERT INTO trades (
                        timestamp, account_id, symbol, asset_type, side, order_type, qty, limit_price,
                        filled_price, fees, status, initiated_by, broker_order_id, signal_id, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        payload["timestamp"],
                        payload["account_id"],
                        payload["symbol"],
                        payload["asset_type"],
                        payload["side"],
                        payload["order_type"],
                        payload["qty"],
                        payload["limit_price"],
                        payload["filled_price"],
                        payload["fees"],
                        payload["status"],
                        payload.get("initiated_by", "trade_updates"),
                        payload["broker_order_id"],
                        payload.get("signal_id"),
                        payload.get("created_at", payload["timestamp"]),
                    ),
                )
                return int(cursor.lastrowid)

            connection.execute(
                """
                UPDATE trades
                SET timestamp = ?, symbol = ?, asset_type = ?, side = ?, order_type = ?,
                    limit_price = ?, filled_price = ?, fees = ?, status = ?,
                    initiated_by = COALESCE(NULLIF(?, 'trade_updates'), initiated_by),
                    signal_id = COALESCE(?, signal_id)
                WHERE account_id = ? AND broker_order_id = ?
                """,
                (
                    payload["timestamp"],
                    payload["symbol"],
                    payload["asset_type"],
                    payload["side"],
                    payload["order_type"],
                    payload["limit_price"],
                    payload["filled_price"],
                    payload["fees"],
                    payload["status"],
                    payload.get("initiated_by", "trade_updates"),
                    payload.get("signal_id"),
                    payload["account_id"],
                    payload["broker_order_id"],
                ),
            )
            current = connection.execute(
                "SELECT id FROM trades WHERE account_id = ? AND broker_order_id = ?",
                (payload["account_id"], payload["broker_order_id"]),
            ).fetchone()
            return int(current["id"]) if current is not None else 0

    def upsert_position(self, payload: dict[str, Any]) -> None:
        now = payload.get("last_updated", self._now_iso())
        with self.connect() as connection:
            row = connection.execute(
                "SELECT id FROM positions WHERE account_id = ? AND symbol = ?",
                (payload["account_id"], payload["symbol"]),
            ).fetchone()
            if row is None:
                connection.execute(
                    """
                    INSERT INTO positions (
                        account_id, symbol, asset_type, qty, total_cost_basis, average_cost,
                        current_price, unrealized_pnl, realized_pnl, status, last_updated
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        payload["account_id"],
                        payload["symbol"],
                        payload["asset_type"],
                        payload["qty"],
                        payload["total_cost_basis"],
                        payload["average_cost"],
                        payload["current_price"],
                        payload["unrealized_pnl"],
                        payload["realized_pnl"],
                        payload["status"],
                        now,
                    ),
                )
                return

            connection.execute(
                """
                UPDATE positions
                SET asset_type = ?, qty = ?, total_cost_basis = ?, average_cost = ?,
                    current_price = ?, unrealized_pnl = ?, realized_pnl = ?, status = ?,
                    last_updated = ?
                WHERE account_id = ? AND symbol = ?
                """,
                (
                    payload["asset_type"],
                    payload["qty"],
                    payload["total_cost_basis"],
                    payload["average_cost"],
                    payload["current_price"],
                    payload["unrealized_pnl"],
                    payload["realized_pnl"],
                    payload["status"],
                    now,
                    payload["account_id"],
                    payload["symbol"],
                ),
            )

    def list_positions(self, account_id: int) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM positions WHERE account_id = ? ORDER BY symbol",
                (account_id,),
            ).fetchall()
            return [dict(row) for row in rows]

    def close_missing_positions(self, account_id: int, open_symbols: set[str]) -> None:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT symbol FROM positions WHERE account_id = ?",
                (account_id,),
            ).fetchall()
            for row in rows:
                symbol = str(row["symbol"])
                if symbol in open_symbols:
                    continue
                connection.execute(
                    "UPDATE positions SET qty = 0, status = 'CLOSED', last_updated = ? WHERE account_id = ? AND symbol = ?",
                    (self._now_iso(), account_id, symbol),
                )

    def insert_training_run(self, payload: dict[str, Any]) -> int:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO model_training_runs (
                    timestamp, model_version, asset_scope, dataset_start, dataset_end,
                    number_of_samples, trained_with_outcomes_count, label_type,
                    accuracy, precision, recall, win_rate,
                    profit_factor, max_drawdown, approved_for_paper, approved_for_live, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["timestamp"],
                    payload["model_version"],
                    payload["asset_scope"],
                    payload["dataset_start"],
                    payload["dataset_end"],
                    payload["number_of_samples"],
                    payload.get("trained_with_outcomes_count", payload["number_of_samples"]),
                    payload.get("label_type", "result_15m_fallback_30m"),
                    payload["accuracy"],
                    payload["precision"],
                    payload["recall"],
                    payload["win_rate"],
                    payload["profit_factor"],
                    payload["max_drawdown"],
                    1 if payload.get("approved_for_paper", False) else 0,
                    1 if payload["approved_for_live"] else 0,
                    payload["notes"],
                ),
            )
            return int(cursor.lastrowid)

    def latest_training_run(self) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM model_training_runs ORDER BY timestamp DESC LIMIT 1"
            ).fetchone()
            return dict(row) if row is not None else None

    def list_training_runs(self, limit: int = 100) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM model_training_runs ORDER BY timestamp DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
            return [dict(row) for row in rows]

    def insert_decision_log(self, payload: dict[str, Any]) -> int:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO bot_decision_logs (
                    timestamp, symbol, decision, reason, blocked_reason, raw_context_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["timestamp"],
                    payload["symbol"],
                    payload["decision"],
                    payload["reason"],
                    payload["blocked_reason"],
                    json.dumps(payload["raw_context_json"], ensure_ascii=False),
                ),
            )
            return int(cursor.lastrowid)

    def latest_decision_logs(self, limit: int = 100) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM bot_decision_logs ORDER BY timestamp DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]

    def latest_signals(self, limit: int = 50) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM signals ORDER BY timestamp DESC LIMIT ?",
                (limit,),
            ).fetchall()
            result: list[dict[str, Any]] = []
            for row in rows:
                payload = dict(row)
                payload["features_json"] = json.loads(payload["features_json"])
                payload["openai_analysis_json"] = json.loads(payload["openai_analysis_json"])
                result.append(payload)
            return result

    def list_signals_since(self, since_iso: str) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM signals WHERE timestamp >= ? ORDER BY timestamp DESC",
                (since_iso,),
            ).fetchall()
            result: list[dict[str, Any]] = []
            for row in rows:
                payload = dict(row)
                payload["features_json"] = json.loads(payload["features_json"])
                payload["openai_analysis_json"] = json.loads(payload["openai_analysis_json"])
                result.append(payload)
            return result

    def latest_market_snapshots(self, symbol: str, limit: int = 120) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM market_snapshots WHERE symbol = ? ORDER BY timestamp DESC LIMIT ?",
                (symbol, limit),
            ).fetchall()
            return [dict(row) for row in rows]

    def list_news_events_since(self, since_iso: str, symbol: str = "", limit: int = 200) -> list[dict[str, Any]]:
        query = "SELECT * FROM news_social_events WHERE timestamp >= ?"
        params: list[Any] = [since_iso]
        if symbol.strip():
            query += " AND symbol = ?"
            params.append(symbol.strip().upper())
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(int(limit))
        with self.connect() as connection:
            rows = connection.execute(query, tuple(params)).fetchall()
            result: list[dict[str, Any]] = []
            for row in rows:
                payload = dict(row)
                payload["raw_payload"] = json.loads(str(payload.get("raw_payload", "{}") or "{}"))
                result.append(payload)
            return result

    def market_snapshots_between(self, symbol: str, start_iso: str, end_iso: str) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM market_snapshots
                WHERE symbol = ? AND timestamp >= ? AND timestamp <= ?
                ORDER BY timestamp ASC
                """,
                (symbol, start_iso, end_iso),
            ).fetchall()
            return [dict(row) for row in rows]

    def upsert_crypto_global_market_data(
        self,
        *,
        symbol: str,
        source: str,
        current_price: float,
        total_volume: float,
        market_cap: float,
        price_change_percentage_24h: float,
        fetched_at: str,
    ) -> None:
        now = self._now_iso()
        symbol_norm = str(symbol or "").upper().replace(" ", "")
        source_norm = str(source or "unknown").strip().lower()
        with self.connect() as connection:
            row = connection.execute(
                "SELECT id FROM crypto_global_market_data WHERE symbol = ? AND source = ?",
                (symbol_norm, source_norm),
            ).fetchone()
            payload = (
                float(current_price),
                float(total_volume),
                float(market_cap),
                float(price_change_percentage_24h),
                str(fetched_at),
                now,
            )
            if row is None:
                connection.execute(
                    """
                    INSERT INTO crypto_global_market_data (
                        symbol, source, current_price, total_volume, market_cap,
                        price_change_percentage_24h, fetched_at, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        symbol_norm,
                        source_norm,
                        *payload[:-1],
                        now,
                        payload[-1],
                    ),
                )
                return

            connection.execute(
                """
                UPDATE crypto_global_market_data
                SET current_price = ?, total_volume = ?, market_cap = ?,
                    price_change_percentage_24h = ?, fetched_at = ?, updated_at = ?
                WHERE symbol = ? AND source = ?
                """,
                (
                    *payload,
                    symbol_norm,
                    source_norm,
                ),
            )

    def insert_crypto_volume_record(self, payload: dict[str, Any]) -> int:
        now = self._now_iso()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO crypto_volume_records (
                    timestamp, symbol, normalized_symbol, price,
                    local_volume_1m, local_volume_5m, local_volume_15m,
                    local_volume_1m_usd, local_volume_5m_usd, local_volume_15m_usd,
                    local_volume_unit, volume_has_clear_unit,
                    trade_count_1m, trade_count_5m, trade_count_15m,
                    global_volume_24h_usd, alpaca_24h_volume,
                    volume_source, global_volume_source, volume_status,
                    latest_bar_age_seconds, data_stale, websocket_stale, volume_valid_for_live_analysis,
                    error_message, last_update, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(payload.get("timestamp", now) or now),
                    str(payload.get("symbol", "") or ""),
                    str(payload.get("normalized_symbol", "") or ""),
                    float(payload.get("price", 0.0) or 0.0),
                    float(payload.get("local_volume_1m", 0.0) or 0.0),
                    float(payload.get("local_volume_5m", 0.0) or 0.0),
                    float(payload.get("local_volume_15m", 0.0) or 0.0),
                    float(payload.get("local_volume_1m_usd", 0.0) or 0.0),
                    float(payload.get("local_volume_5m_usd", 0.0) or 0.0),
                    float(payload.get("local_volume_15m_usd", 0.0) or 0.0),
                    str(payload.get("local_volume_unit", "UNKNOWN") or "UNKNOWN"),
                    1 if bool(payload.get("volume_has_clear_unit", False)) else 0,
                    int(payload.get("trade_count_1m", 0) or 0),
                    int(payload.get("trade_count_5m", 0) or 0),
                    int(payload.get("trade_count_15m", 0) or 0),
                    float(payload.get("global_volume_24h_usd", 0.0) or 0.0),
                    float(payload.get("alpaca_24h_volume", 0.0) or 0.0),
                    str(payload.get("volume_source", "unknown") or "unknown"),
                    str(payload.get("global_volume_source", "unknown") or "unknown"),
                    str(payload.get("volume_status", "UNKNOWN") or "UNKNOWN"),
                    float(payload.get("latest_bar_age_seconds", 999999.0) or 999999.0),
                    1 if bool(payload.get("data_stale", True)) else 0,
                    1 if bool(payload.get("websocket_stale", True)) else 0,
                    1 if bool(payload.get("volume_valid_for_live_analysis", False)) else 0,
                    str(payload.get("error_message", "") or ""),
                    str(payload.get("last_update", payload.get("timestamp", now)) or now),
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def latest_crypto_volume_record(self, normalized_symbol: str) -> dict[str, Any] | None:
        symbol = str(normalized_symbol or "").upper().replace(" ", "")
        if not symbol:
            return None
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM crypto_volume_records
                WHERE normalized_symbol = ?
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (symbol,),
            ).fetchone()
            return dict(row) if row is not None else None

    def get_latest_crypto_global_market_data(
        self,
        *,
        symbol: str,
        source: str = "coingecko",
        max_age_seconds: int | None = None,
    ) -> dict[str, Any] | None:
        symbol_norm = str(symbol or "").upper().replace(" ", "")
        source_norm = str(source or "coingecko").strip().lower()
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM crypto_global_market_data
                WHERE symbol = ? AND source = ?
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (symbol_norm, source_norm),
            ).fetchone()
            if row is None:
                return None
            payload = dict(row)

        if max_age_seconds is None:
            return payload

        fetched_at = str(payload.get("fetched_at", "") or "")
        if not fetched_at:
            return None
        try:
            fetched_dt = datetime.fromisoformat(fetched_at)
        except ValueError:
            return None
        if fetched_dt.tzinfo is None:
            fetched_dt = fetched_dt.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - fetched_dt).total_seconds()
        if age > float(max_age_seconds):
            return None
        return payload

    def upsert_signal_outcome(self, signal_id: int, symbol: str, updates: dict[str, Any]) -> None:
        now = self._now_iso()
        with self.connect() as connection:
            row = connection.execute(
                "SELECT id FROM signal_outcomes WHERE signal_id = ?",
                (signal_id,),
            ).fetchone()
            if row is None:
                connection.execute(
                    """
                    INSERT INTO signal_outcomes (
                        signal_id, symbol, evaluated_at, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (signal_id, symbol, now, now, now),
                )

            assignments = []
            values: list[Any] = []
            for key, value in updates.items():
                assignments.append(f"{key} = ?")
                values.append(value)
            assignments.extend(["evaluated_at = ?", "updated_at = ?"])
            values.extend([now, now, signal_id])
            connection.execute(
                f"UPDATE signal_outcomes SET {', '.join(assignments)} WHERE signal_id = ?",
                tuple(values),
            )

    def get_signal_outcome(self, signal_id: int) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM signal_outcomes WHERE signal_id = ?",
                (signal_id,),
            ).fetchone()
            return dict(row) if row is not None else None

    def count_snapshots_today(self) -> int:
        today_prefix = datetime.now(timezone.utc).date().isoformat()
        with self.connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM market_snapshots WHERE timestamp LIKE ?",
                (f"{today_prefix}%",),
            ).fetchone()
            return int(row["count"]) if row is not None else 0

    def count_signals_today(self) -> int:
        today_prefix = datetime.now(timezone.utc).date().isoformat()
        with self.connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM signals WHERE timestamp LIKE ?",
                (f"{today_prefix}%",),
            ).fetchone()
            return int(row["count"]) if row is not None else 0

    def count_news_events_today(self) -> int:
        today_prefix = datetime.now(timezone.utc).date().isoformat()
        with self.connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM news_social_events WHERE timestamp LIKE ?",
                (f"{today_prefix}%",),
            ).fetchone()
            return int(row["count"]) if row is not None else 0

    def count_evaluated_outcomes(self) -> int:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM signal_outcomes
                                WHERE final_label IS NOT NULL
                """
            ).fetchone()
            return int(row["count"]) if row is not None else 0

    def count_evaluated_outcomes_today(self) -> int:
        today_prefix = datetime.now(timezone.utc).date().isoformat()
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM signal_outcomes
                WHERE updated_at LIKE ?
                                    AND final_label IS NOT NULL
                """,
                (f"{today_prefix}%",),
            ).fetchone()
            return int(row["count"]) if row is not None else 0

    def dashboard_summary(self, account_id: int) -> dict[str, Any]:
        with self.connect() as connection:
            funds_row = connection.execute(
                "SELECT * FROM bot_funds WHERE account_id = ?",
                (account_id,),
            ).fetchone()
            positions = connection.execute(
                "SELECT * FROM positions WHERE account_id = ?",
                (account_id,),
            ).fetchall()
            trades = connection.execute(
                "SELECT * FROM trades WHERE account_id = ?",
                (account_id,),
            ).fetchall()
            signals_count = connection.execute(
                "SELECT COUNT(*) AS count FROM signals"
            ).fetchone()

        total_realized = 0.0
        day_realized = 0.0
        winners = 0
        losers = 0
        today = datetime.now(timezone.utc).date().isoformat()
        for trade in trades:
            if str(trade["side"]).lower() != "sell":
                continue
            pnl = float(trade["filled_price"] or 0.0) * float(trade["qty"] or 0.0)
            total_realized += pnl
            timestamp = str(trade["timestamp"])
            if timestamp.startswith(today):
                day_realized += pnl
            if pnl >= 0:
                winners += 1
            else:
                losers += 1

        open_positions = [dict(row) for row in positions if float(row["qty"] or 0.0) > 0]
        holds = [row for row in open_positions if str(row["status"]).upper() == "HOLD"]
        win_rate = (winners / max(winners + losers, 1)) * 100.0
        return {
            "funds": dict(funds_row) if funds_row is not None else None,
            "open_positions": len(open_positions),
            "hold_positions": len(holds),
            "signals_active": int(signals_count["count"]) if signals_count is not None else 0,
            "daily_pnl": day_realized,
            "total_pnl": total_realized,
            "winners": winners,
            "losers": losers,
            "win_rate": win_rate,
        }

    def load_training_samples(self) -> list[dict[str, Any]]:
        samples: list[dict[str, Any]] = []
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    s.features_json,
                    s.confidence_score,
                    s.timestamp,
                    o.final_label,
                    o.result_15m,
                    o.result_30m,
                    o.max_profit_15m,
                    o.max_drawdown_15m,
                    o.max_profit_30m,
                    o.max_drawdown_30m
                FROM signals AS s
                INNER JOIN signal_outcomes AS o ON o.signal_id = s.id
                WHERE o.final_label IS NOT NULL
                ORDER BY s.timestamp ASC
                """
            ).fetchall()
        for row in rows:
            features = json.loads(str(row["features_json"]))
            final_label = str(row["final_label"] or "neutral").strip().lower()
            label = 1 if final_label == "win" else 0
            if final_label not in {"win", "loss", "neutral"}:
                continue
            label_source = "result_15m" if row["result_15m"] is not None else "result_30m"
            max_profit = float(row["max_profit_15m"] or 0.0) if label_source == "result_15m" else float(row["max_profit_30m"] or 0.0)
            max_drawdown = float(row["max_drawdown_15m"] or 0.0) if label_source == "result_15m" else float(row["max_drawdown_30m"] or 0.0)
            samples.append(
                {
                    "features": features,
                    "label": label,
                    "final_label": final_label,
                    "label_source": label_source,
                    "timestamp": str(row["timestamp"] or ""),
                    "confidence_score": float(row["confidence_score"] or 0.0),
                    "max_profit_pct": max_profit,
                    "max_drawdown_pct": max_drawdown,
                }
            )
        return samples

    def cleanup_old_data(
        self,
        snapshot_minutes_days: int,
        snapshot_five_min_days: int,
        logs_days: int,
        keep_model_versions: int,
    ) -> None:
        now = datetime.now(timezone.utc)
        minute_cutoff = (now - timedelta(days=snapshot_minutes_days)).isoformat()
        five_min_cutoff = (now - timedelta(days=snapshot_five_min_days)).isoformat()
        logs_cutoff = (now - timedelta(days=logs_days)).isoformat()

        with self.connect() as connection:
            connection.execute(
                "DELETE FROM market_snapshots WHERE timestamp < ? AND source = '1m'",
                (minute_cutoff,),
            )
            connection.execute(
                "DELETE FROM market_snapshots WHERE timestamp < ? AND source = '5m'",
                (five_min_cutoff,),
            )
            connection.execute(
                "DELETE FROM bot_decision_logs WHERE timestamp < ?",
                (logs_cutoff,),
            )

            rows = connection.execute(
                "SELECT id FROM model_training_runs ORDER BY timestamp DESC"
            ).fetchall()
            stale = rows[keep_model_versions:]
            for row in stale:
                connection.execute("DELETE FROM model_training_runs WHERE id = ?", (row["id"],))
