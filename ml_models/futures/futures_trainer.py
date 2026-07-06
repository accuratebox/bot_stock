from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score

from ml_models.futures.futures_backtester import FuturesBacktester


@dataclass(frozen=True)
class FuturesTrainerConfig:
    db_path: str = "ml_models/futures/futures_dataset.sqlite"
    models_dir: str = "models/futures"
    model_name: str = "futures_model"
    train_split_ratio: float = 0.8


class FuturesTrainer:
    LABEL_TO_ID = {"LONG": 0, "SHORT": 1, "NO_TRADE": 2}
    ID_TO_LABEL = {0: "LONG", 1: "SHORT", 2: "NO_TRADE"}

    EXCLUDED_COLUMNS = {
        "ts",
        "symbol",
        "label",
        "label_reason",
        "long_result",
        "short_result",
    }

    def __init__(self, config: FuturesTrainerConfig, logger: Any | None = None) -> None:
        self.config = config
        self.logger = logger
        self._db_path = Path(config.db_path)
        self._models_dir = Path(config.models_dir)
        self._models_dir.mkdir(parents=True, exist_ok=True)

    def _log(self, level: str, message: str, *args: Any) -> None:
        if self.logger is not None:
            fn = getattr(self.logger, level, None)
            if callable(fn):
                fn(message, *args)
                return
        if args:
            message = message % args
        print(f"[{level.upper()}] {message}")

    def _load_labels(self, symbol: str) -> list[dict[str, Any]]:
        with sqlite3.connect(self._db_path) as conn:
            rows = conn.execute(
                "SELECT payload FROM futures_labels WHERE symbol = ? ORDER BY ts ASC",
                (symbol.upper(),),
            ).fetchall()
        payloads: list[dict[str, Any]] = []
        for row in rows:
            try:
                payloads.append(json.loads(str(row[0] or "{}")))
            except Exception:
                continue
        return payloads

    @staticmethod
    def _available_feature_names(rows: list[dict[str, Any]]) -> list[str]:
        keys: set[str] = set()
        for row in rows:
            keys.update(row.keys())
        ordered = sorted(key for key in keys if key not in FuturesTrainer.EXCLUDED_COLUMNS and not key.startswith("future_"))
        return [key for key in ordered if key not in {"EV_LONG", "EV_SHORT", "NO_TRADE_SCORE"}]

    @staticmethod
    def _to_vector(row: dict[str, Any], feature_names: list[str]) -> list[float]:
        vector: list[float] = []
        for name in feature_names:
            value = row.get(name, 0.0)
            try:
                vector.append(float(value or 0.0))
            except Exception:
                vector.append(0.0)
        return vector

    def _build_model(self) -> Any:
        try:
            from xgboost import XGBClassifier  # type: ignore

            self._log("info", "Using XGBoost for futures_model")
            return XGBClassifier(
                n_estimators=320,
                max_depth=6,
                learning_rate=0.05,
                subsample=0.9,
                colsample_bytree=0.9,
                objective="multi:softprob",
                num_class=3,
                eval_metric="mlogloss",
                random_state=42,
            )
        except Exception:
            pass

        try:
            from lightgbm import LGBMClassifier  # type: ignore

            self._log("info", "Using LightGBM for futures_model")
            return LGBMClassifier(
                n_estimators=320,
                objective="multiclass",
                num_class=3,
                random_state=42,
            )
        except Exception:
            pass

        self._log("info", "Using RandomForest fallback for futures_model")
        return RandomForestClassifier(n_estimators=360, random_state=42, class_weight="balanced")

    def train(self, symbol: str) -> dict[str, Any]:
        rows = self._load_labels(symbol=symbol)
        if len(rows) < 300:
            return {
                "trained": False,
                "reason": "Not enough labeled futures samples (min 300)",
                "samples": len(rows),
            }

        feature_names = self._available_feature_names(rows)
        if len(feature_names) < 20:
            return {
                "trained": False,
                "reason": "Not enough futures features available for training",
                "samples": len(rows),
                "feature_count": len(feature_names),
            }

        split_idx = int(len(rows) * max(min(self.config.train_split_ratio, 0.95), 0.5))
        split_idx = max(50, min(split_idx, len(rows) - 50))

        train_rows = rows[:split_idx]
        test_rows = rows[split_idx:]

        x_train = [self._to_vector(row, feature_names) for row in train_rows]
        y_train = [self.LABEL_TO_ID.get(str(row.get("label", "NO_TRADE") or "NO_TRADE"), 2) for row in train_rows]
        x_test = [self._to_vector(row, feature_names) for row in test_rows]
        y_test = [self.LABEL_TO_ID.get(str(row.get("label", "NO_TRADE") or "NO_TRADE"), 2) for row in test_rows]

        model = self._build_model()
        model.fit(x_train, y_train)

        pred_ids = [int(value) for value in model.predict(x_test)]
        pred_labels = [self.ID_TO_LABEL.get(value, "NO_TRADE") for value in pred_ids]
        accuracy = float(accuracy_score(y_test, pred_ids))

        backtester = FuturesBacktester()
        backtest_metrics = backtester.evaluate(test_rows, pred_labels)

        trained_at = datetime.now(timezone.utc).isoformat()
        version = f"{self.config.model_name}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        model_path = self._models_dir / f"{version}.pkl"
        metadata = {
            "model_name": self.config.model_name,
            "version": version,
            "trained_at": trained_at,
            "symbol": symbol.upper(),
            "samples_train": len(train_rows),
            "samples_test": len(test_rows),
            "feature_count": len(feature_names),
            "accuracy": accuracy,
            "backtest": backtest_metrics,
        }

        joblib.dump(
            {
                "model": model,
                "feature_names": feature_names,
                "metadata": metadata,
            },
            model_path,
        )

        latest_path = self._models_dir / "futures_model_latest.json"
        latest_path.write_text(json.dumps({"model_path": str(model_path), "metadata": metadata}, indent=2), encoding="utf-8")

        return {
            "trained": True,
            "model_path": str(model_path),
            "metadata": metadata,
            "backtest": backtest_metrics,
            "approved": bool(backtest_metrics.get("approved", False)),
        }
