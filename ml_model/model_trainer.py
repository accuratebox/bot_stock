from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from joblib import dump  # noqa: F401
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score
from sklearn.model_selection import train_test_split

from database.manager import TradingBrainDatabase
from ml_model.feature_builder import build_feature_vector
from ml_model.model_registry import ModelRegistry


class ModelTrainer:
    def __init__(self, database: TradingBrainDatabase, registry: ModelRegistry, logger: Any) -> None:
        self.database = database
        self.registry = registry
        self.logger = logger

    def train_general_model(self) -> dict[str, Any]:
        samples = self.database.load_training_samples()
        if len(samples) < 10:
            return {
                "trained": False,
                "reason": "No hay suficientes muestras para entrenar (minimo 10)",
                "number_of_samples": len(samples),
            }

        x = [build_feature_vector(sample["features"]) for sample in samples]
        y = [int(sample["label"]) for sample in samples]
        x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.25, random_state=42)

        model = RandomForestClassifier(n_estimators=200, random_state=42, class_weight="balanced")
        model.fit(x_train, y_train)
        predictions = model.predict(x_test)

        accuracy = float(accuracy_score(y_test, predictions))
        precision = float(precision_score(y_test, predictions, zero_division=0))
        recall = float(recall_score(y_test, predictions, zero_division=0))
        win_rate = accuracy
        profit_factor = precision if precision > 0 else 0.0
        max_drawdown = 0.0
        version = f"general_model_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

        metadata = {
            "model_version": version,
            "trained_at": datetime.now(timezone.utc).isoformat(),
            "number_of_samples": len(samples),
        }
        self.registry.save_model(model=model, version=version, metadata=metadata)
        self.database.insert_training_run(
            {
                "timestamp": metadata["trained_at"],
                "model_version": version,
                "asset_scope": "general",
                "dataset_start": metadata["trained_at"],
                "dataset_end": metadata["trained_at"],
                "number_of_samples": len(samples),
                "accuracy": accuracy,
                "precision": precision,
                "recall": recall,
                "win_rate": win_rate,
                "profit_factor": profit_factor,
                "max_drawdown": max_drawdown,
                "approved_for_live": False,
                "notes": "Entrenamiento manual del modelo general",
            }
        )
        self.registry.prune_old_versions(keep_last=5)
        return {
            "trained": True,
            "model_version": version,
            "accuracy": accuracy,
            "precision": precision,
            "recall": recall,
            "win_rate": win_rate,
            "profit_factor": profit_factor,
            "max_drawdown": max_drawdown,
            "number_of_samples": len(samples),
        }
