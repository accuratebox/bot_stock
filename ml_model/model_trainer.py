from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from joblib import dump  # noqa: F401
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score

from database.manager import TradingBrainDatabase
from ml_model.feature_builder import build_feature_vector
from ml_model.model_registry import ModelRegistry


class ModelTrainer:
    def __init__(self, database: TradingBrainDatabase, registry: ModelRegistry, logger: Any, settings: Any) -> None:
        self.database = database
        self.registry = registry
        self.logger = logger
        self.settings = settings

    def train_general_model(self) -> dict[str, Any]:
        evaluated_outcomes = int(self.database.count_evaluated_outcomes() or 0)
        if evaluated_outcomes < 200:
            return {
                "trained": False,
                "reason": "No hay suficientes outcomes reales para entrenar.",
                "number_of_samples": evaluated_outcomes,
            }

        samples = self.database.load_training_samples()
        if len(samples) < 20:
            return {
                "trained": False,
                "reason": "No hay suficientes muestras con outcomes reales para entrenar (minimo 20)",
                "number_of_samples": len(samples),
            }

        dataset_start = str(samples[0].get("timestamp", "") or "")
        dataset_end = str(samples[-1].get("timestamp", "") or "")

        split_idx = int(len(samples) * 0.8)
        split_idx = max(1, min(split_idx, len(samples) - 1))
        train_samples = samples[:split_idx]
        test_samples = samples[split_idx:]
        if not test_samples:
            return {
                "trained": False,
                "reason": "No hay suficientes datos para particion temporal train/test.",
                "number_of_samples": len(samples),
            }

        x_train = [build_feature_vector(sample["features"]) for sample in train_samples]
        y_train = [int(sample["label"]) for sample in train_samples]
        x_test = [build_feature_vector(sample["features"]) for sample in test_samples]
        y_test = [int(sample["label"]) for sample in test_samples]

        model = RandomForestClassifier(n_estimators=200, random_state=42, class_weight="balanced")
        model.fit(x_train, y_train)
        predictions = [int(value) for value in model.predict(x_test)]

        accuracy = float(accuracy_score(y_test, predictions))
        precision = float(precision_score(y_test, predictions, zero_division=0))
        recall = float(recall_score(y_test, predictions, zero_division=0))

        predicted_trades: list[dict[str, Any]] = [
            sample
            for sample, pred in zip(test_samples, predictions)
            if int(pred) == 1
        ]
        if not predicted_trades:
            predicted_trades = list(test_samples)

        wins = sum(1 for sample in predicted_trades if str(sample.get("final_label", "")) == "win")
        total = len(predicted_trades)
        win_rate = float(wins / total) if total else 0.0

        gross_profit = sum(max(float(sample.get("max_profit_pct", 0.0) or 0.0), 0.0) for sample in predicted_trades)
        gross_loss = sum(abs(min(float(sample.get("max_drawdown_pct", 0.0) or 0.0), 0.0)) for sample in predicted_trades)
        profit_factor = float(gross_profit / gross_loss) if gross_loss > 0 else float(gross_profit)
        max_drawdown = min(float(sample.get("max_drawdown_pct", 0.0) or 0.0) for sample in predicted_trades)

        label_type = str(getattr(self.settings, "ai_training_label_type", "result_15m_fallback_30m") or "result_15m_fallback_30m")
        version = f"general_model_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

        metadata = {
            "model_version": version,
            "trained_at": datetime.now(timezone.utc).isoformat(),
            "number_of_samples": len(samples),
            "trained_with_outcomes_count": len(samples),
            "label_type": label_type,
            "dataset_start": dataset_start,
            "dataset_end": dataset_end,
        }
        self.registry.save_model(model=model, version=version, metadata=metadata)
        self.database.insert_training_run(
            {
                "timestamp": metadata["trained_at"],
                "model_version": version,
                "asset_scope": "general",
                "dataset_start": metadata["dataset_start"],
                "dataset_end": metadata["dataset_end"],
                "number_of_samples": len(samples),
                "trained_with_outcomes_count": len(samples),
                "label_type": label_type,
                "accuracy": accuracy,
                "precision": precision,
                "recall": recall,
                "win_rate": win_rate,
                "profit_factor": profit_factor,
                "max_drawdown": max_drawdown,
                "approved_for_paper": False,
                "approved_for_live": False,
                "notes": "Entrenamiento con outcomes reales (result_15m fallback result_30m)",
            }
        )
        self.registry.prune_old_versions(keep_last=5)
        return {
            "trained": True,
            "model_version": version,
            "label_type": label_type,
            "dataset_start": dataset_start,
            "dataset_end": dataset_end,
            "accuracy": accuracy,
            "precision": precision,
            "recall": recall,
            "win_rate": win_rate,
            "profit_factor": profit_factor,
            "max_drawdown": max_drawdown,
            "number_of_samples": len(samples),
            "trained_with_outcomes_count": len(samples),
        }
