from __future__ import annotations

from typing import Any

from ml_model.feature_builder import build_feature_vector
from ml_model.model_registry import ModelRegistry

VALID_ACTIONS = {"AVOID", "WATCH", "BUY_SMALL", "BUY", "HOLD", "SELL_ALLOWED"}


class SignalPredictor:
    def __init__(self, registry: ModelRegistry, logger: Any) -> None:
        self.registry = registry
        self.logger = logger

    def predict_signal(self, features: dict[str, Any]) -> dict[str, Any]:
        bundle = self.registry.load_approved_bundle()
        if bundle is None:
            return self._heuristic_prediction(features)

        model = bundle.get("model")
        metadata = bundle.get("metadata", {})
        vector = build_feature_vector(features)
        probability = 0.5
        if hasattr(model, "predict_proba"):
            probabilities = model.predict_proba([vector])[0]
            probability = float(probabilities[-1])
        elif hasattr(model, "predict"):
            probability = float(model.predict([vector])[0])

        action = self._action_from_probability(probability)
        risk_score = max(0.0, min(100.0, float(features.get("news_risk_score", 0.0)) * 20.0 + (1.0 - probability) * 50.0))
        return {
            "action": action,
            "confidence_score": round(probability * 100.0, 2),
            "probability_win": round(probability, 4),
            "risk_score": round(risk_score, 2),
            "reason": f"Model {metadata.get('model_version', 'general_model')} probability={probability:.2f}",
            "model_version": str(metadata.get("model_version", "general_model_heuristic")),
        }

    def _heuristic_prediction(self, features: dict[str, Any]) -> dict[str, Any]:
        score = float(features.get("composite_score", 0.0))
        if score < 60:
            action = "AVOID"
            probability = 0.35
        elif score < 75:
            action = "WATCH"
            probability = 0.55
        elif score < 85:
            action = "BUY_SMALL"
            probability = 0.72
        else:
            action = "BUY"
            probability = 0.88
        if float(features.get("distance_from_average_cost", 0.0)) < 0:
            action = "HOLD"
        return {
            "action": action,
            "confidence_score": round(probability * 100.0, 2),
            "probability_win": round(probability, 4),
            "risk_score": round(max(0.0, 100.0 - score), 2),
            "reason": f"Heuristic composite score {score:.2f}",
            "model_version": "general_model_heuristic",
        }

    @staticmethod
    def _action_from_probability(probability: float) -> str:
        if probability < 0.60:
            return "AVOID"
        if probability < 0.75:
            return "WATCH"
        if probability < 0.85:
            return "BUY_SMALL"
        return "BUY"
