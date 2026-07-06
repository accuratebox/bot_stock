from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import joblib


@dataclass(frozen=True)
class FuturesPredictorConfig:
    model_path: str
    min_confidence: float = 0.55
    max_hold_minutes: int = 15


class FuturesPredictor:
    ACTIONS = ("LONG", "SHORT", "NO_TRADE")

    def __init__(self, config: FuturesPredictorConfig) -> None:
        self.config = config
        self._bundle = joblib.load(Path(config.model_path))
        self._model = self._bundle.get("model")
        self._feature_names = list(self._bundle.get("feature_names", []))
        self._metadata = dict(self._bundle.get("metadata", {}))

    def _vector(self, row: dict[str, Any]) -> list[float]:
        return [float(row.get(name, 0.0) or 0.0) for name in self._feature_names]

    @staticmethod
    def _safe_price(value: Any) -> float:
        return max(float(value or 0.0), 0.0)

    def predict(self, row: dict[str, Any]) -> dict[str, Any]:
        vector = self._vector(row)
        classes = list(getattr(self._model, "classes_", [0, 1, 2]))
        class_to_action = {0: "LONG", 1: "SHORT", 2: "NO_TRADE"}

        prob_long = 0.0
        prob_short = 0.0
        prob_no_trade = 1.0

        if hasattr(self._model, "predict_proba"):
            proba = list(self._model.predict_proba([vector])[0])
            for idx, class_id in enumerate(classes):
                action = class_to_action.get(int(class_id), "NO_TRADE")
                if action == "LONG":
                    prob_long = float(proba[idx])
                elif action == "SHORT":
                    prob_short = float(proba[idx])
                else:
                    prob_no_trade = float(proba[idx])
        else:
            pred = int(self._model.predict([vector])[0])
            action = class_to_action.get(pred, "NO_TRADE")
            if action == "LONG":
                prob_long = 1.0
                prob_short = 0.0
                prob_no_trade = 0.0
            elif action == "SHORT":
                prob_long = 0.0
                prob_short = 1.0
                prob_no_trade = 0.0

        confidence = max(prob_long, prob_short, prob_no_trade)
        action = "NO_TRADE"
        if confidence >= float(self.config.min_confidence):
            if prob_long > prob_short and prob_long > prob_no_trade:
                action = "LONG"
            elif prob_short > prob_long and prob_short > prob_no_trade:
                action = "SHORT"

        ev_long = float(row.get("EV_LONG", 0.0) or 0.0)
        ev_short = float(row.get("EV_SHORT", 0.0) or 0.0)
        expected_value = 0.0
        if action == "LONG":
            expected_value = ev_long
        elif action == "SHORT":
            expected_value = ev_short

        entry = self._safe_price(row.get("close", 0.0))
        atr = max(float(row.get("ATR", 0.0) or 0.0), entry * 0.001)
        spread = max(float(row.get("spread", 0.0) or 0.0), 0.0)

        suggested_sl = 0.0
        suggested_tp = 0.0
        reason = "No clear edge"
        if action == "LONG":
            suggested_sl = max(entry - max(atr * 1.1, entry * 0.0025), 0.0)
            suggested_tp = entry + max(atr * 1.4, entry * 0.0040) + spread
            reason = "LONG edge from model probabilities and EV"
        elif action == "SHORT":
            suggested_sl = entry + max(atr * 1.1, entry * 0.0025)
            suggested_tp = max(entry - (max(atr * 1.4, entry * 0.0040) + spread), 0.0)
            reason = "SHORT edge from model probabilities and EV"

        risk_score = max(0.0, min(1.0, 1.0 - confidence + float(row.get("event_risk_score", 0.0) or 0.0) * 0.5))

        return {
            "symbol": str(row.get("symbol", "BTCUSDT") or "BTCUSDT"),
            "action": action,
            "prob_long_win": prob_long,
            "prob_short_win": prob_short,
            "confidence": confidence,
            "expected_value": expected_value,
            "risk_score": risk_score,
            "suggested_entry": entry,
            "suggested_stop_loss": suggested_sl,
            "suggested_take_profit": suggested_tp,
            "max_hold_minutes": int(self.config.max_hold_minutes),
            "reason": reason,
            "prob_no_trade": prob_no_trade,
            "model_name": str(self._metadata.get("model_name", "futures_model")),
        }
