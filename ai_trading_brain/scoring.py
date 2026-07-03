from __future__ import annotations

from typing import Any


def compute_composite_score(features: dict[str, Any]) -> dict[str, Any]:
    price_action = max(0.0, min(40.0, 20.0 + float(features.get("percent_change_5m", 0.0)) * 4.0 + float(features.get("price_above_vwap", 0.0)) * 8.0))
    volume_score = max(0.0, min(25.0, float(features.get("volume_spike_score", 0.0)) * 25.0))
    sentiment_raw = float(features.get("news_sentiment_score", 0.0))
    importance_raw = float(features.get("news_importance_score", 0.0))
    news_score = max(0.0, min(15.0, ((sentiment_raw + 1.0) / 2.0) * 10.0 + importance_raw * 5.0))
    liquidity = max(0.0, min(10.0, float(features.get("liquidity_score", 0.0)) * 10.0))
    distance = float(features.get("distance_from_average_cost", 0.0))
    risk_penalty = max(0.0, min(10.0, (1.0 - min(max(distance, 0.0), 1.0)) * 4.0 + float(features.get("news_risk_score", 0.0)) * 6.0))
    risk_score = max(0.0, 10.0 - risk_penalty)

    score = round(price_action + volume_score + news_score + liquidity + risk_score, 2)
    if score < 60.0:
        action = "AVOID"
    elif score < 75.0:
        action = "WATCH"
    elif score < 85.0:
        action = "BUY_SMALL"
    else:
        action = "BUY"

    return {
        "score": score,
        "action": action,
        "components": {
            "price_action": round(price_action, 2),
            "volume": round(volume_score, 2),
            "news": round(news_score, 2),
            "liquidity": round(liquidity, 2),
            "risk": round(risk_score, 2),
        },
    }
