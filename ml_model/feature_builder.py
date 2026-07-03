from __future__ import annotations

from typing import Any

FEATURE_ORDER = [
    "asset_type",
    "price",
    "volume",
    "vwap",
    "rsi",
    "atr",
    "spread",
    "percent_change_1m",
    "percent_change_5m",
    "percent_change_15m",
    "price_above_vwap",
    "volume_spike_score",
    "news_sentiment_score",
    "news_importance_score",
    "news_risk_score",
    "market_session",
    "existing_position",
    "distance_from_average_cost",
    "liquidity_score",
]

ASSET_TYPE_MAP = {"stock": 0.0, "crypto": 1.0}
MARKET_SESSION_MAP = {"pre": 0.0, "regular": 1.0, "post": 2.0, "overnight": 3.0, "crypto": 4.0}


def build_feature_vector(features: dict[str, Any]) -> list[float]:
    vector: list[float] = []
    for key in FEATURE_ORDER:
        value = features.get(key, 0.0)
        if key == "asset_type":
            vector.append(float(ASSET_TYPE_MAP.get(str(value).lower(), 0.0)))
            continue
        if key == "market_session":
            vector.append(float(MARKET_SESSION_MAP.get(str(value).lower(), 0.0)))
            continue
        if isinstance(value, bool):
            vector.append(1.0 if value else 0.0)
            continue
        try:
            vector.append(float(value))
        except (TypeError, ValueError):
            vector.append(0.0)
    return vector
