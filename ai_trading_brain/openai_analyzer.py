from __future__ import annotations

import json
from typing import Any

import requests


class OpenAIAnalyzer:
    def __init__(self, api_key: str, logger: Any, model: str = "gpt-4.1-mini") -> None:
        self.api_key = api_key.strip()
        self.logger = logger
        self.model = model

    def analyze_text(
        self,
        symbol: str,
        asset_type: str,
        text: str,
        source: str,
        author: str = "",
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        neutral = self._neutral_response(symbol=symbol, asset_type=asset_type)
        cleaned = str(text or "").strip()
        if not cleaned:
            return neutral
        if not self.api_key:
            return neutral

        prompt = {
            "symbol": symbol,
            "asset_type": asset_type,
            "source": source,
            "author": author,
            "context": context or {},
            "text": cleaned,
            "required_schema": neutral,
        }
        try:
            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": self.model,
                    "temperature": 0.1,
                    "messages": [
                        {
                            "role": "system",
                            "content": (
                                "Devuelve solo JSON valido. No inventes hechos. Si el texto no es concluyente, usa neutral. "
                                "Si parece rumor o pump, sube risk_score."
                            ),
                        },
                        {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
                    ],
                    "response_format": {"type": "json_object"},
                },
                timeout=45,
            )
            response.raise_for_status()
            payload = response.json()
            content = payload["choices"][0]["message"]["content"]
            return self._normalize_response(content=content, symbol=symbol, asset_type=asset_type)
        except Exception as ex:
            if self.logger is not None:
                self.logger.warning("OpenAI analyzer fallback neutral: %s", ex)
            return neutral

    def _normalize_response(self, content: str, symbol: str, asset_type: str) -> dict[str, Any]:
        neutral = self._neutral_response(symbol=symbol, asset_type=asset_type)
        try:
            data = json.loads(content)
        except Exception:
            return neutral

        return {
            "symbol": str(data.get("symbol", symbol)),
            "asset_type": str(data.get("asset_type", asset_type)),
            "sentiment": str(data.get("sentiment", "neutral")),
            "event_type": str(data.get("event_type", "other")),
            "importance_score": int(max(0, min(100, int(data.get("importance_score", 0) or 0)))),
            "risk_score": int(max(0, min(100, int(data.get("risk_score", 0) or 0)))),
            "summary": str(data.get("summary", "")),
            "possible_market_impact": str(data.get("possible_market_impact", "")),
            "action_bias": str(data.get("action_bias", "neutral")),
        }

    @staticmethod
    def _neutral_response(symbol: str, asset_type: str) -> dict[str, Any]:
        return {
            "symbol": symbol,
            "asset_type": asset_type,
            "sentiment": "neutral",
            "event_type": "other",
            "importance_score": 0,
            "risk_score": 0,
            "summary": "",
            "possible_market_impact": "No clear impact detected",
            "action_bias": "neutral",
        }
