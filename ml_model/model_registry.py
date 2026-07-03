from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import joblib


class ModelRegistry:
    def __init__(self, models_dir: str) -> None:
        self.path = Path(models_dir)
        self.path.mkdir(parents=True, exist_ok=True)
        self.meta_path = self.path / "registry.json"
        if not self.meta_path.exists():
            self.meta_path.write_text(json.dumps({"approved": "", "latest": ""}, indent=2), encoding="utf-8")

    def save_model(self, model: Any, version: str, metadata: dict[str, Any]) -> Path:
        model_path = self.path / f"{version}.pkl"
        joblib.dump({"model": model, "metadata": metadata}, model_path)
        state = self._load_state()
        state["latest"] = version
        self._save_state(state)
        return model_path

    def approve_model(self, version: str) -> None:
        state = self._load_state()
        state["approved"] = version
        self._save_state(state)

    def rollback_to_previous(self) -> str | None:
        versions = self.available_versions()
        if len(versions) < 2:
            return None
        previous = versions[1]
        self.approve_model(previous)
        return previous

    def available_versions(self) -> list[str]:
        versions = [path.stem for path in self.path.glob("*.pkl")]
        versions.sort(reverse=True)
        return versions

    def load_approved_bundle(self) -> dict[str, Any] | None:
        state = self._load_state()
        version = state.get("approved") or state.get("latest")
        if not version:
            return None
        model_path = self.path / f"{version}.pkl"
        if not model_path.exists():
            return None
        return joblib.load(model_path)

    def latest_version(self) -> str:
        return str(self._load_state().get("latest", ""))

    def approved_version(self) -> str:
        return str(self._load_state().get("approved", ""))

    def prune_old_versions(self, keep_last: int) -> None:
        versions = self.available_versions()
        for version in versions[keep_last:]:
            model_path = self.path / f"{version}.pkl"
            if model_path.exists():
                model_path.unlink()

    def _load_state(self) -> dict[str, Any]:
        try:
            return json.loads(self.meta_path.read_text(encoding="utf-8"))
        except Exception:
            return {"approved": "", "latest": ""}

    def _save_state(self, state: dict[str, Any]) -> None:
        self.meta_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
