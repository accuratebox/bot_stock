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
            self.meta_path.write_text(
                json.dumps({"approved": "", "latest": "", "frozen_candidate": "", "aliases": {}}, indent=2),
                encoding="utf-8",
            )

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
        if str(state.get("frozen_candidate", "")) == str(version):
            state["frozen_candidate"] = ""
        self._save_state(state)

    def freeze_candidate(self, version: str) -> None:
        if version not in self.available_versions():
            raise ValueError(f"No existe modelo para congelar: {version}")
        state = self._load_state()
        state["frozen_candidate"] = version
        self._save_state(state)

    def clear_frozen_candidate(self) -> None:
        state = self._load_state()
        state["frozen_candidate"] = ""
        self._save_state(state)

    def set_alias(self, version: str, alias: str) -> None:
        version_text = str(version or "").strip()
        if not version_text:
            raise ValueError("Version invalida")
        if version_text not in self.available_versions():
            raise ValueError(f"Modelo no encontrado: {version_text}")
        state = self._load_state()
        aliases = state.setdefault("aliases", {})
        alias_text = str(alias or "").strip()
        if alias_text:
            aliases[version_text] = alias_text
        else:
            aliases.pop(version_text, None)
        self._save_state(state)

    def get_alias(self, version: str) -> str:
        state = self._load_state()
        aliases = state.get("aliases", {}) if isinstance(state.get("aliases", {}), dict) else {}
        return str(aliases.get(str(version or "").strip(), "") or "")

    def delete_version(self, version: str) -> None:
        version_text = str(version or "").strip()
        if not version_text:
            raise ValueError("Version invalida")
        model_path = self.path / f"{version_text}.pkl"
        if not model_path.exists():
            raise ValueError(f"Modelo no encontrado: {version_text}")
        model_path.unlink()

        state = self._load_state()
        if str(state.get("approved", "")) == version_text:
            state["approved"] = ""
        if str(state.get("latest", "")) == version_text:
            remaining = self.available_versions()
            state["latest"] = remaining[0] if remaining else ""
        if str(state.get("frozen_candidate", "")) == version_text:
            state["frozen_candidate"] = ""
        aliases = state.get("aliases", {}) if isinstance(state.get("aliases", {}), dict) else {}
        aliases.pop(version_text, None)
        state["aliases"] = aliases
        self._save_state(state)

    def frozen_candidate(self) -> str:
        state = self._load_state()
        frozen = str(state.get("frozen_candidate", "") or "")
        if not frozen:
            return ""
        model_path = self.path / f"{frozen}.pkl"
        if not model_path.exists():
            return ""
        return frozen

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
        version = state.get("approved")
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
            payload = json.loads(self.meta_path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                return {"approved": "", "latest": "", "frozen_candidate": "", "aliases": {}}
            payload.setdefault("approved", "")
            payload.setdefault("latest", "")
            payload.setdefault("frozen_candidate", "")
            payload.setdefault("aliases", {})
            if not isinstance(payload.get("aliases", {}), dict):
                payload["aliases"] = {}
            return payload
        except Exception:
            return {"approved": "", "latest": "", "frozen_candidate": "", "aliases": {}}

    def _save_state(self, state: dict[str, Any]) -> None:
        self.meta_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
