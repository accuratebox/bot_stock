from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import joblib


class ModelRegistry:
    def __init__(self, models_dir: str) -> None:
        self.path = Path(models_dir)
        self.path.mkdir(parents=True, exist_ok=True)
        self.candidates_path = self.path / "candidates"
        self.candidates_path.mkdir(parents=True, exist_ok=True)
        self.approved_path = self.path / "approved"
        self.approved_path.mkdir(parents=True, exist_ok=True)
        self.current_model_path = self.approved_path / "current_model.pkl"
        self.current_model_meta_path = self.approved_path / "current_model.json"
        self.meta_path = self.path / "registry.json"
        if not self.meta_path.exists():
            self.meta_path.write_text(
                json.dumps({"approved": "", "latest": "", "frozen_candidate": "", "aliases": {}}, indent=2),
                encoding="utf-8",
            )
        self._ensure_legacy_approved_bundle()

    def save_model(self, model: Any, version: str, metadata: dict[str, Any]) -> Path:
        model_path = self.candidates_path / f"{version}.pkl"
        joblib.dump({"model": model, "metadata": metadata}, model_path)
        state = self._load_state()
        state["latest"] = version
        self._save_state(state)
        return model_path

    def approve_model(self, version: str) -> None:
        version_text = str(version or "").strip()
        if not version_text:
            raise ValueError("Version invalida")
        source_path = self.model_path_for_version(version_text)
        if source_path is None or not source_path.exists():
            raise ValueError(f"Modelo no encontrado: {version_text}")
        bundle = joblib.load(source_path)
        joblib.dump(bundle, self.current_model_path)
        metadata = bundle.get("metadata", {}) if isinstance(bundle, dict) else {}
        self.current_model_meta_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
        state = self._load_state()
        state["approved"] = version_text
        if str(state.get("frozen_candidate", "")) == version_text:
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
        model_path = self.model_path_for_version(version_text)
        if model_path is None or not model_path.exists():
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
        model_path = self.model_path_for_version(frozen)
        if model_path is None or not model_path.exists():
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
        versions = {path.stem for path in self.candidates_path.glob("*.pkl")}
        versions.update(path.stem for path in self.path.glob("*.pkl"))
        return sorted(versions, reverse=True)

    def load_approved_bundle(self) -> dict[str, Any] | None:
        self._ensure_legacy_approved_bundle()
        if not self.current_model_path.exists():
            return None
        return joblib.load(self.current_model_path)

    def latest_version(self) -> str:
        return str(self._load_state().get("latest", ""))

    def approved_version(self) -> str:
        return str(self._load_state().get("approved", ""))

    def approved_model_available(self) -> bool:
        self._ensure_legacy_approved_bundle()
        return self.current_model_path.exists()

    def model_path_for_version(self, version: str) -> Path | None:
        version_text = str(version or "").strip()
        if not version_text:
            return None
        candidate_path = self.candidates_path / f"{version_text}.pkl"
        if candidate_path.exists():
            return candidate_path
        legacy_path = self.path / f"{version_text}.pkl"
        if legacy_path.exists():
            return legacy_path
        return None

    def prune_old_versions(self, keep_last: int) -> None:
        versions = self.available_versions()
        state = self._load_state()
        protected_versions = {
            str(state.get("approved", "") or ""),
            str(state.get("latest", "") or ""),
            str(state.get("frozen_candidate", "") or ""),
        }
        protected_versions.discard("")
        kept_unprotected = 0
        for version in versions:
            if version in protected_versions:
                continue
            kept_unprotected += 1
            if kept_unprotected <= int(keep_last):
                continue
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

    def _ensure_legacy_approved_bundle(self) -> None:
        if self.current_model_path.exists():
            return
        approved_version = self.approved_version()
        if not approved_version:
            return
        source_path = self.model_path_for_version(approved_version)
        if source_path is None or not source_path.exists():
            return
        bundle = joblib.load(source_path)
        joblib.dump(bundle, self.current_model_path)
        metadata = bundle.get("metadata", {}) if isinstance(bundle, dict) else {}
        self.current_model_meta_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
