from __future__ import annotations

from dataclasses import dataclass
import threading
import time
from typing import Any


@dataclass
class ThreadStatus:
    name: str
    role: str
    state: str
    started_at: float
    last_heartbeat: float
    last_error: str


class ThreadManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._threads: dict[str, ThreadStatus] = {}

    def register(self, name: str, role: str) -> None:
        now = time.time()
        with self._lock:
            existing = self._threads.get(name)
            if existing is None:
                self._threads[name] = ThreadStatus(
                    name=name,
                    role=role,
                    state="running",
                    started_at=now,
                    last_heartbeat=now,
                    last_error="",
                )
                return
            existing.role = role
            existing.state = "running"
            existing.last_heartbeat = now
            if existing.started_at <= 0.0:
                existing.started_at = now

    def heartbeat(self, name: str) -> None:
        now = time.time()
        with self._lock:
            row = self._threads.get(name)
            if row is None:
                self._threads[name] = ThreadStatus(
                    name=name,
                    role="worker",
                    state="running",
                    started_at=now,
                    last_heartbeat=now,
                    last_error="",
                )
                return
            row.last_heartbeat = now
            if row.state != "error":
                row.state = "running"

    def set_error(self, name: str, error: str) -> None:
        with self._lock:
            row = self._threads.get(name)
            if row is None:
                now = time.time()
                self._threads[name] = ThreadStatus(
                    name=name,
                    role="worker",
                    state="error",
                    started_at=now,
                    last_heartbeat=now,
                    last_error=str(error),
                )
                return
            row.state = "error"
            row.last_error = str(error)
            row.last_heartbeat = time.time()

    def set_stopped(self, name: str) -> None:
        with self._lock:
            row = self._threads.get(name)
            if row is None:
                return
            row.state = "stopped"
            row.last_heartbeat = time.time()

    def summary(self) -> dict[str, Any]:
        with self._lock:
            rows = [
                {
                    "name": status.name,
                    "role": status.role,
                    "state": status.state,
                    "started_at": status.started_at,
                    "last_heartbeat": status.last_heartbeat,
                    "last_error": status.last_error,
                }
                for status in self._threads.values()
            ]
        rows.sort(key=lambda item: (item["role"], item["name"]))
        now = time.time()
        running = sum(1 for row in rows if row.get("state") == "running")
        errors = sum(1 for row in rows if row.get("state") == "error")
        stale = sum(
            1
            for row in rows
            if row.get("state") == "running" and (now - float(row.get("last_heartbeat", 0.0) or 0.0)) > 60.0
        )
        return {
            "active_threads": running,
            "error_threads": errors,
            "stale_threads": stale,
            "threads": rows,
        }
