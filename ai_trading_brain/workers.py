from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class AutoTradingController:
    signal_only_mode: bool = True
    paper_trading: bool = True
    live_trading_enabled: bool = False
    manual_approval_required: bool = True

    def mode_label(self) -> str:
        if self.live_trading_enabled:
            return "Live"
        if self.paper_trading and not self.signal_only_mode:
            return "Paper"
        return "Solo señales"


class _LoopWorker:
    def __init__(
        self,
        name: str,
        loop_fn: Callable[[], None],
        sleep_seconds_fn: Callable[[], float],
        logger: Any,
    ) -> None:
        self.name = name
        self._loop_fn = loop_fn
        self._sleep_seconds_fn = sleep_seconds_fn
        self.logger = logger
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self.running = False
        self.started_at = 0.0
        self.last_attempt_at = 0.0
        self.last_run_at = 0.0
        self.last_error = ""

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self.started_at = time.time()
        self._thread = threading.Thread(target=self._run, daemon=True, name=self.name)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()

    def _run(self) -> None:
        self.running = True
        while not self._stop_event.is_set():
            self.last_attempt_at = time.time()
            try:
                self._loop_fn()
                self.last_run_at = time.time()
                self.last_error = ""
            except Exception as ex:
                self.last_error = str(ex)
                try:
                    self.logger.warning("Worker %s error: %s", self.name, ex)
                except Exception:
                    pass
            sleep_seconds = max(float(self._sleep_seconds_fn()), 1.0)
            self._stop_event.wait(timeout=sleep_seconds)
        self.running = False


class DataCollectorWorker(_LoopWorker):
    pass


class SignalScannerWorker(_LoopWorker):
    pass


class OutcomeLabelerWorker(_LoopWorker):
    pass


class NewsSocialCollectorWorker(_LoopWorker):
    pass


class ModelTrainerWorker(_LoopWorker):
    pass
