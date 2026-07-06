from __future__ import annotations

from concurrent.futures import Future
import queue
import threading
import time
from typing import Any, Callable

from runtime.thread_manager import ThreadManager


class DatabaseWriterWorker:
    def __init__(
        self,
        *,
        logger: Any,
        thread_manager: ThreadManager | None = None,
        max_queue_size: int = 5000,
    ) -> None:
        self.logger = logger
        self.thread_manager = thread_manager
        self._queue: queue.Queue[tuple[str, Callable[..., Any], tuple[Any, ...], dict[str, Any], Future[Any]]] = queue.Queue(
            maxsize=max(int(max_queue_size), 100)
        )
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        if self.thread_manager is not None:
            self.thread_manager.register("DatabaseWriterWorker", "database")
        self._thread = threading.Thread(target=self._run, daemon=True, name="DatabaseWriterWorker")
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        self._thread = None
        if self.thread_manager is not None:
            self.thread_manager.set_stopped("DatabaseWriterWorker")

    def queue_size(self) -> int:
        return int(self._queue.qsize())

    def submit(self, action: str, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        future: Future[Any] = Future()
        item = (action, fn, args, kwargs, future)
        try:
            self._queue.put(item, timeout=2.0)
        except queue.Full as ex:
            if self.thread_manager is not None:
                self.thread_manager.set_error("DatabaseWriterWorker", f"queue full on {action}")
            raise RuntimeError("Database writer queue is full") from ex
        return future.result(timeout=30.0)

    def wrap_method(self, action: str, fn: Callable[..., Any]) -> Callable[..., Any]:
        def _wrapped(*args: Any, **kwargs: Any) -> Any:
            return self.submit(action, fn, *args, **kwargs)

        return _wrapped

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                action, fn, args, kwargs, future = self._queue.get(timeout=0.5)
            except queue.Empty:
                if self.thread_manager is not None:
                    self.thread_manager.heartbeat("DatabaseWriterWorker")
                continue

            try:
                result = fn(*args, **kwargs)
                if not future.done():
                    future.set_result(result)
                if self.thread_manager is not None:
                    self.thread_manager.heartbeat("DatabaseWriterWorker")
            except Exception as ex:
                if not future.done():
                    future.set_exception(ex)
                if self.thread_manager is not None:
                    self.thread_manager.set_error("DatabaseWriterWorker", f"{action}: {ex}")
                try:
                    self.logger.warning("DatabaseWriterWorker error in %s: %s", action, ex)
                except Exception:
                    pass
            finally:
                self._queue.task_done()

        if self.thread_manager is not None:
            self.thread_manager.set_stopped("DatabaseWriterWorker")
