from __future__ import annotations

from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
import os
import queue
import threading
import time
import uuid
from typing import Any, Callable


@dataclass
class TaskEvent:
    task_id: str
    task_name: str
    status: str
    started_at: float
    finished_at: float
    duration_seconds: float
    result: Any = None
    error: str = ""
    task_type: str = "IO"
    metadata: dict[str, Any] | None = None


@dataclass
class TaskRecord:
    future: Future[Any]
    metadata: dict[str, Any]
    cancel_requested: bool = False
    cancel_flag_path: str = ""


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class TaskManager:
    """Centralized scheduler with dedicated queues/channels and task metadata."""

    VALID_TASK_TYPES = {"IO", "CPU", "DB", "UI"}
    VALID_PRIORITIES = {"LOW", "NORMAL", "HIGH", "CRITICAL"}
    STATUS_PENDING = "PENDING"
    STATUS_RUNNING = "RUNNING"
    STATUS_DONE = "DONE"
    STATUS_FAILED = "FAILED"
    STATUS_CANCELLED = "CANCELLED"

    def __init__(
        self,
        *,
        logger: Any,
        event_queue: queue.Queue[TaskEvent],
        max_io_workers: int = 8,
        max_cpu_workers: int | None = None,
    ) -> None:
        self.logger = logger
        self.event_queue = event_queue
        self._io_executor = ThreadPoolExecutor(max_workers=max(int(max_io_workers), 2), thread_name_prefix="IOTask")
        cpu_workers = max_cpu_workers if max_cpu_workers is not None else max((os.cpu_count() or 2) - 1, 1)
        self._cpu_executor = ProcessPoolExecutor(max_workers=max(int(cpu_workers), 1))
        self._db_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="DBTask")
        self._lock = threading.Lock()
        self._tasks: dict[str, TaskRecord] = {}
        self.io_queue: queue.Queue[str] = queue.Queue(maxsize=2000)
        self.cpu_queue: queue.Queue[str] = queue.Queue(maxsize=500)
        self.db_queue: queue.Queue[str] = queue.Queue(maxsize=5000)
        self.ui_queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=5000)

    def _normalize_task_type(self, task_type: str) -> str:
        normalized = str(task_type or "IO").strip().upper()
        return normalized if normalized in self.VALID_TASK_TYPES else "IO"

    def _normalize_priority(self, priority: str) -> str:
        normalized = str(priority or "NORMAL").strip().upper()
        return normalized if normalized in self.VALID_PRIORITIES else "NORMAL"

    def _queue_for_type(self, task_type: str) -> queue.Queue[str]:
        if task_type == "CPU":
            return self.cpu_queue
        if task_type == "DB":
            return self.db_queue
        if task_type == "UI":
            # UI work is processed by main thread loop, but we still track queue pressure.
            return self.io_queue
        return self.io_queue

    def _executor_for_type(self, task_type: str) -> Any:
        if task_type == "CPU":
            return self._cpu_executor
        if task_type == "DB":
            return self._db_executor
        return self._io_executor

    def _new_task_id(self) -> str:
        return uuid.uuid4().hex

    def submit(
        self,
        task_name: str,
        fn: Callable[..., Any],
        *args: Any,
        task_type: str = "IO",
        priority: str = "NORMAL",
        cancelable: bool = False,
        timeout_seconds: float = 30.0,
        cancel_flag_path: str = "",
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> str:
        normalized_type = self._normalize_task_type(task_type)
        normalized_priority = self._normalize_priority(priority)
        task_id = self._new_task_id()
        created_at = _iso_now()
        safe_timeout = max(float(timeout_seconds or 0.0), 0.0)

        task_meta: dict[str, Any] = {
            "task_id": task_id,
            "task_name": str(task_name or "task"),
            "task_type": normalized_type,
            "queue_name": f"{normalized_type.lower()}_queue",
            "priority": normalized_priority,
            "status": self.STATUS_PENDING,
            "created_at": created_at,
            "started_at": "",
            "finished_at": "",
            "last_error": "",
            "cancelable": bool(cancelable),
            "cancel_requested": False,
            "timeout_seconds": safe_timeout,
        }
        if isinstance(metadata, dict):
            task_meta.update(metadata)
            task_meta["task_id"] = task_id
            task_meta["task_name"] = str(task_name or task_meta.get("task_name", "task"))
            task_meta["task_type"] = normalized_type
            task_meta["queue_name"] = f"{normalized_type.lower()}_queue"
            task_meta["priority"] = normalized_priority
            task_meta["status"] = self.STATUS_PENDING
            task_meta["created_at"] = created_at
            task_meta["cancelable"] = bool(cancelable)
            task_meta["timeout_seconds"] = safe_timeout

        execution_queue = self._queue_for_type(normalized_type)
        try:
            execution_queue.put_nowait(task_id)
        except queue.Full as ex:
            raise RuntimeError(f"{normalized_type} queue is full") from ex

        def _wrapped() -> Any:
            with self._lock:
                record = self._tasks.get(task_id)
                if record is not None:
                    record.metadata["status"] = self.STATUS_RUNNING
                    record.metadata["started_at"] = _iso_now()
            return fn(*args, **kwargs)

        executor = self._executor_for_type(normalized_type)
        future = executor.submit(_wrapped)
        with self._lock:
            self._tasks[task_id] = TaskRecord(
                future=future,
                metadata=task_meta,
                cancel_requested=False,
                cancel_flag_path=str(cancel_flag_path or ""),
            )

        if safe_timeout > 0.0:
            threading.Thread(
                target=self._watch_timeout,
                args=(task_id, safe_timeout),
                daemon=True,
                name=f"TaskTimeout-{task_id[:8]}",
            ).start()

        def _on_done(done_future: Future[Any]) -> None:
            finished_at = time.time()
            result: Any = None
            error = ""
            event_status = self.STATUS_DONE
            snapshot_meta: dict[str, Any] = {}
            try:
                result = done_future.result()
            except Exception as ex:  # noqa: BLE001
                error = str(ex)
                event_status = self.STATUS_FAILED

            with self._lock:
                record = self._tasks.get(task_id)
                if record is not None:
                    snapshot_meta = dict(record.metadata)
                    if record.cancel_requested and bool(record.metadata.get("cancelable", False)):
                        event_status = self.STATUS_CANCELLED
                        if not error:
                            error = "cancel_requested"
                    snapshot_meta["status"] = event_status
                    snapshot_meta["finished_at"] = _iso_now()
                    snapshot_meta["last_error"] = error
                    record.metadata.update(snapshot_meta)

            event = TaskEvent(
                task_id=task_id,
                task_name=task_name,
                status=event_status,
                started_at=finished_at,
                finished_at=finished_at,
                duration_seconds=0.0,
                result=result,
                error=error,
                task_type=normalized_type,
                metadata=snapshot_meta,
            )
            started_raw = snapshot_meta.get("started_at", "") or snapshot_meta.get("created_at", "")
            if isinstance(started_raw, str) and started_raw:
                try:
                    started_epoch = datetime.fromisoformat(started_raw).timestamp()
                    event.started_at = started_epoch
                    event.duration_seconds = max(finished_at - started_epoch, 0.0)
                except Exception:
                    event.duration_seconds = 0.0

            try:
                if normalized_type == "CPU":
                    self.cpu_queue.get_nowait()
                elif normalized_type == "DB":
                    self.db_queue.get_nowait()
                else:
                    self.io_queue.get_nowait()
            except Exception:
                pass

            try:
                self.event_queue.put_nowait(event)
            except Exception:
                try:
                    self.logger.warning("TaskManager could not enqueue task event: %s", task_id)
                except Exception:
                    pass
            with self._lock:
                self._tasks.pop(task_id, None)

        future.add_done_callback(_on_done)
        return task_id

    def push_ui_task(self, payload: dict[str, Any]) -> bool:
        try:
            self.ui_queue.put_nowait(dict(payload))
            return True
        except queue.Full:
            return False

    def pop_ui_task(self) -> dict[str, Any] | None:
        try:
            return self.ui_queue.get_nowait()
        except queue.Empty:
            return None

    def _watch_timeout(self, task_id: str, timeout_seconds: float) -> None:
        deadline = time.monotonic() + max(float(timeout_seconds), 0.0)
        while time.monotonic() < deadline:
            with self._lock:
                record = self._tasks.get(task_id)
                if record is None:
                    return
                if record.future.done():
                    return
            time.sleep(0.25)
        self.cancel_task(task_id, reason="timeout")

    def cancel_task(self, task_id: str, reason: str = "cancel_requested") -> bool:
        with self._lock:
            record = self._tasks.get(task_id)
            if record is None:
                return False
            if not bool(record.metadata.get("cancelable", False)):
                return False
            record.cancel_requested = True
            record.metadata["cancel_requested"] = True
            record.metadata["last_error"] = str(reason or "cancel_requested")

            if record.cancel_flag_path:
                try:
                    with open(record.cancel_flag_path, "w", encoding="utf-8") as handle:
                        handle.write(_iso_now())
                except Exception:
                    pass

            cancelled = record.future.cancel()
            if cancelled:
                record.metadata["status"] = self.STATUS_CANCELLED
                record.metadata["finished_at"] = _iso_now()
            return True

    def list_active_tasks(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = [dict(record.metadata) for record in self._tasks.values()]
        rows.sort(key=lambda row: str(row.get("created_at", "")))
        return rows

    def queue_sizes(self) -> dict[str, int]:
        return {
            "task_queue": self.pending_tasks(),
            "io_queue": int(self.io_queue.qsize()),
            "cpu_queue": int(self.cpu_queue.qsize()),
            "db_queue": int(self.db_queue.qsize()),
            "ui_queue": int(self.ui_queue.qsize()),
        }

    def cpu_task_running(self) -> bool:
        with self._lock:
            for record in self._tasks.values():
                if str(record.metadata.get("task_type", "")).upper() != "CPU":
                    continue
                if str(record.metadata.get("status", "")).upper() == self.STATUS_RUNNING:
                    return True
        return False

    def pending_tasks(self) -> int:
        with self._lock:
            return len(self._tasks)

    def shutdown(self) -> None:
        self._io_executor.shutdown(wait=False, cancel_futures=True)
        self._db_executor.shutdown(wait=False, cancel_futures=True)
        self._cpu_executor.shutdown(wait=False, cancel_futures=True)
