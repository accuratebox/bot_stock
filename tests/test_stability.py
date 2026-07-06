from __future__ import annotations

import time

from runtime.thread_manager import ThreadManager


def test_thread_manager_register_and_heartbeat() -> None:
    manager = ThreadManager()
    manager.register("DataCollectorWorker", "collector")
    time.sleep(0.01)
    manager.heartbeat("DataCollectorWorker")

    summary = manager.summary()
    rows = {row["name"]: row for row in summary["threads"]}
    assert "DataCollectorWorker" in rows
    assert rows["DataCollectorWorker"]["state"] == "running"
    assert summary["active_threads"] >= 1


def test_thread_manager_error_and_stop() -> None:
    manager = ThreadManager()
    manager.register("WebSocketWatchdog", "websocket")
    manager.set_error("WebSocketWatchdog", "simulated failure")
    manager.set_stopped("WebSocketWatchdog")

    summary = manager.summary()
    rows = {row["name"]: row for row in summary["threads"]}
    assert rows["WebSocketWatchdog"]["state"] == "stopped"
