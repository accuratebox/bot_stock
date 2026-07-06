from __future__ import annotations

import json
import threading
import time
from typing import Any, Callable

import websocket

from runtime.thread_manager import ThreadManager


SymbolProvider = Callable[[str], dict[str, list[str]]]
EventCallback = Callable[[str, dict[str, Any]], None]


class AlpacaStreamManager:
    def __init__(
        self,
        *,
        alpaca_endpoint: str,
        api_key: str,
        api_secret: str,
        logger: Any,
        symbol_provider: SymbolProvider,
        market_event_callback: EventCallback,
        news_event_callback: EventCallback,
        trade_update_callback: EventCallback,
        paper_trading: bool,
        thread_manager: ThreadManager | None = None,
        stale_seconds: float = 45.0,
        max_backoff_seconds: float = 30.0,
    ) -> None:
        self.alpaca_endpoint = alpaca_endpoint.rstrip("/")
        self.api_key = api_key
        self.api_secret = api_secret
        self.logger = logger
        self.symbol_provider = symbol_provider
        self.market_event_callback = market_event_callback
        self.news_event_callback = news_event_callback
        self.trade_update_callback = trade_update_callback
        self.paper_trading = bool(paper_trading)
        self.thread_manager = thread_manager
        self.stale_seconds = max(float(stale_seconds or 45.0), 20.0)
        self.max_backoff_seconds = max(float(max_backoff_seconds or 30.0), 5.0)

        self._stop_event = threading.Event()
        self._threads: list[threading.Thread] = []
        self._running_lock = threading.Lock()
        self._connected_streams: set[str] = set()
        self._stream_last_message_at: dict[str, float] = {}
        self._stream_reconnect_attempts: dict[str, int] = {}
        self._stream_last_error: dict[str, str] = {}
        self._live_sockets: dict[str, websocket.WebSocketApp] = {}
        self._watchdog_thread: threading.Thread | None = None
        self._running = False

    @property
    def running(self) -> bool:
        with self._running_lock:
            return self._running

    @property
    def connected(self) -> bool:
        with self._running_lock:
            return bool(self._connected_streams)

    def start(self, account_name: str) -> None:
        if self.running:
            return

        self._stop_event.clear()
        threads = [
            threading.Thread(target=self._run_market_stream, args=(account_name, "stocks"), daemon=True, name=f"alpaca-market-stocks-{account_name}"),
            threading.Thread(target=self._run_market_stream, args=(account_name, "crypto"), daemon=True, name=f"alpaca-market-crypto-{account_name}"),
            threading.Thread(target=self._run_news_stream, args=(account_name,), daemon=True, name=f"alpaca-news-{account_name}"),
            threading.Thread(target=self._run_trade_updates_stream, args=(account_name,), daemon=True, name=f"alpaca-trade-updates-{account_name}"),
        ]
        with self._running_lock:
            self._threads = threads
            self._connected_streams.clear()
            self._stream_last_message_at.clear()
            self._stream_reconnect_attempts.clear()
            self._stream_last_error.clear()
            self._live_sockets.clear()
            self._running = True
        for thread in threads:
            thread.start()
        self._watchdog_thread = threading.Thread(target=self._run_watchdog, daemon=True, name=f"alpaca-watchdog-{account_name}")
        self._watchdog_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        with self._running_lock:
            threads = list(self._threads)
            self._threads = []
            self._connected_streams.clear()
            live_sockets = dict(self._live_sockets)
            self._live_sockets.clear()
            self._running = False

        for ws in live_sockets.values():
            try:
                ws.close()
            except Exception:
                pass

        for thread in threads:
            if thread.is_alive():
                thread.join(timeout=2.0)

        if self._watchdog_thread is not None and self._watchdog_thread.is_alive():
            self._watchdog_thread.join(timeout=2.0)
        self._watchdog_thread = None

    def status_snapshot(self) -> dict[str, Any]:
        with self._running_lock:
            now = time.time()
            stream_rows = []
            for stream_name in sorted(set(self._stream_last_message_at.keys()) | set(self._stream_reconnect_attempts.keys())):
                last_message_at = float(self._stream_last_message_at.get(stream_name, 0.0) or 0.0)
                age = (now - last_message_at) if last_message_at > 0 else float("inf")
                stream_rows.append(
                    {
                        "stream": stream_name,
                        "connected": stream_name in self._connected_streams,
                        "last_message_age_seconds": age,
                        "reconnect_attempts": int(self._stream_reconnect_attempts.get(stream_name, 0) or 0),
                        "last_error": str(self._stream_last_error.get(stream_name, "") or ""),
                        "stale": bool(age > self.stale_seconds),
                    }
                )
        return {
            "running": self.running,
            "connected": self.connected,
            "streams": stream_rows,
        }

    def reconnect_now(self) -> None:
        with self._running_lock:
            live_sockets = dict(self._live_sockets)
        for ws in live_sockets.values():
            try:
                ws.close()
            except Exception:
                pass

    def _run_watchdog(self) -> None:
        watchdog_name = "WebSocketWatchdog"
        if self.thread_manager is not None:
            self.thread_manager.register(watchdog_name, "websocket")
        while not self._stop_event.is_set():
            now = time.time()
            with self._running_lock:
                stream_names = list(self._live_sockets.keys())
                stale_streams = [
                    stream_name
                    for stream_name in stream_names
                    if (now - float(self._stream_last_message_at.get(stream_name, 0.0) or 0.0)) > self.stale_seconds
                ]
                sockets = {name: self._live_sockets.get(name) for name in stale_streams}
            for stream_name, ws in sockets.items():
                if ws is None:
                    continue
                try:
                    self.logger.warning("WebSocket stale detectado (%s). Forzando reconexion controlada.", stream_name)
                    ws.close()
                except Exception:
                    pass
            if self.thread_manager is not None:
                self.thread_manager.heartbeat(watchdog_name)
            self._stop_event.wait(timeout=5.0)
        if self.thread_manager is not None:
            self.thread_manager.set_stopped(watchdog_name)

    def _market_stream_url(self, asset_type: str) -> str:
        if asset_type == "crypto":
            return "wss://stream.data.alpaca.markets/v1beta3/crypto/us"
        return "wss://stream.data.alpaca.markets/v2/iex"

    def _trade_updates_url(self) -> str:
        return "wss://paper-api.alpaca.markets/stream" if self.paper_trading else "wss://api.alpaca.markets/stream"

    def _auth_payload(self) -> str:
        return json.dumps(
            {
                "action": "auth",
                "key": self.api_key,
                "secret": self.api_secret,
            }
        )

    @staticmethod
    def _payload_text(message: Any) -> str:
        if isinstance(message, bytes):
            return message.decode("utf-8", errors="ignore")
        return str(message)

    def _parse_message(self, message: Any) -> list[dict[str, Any]]:
        raw = self._payload_text(message).strip()
        if not raw:
            return []
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return []
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            return [payload]
        return []

    def _subscribe_market(self, ws: websocket.WebSocketApp, account_name: str, asset_type: str) -> None:
        symbols = self.symbol_provider(account_name).get(asset_type, [])
        if not symbols:
            return
        payload = {
            "action": "subscribe",
            "trades": symbols,
            "quotes": symbols,
            "bars": symbols,
        }
        ws.send(json.dumps(payload))

    def _run_market_stream(self, account_name: str, asset_type: str) -> None:
        self._run_stream(
            account_name=account_name,
            stream_name=f"market-{asset_type}",
            url=self._market_stream_url("crypto" if asset_type == "crypto" else "stock"),
            subscribe_after_auth=lambda ws: self._subscribe_market(ws, account_name, asset_type),
            event_handler=self._handle_market_message,
        )

    def _run_news_stream(self, account_name: str) -> None:
        def _subscribe(ws: websocket.WebSocketApp) -> None:
            ws.send(json.dumps({"action": "subscribe", "news": ["*"]}))

        self._run_stream(
            account_name=account_name,
            stream_name="news",
            url="wss://stream.data.alpaca.markets/v1beta1/news",
            subscribe_after_auth=_subscribe,
            event_handler=self._handle_news_message,
        )

    def _run_trade_updates_stream(self, account_name: str) -> None:
        def _listen(ws: websocket.WebSocketApp) -> None:
            ws.send(json.dumps({"action": "listen", "data": {"streams": ["trade_updates"]}}))

        self._run_stream(
            account_name=account_name,
            stream_name="trade_updates",
            url=self._trade_updates_url(),
            subscribe_after_auth=_listen,
            event_handler=self._handle_trade_updates_message,
        )

    def _run_stream(
        self,
        *,
        account_name: str,
        stream_name: str,
        url: str,
        subscribe_after_auth: Callable[[websocket.WebSocketApp], None],
        event_handler: Callable[[str, dict[str, Any]], None],
    ) -> None:
        backoff_seconds = 1.0
        subscribed = False

        while not self._stop_event.is_set():
            subscribed = False
            if self.thread_manager is not None:
                self.thread_manager.register(f"WS-{stream_name}", "websocket")

            def on_open(ws: websocket.WebSocketApp) -> None:
                try:
                    ws.send(self._auth_payload())
                except Exception as ex:
                    self.logger.warning("Alpaca stream %s auth send failed: %s", stream_name, ex)

            def on_message(ws: websocket.WebSocketApp, message: Any) -> None:
                nonlocal subscribed, backoff_seconds
                with self._running_lock:
                    self._stream_last_message_at[stream_name] = time.time()
                for payload in self._parse_message(message):
                    if self._is_auth_success(payload):
                        if not subscribed:
                            try:
                                subscribe_after_auth(ws)
                                subscribed = True
                                with self._running_lock:
                                    self._connected_streams.add(stream_name)
                                    self._stream_reconnect_attempts[stream_name] = 0
                                backoff_seconds = 1.0
                            except Exception as ex:
                                self.logger.warning("Alpaca stream %s subscribe failed: %s", stream_name, ex)
                        continue
                    if self._is_ack_message(payload):
                        continue
                    event_handler(stream_name, payload)

            def on_error(_: websocket.WebSocketApp, error: Any) -> None:
                with self._running_lock:
                    self._stream_last_error[stream_name] = str(error)
                if not self._stop_event.is_set():
                    self.logger.warning("Alpaca stream %s error for %s: %s", stream_name, account_name, error)

            def on_close(_: websocket.WebSocketApp, close_status_code: Any, close_msg: Any) -> None:
                with self._running_lock:
                    self._connected_streams.discard(stream_name)
                if not self._stop_event.is_set():
                    self.logger.info(
                        "Alpaca stream %s closed for %s: status=%s msg=%s",
                        stream_name,
                        account_name,
                        close_status_code,
                        close_msg,
                    )

            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )

            with self._running_lock:
                self._live_sockets[stream_name] = ws
                self._stream_last_message_at.setdefault(stream_name, time.time())

            try:
                ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as ex:
                with self._running_lock:
                    self._stream_last_error[stream_name] = str(ex)
                if not self._stop_event.is_set():
                    self.logger.warning("Alpaca stream %s reconnecting after error for %s: %s", stream_name, account_name, ex)
                    if self.thread_manager is not None:
                        self.thread_manager.set_error(f"WS-{stream_name}", str(ex))

            with self._running_lock:
                self._live_sockets.pop(stream_name, None)

            if self._stop_event.is_set():
                break

            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2.0, self.max_backoff_seconds)
            with self._running_lock:
                self._stream_reconnect_attempts[stream_name] = int(self._stream_reconnect_attempts.get(stream_name, 0) or 0) + 1
            if self.thread_manager is not None:
                self.thread_manager.heartbeat(f"WS-{stream_name}")

        if self.thread_manager is not None:
            self.thread_manager.set_stopped(f"WS-{stream_name}")

    @staticmethod
    def _is_ack_message(payload: dict[str, Any]) -> bool:
        stream = str(payload.get("stream", "")).lower().strip()
        if stream in {"listening", "authorization"}:
            return True
        message_type = str(payload.get("T", "")).lower().strip()
        return message_type == "success"

    @staticmethod
    def _is_auth_success(payload: dict[str, Any]) -> bool:
        if str(payload.get("T", "")).lower().strip() == "success":
            msg = str(payload.get("msg", "")).lower().strip()
            return msg in {"connected", "authenticated"}
        if str(payload.get("stream", "")).lower().strip() == "authorization":
            data = payload.get("data", {})
            if isinstance(data, dict):
                return str(data.get("status", "")).lower().strip() == "authorized"
        return False

    def _handle_market_message(self, _: str, payload: dict[str, Any]) -> None:
        message_type = str(payload.get("T", "")).lower().strip()
        symbol = str(payload.get("S", "")).upper().strip()
        if not symbol:
            return
        self.market_event_callback(message_type, payload)

    def _handle_news_message(self, _: str, payload: dict[str, Any]) -> None:
        if str(payload.get("T", "")).lower().strip() != "n":
            return
        self.news_event_callback("news", payload)

    def _handle_trade_updates_message(self, _: str, payload: dict[str, Any]) -> None:
        if str(payload.get("stream", "")).lower().strip() != "trade_updates":
            return
        data = payload.get("data", {})
        if isinstance(data, dict):
            self.trade_update_callback("trade_updates", data)