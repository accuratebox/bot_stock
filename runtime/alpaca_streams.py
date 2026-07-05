from __future__ import annotations

import json
import threading
import time
from typing import Any, Callable

import websocket


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

        self._stop_event = threading.Event()
        self._threads: list[threading.Thread] = []
        self._running_lock = threading.Lock()
        self._connected_streams: set[str] = set()
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
            self._running = True
        for thread in threads:
            thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        with self._running_lock:
            threads = list(self._threads)
            self._threads = []
            self._connected_streams.clear()
            self._running = False

        for thread in threads:
            if thread.is_alive():
                thread.join(timeout=2.0)

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

            def on_open(ws: websocket.WebSocketApp) -> None:
                try:
                    ws.send(self._auth_payload())
                except Exception as ex:
                    self.logger.warning("Alpaca stream %s auth send failed: %s", stream_name, ex)

            def on_message(ws: websocket.WebSocketApp, message: Any) -> None:
                nonlocal subscribed, backoff_seconds
                for payload in self._parse_message(message):
                    if self._is_auth_success(payload):
                        if not subscribed:
                            try:
                                subscribe_after_auth(ws)
                                subscribed = True
                                with self._running_lock:
                                    self._connected_streams.add(stream_name)
                                backoff_seconds = 1.0
                            except Exception as ex:
                                self.logger.warning("Alpaca stream %s subscribe failed: %s", stream_name, ex)
                        continue
                    if self._is_ack_message(payload):
                        continue
                    event_handler(stream_name, payload)

            def on_error(_: websocket.WebSocketApp, error: Any) -> None:
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

            try:
                ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as ex:
                if not self._stop_event.is_set():
                    self.logger.warning("Alpaca stream %s reconnecting after error for %s: %s", stream_name, account_name, ex)

            if self._stop_event.is_set():
                break

            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2.0, 30.0)

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