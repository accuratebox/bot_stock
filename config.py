import os
import json
import re
from dataclasses import dataclass
from pathlib import Path


def _load_local_env_file() -> None:
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.exists():
        return

    try:
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if not key or key in os.environ:
                continue
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
                value = value[1:-1]
            os.environ[key] = value
    except Exception:
        # Ignore malformed local env files and keep process env as source of truth.
        return


_load_local_env_file()


def _env(*names: str, default: str = "") -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None and str(value).strip() != "":
            return str(value).strip()
    return default


def _env_bool(*names: str, default: bool) -> bool:
    raw = _env(*names, default="true" if default else "false")
    return raw.lower() == "true"


@dataclass(frozen=True)
class Settings:
    broker_provider: str = _env("BROKER_PROVIDER", default="binance").lower()

    alpaca_endpoint: str = _env("BINANCE_BASE_URL", "BINANCE_ENDPOINT", "ALPACA_BASE_URL", "ALPACA_ENDPOINT", default="https://testnet.binancefuture.com")
    alpaca_api_key: str = _env("BINANCE_API_KEY", "BINANCE_DEMO_API_KEY", "ALPACA_API_KEY", "ALPACA_KEY", default="")
    alpaca_api_secret: str = _env("BINANCE_API_SECRET", "BINANCE_DEMO_API_SECRET", "ALPACA_SECRET_KEY", "ALPACA_API_SECRET", default="")

    alpaca_paper_endpoint: str = _env(
        "BINANCE_DEMO_ENDPOINT",
        "ALPACA_PAPER_BASE_URL",
        "ALPACA_PAPER_ENDPOINT",
        default="https://testnet.binancefuture.com",
    )
    alpaca_paper_key: str = _env("BINANCE_DEMO_API_KEY", "BINANCE_API_KEY", "ALPACA_PAPER_KEY", "ALPACA_API_KEY", default="")
    alpaca_paper_secret: str = _env("BINANCE_DEMO_API_SECRET", "BINANCE_API_SECRET", "ALPACA_PAPER_SECRET", "ALPACA_SECRET_KEY", "ALPACA_API_SECRET", default="")

    alpaca_paper2_endpoint: str = _env(
        "ALPACA_PAPER2_ENDPOINT",
        "ALPACA_PAPER_2_ENDPOINT",
        default="https://paper-api.alpaca.markets/v2",
    )
    alpaca_paper2_key: str = _env("ALPACA_PAPER2_KEY", "ALPACA_PAPER_2_KEY", default="")
    alpaca_paper2_secret: str = _env("ALPACA_PAPER2_SECRET", "ALPACA_PAPER_2_SECRET", default="")

    alpaca_paper3_endpoint: str = _env("ALPACA_PAPER3_ENDPOINT", "ALPACA_PAPER_3_ENDPOINT", default="")
    alpaca_paper3_key: str = _env("ALPACA_PAPER3_KEY", "ALPACA_PAPER_3_KEY", default="")
    alpaca_paper3_secret: str = _env("ALPACA_PAPER3_SECRET", "ALPACA_PAPER_3_SECRET", default="")

    alpaca_live_endpoint: str = _env("ALPACA_LIVE_ENDPOINT", default="https://api.alpaca.markets/v2")
    alpaca_live_key: str = _env("ALPACA_LIVE_KEY", default="")
    alpaca_live_secret: str = _env("ALPACA_LIVE_SECRET", default="")

    binance_demo_endpoint: str = _env("BINANCE_DEMO_ENDPOINT", default="https://testnet.binancefuture.com")
    binance_demo_key: str = _env("BINANCE_DEMO_API_KEY", "BINANCE_API_KEY", default="")
    binance_demo_secret: str = _env("BINANCE_DEMO_API_SECRET", "BINANCE_API_SECRET", default="")

    openai_api_key: str = _env("OPENAI_API_KEY", default="")
    openai_model: str = _env("OPENAI_MODEL", default="gpt-4.1-mini")
    coingecko_api_key: str = _env("COINGECKO_API_KEY", default="")
    cryptopanic_api_key: str = _env("CRYPTOPANIC_API_KEY", default="")

    default_symbol: str = os.getenv("BOT_SYMBOL", "AAPL")
    default_interval: str = os.getenv("BOT_INTERVAL", "1m")

    stop_loss_pct: float = float(os.getenv("BOT_STOP_LOSS_PCT", "0.5"))
    risk_per_trade_pct: float = float(os.getenv("BOT_RISK_PER_TRADE_PCT", "1.0"))
    max_daily_loss: float = float(os.getenv("BOT_MAX_DAILY_LOSS", "200.0"))

    max_open_positions: int = int(os.getenv("MAX_OPEN_POSITIONS", "5"))
    target_profit_per_share: float = float(os.getenv("TARGET_PROFIT_PER_SHARE", "0.50"))
    auto_sell_only_if_profitable: bool = os.getenv("AUTO_SELL_ONLY_IF_PROFITABLE", "True").lower() == "true"
    never_sell_at_loss: bool = os.getenv("NEVER_SELL_AT_LOSS", "True").lower() == "true"
    allow_manual_sell: bool = os.getenv("ALLOW_MANUAL_SELL", "True").lower() == "true"
    allow_averaging_down: bool = os.getenv("ALLOW_AVERAGING_DOWN", "False").lower() == "true"
    max_hold_positions: int = int(os.getenv("MAX_HOLD_POSITIONS", "5"))
    stop_new_trades_minutes_before_close: int = int(os.getenv("STOP_NEW_TRADES_MINUTES_BEFORE_CLOSE", "60"))
    close_only_profitable_positions_before_close: bool = (
        os.getenv("CLOSE_ONLY_PROFITABLE_POSITIONS_BEFORE_CLOSE", "True").lower() == "true"
    )
    paper_trading: bool = _env_bool("PAPER_TRADING", default=True)
    live_trading_enabled: bool = _env_bool("LIVE_TRADING_ENABLED", default=False)
    manual_approval_required: bool = _env_bool("MANUAL_APPROVAL_REQUIRED", default=True)
    use_paper_trading: bool = _env_bool("USE_PAPER_TRADING", default=True)
    position_monitor_interval_seconds: int = int(os.getenv("POSITION_MONITOR_INTERVAL_SECONDS", "30"))
    default_trade_capital: float = float(os.getenv("DEFAULT_TRADE_CAPITAL", "500.0"))
    min_hold_seconds_before_auto_sell: int = int(os.getenv("MIN_HOLD_SECONDS_BEFORE_AUTO_SELL", "20"))
    crypto_entry_time_in_force: str = os.getenv("CRYPTO_ENTRY_TIME_IN_FORCE", "ioc").lower()

    ai_brain_db_path: str = _env("AI_BRAIN_DB_PATH", default=str(os.path.join(os.path.dirname(__file__), "ai_trading_brain.sqlite")))
    ai_models_dir: str = _env("AI_MODELS_DIR", default=str(os.path.join(os.path.dirname(__file__), "models")))
    ai_signal_only_mode: bool = _env_bool("AI_SIGNAL_ONLY_MODE", default=True)
    ai_default_max_capital_assigned: float = float(_env("AI_DEFAULT_MAX_CAPITAL_ASSIGNED", default="1000.0"))
    ai_default_max_position_size: float = float(_env("AI_DEFAULT_MAX_POSITION_SIZE", default="250.0"))
    ai_default_max_daily_loss: float = float(_env("AI_DEFAULT_MAX_DAILY_LOSS", default="100.0"))
    ai_target_profit_per_operation: float = float(_env("AI_TARGET_PROFIT_PER_OPERATION", default=_env("AI_TARGET_PROFIT_PER_SHARE", default="0.05")))
    ai_target_profit_per_operation_stocks: float = float(
        _env(
            "AI_TARGET_PROFIT_PER_OPERATION_STOCKS",
            default=_env("AI_TARGET_PROFIT_PER_SHARE_STOCKS", default=_env("AI_TARGET_PROFIT_PER_OPERATION", default=_env("AI_TARGET_PROFIT_PER_SHARE", default="0.05"))),
        )
    )
    ai_target_profit_per_operation_cryptos: float = float(
        _env(
            "AI_TARGET_PROFIT_PER_OPERATION_CRYPTOS",
            default=_env("AI_TARGET_PROFIT_PER_SHARE_CRYPTOS", default=_env("AI_TARGET_PROFIT_PER_OPERATION", default=_env("AI_TARGET_PROFIT_PER_SHARE", default="0.05"))),
        )
    )
    ai_fees_buffer: float = float(_env("AI_FEES_BUFFER", default="0.02"))
    ai_slippage_buffer: float = float(_env("AI_SLIPPAGE_BUFFER", default="0.03"))
    ai_minimum_profit: float = float(_env("AI_MINIMUM_PROFIT", default="0.05"))
    ai_min_volume_required: float = float(_env("AI_MIN_VOLUME_REQUIRED", default="1000"))
    ai_max_spread_allowed: float = float(_env("AI_MAX_SPREAD_ALLOWED", default="0.05"))
    ai_snapshots_1m_days: int = int(_env("AI_SNAPSHOTS_1M_DAYS", default="60"))
    ai_snapshots_5m_days: int = int(_env("AI_SNAPSHOTS_5M_DAYS", default="365"))
    ai_logs_retention_days: int = int(_env("AI_LOGS_RETENTION_DAYS", default="90"))
    ai_keep_model_versions: int = int(_env("AI_KEEP_MODEL_VERSIONS", default="5"))
    ai_crypto_collection_interval_seconds: int = int(_env("AI_CRYPTO_COLLECTION_INTERVAL_SECONDS", default="20"))
    ai_stock_open_collection_interval_seconds: int = int(_env("AI_STOCK_OPEN_COLLECTION_INTERVAL_SECONDS", default="45"))
    ai_stock_closed_collection_interval_seconds: int = int(_env("AI_STOCK_CLOSED_COLLECTION_INTERVAL_SECONDS", default="240"))
    ai_news_social_interval_seconds: int = int(_env("AI_NEWS_SOCIAL_INTERVAL_SECONDS", default="180"))
    ai_openai_signal_trigger_score: float = float(_env("AI_OPENAI_SIGNAL_TRIGGER_SCORE", default="85"))
    ai_openai_news_trigger_importance: float = float(_env("AI_OPENAI_NEWS_TRIGGER_IMPORTANCE", default="65"))
    ai_min_volume_24h_usd: float = float(_env("AI_MIN_VOLUME_24H_USD", default="100000"))
    ai_min_execution_confidence: float = float(_env("AI_MIN_EXECUTION_CONFIDENCE", default="60"))
    ai_watch_as_buy_small: bool = _env_bool("AI_WATCH_AS_BUY_SMALL", default=False)
    ai_watch_resolution_min_evidence: int = int(_env("AI_WATCH_RESOLUTION_MIN_EVIDENCE", default="2"))
    ai_watch_resolution_min_edge: int = int(_env("AI_WATCH_RESOLUTION_MIN_EDGE", default="1"))
    ai_watch_resolution_min_volume_1m_usd: float = float(_env("AI_WATCH_RESOLUTION_MIN_VOLUME_1M_USD", default="1000"))
    ai_watch_resolution_tie_news_bias: float = float(_env("AI_WATCH_RESOLUTION_TIE_NEWS_BIAS", default="0.12"))
    ai_outcome_win_profit_pct: float = float(_env("AI_OUTCOME_WIN_PROFIT_PCT", default="0.25"))
    ai_outcome_loss_drawdown_pct: float = float(_env("AI_OUTCOME_LOSS_DRAWDOWN_PCT", default="0.25"))
    ai_training_label_type: str = _env("AI_TRAINING_LABEL_TYPE", default="result_15m_fallback_30m").lower()
    ai_auto_train_mode: str = _env("AI_AUTO_TRAIN_MODE", default="12h").lower()
    ai_dev_mode: bool = _env_bool("AI_DEV_MODE", default=False)
    ai_focus_stocks_only: bool = _env_bool("AI_FOCUS_STOCKS_ONLY", default=False)
    ai_focus_cryptos_only: bool = _env_bool("AI_FOCUS_CRYPTOS_ONLY", default=False)
    ai_focus_stocks_symbols: str = _env("AI_FOCUS_STOCKS_SYMBOLS", default="")
    ai_focus_cryptos_symbols: str = _env("AI_FOCUS_CRYPTOS_SYMBOLS", default="")

    # Stability / timeout controls
    http_timeout_alpaca_seconds: int = int(_env("HTTP_TIMEOUT_BINANCE_SECONDS", "HTTP_TIMEOUT_ALPACA_SECONDS", default="10"))
    http_timeout_cryptopanic_seconds: int = int(_env("HTTP_TIMEOUT_CRYPTOPANIC_SECONDS", default="10"))
    http_timeout_openai_seconds: int = int(_env("HTTP_TIMEOUT_OPENAI_SECONDS", default="25"))
    http_timeout_news_seconds: int = int(_env("HTTP_TIMEOUT_NEWS_SECONDS", default="10"))
    websocket_stale_seconds: int = int(_env("WEBSOCKET_STALE_SECONDS", default="45"))
    websocket_max_reconnect_backoff_seconds: int = int(_env("WEBSOCKET_MAX_RECONNECT_BACKOFF_SECONDS", default="30"))
    health_monitor_interval_seconds: int = int(_env("HEALTH_MONITOR_INTERVAL_SECONDS", default="30"))
    ui_max_log_lines: int = int(_env("UI_MAX_LOG_LINES", default="1000"))
    require_vpn_before_trading: bool = _env_bool("REQUIRE_VPN_BEFORE_TRADING", default=True)
    required_vpn_provider: str = _env("REQUIRED_VPN_PROVIDER", default="WireGuard")
    required_vpn_country: str = _env("REQUIRED_VPN_COUNTRY", default="Dominican Republic")
    required_vpn_connection_name: str = _env("REQUIRED_VPN_CONNECTION_NAME", default="")
    required_vpn_interface_name: str = _env("REQUIRED_VPN_INTERFACE_NAME", default="")
    vpn_expected_ipv4: str = _env("VPN_EXPECTED_IPV4", default="")
    vpn_require_no_ipv6: bool = _env_bool("VPN_REQUIRE_NO_IPV6", default=True)

    # ===== STOCKS CONFIGURATION =====
    stock_allow_hold: bool = _env_bool("STOCK_ALLOW_HOLD", default=True)
    stock_allow_stop_loss: bool = _env_bool("STOCK_ALLOW_STOP_LOSS", default=False)
    stock_no_auto_sell_below_avg_cost: bool = _env_bool("STOCK_NO_AUTO_SELL_BELOW_AVG_COST", default=True)
    stock_auto_sell_profit_pct: float = float(_env("STOCK_AUTO_SELL_PROFIT_PCT", default="0.50"))

    # ===== CRYPTO CONFIGURATION =====
    crypto_allow_hold: bool = _env_bool("CRYPTO_ALLOW_HOLD", default=False)
    crypto_allow_stop_loss: bool = _env_bool("CRYPTO_ALLOW_STOP_LOSS", default=True)
    crypto_allow_short_signals: bool = _env_bool("CRYPTO_ALLOW_SHORT_SIGNALS", default=True)
    crypto_allow_short_execution: bool = _env_bool("CRYPTO_ALLOW_SHORT_EXECUTION", default=False)
    crypto_futures_only_mode: bool = _env_bool("CRYPTO_FUTURES_ONLY_MODE", default=True)
    crypto_futures_default_leverage: int = int(_env("CRYPTO_FUTURES_DEFAULT_LEVERAGE", default="1"))
    crypto_futures_max_leverage: int = int(_env("CRYPTO_FUTURES_MAX_LEVERAGE", default="20"))
    ai_futures_require_technical: bool = _env_bool("AI_FUTURES_REQUIRE_TECHNICAL", default=True)
    ai_futures_require_news: bool = _env_bool("AI_FUTURES_REQUIRE_NEWS", default=False)
    ai_futures_enable_long: bool = _env_bool("AI_FUTURES_ENABLE_LONG", default=True)
    ai_futures_enable_short: bool = _env_bool("AI_FUTURES_ENABLE_SHORT", default=True)
    crypto_max_hold_minutes: int = int(_env("CRYPTO_MAX_HOLD_MINUTES", default="30"))
    crypto_min_entry_score: float = float(_env("CRYPTO_MIN_ENTRY_SCORE", default="75.0"))
    crypto_buy_small_score_threshold: float = float(_env("CRYPTO_BUY_SMALL_SCORE_THRESHOLD", default="84.0"))
    crypto_no_auto_sell_below_avg_cost: bool = _env_bool("CRYPTO_NO_AUTO_SELL_BELOW_AVG_COST", default=False)
    crypto_force_market_order_for_exits: bool = _env_bool("CRYPTO_FORCE_MARKET_ORDER_FOR_EXITS", default=True)
    
    # Volatility tiers: BTC/ETH (tier1), SOL/XRP/LINK/AVAX/DOGE (tier2), Memes (tier3)
    # BTC/ETH (most stable)
    crypto_btc_eth_stop_loss_pct: float = float(_env("CRYPTO_BTC_ETH_STOP_LOSS_PCT", default="0.30"))
    crypto_btc_eth_tp1_pct: float = float(_env("CRYPTO_BTC_ETH_TP1_PCT", default="0.35"))
    crypto_btc_eth_tp2_pct: float = float(_env("CRYPTO_BTC_ETH_TP2_PCT", default="0.60"))
    crypto_btc_eth_max_tp_pct: float = float(_env("CRYPTO_BTC_ETH_MAX_TP_PCT", default="0.80"))

    # ALT-coins (medium volatility)
    crypto_alt_stop_loss_pct: float = float(_env("CRYPTO_ALT_STOP_LOSS_PCT", default="0.42"))
    crypto_alt_tp1_pct: float = float(_env("CRYPTO_ALT_TP1_PCT", default="0.50"))
    crypto_alt_tp2_pct: float = float(_env("CRYPTO_ALT_TP2_PCT", default="0.90"))
    crypto_alt_max_tp_pct: float = float(_env("CRYPTO_ALT_MAX_TP_PCT", default="1.20"))

    # Memes (high volatility)
    crypto_meme_stop_loss_pct: float = float(_env("CRYPTO_MEME_STOP_LOSS_PCT", default="0.75"))
    crypto_meme_tp1_pct: float = float(_env("CRYPTO_MEME_TP1_PCT", default="0.80"))
    crypto_meme_tp2_pct: float = float(_env("CRYPTO_MEME_TP2_PCT", default="1.50"))
    crypto_meme_max_tp_pct: float = float(_env("CRYPTO_MEME_MAX_TP_PCT", default="2.50"))

    # CryptoPanic caching (to respect 600 req/month quota)
    cryptopanic_cache_seconds: int = int(_env("CRYPTOPANIC_CACHE_SECONDS", default="1800"))  # 30 minutes
    cryptopanic_general_news_interval_seconds: int = int(_env("CRYPTOPANIC_GENERAL_NEWS_INTERVAL_SECONDS", default="7200"))  # 2 hours
    cryptopanic_max_requests_per_day: int = int(_env("CRYPTOPANIC_MAX_REQUESTS_PER_DAY", default="20"))
    cryptopanic_request_tracking_enabled: bool = _env_bool("CRYPTOPANIC_REQUEST_TRACKING_ENABLED", default=True)
    cryptopanic_monthly_limit: int = int(_env("CRYPTOPANIC_MONTHLY_LIMIT", default="600"))
    cryptopanic_used_this_month: int = int(_env("CRYPTOPANIC_USED_THIS_MONTH", default="0"))
    cryptopanic_request_days: str = _env("CRYPTOPANIC_REQUEST_DAYS", default="mon,tue,wed,thu,fri")

    # Crypto dynamic asset list (will be fetched from Alpaca)
    crypto_preferred_symbols: str = _env("CRYPTO_PREFERRED_SYMBOLS", default="BTC/USD,ETH/USD,SOL/USD,XRP/USD,DOGE/USD,LINK/USD,AVAX/USD,LTC/USD")

    def get_crypto_thresholds(self, symbol: str) -> dict[str, float]:
        """Get stop loss and TP thresholds based on crypto volatility tier."""
        symbol_upper = str(symbol or "").upper().replace("/USD", "")
        
        # Tier 1: BTC, ETH (stable)
        if symbol_upper in {"BTC", "ETH"}:
            return {
                "stop_loss_pct": self.crypto_btc_eth_stop_loss_pct,
                "tp1_pct": self.crypto_btc_eth_tp1_pct,
                "tp2_pct": self.crypto_btc_eth_tp2_pct,
                "max_tp_pct": self.crypto_btc_eth_max_tp_pct,
            }
        
        # Tier 3: Memes (high volatility)
        meme_symbols = {"PEPE", "BONK", "SHIB", "WIF", "TRUMP", "HYPE"}
        if symbol_upper in meme_symbols:
            return {
                "stop_loss_pct": self.crypto_meme_stop_loss_pct,
                "tp1_pct": self.crypto_meme_tp1_pct,
                "tp2_pct": self.crypto_meme_tp2_pct,
                "max_tp_pct": self.crypto_meme_max_tp_pct,
            }
        
        # Tier 2: ALT-coins (default)
        return {
            "stop_loss_pct": self.crypto_alt_stop_loss_pct,
            "tp1_pct": self.crypto_alt_tp1_pct,
            "tp2_pct": self.crypto_alt_tp2_pct,
            "max_tp_pct": self.crypto_alt_max_tp_pct,
        }

    def account_profiles(self) -> dict[str, dict[str, str]]:
        if str(self.broker_provider or "alpaca").strip().lower() == "binance":
            return {
                "Binance Demo": {
                    "mode": "DEMO",
                    "endpoint": self.binance_demo_endpoint,
                    "key": self.binance_demo_key,
                    "secret": self.binance_demo_secret,
                    "provider": "binance",
                }
            }

        profiles: dict[str, dict[str, str]] = {
            "Paper": {
                "mode": "PAPER",
                "endpoint": self.alpaca_paper_endpoint,
                "key": self.alpaca_paper_key,
                "secret": self.alpaca_paper_secret,
                "provider": "alpaca",
            },
            "Real": {
                "mode": "LIVE",
                "endpoint": self.alpaca_live_endpoint,
                "key": self.alpaca_live_key,
                "secret": self.alpaca_live_secret,
                "provider": "alpaca",
            },
        }

        # Include the currently configured account if it differs from Paper/Real.
        current_profile = {
            "mode": "PAPER" if "paper" in str(self.alpaca_endpoint).lower() else "LIVE",
            "endpoint": self.alpaca_endpoint,
            "key": self.alpaca_api_key,
            "secret": self.alpaca_api_secret,
            "provider": "alpaca",
        }

        known_fingerprints = {
            (str(p.get("endpoint", "")), str(p.get("key", "")), str(p.get("secret", "")))
            for p in profiles.values()
        }
        current_fingerprint = (
            str(current_profile.get("endpoint", "")),
            str(current_profile.get("key", "")),
            str(current_profile.get("secret", "")),
        )
        if current_fingerprint not in known_fingerprints and all(current_fingerprint):
            profiles["Cuenta Actual"] = current_profile

        if self.alpaca_paper2_endpoint and self.alpaca_paper2_key and self.alpaca_paper2_secret:
            profiles["Paper 2"] = {
                "mode": "PAPER",
                "endpoint": self.alpaca_paper2_endpoint,
                "key": self.alpaca_paper2_key,
                "secret": self.alpaca_paper2_secret,
                "provider": "alpaca",
            }

        if self.alpaca_paper3_endpoint and self.alpaca_paper3_key and self.alpaca_paper3_secret:
            profiles["Paper 3"] = {
                "mode": "PAPER",
                "endpoint": self.alpaca_paper3_endpoint,
                "key": self.alpaca_paper3_key,
                "secret": self.alpaca_paper3_secret,
                "provider": "alpaca",
            }

        # Conventional second/third paper account env names.
        # Examples: ALPACA_PAPER2_KEY / ALPACA_PAPER_2_KEY
        for idx in range(2, 10):
            endpoint = os.getenv(f"ALPACA_PAPER{idx}_ENDPOINT") or os.getenv(f"ALPACA_PAPER_{idx}_ENDPOINT")
            key = os.getenv(f"ALPACA_PAPER{idx}_KEY") or os.getenv(f"ALPACA_PAPER_{idx}_KEY")
            secret = os.getenv(f"ALPACA_PAPER{idx}_SECRET") or os.getenv(f"ALPACA_PAPER_{idx}_SECRET")
            if endpoint and key and secret:
                profiles[f"Paper {idx}"] = {
                    "mode": "PAPER",
                    "endpoint": endpoint,
                    "key": key,
                    "secret": secret,
                    "provider": "alpaca",
                }

        # Optional JSON format:
        # ALPACA_ACCOUNTS_JSON={"Cuenta3":{"mode":"PAPER","endpoint":"...","key":"...","secret":"..."}}
        raw_json = os.getenv("ALPACA_ACCOUNTS_JSON", "").strip()
        if raw_json:
            try:
                loaded = json.loads(raw_json)
                if isinstance(loaded, dict):
                    for name, payload in loaded.items():
                        if not isinstance(payload, dict):
                            continue
                        profiles[str(name)] = {
                            "mode": str(payload.get("mode", "CUSTOM")).upper(),
                            "endpoint": str(payload.get("endpoint", "")),
                            "key": str(payload.get("key", "")),
                            "secret": str(payload.get("secret", "")),
                            "provider": str(payload.get("provider", "alpaca")).lower(),
                        }
            except Exception:
                pass

        # Optional per-account env format:
        # ALPACA_ACCOUNT_CUENTA3_ENDPOINT / _KEY / _SECRET / _MODE
        pattern = re.compile(r"^ALPACA_ACCOUNT_([A-Z0-9_]+)_(ENDPOINT|KEY|SECRET|MODE)$")
        temp_accounts: dict[str, dict[str, str]] = {}
        for env_key, env_value in os.environ.items():
            match = pattern.match(env_key)
            if not match:
                continue

            account_token = match.group(1)
            field = match.group(2)
            account_name = account_token.replace("_", " ").title()
            bucket = temp_accounts.setdefault(
                account_name,
                {"mode": "CUSTOM", "endpoint": "", "key": "", "secret": ""},
            )
            if field == "ENDPOINT":
                bucket["endpoint"] = env_value
            elif field == "KEY":
                bucket["key"] = env_value
            elif field == "SECRET":
                bucket["secret"] = env_value
            elif field == "MODE":
                bucket["mode"] = env_value.upper()

        for account_name, payload in temp_accounts.items():
            payload["provider"] = "alpaca"
            profiles[account_name] = payload

        return profiles


settings = Settings()
