import os
import json
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    alpaca_endpoint: str = os.getenv("ALPACA_ENDPOINT", "https://paper-api.alpaca.markets/v2")
    alpaca_api_key: str = os.getenv("ALPACA_API_KEY", "PKOA2TP3QI7GK5MRWQJVM5CYBC")
    alpaca_api_secret: str = os.getenv("ALPACA_API_SECRET", "4VporwXFvWbNiAZiuuGZFwvViMahWh6nrB3m67A57vjC")

    alpaca_paper_endpoint: str = os.getenv(
        "ALPACA_PAPER_ENDPOINT",
        "https://paper-api.alpaca.markets/v2",
    )
    alpaca_paper_key: str = os.getenv("ALPACA_PAPER_KEY", "PKOA2TP3QI7GK5MRWQJVM5CYBC")
    alpaca_paper_secret: str = os.getenv(
        "ALPACA_PAPER_SECRET",
        "4VporwXFvWbNiAZiuuGZFwvViMahWh6nrB3m67A57vjC",
    )

    alpaca_paper2_endpoint: str = os.getenv(
        "ALPACA_PAPER2_ENDPOINT",
        os.getenv("ALPACA_PAPER_2_ENDPOINT", "https://paper-api.alpaca.markets/v2"),
    )
    alpaca_paper2_key: str = os.getenv("ALPACA_PAPER2_KEY", os.getenv("ALPACA_PAPER_2_KEY", "PKJSDK6MR2VREKERVJ77GH5BUZ"))
    alpaca_paper2_secret: str = os.getenv(
        "ALPACA_PAPER2_SECRET",
        os.getenv("ALPACA_PAPER_2_SECRET", "GgdQvbKw1bnBBr52VFaNKyj24tVWiwWFgUJcVahQ6mxQ"),
    )

    alpaca_paper3_endpoint: str = os.getenv(
        "ALPACA_PAPER3_ENDPOINT",
        os.getenv("ALPACA_PAPER_3_ENDPOINT", ""),
    )
    alpaca_paper3_key: str = os.getenv("ALPACA_PAPER3_KEY", os.getenv("ALPACA_PAPER_3_KEY", ""))
    alpaca_paper3_secret: str = os.getenv("ALPACA_PAPER3_SECRET", os.getenv("ALPACA_PAPER_3_SECRET", ""))

    alpaca_live_endpoint: str = os.getenv(
        "ALPACA_LIVE_ENDPOINT",
        "https://api.alpaca.markets/v2",
    )
    alpaca_live_key: str = os.getenv("ALPACA_LIVE_KEY", "AKUW6PSXZNYZBLDD7PC5J5UOQV")
    alpaca_live_secret: str = os.getenv("ALPACA_LIVE_SECRET", "9emBDPmtFgxNkYVBvZ25Pebzsj1NopsSA3kRzbVFXdLb")

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
    use_paper_trading: bool = os.getenv("USE_PAPER_TRADING", "True").lower() == "true"
    position_monitor_interval_seconds: int = int(os.getenv("POSITION_MONITOR_INTERVAL_SECONDS", "30"))
    default_trade_capital: float = float(os.getenv("DEFAULT_TRADE_CAPITAL", "500.0"))
    min_hold_seconds_before_auto_sell: int = int(os.getenv("MIN_HOLD_SECONDS_BEFORE_AUTO_SELL", "20"))
    crypto_entry_time_in_force: str = os.getenv("CRYPTO_ENTRY_TIME_IN_FORCE", "ioc").lower()

    def account_profiles(self) -> dict[str, dict[str, str]]:
        profiles: dict[str, dict[str, str]] = {
            "Paper": {
                "mode": "PAPER",
                "endpoint": self.alpaca_paper_endpoint,
                "key": self.alpaca_paper_key,
                "secret": self.alpaca_paper_secret,
            },
            "Real": {
                "mode": "LIVE",
                "endpoint": self.alpaca_live_endpoint,
                "key": self.alpaca_live_key,
                "secret": self.alpaca_live_secret,
            },
        }

        # Include the currently configured account if it differs from Paper/Real.
        current_profile = {
            "mode": "PAPER" if "paper" in str(self.alpaca_endpoint).lower() else "LIVE",
            "endpoint": self.alpaca_endpoint,
            "key": self.alpaca_api_key,
            "secret": self.alpaca_api_secret,
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
            }

        if self.alpaca_paper3_endpoint and self.alpaca_paper3_key and self.alpaca_paper3_secret:
            profiles["Paper 3"] = {
                "mode": "PAPER",
                "endpoint": self.alpaca_paper3_endpoint,
                "key": self.alpaca_paper3_key,
                "secret": self.alpaca_paper3_secret,
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
            profiles[account_name] = payload

        return profiles


settings = Settings()
