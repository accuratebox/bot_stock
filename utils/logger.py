import logging
from logging import Logger
from pathlib import Path


class _KeywordFilter(logging.Filter):
    def __init__(self, keywords: list[str]) -> None:
        super().__init__()
        self.keywords = [item.lower() for item in keywords]

    def filter(self, record: logging.LogRecord) -> bool:
        text = f"{record.name} {record.getMessage()}".lower()
        return any(token in text for token in self.keywords)


def get_logger(name: str, app_scope: str = "trading") -> Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    scope = str(app_scope or "trading").strip().lower()
    logs_dir = Path("logs") / scope
    logs_dir.mkdir(parents=True, exist_ok=True)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    bot_handler = logging.FileHandler(logs_dir / "bot.log")
    bot_handler.setFormatter(formatter)
    logger.addHandler(bot_handler)

    if scope == "trading":
        legacy_handler = logging.FileHandler("trading_bot.log")
        legacy_handler.setFormatter(formatter)
        logger.addHandler(legacy_handler)

    errors_handler = logging.FileHandler(logs_dir / "errors.log")
    errors_handler.setLevel(logging.WARNING)
    errors_handler.setFormatter(formatter)
    logger.addHandler(errors_handler)

    trades_handler = logging.FileHandler(logs_dir / "trades.log")
    trades_handler.setFormatter(formatter)
    trades_handler.addFilter(_KeywordFilter(["trade", "order", "buy", "sell", "position"]))
    logger.addHandler(trades_handler)

    api_handler = logging.FileHandler(logs_dir / "api_calls.log")
    api_handler.setFormatter(formatter)
    api_handler.addFilter(_KeywordFilter(["api", "http", "alpaca", "openai", "cryptopanic", "stream", "429"]))
    logger.addHandler(api_handler)

    return logger
