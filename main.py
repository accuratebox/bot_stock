import faulthandler
import fcntl
import signal
from pathlib import Path
from typing import Any

from ai_trading_brain import AITradingBrainService
import requests
from broker.broker_client import AlpacaBrokerClient
from config import settings
from data.market_data import MarketDataService
from orders.order_manager import OrderManager
from portfolio.position_manager import PositionManager
from runtime.alpaca_state import AlpacaRuntimeState
from scheduling.market_open_scheduler import MarketOpenScheduler
from risk.risk_manager import RiskManager
from strategies.scalping_strategy import ScalpingStrategy
from ui.bot_window import BotControlWindow
from utils.logger import get_logger


_SINGLE_INSTANCE_LOCK = None


def _acquire_single_instance_lock(logger: Any) -> object | None:
    lock_path = Path(__file__).resolve().parent / "runtime" / "trading_bot_main.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(lock_path, "w", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        try:
            logger.warning("Otra instancia del bot ya está activa. Cancelando segundo arranque.")
        except Exception:
            pass
        try:
            handle.close()
        except Exception:
            pass
        return None

    handle.write(str(Path(__file__).resolve()))
    handle.write("\n")
    handle.flush()
    return handle


def main() -> None:
    global _SINGLE_INSTANCE_LOCK
    faulthandler.register(signal.SIGUSR1, all_threads=True)
    logger = get_logger("trading_bot")
    _SINGLE_INSTANCE_LOCK = _acquire_single_instance_lock(logger)
    if _SINGLE_INSTANCE_LOCK is None:
        return
    logger.info("Iniciando trading bot UI")
    runtime_state = AlpacaRuntimeState()

    provider = str(getattr(settings, "broker_provider", "alpaca") or "alpaca").lower()
    if provider == "binance":
        endpoint = str(getattr(settings, "binance_demo_endpoint", "https://testnet.binancefuture.com") or "https://testnet.binancefuture.com")
        api_key = str(getattr(settings, "binance_demo_key", "") or "")
        api_secret = str(getattr(settings, "binance_demo_secret", "") or "")
    else:
        endpoint = settings.alpaca_endpoint
        api_key = settings.alpaca_api_key
        api_secret = settings.alpaca_api_secret

    broker = AlpacaBrokerClient(
        endpoint=endpoint,
        api_key=api_key,
        api_secret=api_secret,
        logger=logger,
        account_name="main",
        runtime_state=runtime_state,
    )
    market_data = MarketDataService(logger=logger, account_name="main", runtime_state=runtime_state)
    risk_manager = RiskManager(
        max_daily_loss=settings.max_daily_loss,
        risk_per_trade_pct=settings.risk_per_trade_pct,
        stop_loss_pct=settings.stop_loss_pct,
    )
    strategy = ScalpingStrategy()
    order_manager = OrderManager(broker=broker, logger=logger)
    position_manager = PositionManager(
        broker=broker,
        market_data=market_data,
        order_manager=order_manager,
        logger=logger,
        settings=settings,
    )
    try:
        recovery = position_manager.synchronize_open_positions()
        logger.info(
            "Recuperacion inicial | abiertas=%s sincronizadas=%s ya_linkeadas=%s",
            recovery.get("open_positions", 0),
            recovery.get("synced", 0),
            recovery.get("already_linked", 0),
        )
    except requests.exceptions.HTTPError as ex:
        response = getattr(ex, "response", None)
        status = getattr(response, "status_code", None)
        if status == 401:
            logger.warning(
                "No se pudo sincronizar posiciones al iniciar: HTTP 401 Unauthorized. "
                "La UI seguira iniciando para permitir cambiar o corregir la cuenta."
            )
        else:
            raise
    except requests.exceptions.RequestException as ex:
        logger.warning("No se pudo sincronizar posiciones al iniciar: %s", ex)
    scheduler = MarketOpenScheduler(
        broker=broker,
        market_data=market_data,
        strategy=strategy,
        position_manager=position_manager,
        risk_manager=risk_manager,
        logger=logger,
        settings=settings,
    )
    ai_trading_brain = AITradingBrainService(
        broker=broker,
        market_data=market_data,
        order_manager=order_manager,
        position_manager=position_manager,
        risk_manager=risk_manager,
        settings=settings,
        logger=logger,
    )

    app = BotControlWindow(
        broker=broker,
        market_data=market_data,
        risk_manager=risk_manager,
        strategy=strategy,
        order_manager=order_manager,
        position_manager=position_manager,
        scheduler=scheduler,
        ai_trading_brain=ai_trading_brain,
        logger=logger,
    )
    ai_trading_brain.set_post_trade_update_hook(app._sync_ai_watch_tabs_from_recent_trades_async)
    app.run()


if __name__ == "__main__":
    main()
