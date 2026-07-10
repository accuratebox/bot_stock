from __future__ import annotations

import fcntl
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from ai_trading_brain import AITradingBrainService
from broker.broker_client import AlpacaBrokerClient
from config import settings
from data.market_data import MarketDataService
from orders.order_manager import OrderManager
from portfolio.position_manager import PositionManager
from risk.risk_manager import RiskManager
from runtime.alpaca_state import AlpacaRuntimeState
from scheduling.market_open_scheduler import MarketOpenScheduler
from strategies.scalping_strategy import ScalpingStrategy


@dataclass
class AppComponents:
    runtime_state: AlpacaRuntimeState
    broker: AlpacaBrokerClient
    market_data: MarketDataService
    risk_manager: RiskManager
    strategy: ScalpingStrategy
    order_manager: OrderManager
    position_manager: PositionManager
    scheduler: MarketOpenScheduler
    ai_trading_brain: AITradingBrainService


def acquire_single_instance_lock(lock_name: str, logger: Any, warning_message: str) -> object | None:
    lock_path = Path(__file__).resolve().parents[1] / "runtime" / lock_name
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(lock_path, "w", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        try:
            logger.warning(warning_message)
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


def build_app_components(logger: Any, *, runtime_role: str, recover_positions: bool) -> AppComponents:
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
    if recover_positions:
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
                    "La UI seguira iniciando para permitir corregir la cuenta."
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
        runtime_role=runtime_role,
    )

    return AppComponents(
        runtime_state=runtime_state,
        broker=broker,
        market_data=market_data,
        risk_manager=risk_manager,
        strategy=strategy,
        order_manager=order_manager,
        position_manager=position_manager,
        scheduler=scheduler,
        ai_trading_brain=ai_trading_brain,
    )