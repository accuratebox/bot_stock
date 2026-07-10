from __future__ import annotations

import signal
import sys
from pathlib import Path

import faulthandler


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from runtime.app_runtime import acquire_single_instance_lock, build_app_components
from ui.bot_window import BotControlWindow
from utils.logger import get_logger


_SINGLE_INSTANCE_LOCK = None


def main() -> None:
    global _SINGLE_INSTANCE_LOCK
    faulthandler.register(signal.SIGUSR1, all_threads=True)
    logger = get_logger("trading_bot", app_scope="trading")
    _SINGLE_INSTANCE_LOCK = acquire_single_instance_lock(
        lock_name="trading_bot_main.lock",
        logger=logger,
        warning_message="Otra instancia del Trading Bot ya está activa. Cancelando segundo arranque.",
    )
    if _SINGLE_INSTANCE_LOCK is None:
        return

    logger.info("Iniciando Trading Bot")
    components = build_app_components(logger, runtime_role="trading", recover_positions=True)
    app = BotControlWindow(
        broker=components.broker,
        market_data=components.market_data,
        risk_manager=components.risk_manager,
        strategy=components.strategy,
        order_manager=components.order_manager,
        position_manager=components.position_manager,
        scheduler=components.scheduler,
        ai_trading_brain=components.ai_trading_brain,
        logger=logger,
    )
    components.ai_trading_brain.set_post_trade_update_hook(app._sync_ai_watch_tabs_from_recent_trades_async)
    app.run()


if __name__ == "__main__":
    main()
