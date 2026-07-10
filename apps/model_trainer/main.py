from __future__ import annotations

import signal
import sys
from pathlib import Path

import faulthandler


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from runtime.app_runtime import acquire_single_instance_lock, build_app_components
from ui.model_trainer_window import ModelTrainerWindow
from utils.logger import get_logger


_SINGLE_INSTANCE_LOCK = None


def main() -> None:
    global _SINGLE_INSTANCE_LOCK
    faulthandler.register(signal.SIGUSR1, all_threads=True)
    logger = get_logger("model_trainer", app_scope="training")
    _SINGLE_INSTANCE_LOCK = acquire_single_instance_lock(
        lock_name="model_trainer_main.lock",
        logger=logger,
        warning_message="Otra instancia de Model Trainer ya está activa. Cancelando segundo arranque.",
    )
    if _SINGLE_INSTANCE_LOCK is None:
        return

    logger.info("Iniciando Model Trainer")
    components = build_app_components(logger, runtime_role="training", recover_positions=False)
    app = ModelTrainerWindow(
        ai_trading_brain=components.ai_trading_brain,
        account_profiles=components.ai_trading_brain.settings.account_profiles(),
        logger=logger,
    )
    app.run()


if __name__ == "__main__":
    main()
