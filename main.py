from broker.broker_client import AlpacaBrokerClient
from config import settings
from data.market_data import MarketDataService
from orders.order_manager import OrderManager
from portfolio.position_manager import PositionManager
from scheduling.market_open_scheduler import MarketOpenScheduler
from risk.risk_manager import RiskManager
from strategies.scalping_strategy import ScalpingStrategy
from ui.bot_window import BotControlWindow
from utils.logger import get_logger


def main() -> None:
    logger = get_logger("trading_bot")
    logger.info("Iniciando trading bot UI")

    broker = AlpacaBrokerClient(
        endpoint=settings.alpaca_endpoint,
        api_key=settings.alpaca_api_key,
        api_secret=settings.alpaca_api_secret,
        logger=logger,
    )
    market_data = MarketDataService(logger=logger)
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
    recovery = position_manager.synchronize_open_positions()
    logger.info(
        "Recuperacion inicial | abiertas=%s sincronizadas=%s ya_linkeadas=%s",
        recovery.get("open_positions", 0),
        recovery.get("synced", 0),
        recovery.get("already_linked", 0),
    )
    scheduler = MarketOpenScheduler(
        broker=broker,
        market_data=market_data,
        strategy=strategy,
        position_manager=position_manager,
        risk_manager=risk_manager,
        logger=logger,
        settings=settings,
    )

    app = BotControlWindow(
        broker=broker,
        market_data=market_data,
        risk_manager=risk_manager,
        strategy=strategy,
        order_manager=order_manager,
        position_manager=position_manager,
        scheduler=scheduler,
        logger=logger,
    )
    app.run()


if __name__ == "__main__":
    main()
