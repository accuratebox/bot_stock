from typing import Any

from broker.broker_client import AlpacaBrokerClient


class OrderManager:
    def __init__(self, broker: AlpacaBrokerClient, logger: Any) -> None:
        self.broker = broker
        self.logger = logger

    def create_market_order(self, symbol: str, qty: float, side: str, time_in_force: str | None = None) -> dict:
        if qty <= 0:
            raise ValueError("La cantidad debe ser mayor que cero")

        order = self.broker.send_market_order(symbol=symbol, qty=qty, side=side, time_in_force=time_in_force)
        self.logger.info("Orden creada: %s", order.get("id", "sin-id"))
        return order

    def create_limit_order(
        self,
        symbol: str,
        qty: float,
        side: str,
        limit_price: float,
        time_in_force: str | None = None,
    ) -> dict:
        if qty <= 0:
            raise ValueError("La cantidad debe ser mayor que cero")
        if limit_price <= 0:
            raise ValueError("El precio limite debe ser mayor que cero")

        order = self.broker.send_limit_order(
            symbol=symbol,
            qty=qty,
            side=side,
            limit_price=limit_price,
            time_in_force=time_in_force,
        )
        self.logger.info("Orden limite creada: %s", order.get("id", "sin-id"))
        return order

    def cancel_order(self, order_id: str) -> bool:
        cancelled = self.broker.cancel_order(order_id)
        if cancelled:
            self.logger.info("Orden cancelada: %s", order_id)
        return cancelled

    def review_orders(self, status: str = "open", limit: int = 50) -> list[dict]:
        orders = self.broker.list_orders(status=status, limit=limit)
        self.logger.info("Ordenes consultadas: %s", len(orders))
        return orders
