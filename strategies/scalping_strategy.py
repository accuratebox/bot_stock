from dataclasses import dataclass


@dataclass(frozen=True)
class ScalpingSignal:
    action: str
    reason: str
    details: str = ""


class ScalpingStrategy:
    def __init__(self, short_window: int = 5, long_window: int = 20) -> None:
        self.short_window = short_window
        self.long_window = long_window

    def generate_signal(
        self,
        symbol: str,
        candles_1m: list[dict],
        candles_5m: list[dict],
        current_price: float,
        spread_pct: float,
        vwap: float,
        asset_type: str = "stock",
        max_extension_pct: float = 4.0,
    ) -> ScalpingSignal:
        if len(candles_1m) < self.long_window or len(candles_5m) < self.long_window:
            return ScalpingSignal(action="hold", reason=f"No hay velas suficientes para {symbol}")

        closes_1m = [float(c.get("close", 0.0) or 0.0) for c in candles_1m]
        closes_5m = [float(c.get("close", 0.0) or 0.0) for c in candles_5m]
        volumes_1m = [float(c.get("volume", 0.0) or 0.0) for c in candles_1m]

        short_ma_1m = self._moving_average(closes_1m, self.short_window)
        long_ma_1m = self._moving_average(closes_1m, self.long_window)
        short_ma_5m = self._moving_average(closes_5m, self.short_window)
        long_ma_5m = self._moving_average(closes_5m, self.long_window)

        recent_high = max(closes_1m[-self.long_window:])
        recent_low = min(closes_1m[-self.long_window:])
        average_volume = sum(volumes_1m[-self.long_window:]) / float(self.long_window)
        last_volume = volumes_1m[-1]

        if current_price <= 0:
            return ScalpingSignal(action="hold", reason="Precio invalido")
        spread_threshold = 0.35 if asset_type == "stock" else 0.80
        min_average_volume = 50000.0 if asset_type == "stock" else 100.0

        if asset_type == "stock" and current_price < 10:
            return ScalpingSignal(action="hold", reason="Activo demasiado barato")
        if spread_pct > spread_threshold:
            return ScalpingSignal(action="hold", reason="Spread demasiado amplio")
        if average_volume < min_average_volume:
            return ScalpingSignal(action="hold", reason="Volumen insuficiente")
        if current_price < vwap:
            return ScalpingSignal(action="hold", reason="Precio por debajo del VWAP")
        if current_price > recent_high * (1.0 + (max_extension_pct / 100.0)):
            return ScalpingSignal(action="hold", reason="Activo demasiado extendido")

        trend_favorable = short_ma_1m > long_ma_1m and short_ma_5m > long_ma_5m
        breakout_or_support = current_price >= recent_high * 0.995 or current_price <= recent_low * 1.02
        momentum_ok = current_price >= short_ma_1m and last_volume >= (average_volume * 0.7)

        if trend_favorable and breakout_or_support and momentum_ok:
            details = (
                f"short_ma_1m={short_ma_1m:.2f}, long_ma_1m={long_ma_1m:.2f}, "
                f"short_ma_5m={short_ma_5m:.2f}, long_ma_5m={long_ma_5m:.2f}, "
                f"vwap={vwap:.2f}, spread_pct={spread_pct:.2f}, avg_volume={average_volume:.0f}"
            )
            return ScalpingSignal(action="buy", reason="Condiciones tecnicas favorables", details=details)

        return ScalpingSignal(
            action="hold",
            reason="Sin señal de compra clara",
            details=(
                f"trend={trend_favorable}, breakout_or_support={breakout_or_support}, "
                f"momentum_ok={momentum_ok}, spread_pct={spread_pct:.2f}, vwap={vwap:.2f}"
            ),
        )

    @staticmethod
    def _moving_average(values: list[float], window: int) -> float:
        if window <= 0 or len(values) < window:
            return 0.0
        subset = values[-window:]
        return sum(subset) / window
