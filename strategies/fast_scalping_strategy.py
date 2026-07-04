"""Fast scalping strategy for crypto - focused on quick entries and exits."""

from dataclasses import dataclass


@dataclass(frozen=True)
class CryptoScalpingSignal:
    """Signal for crypto fast scalping: LONG, SHORT (informative), or HOLD."""
    action: str  # "long", "short_signal" (informative only), or "hold"
    reason: str
    confidence: float = 0.0  # 0-100 score
    entry_type: str = "market"  # "market" or "limit"
    stop_loss_pct: float = 0.0
    tp1_pct: float = 0.0
    tp2_pct: float = 0.0
    max_tp_pct: float = 0.0
    details: str = ""


class FastScalpingStrategy:
    """
    Fast scalping strategy for crypto (no overnight holds, quick exits).
    
    - No HOLD > 30 minutes
    - Allow stop loss
    - LONG only in Alpaca spot (no actual SHORT execution)
    - SHORT_SIGNAL generated informatively for future short-capable brokers
    - Tight TP levels for quick profit-taking
    """

    def __init__(self, short_window: int = 5, long_window: int = 20) -> None:
        self.short_window = short_window
        self.long_window = long_window

    def generate_signal(
        self,
        symbol: str,
        candles_1m: list[dict],
        candles_3m: list[dict],
        candles_5m: list[dict],
        candles_15m: list[dict],
        current_price: float,
        spread_pct: float,
        vwap: float,
        rsi_14: float = 0.0,
        macd_signal: float = 0.0,
        stop_loss_pct: float = 0.30,
        tp1_pct: float = 0.35,
        tp2_pct: float = 0.60,
        max_tp_pct: float = 0.80,
    ) -> CryptoScalpingSignal:
        """
        Generate crypto scalping signal based on technical analysis.
        
        Args:
            symbol: Crypto pair (e.g., "BTC/USD")
            candles_*: OHLCV candles at different timeframes
            current_price: Last bid/ask midpoint
            spread_pct: Bid-ask spread percentage
            vwap: Volume Weighted Average Price
            rsi_14: RSI indicator (0-100)
            macd_signal: MACD signal line crossover
            stop_loss_pct: Stop loss threshold (default 0.30% for BTC/ETH)
            tp1_pct: Take profit level 1 (default 0.35%)
            tp2_pct: Take profit level 2 (default 0.60%)
            max_tp_pct: Maximum TP level (default 0.80%)
        
        Returns:
            CryptoScalpingSignal with action and risk parameters
        """
        
        # Validate inputs
        if not all([candles_1m, candles_3m, candles_5m, candles_15m]):
            return CryptoScalpingSignal(
                action="hold",
                reason="Insufficient candle data",
                confidence=0.0,
            )

        if current_price <= 0:
            return CryptoScalpingSignal(
                action="hold",
                reason="Invalid price",
                confidence=0.0,
            )

        # Crypto-specific thresholds (tighter than stocks)
        spread_threshold = 0.10  # Very tight spread for crypto scalping
        min_volume_usd = 10000.0  # Minimum volume

        # Check basic filters
        if spread_pct > spread_threshold:
            return CryptoScalpingSignal(
                action="hold",
                reason=f"Spread too wide: {spread_pct:.3f}%",
                confidence=0.0,
            )

        if current_price < vwap:
            return CryptoScalpingSignal(
                action="hold",
                reason="Price below VWAP",
                confidence=0.0,
            )

        # Extract closes and volumes
        closes_1m = [float(c.get("close", 0.0) or 0.0) for c in candles_1m]
        closes_3m = [float(c.get("close", 0.0) or 0.0) for c in candles_3m]
        closes_5m = [float(c.get("close", 0.0) or 0.0) for c in candles_5m]
        closes_15m = [float(c.get("close", 0.0) or 0.0) for c in candles_15m]
        volumes_1m = [float(c.get("volume", 0.0) or 0.0) for c in candles_1m]

        # Calculate moving averages
        ma_short_1m = self._moving_average(closes_1m, self.short_window)
        ma_long_1m = self._moving_average(closes_1m, self.long_window)
        ma_short_5m = self._moving_average(closes_5m, self.short_window)
        ma_long_5m = self._moving_average(closes_5m, self.long_window)
        ma_short_15m = self._moving_average(closes_15m, self.short_window)
        ma_long_15m = self._moving_average(closes_15m, self.long_window)

        # Trend analysis
        uptrend_1m = ma_short_1m > ma_long_1m
        uptrend_5m = ma_short_5m > ma_long_5m
        uptrend_15m = ma_short_15m > ma_long_15m

        downtrend_1m = ma_short_1m < ma_long_1m
        downtrend_5m = ma_short_5m < ma_long_5m
        downtrend_15m = ma_short_15m < ma_long_15m

        # Momentum indicators
        recent_high = max(closes_5m[-10:]) if len(closes_5m) >= 10 else max(closes_5m)
        recent_low = min(closes_5m[-10:]) if len(closes_5m) >= 10 else min(closes_5m)
        avg_volume = sum(volumes_1m[-5:]) / 5 if len(volumes_1m) >= 5 else sum(volumes_1m) / len(volumes_1m)
        last_volume = volumes_1m[-1] if volumes_1m else 0.0

        volume_ok = avg_volume > 0 and last_volume >= (avg_volume * 0.6)
        rsi_not_overbought = rsi_14 < 75.0 if rsi_14 > 0 else True
        rsi_not_oversold = rsi_14 > 25.0 if rsi_14 > 0 else True

        # ===== LONG SIGNAL =====
        if uptrend_1m and uptrend_5m and uptrend_15m and volume_ok and rsi_not_overbought:
            # Calculate confidence
            trend_strength = 0.0
            if uptrend_1m:
                trend_strength += 25.0
            if uptrend_5m:
                trend_strength += 25.0
            if uptrend_15m:
                trend_strength += 25.0
            if volume_ok:
                trend_strength += 15.0
            if rsi_not_overbought and rsi_14 < 60:
                trend_strength += 10.0

            confidence = min(trend_strength, 100.0)

            details = (
                f"Uptrend: 1m={uptrend_1m}, 5m={uptrend_5m}, 15m={uptrend_15m} | "
                f"MA_1m: {ma_short_1m:.2f}>{ma_long_1m:.2f} | "
                f"Volume: avg={avg_volume:.0f}, last={last_volume:.0f} | "
                f"RSI={rsi_14:.1f}"
            )

            return CryptoScalpingSignal(
                action="long",
                reason="Multi-timeframe uptrend with volume confirmation",
                confidence=confidence,
                entry_type="limit" if confidence >= 85 else "market",
                stop_loss_pct=stop_loss_pct,
                tp1_pct=tp1_pct,
                tp2_pct=tp2_pct,
                max_tp_pct=max_tp_pct,
                details=details,
            )

        # ===== SHORT SIGNAL (informative only for Alpaca) =====
        if downtrend_1m and downtrend_5m and downtrend_15m and volume_ok and rsi_not_oversold:
            # Calculate confidence
            trend_strength = 0.0
            if downtrend_1m:
                trend_strength += 25.0
            if downtrend_5m:
                trend_strength += 25.0
            if downtrend_15m:
                trend_strength += 25.0
            if volume_ok:
                trend_strength += 15.0
            if rsi_not_oversold and rsi_14 > 40:
                trend_strength += 10.0

            confidence = min(trend_strength, 100.0)

            details = (
                f"Downtrend: 1m={downtrend_1m}, 5m={downtrend_5m}, 15m={downtrend_15m} | "
                f"MA_1m: {ma_short_1m:.2f}<{ma_long_1m:.2f} | "
                f"Volume: avg={avg_volume:.0f}, last={last_volume:.0f} | "
                f"RSI={rsi_14:.1f}"
            )

            return CryptoScalpingSignal(
                action="short_signal",  # Informative only (no actual short in Alpaca)
                reason="Multi-timeframe downtrend - SHORT_SIGNAL informative only",
                confidence=confidence,
                entry_type="limit",
                stop_loss_pct=stop_loss_pct,
                tp1_pct=tp1_pct,
                tp2_pct=tp2_pct,
                max_tp_pct=max_tp_pct,
                details=details,
            )

        # ===== NO SIGNAL =====
        return CryptoScalpingSignal(
            action="hold",
            reason="No clear directional bias",
            confidence=0.0,
            details=(
                f"Trends: 1m={uptrend_1m}, 5m={uptrend_5m}, 15m={uptrend_15m} | "
                f"Volume: {volume_ok} | RSI: {rsi_14:.1f}"
            ),
        )

    @staticmethod
    def _moving_average(values: list[float], window: int) -> float:
        """Calculate simple moving average."""
        if window <= 0 or len(values) < window:
            return 0.0
        subset = values[-window:]
        return sum(subset) / window
