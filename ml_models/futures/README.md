# Futures Model Stack (Binance Futures)

This module is isolated from stock and spot models.

## Scope

- Model name: `futures_model`
- Actions: `LONG`, `SHORT`, `NO_TRADE`
- Exit actions supported in runtime policy: `EXIT_LONG`, `EXIT_SHORT`
- Target: positive expected value, not accuracy-only.

## Files

- `futures_feature_builder.py`
- `futures_labeler.py`
- `futures_trainer.py`
- `futures_predictor.py`
- `futures_backtester.py`
- `futures_risk_manager.py`

## Data Columns

Feature builder stores rows with:

- Market data: OHLCV, quote volume, trades, taker buy/sell, spread, bid/ask, orderbook imbalance.
- Futures data: mark/index, basis, funding, open interest, liquidation events (fallback-safe), long/short ratio (fallback-safe), leverage bracket (fallback-safe), maintenance margin.
- Indicators: returns 1/3/5/15m, volatility 5/15m, ATR, RSI, VWAP, EMA9/EMA21, volume spike, taker delta.
- News: news score, sentiment score, event risk score, catalyst score.

## Storage

- SQLite: `ml_models/futures/futures_dataset.sqlite`
  - `futures_features`
  - `futures_labels`
- Optional Parquet export is available when pandas/pyarrow are installed.

## Labeling

For each timestamp:

1. Simulate LONG over horizons 5/10/15m with TP/SL grid.
2. Simulate SHORT over horizons 5/10/15m with TP/SL grid.
3. Compute EV for each direction.
4. Select:
   - `LONG` if EV_LONG dominates and passes filters.
   - `SHORT` if EV_SHORT dominates and passes filters.
   - `NO_TRADE` otherwise.

Costs included in EV:

- fees
- slippage

## Training

Trainer model preference:

1. XGBoost (if installed)
2. LightGBM (if installed)
3. RandomForest fallback

Model output is multiclass (`LONG`, `SHORT`, `NO_TRADE`).

## Backtesting Approval Gate

Model is approved only when all are true:

- profit factor > 1.2
- max drawdown under configured cap
- expectancy > 0
- does not overtrade

## Risk Rules

Risk manager blocks entries when:

- websocket disconnected
- stale/incomplete data
- API/rate-limit errors
- cannot place stop or take profit
- spread/funding unacceptable
- daily loss / per-hour trades / consecutive losses limits exceeded
- model says `NO_TRADE`

All futures trades require:

- stop loss
- take profit
- max hold time

## Demo-first policy

Before live trading:

- at least 2 weeks on Binance Futures demo/testnet
- at least 200 simulated trades
- positive profit factor
- controlled drawdown
- complete logs
