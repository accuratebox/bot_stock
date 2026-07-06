from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ai_trading_brain.openai_analyzer import OpenAIAnalyzer
from ai_trading_brain.service import AITradingBrainService
from ml_model.model_registry import ModelRegistry
from ml_model.predictor import SignalPredictor


class FakeLogger:
    def info(self, *args, **kwargs):
        return None

    def warning(self, *args, **kwargs):
        return None

    def exception(self, *args, **kwargs):
        return None


class FakeJournal:
    def get_daily_realized_pnl(self):
        return 0.0


class FakePositionManager:
    def __init__(self):
        self.journal = FakeJournal()


class FakeRiskManager:
    def can_trade(self, current_daily_pnl: float) -> bool:
        return current_daily_pnl > -100.0


class FakeBroker:
    def __init__(self):
        self.endpoint = "https://paper-api.alpaca.markets/v2"
        self.api_key = "paper-key"
        self.api_secret = "paper-secret"
        self.positions = []
        self.supported = [{"symbol": "SOL/USD"}, {"symbol": "AAPL"}]

    def get_positions(self):
        return list(self.positions)

    def list_stocks(self, status="active", only_tradable=True):
        return [{"symbol": "AAPL"}]

    def list_cryptos(self, status="active", only_tradable=True):
        return [{"symbol": "SOL/USD"}]

    def list_tradable_assets(self):
        return list(self.supported)


class FakeMarketData:
    def __init__(self):
        self.price = 10.0
        self.spread = 0.01
        self.volume = 5000.0

    def get_last_price(self, symbol: str) -> float:
        return self.price

    def get_latest_quote(self, symbol: str) -> dict:
        return {"spread": self.spread, "spread_pct": (self.spread / max(self.price, 1e-8)) * 100.0}

    def get_candles(self, symbol: str, interval: str = "1m", limit: int = 60):
        candles = []
        for index in range(limit):
            base = self.price * (1 + (index - limit) * 0.0005)
            candles.append(
                {
                    "open": base,
                    "high": base + 0.05,
                    "low": base - 0.05,
                    "close": base + 0.01,
                    "volume": self.volume,
                    "timestamp": f"t{index}",
                }
            )
        return candles

    def calculate_vwap(self, candles):
        total_volume = sum(float(item.get("volume", 0.0) or 0.0) for item in candles)
        if total_volume <= 0:
            return self.price
        return sum(float(item.get("close", 0.0) or 0.0) * float(item.get("volume", 0.0) or 0.0) for item in candles) / total_volume


class FakeOrderManager:
    def create_limit_order(self, symbol, qty, side, limit_price, time_in_force="gtc"):
        return {
            "id": f"order-{symbol}-{side}",
            "status": "new",
            "filled_avg_price": 0.0,
            "symbol": symbol,
            "qty": qty,
            "side": side,
            "limit_price": limit_price,
            "time_in_force": time_in_force,
        }


class FakeSettings:
    def __init__(self, root: Path):
        self.ai_brain_db_path = str(root / "ai_trading_brain.sqlite")
        self.ai_models_dir = str(root / "models")
        self.ai_signal_only_mode = True
        self.ai_default_max_capital_assigned = 1000.0
        self.ai_default_max_position_size = 250.0
        self.ai_default_max_daily_loss = 100.0
        self.ai_fees_buffer = 0.02
        self.ai_slippage_buffer = 0.03
        self.ai_minimum_profit = 0.05
        self.ai_min_volume_required = 1000.0
        self.ai_max_spread_allowed = 0.05
        self.ai_snapshots_1m_days = 60
        self.ai_snapshots_5m_days = 365
        self.ai_logs_retention_days = 90
        self.ai_keep_model_versions = 5
        self.openai_api_key = ""
        self.openai_model = "gpt-4.1-mini"
        self.paper_trading = True
        self.live_trading_enabled = False
        self.manual_approval_required = True
        self.alpaca_api_key = "paper-key"
        self.alpaca_api_secret = "paper-secret"
        self.require_nordvpn_before_trading = False
        self.required_vpn_provider = "NordVPN"
        self.required_vpn_country = "Dominican Republic"

    def account_profiles(self):
        return {
            "Paper": {
                "mode": "PAPER",
                "endpoint": "https://paper-api.alpaca.markets/v2",
                "key": "paper-key",
                "secret": "paper-secret",
            }
        }


class AITradingBrainTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.settings = FakeSettings(root)
        self.broker = FakeBroker()
        self.market_data = FakeMarketData()
        self.order_manager = FakeOrderManager()
        self.position_manager = FakePositionManager()
        self.risk_manager = FakeRiskManager()
        self.logger = FakeLogger()
        self.service = AITradingBrainService(
            broker=self.broker,
            market_data=self.market_data,
            order_manager=self.order_manager,
            position_manager=self.position_manager,
            risk_manager=self.risk_manager,
            settings=self.settings,
            logger=self.logger,
        )
        self.account = self.service.refresh_account_context("Paper")
        self.account_id = int(self.account["id"])

    def tearDown(self):
        self.temp_dir.cleanup()

    def _insert_buy_trade(self, symbol="SOL/USD", qty=10.0, price=10.0):
        self.service.database.insert_trade(
            {
                "timestamp": self.service._now_iso(),
                "account_id": self.account_id,
                "symbol": symbol,
                "asset_type": "crypto",
                "side": "buy",
                "order_type": "limit",
                "qty": qty,
                "limit_price": price,
                "filled_price": price,
                "fees": 0.0,
                "status": "filled",
                "broker_order_id": "buy-1",
                "signal_id": None,
                "created_at": self.service._now_iso(),
            }
        )

    def test_no_sell_below_average_cost(self):
        self._insert_buy_trade()
        self.broker.positions = [{"symbol": "SOL/USD", "qty": 10.0, "avg_entry_price": 10.0}]
        self.market_data.price = 9.0
        result = self.service.place_limit_sell_if_allowed("SOL/USD", "Paper")
        self.assertEqual(result["status"], "blocked")

    def test_hold_when_price_below_average_cost(self):
        self._insert_buy_trade()
        self.broker.positions = [{"symbol": "SOL/USD", "qty": 10.0, "avg_entry_price": 10.0}]
        self.market_data.price = 9.0
        positions = self.service.sync_positions("Paper")
        self.assertEqual(positions[0]["status"], "HOLD")

    def test_sell_allowed_only_above_average_cost_plus_buffer(self):
        self._insert_buy_trade()
        self.broker.positions = [{"symbol": "SOL/USD", "qty": 10.0, "avg_entry_price": 10.0}]
        self.market_data.price = 10.2
        positions = self.service.sync_positions("Paper")
        self.assertEqual(positions[0]["status"], "SELL_ALLOWED")

    def test_no_buy_if_no_funds_assigned(self):
        self.service.update_runtime_controls(
            account_name="Paper",
            max_capital_assigned=0.0,
            max_position_size=0.0,
            max_daily_loss=100.0,
            enabled=True,
            signal_only_mode=True,
            paper_trading=True,
            live_trading_enabled=False,
            manual_approval_required=True,
            kill_switch=False,
            auto_trade_stocks_enabled=True,
            auto_trade_cryptos_enabled=True,
        )
        signal = self.service.generate_signal("SOL/USD", "crypto", "Paper")
        self.assertIn("No hay fondos asignados", signal["reason"])

    def test_no_buy_if_spread_is_high(self):
        self.market_data.spread = 1.0
        signal = self.service.generate_signal("SOL/USD", "crypto", "Paper")
        self.assertIn("Spread demasiado alto", signal["reason"])

    def test_live_trading_disabled_by_default(self):
        security = self.service.get_security_state("Paper")
        self.assertFalse(security["live_trading_enabled"])
        self.assertTrue(security["paper_trading"])

    def test_openai_analyzer_returns_valid_json(self):
        analyzer = OpenAIAnalyzer(api_key="", logger=self.logger)
        normalized = analyzer._normalize_response(
            '{"symbol":"SOL/USD","asset_type":"crypto","sentiment":"positive","event_type":"news","importance_score":20,"risk_score":10,"summary":"ok","possible_market_impact":"up","action_bias":"bullish"}',
            symbol="SOL/USD",
            asset_type="crypto",
        )
        self.assertEqual(normalized["symbol"], "SOL/USD")
        self.assertIn(normalized["sentiment"], {"positive", "negative", "neutral"})

    def test_predictor_returns_valid_action(self):
        registry = ModelRegistry(self.settings.ai_models_dir)
        predictor = SignalPredictor(registry=registry, logger=self.logger)
        result = predictor.predict_signal({"composite_score": 86.0, "distance_from_average_cost": 1.0})
        self.assertIn(result["action"], {"AVOID", "WATCH", "BUY_SMALL", "BUY", "HOLD", "SELL_ALLOWED"})

    def test_crypto_symbol_key_treats_usdc_as_usd_pair(self):
        self.assertEqual(AITradingBrainService._symbol_key("SOL/USDC"), "SOLUSD")
        self.assertEqual(AITradingBrainService._symbol_key("SOL/USD"), "SOLUSD")
        self.assertEqual(AITradingBrainService._symbol_key("SOLUSD"), "SOLUSD")

    def test_model_pruning_keeps_approved_model(self):
        registry = ModelRegistry(self.settings.ai_models_dir)
        versions = [f"general_model_20260706_00000{index}" for index in range(8)]
        for version in versions:
            registry.save_model(model={"version": version}, version=version, metadata={"model_version": version})

        approved_version = versions[0]
        registry.approve_model(approved_version)
        registry.prune_old_versions(keep_last=5)

        self.assertTrue((Path(self.settings.ai_models_dir) / f"{approved_version}.pkl").exists())
        self.assertTrue(registry.approved_model_available())
        self.assertNotIn(versions[1], registry.available_versions())

    def test_database_stores_signals_and_trades(self):
        signal = self.service.generate_signal("SOL/USD", "crypto", "Paper")
        self.service.database.insert_trade(
            {
                "timestamp": self.service._now_iso(),
                "account_id": self.account_id,
                "symbol": "SOL/USD",
                "asset_type": "crypto",
                "side": "buy",
                "order_type": "limit",
                "qty": 1.0,
                "limit_price": 10.0,
                "filled_price": 10.0,
                "fees": 0.0,
                "status": "filled",
                "broker_order_id": "trade-1",
                "signal_id": int(signal["id"]),
                "created_at": self.service._now_iso(),
            }
        )
        trades = self.service.list_history("Paper")
        signals = self.service.list_signals(limit=5)
        self.assertGreaterEqual(len(trades), 1)
        self.assertGreaterEqual(len(signals), 1)


if __name__ == "__main__":
    unittest.main()
