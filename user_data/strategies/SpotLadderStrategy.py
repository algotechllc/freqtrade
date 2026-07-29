# pragma pylint: disable=missing-docstring, unused-argument
"""
Spot ladder strategy — runs OrderManager on each bot loop (live/dry-run only).
Freqtrade trade signals are unused; exchange limit ladders are managed directly.
"""

from __future__ import annotations

import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from pandas import DataFrame

from freqtrade.strategy import IStrategy

logger = logging.getLogger(__name__)

USER_DATA_DIR = Path(__file__).resolve().parents[1]
if str(USER_DATA_DIR) not in sys.path:
    sys.path.insert(0, str(USER_DATA_DIR))

LADDER_CONFIG_PATH = USER_DATA_DIR / "spot_ladder" / "config.yaml"


class SpotLadderStrategy(IStrategy):
    INTERFACE_VERSION = 3

    timeframe = "5m"
    process_only_new_candles = False
    startup_candle_count = 10
    max_open_trades = 0
    can_short = False

    stoploss = -0.99
    minimal_roi = {"0": 100}

    order_types = {
        "entry": "limit",
        "exit": "limit",
        "stoploss": "limit",
        "stoploss_on_exchange": False,
    }

    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self._ladder_config: dict[str, Any] | None = None
        self._managers: list[Any] = []
        self._last_ladder_run = 0.0
        self._pair_by_symbol: dict[str, str] = {}

    def _load_ladder_config(self) -> dict[str, Any]:
        if not LADDER_CONFIG_PATH.is_file():
            raise FileNotFoundError(f"Spot ladder config not found: {LADDER_CONFIG_PATH}")
        with open(LADDER_CONFIG_PATH, encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        state_rel = cfg.get("paths", {}).get("state_dir", "spot_ladder/state")
        cfg.setdefault("paths", {})["state_dir"] = str((USER_DATA_DIR / state_rel).resolve())
        return cfg

    def _resolve_ccxt_pair(self, symbol: str) -> str:
        """Map ladder symbol to Freqtrade/ccxt pair (whitelist first)."""
        whitelist = self.config.get("exchange", {}).get("pair_whitelist") or []
        ccxt_overrides = (self._ladder_config or {}).get("ccxt_pairs") or {}
        if symbol in ccxt_overrides:
            return ccxt_overrides[symbol]
        base = symbol.split("/")[0].upper()
        for pair in whitelist:
            if pair.upper().startswith(f"{base}/"):
                return pair
        if len(whitelist) == 1:
            return whitelist[0]
        return symbol

    def _init_managers(self) -> None:
        from spot_ladder.hyperliquid_adapter import HyperliquidExchangeAdapter
        from spot_ladder.order_manager import OrderManager
        from spot_ladder.telegram_notifier import TelegramNotifier

        if self.dp._exchange is None:
            raise RuntimeError("Exchange not available on DataProvider")

        self._ladder_config = self._load_ladder_config()
        notifier = TelegramNotifier()
        symbols = self._ladder_config["trading"]["symbols"]

        self._managers = []
        for symbol in symbols:
            pair = self._resolve_ccxt_pair(symbol)
            self._pair_by_symbol[symbol] = pair
            adapter = HyperliquidExchangeAdapter(self.dp._exchange, pair)
            manager = OrderManager(
                symbol=symbol,
                api=adapter,
                notifier=notifier,
                config=self._ladder_config,
            )
            self._managers.append(manager)
            logger.info("Spot ladder manager ready for %s (ccxt pair %s)", symbol, pair)

    def _dry_run_balances_flat(self) -> dict[str, dict[str, float]]:
        """Simulate quote/base balances from dry-run orders or filled_orders JSON."""
        from spot_ladder.dry_run_balances import compute_dry_run_balances_flat

        if self.dp._exchange is None:
            raise RuntimeError("Exchange not available on DataProvider")

        cfg = self._ladder_config or self._load_ladder_config()
        stake = self.config.get("stake_currency", "USDC")
        wallet = float(self.config.get("dry_run_wallet", 1000))
        pair = next(iter(self._pair_by_symbol.values()), "XRP/USDC:USDC")
        base = pair.split("/")[0].upper()
        state_dir = cfg.get("paths", {}).get("state_dir") or str(USER_DATA_DIR / "spot_ladder/state")
        filled_path = str(Path(state_dir) / f"filled_orders_{base}.json")

        return compute_dry_run_balances_flat(
            self.dp._exchange,
            pair,
            stake,
            base,
            wallet,
            filled_path,
        )

    def _fetch_balances_flat(self) -> dict[str, dict[str, float]]:
        from spot_ladder.hyperliquid_adapter import HyperliquidExchangeAdapter

        wallet_address = (self.config.get("exchange", {}) or {}).get("walletAddress") or ""
        private_key = (self.config.get("exchange", {}) or {}).get("privateKey") or ""
        if self.config.get("dry_run") and not wallet_address.strip() and not private_key.strip():
            return self._dry_run_balances_flat()

        adapter = HyperliquidExchangeAdapter(
            self.dp._exchange, next(iter(self._pair_by_symbol.values()), "XRP/USDC:USDC")
        )
        try:
            return HyperliquidExchangeAdapter.balances_to_flat(adapter.get_balances())
        except Exception as e:
            if self.config.get("dry_run"):
                logger.warning(
                    "Exchange balance fetch failed in dry-run (%s); using simulated dry-run balances.", e
                )
                return self._dry_run_balances_flat()
            raise

    def bot_start(self, **kwargs) -> None:
        if self.config["runmode"].value in ("live", "dry_run"):
            try:
                self._init_managers()
            except Exception as e:
                logger.exception("Failed to initialize spot ladder managers: %s", e)

    def bot_loop_start(self, current_time: datetime, **kwargs) -> None:
        if self.config["runmode"].value not in ("live", "dry_run"):
            return
        if not self._managers:
            try:
                self._init_managers()
            except Exception as e:
                logger.error("Spot ladder init failed: %s", e)
                return

        cfg = self._ladder_config or self._load_ladder_config()
        interval = float(cfg["trading"].get("loop_interval", 120))
        now = time.time()
        if now - self._last_ladder_run < interval:
            return
        self._last_ladder_run = now

        if not cfg.get("safety", {}).get("trading_enabled", True):
            logger.debug("Spot ladder trading_enabled=false, skipping cycle")
            return

        try:
            balances = self._fetch_balances_flat()
        except Exception as e:
            logger.error("Balance fetch failed: %s", e)
            return

        for manager in self._managers:
            try:
                manager.update_balances(balances)
                manager.process()
            except Exception as e:
                logger.exception("Spot ladder process error (%s): %s", manager.symbol, e)

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        return dataframe
