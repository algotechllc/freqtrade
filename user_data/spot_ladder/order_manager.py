"""
Hyperliquid ladder bot — buy/sell limit order management.

Each cycle (driven by SpotLadderStrategy + trading.loop_interval):
  - Buy ladder below last price (weighted sizing; optional price_elevation throttling)
  - Core sell ladder above average entry (recovery; LIFO ledger in filled_orders JSON)
  - Working sell ladder above current price when underwater (mean_reversion config)

Exchange I/O goes through the adapter (HyperliquidExchangeAdapter); notifications
through the notifier interface (Slack by default).
"""
import logging
import time
import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from typing import Any

from spot_ladder.telegram_notifier import TelegramNotifier

try:
    from reporting.performance_tracker import get_tracker
except ImportError:
    get_tracker = None


@dataclass
class Order:
    """Represents an open order"""
    order_id: str
    symbol: str
    side: str  # 'buy' or 'sell'
    amount: float
    rate: float
    market: str


class OrderManager:
    """Manages buy/sell ladders for a symbol"""
    
    def __init__(
        self,
        symbol: str,
        api: Any,
        notifier: TelegramNotifier,
        config: Dict
    ):
        self.symbol = symbol
        self.api = api
        self.notifier = notifier
        self.config = config

        paths = config.get("paths", {})
        self._state_dir = paths.get("state_dir") or os.path.join(os.path.dirname(__file__), "state")
        os.makedirs(self._state_dir, exist_ok=True)
        
        # Parse symbol (e.g., "XRP/USDC" -> cointype="XRP", market="USDC")
        parts = symbol.split('/')
        self.cointype = parts[0].upper()
        self.market = parts[1].upper() if len(parts) > 1 else "USDC"
        
        # Trading parameters from config
        self.buy_levels = config['trading']['buy_levels']
        self.sell_levels = config['trading']['sell_levels']
        self.balance_percentage = config['trading']['balance_percentage_per_symbol']
        self.min_order_size = config['trading']['min_order_size']
        self.price_update_threshold = config['trading']['price_update_threshold']
        self.buy_ladder_rebuild_on_fill_pct = float(
            config['trading'].get('buy_ladder_rebuild_on_fill_pct', 3.0)
        )
        self.price_update_time_window = config['trading'].get('price_update_time_window', 3600)  # Default 1 hour in seconds
        self.price_update_short_window = config['trading'].get('price_update_short_window', 1800)  # Default 30 minutes for catching gradual drops
        self.price_update_quick_window = config['trading'].get('price_update_quick_window', 600)  # Default 10 minutes for catching sudden drops
        self.price_update_quick_threshold = config['trading'].get('price_update_quick_threshold', 5.0)  # Higher threshold for quick window (5% vs 3.0%)
        # Support separate buy/sell distribution settings (with fallback to legacy setting)
        legacy_distribution = config['trading'].get('order_size_distribution', 'weighted')
        self.buy_order_distribution = config['trading'].get('buy_order_distribution', legacy_distribution)
        self.sell_order_distribution = config['trading'].get('sell_order_distribution', legacy_distribution)
        self.order_size_distribution = legacy_distribution  # Keep for backward compatibility
        self.small_position_multiplier = config['trading'].get('small_position_multiplier', 1.0)  # Default 1.0 (no reduction)
        self.small_position_threshold = config['trading'].get('small_position_threshold', 999.0)  # Default 999 (effectively disabled)
        self.increased_capital_threshold = config['trading'].get('increased_capital_threshold', 500.0)  # Default $500
        self.increased_capital_observation_period = config['trading'].get('increased_capital_observation_period', 3600)  # Default 1 hour in seconds
        self.max_buy_ladder_usdc = float(config['trading'].get('max_buy_ladder_usdc', 0) or 0)  # 0 = unlimited
        
        # Trading fees
        self.buy_fee = config['trading'].get('buy_fee_percentage', 0.01)  # Default 1%
        self.sell_fee = config['trading'].get('sell_fee_percentage', 0.01)  # Default 1%
        self.total_fee = self.buy_fee + self.sell_fee  # Total round-trip fee
        
        # Warn if sell levels are too low to be profitable after fees
        min_sell_level = min(self.sell_levels) if self.sell_levels else 0
        if min_sell_level < (self.total_fee * 100 * 1.1):  # Need at least 10% buffer above fees
            logging.warning(
                f"{self.symbol}: Minimum sell level ({min_sell_level}%) may not be profitable "
                f"after fees ({self.total_fee * 100}%). Consider increasing sell levels."
            )
        
        # Safety limits
        self.max_buy_orders = config['safety']['max_buy_orders']
        self.max_sell_orders = config['safety']['max_sell_orders']
        # Independent limits for Core and Working (if specified, otherwise use split logic)
        self.max_core_sell_orders = config['safety'].get('max_core_sell_orders', None)
        self.max_working_sell_orders = config['safety'].get('max_working_sell_orders', None)
        self.max_position_size = config['safety']['max_position_size']
        
        # State tracking
        self.current_price = 0.0
        self.last_price = 0.0
        self.price_when_orders_placed = 0.0  # Track price when orders were last placed/updated
        self.price_history = []  # List of (timestamp, price) tuples for longer time window checks
        self.bid_price = 0.0  # Current bid price
        self.ask_price = 0.0  # Current ask price
        self.mid_price = 0.0  # Mid price (bid + ask) / 2
        self.spread_abs = 0.0  # Absolute spread (ask - bid)
        self.spread_pct = 0.0  # Spread percentage
        self.available_balance = 0.0
        self.coin_balance = 0.0
        self.open_orders: List[Order] = []
        self.previous_order_ids: set = set()  # Track previous orders to detect fills
        self._startup_sync_done = False  # Track if startup sync has been performed
        self._last_sync_time = 0.0  # Track when we last synced missing orders (timestamp)
        self._sync_interval = 600  # Sync every 10 minutes (600 seconds) during normal operation
        self.average_entry_price = 0.0
        self.total_invested = 0.0
        self.total_coins = 0.0

        # Trading-cycle log buffer: one timestamp at cycle start (multi-line body), one at end
        self._cycle_logging = False
        self._cycle_log_buffer: List[str] = []
        
        # Track previous balance to verify fills
        self.previous_coin_balance = 0.0
        self.previous_available_balance = 0.0
        
        # Track last values for sell ladder recalculation
        self._last_avg_entry = 0.0
        self._last_coin_amount = 0.0  # Legacy - kept for backward compatibility
        self._last_core_coins = 0.0  # Core position tracking
        self._last_working_price = 0.0  # Working ladder price tracking
        
        # Track last committed buy amount for buy ladder resize detection
        self._last_buy_committed = 0.0
        # Track when increased capital was first detected (for observation period safety delay)
        self._increased_capital_detected_at = None
        
        # Local storage for filled buy orders
        self.filled_orders_file = os.path.join(self._state_dir, f"filled_orders_{self.cointype}.json")
        
        # Long-term performance tracking (signals / copy-trade reporting)
        self._performance_tracker = None
        if get_tracker:
            try:
                self._performance_tracker = get_tracker(self.symbol, self.cointype, self.config)
            except Exception as e:
                logging.debug(f"{self.symbol}: Performance tracker not used: {e}")
        
        # Load previously stored filled orders at startup
        self._load_filled_orders()
        
        # Price elevation tracking for capital preservation during high prices
        self.price_elevation_config = config.get('price_elevation', {})
        self.price_elevation_enabled = self.price_elevation_config.get('enabled', False)
        self.price_elevation_window_hours = self.price_elevation_config.get('window_hours', 4320)  # 180 days default
        # Mean-reversion tiers matched by max_percentile (upper percentile bound).
        # Cheaper (lower percentile) = more aggressive (higher allocation, fewer skips).
        self.price_elevation_tiers = self.price_elevation_config.get('tiers', [
            {'max_percentile': 20, 'allocation': 100, 'skip_levels': 0},
            {'max_percentile': 40, 'allocation': 85, 'skip_levels': 1},
            {'max_percentile': 60, 'allocation': 65, 'skip_levels': 1},
            {'max_percentile': 80, 'allocation': 40, 'skip_levels': 1},
            {'max_percentile': 100, 'allocation': 20, 'skip_levels': 1},
        ])
        self._load_position_aware_config()
        self._load_entry_aware_config()
        self.rolling_price_high = 0.0  # Highest price in the window (context/logging)
        self.rolling_price_low = 0.0  # Lowest price in the window (context/logging)
        self.rolling_price_mean = 0.0  # Mean price in the window (fair-value reference)
        self.price_percentile = 0.0  # Current price's percentile rank within the window distribution
        self.price_high_history = []  # List of (timestamp, price) for the rolling distribution
        self._price_elevation_file = os.path.join(
            self._state_dir, f"price_high_{self.cointype}.json"
        )  # Persist price history across restarts
        self._price_high_save_counter = 0  # Counter for periodic saves
        self._price_high_file_metadata: Dict = {}
        if self.price_elevation_enabled:
            self._init_price_high_from_disk()
        
        # Skim functionality
        self.skim_config = config.get('skim', {})
        self.skim_enabled = self.skim_config.get('enabled', False)
        self.skim_profit_percentage = self.skim_config.get('profit_percentage', 0.15)
        self.skim_tokens = self.skim_config.get('tokens', [])
        self.skim_min_amount = self.skim_config.get('min_skim_amount', 5.0)
        
        # Daily profit tracking for skim
        self.daily_realized_profit = 0.0  # Running total for current day
        self.last_reset_date = None  # Track when we last reset (for daily reset)
        
        # Track daily skim purchases: {cointype: {'amount': float, 'total_cost': float, 'purchases': int}}
        self.daily_skim_purchases = {}
        
        # File to persist skim purchases for daily summary
        self.skim_purchases_file = os.path.join(self._state_dir, f"skim_purchases_{self.cointype}.json")
        
        # Mean-reversion strategy: Core/Working position split
        mean_reversion_config = config['trading'].get('mean_reversion', {})
        self.mean_reversion_enabled = mean_reversion_config.get('enabled', False)
        self.working_position_pct = mean_reversion_config.get('working_position_pct', 0.15)  # Default 15% working
        self.working_sell_levels = mean_reversion_config.get('working_sell_levels', [1.0, 2.0, 3.0, 4.0, 5.0])
        self.cancel_working_threshold_pct = mean_reversion_config.get('cancel_working_threshold_pct', 5.0)  # Default 5% below entry
        # Working ladder price update threshold (default 3.0% — config working_price_update_threshold)
        # When price moves by this amount, Working orders are cancelled and recreated at new levels.
        # Set above the first working_sell_levels (e.g. 1.5%, 1.75%) so near-price rungs can fill
        # before the ladder recenters.
        self.working_price_update_threshold = mean_reversion_config.get('working_price_update_threshold', 5.0)
        self.working_ladder_enabled = mean_reversion_config.get('working_ladder_enabled', False)
        
        # Sell ladder rebalance: when available coins exceed this fraction of committed, rebalance Core/Working ladders
        self.sell_ladder_rebalance_threshold = config['trading'].get('sell_ladder_rebalance_threshold_pct', 5.0) / 100.0  # Default 5%
        
        # Validate working position percentage
        if self.mean_reversion_enabled:
            if not (0.0 < self.working_position_pct < 1.0):
                logging.warning(f"{self.symbol}: Invalid working_position_pct ({self.working_position_pct}), must be between 0 and 1. Disabling mean-reversion.")
                self.mean_reversion_enabled = False
            elif self.working_position_pct > 0.5:
                logging.warning(f"{self.symbol}: Working position ({self.working_position_pct*100:.1f}%) is >50% - this may be too aggressive. Consider reducing.")
        
        # Track Core vs Working position allocation
        self.core_coins = 0.0
        self.working_coins = 0.0
        
        # Track Working order IDs to distinguish from Core orders
        # This is necessary because Working orders are placed relative to current_price,
        # so we can't identify them by price-matching after price moves
        # Persisted to file to survive bot restarts
        self._working_orders_file = os.path.join(self._state_dir, f"working_orders_{self.cointype}.json")
        self._working_order_ids: set = set()
        self._load_working_order_ids()

        if self.mean_reversion_enabled:
            self._info(
                f"{self.symbol}: Mean-reversion enabled — "
                f"working_ladder={'on' if self.working_ladder_enabled else 'off'}, "
                f"working_position_pct={self.working_position_pct * 100:.2f}%"
            )
    
    def update_config(self, config: Dict):
        """Update configuration values from a new config dict (hot reload support)"""
        self.config = config
        
        # Update trading parameters
        self.buy_levels = config['trading']['buy_levels']
        self.sell_levels = config['trading']['sell_levels']
        self.balance_percentage = config['trading']['balance_percentage_per_symbol']
        self.min_order_size = config['trading']['min_order_size']
        self.price_update_threshold = config['trading']['price_update_threshold']
        self.buy_ladder_rebuild_on_fill_pct = float(
            config['trading'].get('buy_ladder_rebuild_on_fill_pct', 3.0)
        )
        self.price_update_time_window = config['trading'].get('price_update_time_window', 3600)
        self.price_update_short_window = config['trading'].get('price_update_short_window', 1800)
        self.price_update_quick_window = config['trading'].get('price_update_quick_window', 600)
        self.price_update_quick_threshold = config['trading'].get('price_update_quick_threshold', 5.0)
        # Support separate buy/sell distribution settings (with fallback to legacy setting)
        legacy_distribution = config['trading'].get('order_size_distribution', 'weighted')
        self.buy_order_distribution = config['trading'].get('buy_order_distribution', legacy_distribution)
        self.sell_order_distribution = config['trading'].get('sell_order_distribution', legacy_distribution)
        self.order_size_distribution = legacy_distribution  # Keep for backward compatibility
        self.small_position_multiplier = config['trading'].get('small_position_multiplier', 1.0)
        self.small_position_threshold = config['trading'].get('small_position_threshold', 999.0)
        self.increased_capital_threshold = config['trading'].get('increased_capital_threshold', 500.0)
        self.increased_capital_observation_period = config['trading'].get('increased_capital_observation_period', 3600)
        self.max_buy_ladder_usdc = float(config['trading'].get('max_buy_ladder_usdc', 0) or 0)
        
        # Update trading fees
        self.buy_fee = config['trading'].get('buy_fee_percentage', 0.01)
        self.sell_fee = config['trading'].get('sell_fee_percentage', 0.01)
        self.total_fee = self.buy_fee + self.sell_fee
        
        # Update safety limits
        self.max_buy_orders = config['safety']['max_buy_orders']
        self.max_sell_orders = config['safety']['max_sell_orders']
        # Independent limits for Core and Working (if specified, otherwise use split logic)
        self.max_core_sell_orders = config['safety'].get('max_core_sell_orders', None)
        self.max_working_sell_orders = config['safety'].get('max_working_sell_orders', None)
        self.max_position_size = config['safety']['max_position_size']
        
        # Update price elevation settings
        self.price_elevation_config = config.get('price_elevation', {})
        self.price_elevation_enabled = self.price_elevation_config.get('enabled', False)
        self.price_elevation_window_hours = self.price_elevation_config.get('window_hours', 4320)
        self.price_elevation_tiers = self.price_elevation_config.get('tiers', [
            {'max_percentile': 20, 'allocation': 100, 'skip_levels': 0},
            {'max_percentile': 40, 'allocation': 85, 'skip_levels': 1},
            {'max_percentile': 60, 'allocation': 65, 'skip_levels': 1},
            {'max_percentile': 80, 'allocation': 40, 'skip_levels': 1},
            {'max_percentile': 100, 'allocation': 20, 'skip_levels': 1},
        ])
        self._load_position_aware_config()
        self._load_entry_aware_config()
        if self.price_elevation_enabled and not self.price_high_history:
            self._init_price_high_from_disk()
        
        # Update skim settings
        self.skim_config = config.get('skim', {})
        self.skim_enabled = self.skim_config.get('enabled', False)
        self.skim_profit_percentage = self.skim_config.get('profit_percentage', 0.15)
        self.skim_tokens = self.skim_config.get('tokens', [])
        self.skim_min_amount = self.skim_config.get('min_skim_amount', 5.0)
        
        # Update mean-reversion settings
        mean_reversion_config = config['trading'].get('mean_reversion', {})
        self.mean_reversion_enabled = mean_reversion_config.get('enabled', False)
        self.working_position_pct = mean_reversion_config.get('working_position_pct', 0.15)
        self.working_sell_levels = mean_reversion_config.get('working_sell_levels', [1.0, 2.0, 3.0, 4.0, 5.0])
        self.cancel_working_threshold_pct = mean_reversion_config.get('cancel_working_threshold_pct', 5.0)
        # Working ladder price update threshold (default 3.0% — config working_price_update_threshold)
        # When price moves by this amount, Working orders are cancelled and recreated at new levels.
        # Set above the first working_sell_levels (e.g. 1.5%, 1.75%) so near-price rungs can fill
        # before the ladder recenters.
        self.working_price_update_threshold = mean_reversion_config.get('working_price_update_threshold', 5.0)
        self.working_ladder_enabled = mean_reversion_config.get('working_ladder_enabled', False)
        self.sell_ladder_rebalance_threshold = config['trading'].get('sell_ladder_rebalance_threshold_pct', 5.0) / 100.0
        
        self._info(f"{self.symbol}: Configuration updated (hot reload)")
    
    def _info(self, message: str, *args) -> None:
        """Log info, or buffer during an active trading cycle (see _flush_cycle_log)."""
        text = message % args if args else message
        if self._cycle_logging:
            self._cycle_log_buffer.append(text)
        else:
            logging.info(text)

    def _begin_cycle_log(self) -> None:
        self._cycle_logging = True
        self._cycle_log_buffer = []

    def _flush_cycle_log(self, suffix: str = "") -> None:
        """Emit buffered cycle lines under one timestamp, then a separate END line."""
        self._cycle_logging = False
        end_line = f"{self.symbol}: ========== END TRADING CYCLE{suffix} =========="
        if self._cycle_log_buffer:
            logging.info("\n".join(self._cycle_log_buffer))
        logging.info(end_line)
        self._cycle_log_buffer = []

    def _get_filled_orders_file_path(self) -> str:
        """Get the full path to the filled orders file"""
        return self.filled_orders_file
    
    def _get_working_orders_file_path(self) -> str:
        """Get the full path to the working orders file"""
        return self._working_orders_file
    
    def _load_working_order_ids(self):
        """Load Working order IDs from JSON file to survive bot restarts"""
        file_path = self._get_working_orders_file_path()
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    order_ids = data.get('working_order_ids', [])
                    self._working_order_ids = set(order_ids)
                    if order_ids:
                        self._info(f"{self.symbol}: Loaded {len(order_ids)} Working order IDs from {self._working_orders_file}")
            except Exception as e:
                logging.warning(f"{self.symbol}: Failed to load Working order IDs from {file_path}: {e}")
                self._working_order_ids = set()
        else:
            self._working_order_ids = set()
    
    def _save_working_order_ids(self):
        """Save Working order IDs to JSON file for persistence across restarts"""
        file_path = self._get_working_orders_file_path()
        try:
            data = {
                'working_order_ids': list(self._working_order_ids),
                'last_updated': datetime.now(timezone.utc).isoformat(),
                'symbol': self.symbol
            }
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
            logging.debug(f"{self.symbol}: Saved {len(self._working_order_ids)} Working order IDs to {self._working_orders_file}")
        except Exception as e:
            logging.warning(f"{self.symbol}: Failed to save Working order IDs to {file_path}: {e}")

    def _replace_tracked_order_id(self, old_id: str, new_id: str, new_rate: float = None) -> None:
        """After cancel+replace edit, keep fill/Working tracking on the new exchange id."""
        if not old_id:
            return
        old_id = str(old_id)
        new_id = str(new_id) if new_id else old_id
        if old_id == new_id:
            if new_rate is not None:
                for o in self.open_orders:
                    if o.order_id == old_id:
                        o.rate = new_rate
            return
        if hasattr(self, 'previous_order_ids') and self.previous_order_ids is not None:
            if old_id in self.previous_order_ids:
                self.previous_order_ids.discard(old_id)
                self.previous_order_ids.add(new_id)
        if old_id in self._working_order_ids:
            self._working_order_ids.discard(old_id)
            self._working_order_ids.add(new_id)
            self._save_working_order_ids()
        for o in self.open_orders:
            if o.order_id == old_id:
                o.order_id = new_id
                if new_rate is not None:
                    o.rate = new_rate

    def _nearest_ladder_level_pct(
        self, order_rate: float, levels: list, base_price: float, *, is_buy: bool
    ) -> Optional[float]:
        """Map an open order to the closest configured ladder level by target price."""
        if order_rate <= 0 or base_price <= 0 or not levels:
            return None
        best_level = None
        best_diff = float("inf")
        for level_pct in levels:
            if is_buy:
                target = base_price * (1 - float(level_pct) / 100)
            else:
                target = base_price * (1 + float(level_pct) / 100)
            if target <= 0:
                continue
            diff = abs(target - order_rate) / order_rate
            if diff < best_diff:
                best_diff = diff
                best_level = float(level_pct)
        if best_level is None or best_diff > 0.15:
            return None
        return best_level
    
    def _get_filled_working_orders_file_path(self) -> str:
        """Get path to filled working orders history JSON file"""
        return os.path.join(self._state_dir, f"filled_working_orders_{self.cointype}.json")
    
    def _save_filled_working_order_id(self, order_id: str):
        """Save a filled Working order ID to history file for reporting purposes
        
        This allows daily_summary and other reports to identify which historical
        sell orders were Working orders (mean-reversion) vs Core orders (recovery).
        """
        file_path = self._get_filled_working_orders_file_path()
        
        # Load existing data
        existing_ids = set()
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    existing_ids = set(data.get('working_order_ids', []))
            except Exception as e:
                logging.warning(f"{self.symbol}: Failed to load filled working orders: {e}")
        
        # Add the new order ID
        existing_ids.add(order_id)
        
        # Save back
        try:
            data = {
                'working_order_ids': list(existing_ids),
                'last_updated': datetime.now(timezone.utc).isoformat(),
                'symbol': self.symbol,
                'description': 'Historical Working order IDs for reporting (mean-reversion fills)'
            }
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
            logging.debug(f"{self.symbol}: Saved filled Working order {order_id} to history")
        except Exception as e:
            logging.warning(f"{self.symbol}: Failed to save filled working order ID: {e}")
    
    def _load_filled_orders(self) -> List[Dict]:
        """Load previously stored filled buy orders from JSON file"""
        file_path = self._get_filled_orders_file_path()
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    orders = data.get('buy_orders', [])
                    logging.debug(f"{self.symbol}: Loaded {len(orders)} stored filled buy orders from {self.filled_orders_file}")
                    return orders
            except Exception as e:
                logging.warning(f"{self.symbol}: Failed to load filled orders from {file_path}: {e}")
        return []
    
    def _is_order_already_in_json(self, order: Order, fill_timestamp: str = None) -> bool:
        """Check if an order already exists in the JSON file by matching amount, rate, and timestamp
        
        Args:
            order: Order object to check
            fill_timestamp: Optional fill timestamp to match against
            
        Returns:
            True if a similar order already exists in JSON, False otherwise
        """
        file_path = self._get_filled_orders_file_path()
        
        # Load the appropriate orders list based on order side
        if order.side == 'buy':
            stored_orders = self._load_filled_orders()
        else:  # sell
            # Load sell orders from JSON
            stored_orders = []
            if os.path.exists(file_path):
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                        stored_orders = data.get('sell_orders', [])
                except Exception as e:
                    logging.warning(f"{self.symbol}: Failed to load sell orders for duplicate check: {e}")
                    return False
        
        order_id = order.order_id
        order_amount = float(order.amount)
        order_rate = float(order.rate)
        
        # FIRST: Check by order_id (most reliable - same order_id = same order)
        # Also check api_order_id field (sync-saved records store exchange order ID there)
        for stored_order in stored_orders:
            stored_order_id = stored_order.get('order_id', '')
            stored_api_order_id = stored_order.get('api_order_id', '')
            if order_id and (
                (stored_order_id and stored_order_id == order_id) or
                (stored_api_order_id and stored_api_order_id == order_id)
            ):
                logging.debug(f"{self.symbol}: Order {order_id} already exists in JSON (matched by order_id/api_order_id against {stored_order.get('order_id')})")
                return True
        
        # For sell orders without a known fill_timestamp, avoid treating different
        # fills at the same ladder level (same amount/rate) as duplicates.
        # In that case we only dedupe by order_id/api_order_id above.
        if order.side == 'sell' and not fill_timestamp:
            return False
        
        # SECOND: If order_id doesn't match, check by amount+rate+timestamp (for cases where order_id might differ)
        # If we have a fill timestamp, use it for matching
        # Otherwise, we'll match on amount/rate only (less strict)
        use_timestamp = fill_timestamp is not None
        
        for stored_order in stored_orders:
            stored_amount = float(stored_order.get('amount', 0))
            stored_rate = float(stored_order.get('rate', 0))
            stored_timestamp = stored_order.get('fill_timestamp', '')
            
            # Match if amount and rate are close (1% for amount, 0.1% for rate)
            # Placed vs filled amounts can differ by ~0.1% due to exchange fee adjustments,
            # so we use 1% tolerance as headroom.
            amount_match = stored_amount > 0 and abs(order_amount - stored_amount) / stored_amount < 0.01
            rate_match = stored_rate > 0 and abs(order_rate - stored_rate) / stored_rate < 0.001
            
            # Check timestamp if available (within 10 minutes)
            timestamp_match = True
            if use_timestamp and fill_timestamp and stored_timestamp:
                try:
                    ts1 = fill_timestamp.replace('Z', '+00:00')
                    ts2 = stored_timestamp.replace('Z', '+00:00')
                    dt1 = datetime.fromisoformat(ts1)
                    dt2 = datetime.fromisoformat(ts2)
                    
                    # Make sure both are timezone-aware
                    if dt1.tzinfo is None:
                        dt1 = dt1.replace(tzinfo=timezone.utc)
                    if dt2.tzinfo is None:
                        dt2 = dt2.replace(tzinfo=timezone.utc)
                    
                    time_diff = abs((dt1 - dt2).total_seconds())
                    timestamp_match = time_diff < 600  # 10 minutes
                except (ValueError, AttributeError, TypeError):
                    # If we can't parse timestamps, just match on amount/rate
                    timestamp_match = True
            
            if amount_match and rate_match and timestamp_match:
                logging.debug(f"{self.symbol}: Order {order_id} already exists in JSON (matched by amount+rate+timestamp: {stored_order.get('order_id')})")
                return True
        
        return False

    def _find_stored_buy_row_for_order(self, order: Order) -> Optional[dict]:
        """Return the buy_orders JSON row matching this fill, if any."""
        order_id = str(order.order_id) if order.order_id else ""
        stored_orders = self._load_filled_orders()
        for row in stored_orders:
            if order_id and (
                str(row.get("order_id", "")) == order_id
                or str(row.get("api_order_id", "")) == order_id
            ):
                return row
        order_amount = float(order.amount)
        order_rate = float(order.rate)
        for row in stored_orders:
            stored_amount = float(row.get("amount", 0))
            stored_rate = float(row.get("rate", 0))
            amount_match = stored_amount > 0 and abs(order_amount - stored_amount) / stored_amount < 0.01
            rate_match = stored_rate > 0 and abs(order_rate - stored_rate) / stored_rate < 0.001
            if amount_match and rate_match:
                return row
        return None

    def _buy_fill_slack_was_sent(self, order: Order) -> bool:
        row = self._find_stored_buy_row_for_order(order)
        return bool(row and row.get("slack_notified"))

    def _mark_buy_fill_slack_notified(self, order: Order) -> None:
        """Persist slack_notified on the matching buy_orders row (no-op if not in ledger yet)."""
        file_path = self._get_filled_orders_file_path()
        if not os.path.exists(file_path):
            return
        order_id = str(order.order_id) if order.order_id else ""
        try:
            with open(file_path, encoding="utf-8") as f:
                data = json.load(f)
            buys = data.get("buy_orders") or []
            updated = False
            for row in buys:
                if order_id and (
                    str(row.get("order_id", "")) == order_id
                    or str(row.get("api_order_id", "")) == order_id
                ):
                    row["slack_notified"] = True
                    updated = True
                    break
            if not updated:
                match = self._find_stored_buy_row_for_order(order)
                if match:
                    match_id = match.get("order_id")
                    for r in buys:
                        if r.get("order_id") == match_id:
                            r["slack_notified"] = True
                            updated = True
                            break
            if updated:
                data["buy_orders"] = buys
                data["last_updated"] = datetime.utcnow().isoformat()
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
        except Exception as e:
            logging.debug(f"{self.symbol}: Could not mark slack_notified: {e}")

    def _notify_buy_fill_if_needed(
        self, order: Order, amount: float, rate: float
    ) -> None:
        if self._buy_fill_slack_was_sent(order):
            logging.debug(
                f"{self.symbol}: Buy fill Slack already sent for order {order.order_id}"
            )
            return
        self.notifier.notify_order_filled(
            symbol=self.symbol,
            side="buy",
            amount=amount,
            rate=rate,
            avg_entry=None,
        )
        self._mark_buy_fill_slack_notified(order)

    def _save_filled_order(self, order: Order, fill_timestamp: str = None) -> bool:
        """Save a filled buy order to local JSON file
        
        IMPORTANT: This method should ONLY be called for orders that have been
        verified as filled (balance changed as expected). It should NEVER be
        called for orders that were just placed or cancelled.
        
        The JSON file should only contain prices from tokens we've actually
        bought, not orders placed until we buy those tokens.
        """
        if order.side != 'buy':
            return False  # Only save buy orders for average entry calculation
        
        # Additional safety check: verify the order is not in open_orders
        # If it's still open, it hasn't been filled yet and shouldn't be saved
        if hasattr(self, 'open_orders') and self.open_orders:
            if any(o.order_id == order.order_id for o in self.open_orders):
                logging.warning(f"{self.symbol}: ⚠️ Attempted to save order {order.order_id} to JSON, but it's still in open_orders. "
                              f"This order has NOT been filled yet - skipping save to prevent inaccurate prices.")
                return False
        
        file_path = self._get_filled_orders_file_path()
        
        # Load existing orders
        existing_orders = self._load_filled_orders()
        
        order_id = order.order_id
        current_balance = self.coin_balance
        fill_ts = fill_timestamp or datetime.utcnow().isoformat()
        
        # ENHANCED: Check for other orders with the same balance saved recently (within 30 seconds)
        # This catches cases where multiple orders were incorrectly detected as filled in the same cycle
        recent_same_balance_orders = []
        try:
            fill_dt = datetime.fromisoformat(fill_ts.replace('Z', '+00:00'))
            if fill_dt.tzinfo is None:
                fill_dt = fill_dt.replace(tzinfo=timezone.utc)
            
            for existing_order in existing_orders:
                existing_balance = existing_order.get('coin_balance_at_save')
                if existing_balance is not None and abs(float(existing_balance) - float(current_balance)) < 0.00000001:
                    # Same balance - check if timestamp is within 30 seconds
                    existing_timestamp = existing_order.get('fill_timestamp', '')
                    if existing_timestamp:
                        try:
                            existing_dt = datetime.fromisoformat(existing_timestamp.replace('Z', '+00:00'))
                            if existing_dt.tzinfo is None:
                                existing_dt = existing_dt.replace(tzinfo=timezone.utc)
                            time_diff = abs((fill_dt - existing_dt).total_seconds())
                            if time_diff < 30:  # Within 30 seconds
                                recent_same_balance_orders.append(existing_order)
                        except (ValueError, AttributeError, TypeError):
                            pass
        except (ValueError, AttributeError, TypeError):
            pass
        
        # If there are other orders with the same balance saved recently, verify this order exists in API
        # This prevents saving orders that don't actually exist in the completed orders API
        if recent_same_balance_orders:
            logging.warning(
                f"{self.symbol}: ⚠️ Found {len(recent_same_balance_orders)} other order(s) with same balance ({current_balance:.8f}) "
                f"saved within 30 seconds. Verifying order {order_id} exists in completed orders API before saving..."
            )
            
            # Verify this order exists in the completed orders API
            if not self._verify_order_via_api(order):
                logging.error(
                    f"{self.symbol}: ❌ Order {order_id} ({order.amount:.8f} @ {order.rate:.4f}) NOT found in completed orders API. "
                    f"This order may not have actually filled. Skipping save to prevent inaccurate tracking."
                )
                return False
            
            self._info(f"{self.symbol}: ✓ Order {order_id} verified in completed orders API, proceeding with save")
        
        # Check for duplicates - allow same order_id if it's a partial fill (different balance)
        # but prevent duplicates where same order_id AND same balance (exact duplicate)
        for existing_order in existing_orders:
            existing_order_id = existing_order.get('order_id')
            existing_balance = existing_order.get('coin_balance_at_save')
            
            # If same order_id AND same balance, it's an exact duplicate (skip)
            if existing_order_id == order_id:
                if existing_balance is not None and abs(float(existing_balance) - float(current_balance)) < 0.00000001:
                    logging.debug(f"{self.symbol}: Order {order_id} already stored with same balance ({current_balance:.8f}), skipping duplicate")
                    return False
                # Same order_id but different balance - likely a partial fill, allow it
                continue
            
            # Check for duplicate by coin_balance_at_save - if balance didn't change, it's likely a duplicate
            # This catches cases where the same order was saved with different IDs
            if existing_balance is not None and abs(float(existing_balance) - float(current_balance)) < 0.00000001:
                # Same balance - check if amount/rate/timestamp are similar (likely duplicate)
                existing_amount = float(existing_order.get('amount', 0))
                existing_rate = float(existing_order.get('rate', 0))
                existing_timestamp = existing_order.get('fill_timestamp', '')
                
                amount_match = existing_amount > 0 and abs(order.amount - existing_amount) / existing_amount < 0.01  # 1% tolerance
                rate_match = existing_rate > 0 and abs(order.rate - existing_rate) / existing_rate < 0.001  # 0.1% tolerance (increased from 0.01% to handle API rounding)
                
                # Check timestamp within 10 minutes
                timestamp_match = True
                if existing_timestamp:
                    try:
                        ts1 = fill_ts.replace('Z', '+00:00')
                        ts2 = existing_timestamp.replace('Z', '+00:00')
                        dt1 = datetime.fromisoformat(ts1)
                        dt2 = datetime.fromisoformat(ts2)
                        if dt1.tzinfo is None:
                            dt1 = dt1.replace(tzinfo=timezone.utc)
                        if dt2.tzinfo is None:
                            dt2 = dt2.replace(tzinfo=timezone.utc)
                        time_diff = abs((dt1 - dt2).total_seconds())
                        timestamp_match = time_diff < 600  # 10 minutes
                    except (ValueError, AttributeError, TypeError):
                        timestamp_match = True
                
                if amount_match and rate_match and timestamp_match:
                    logging.warning(f"{self.symbol}: Duplicate order detected - same coin_balance_at_save ({current_balance:.8f}) "
                                  f"and similar amount/rate/timestamp. Existing: {existing_order_id}, "
                                  f"New: {order_id}. Skipping duplicate.")
                    return False
        
        # Create order record
        # NOTE: We use the order.rate (the price at which the order was placed)
        # For limit orders, this should be the fill price. If the order filled
        # at a different price, we'd need to get the actual fill price from the exchange.
        # CRITICAL: Also record the coin balance at the time of save to verify we actually have these tokens
        order_record = {
            'order_id': order_id,
            'symbol': order.symbol,
            'amount': order.amount,
            'rate': order.rate,
            'market': order.market,
            'fill_timestamp': fill_timestamp or datetime.utcnow().isoformat(),
            'total_usd': order.amount * order.rate,
            'coin_balance_at_save': self.coin_balance  # Record balance to verify we actually have these tokens
        }
        
        # Add to list
        existing_orders.append(order_record)
        
        # Save to file - preserve sell_orders and metadata if they exist
        try:
            # Load existing file to preserve other data
            existing_data = {}
            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    existing_data = json.load(f)
            
            data = {
                'symbol': self.symbol,
                'cointype': self.cointype,
                'market': self.market,
                'last_updated': datetime.utcnow().isoformat(),
                'buy_orders': existing_orders,
                'sell_orders': existing_data.get('sell_orders', []),  # Preserve sell_orders
                'metadata': existing_data.get('metadata', {})  # Preserve metadata
            }
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
            self._info(f"{self.symbol}: Saved filled buy order {order_id} to {self.filled_orders_file} "
                        f"({order.amount:.8f} @ {order.rate:.4f})")
            # Long-term performance ledger (signals / copy-trade reporting)
            if self._performance_tracker:
                try:
                    self._performance_tracker.record_trade(
                        side='buy',
                        order_id=order_id,
                        amount=order.amount,
                        rate=order.rate,
                        fill_timestamp=fill_ts,
                        total_usd=order.amount * order.rate,
                        profit_usd=None,
                    )
                except Exception as e:
                    logging.debug(f"{self.symbol}: Performance tracker record_trade (buy) failed: {e}")
            return True
        except Exception as e:
            logging.error(f"{self.symbol}: Failed to save filled order to {file_path}: {e}")
        return False
    
    def _save_filled_sell_order(self, order: Order, fill_timestamp: str = None):
        """Save a filled sell order to local JSON file and consume buy orders via LIFO
        
        When a sell order fills, we:
        1. Record the sell in the sell_orders array for tracking
        2. Mark the corresponding buy orders as consumed (LIFO - newest first)
        
        This maintains accurate LIFO accounting for average entry calculation.
        LIFO is used for mean-reversion trading to maximize profit and ensure each
        individual order is sold at a profit by selling lower-cost-basis coins first.
        """
        if order.side != 'sell':
            return  # Only save sell orders
        
        # Calculate realized profit for this sell (before we update JSON) for performance ledger
        sell_profit_usd, _ = self._calculate_sell_profit(order)
        
        file_path = self._get_filled_orders_file_path()
        
        # Load existing data
        existing_data = {}
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r') as f:
                    existing_data = json.load(f)
            except Exception as e:
                logging.warning(f"{self.symbol}: Failed to load JSON for sell tracking: {e}")
                return
        
        buy_orders = existing_data.get('buy_orders', [])
        sell_orders = existing_data.get('sell_orders', [])
        
        # Check if this order ID already exists (avoid duplicates)
        order_id = order.order_id
        if any(o.get('order_id') == order_id for o in sell_orders):
            logging.debug(f"{self.symbol}: Sell order {order_id} already tracked, skipping")
            return
        
        # Determine if this was a Working order (mean-reversion)
        working_order_ids = getattr(self, '_working_order_ids', set())
        is_working = order_id in working_order_ids
        
        # Create sell order record
        sell_record = {
            'order_id': order_id,
            'symbol': order.symbol,
            'amount': order.amount,
            'rate': order.rate,
            'market': order.market,
            'fill_timestamp': fill_timestamp or datetime.utcnow().isoformat(),
            'total_usd': order.amount * order.rate,
            'coin_balance_at_save': self.coin_balance,
            'is_working': is_working  # Track order type for reporting
        }
        
        # Add to sell orders list
        sell_orders.append(sell_record)
        
        # If this was a Working order, save to filled working orders history for reporting
        if is_working:
            self._save_filled_working_order_id(order_id)
            # Remove from active working order IDs since it's now filled
            self._working_order_ids.discard(order_id)
            self._save_working_order_ids()
            self._info(f"{self.symbol}: Working sell order {order_id} filled and recorded")
        
        # LIFO consumption: mark buy orders as consumed based on sold amount
        # We consume from the newest orders first (LIFO)
        # IMPROVED: Validate timestamps before sorting
        def get_timestamp_for_sort(order):
            """Get timestamp for sorting, with validation"""
            timestamp = order.get('fill_timestamp', '')
            if not timestamp:
                return '1970-01-01T00:00:00'  # Put orders without timestamps at the beginning
            try:
                # Validate timestamp format
                ts = timestamp.replace('Z', '+00:00')
                dt = datetime.fromisoformat(ts)
                return timestamp
            except (ValueError, AttributeError, TypeError):
                logging.warning(f"{self.symbol}: Invalid timestamp in buy order {order.get('order_id', 'unknown')}: {timestamp}")
                return '1970-01-01T00:00:00'  # Put invalid timestamps at the beginning
        
        buy_orders.sort(key=get_timestamp_for_sort, reverse=True)  # Newest first (LIFO)
        
        amount_to_consume = order.amount
        consumed_orders = []
        sell_timestamp = fill_timestamp or datetime.utcnow().isoformat()
        
        for buy_order in buy_orders:
            if amount_to_consume <= 0:
                break
            
            # IMPROVED: Only consume buys that occurred BEFORE the sell (LIFO timestamp validation)
            buy_timestamp = buy_order.get('fill_timestamp', '')
            if buy_timestamp and sell_timestamp:
                try:
                    buy_ts = buy_timestamp.replace('Z', '+00:00')
                    sell_ts = sell_timestamp.replace('Z', '+00:00')
                    buy_dt = datetime.fromisoformat(buy_ts)
                    sell_dt = datetime.fromisoformat(sell_ts)
                    if buy_dt.tzinfo is None:
                        buy_dt = buy_dt.replace(tzinfo=timezone.utc)
                    if sell_dt.tzinfo is None:
                        sell_dt = sell_dt.replace(tzinfo=timezone.utc)
                    
                    # Only consume if buy occurred before sell
                    if buy_dt >= sell_dt:
                        continue  # Skip buys that occurred after or at the same time as the sell
                except (ValueError, AttributeError, TypeError):
                    # If we can't parse timestamps, continue (better to consume than skip)
                    logging.debug(f"{self.symbol}: Could not parse timestamps for LIFO validation, continuing anyway")
                    pass
            
            buy_amount = float(buy_order.get('amount', 0))
            already_consumed = float(buy_order.get('consumed_amount', 0))
            remaining = buy_amount - already_consumed
            
            if remaining <= 0:
                continue  # Already fully consumed
            
            if remaining <= amount_to_consume:
                # Fully consume this order
                buy_order['consumed_amount'] = buy_amount
                buy_order['fully_consumed'] = True
                buy_order['consumed_by_sell'] = order_id
                consumed_orders.append((buy_order.get('order_id'), remaining))
                amount_to_consume -= remaining
            else:
                # Partially consume this order
                buy_order['consumed_amount'] = already_consumed + amount_to_consume
                consumed_orders.append((buy_order.get('order_id'), amount_to_consume))
                amount_to_consume = 0
        
        if consumed_orders:
            consumed_summary = ', '.join([f"{oid[:12]}...: {amt:.2f}" for oid, amt in consumed_orders[:3]])
            if len(consumed_orders) > 3:
                consumed_summary += f" (+{len(consumed_orders) - 3} more)"
            self._info(f"{self.symbol}: LIFO consumed {order.amount:.4f} coins from buy orders (newest first): {consumed_summary}")
        
        # Save updated data
        try:
            data = {
                'symbol': self.symbol,
                'cointype': self.cointype,
                'market': self.market,
                'last_updated': datetime.utcnow().isoformat(),
                'buy_orders': buy_orders,
                'sell_orders': sell_orders,
                'metadata': existing_data.get('metadata', {})
            }
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
            self._info(f"{self.symbol}: Tracked filled sell order {order_id} - "
                        f"sold {order.amount:.8f} @ {order.rate:.4f} = ${order.amount * order.rate:.2f}")
            # Long-term performance ledger (signals / copy-trade reporting)
            if self._performance_tracker:
                try:
                    self._performance_tracker.record_trade(
                        side='sell',
                        order_id=order_id,
                        amount=order.amount,
                        rate=order.rate,
                        fill_timestamp=fill_timestamp or datetime.utcnow().isoformat(),
                        total_usd=order.amount * order.rate,
                        profit_usd=sell_profit_usd,
                        is_working=is_working,
                    )
                except Exception as e:
                    logging.debug(f"{self.symbol}: Performance tracker record_trade (sell) failed: {e}")
        except Exception as e:
            logging.error(f"{self.symbol}: Failed to save sell order tracking: {e}")
        
        # Set flag to prevent sell ladder resize this cycle
        self._sell_filled_this_cycle = True
    
    def _reset_daily_profit_if_new_day(self):
        """Reset daily realized profit and skim purchases if it's a new day"""
        current_date = datetime.now(timezone.utc).date()
        if self.last_reset_date != current_date:
            if self.last_reset_date is not None:
                self._info(f"{self.symbol}: New day detected - resetting daily realized profit "
                            f"(previous: ${self.daily_realized_profit:.2f})")
            self.daily_realized_profit = 0.0
            self.last_reset_date = current_date
            self.daily_skim_purchases = {}  # Reset daily skim purchases tracking
    
    def _save_skim_purchase(self, cointype: str, amount: float, total_cost: float, market: str):
        """Save a skim purchase to JSON file for daily summary tracking"""
        file_path = os.path.join(os.path.dirname(__file__), self.skim_purchases_file)
        
        # Load existing data
        existing_data = {}
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r') as f:
                    existing_data = json.load(f)
            except Exception as e:
                logging.warning(f"{self.symbol}: Failed to load skim purchases: {e}")
        
        # Get current date
        current_date = datetime.now(timezone.utc).date().isoformat()
        
        # Initialize date entry if needed
        if 'daily_purchases' not in existing_data:
            existing_data['daily_purchases'] = {}
        
        if current_date not in existing_data['daily_purchases']:
            existing_data['daily_purchases'][current_date] = {}
        
        # Add or update token entry for this date
        if cointype not in existing_data['daily_purchases'][current_date]:
            existing_data['daily_purchases'][current_date][cointype] = {
                'amount': 0.0,
                'total_cost': 0.0,
                'purchases': 0,
                'market': market
            }
        
        # Update totals
        existing_data['daily_purchases'][current_date][cointype]['amount'] += amount
        existing_data['daily_purchases'][current_date][cointype]['total_cost'] += total_cost
        existing_data['daily_purchases'][current_date][cointype]['purchases'] += 1
        existing_data['daily_purchases'][current_date][cointype]['market'] = market
        
        # Update metadata
        existing_data['last_updated'] = datetime.now(timezone.utc).isoformat()
        existing_data['symbol'] = self.symbol
        existing_data['cointype'] = self.cointype
        
        # Save to file
        try:
            with open(file_path, 'w') as f:
                json.dump(existing_data, f, indent=2)
            logging.debug(f"{self.symbol}: Saved skim purchase to {self.skim_purchases_file}")
        except Exception as e:
            logging.warning(f"{self.symbol}: Failed to save skim purchase: {e}")
    
    def _calculate_sell_profit(self, sell_order: Order) -> Tuple[float, float]:
        """Calculate realized profit for a specific sell order using LIFO
        
        Uses ALL non-consumed buy orders (not balance-trimmed) for LIFO matching.
        This avoids double-counting: consumed_amount already tracks which buys were
        sold via LIFO, so we must not also trim newest buys from the set.
        
        Returns:
            Tuple of (net_profit_usd, total_cost_basis). Net profit is after fees.
            total_cost_basis is the cost of the consumed buys (for notification:
            avg_entry = total_cost_basis / sell_order.amount).
        """
        stored_orders = self._load_filled_orders()
        if not stored_orders:
            return (0.0, 0.0)
        
        # Filter to active (non-consumed) orders with remaining balance
        active_orders = []
        for order in stored_orders:
            if order.get('fully_consumed', False):
                continue
            original_amount = float(order.get('amount', 0))
            consumed_amount = float(order.get('consumed_amount', 0))
            remaining = original_amount - consumed_amount
            if remaining > 0.0001:
                order_copy = order.copy()
                order_copy['_remaining_amount'] = remaining
                active_orders.append(order_copy)
        
        if not active_orders:
            return (0.0, 0.0)
        
        # Sort by parsed datetime (newest first) for LIFO
        def _ts_key(o):
            ts = o.get('fill_timestamp', '') or ''
            if not ts:
                return datetime.min.replace(tzinfo=timezone.utc)
            try:
                s = ts.replace('Z', '+00:00').strip()
                dt = datetime.fromisoformat(s)
                return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            except (ValueError, TypeError, AttributeError):
                return datetime.min.replace(tzinfo=timezone.utc)
        active_orders = sorted(active_orders, key=_ts_key, reverse=True)
        
        amount_to_match = sell_order.amount
        total_cost_basis = 0.0
        
        for buy_order in active_orders:
            if amount_to_match <= 0:
                break
            
            remaining = float(buy_order.get('_remaining_amount', 0))
            if remaining <= 0:
                continue
            
            consume_amount = min(remaining, amount_to_match)
            buy_rate = float(buy_order.get('rate', 0))
            cost_basis = consume_amount * buy_rate
            total_cost_basis += cost_basis
            amount_to_match -= consume_amount
        
        sell_proceeds = sell_order.amount * sell_order.rate
        buy_fee = total_cost_basis * self.buy_fee
        sell_fee = sell_proceeds * self.sell_fee
        net_profit = sell_proceeds - total_cost_basis - buy_fee - sell_fee
        
        return (net_profit, total_cost_basis)
    
    def _calculate_lifo_profit_for_pending_sell(self, sell_amount: float, sell_rate: float) -> tuple:
        """Calculate projected profit for a pending sell order using LIFO matching
        
        Uses ALL non-consumed buy orders (not balance-trimmed) for LIFO matching.
        This avoids double-counting: consumed_amount already tracks which buys were
        sold via LIFO, so we must not also trim newest buys from the set.
        
        Args:
            sell_amount: Amount of coins to sell
            sell_rate: Price per coin for the sell
            
        Returns:
            Tuple of (net_profit, net_profit_pct, cost_basis, total_investment)
            Returns (0, 0, 0, 0) if unable to calculate
        """
        stored_orders = self._load_filled_orders()
        if not stored_orders:
            return (0.0, 0.0, 0.0, 0.0)
        
        # Filter to active (non-consumed) orders
        active_orders = []
        for order in stored_orders:
            if order.get('fully_consumed', False):
                continue
            original_amount = float(order.get('amount', 0))
            consumed_amount = float(order.get('consumed_amount', 0))
            remaining = original_amount - consumed_amount
            if remaining > 0.0001:
                order_copy = order.copy()
                order_copy['_remaining_amount'] = remaining
                active_orders.append(order_copy)
        
        if not active_orders:
            return (0.0, 0.0, 0.0, 0.0)
        
        # Sort by timestamp newest first for LIFO (newest coins sold first)
        active_orders = sorted(active_orders, key=lambda x: x.get('fill_timestamp', ''), reverse=True)
        
        amount_to_match = sell_amount
        total_cost_basis = 0.0
        
        for buy_order in active_orders:
            if amount_to_match <= 0:
                break
            
            remaining = float(buy_order.get('_remaining_amount', 0))
            if remaining <= 0:
                continue
            
            buy_rate = float(buy_order.get('rate', 0))
            consume_amount = min(remaining, amount_to_match)
            cost_basis = consume_amount * buy_rate
            total_cost_basis += cost_basis
            amount_to_match -= consume_amount
        
        # Remainder only if we ran out of buy orders (e.g. sell amount > total tracked buys).
        # Use sell_rate so that portion is break-even in display.
        if amount_to_match > 0.0001:
            total_cost_basis += amount_to_match * sell_rate
        
        # Calculate profit
        sell_proceeds = sell_amount * sell_rate
        buy_fee = total_cost_basis * self.buy_fee
        sell_fee = sell_proceeds * self.sell_fee
        net_profit = sell_proceeds - total_cost_basis - buy_fee - sell_fee
        
        # Calculate profit percentage relative to total investment (cost basis + buy fee)
        total_investment = total_cost_basis + buy_fee
        net_profit_pct = (net_profit / total_investment * 100) if total_investment > 0 else 0.0
        
        return (net_profit, net_profit_pct, total_cost_basis, total_investment)
    
    def _get_active_lifo_buy_queue(self) -> List[Dict]:
        """Mutable LIFO buy queue (newest first) from unconsumed stored buy orders."""
        stored_orders = self._load_filled_orders()
        if not stored_orders:
            return []
        active_orders = []
        for order in stored_orders:
            if order.get('fully_consumed', False):
                continue
            original_amount = float(order.get('amount', 0))
            consumed_amount = float(order.get('consumed_amount', 0))
            remaining = original_amount - consumed_amount
            if remaining > 0.0001:
                active_orders.append({
                    'rate': float(order.get('rate', 0)),
                    'remaining': remaining,
                    'fill_timestamp': order.get('fill_timestamp', ''),
                })
        return sorted(active_orders, key=lambda x: x.get('fill_timestamp', ''), reverse=True)

    def _lifo_match_from_queue(self, buy_queue: List[Dict], sell_amount: float, sell_rate: float) -> Tuple[float, float, float, float]:
        """Match a sell against a mutable LIFO queue; decrements remaining on each buy lot."""
        amount_to_match = sell_amount
        total_cost_basis = 0.0
        for buy_order in buy_queue:
            if amount_to_match <= 0:
                break
            remaining = buy_order['remaining']
            if remaining <= 0:
                continue
            consume_amount = min(remaining, amount_to_match)
            total_cost_basis += consume_amount * buy_order['rate']
            buy_order['remaining'] -= consume_amount
            amount_to_match -= consume_amount
        if amount_to_match > 0.0001:
            total_cost_basis += amount_to_match * sell_rate
        sell_proceeds = sell_amount * sell_rate
        buy_fee = total_cost_basis * self.buy_fee
        sell_fee = sell_proceeds * self.sell_fee
        net_profit = sell_proceeds - total_cost_basis - buy_fee - sell_fee
        total_investment = total_cost_basis + buy_fee
        net_profit_pct = (net_profit / total_investment * 100) if total_investment > 0 else 0.0
        return (net_profit, net_profit_pct, total_cost_basis, total_investment)

    def _sequential_working_lifo_profits(self, working_orders: List[Order]) -> Dict[str, Tuple[float, float, float]]:
        """Project Working ladder profit assuming lowest sell fills first (LIFO stack walk)."""
        if not working_orders:
            return {}
        buy_queue = self._get_active_lifo_buy_queue()
        if not buy_queue:
            return {}
        profits: Dict[str, Tuple[float, float, float]] = {}
        for order in sorted(working_orders, key=lambda o: o.rate):
            net_profit, net_profit_pct, cost_basis, _ = self._lifo_match_from_queue(
                buy_queue, order.amount, order.rate
            )
            matched_cost = (cost_basis / order.amount) if order.amount > 0 else 0.0
            profits[order.order_id] = (net_profit, net_profit_pct, matched_cost)
        return profits
    
    def _execute_skim(self, sell_profit: float):
        """Execute skim: buy configured tokens with percentage of sell profit
        
        Args:
            sell_profit: Profit from the current sell in quote currency
        """
        if not self.skim_enabled:
            return
        
        if sell_profit <= 0:
            return
        
        # Calculate skim amount for this sell
        skim_amount = sell_profit * self.skim_profit_percentage
        
        if skim_amount < self.skim_min_amount:
            logging.debug(f"{self.symbol}: Skim amount (${skim_amount:.2f}) below minimum "
                         f"(${self.skim_min_amount:.2f}), skipping")
            return
        
        if not self.skim_tokens:
            logging.warning(f"{self.symbol}: Skim enabled but no tokens configured")
            return
        
        # Calculate amount per token (equal distribution)
        amount_per_token = skim_amount / len(self.skim_tokens)
        
        if amount_per_token < self.skim_min_amount:
            logging.warning(f"{self.symbol}: Skim amount per token (${amount_per_token:.2f}) "
                           f"below minimum (${self.skim_min_amount:.2f}), skipping skim")
            return
        
        self._info(f"{self.symbol}: Executing skim - ${skim_amount:.2f} from "
                    f"${sell_profit:.2f} sell profit ({self.skim_profit_percentage*100:.1f}%)")
        
        purchases_for_notify = []
        # Place buy orders for each token
        for token_config in self.skim_tokens:
            cointype = token_config.get('cointype', '').upper()
            market = token_config.get('market', 'USDC').upper()
            
            if not cointype:
                logging.warning(f"{self.symbol}: Invalid skim token config (missing cointype): {token_config}")
                continue
            
            try:
                # Place market buy order using quote amount
                result = self.api.buy_now(
                    cointype=cointype,
                    amounttype='quote',
                    amount=round(amount_per_token, 2),  # Quote precision: 2 decimal places
                    market=market
                )
                
                if result.get('status') == 'ok':
                    bought_amount = result.get('amount', 0)
                    total_paid = result.get('total', 0)
                    self._info(f"{self.symbol}: ✅ Skim buy executed - {bought_amount:.8f} {cointype} "
                               f"@ ${total_paid:.2f} {self.market}")
                    purchases_for_notify.append({
                        'cointype': cointype,
                        'amount': bought_amount,
                        'total': total_paid,
                        'market': market,
                    })
                    # Track daily skim purchases
                    if cointype not in self.daily_skim_purchases:
                        self.daily_skim_purchases[cointype] = {
                            'amount': 0.0,
                            'total_cost': 0.0,
                            'purchases': 0,
                            'market': market
                        }
                    self.daily_skim_purchases[cointype]['amount'] += bought_amount
                    self.daily_skim_purchases[cointype]['total_cost'] += total_paid
                    self.daily_skim_purchases[cointype]['purchases'] += 1
                    
                    # Save skim purchase to JSON file for daily summary
                    self._save_skim_purchase(cointype, bought_amount, total_paid, market)
                else:
                    error_msg = result.get('message', 'Unknown error')
                    logging.error(f"{self.symbol}: ❌ Skim buy failed for {cointype}/{market}: {error_msg}")
                    self.notifier.notify_error(
                        self.symbol,
                        f"Skim buy failed for {cointype}/{market}: {error_msg}"
                    )
            except Exception as e:
                logging.error(f"{self.symbol}: Exception during skim buy for {cointype}/{market}: {e}")
                self.notifier.notify_error(
                    self.symbol,
                    f"Skim buy exception for {cointype}/{market}: {str(e)}"
                )
        if purchases_for_notify:
            self.notifier.notify_skim_purchase(
                self.symbol,
                sell_profit,
                skim_amount,
                self.skim_profit_percentage,
                purchases_for_notify,
            )
    
    def _get_json_last_updated(self) -> Optional[datetime]:
        """Get the last_updated timestamp from the JSON file"""
        file_path = self._get_filled_orders_file_path()
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    last_updated_str = data.get('last_updated')
                    if last_updated_str:
                        # Parse ISO format timestamp
                        try:
                            return datetime.fromisoformat(last_updated_str.replace('Z', '+00:00'))
                        except (ValueError, AttributeError):
                            # Try other formats
                            try:
                                return datetime.strptime(last_updated_str, '%Y-%m-%dT%H:%M:%S.%f')
                            except ValueError:
                                try:
                                    return datetime.strptime(last_updated_str, '%Y-%m-%dT%H:%M:%S')
                                except ValueError:
                                    pass
            except Exception as e:
                logging.debug(f"{self.symbol}: Could not read last_updated from JSON: {e}")
        return None
    
    def _should_telegram_notify_synced_fill(self, fill_timestamp: str, is_startup: bool,
                                            baseline_last_updated: 'Optional[datetime]' = None) -> bool:
        """Only notifier-push synced fills that are plausibly new — avoids belated Slack alerts for old trades.

        baseline_last_updated must be the JSON last_updated captured BEFORE this sync run wrote
        anything; otherwise the buy-sync rewrites last_updated to "now" and would suppress every
        subsequent startup notification.
        """
        if not fill_timestamp:
            return False
        try:
            fill_dt = datetime.fromisoformat(fill_timestamp.replace('Z', '+00:00'))
            if fill_dt.tzinfo is None:
                fill_dt = fill_dt.replace(tzinfo=timezone.utc)
        except (ValueError, AttributeError, TypeError):
            return False
        now = datetime.now(timezone.utc)
        age_seconds = (now - fill_dt).total_seconds()
        # Tolerate minor clock skew (fill stamped slightly in the future is still "fresh"),
        # but reject clearly bogus far-future timestamps.
        if age_seconds < -300:
            return False
        if is_startup:
            # Notify only fills newer than the bot's last-known activity snapshot.
            if baseline_last_updated is not None:
                base = baseline_last_updated
                if base.tzinfo is None:
                    base = base.replace(tzinfo=timezone.utc)
                if fill_dt <= base:
                    return False
            # Cap so a long offline gap doesn't spam ancient fills.
            return age_seconds <= 86400
        # Periodic: notify only fills within the recent window. Use 2x the interval as a buffer
        # for cycle-timing jitter so genuinely fresh fills aren't dropped.
        return age_seconds <= self._sync_interval * 2
    
    def _find_completed_order(self, order: 'Order') -> Optional[dict]:
        """Find a matching completed order from exchange history, if any."""
        try:
            if hasattr(self.api, "get_completed_orders"):
                result = self.api.get_completed_orders(self.cointype, self.market)
            else:
                endpoint = "/my/orders/completed"
                result = self.api._make_request(endpoint, {'cointype': self.cointype}, use_readonly=True)
            
            if result.get('status') != 'ok':
                return None
            
            if order.side == 'buy':
                completed_orders = result.get('buyorders', [])
            else:
                completed_orders = result.get('sellorders', [])
            
            # Prefer exact ID match (Hyperliquid numeric IDs, dry-run UUIDs, etc.)
            order_id = str(order.order_id) if hasattr(order, 'order_id') else ''
            if order_id:
                for completed in completed_orders:
                    if str(completed.get('id', '')) == order_id:
                        return completed
            
            for completed in completed_orders:
                try:
                    comp_amount = float(completed.get('amount', 0))
                    comp_rate = float(completed.get('rate', 0))
                    
                    amount_match = abs(comp_amount - order.amount) / order.amount < 0.01 if order.amount > 0 else False
                    rate_match = abs(comp_rate - order.rate) / order.rate < 0.001 if order.rate > 0 else False
                    
                    if not (amount_match and rate_match):
                        continue
                    
                    comp_solddate = completed.get('solddate', '')
                    if comp_solddate:
                        try:
                            comp_dt = datetime.fromisoformat(comp_solddate.replace('Z', '+00:00'))
                            if comp_dt.tzinfo is None:
                                comp_dt = comp_dt.replace(tzinfo=timezone.utc)
                            age_seconds = (datetime.now(timezone.utc) - comp_dt).total_seconds()
                            if age_seconds > 1800:
                                continue
                        except (ValueError, AttributeError, TypeError):
                            continue
                    else:
                        continue
                    return completed
                except (ValueError, TypeError):
                    continue
            return None
        except Exception as e:
            logging.debug(f"{self.symbol}: Error finding completed order via API: {e}")
            return None

    def _verify_order_via_api(self, order: 'Order') -> bool:
        """Verify if an order was filled by checking the completed orders API.
        
        Primary fill check in process(); balance change is the secondary signal when
        the API has no match (e.g. concurrent fills in one cycle).
        
        Returns True if the order is found in completed orders, False otherwise.
        """
        completed = self._find_completed_order(order)
        if completed:
            amount = float(completed.get('amount', order.amount))
            rate = float(completed.get('rate', order.rate))
            self._info(f"{self.symbol}: ✓ Order verified via API - {order.side} {amount:.8f} @ {rate:.4f}")
            return True
        logging.debug(f"{self.symbol}: Order not found in completed orders API - {order.side} {order.amount:.8f} @ {order.rate:.4f}")
        return False
    
    def _validate_orders_vs_balance(self):
        """Validate that stored orders total matches actual coin balance
        
        This is a sanity check to detect discrepancies between what we've tracked
        in JSON and what we actually hold. Triggers sync if discrepancy is significant.
        """
        if self.coin_balance == 0:
            return
        
        stored_orders = self._load_filled_orders()
        
        # Calculate total from stored buy orders (excluding consumed amounts)
        total_stored_amount = 0.0
        for order in stored_orders:
            amount = float(order.get('amount', 0))
            consumed = float(order.get('consumed_amount', 0))
            total_stored_amount += (amount - consumed)
        
        difference = self.coin_balance - total_stored_amount
        difference_pct = (difference / self.coin_balance * 100) if self.coin_balance > 0 else 0
        
        # Log discrepancy if significant (> 1%)
        if abs(difference_pct) > 1.0:
            if difference > 0:
                self._info(
                    f"{self.symbol}: Balance reconciliation: {difference:.2f} coins missing ({difference_pct:.1f}%) - syncing..."
                )
                # Trigger sync for missing orders
                self._detect_and_sync_missing_fills()
            else:
                # Only log at debug level - this is expected when using LIFO (newest orders were sold)
                logging.debug(
                    f"{self.symbol}: Stored orders exceed balance by {abs(difference):.2f} coins ({abs(difference_pct):.1f}%) - using oldest orders (LIFO: newest sold first)"
                )
        else:
            logging.debug(
                f"{self.symbol}: Balance reconciliation OK - stored: {total_stored_amount:.8f}, "
                f"actual: {self.coin_balance:.8f}, difference: {difference:+.8f} ({difference_pct:+.2f}%)"
            )
    
    def _sync_sells_from_dry_run_store(self) -> None:
        """Backfill sell_orders from persisted dry-run closed orders (same as daily summary)."""
        try:
            from pathlib import Path

            from spot_ladder.ledger_sell_sync import (
                refresh_sell_fill_timestamps_from_dry_store,
                sync_missing_sells_from_dry_run_store,
            )

            pair = getattr(self.api, "pair", f"{self.cointype}/{self.market}:USDC")
            filled_path = Path(self._get_filled_orders_file_path())
            state_dir = Path(self._state_dir)
            refresh_sell_fill_timestamps_from_dry_store(
                filled_path, state_dir, self.cointype, pair
            )
            added = sync_missing_sells_from_dry_run_store(
                filled_path,
                state_dir,
                self.cointype,
                pair,
                symbol=self.symbol,
                market=self.market,
            )
            if added:
                self._info(
                    f"{self.symbol}: Synced {added} sell fill(s) from dry-run order store into ledger"
                )
        except Exception as e:
            logging.debug(f"{self.symbol}: Dry-run sell ledger sync skipped: {e}")

    def _detect_and_sync_missing_fills(self):
        """Detect missing fills by comparing current balance with stored orders
        and sync them from exchange fill history via the API adapter.
        Also checks for orders newer than the JSON's last_updated timestamp.
        """
        self._sync_sells_from_dry_run_store()

        if self.coin_balance == 0:
            return
        
        stored_orders = self._load_filled_orders()
        # Only count unconsumed amounts when calculating missing coins
        # Consumed orders are still in JSON but those coins were sold
        total_stored_amount = 0.0
        for order in stored_orders:
            amount = float(order.get('amount', 0))
            consumed = float(order.get('consumed_amount', 0))
            total_stored_amount += (amount - consumed)
        missing_amount = self.coin_balance - total_stored_amount
        
        # Get the last_updated timestamp from JSON
        json_last_updated = self._get_json_last_updated()
        # Immutable snapshot of the bot's last-known activity, taken BEFORE any sync writes.
        # Used to decide which fills are "new" for startup notifier alerts (Slack when configured).
        baseline_last_updated = json_last_updated
        
        # IMPROVED: Always check for orders newer than last_updated (regardless of balance threshold)
        # This ensures we catch orders even if balance discrepancy is small
        should_sync_by_balance = missing_amount >= 0.0001 and missing_amount >= self.coin_balance * 0.01
        should_sync_by_time = json_last_updated is not None
        
        # Always sync if there's a last_updated timestamp (check for new orders)
        # OR if balance discrepancy is significant
        if not should_sync_by_balance and not should_sync_by_time:
            return
        
        if should_sync_by_balance:
            logging.warning(
                f"{self.symbol}: 🔍 Detected missing fills - balance ({self.coin_balance:.8f}) exceeds stored orders "
                f"({total_stored_amount:.8f}) by {missing_amount:.8f} coins ({((missing_amount/self.coin_balance)*100):.2f}%). "
                f"Attempting to sync from exchange fill history..."
            )
        elif should_sync_by_time:
            self._info(
                f"{self.symbol}: 🔍 Checking for orders newer than JSON last_updated ({json_last_updated.isoformat()})..."
            )
        
        try:
            if hasattr(self.api, "get_completed_orders"):
                result = self.api.get_completed_orders(self.cointype, self.market)
            else:
                result = self.api._make_request("/my/orders/completed", {'cointype': self.cointype}, use_readonly=True)
            
            if result.get('status') != 'ok':
                logging.warning(f"{self.symbol}: Failed to get order history: {result.get('message', 'Unknown error')}")
                return
            
            buy_orders = result.get('buyorders', [])
            is_startup_sync = not self._startup_sync_done
            missing_orders = []

            if buy_orders:
                for order in buy_orders:
                    amount = float(order.get('amount', 0))
                    rate = float(order.get('rate', 0))
                    solddate = order.get('solddate', '')
                    api_order_id = str(order.get('id', ''))
                    
                    # Skip zero-amount orders
                    if amount <= 0:
                        continue
                    
                    # FIRST: Check if this API order's ID already exists in stored orders
                    # Match on exchange API ID in either order_id or api_order_id field
                    skip_order = False
                    if api_order_id:
                        for stored_order in stored_orders:
                            stored_order_id = stored_order.get('order_id', '')
                            stored_api_id = stored_order.get('api_order_id', '')
                            if stored_order_id == api_order_id or stored_api_id == api_order_id:
                                logging.debug(f"{self.symbol}: API order {api_order_id} already in stored orders (matched {stored_order.get('order_id')}), skipping")
                                skip_order = True
                                break
                    if skip_order:
                        continue
                    
                    # SECOND: Check if this order matches any stored order (by amount, rate, and timestamp)
                    is_matched = False
                    for stored_order in stored_orders:
                        stored_amount = float(stored_order.get('amount', 0))
                        stored_rate = float(stored_order.get('rate', 0))
                        
                        rate_rounded = round(rate, 4)
                        stored_rate_rounded = round(stored_rate, 4)
                        
                        # 1% amount tolerance: fill-detection saves placed amount, API has actual fill
                        amount_match = stored_amount > 0 and abs(amount - stored_amount) / stored_amount < 0.01
                        rate_match = stored_rate > 0 and abs(rate_rounded - stored_rate_rounded) < 0.0001
                        
                        if amount_match and rate_match:
                            is_matched = True
                            logging.debug(f"{self.symbol}: API order {api_order_id} ({amount:.8f} @ {rate:.4f}) matched stored order {stored_order.get('order_id')} ({stored_amount:.8f} @ {stored_rate:.4f})")
                            break
                    
                    if is_matched:
                        continue
                    
                    if should_sync_by_time and not should_sync_by_balance and json_last_updated and solddate:
                        try:
                            order_date = datetime.fromisoformat(solddate.replace('Z', '+00:00'))
                            if order_date.tzinfo is None:
                                order_date = order_date.replace(tzinfo=datetime.now().astimezone().tzinfo)
                            if json_last_updated.tzinfo is None:
                                json_last_updated = json_last_updated.replace(tzinfo=datetime.now().astimezone().tzinfo)
                            
                            if order_date <= json_last_updated:
                                continue
                        except (ValueError, AttributeError):
                            continue
                    
                    missing_orders.append(order)
            else:
                logging.debug(f"{self.symbol}: No buy orders in completed order history")
            
            synced_count = 0
            synced_amount = 0.0
            if not missing_orders:
                self._info(f"{self.symbol}: All buy orders are already in JSON")
            else:
                for order in missing_orders:
                    amount = float(order.get('amount', 0))
                    rate = float(order.get('rate', 0))
                    solddate = order.get('solddate', datetime.utcnow().isoformat())
                
                    # Parse date and create order ID
                    try:
                        dt = datetime.fromisoformat(solddate.replace('Z', '+00:00'))
                        date_str = dt.strftime('%Y-%m-%d_%H-%M')
                    except:
                        date_str = datetime.now().strftime('%Y-%m-%d_%H-%M')
                
                    order_id = f'verified_{len(stored_orders) + synced_count + 1}_{date_str}'
                
                    # Check for duplicate by coin_balance_at_save - if balance didn't change, it's likely a duplicate
                    # This catches cases where the same order was already saved with a different ID
                    # CRITICAL: API is source of truth - only skip if ALL criteria match (amount, rate, timestamp)
                    # This prevents orders with same rate/timestamp but different amounts from being incorrectly skipped
                    # Reload stored orders in case they changed during this loop
                    current_stored_orders = self._load_filled_orders()
                    current_balance = self.coin_balance
                    is_duplicate = False
                    for existing_order in current_stored_orders:
                        existing_balance = existing_order.get('coin_balance_at_save')
                        if existing_balance is not None and abs(float(existing_balance) - float(current_balance)) < 0.00000001:
                            # Same balance - check if amount/rate/timestamp ALL match (likely duplicate)
                            # CRITICAL: Require ALL three to match - don't skip orders just because they have same rate/timestamp
                            existing_amount = float(existing_order.get('amount', 0))
                            existing_rate = float(existing_order.get('rate', 0))
                            existing_timestamp = existing_order.get('fill_timestamp', '')
                        
                            # Amount must match exactly (within 0.1% tolerance) - this is the key check
                            # Orders with different amounts should NOT be considered duplicates even if rate/timestamp match
                            amount_match = existing_amount > 0 and abs(amount - existing_amount) / existing_amount < 0.001  # 0.1% tolerance (stricter)
                            rate_match = existing_rate > 0 and abs(rate - existing_rate) / existing_rate < 0.0001  # 0.01% tolerance
                        
                            # Check timestamp within 5 minutes (tighter window)
                            timestamp_match = True
                            if existing_timestamp and solddate:
                                try:
                                    ts1 = solddate.replace('Z', '+00:00')
                                    ts2 = existing_timestamp.replace('Z', '+00:00')
                                    dt1 = datetime.fromisoformat(ts1)
                                    dt2 = datetime.fromisoformat(ts2)
                                    if dt1.tzinfo is None:
                                        dt1 = dt1.replace(tzinfo=timezone.utc)
                                    if dt2.tzinfo is None:
                                        dt2 = dt2.replace(tzinfo=timezone.utc)
                                    time_diff = abs((dt1 - dt2).total_seconds())
                                    timestamp_match = time_diff < 300  # 5 minutes (tighter)
                                except (ValueError, AttributeError, TypeError):
                                    timestamp_match = True
                        
                            # REQUIRE ALL THREE to match - this ensures orders with same rate/timestamp but different amounts are NOT skipped
                            if amount_match and rate_match and timestamp_match:
                                logging.warning(f"{self.symbol}: Duplicate order detected during sync - same coin_balance_at_save ({current_balance:.8f}) "
                                              f"and matching amount/rate/timestamp. Existing: {existing_order.get('order_id')} ({existing_amount:.8f} @ {existing_rate:.4f}), "
                                              f"Would add: {order_id} ({amount:.8f} @ {rate:.4f}). Skipping duplicate.")
                                is_duplicate = True
                                break
                            else:
                                # Same balance but different amount/rate/timestamp - these are different orders, allow both
                                logging.debug(f"{self.symbol}: Order {order_id} ({amount:.8f} @ {rate:.4f}) has same balance as {existing_order.get('order_id')} "
                                            f"({existing_amount:.8f} @ {existing_rate:.4f}) but different amount/rate/timestamp - allowing both (API is source of truth)")
                
                    if is_duplicate:
                        continue  # Skip this order, don't add it
                
                    # Create order record
                    order_record = {
                        'order_id': order_id,
                        'api_order_id': str(order.get('id', '')),
                        'symbol': self.symbol,
                        'amount': amount,
                        'rate': rate,
                        'market': self.market,
                        'fill_timestamp': solddate,
                        'total_usd': amount * rate,
                        'coin_balance_at_save': self.coin_balance,
                        'synced_from_api': True
                    }
                
                    # Save to JSON - preserve sell_orders and metadata
                    file_path = self._get_filled_orders_file_path()
                    existing_orders = self._load_filled_orders()
                    existing_orders.append(order_record)
                
                    # Load existing file to preserve sell_orders and metadata
                    existing_data = {}
                    if os.path.exists(file_path):
                        try:
                            with open(file_path, 'r') as f:
                                existing_data = json.load(f)
                        except Exception as e:
                            logging.warning(f"{self.symbol}: Failed to load existing data when syncing: {e}")
                
                    data = {
                        'symbol': self.symbol,
                        'cointype': self.cointype,
                        'market': self.market,
                        'last_updated': datetime.utcnow().isoformat(),
                        'buy_orders': existing_orders,
                        'sell_orders': existing_data.get('sell_orders', []),  # Preserve sell_orders
                        'metadata': existing_data.get('metadata', {})  # Preserve metadata
                    }
                
                    with open(file_path, 'w') as f:
                        json.dump(data, f, indent=2)
                
                    synced_count += 1
                    synced_amount += amount
                
                    # Long-term performance ledger (signals / copy-trade reporting)
                    if self._performance_tracker:
                        try:
                            self._performance_tracker.record_trade(
                                side='buy',
                                order_id=order_id,
                                amount=amount,
                                rate=rate,
                                fill_timestamp=solddate,
                                total_usd=amount * rate,
                                profit_usd=None,
                            )
                        except Exception as e:
                            logging.debug(f"{self.symbol}: Performance tracker record_trade (synced buy) failed: {e}")
                
                    self._info(
                        f"{self.symbol}: Synced missing order {order_id}: {amount:.8f} @ {rate:.4f} {self.market} "
                        f"(date: {solddate})"
                    )
                
                    # Send individual order_filled notification for synced orders
                    # This ensures users get notified even if orders filled while bot was offline
                    if self._should_telegram_notify_synced_fill(solddate, is_startup_sync, baseline_last_updated):
                        stub = Order(
                            order_id=api_order_id or order_id,
                            symbol=self.symbol,
                            amount=amount,
                            rate=rate,
                            side="buy",
                            market=self.market,
                        )
                        self._notify_buy_fill_if_needed(stub, amount, rate)
                    else:
                        self._info(
                            f"{self.symbol}: Skipping notifier for synced buy fill "
                            f"({amount:.8f} @ {rate:.4f}, date: {(solddate or '')[:19]}) — fill too old for sync notify"
                        )
                
                    # Only stop early if syncing by balance (not by time)
                    # When syncing by time, we want ALL new orders
                    if should_sync_by_balance and not should_sync_by_time:
                        if synced_amount >= missing_amount * 0.95:
                            break
            
            if synced_count > 0:
                self._info(
                    f"{self.symbol}: Synced {synced_count} missing order(s) totaling {synced_amount:.8f} coins "
                    f"from exchange order history"
                )
                # Also send summary notification for context
                # Determine if this is startup or periodic sync based on _startup_sync_done flag
                sync_type = "Startup sync" if not self._startup_sync_done else "Periodic sync"
                
                # Only notify on startup sync, not periodic syncs (to reduce Slack noise)
                if not self._startup_sync_done:
                    self.notifier.notify_status(
                        f"🔄 {self.symbol}: {sync_type} - synced {synced_count} missing order(s), "
                        f"total: {synced_amount:.4f} coins"
                    )
                else:
                    # Log periodic syncs but don't push to Slack
                    self._info(
                        f"{self.symbol}: {sync_type} - synced {synced_count} missing order(s), "
                        f"total: {synced_amount:.4f} coins (logged only, not sent to Slack)"
                    )
            elif missing_orders:
                logging.warning(
                    f"{self.symbol}: Found {len(missing_orders)} orders in API but couldn't match them. "
                    f"Run sync_missing_orders.py manually to investigate."
                )
            
            # Sync missing SELL orders from the same API result (sells that filled while bot was offline)
            # This ensures daily summary and profit calculations include them, and user can get a late notification
            api_sell_orders = result.get('sellorders', [])
            if api_sell_orders:
                file_path = self._get_filled_orders_file_path()
                try:
                    with open(file_path, 'r') as f:
                        existing_data = json.load(f)
                except Exception as e:
                    logging.warning(f"{self.symbol}: Failed to load JSON for sell sync: {e}")
                    existing_data = {}
                existing_sell_orders = existing_data.get('sell_orders', [])
                existing_sell_ids = {str(o.get('order_id', '')) for o in existing_sell_orders}
                # Also match by amount+rate+timestamp in case API id differs from stored
                existing_sell_keys = {
                    (round(float(o.get('amount', 0)), 8), round(float(o.get('rate', 0)), 4), (o.get('fill_timestamp') or '')[:19])
                    for o in existing_sell_orders
                }
                missing_sells = []
                for api_sell in api_sell_orders:
                    amount = float(api_sell.get('amount', 0))
                    rate = float(api_sell.get('rate', 0))
                    solddate = api_sell.get('solddate', '') or ''
                    api_id = str(api_sell.get('id', ''))
                    if amount <= 0:
                        continue
                    if api_id and api_id in existing_sell_ids:
                        continue
                    key = (round(amount, 8), round(rate, 4), solddate[:19] if solddate else '')
                    if key in existing_sell_keys:
                        continue
                    if not solddate:
                        solddate = datetime.now(timezone.utc).isoformat()
                    missing_sells.append(api_sell)
                if missing_sells:
                    def _sell_sort_key(api_sell: Dict) -> datetime:
                        solddate = api_sell.get('solddate', '') or ''
                        try:
                            dt = datetime.fromisoformat(solddate.replace('Z', '+00:00'))
                            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
                        except (ValueError, AttributeError, TypeError):
                            return datetime.min.replace(tzinfo=timezone.utc)

                    missing_sells.sort(key=_sell_sort_key)
                    synced_sell_count = 0
                    for api_sell in missing_sells:
                        amount = float(api_sell.get('amount', 0))
                        rate = float(api_sell.get('rate', 0))
                        solddate = api_sell.get('solddate', datetime.utcnow().isoformat())
                        api_id = str(api_sell.get('id', ''))
                        order_id = api_id if api_id else f"verified_sell_{synced_sell_count + 1}_{solddate.replace(':', '-').replace(' ', '_')[:19]}"
                        sell_order = Order(
                            order_id=order_id,
                            symbol=self.symbol,
                            side='sell',
                            amount=amount,
                            rate=rate,
                            market=self.market,
                        )
                        sell_profit, sell_cost_basis = self._calculate_sell_profit(sell_order)
                        self._save_filled_sell_order(sell_order, solddate)
                        synced_sell_count += 1
                        self._info(
                            f"{self.symbol}: Synced missing SELL {order_id}: {amount:.8f} @ {rate:.4f} {self.market} (date: {solddate[:19]})"
                        )
                        if self.skim_enabled and sell_profit > 0:
                            self._execute_skim(sell_profit)
                        if sell_cost_basis > 0 and amount > 0:
                            avg_entry_for_notify = sell_cost_basis / amount
                        else:
                            avg_entry_for_notify = self.average_entry_price if getattr(self, 'average_entry_price', None) else None
                        if self._should_telegram_notify_synced_fill(solddate, is_startup_sync, baseline_last_updated):
                            self.notifier.notify_order_filled(
                                symbol=self.symbol,
                                side='sell',
                                amount=amount,
                                rate=rate,
                                avg_entry=avg_entry_for_notify,
                                profit_usd=sell_profit,
                            )
                        else:
                            self._info(
                                f"{self.symbol}: Skipping notifier for synced sell fill "
                                f"({amount:.8f} @ {rate:.4f}, date: {solddate[:19]}) — fill too old for sync notify"
                            )
                    if synced_sell_count:
                        self._info(
                            f"{self.symbol}: Synced {synced_sell_count} missing SELL(s) from order history (LIFO applied)"
                        )
        
        except Exception as e:
            logging.warning(f"{self.symbol}: Failed to sync missing fills from order history: {e}")
            self._info(f"{self.symbol}: Run sync_missing_orders.py manually to sync missing orders")
    
    def _detect_duplicate_orders(self) -> List[Tuple[Dict, Dict]]:
        """
        Detect potential duplicate orders in the JSON file.
        Checks for orders with similar amounts, rates, and timestamps.
        
        Returns a list of tuples containing pairs of potentially duplicate orders.
        """
        stored_orders = self._load_filled_orders()
        duplicates = []
        
        # Compare each order with every other order
        for i, order1 in enumerate(stored_orders):
            amount1 = float(order1.get('amount', 0))
            rate1 = float(order1.get('rate', 0))
            timestamp1 = order1.get('fill_timestamp', '')
            order_id1 = order1.get('order_id', '')
            
            # Skip zero amounts
            if amount1 <= 0:
                continue
            
            for j, order2 in enumerate(stored_orders[i+1:], start=i+1):
                amount2 = float(order2.get('amount', 0))
                rate2 = float(order2.get('rate', 0))
                timestamp2 = order2.get('fill_timestamp', '')
                order_id2 = order2.get('order_id', '')
                
                # Skip zero amounts
                if amount2 <= 0:
                    continue
                
                # Check if timestamps are within 5 minutes of each other
                time_diff_seconds = None
                timestamp_match = False
                if timestamp1 and timestamp2:
                    try:
                        # Parse timestamps and ensure both are timezone-aware
                        ts1 = timestamp1.replace('Z', '+00:00')
                        ts2 = timestamp2.replace('Z', '+00:00')
                        dt1 = datetime.fromisoformat(ts1)
                        dt2 = datetime.fromisoformat(ts2)
                        
                        # Make sure both are timezone-aware (UTC)
                        if dt1.tzinfo is None:
                            dt1 = dt1.replace(tzinfo=timezone.utc)
                        if dt2.tzinfo is None:
                            dt2 = dt2.replace(tzinfo=timezone.utc)
                        
                        # Calculate time difference
                        time_diff_seconds = abs((dt1 - dt2).total_seconds())
                        timestamp_match = time_diff_seconds < 300  # 5 minutes
                    except (ValueError, AttributeError, TypeError) as e:
                        logging.debug(f"{self.symbol}: Error comparing timestamps '{timestamp1}' and '{timestamp2}': {e}")
                        pass
                
                # Check if amounts and rates are very similar
                # Use more lenient threshold if timestamps are very close (< 2 minutes)
                # This catches cases where slight rounding differences exist
                if time_diff_seconds is not None and time_diff_seconds < 120:  # Less than 2 minutes
                    # Very close timestamps - use 0.15% threshold for amount
                    amount_threshold = 0.0015
                else:
                    # Normal threshold - 0.1% for amount
                    amount_threshold = 0.001
                
                amount_match = abs(amount1 - amount2) / max(amount1, amount2) < amount_threshold if max(amount1, amount2) > 0 else False
                rate_match = abs(rate1 - rate2) / max(rate1, rate2) < 0.0001 if max(rate1, rate2) > 0 else False
                
                # If amounts, rates match, and timestamps are close, it's likely a duplicate
                if amount_match and rate_match and timestamp_match:
                    duplicates.append((order1, order2))
                    logging.warning(
                        f"{self.symbol}: ⚠️  Potential duplicate orders detected:\n"
                        f"   Order 1: {order_id1} - {amount1:.8f} @ {rate1:.4f} ({timestamp1})\n"
                        f"   Order 2: {order_id2} - {amount2:.8f} @ {rate2:.4f} ({timestamp2})\n"
                        f"   Amount diff: {abs(amount1 - amount2):.8f} ({abs(amount1 - amount2) / max(amount1, amount2) * 100:.4f}%)\n"
                        f"   Rate diff: {abs(rate1 - rate2):.6f} ({abs(rate1 - rate2) / max(rate1, rate2) * 100:.4f}%)"
                    )
        
        if duplicates:
            logging.warning(
                f"{self.symbol}: 🔍 Found {len(duplicates)} potential duplicate order pair(s). "
                f"Attempting to remove duplicates automatically..."
            )
            # Automatically remove duplicates
            self._remove_duplicate_orders(duplicates)
        else:
            logging.debug(f"{self.symbol}: ✅ No duplicate orders detected")
        
        return duplicates
    
    def _remove_duplicate_orders(self, duplicates: List[Tuple[Dict, Dict]]):
        """
        Remove duplicate orders from the JSON file.
        Keeps the order with the better order_id (prefers api_buy_* over verified_*).
        """
        if not duplicates:
            return
        
        file_path = self._get_filled_orders_file_path()
        stored_orders = self._load_filled_orders()
        
        # Create a set of order IDs to remove
        orders_to_remove = set()
        
        for order1, order2 in duplicates:
            order_id1 = order1.get('order_id', '')
            order_id2 = order2.get('order_id', '')
            
            # Prefer api_buy_* or api_* prefixed orders over verified_* or other IDs
            # Also prefer orders with more complete data (source field, etc.)
            def order_priority(order_id: str, order: Dict) -> int:
                priority = 0
                # Higher priority for api_buy_* or api_* prefixed orders
                if order_id.startswith('api_buy_') or order_id.startswith('api_'):
                    priority += 100
                # Lower priority for verified_* orders
                elif order_id.startswith('verified_'):
                    priority -= 50
                # Prefer orders with source field
                if order.get('source'):
                    priority += 10
                # Prefer orders with synced_from_api flag
                if order.get('synced_from_api'):
                    priority -= 20  # These are less reliable
                return priority
            
            priority1 = order_priority(order_id1, order1)
            priority2 = order_priority(order_id2, order2)
            
            # Remove the one with lower priority
            if priority1 > priority2:
                orders_to_remove.add(order_id2)
                self._info(
                    f"{self.symbol}: Removing duplicate order {order_id2} (keeping {order_id1})"
                )
            elif priority2 > priority1:
                orders_to_remove.add(order_id1)
                self._info(
                    f"{self.symbol}: Removing duplicate order {order_id1} (keeping {order_id2})"
                )
            else:
                # Same priority - keep the first one (older order_id or first in list)
                # Prefer keeping the one that appears first in the list
                if order_id1 in [o.get('order_id') for o in stored_orders[:len(stored_orders)//2]]:
                    orders_to_remove.add(order_id2)
                    self._info(
                        f"{self.symbol}: Removing duplicate order {order_id2} (keeping {order_id1} - appeared first)"
                    )
                else:
                    orders_to_remove.add(order_id1)
                    self._info(
                        f"{self.symbol}: Removing duplicate order {order_id1} (keeping {order_id2} - appeared first)"
                    )
        
        if not orders_to_remove:
            return
        
        # Filter out orders to remove
        original_count = len(stored_orders)
        cleaned_orders = [o for o in stored_orders if o.get('order_id') not in orders_to_remove]
        removed_count = original_count - len(cleaned_orders)
        
        if removed_count > 0:
            # Save cleaned orders back to file
            try:
                # Load existing file to preserve other data
                existing_data = {}
                if os.path.exists(file_path):
                    with open(file_path, 'r') as f:
                        existing_data = json.load(f)
                
                data = {
                    'symbol': self.symbol,
                    'cointype': self.cointype,
                    'market': self.market,
                    'last_updated': datetime.utcnow().isoformat(),
                    'buy_orders': cleaned_orders,
                    'sell_orders': existing_data.get('sell_orders', []),  # Preserve sell_orders
                    'metadata': existing_data.get('metadata', {})  # Preserve metadata
                }
                
                # Update metadata total_buys count
                if 'metadata' in data and isinstance(data['metadata'], dict):
                    data['metadata']['total_buys'] = len(cleaned_orders)
                
                with open(file_path, 'w') as f:
                    json.dump(data, f, indent=2)
                
                self._info(
                    f"{self.symbol}: ✅ Removed {removed_count} duplicate order(s). "
                    f"Orders: {original_count} → {len(cleaned_orders)}"
                )
            except Exception as e:
                logging.error(f"{self.symbol}: Failed to remove duplicates: {e}")
    
    def _get_stored_filled_orders(self) -> List[Dict]:
        """Get stored filled buy orders from JSON
        
        CRITICAL: If an order is in the JSON, we can trust it - it was only saved after
        we verified we actually received the tokens. So we can use ALL entries in the JSON.
        
        We use LIFO (last in, first out) for matching sells to buys (newest buys sold first):
        - When tokens are sold, NEWEST buy orders are consumed first
        - Orders marked as 'fully_consumed' are excluded from calculations
        - Partially consumed orders use remaining amount only
        - This maintains accurate cost basis as position changes
        - LIFO optimizes profit by selling lower-cost-basis coins first
        
        Algorithm:
        1. Filter out fully consumed orders
        2. Calculate remaining amount in each order (amount - consumed_amount)
        3. If total matches balance (within tolerance), use all remaining orders
        4. If balance > total, use all orders (some coins not tracked)
        5. Otherwise, accumulate from newest until we match current balance
        """
        stored_orders = self._load_filled_orders()
        current_coin_amount = self.coin_balance
        
        if not stored_orders or current_coin_amount == 0:
            return []
        
        # Filter out fully consumed orders and calculate remaining amounts
        active_orders = []
        for order in stored_orders:
            if order.get('fully_consumed', False):
                continue  # Skip fully consumed orders
            
            original_amount = float(order.get('amount', 0))
            consumed_amount = float(order.get('consumed_amount', 0))
            remaining_amount = original_amount - consumed_amount
            
            if remaining_amount > 0.0001:  # Only include if meaningful amount remains
                # Create a copy with the remaining amount for calculations
                order_copy = order.copy()
                order_copy['_remaining_amount'] = remaining_amount
                active_orders.append(order_copy)
        
        if not active_orders:
            return []
        
        # Calculate total remaining amount in all active orders
        total_in_orders = sum(o.get('_remaining_amount', float(o.get('amount', 0))) for o in active_orders)
        
        # Log for debugging
        logging.debug(f"{self.symbol}: Total remaining in orders: {total_in_orders:.8f}, Current balance: {current_coin_amount:.8f}")
        
        # If total matches balance (within tolerance), use all active orders
        balance_diff_pct = abs(total_in_orders - current_coin_amount) / current_coin_amount * 100 if current_coin_amount > 0 else 0
        if current_coin_amount > 0 and balance_diff_pct < 2.0:  # Only if within 2% (very close match)
            # All orders match balance closely - use all of them
            self._info(f"{self.symbol}: Total matches balance within 2% ({balance_diff_pct:.2f}% diff) - using all {len(active_orders)} orders")
            active_orders.sort(key=lambda x: x.get('fill_timestamp', ''), reverse=False)
            return active_orders
        
        # Calculate how much is untracked (if any)
        untracked_amount = current_coin_amount - total_in_orders
        
        if untracked_amount > 0:
            # More balance than orders - some coins not tracked, use all orders
            self._info(f"{self.symbol}: Balance ({current_coin_amount:.8f}) > total orders ({total_in_orders:.8f}) by {untracked_amount:.8f} - using all {len(active_orders)} orders (some coins not tracked in JSON)")
            active_orders.sort(key=lambda x: x.get('fill_timestamp', ''), reverse=False)
            return active_orders
        
        # total_in_orders > current_coin_amount: more in orders than balance.
        # The excess is from tracking gaps in old orders (consumed_amount already handles
        # LIFO consumption). Always keep NEWEST orders and trim the oldest excess.
        if total_in_orders > current_coin_amount:
            self._info(f"{self.symbol}: Balance ({current_coin_amount:.8f}) < total orders ({total_in_orders:.8f}) "
                        f"by {total_in_orders - current_coin_amount:.8f} ({balance_diff_pct:.1f}% excess) - "
                        f"keeping newest orders (consumed_amount already tracks LIFO)")
            active_orders.sort(key=lambda x: x.get('fill_timestamp', ''), reverse=True)  # NEWEST first
        else:
            active_orders.sort(key=lambda x: x.get('fill_timestamp', ''), reverse=True)  # NEWEST first
        
        # Accumulate from newest until we reach current balance
        # Oldest orders with tracking gaps are excluded
        orders_to_use = []
        accumulated_amount = 0.0
        tolerance = 0.0001  # Minimum coin amount to treat as "still need more"
        
        logging.debug(f"{self.symbol}: Counting from newest orders (excluding oldest to match balance)")
        
        for order in active_orders:
            # Use remaining amount (accounts for partial consumption)
            amount = order.get('_remaining_amount', float(order.get('amount', 0)))
            if amount <= 0:
                continue
            
            # When adding this order would exceed actual balance, take only what's needed so total = balance
            if accumulated_amount + amount > current_coin_amount:
                remaining_needed = current_coin_amount - accumulated_amount
                if remaining_needed > tolerance:
                    # Use partial amount from this order to reach balance exactly
                    partial_amount = min(remaining_needed, amount)
                    order_copy = order.copy()
                    order_copy['_remaining_amount'] = partial_amount  # Override with partial amount
                    orders_to_use.append(order_copy)
                    accumulated_amount += partial_amount
                    logging.debug(f"{self.symbol}: Using partial order: {partial_amount:.2f} of {amount:.2f} coins (total now {accumulated_amount:.2f})")
                else:
                    # Already at or over balance - stop without adding this order
                    logging.debug(f"{self.symbol}: Balance reached at {accumulated_amount:.2f} coins")
                break
            
            # Add full order
            orders_to_use.append(order)
            accumulated_amount += amount
        
        logging.debug(f"{self.symbol}: Accumulated {accumulated_amount:.2f} coins from {len(orders_to_use)} newest orders")
        
        # If accumulated amount is under balance, fill the shortfall with current average price
        if accumulated_amount < current_coin_amount:
            missing = current_coin_amount - accumulated_amount
            missing_pct = (missing / current_coin_amount * 100) if current_coin_amount > 0 else 0
            
            # Calculate average price from orders we have so far
            if orders_to_use:
                total_cost = sum(o.get('_remaining_amount', float(o.get('amount', 0))) * float(o.get('rate', 0)) for o in orders_to_use)
                avg_price = total_cost / accumulated_amount if accumulated_amount > 0 else self.current_price
            else:
                # No orders yet, use current market price
                avg_price = self.current_price if self.current_price > 0 else 0
            
            if avg_price > 0:
                # Create a synthetic order to fill the shortfall using the average price
                synthetic_order = {
                    'order_id': f'synthetic_shortfall_{datetime.utcnow().strftime("%Y%m%d_%H%M%S")}',
                    'symbol': self.symbol,
                    'amount': missing,
                    '_remaining_amount': missing,
                    'rate': avg_price,
                    'market': self.market,
                    'fill_timestamp': datetime.utcnow().isoformat(),
                    'total_usd': missing * avg_price,
                    'synthetic': True,  # Mark as synthetic
                    'note': f'Fills {missing:.8f} coin shortfall using average price ${avg_price:.4f}'
                }
                orders_to_use.append(synthetic_order)
                accumulated_amount += missing
                self._info(
                    f"{self.symbol}: Filled shortfall of {missing:.8f} coins ({missing_pct:.2f}%) using average price ${avg_price:.4f} "
                    f"(from {len(orders_to_use)-1} tracked orders). Total now: {accumulated_amount:.8f} coins."
                )
            else:
                logging.warning(
                    f"{self.symbol}: ⚠️ Cannot fill shortfall - no price available. Missing: {missing:.8f} coins ({missing_pct:.2f}%). "
                    f"Using {len(orders_to_use)} newest orders to represent current holdings."
                )
        
        # If we're significantly under the balance, log a warning
        # This might mean some coins weren't tracked in orders (bought before tracking started)
        # Only warn if difference is > 15% to avoid noise for small discrepancies
        if accumulated_amount < current_coin_amount * 0.85:
            logging.warning(
                f"{self.symbol}: ⚠️ Accumulated orders ({accumulated_amount:.8f}) are less than 85% of "
                f"current balance ({current_coin_amount:.8f}). Difference: "
                f"{current_coin_amount - accumulated_amount:.8f} coins. "
                f"This may indicate some coins weren't tracked in orders (bought before tracking started)."
            )
        elif accumulated_amount < current_coin_amount * 0.95:
            # Small difference - just log at debug level, not warning
            logging.debug(
                f"{self.symbol}: Accumulated orders ({accumulated_amount:.8f}) are {((current_coin_amount - accumulated_amount) / current_coin_amount * 100):.1f}% less than "
                f"current balance ({current_coin_amount:.8f}). Some coins may not be tracked (bought before tracking started)."
            )
        
        # Final verification: accumulated amount should match current balance
        # Only warn if difference is significant (>15%) to avoid noise for small discrepancies
        difference_pct = abs(accumulated_amount - current_coin_amount) / current_coin_amount * 100
        if difference_pct > 15.0:
            if accumulated_amount < current_coin_amount:
                # We're under the balance - some coins may not be tracked in orders
                logging.warning(
                    f"{self.symbol}: ⚠️ Accumulated orders ({accumulated_amount:.8f}) are {difference_pct:.1f}% LESS than "
                    f"current balance ({current_coin_amount:.8f}) - difference: "
                    f"{current_coin_amount - accumulated_amount:.8f} coins. "
                    f"This may indicate some coins weren't tracked in orders, or some orders were excluded."
                )
            else:
                # We're over the balance - this shouldn't happen with our checks
                logging.error(
                    f"{self.symbol}: ERROR: Accumulated orders ({accumulated_amount:.8f}) are {difference_pct:.1f}% MORE than "
                    f"current balance ({current_coin_amount:.8f}) - difference: "
                    f"{accumulated_amount - current_coin_amount:.8f} coins. "
                    f"This should not happen - orders should not exceed balance!"
                )
        elif difference_pct > 1.0:
            # Small difference - log at debug level only (some coins not tracked is normal)
            logging.debug(
                f"{self.symbol}: Accumulated orders ({accumulated_amount:.8f}) are {difference_pct:.1f}% different from "
                f"current balance ({current_coin_amount:.8f}). Some coins may not be tracked (bought before tracking started)."
            )
        
        # If no orders accumulated, use the single order closest to current balance
        if len(orders_to_use) == 0 and active_orders:
            closest_order = min(active_orders, 
                              key=lambda x: abs(x.get('_remaining_amount', float(x.get('amount', 0))) - current_coin_amount))
            orders_to_use = [closest_order]
            accumulated_amount = closest_order.get('_remaining_amount', float(closest_order.get('amount', 0)))
            logging.warning(
                f"{self.symbol}: No orders matched current balance ({current_coin_amount:.8f}), "
                f"using closest single order ({accumulated_amount:.8f} coins)"
            )
        
        # Sort orders by timestamp for calculation/logging (oldest first for consistency)
        orders_to_use.sort(key=lambda x: x.get('fill_timestamp', ''), reverse=False)
        
        # Log summary of filtering (debug level as this is called frequently)
        if len(orders_to_use) < len(active_orders):
            excluded_count = len(active_orders) - len(orders_to_use)
            excluded_amount = sum(o.get('_remaining_amount', float(o.get('amount', 0))) for o in active_orders if o not in orders_to_use)
            logging.debug(
                f"{self.symbol}: Selected {len(orders_to_use)} orders (keeping newest, oldest excluded) "
                f"to match current balance ({current_coin_amount:.8f} coins). "
                f"Excluded {excluded_count} order(s) totaling {excluded_amount:.2f} coins."
            )
        
        return orders_to_use
    
    def _cleanup_filled_orders_json(self, orders_to_keep: List[Dict]):
        """Update JSON file to only contain orders we're actually using
        
        Removes orders from JSON that don't match tokens we currently hold.
        This ensures the JSON file only contains orders for tokens we've actually bought
        and currently possess, not orders for tokens we've sold or never received.
        """
        if not orders_to_keep:
            # If we have no orders to keep, clear the JSON (but keep structure and sell_orders)
            file_path = self._get_filled_orders_file_path()
            try:
                # Load existing file to preserve sell_orders and metadata
                existing_data = {}
                if os.path.exists(file_path):
                    try:
                        with open(file_path, 'r') as f:
                            existing_data = json.load(f)
                    except Exception as e:
                        logging.warning(f"{self.symbol}: Failed to load existing data when cleaning: {e}")
                
                data = {
                    'symbol': self.symbol,
                    'cointype': self.cointype,
                    'market': self.market,
                    'last_updated': datetime.utcnow().isoformat(),
                    'buy_orders': [],
                    'sell_orders': existing_data.get('sell_orders', []),  # Preserve sell_orders
                    'metadata': existing_data.get('metadata', {})  # Preserve metadata
                }
                with open(file_path, 'w') as f:
                    json.dump(data, f, indent=2)
                self._info(f"{self.symbol}: Cleaned up JSON - removed all orders (no matching tokens in balance)")
            except Exception as e:
                logging.error(f"{self.symbol}: Failed to cleanup JSON file: {e}")
            return
        
        # Get order IDs we want to keep
        orders_to_keep_ids = {order.get('order_id') for order in orders_to_keep}
        
        # Load all stored orders
        all_stored_orders = self._load_filled_orders()
        
        # Filter to only keep orders we're using
        orders_to_save = [order for order in all_stored_orders 
                         if order.get('order_id') in orders_to_keep_ids]
        
        # If we're removing orders, update the JSON
        if len(orders_to_save) < len(all_stored_orders):
            removed_count = len(all_stored_orders) - len(orders_to_save)
            file_path = self._get_filled_orders_file_path()
            try:
                # Load existing file to preserve sell_orders and metadata
                existing_data = {}
                if os.path.exists(file_path):
                    try:
                        with open(file_path, 'r') as f:
                            existing_data = json.load(f)
                    except Exception as e:
                        logging.warning(f"{self.symbol}: Failed to load existing data when cleaning: {e}")
                
                data = {
                    'symbol': self.symbol,
                    'cointype': self.cointype,
                    'market': self.market,
                    'last_updated': datetime.utcnow().isoformat(),
                    'buy_orders': orders_to_save,
                    'sell_orders': existing_data.get('sell_orders', []),  # Preserve sell_orders
                    'metadata': existing_data.get('metadata', {})  # Preserve metadata
                }
                with open(file_path, 'w') as f:
                    json.dump(data, f, indent=2)
                self._info(
                    f"{self.symbol}: Cleaned up JSON - removed {removed_count} order(s) that don't match current holdings. "
                    f"Now contains {len(orders_to_save)} order(s) for tokens we actually have."
                )
            except Exception as e:
                logging.error(f"{self.symbol}: Failed to cleanup JSON file: {e}")
    
    # =========================================================================
    # PRICE ELEVATION TRACKING - Capital Preservation During High Prices
    # =========================================================================
    
    def _get_price_high_file_path(self) -> str:
        """Get the full path to the price high tracking file"""
        return self._price_elevation_file

    def _normalize_price_history_timestamp(self, ts: float) -> float:
        """Normalize persisted timestamps to Unix seconds (handles ms or str from JSON)."""
        if isinstance(ts, str):
            ts = float(ts)
        ts = float(ts)
        if ts > 1e12:
            ts /= 1000.0
        return ts

    def _filter_price_history_to_window(
        self, history: List[Tuple[float, float]], cutoff_time: float
    ) -> List[Tuple[float, float]]:
        return [(t, p) for t, p in history if t >= cutoff_time]

    def _recompute_rolling_price_stats(self):
        """Recompute rolling high/low/mean from price_high_history (percentile needs current_price)."""
        if not self.price_high_history:
            return
        prices = [p for _, p in self.price_high_history]
        n = len(prices)
        self.rolling_price_high = max(prices)
        self.rolling_price_low = min(prices)
        self.rolling_price_mean = sum(prices) / n
        if self.current_price > 0:
            below = sum(1 for p in prices if p < self.current_price)
            self.price_percentile = (below / n) * 100.0

    def _init_price_high_from_disk(self):
        """Load price distribution from disk at startup so restarts never start from a blank window."""
        file_path = self._get_price_high_file_path()
        cutoff_time = time.time() - self.price_elevation_window_hours * 3600
        raw_count = 0
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                self._price_high_file_metadata = {
                    k: data.get(k)
                    for k in ('seeded', 'seeded_at', 'data_source', 'real_historical_data')
                    if data.get(k) is not None
                }
                parsed = [
                    (self._normalize_price_history_timestamp(e['timestamp']), float(e['price']))
                    for e in data.get('price_history', [])
                ]
                raw_count = len(parsed)
                self.price_high_history = self._filter_price_history_to_window(parsed, cutoff_time)
                if raw_count >= self._MIN_PERCENTILE_SAMPLES and len(self.price_high_history) < self._MIN_PERCENTILE_SAMPLES:
                    ts_min = min(t for t, _ in parsed)
                    ts_max = max(t for t, _ in parsed)
                    logging.warning(
                        f"{self.symbol}: Price history window filter kept only "
                        f"{len(self.price_high_history)}/{raw_count} entries from {file_path} "
                        f"(cutoff {datetime.utcfromtimestamp(cutoff_time).strftime('%Y-%m-%d')}, "
                        f"file ts range {datetime.utcfromtimestamp(ts_min).strftime('%Y-%m-%d')} – "
                        f"{datetime.utcfromtimestamp(ts_max).strftime('%Y-%m-%d')}). "
                        f"Re-run spot_ladder/fetch_historical_prices.py --days "
                        f"{int(self.price_elevation_window_hours / 24)}"
                    )
            except Exception as e:
                logging.warning(f"{self.symbol}: Could not load price high history from {file_path}: {e}")
        else:
            logging.warning(
                f"{self.symbol}: No price history file at {file_path} — elevation percentile will be "
                f"neutral until samples accumulate or you run fetch_historical_prices.py"
            )

        if self.price_high_history:
            self._recompute_rolling_price_stats()
            oldest_ts = min(t for t, _ in self.price_high_history)
            span_days = (time.time() - oldest_ts) / 86400.0
            logging.info(
                f"{self.symbol}: Loaded price elevation history: {len(self.price_high_history)} samples "
                f"over {span_days:.0f}d from {file_path} "
                f"(rolling high ${self.rolling_price_high:.4f})"
            )
        elif raw_count > 0:
            logging.warning(
                f"{self.symbol}: Price history file had {raw_count} entries but none within the "
                f"{self.price_elevation_window_hours / 24:.0f}d window — re-backfill recommended"
            )
    
    def _load_price_high_history(self) -> List[Tuple[float, float]]:
        """Load price high history from JSON file (used if startup load was skipped)."""
        file_path = self._get_price_high_file_path()
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                self._price_high_file_metadata = {
                    k: data.get(k)
                    for k in ('seeded', 'seeded_at', 'data_source', 'real_historical_data')
                    if data.get(k) is not None
                }
                cutoff_time = time.time() - self.price_elevation_window_hours * 3600
                parsed = [
                    (self._normalize_price_history_timestamp(e['timestamp']), float(e['price']))
                    for e in data.get('price_history', [])
                ]
                return self._filter_price_history_to_window(parsed, cutoff_time)
            except Exception as e:
                logging.warning(f"{self.symbol}: Could not load price high history: {e}")
        return []
    
    def _save_price_high_history(self):
        """Save the rolling price distribution to JSON for persistence across restarts"""
        file_path = self._get_price_high_file_path()
        try:
            history = [{'timestamp': t, 'price': p} for t, p in self.price_high_history]
            new_count = len(history)

            existing_count = 0
            if os.path.exists(file_path):
                try:
                    with open(file_path, 'r') as f:
                        existing_data = json.load(f)
                    existing_count = len(existing_data.get('price_history', []))
                    for key in ('seeded', 'seeded_at', 'data_source', 'real_historical_data'):
                        if existing_data.get(key) is not None:
                            self._price_high_file_metadata[key] = existing_data.get(key)
                except Exception:
                    pass

            if existing_count >= 50 and new_count < max(10, int(existing_count * 0.25)):
                logging.error(
                    f"{self.symbol}: Refusing to save price history ({new_count} samples) — would "
                    f"overwrite {existing_count} entries in {file_path}. Re-run fetch_historical_prices.py"
                )
                return

            data = {
                'symbol': self.symbol,
                'last_updated': datetime.utcnow().isoformat(),
                'rolling_high': self.rolling_price_high,
                'rolling_low': self.rolling_price_low,
                'rolling_mean': self.rolling_price_mean,
                'price_percentile': self.price_percentile,
                'window_hours': self.price_elevation_window_hours,
                'price_history': history,
            }
            data.update(self._price_high_file_metadata)
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logging.warning(f"{self.symbol}: Failed to save price history: {e}")
    
    # Minimum samples required before the percentile signal is trusted. Below this
    # the distribution is too sparse to be meaningful, so we stay neutral (full ladder).
    _MIN_PERCENTILE_SAMPLES = 10
    
    def _update_rolling_price_high(self):
        """Update the rolling price distribution stats over the configured window.
        
        Maintains the window of observed prices and recomputes the high, low, mean,
        and the percentile rank of the current price within that distribution.
        The percentile drives price_elevation buy-ladder tiers (see _get_price_elevation_tier).
        Separate from Core/Working mean_reversion sell logic.
        """
        if not self.price_elevation_enabled:
            return
        
        current_time = time.time()
        current_price = self.current_price
        
        if current_price <= 0:
            return
        
        # Load persisted history on first call (normally already loaded at startup)
        if not self.price_high_history and not self._price_high_file_metadata:
            self.price_high_history = self._load_price_high_history()
        
        # Add current price to history
        self.price_high_history.append((current_time, current_price))
        
        # Calculate cutoff time based on window
        window_seconds = self.price_elevation_window_hours * 3600
        cutoff_time = current_time - window_seconds
        
        before_filter = len(self.price_high_history)
        self.price_high_history = self._filter_price_history_to_window(self.price_high_history, cutoff_time)
        if before_filter >= self._MIN_PERCENTILE_SAMPLES and len(self.price_high_history) < self._MIN_PERCENTILE_SAMPLES:
            logging.warning(
                f"{self.symbol}: Price history dropped to {len(self.price_high_history)} samples "
                f"after window filter (had {before_filter}) — check {self._get_price_high_file_path()}"
            )
        
        if not self.price_high_history:
            return
        
        prices = [p for _, p in self.price_high_history]
        n = len(prices)
        
        old_high = self.rolling_price_high
        self.rolling_price_high = max(prices)
        self.rolling_price_low = min(prices)
        self.rolling_price_mean = sum(prices) / n
        
        # Percentile rank of the current price within the window distribution:
        # fraction of observations strictly below current price. 0 = cheapest the
        # asset has been, 100 = the most expensive. This uses the whole distribution
        # (not a single extreme) so it is robust to flash wicks and stale peaks.
        below = sum(1 for p in prices if p < current_price)
        self.price_percentile = (below / n) * 100.0
        
        # Log significant changes in the rolling high for context
        if old_high > 0 and abs(self.rolling_price_high - old_high) / old_high > 0.01:
            window_days = self.price_elevation_window_hours / 24
            self._info(f"{self.symbol}: Rolling {window_days:.0f}-day high updated: "
                       f"${old_high:.4f} -> ${self.rolling_price_high:.4f}")
        
        # Save to disk every 5 OrderManager cycles (~10 min at default loop_interval 120s)
        self._price_high_save_counter += 1
        if self._price_high_save_counter >= 5:
            self._save_price_high_history()
            self._price_high_save_counter = 0
    
    def _get_price_elevation_tier(self) -> Dict:
        """Select the buy-ladder tier from the current price's percentile rank.
        
        The anchor is the percentile rank of the current price within the rolling
        window distribution: cheap prices (low percentile) get aggressive allocation
        and the full ladder; expensive prices (high percentile) get reduced allocation
        and skip shallow rungs. Independent of Core/Working sell mean_reversion.
        
        Returns the matched tier dict plus:
        - percentile: the current price's percentile rank (0-100)
        - position_pct: how far below the rolling high we are (context only)
        - tier_name: descriptive name for the tier
        
        Tiers are matched by max_percentile (upper percentile bound): the cheapest
        matching tier wins, i.e. the first tier (ordered by max_percentile ascending)
        whose max_percentile >= the current percentile.
        
        NOTE: With LIFO implementation, this protection still provides value by preserving
        capital for cheaper entry opportunities. See detailed analysis in place_buy_ladder().
        """
        # Neutral default (full allocation, no skips) when disabled or when the
        # distribution is too sparse to produce a trustworthy percentile.
        if (not self.price_elevation_enabled or self.rolling_price_high <= 0
                or len(self.price_high_history) < self._MIN_PERCENTILE_SAMPLES):
            return {
                'max_percentile': 100,
                'allocation': 100,
                'skip_levels': 0,
                'percentile': self.price_percentile,
                'position_pct': 0,
                'tier_name': 'neutral (insufficient data)' if self.price_elevation_enabled else 'disabled'
            }
        
        percentile = self.price_percentile
        position_pct = (self.rolling_price_high - self.current_price) / self.rolling_price_high * 100
        
        # Cheapest matching tier wins: first tier (ascending max_percentile) whose
        # bound is at or above the current percentile.
        sorted_tiers = sorted(self.price_elevation_tiers, key=lambda x: x.get('max_percentile', 100))
        for tier in sorted_tiers:
            if percentile <= tier.get('max_percentile', 100):
                return {
                    **tier,
                    'percentile': percentile,
                    'position_pct': position_pct,
                    'tier_name': f"<={tier.get('max_percentile', 100):.0f}th pct"
                }
        
        # Fallback: most conservative tier (lowest allocation) if nothing matched.
        most_conservative = min(self.price_elevation_tiers, key=lambda x: x.get('allocation', 100))
        return {
            **most_conservative,
            'percentile': percentile,
            'position_pct': position_pct,
            'tier_name': 'most conservative'
        }

    def _load_position_aware_config(self):
        """Load position-aware buy allocation settings from price_elevation.position_aware."""
        pa_config = self.price_elevation_config.get('position_aware', {})
        self.position_aware_enabled = pa_config.get('enabled', False)
        self.position_aware_low_token_pct = pa_config.get('low_token_threshold_pct', 30.0)
        self.position_aware_high_token_pct = pa_config.get('high_token_threshold_pct', 70.0)
        self.position_aware_boost_allocation_pct = pa_config.get('boost_allocation_pct', 75.0)
        self.position_aware_max_boost_percentile = pa_config.get('max_boost_percentile', 80.0)
        if self.position_aware_max_boost_percentile >= 100:
            logging.warning(
                f"{self.symbol}: position_aware max_boost_percentile must be < 100 — using 80"
            )
            self.position_aware_max_boost_percentile = 80.0
        if self.position_aware_high_token_pct <= self.position_aware_low_token_pct:
            logging.warning(
                f"{self.symbol}: position_aware high_token_threshold_pct must exceed "
                f"low_token_threshold_pct — disabling position-aware allocation"
            )
            self.position_aware_enabled = False

    def _get_portfolio_token_pct(self) -> Optional[float]:
        """Token value as % of total symbol portfolio (coins + quote)."""
        if self.current_price <= 0 or self.balance_percentage <= 0:
            return None
        coin_value = self.coin_balance * self.current_price
        quote_balance = self.available_balance / self.balance_percentage
        total = coin_value + quote_balance
        if total <= 0:
            return None
        return (coin_value / total) * 100.0

    def _load_entry_aware_config(self):
        """Load entry-aware buy allocation floor from price_elevation.entry_aware."""
        ea_config = self.price_elevation_config.get('entry_aware', {})
        self.entry_aware_enabled = ea_config.get('enabled', False)
        self.entry_aware_min_discount_pct = ea_config.get('min_discount_pct', 15.0)
        self.entry_aware_allocation_floor_pct = ea_config.get('allocation_floor_pct', 50.0)
        self.entry_aware_extra_per_10_pct = ea_config.get('extra_per_10_pct_below', 5.0)
        self.entry_aware_max_allocation_pct = ea_config.get('max_allocation_pct', 65.0)

    def _get_entry_aware_allocation_floor(self) -> float:
        """Minimum buy allocation when price is materially below average entry (recovery context)."""
        if not self.entry_aware_enabled:
            return 0.0
        if self.average_entry_price <= 0 or self.current_price <= 0:
            return 0.0
        discount_pct = (
            (self.average_entry_price - self.current_price) / self.average_entry_price * 100.0
        )
        if discount_pct < self.entry_aware_min_discount_pct:
            return 0.0
        extra_pct = discount_pct - self.entry_aware_min_discount_pct
        floor = self.entry_aware_allocation_floor_pct + (
            extra_pct / 10.0 * self.entry_aware_extra_per_10_pct
        )
        return min(self.entry_aware_max_allocation_pct, floor)

    def _get_effective_buy_allocation_pct(self, elevation_allocation_pct: float, percentile: float = 0.0) -> float:
        """Blend price-elevation allocation with position-aware boost when underweight tokens.

        Boost is scaled down at elevated percentiles so cash-heavy portfolios are not pushed
        to commit heavily while price is still expensive vs the rolling window (avoids filling
        a large ladder on the way down from a local high).

        Entry-aware floor applies after blending: underwater vs avg entry overrides local-high
        conservatism (recovery buys are still attractive even at a rolling-window high).
        """
        effective = elevation_allocation_pct
        if self.position_aware_enabled:
            token_pct = self._get_portfolio_token_pct()
            if token_pct is not None:
                low = self.position_aware_low_token_pct
                high = self.position_aware_high_token_pct
                boost = self.position_aware_boost_allocation_pct
                max_boost_pct = self.position_aware_max_boost_percentile

                if token_pct <= low:
                    target = max(elevation_allocation_pct, boost)
                elif token_pct >= high:
                    target = elevation_allocation_pct
                else:
                    blend = (token_pct - low) / (high - low)
                    cash_heavy = max(elevation_allocation_pct, boost)
                    target = cash_heavy * (1 - blend) + elevation_allocation_pct * blend

                fade_end = 99.5
                if percentile <= max_boost_pct:
                    boost_factor = 1.0
                elif percentile >= fade_end or max_boost_pct >= fade_end:
                    boost_factor = 0.0
                else:
                    boost_factor = max(0.0, (fade_end - percentile) / (fade_end - max_boost_pct))

                effective = elevation_allocation_pct + (target - elevation_allocation_pct) * boost_factor

        entry_floor = self._get_entry_aware_allocation_floor()
        if entry_floor > effective:
            effective = entry_floor
        return min(100.0, max(0.0, effective))

    def _apply_buy_ladder_quote_cap(self, amount: float) -> float:
        """Cap total buy-ladder deployment in quote (0 = no cap)."""
        if self.max_buy_ladder_usdc <= 0:
            return amount
        return min(amount, self.max_buy_ladder_usdc)

    def _occupied_buy_level_indices(
        self,
        orders: List,
        levels: List[float],
        reference_price: float,
        tolerance_pct: float = 0.5,
    ) -> set:
        """Return effective-level indices that already have a resting order near the expected price."""
        occupied = set()
        if reference_price <= 0 or not levels:
            return occupied
        for order in orders:
            for idx, level_pct in enumerate(levels):
                if idx in occupied:
                    continue
                expected = reference_price * (1 - level_pct / 100.0)
                if expected <= 0:
                    continue
                if abs((order.rate - expected) / expected) * 100.0 < tolerance_pct:
                    occupied.add(idx)
                    break
        return occupied

    def _missing_buy_level_pcts(
        self,
        orders: List,
        levels: List[float],
        reference_price: float,
    ) -> List[float]:
        """Discount % of effective rungs that are not currently on the book."""
        occupied = self._occupied_buy_level_indices(orders, levels, reference_price)
        return [levels[i] for i in range(len(levels)) if i not in occupied]

    def _should_full_rebuild_buy_ladder(
        self,
        existing_buy_orders: List,
        effective_levels: List[float],
        price_moved_significantly: bool,
    ) -> Tuple[bool, str]:
        """Whether to cancel remaining buy rungs and rebuild the whole ladder.

        Shallow fills (0.5/1/2%) must not cannibalise -7/-10/-15% insurance.
        Rebuild only when price has moved by price_update_threshold, or a real
        dip has filled a rung at/deeper than buy_ladder_rebuild_on_fill_pct.
        """
        if price_moved_significantly:
            return True, (
                f"price moved ≥{self.price_update_threshold:.1f}% from placement "
                f"(reposition entire ladder)"
            )
        if not existing_buy_orders or not effective_levels:
            return False, ""
        reference_price = (
            self.price_when_orders_placed
            if self.price_when_orders_placed > 0
            else self.current_price
        )
        missing_pcts = self._missing_buy_level_pcts(
            existing_buy_orders, effective_levels, reference_price
        )
        rebuild_at = self.buy_ladder_rebuild_on_fill_pct
        deep_missing = [p for p in missing_pcts if p >= rebuild_at]
        if deep_missing:
            return True, (
                f"{min(deep_missing):.1f}%+ rung filled "
                f"(rebuild threshold {rebuild_at:.1f}%; missing {deep_missing})"
            )
        return False, ""

    def _gross_buy_deployable(self) -> float:
        """Quote available for buy-ladder deployment after fee buffer (before allocation % or cap)."""
        if self.available_balance <= 0:
            return 0.0
        return (self.available_balance / (1.0 + self.buy_fee)) * 0.99

    def _intended_buy_committed(self, allocation_pct: float) -> float:
        """Quote the buy ladder is supposed to commit at the current allocation.

        Elevation/position-aware allocation only applies when sizing orders — it does
        not reduce available_balance. Callers must not treat the leftover as unused capital.
        Applies max_buy_ladder_usdc when configured.
        """
        if self.available_balance <= 0:
            return 0.0
        deploy_pct = min(100.0, max(0.0, allocation_pct)) / 100.0
        amount = self._gross_buy_deployable() * deploy_pct
        return self._apply_buy_ladder_quote_cap(amount)

    def _log_buy_deployment_summary(
        self,
        allocation_pct: float,
        elevation_allocation_pct: float,
        existing_buy_committed: float,
        existing_order_count: int,
        needed_orders: int,
        skip_levels: int = 0,
    ):
        """One-place summary of how allocation %, entry floor, and quote cap set ladder size."""
        gross = self._gross_buy_deployable()
        at_pct = gross * (allocation_pct / 100.0)
        intended = self._intended_buy_committed(allocation_pct)
        entry_floor = self._get_entry_aware_allocation_floor()
        portfolio_token_pct = self._get_portfolio_token_pct()

        chain_parts = [f"elevation tier {elevation_allocation_pct:.0f}%"]
        if entry_floor > elevation_allocation_pct:
            if self.average_entry_price > 0 and self.current_price > 0:
                disc = (self.average_entry_price - self.current_price) / self.average_entry_price * 100.0
                chain_parts.append(
                    f"entry floor {entry_floor:.0f}% ({disc:.0f}% below entry ${self.average_entry_price:.4f})"
                )
            else:
                chain_parts.append(f"entry floor {entry_floor:.0f}%")
            chain_parts.append(f"effective {allocation_pct:.0f}%")
        if (self.position_aware_enabled
                and portfolio_token_pct is not None
                and allocation_pct != elevation_allocation_pct
                and allocation_pct != entry_floor):
            chain_parts.append(
                f"position-aware → {allocation_pct:.0f}% (tokens {portfolio_token_pct:.0f}% of portfolio)"
            )
        if allocation_pct == elevation_allocation_pct and entry_floor <= elevation_allocation_pct:
            chain_parts.append(f"effective {allocation_pct:.0f}%")

        limit_parts = [f"{allocation_pct:.0f}% of ${gross:.0f} deployable → ${at_pct:.0f}"]
        if self.max_buy_ladder_usdc > 0:
            if intended < at_pct - 0.5:
                limit_parts.append(f"cap ${self.max_buy_ladder_usdc:.0f} binding → ${intended:.0f} ladder")
            else:
                limit_parts.append(f"cap ${self.max_buy_ladder_usdc:.0f} (not binding)")
        else:
            limit_parts.append(f"→ ${intended:.0f} ladder target")

        intentional_reserve = max(0.0, self.available_balance - intended)
        deployable_gap = intended - existing_buy_committed
        if existing_buy_committed > intended + self.increased_capital_threshold:
            gap_label = f"OVER by ${existing_buy_committed - intended:.0f}"
        elif abs(deployable_gap) <= self.increased_capital_threshold:
            gap_label = "at target"
        else:
            gap_label = f"room to add ${deployable_gap:.0f}"

        levels_note = f"{needed_orders} levels"
        if skip_levels > 0:
            levels_note += f" (skip {skip_levels} shallow)"

        self._info(
            f"{self.symbol}: Buy deployment summary\n"
            f"  orders: {existing_order_count}/{needed_orders} ({levels_note})\n"
            f"  balance: ${self.available_balance / self.balance_percentage:.0f} base → "
            f"${self.available_balance:.0f} available ({self.balance_percentage * 100:.0f}% slice)\n"
            f"  allocation: {' → '.join(chain_parts)}\n"
            f"  ladder $: {'; '.join(limit_parts)}\n"
            f"  committed: ${existing_buy_committed:.0f} | "
            f"intentional reserve: ${intentional_reserve:.0f} | "
            f"gap: ${deployable_gap:+.0f} ({gap_label})"
        )
        
    def update_balances(self, balances: Dict):
        """Update available balances for this symbol"""
        # Store previous balances before updating (for fill verification)
        self.previous_coin_balance = self.coin_balance
        self.previous_available_balance = self.available_balance
        
        # Get base currency balance (USDC / quote)
        base_balance = balances.get(self.market, {}).get('balance', 0.0)
        self.available_balance = base_balance * self.balance_percentage
        
        # Get coin balance
        self.coin_balance = balances.get(self.cointype, {}).get('balance', 0.0)
        
        logging.debug(f"{self.symbol}: Available {self.market}: {self.available_balance:.2f}, "
                     f"Coin balance: {self.coin_balance:.8f}")

    def _min_working_coins_threshold(self) -> float:
        """Minimum Working slice size (coins) — one min-notional sell, not the full-position 10-coin floor."""
        if self.current_price > 0:
            return max(self.min_order_size / self.current_price, 1e-8)
        return 10.0
    
    def update_price(self, price: float):
        """Update current price and track price movement"""
        self.last_price = self.current_price
        self.current_price = price
        
        if self.last_price == 0:
            self.last_price = price
        
        # Track price history for time window checks
        current_time = time.time()
        self.price_history.append((current_time, price))
        
        # Keep only prices within the longest time window (plus a buffer)
        # Use the longest of all windows to ensure we have data for all checks
        max_window = max(self.price_update_time_window, self.price_update_short_window, self.price_update_quick_window)
        cutoff_time = current_time - (max_window * 1.5)  # Keep 1.5x window for buffer
        self.price_history = [(t, p) for t, p in self.price_history if t >= cutoff_time]
    
    def update_open_orders(self, orders_data: Dict):
        """Update list of open orders from API response"""
        if not orders_data or orders_data.get('status') == 'error':
            logging.error(
                f"{self.symbol}: Refusing to update open_orders from failed fetch "
                f"({(orders_data or {}).get('message', 'no data')})"
            )
            return False

        self.open_orders = []
        
        # Parse orders from API response
        # exchange API may return orders in different formats
        orders_list = []
        
        if 'orders' in orders_data:
            orders_list = orders_data['orders']
        elif isinstance(orders_data, list):
            orders_list = orders_data
        elif 'buyorders' in orders_data or 'sellorders' in orders_data:
            # Some APIs separate buy and sell orders
            buy_orders_list = orders_data.get('buyorders', [])
            sell_orders_list = orders_data.get('sellorders', [])
            logging.debug(f"{self.symbol}: Found {len(buy_orders_list)} buy orders and {len(sell_orders_list)} sell orders in API response")
            
            # Process buy orders - handle duplicate order IDs (could be partial fills)
            # Group by order ID and sum amounts if same ID appears multiple times
            orders_by_id = {}
            
            for order_data in buy_orders_list:
                try:
                    # Only process orders for this symbol
                    coin = order_data.get('coin', '').upper()
                    if coin and coin != self.cointype:
                        continue
                    
                    order_id = str(order_data.get('id', ''))
                    amount = float(order_data.get('amount', 0))
                    rate = float(order_data.get('rate', 0))
                    
                    if order_id and amount > 0 and rate > 0:
                        if order_id not in orders_by_id:
                            orders_by_id[order_id] = {
                                'amounts': [],
                                'rates': [],
                                'total_amount': 0,
                                'total_value': 0
                            }
                        
                        # Accumulate amounts and values for weighted average calculation
                        orders_by_id[order_id]['amounts'].append(amount)
                        orders_by_id[order_id]['rates'].append(rate)
                        orders_by_id[order_id]['total_amount'] += amount
                        orders_by_id[order_id]['total_value'] += amount * rate
                except (ValueError, KeyError, TypeError) as e:
                    logging.debug(f"{self.symbol}: Failed to parse buy order: {order_data}, error: {e}")
            
            # Create Order objects from consolidated data
            for order_id, order_info in orders_by_id.items():
                total_amount = order_info['total_amount']
                total_value = order_info['total_value']
                
                # Calculate weighted average rate if multiple fills
                if len(order_info['amounts']) > 1:
                    weighted_avg_rate = total_value / total_amount if total_amount > 0 else 0
                    self._info(f"{self.symbol}: Order {order_id[:20]}... has {len(order_info['amounts'])} partial fills - "
                               f"consolidating: {total_amount:.8f} XRP @ weighted avg ${weighted_avg_rate:.4f} "
                               f"(fills: {', '.join([f'{a:.2f}@{r:.4f}' for a, r in zip(order_info['amounts'], order_info['rates'])])})")
                    avg_rate = weighted_avg_rate
                else:
                    avg_rate = order_info['rates'][0]
                
                order = Order(
                    order_id=order_id,
                    symbol=self.symbol,
                    side='buy',
                    amount=total_amount,
                    rate=avg_rate,
                    market=self.market
                )
                self.open_orders.append(order)
                # Track order ID for fill detection
                if not hasattr(self, 'previous_order_ids'):
                    self.previous_order_ids = set()
                self.previous_order_ids.add(order.order_id)
            
            # Process sell orders - handle duplicate order IDs (could be partial fills)
            # Group by order ID and sum amounts if same ID appears multiple times
            sell_orders_by_id = {}
            
            for order_data in sell_orders_list:
                try:
                    # Only process orders for this symbol
                    coin = order_data.get('coin', '').upper()
                    if coin and coin != self.cointype:
                        continue
                    
                    order_id = str(order_data.get('id', ''))
                    amount = float(order_data.get('amount', 0))
                    rate = float(order_data.get('rate', 0))
                    
                    if order_id and amount > 0 and rate > 0:
                        if order_id not in sell_orders_by_id:
                            sell_orders_by_id[order_id] = {
                                'amounts': [],
                                'rates': [],
                                'total_amount': 0,
                                'total_value': 0
                            }
                        
                        # Accumulate amounts and values for weighted average calculation
                        sell_orders_by_id[order_id]['amounts'].append(amount)
                        sell_orders_by_id[order_id]['rates'].append(rate)
                        sell_orders_by_id[order_id]['total_amount'] += amount
                        sell_orders_by_id[order_id]['total_value'] += amount * rate
                except (ValueError, KeyError, TypeError) as e:
                    logging.debug(f"{self.symbol}: Failed to parse sell order: {order_data}, error: {e}")
            
            # Create Order objects from consolidated data
            for order_id, order_info in sell_orders_by_id.items():
                total_amount = order_info['total_amount']
                total_value = order_info['total_value']
                
                # Calculate weighted average rate if multiple fills
                if len(order_info['amounts']) > 1:
                    weighted_avg_rate = total_value / total_amount if total_amount > 0 else 0
                    self._info(f"{self.symbol}: Sell order {order_id[:20]}... has {len(order_info['amounts'])} partial fills - "
                               f"consolidating: {total_amount:.8f} XRP @ weighted avg ${weighted_avg_rate:.4f} "
                               f"(fills: {', '.join([f'{a:.2f}@{r:.4f}' for a, r in zip(order_info['amounts'], order_info['rates'])])})")
                    avg_rate = weighted_avg_rate
                else:
                    avg_rate = order_info['rates'][0]
                
                order = Order(
                    order_id=order_id,
                    symbol=self.symbol,
                    side='sell',
                    amount=total_amount,
                    rate=avg_rate,
                    market=self.market
                )
                self.open_orders.append(order)
                # Track order ID for fill detection
                if not hasattr(self, 'previous_order_ids'):
                    self.previous_order_ids = set()
                self.previous_order_ids.add(order.order_id)
            
            return  # Already processed, don't process again below
        
        # Fallback: process as single list (for other API formats)
        for order_data in orders_list:
            try:
                # Handle different field name variations
                order_id = str(order_data.get('id') or order_data.get('orderid') or order_data.get('oid', ''))
                side = str(order_data.get('type') or order_data.get('side') or order_data.get('ordertype', '')).lower()
                amount = float(order_data.get('amount') or order_data.get('qty') or order_data.get('quantity', 0))
                rate = float(order_data.get('rate') or order_data.get('price') or order_data.get('rate', 0))
                
                if order_id and amount > 0 and rate > 0:
                    order = Order(
                        order_id=order_id,
                        symbol=self.symbol,
                        side=side,
                        amount=amount,
                        rate=rate,
                        market=self.market
                    )
                    self.open_orders.append(order)
                    # Track order ID for fill detection
                    if not hasattr(self, 'previous_order_ids'):
                        self.previous_order_ids = set()
                    self.previous_order_ids.add(order.order_id)
            except (ValueError, KeyError, TypeError) as e:
                logging.debug(f"{self.symbol}: Failed to parse order data: {order_data}, error: {e}")
    
    def calculate_average_entry(self) -> Tuple[float, float]:
        """Calculate average entry price based on current holdings
        
        Returns: (average_entry_price, coin_amount)
        Always uses current coin_balance as coin_amount to ensure we have the latest balance
        
        IMPORTANT: ONLY uses stored JSON buy orders. No API fallback, no tracked values.
        This ensures sell orders are always based on YOUR actual historical buy prices,
        not other users' orders or estimates that could skew results.
        """
        # Use current coin_balance as the source of truth for coin amount
        current_coin_amount = self.coin_balance
        
        # --- Subsection: Average Entry Calculation (LIFO) ---
        # ONLY use stored filled orders from JSON - this is the single source of truth
        stored_orders = self._get_stored_filled_orders()
        if stored_orders:
            self._info(f"{self.symbol}: Using {len(stored_orders)} stored filled buy orders for average entry calculation")
            
            total_cost = 0.0
            total_amount = 0.0
            
            for i, order in enumerate(stored_orders, 1):
                # CRITICAL FIX: Use remaining amount (after LIFO consumption) instead of full amount
                # This ensures we only count coins we actually hold, not coins that were sold
                remaining = order.get('_remaining_amount', float(order.get('amount', 0)))
                rate = float(order.get('rate', 0))
                
                if remaining > 0 and rate > 0:
                    total_cost += remaining * rate
                    total_amount += remaining
                    fill_timestamp = order.get('fill_timestamp', 'unknown')
                    try:
                        fill_dt = datetime.fromisoformat(fill_timestamp.replace('Z', '+00:00'))
                        formatted_date = fill_dt.strftime('%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        formatted_date = fill_timestamp
                    
                    original_amount = float(order.get('amount', 0))
                    consumed = float(order.get('consumed_amount', 0))
                    if consumed > 0:
                        self._info(f"{self.symbol}: Using stored buy order #{i}: {remaining:.8f} {self.cointype} @ {rate:.4f} {self.market} "
                                    f"(date: {formatted_date}, remaining of {original_amount:.8f}, {consumed:.8f} consumed, total: ${remaining * rate:.2f} {self.market})")
                    else:
                        self._info(f"{self.symbol}: Using stored buy order #{i}: {remaining:.8f} {self.cointype} @ {rate:.4f} {self.market} "
                                    f"(date: {formatted_date}, total: ${remaining * rate:.2f} {self.market})")
            
            if total_amount > 0:
                self.total_coins = total_amount
                self.total_invested = total_cost
                self.average_entry_price = total_cost / total_amount
                self._info(f"{self.symbol}: Calculated average entry from stored orders: {self.average_entry_price:.4f} "
                            f"(from {len(stored_orders)} buy orders, {total_amount:.8f} coins, ${total_cost:.2f} invested)")
                
                # NOTE: We do NOT cleanup the JSON file here anymore.
                # LIFO exclusion in _get_stored_filled_orders() is only for calculation purposes.
                # All historical orders should remain in the JSON for tax/accounting purposes.
                # The JSON file is the complete historical record of all buy orders.
                
                return self.average_entry_price, current_coin_amount
        
        # No stored orders found - JSON storage is the ONLY source of truth
        # We do NOT use API fallback or tracked values to avoid skewing results
        if current_coin_amount > 0:
            logging.error(f"{self.symbol}: ⚠️ CRITICAL: Have {current_coin_amount:.8f} coins but NO stored buy orders in JSON file. "
                         f"Cannot calculate accurate average entry. Sell orders will NOT be placed until buy orders are stored. "
                         f"This usually means the bot was restarted before any buy orders were saved to JSON.")
            # Set to 0 to prevent sell orders from being placed with wrong average entry
            self.average_entry_price = 0.0
            self.total_coins = 0.0
            self.total_invested = 0.0
        else:
            # No coins, no problem
            self.average_entry_price = 0.0
            self.total_coins = 0.0
            self.total_invested = 0.0
        
        return self.average_entry_price, current_coin_amount
    
    def get_buy_orders(self) -> List[Order]:
        """Get all open buy orders"""
        return [o for o in self.open_orders if o.side == 'buy']
    
    def get_sell_orders(self) -> List[Order]:
        """Get all open sell orders"""
        return [o for o in self.open_orders if o.side == 'sell']
    
    def _calculate_fifo_cost_basis_marginal(self, start_offset: float, sell_amount: float) -> float:
        """Calculate the LIFO-based cost basis for a specific slice of coins
        
        This simulates LIFO consumption and returns the weighted average cost
        of coins from position 'start_offset' to 'start_offset + sell_amount'.
        Note: Function name kept as _calculate_fifo_cost_basis_marginal for compatibility,
        but now uses LIFO logic (newest first).
        
        Uses ALL non-consumed buy orders (not balance-trimmed) to avoid
        double-counting with consumed_amount tracking.
        
        Args:
            start_offset: Coins already consumed before this order
            sell_amount: Amount of coins being sold in this order
            
        Returns:
            Weighted average cost per coin for this specific slice
        """
        raw_orders = self._load_filled_orders()
        stored_orders = []
        for order in raw_orders:
            if order.get('fully_consumed', False):
                continue
            original_amount = float(order.get('amount', 0))
            consumed_amount = float(order.get('consumed_amount', 0))
            remaining = original_amount - consumed_amount
            if remaining > 0.0001:
                order_copy = order.copy()
                order_copy['_remaining_amount'] = remaining
                stored_orders.append(order_copy)
        if not stored_orders:
            return self.average_entry_price  # Fallback to overall average
        
        # Sort by timestamp (newest first) for LIFO
        stored_orders.sort(key=lambda x: x.get('fill_timestamp', ''), reverse=True)
        
        # First, skip past the already-consumed offset
        offset_remaining = start_offset
        order_idx = 0
        order_position = 0.0  # How much we've used from current order
        
        for i, order in enumerate(stored_orders):
            remaining = float(order.get('_remaining_amount', order.get('amount', 0)))
            if remaining <= 0:
                continue
            
            if offset_remaining <= remaining:
                order_idx = i
                order_position = offset_remaining
                break
            offset_remaining -= remaining
        
        # Now consume 'sell_amount' starting from this position
        amount_to_consume = sell_amount
        total_cost = 0.0
        total_consumed = 0.0
        
        for order in stored_orders[order_idx:]:
            if amount_to_consume <= 0:
                break
            
            remaining = float(order.get('_remaining_amount', order.get('amount', 0)))
            rate = float(order.get('rate', 0))
            
            if remaining <= 0 or rate <= 0:
                continue
            
            # Adjust for partial consumption at start of first order
            if order_position > 0:
                remaining -= order_position
                order_position = 0
            
            if remaining <= 0:
                continue
            
            consume_from_this = min(remaining, amount_to_consume)
            total_cost += consume_from_this * rate
            total_consumed += consume_from_this
            amount_to_consume -= consume_from_this
        
        if total_consumed > 0:
            return total_cost / total_consumed
        return self.average_entry_price  # Fallback
    
    def log_position_health(self):
        """Log position health indicator showing P&L and sell order profitability"""
        if self.coin_balance <= 0:
            self._info(f"{self.symbol}: No position - coin balance: {self.coin_balance:.8f}")
            return
        
        if self.average_entry_price <= 0:
            self._info(f"{self.symbol}: Position health unavailable - no average entry price calculated")
            return
        
        # Use current_price for valuation (real-time price from API)
        # Note: Daily summary uses closing_price for a specific date, which may differ slightly
        # The cost basis calculation is identical in both, so when prices are similar, P&L will be close
        # Priority: current_price > mid_price > bid_price > last_price (last_price is previous price, not API last)
        valuation_price = self.current_price if self.current_price > 0 else (self.mid_price if self.mid_price > 0 else (self.bid_price if self.bid_price > 0 else self.last_price))
        
        if valuation_price <= 0:
            self._info(f"{self.symbol}: Position health unavailable - no valid price data")
            return
        
        # Calculate position metrics using EXACT SAME method as daily summary
        # Import and use the same function that daily summary uses for consistency
        try:
            from spot_ladder.daily_summary import calculate_average_entry_from_stored_orders
            avg_entry_ds, fifo_coins, total_cost_ds = calculate_average_entry_from_stored_orders(
                cointype=self.cointype,
                current_balance=self.coin_balance,
                state_dir=self._state_dir,
            )
            # Scale cost basis to match actual balance (same as daily summary)
            if fifo_coins > 0 and self.coin_balance > 0:
                total_invested = total_cost_ds * (self.coin_balance / fifo_coins)
            else:
                total_invested = total_cost_ds
        except Exception as e:
            # Fallback to order_manager's own calculation if daily_summary function unavailable
            logging.debug(f"{self.symbol}: Could not use daily_summary calculation, using fallback: {e}")
            if self.total_invested > 0 and self.total_coins > 0 and self.coin_balance > 0:
                total_invested = self.total_invested * (self.coin_balance / self.total_coins)
            else:
                total_invested = self.coin_balance * self.average_entry_price
        
        current_value = self.coin_balance * valuation_price
        unrealized_pnl = current_value - total_invested
        unrealized_pnl_pct = (unrealized_pnl / total_invested * 100) if total_invested > 0 else 0
        
        # Determine health status
        if unrealized_pnl_pct > 5:
            health_status = "🟢 STRONG"
        elif unrealized_pnl_pct > 0:
            health_status = "🟡 PROFITABLE"
        elif unrealized_pnl_pct > -5:
            health_status = "🟠 SLIGHT LOSS"
        else:
            health_status = "🔴 SIGNIFICANT LOSS"
        
        # Log position health (summary only - order lists moved to respective ladder sections)
        # Note: Current price is already shown in Price Information section above
        # Cost basis calculated using same method as daily summary for consistency
        self._info(f"{self.symbol}: 📊 POSITION HEALTH")
        self._info(f"  Position: {self.coin_balance:.8f} {self.cointype}")
        self._info(f"  Average Entry: ${self.average_entry_price:.4f} {self.market}")
        self._info(f"  Total Invested: ${total_invested:.2f} {self.market}")
        self._info(f"  Current Value: ${current_value:.2f} {self.market} (at ${valuation_price:.4f} {self.market})")
        self._info(f"  Unrealized P&L: ${unrealized_pnl:+.2f} {self.market} ({unrealized_pnl_pct:+.2f}%)")
        self._info(f"  Status: {health_status}")
    
    def log_buy_orders_list(self):
        """Log detailed buy orders list (called from buy ladder section)"""
        buy_orders = self.get_buy_orders()
        if buy_orders:
            # Sort by price (highest first) to show which will execute first (closest to current price)
            sorted_buy_orders = sorted(buy_orders, key=lambda o: o.rate, reverse=True)
            highest_buy_price = sorted_buy_orders[0].rate if sorted_buy_orders else 0
            
            total_buy_committed = sum(o.amount * o.rate for o in buy_orders)
            self._info(f"{self.symbol}:   📉 BUY ORDERS ({len(buy_orders)} orders):")
            
            # Show execution requirement
            if self.ask_price > 0 and highest_buy_price > 0:
                price_to_hit = highest_buy_price
                price_gap = self.ask_price - price_to_hit
                price_gap_pct = (price_gap / self.ask_price * 100) if self.ask_price > 0 else 0
                if price_gap > 0:
                    self._info(f"{self.symbol}:   💡 To execute highest buy: ask needs to reach ${price_to_hit:.4f} "
                               f"(currently ${self.ask_price:.4f}, need -${price_gap:.4f} or -{price_gap_pct:.2f}%)")
                else:
                    self._info(f"{self.symbol}:   Highest buy order at ${price_to_hit:.4f} - ask (${self.ask_price:.4f}) is below it!")
            
            # List each buy order with details
            total_buy_value = 0.0
            for order in sorted_buy_orders:
                buy_value = order.amount * order.rate
                total_buy_value += buy_value
                
                # Calculate discount from current ask price
                if self.ask_price > 0:
                    discount = ((self.ask_price - order.rate) / self.ask_price * 100) if self.ask_price > 0 else 0
                    discount_str = f"discount: -{discount:.2f}%"
                else:
                    discount_str = ""
                
                self._info(f"{self.symbol}:     {order.amount:.8f} @ ${order.rate:.4f} = ${buy_value:.2f} ({discount_str})")
            
            self._info(f"{self.symbol}:   Total Buy Orders Value: ${total_buy_value:.2f} {self.market}")
        else:
            self._info(f"{self.symbol}:   📉 BUY ORDERS: None placed")
    
    def log_sell_orders_list(self):
        """Log detailed sell orders list (called from sell ladder section)"""
        sell_orders = self.get_sell_orders()
        if sell_orders:
            # Sort by price (lowest first) to show which will execute first
            sorted_sell_orders = sorted(sell_orders, key=lambda o: o.rate)
            lowest_sell_price = sorted_sell_orders[0].rate if sorted_sell_orders else 0
            
            # Count Core vs Working orders for header
            working_order_ids = getattr(self, '_working_order_ids', set())
            core_count = sum(1 for o in sell_orders if o.order_id not in working_order_ids)
            working_count = sum(1 for o in sell_orders if o.order_id in working_order_ids)
            
            if self.mean_reversion_enabled and working_count > 0:
                self._info(f"{self.symbol}:   📈 SELL ORDERS ({len(sell_orders)} orders: {core_count} Core, {working_count} Working):")
            else:
                self._info(f"{self.symbol}:   📈 SELL ORDERS ({len(sell_orders)} orders):")
            
            # Show execution requirement
            if self.bid_price > 0 and lowest_sell_price > 0:
                price_to_hit = lowest_sell_price
                price_gap = price_to_hit - self.bid_price
                price_gap_pct = (price_gap / self.bid_price * 100) if self.bid_price > 0 else 0
                if price_gap > 0:
                    self._info(f"{self.symbol}:   💡 To execute lowest sell: bid needs to reach ${price_to_hit:.4f} "
                               f"(currently ${self.bid_price:.4f}, need +${price_gap:.4f} or +{price_gap_pct:.2f}%)")
                else:
                    self._info(f"{self.symbol}:   Lowest sell order at ${price_to_hit:.4f} - bid (${self.bid_price:.4f}) is above it!")
            
            total_sell_value = 0.0
            profitable_orders = 0
            core_profitable_orders = 0  # Track Core profitable orders separately
            unprofitable_orders = 0
            working_unprofitable = 0  # Track Working orders separately (expected to be "unprofitable" vs avg entry)
            
            # Use average entry price for core sell orders (consistent ladder display)
            # Working orders use LIFO matching for more accurate profit calculation
            # LIFO is used for actual fills (newest buys sold first), but for display purposes, average entry
            # shows more intuitive profit progression across ladder levels for core orders
            avg_cost_per_coin = self.average_entry_price

            working_orders_for_lifo = [
                o for o in sorted_sell_orders if o.order_id in working_order_ids
            ]
            sequential_working_profits = self._sequential_working_lifo_profits(working_orders_for_lifo)
            
            for order in sorted_sell_orders:
                sell_value = order.amount * order.rate
                total_sell_value += sell_value
                
                # Determine if this is a Working order
                is_working = order.order_id in working_order_ids
                order_tag = "[Working]" if is_working else "[Core]" if self.mean_reversion_enabled else ""
                
                # Calculate % above average entry (similar to buy orders showing discount)
                if avg_cost_per_coin > 0:
                    above_entry_pct = ((order.rate - avg_cost_per_coin) / avg_cost_per_coin * 100) if avg_cost_per_coin > 0 else 0
                    above_entry_str = f"+{above_entry_pct:.2f}% above entry"
                else:
                    above_entry_str = ""
                
                # Calculate profit - use LIFO for Working orders, average entry for Core orders
                if is_working:
                    if order.order_id in sequential_working_profits:
                        net_profit, net_profit_pct, matched_cost = sequential_working_profits[order.order_id]
                        cost_basis = order.amount * matched_cost
                        total_investment = cost_basis + cost_basis * self.buy_fee
                    else:
                        net_profit, net_profit_pct, cost_basis, total_investment = self._calculate_lifo_profit_for_pending_sell(
                            order.amount, order.rate
                        )
                    # Calculate % above average entry for reference
                    if avg_cost_per_coin > 0:
                        above_entry_pct = ((order.rate - avg_cost_per_coin) / avg_cost_per_coin * 100) if avg_cost_per_coin > 0 else 0
                        above_entry_str = f"+{above_entry_pct:.2f}% above entry"
                else:
                    # Use average entry price for core orders (consistent ladder display)
                    cost_basis = order.amount * avg_cost_per_coin
                    profit = sell_value - cost_basis
                    profit_pct = (profit / cost_basis * 100) if cost_basis > 0 else 0
                    
                    # Account for fees (buy fee + sell fee)
                    buy_fee = cost_basis * self.buy_fee
                    sell_fee = sell_value * self.sell_fee
                    net_profit = profit - buy_fee - sell_fee
                    # Calculate profit percentage relative to total investment (cost basis + buy fee)
                    total_investment = cost_basis + buy_fee
                    net_profit_pct = (net_profit / total_investment * 100) if total_investment > 0 else 0
                
                if net_profit > 0:
                    profitable_orders += 1
                    if not is_working:
                        core_profitable_orders += 1  # Track core profitable separately
                    status_icon = ""
                else:
                    if is_working:
                        # Working orders below avg entry are expected (mean-reversion), no warning icon
                        working_unprofitable += 1
                        status_icon = "↻ "  # Cycle icon for mean-reversion
                    else:
                        unprofitable_orders += 1
                        status_icon = "⚠️ "
                
                # Show profit for Working orders using LIFO, similar to Core orders
                if is_working and self.current_price > 0:
                    above_current_pct = ((order.rate - self.current_price) / self.current_price * 100)
                    above_current_str = f"+{above_current_pct:.2f}% above current"
                    matched_cost = (cost_basis / order.amount) if order.amount > 0 else 0.0
                    self._info(f"{self.symbol}:     {status_icon}{order_tag} {order.amount:.8f} @ ${order.rate:.4f} = ${sell_value:.2f} "
                               f"({above_current_str}, LIFO cost: ${matched_cost:.4f}, profit: ${net_profit:+.2f}, {net_profit_pct:+.2f}% after fees)")
                else:
                    self._info(f"{self.symbol}:     {status_icon}{order_tag} {order.amount:.8f} @ ${order.rate:.4f} = ${sell_value:.2f} "
                               f"({above_entry_str}, profit: ${net_profit:+.2f}, {net_profit_pct:+.2f}% after fees)")
            
            self._info(f"{self.symbol}:   Total Sell Orders Value: ${total_sell_value:.2f} {self.market}")
            
            # Differentiated summary for Core vs Working
            if self.mean_reversion_enabled and working_count > 0:
                self._info(f"{self.symbol}:   Core Orders: {core_profitable_orders}/{core_count} profitable (recovery strategy)")
                self._info(f"{self.symbol}:   Working Orders: {working_count} active (mean-reversion; profit assumes lowest fill first)")
            else:
                self._info(f"{self.symbol}:   Profitable Orders: {profitable_orders}/{len(sell_orders)}")
            
            # Only warn about unprofitable Core orders (Working below entry is expected)
            if unprofitable_orders > 0:
                logging.warning(f"{self.symbol}:   ⚠️ {unprofitable_orders} Core sell order(s) may be unprofitable after fees")
        else:
            self._info(f"{self.symbol}:   📈 SELL ORDERS: None placed")
    
    def calculate_order_size(self, level_index: int, total_levels: int, total_amount: float, is_sell_order: bool = False, buy_level_pct: float = None) -> float:
        """Calculate order size for a ladder level
        
        Args:
            level_index: Index of the level (0 = first/closest, higher = further away)
            total_levels: Total number of levels
            total_amount: Total amount to distribute
            is_sell_order: If True, use inverse-weighted for weighted distribution (larger at lower profit levels)
                          If False (buy orders), use inverse-weighted (larger at shallower discounts)
            buy_level_pct: Optional buy level percentage (e.g., 0.5, 1.0, 1.5). Used to apply small position multiplier
                          for levels at or below small_position_threshold
        """
        if total_levels <= 0:
            logging.warning(f"{self.symbol}: Invalid total_levels ({total_levels}) for order size calculation")
            return 0.0
        
        # Use appropriate distribution setting based on order type
        distribution = self.sell_order_distribution if is_sell_order else self.buy_order_distribution
        
        if distribution == "equal":
            base_size = total_amount / total_levels
        elif distribution == "weighted":
            if total_levels == 1:
                base_size = total_amount
            else:
                # Both sides taper away from the current price: size concentrates on the
                # near levels that fill frequently, leaving the far levels as small
                # standing insurance against a spike or crash rather than the bulk of
                # the allocation.
                if is_sell_order:
                    # level_index 0 (lowest profit) = 1.5, level_index (total_levels-1) (highest profit) = 0.5
                    weight_range = 1.5 - 0.5  # 1.0
                    weight = 1.5 - (weight_range * level_index / max(1, total_levels - 1))
                else:
                    # level_index 0 (shallowest discount) = 1.3, level_index (total_levels-1) (deepest) = 0.7
                    weight_range = 1.3 - 0.7  # 0.6
                    weight = 1.3 - (weight_range * level_index / max(1, total_levels - 1))
                
                # Calculate total weight for normalization
                total_weight = 0.0
                for i in range(total_levels):
                    if is_sell_order:
                        level_weight = 1.5 - (weight_range * i / max(1, total_levels - 1))
                    else:
                        level_weight = 1.3 - (weight_range * i / max(1, total_levels - 1))
                    total_weight += level_weight
                
                if total_weight <= 0:
                    logging.warning(f"{self.symbol}: Invalid total_weight ({total_weight}) for weighted order size")
                    base_size = total_amount / total_levels
                else:
                    base_size = (total_amount * weight) / total_weight
        else:
            base_size = total_amount / total_levels
        
        # Apply small position multiplier for buy orders at or below threshold
        if not is_sell_order and buy_level_pct is not None:
            if buy_level_pct <= self.small_position_threshold:
                base_size = base_size * self.small_position_multiplier
                logging.debug(f"{self.symbol}: Applying small position multiplier ({self.small_position_multiplier}) "
                            f"to level {buy_level_pct}% (threshold: {self.small_position_threshold}%)")
        
        return base_size
    
    def should_update_orders(self) -> bool:
        """Check if orders should be updated based on price movement
        
        Compares current price to:
        1. Price when orders were last placed (catches slow drops over days/weeks)
        2. Price from quick window ago (10 min) with higher threshold (5%) - catches sudden drops
        3. Price from short window ago (30 min) with standard threshold (3.0%) - catches gradual drops
        4. Price from long window ago (1 hour) with standard threshold (3.0%) - catches very gradual drops
        
        This multi-window approach ensures we:
        - Catch sudden drops (5% over 10 min) without over-trading (higher threshold)
        - Catch gradual drops (10% over 45 min) with standard threshold
        - Catch very gradual drops over hours
        - Avoid excessive rebalancing on small movements
        
        Updates if any comparison exceeds its threshold.
        """
        if self.current_price == 0:
            return False
        
        # Check 1: Compare to price when orders were last placed (catches slow drops over days/weeks)
        if self.price_when_orders_placed > 0:
            change_from_placement = abs((self.current_price - self.price_when_orders_placed) / self.price_when_orders_placed) * 100
            if change_from_placement >= self.price_update_threshold:
                direction = "↑" if self.current_price > self.price_when_orders_placed else "↓"
                self._info(f"{self.symbol}: Price {direction} {change_from_placement:.2f}% from when orders were placed "
                           f"(${self.price_when_orders_placed:.4f} → ${self.current_price:.4f}), threshold exceeded - updating buy ladder")
                return True
        
        # Check 2: Compare to price from quick time window ago (catches sudden drops over 5-15 minutes)
        # This catches sudden drops that happen quickly (e.g., 5% over 10 minutes)
        # Uses a higher threshold to avoid over-trading on small quick moves
        if self.price_history and self.price_update_quick_window > 0:
            current_time = time.time()
            target_time = current_time - self.price_update_quick_window
            
            # Find prices that are at or before the target time (from the quick window ago or earlier)
            prices_before_target = [(t, p) for t, p in self.price_history if t <= target_time]
            
            if prices_before_target:
                # Use the most recent price at or before the target time (closest to quick window ago)
                historical_time, historical_price = max(prices_before_target, key=lambda x: x[0])
                time_diff_minutes = (current_time - historical_time) / 60.0
                
                price_change_pct = abs((self.current_price - historical_price) / historical_price) * 100
                if price_change_pct >= self.price_update_quick_threshold:
                    logging.debug(f"{self.symbol}: Price changed {price_change_pct:.2f}% over {time_diff_minutes:.1f} minutes (quick window) "
                                f"({historical_price:.4f} -> {self.current_price:.4f})")
                    return True
        
        # Check 3: Compare to price from shorter time window ago (catches gradual drops over 30-45 minutes)
        # This catches slow, steady drops that accumulate over shorter periods (e.g., 10% over 45 minutes)
        # This is important because we want to catch significant drops even if they happen relatively quickly
        if self.price_history and self.price_update_short_window > 0:
            current_time = time.time()
            target_time = current_time - self.price_update_short_window
            
            # Find prices that are at or before the target time (from the short window ago or earlier)
            prices_before_target = [(t, p) for t, p in self.price_history if t <= target_time]
            
            if prices_before_target:
                # Use the most recent price at or before the target time (closest to short window ago)
                historical_time, historical_price = max(prices_before_target, key=lambda x: x[0])
                time_diff_minutes = (current_time - historical_time) / 60.0
                
                price_change_pct = abs((self.current_price - historical_price) / historical_price) * 100
                if price_change_pct >= self.price_update_threshold:
                    logging.debug(f"{self.symbol}: Price changed {price_change_pct:.2f}% over {time_diff_minutes:.1f} minutes (short window) "
                                f"({historical_price:.4f} -> {self.current_price:.4f})")
                    return True
        
        # Check 4: Compare to price from longer time window ago (catches very gradual drops over hours)
        # This catches slow, steady drops that accumulate over longer periods
        if self.price_history and self.price_update_time_window > 0:
            current_time = time.time()
            target_time = current_time - self.price_update_time_window
            
            # Find prices that are at or before the target time (from the time window ago or earlier)
            prices_before_target = [(t, p) for t, p in self.price_history if t <= target_time]
            
            if prices_before_target:
                # Use the most recent price at or before the target time (closest to time window ago)
                historical_time, historical_price = max(prices_before_target, key=lambda x: x[0])
                time_diff_minutes = (current_time - historical_time) / 60.0
                
                price_change_pct = abs((self.current_price - historical_price) / historical_price) * 100
                if price_change_pct >= self.price_update_threshold:
                    logging.debug(f"{self.symbol}: Price changed {price_change_pct:.2f}% over {time_diff_minutes:.1f} minutes (long window) "
                                f"({historical_price:.4f} -> {self.current_price:.4f})")
                    return True
        
        # First run - no previous price
        if self.last_price == 0:
            return True
        
        return False
    
    def place_buy_ladder(self):
        """Place buy orders at ladder levels below current price"""
        if self.current_price == 0:
            logging.warning(f"{self.symbol}: Cannot place buy ladder - no current price")
            return
        
        # Ensure open_orders is initialized
        if not hasattr(self, 'open_orders') or self.open_orders is None:
            self.open_orders = []
        
        # --- Price Elevation Protection: Adjust allocation based on distance from recent high ---
        # 
        # ANALYSIS: Is Price Elevation Protection still needed with LIFO?
        # 
        # With LIFO (Last In, First Out), when selling, the newest buy orders are consumed first.
        # This means if you buy at high prices and then buy at lower prices, you'll sell the
        # cheaper coins first when taking profits. This optimizes realized profit.
        #
        # However, Price Elevation Protection still serves important purposes:
        #
        # 1. CAPITAL PRESERVATION: Even with LIFO, buying at high prices ties up capital.
        #    If you deploy 100% of capital at high prices, you have no funds available to
        #    buy at lower prices when the market retraces. LIFO helps when selling, but
        #    doesn't prevent capital from being locked in high-priced positions.
        #
        # 2. RISK MITIGATION: If price stays elevated and never retraces enough to trigger
        #    your buy ladder, you could be fully invested at high prices with no way to
        #    average down. Price Elevation Protection reserves capital for better entries.
        #
        # 3. LIFO LIMITATION: LIFO only helps if you have cheaper buys to sell first. If
        #    all your buys are at high prices (because you deployed all capital at the top),
        #    LIFO can't help - you'll still be selling high-cost-basis coins.
        #
        # 4. SHALLOW LEVEL SKIPPING: Skipping shallow buy levels (0.5%, 1.0%) when near
        #    highs prevents accumulating tokens at prices very close to recent highs,
        #    which is still valuable even with LIFO.
        #
        # RECOMMENDATION:
        # - Price Elevation Protection is STILL VALUABLE with LIFO because it preserves
        #   capital for better entry opportunities and prevents over-committing at highs.
        # - LIFO optimizes profit when selling, but Price Elevation Protection optimizes
        #   capital deployment when buying - they serve complementary purposes.
        # - You may consider being LESS conservative (e.g., 75% allocation instead of 50%
        #   when near highs) since LIFO provides some protection, but keeping some capital
        #   reserved is still prudent.
        # - To disable: Set price_elevation.enabled: false in config.yaml
        #
        elevation_tier = self._get_price_elevation_tier()
        skip_levels = elevation_tier.get('skip_levels', 0)
        elevation_allocation_pct = elevation_tier.get('allocation', 100)
        percentile = elevation_tier.get('percentile', 0)
        position_pct = elevation_tier.get('position_pct', 0)
        tier_name = elevation_tier.get('tier_name', 'disabled')
        allocation_pct = self._get_effective_buy_allocation_pct(
            elevation_allocation_pct if self.price_elevation_enabled else 100.0,
            percentile=percentile,
        )
        portfolio_token_pct = self._get_portfolio_token_pct()
        
        # Log price elevation signal (percentile / rolling window); dollar limits in deployment summary
        if self.price_elevation_enabled and self.rolling_price_high > 0:
            window_days = self.price_elevation_window_hours / 24
            coverage_log = ""
            if self.price_high_history:
                oldest_ts = min(t for t, _ in self.price_high_history)
                span_days = (time.time() - oldest_ts) / 86400.0
                coverage_log = (
                    f"  samples: {len(self.price_high_history)} over {span_days:.0f}d "
                    f"(configured window: {window_days:.0f}d)\n"
                )
                if span_days < window_days * 0.85:
                    coverage_log += (
                        f"  ⚠️ history shorter than window — run "
                        f"spot_ladder/fetch_historical_prices.py --days {int(window_days)} to backfill\n"
                    )
            self._info(f"{self.symbol}: Price elevation (percentile signal):\n"
                        f"{coverage_log}"
                        f"  rolling range: ${self.rolling_price_low:.4f} - ${self.rolling_price_high:.4f} (mean ${self.rolling_price_mean:.4f})\n"
                        f"  current price: ${self.current_price:.4f}\n"
                        f"  percentile: {percentile:.1f}th (0=cheapest, 100=most expensive)\n"
                        f"  {position_pct:.1f}% below rolling high\n"
                        f"  tier: {tier_name} ({elevation_allocation_pct:.0f}% tier alloc) | skip_levels: {skip_levels}")
        elif self.position_aware_enabled and portfolio_token_pct is not None and allocation_pct != elevation_allocation_pct:
            self._info(f"{self.symbol}: Position-aware allocation: {allocation_pct:.0f}% "
                        f"(elevation {elevation_allocation_pct:.0f}%, tokens {portfolio_token_pct:.0f}% of portfolio)")
        
        # CRITICAL: Always fetch fresh orders from API before processing
        # This ensures we're working with actual API state, not stale internal tracking
        try:
            orders_data = self.api.get_orders(self.cointype, self.market)
            if orders_data:
                self.update_open_orders(orders_data)
                logging.debug(f"{self.symbol}: Fetched fresh orders from API before buy ladder check")
        except Exception as e:
            logging.warning(f"{self.symbol}: Could not fetch fresh orders from API: {e}")
            # Continue with existing open_orders if API fetch fails
        
        existing_buy_orders = self.get_buy_orders()
        
        # Check for and cancel dust orders FIRST (before any other validation)
        # These are tiny orders left after partial fills that should be removed
        # This check must run before early returns to ensure dust orders are always caught
        dust_threshold = min(0.10, self.min_order_size * 0.01)  # $0.10 or 1% of min_order_size, whichever is smaller
        dust_orders = []
        for order in existing_buy_orders:
            order_value = order.amount * order.rate
            if order_value < dust_threshold:
                dust_orders.append(order)
                self._info(f"{self.symbol}: Detected dust buy order: {order.amount:.8f} @ ${order.rate:.4f} = ${order_value:.2f} "
                           f"(below ${dust_threshold:.2f} threshold) - will cancel")
        
        # Cancel dust orders
        if dust_orders:
            for order in dust_orders:
                try:
                    self._info(f"{self.symbol}: Cancelling dust buy order {order.order_id} at {order.rate:.4f} "
                               f"(value: ${order.amount * order.rate:.2f})")
                    self.api.cancel_order(order.order_id, order_type="buy")
                    existing_buy_orders.remove(order)  # Remove from list to avoid processing it further
                except Exception as e:
                    logging.warning(f"{self.symbol}: Failed to cancel dust order {order.order_id}: {e}")
            
            self._info(f"{self.symbol}: Cancelled {len(dust_orders)} dust order(s), waiting for cancellations to process...")
            time.sleep(0.5)
            # Re-fetch orders to get accurate state after cancellation
            try:
                orders_data = self.api.get_orders(self.cointype, self.market)
                if orders_data:
                    self.update_open_orders(orders_data)
                    existing_buy_orders = self.get_buy_orders()
                    logging.debug(f"{self.symbol}: Re-fetched orders after dust cancellation, found {len(existing_buy_orders)} remaining buy orders")
            except Exception as e:
                logging.warning(f"{self.symbol}: Could not re-fetch orders after dust cancellation: {e}")
        
        # Validate: Check if any tracked orders don't exist in API
        # This catches phantom orders that might exist in tracking but not in API
        if existing_buy_orders:
            try:
                api_orders_data = self.api.get_orders(self.cointype, self.market)
                if api_orders_data and api_orders_data.get('status') == 'ok':
                    api_buy_orders = api_orders_data.get('buyorders', [])
                    api_order_ids = {str(o.get('id', '')) for o in api_buy_orders}
                    
                    phantom_orders = [o for o in existing_buy_orders if o.order_id not in api_order_ids]
                    if phantom_orders:
                        logging.warning(f"{self.symbol}: Found {len(phantom_orders)} phantom buy order(s) in tracking that don't exist in API:")
                        for order in phantom_orders:
                            logging.warning(f"{self.symbol}:   - Order {order.order_id} at ${order.rate:.4f} (amount: {order.amount:.8f})")
                        self._info(f"{self.symbol}: Removing phantom orders from tracking - they will be excluded from validation")
                        # Remove phantom orders from existing_buy_orders
                        existing_buy_orders = [o for o in existing_buy_orders if o.order_id in api_order_ids]
            except Exception as e:
                logging.debug(f"{self.symbol}: Could not validate orders against API: {e}")
        
        # Adjust needed_orders based on price elevation tier
        # If we're near a high, skip shallow levels to avoid buying high
        full_needed_orders = min(self.max_buy_orders, len(self.buy_levels))
        if skip_levels > 0 and self.price_elevation_enabled:
            # Only use levels from skip_levels onwards
            effective_levels = self.buy_levels[skip_levels:]
            needed_orders = min(self.max_buy_orders - skip_levels, len(effective_levels))
            self._info(f"{self.symbol}: Skipping {skip_levels} shallow buy levels (using levels {skip_levels+1}-{skip_levels+needed_orders} of {full_needed_orders})")
        else:
            needed_orders = full_needed_orders
            effective_levels = self.buy_levels[:needed_orders]  # Always define effective_levels for consistency
        
        # Calculate committed quote in buy orders
        existing_buy_committed = sum(order.amount * order.rate for order in existing_buy_orders)
        intended_buy_committed = self._intended_buy_committed(allocation_pct)
        
        # Calculate uncommitted balance for logging
        uncommitted_balance = self.available_balance - existing_buy_committed
        
        # Calculate price change from when orders were placed
        # If we have existing orders but no tracking price, estimate from highest buy order
        # The highest buy order is typically at the shallowest level (e.g., 1% below placement price)
        if self.price_when_orders_placed == 0 and len(existing_buy_orders) > 0 and self.current_price > 0:
            # Find the highest priced buy order
            highest_order_price = max(order.rate for order in existing_buy_orders)
            # Estimate the placement price (highest order is at shallowest level, typically 1% below)
            shallowest_level = effective_levels[0] if effective_levels else (self.buy_levels[0] if self.buy_levels else 1.0)
            estimated_placement_price = highest_order_price / (1 - shallowest_level / 100)
            self.price_when_orders_placed = estimated_placement_price
            self._info(f"{self.symbol}: Estimated placement price at ${estimated_placement_price:.4f} "
                        f"(from highest order ${highest_order_price:.4f} at {shallowest_level}% level)")
        
        if self.price_when_orders_placed > 0 and self.current_price > 0:
            change_pct = abs((self.current_price - self.price_when_orders_placed) / self.price_when_orders_placed) * 100
            remaining_pct = max(0, self.price_update_threshold - change_pct)
            direction = "↑" if self.current_price > self.price_when_orders_placed else "↓"
            price_status = (
                f"  price: {direction}{change_pct:.2f}% from placement "
                f"(${self.price_when_orders_placed:.4f} → ${self.current_price:.4f}), "
                f"need {remaining_pct:.2f}% more to recalc (threshold: {self.price_update_threshold}%)"
            )
        else:
            price_status = f"  price: tracking from ${self.current_price:.4f}"
        
        self._log_buy_deployment_summary(
            allocation_pct, elevation_allocation_pct, existing_buy_committed,
            len(existing_buy_orders), needed_orders, skip_levels,
        )
        self._info(f"{self.symbol}: Buy ladder timing\n{price_status}")
        
        # Check for over-commitment: buy orders using more quote than current allocation allows
        # This happens when buys fill (reducing quote balance) but remaining orders keep their old sizes,
        # or when balance_percentage_per_symbol is reduced in config
        # If uncommitted_balance is negative, orders exceed the available allocation
        if uncommitted_balance < 0 and len(existing_buy_orders) > 0 and self.available_balance >= self.min_order_size:
            over_commit_pct = ((existing_buy_committed - self.available_balance) / self.available_balance) * 100
            self._info(f"{self.symbol}: Buy orders OVER-COMMITTED - "
                       f"committed ${existing_buy_committed:.2f} exceeds available ${self.available_balance:.2f} "
                       f"by {over_commit_pct:.1f}%. Cancelling all {len(existing_buy_orders)} orders to resize.")
            for order in existing_buy_orders:
                try:
                    self.api.cancel_order(order.order_id, order_type="buy")
                    logging.debug(f"{self.symbol}: Cancelled over-committed buy order {order.order_id}")
                except Exception as e:
                    logging.warning(f"{self.symbol}: Failed to cancel buy order {order.order_id}: {e}")
            if existing_buy_orders:
                time.sleep(0.5)
            existing_buy_orders = []
            existing_buy_committed = 0
            uncommitted_balance = self.available_balance
            # Reset placement price so new orders use current price
            self.price_when_orders_placed = self.current_price
        
        # Shrink ladder when committed exceeds intended (allocation % or max_buy_ladder_usdc cap)
        if (len(existing_buy_orders) > 0
                and existing_buy_committed > intended_buy_committed + self.increased_capital_threshold):
            cap_note = (
                f", cap ${self.max_buy_ladder_usdc:.0f}" if self.max_buy_ladder_usdc > 0 else ""
            )
            self._info(
                f"{self.symbol}: Buy ladder OVER-SIZED — committed ${existing_buy_committed:.2f} exceeds "
                f"intended ${intended_buy_committed:.2f}{cap_note}. Cancelling all orders to resize."
            )
            for order in existing_buy_orders:
                try:
                    self.api.cancel_order(order.order_id, order_type="buy")
                    logging.debug(f"{self.symbol}: Cancelled oversized buy order {order.order_id}")
                except Exception as e:
                    logging.warning(f"{self.symbol}: Failed to cancel order {order.order_id}: {e}")
            if existing_buy_orders:
                time.sleep(0.5)
            existing_buy_orders = []
            existing_buy_committed = 0
            uncommitted_balance = self.available_balance
            self.price_when_orders_placed = self.current_price
        
        if self.available_balance < self.min_order_size:
            if len(existing_buy_orders) == 0:
                self._info(f"{self.symbol}: Insufficient balance for buy orders "
                           f"(available: {self.available_balance:.2f} {self.market}, "
                           f"minimum: {self.min_order_size:.2f} {self.market})")
            else:
                logging.debug(f"{self.symbol}: Insufficient balance for new buy orders "
                            f"(available: {self.available_balance:.2f} {self.market}, "
                            f"but {len(existing_buy_orders)} existing orders)")
            # Always log buy orders list before returning
            self.log_buy_orders_list()
            return
        
        # If we have enough orders, check if we need to update them first
        # This allows us to EDIT orders when price moves, rather than cancelling/recreating
        if len(existing_buy_orders) >= needed_orders:
            # Check if we need to resize orders due to increased capital (similar to sell ladder)
            old_buy_committed = getattr(self, '_last_buy_committed', 0)
            # uncommitted_balance already calculated above
            needs_resize = False
            
            # Only treat uncommitted quote as "new capital" if it exceeds the elevation
            # reserve. available_balance is the 90% symbol slice — it is NOT reduced by
            # allocation_pct. At 20% deploy, ~80% idle is intentional dry powder.
            intended_committed = self._intended_buy_committed(allocation_pct)
            deployable_gap = intended_committed - existing_buy_committed
            has_excess_capital = deployable_gap > self.increased_capital_threshold
            
            # Check if this is the first run (startup) - if so, bypass observation period
            # Only treat as first run if we have no tracking AND no existing orders
            is_first_run = old_buy_committed == 0 and len(existing_buy_orders) == 0
            
            if has_excess_capital:
                unused_pct = (uncommitted_balance / self.available_balance) * 100 if self.available_balance > 0 else 0
                elevation_reserve = max(0.0, self.available_balance - intended_committed)
                
                # On first run (startup), resize immediately if the deployable gap is large
                if is_first_run:
                    if unused_pct > 10.0 and deployable_gap > self.increased_capital_threshold:
                        needs_resize = True
                        self._info(f"{self.symbol}: First run detected - ${deployable_gap:.2f} below intended "
                                   f"ladder size ${intended_committed:.2f}. Resizing buy orders.")
                    else:
                        self._info(f"{self.symbol}: First run detected - uncommitted {uncommitted_balance:.2f} {self.market} "
                                   f"(elevation reserve ${elevation_reserve:.2f}, deployable gap ${deployable_gap:.2f}). "
                                   f"Keeping existing {len(existing_buy_orders)} orders.")
                elif self._last_buy_committed > 0:
                    if self._increased_capital_detected_at is None:
                        self._increased_capital_detected_at = datetime.now()
                        observation_period_minutes = self.increased_capital_observation_period / 60
                        self._info(f"{self.symbol}: Increased deployable capital - gap ${deployable_gap:.2f} above "
                                   f"threshold (${self.increased_capital_threshold:.2f}) vs intended ${intended_committed:.2f} "
                                   f"(committed ${existing_buy_committed:.2f}, reserve ${elevation_reserve:.2f}). "
                                   f"Starting {observation_period_minutes:.0f}-minute observation period before resizing.")
                    else:
                        elapsed_seconds = (datetime.now() - self._increased_capital_detected_at).total_seconds()
                        if elapsed_seconds >= self.increased_capital_observation_period:
                            needs_resize = True
                            observation_period_minutes = self.increased_capital_observation_period / 60
                            self._info(f"{self.symbol}: Observation period ({observation_period_minutes:.0f} minutes) complete - "
                                       f"resizing buy orders to close ${deployable_gap:.2f} gap vs intended ${intended_committed:.2f}.")
                        else:
                            remaining_seconds = self.increased_capital_observation_period - elapsed_seconds
                            remaining_minutes = int(remaining_seconds / 60)
                            remaining_secs = int(remaining_seconds % 60)
                            elapsed_minutes = int(elapsed_seconds / 60)
                            self._info(f"{self.symbol}: ⏳ Capital observation period: {elapsed_minutes}m elapsed, "
                                       f"{remaining_minutes}m {remaining_secs}s remaining before resize "
                                       f"(deployable gap ${deployable_gap:.2f})")
                elif self._increased_capital_detected_at is None:
                    self._increased_capital_detected_at = datetime.now()
                    observation_period_minutes = self.increased_capital_observation_period / 60
                    self._info(f"{self.symbol}: Increased deployable capital - gap ${deployable_gap:.2f}. "
                               f"Starting {observation_period_minutes:.0f}-minute observation period before resizing.")
                else:
                    elapsed_seconds = (datetime.now() - self._increased_capital_detected_at).total_seconds()
                    if elapsed_seconds >= self.increased_capital_observation_period:
                        needs_resize = True
                        observation_period_minutes = self.increased_capital_observation_period / 60
                        self._info(f"{self.symbol}: Observation period ({observation_period_minutes:.0f} minutes) complete - "
                                   f"resizing buy orders to close ${deployable_gap:.2f} gap vs intended ${intended_committed:.2f}.")
                    else:
                        remaining_seconds = self.increased_capital_observation_period - elapsed_seconds
                        remaining_minutes = int(remaining_seconds / 60)
                        remaining_secs = int(remaining_seconds % 60)
                        elapsed_minutes = int(elapsed_seconds / 60)
                        self._info(f"{self.symbol}: ⏳ Capital observation period: {elapsed_minutes}m elapsed, "
                                   f"{remaining_minutes}m {remaining_secs}s remaining before resize "
                                   f"(deployable gap ${deployable_gap:.2f})")
            else:
                # Committed amount is at (or above) the allocated ladder size — reserve is intentional
                if self._increased_capital_detected_at is not None:
                    self._info(f"{self.symbol}: Buy ladder at allocated size "
                               f"(committed ${existing_buy_committed:.2f} vs intended ${intended_committed:.2f}) — "
                               f"resetting observation period (idle {self.market} is elevation reserve, not new capital).")
                    self._increased_capital_detected_at = None
            
            # If buys filled, committed can fall below intended — same deployable-gap check above handles it.
            if old_buy_committed > 0 and existing_buy_committed < old_buy_committed * 0.7:
                if has_excess_capital:
                    logging.debug(f"{self.symbol}: Buy committed dropped after fills "
                                f"(${old_buy_committed:.2f} → ${existing_buy_committed:.2f}); "
                                f"deployable gap ${deployable_gap:.2f} will resize after observation.")
            
            # Check if price moved significantly - if so, try to edit orders to new levels
            price_moved = self.should_update_orders()
            
            if needs_resize or price_moved:
                if needs_resize and not price_moved:
                    # Check if this is a first-run resize (old_buy_committed == 0 means first run)
                    is_first_run_resize = old_buy_committed == 0
                    if is_first_run_resize:
                        self._info(f"{self.symbol}: Buy ladder complete ({len(existing_buy_orders)} orders), resizing to use all available capital (first run at startup)")
                    else:
                        observation_period_minutes = self.increased_capital_observation_period / 60
                        self._info(f"{self.symbol}: Buy ladder complete ({len(existing_buy_orders)} orders), resizing to use more capital (after {observation_period_minutes:.0f}-minute observation period)")
                    # Reset the detection timestamp since we're now resizing
                    self._increased_capital_detected_at = None
                    # After first-run resize, temporarily disable excess capital detection for one cycle
                    # to avoid immediately starting observation period if small buffer remains
                    if is_first_run_resize:
                        # Set a flag or timestamp to skip excess capital detection on next cycle
                        # We'll use a special value: set detection timestamp far in the past
                        # so it won't trigger immediately, but will reset on next check if capital increases
                        pass  # The None reset above is sufficient - it won't detect again until capital actually increases
                    # Cancel all existing orders to resize them
                    for order in existing_buy_orders:
                        try:
                            self.api.cancel_order(order.order_id, order_type="buy")
                            logging.debug(f"{self.symbol}: Cancelled buy order {order.order_id} for resize")
                        except Exception as e:
                            logging.warning(f"{self.symbol}: Failed to cancel order {order.order_id}: {e}")
                    # Wait for cancellations, then fall through to place new orders
                    if existing_buy_orders:
                        time.sleep(0.5)
                        # Recalculate existing_buy_orders to empty list since we cancelled them
                        existing_buy_orders = []
                    # Skip validation logic and fall through to place new orders
                elif price_moved:
                    self._info(f"{self.symbol}: Buy ladder complete ({len(existing_buy_orders)} orders), price moved significantly - updating to maintain structure")
                    self._update_buy_orders(existing_buy_orders)
                    # After updating, re-fetch orders to get accurate state
                    try:
                        orders_data = self.api.get_orders(self.cointype, self.market)
                        if orders_data:
                            self.update_open_orders(orders_data)
                            existing_buy_orders = self.get_buy_orders()
                    except Exception as e:
                        logging.warning(f"{self.symbol}: Could not re-fetch orders after update: {e}")
                    
                    # After potential update, validate orders match ladder structure
                    # Cancel any that still don't match (e.g., if editing failed)
                    # Also handle duplicates - if we have more orders than needed, keep only one per level
                    orders_to_cancel = []
                    valid_orders = []
                    level_used = {}  # Track which levels already have an order
                    
                    for order in existing_buy_orders:
                        # Calculate expected price for each ladder level (use effective_levels)
                        matches_ladder = False
                        matched_level = None
                        
                        for level_idx, level_pct in enumerate(effective_levels):
                            expected_price = self.current_price * (1 - level_pct / 100)
                            # Allow 0.5% tolerance for price matching
                            price_diff_pct = abs((order.rate - expected_price) / expected_price) * 100
                            if price_diff_pct < 0.5:
                                matches_ladder = True
                                matched_level = level_idx
                                break
                        
                        if matches_ladder:
                            # Check if we already have an order at this level
                            if matched_level in level_used:
                                # Duplicate at same level - cancel this one (keep the first one we found)
                                orders_to_cancel.append(order)
                                logging.debug(f"{self.symbol}: Cancelling duplicate buy order {order.order_id} at {order.rate:.4f} "
                                            f"(level {matched_level} already has an order)")
                            else:
                                # First order at this level - keep it
                                valid_orders.append(order)
                                level_used[matched_level] = order
                        else:
                            # Doesn't match any ladder level - cancel it
                            orders_to_cancel.append(order)
                    
                    # --- Amount validation: check if valid orders have proper weighted amounts ---
                    # SKIP undersized check immediately after placing orders - this prevents false positives
                    # when orders haven't fully propagated through the API yet, or when checking against
                    # a total calculated from orders that may not yet reflect the true committed amount.
                    # The undersized check at the end of the function (after price movement check) is sufficient.
                    # Only check for undersized orders if price has moved significantly (not immediate check after placement)
                    check_undersized_here = False
                    if self.price_when_orders_placed > 0:
                        price_change_pct = abs((self.current_price - self.price_when_orders_placed) / self.price_when_orders_placed) * 100
                        if price_change_pct >= self.price_update_threshold:
                            check_undersized_here = True
                    
                    if check_undersized_here and len(valid_orders) > 0:
                        # Calculate total committed from valid orders to determine expected distribution
                        total_valid_committed = sum(o.amount * o.rate for o in valid_orders)
                        if total_valid_committed > 0:
                            undersized_orders = []
                            for order in valid_orders:
                                # Find which level this order is at (use effective_levels)
                                for level_idx, level_pct in enumerate(effective_levels):
                                    expected_price = self.current_price * (1 - level_pct / 100)
                                    price_diff_pct = abs((order.rate - expected_price) / expected_price) * 100
                                    if price_diff_pct < 0.5:
                                        # Calculate expected amount for this level (include buy_level_pct for small position multiplier)
                                        # Use level_idx relative to effective_levels for proper weighted distribution
                                        expected_amount = self.calculate_order_size(level_idx, len(effective_levels), total_valid_committed, is_sell_order=False, buy_level_pct=level_pct)
                                        actual_amount = order.amount * order.rate
                                        # Check if order is undersized: must be below 50% of expected AND dollar difference must be significant
                                        # This prevents small dollar differences from triggering unnecessary rebalances
                                        dollar_diff = expected_amount - actual_amount
                                        min_dollar_threshold = max(self.min_order_size * 20, 100.0)  # At least $100 or 20x min_order_size
                                        is_percentage_undersized = actual_amount < expected_amount * 0.5
                                        is_dollar_significant = dollar_diff >= min_dollar_threshold
                                        
                                        if is_percentage_undersized and is_dollar_significant:
                                            undersized_orders.append((order, level_idx, actual_amount, expected_amount))
                                            self._info(f"{self.symbol}: Buy order at level {level_idx} ({level_pct}%) is undersized: "
                                                       f"${actual_amount:.2f} vs expected ${expected_amount:.2f} ({actual_amount/expected_amount*100:.0f}%, diff: ${dollar_diff:.2f})")
                                        elif is_percentage_undersized and not is_dollar_significant:
                                            # Order is percentage-wise undersized but dollar difference is too small to matter
                                            logging.debug(f"{self.symbol}: Buy order at level {level_idx} ({level_pct}%) is slightly undersized but dollar diff (${dollar_diff:.2f}) below threshold (${min_dollar_threshold:.2f}) - ignoring")
                                        break
                            
                            # Only cancel ALL orders if there are undersized orders that need rebalancing
                            if undersized_orders:
                                # Cancel ALL orders - need full balance to redistribute properly
                                self._info(f"{self.symbol}: Cancelling ALL {len(valid_orders)} orders for full rebalance due to {len(undersized_orders)} undersized")
                                for order in valid_orders:
                                    orders_to_cancel.append(order)
                                valid_orders = []
                                level_used = {}
                    
                    # Cancel orders that don't match the ladder or are undersized
                    for order in orders_to_cancel:
                        try:
                            self._info(f"{self.symbol}: Cancelling buy order {order.order_id} at {order.rate:.4f} "
                                       f"(doesn't match ladder structure or is undersized)")
                            self.api.cancel_order(order.order_id, order_type="buy")
                        except Exception as e:
                            logging.warning(f"{self.symbol}: Failed to cancel order {order.order_id}: {e}")
                    
                    # If we cancelled some, we'll need to place new ones
                    if orders_to_cancel:
                        existing_buy_orders = valid_orders
                        # Fall through to place missing orders
                    else:
                        # All orders are valid and up to date
                        self.log_buy_orders_list()
                        return
            else:
                # --- Subsection: Buy Ladder Stability Check ---
                # Check if we have too many orders (shouldn't happen, but handle it)
                if len(existing_buy_orders) > needed_orders:
                    # Have more orders than needed - cancel extras
                    # Validate orders match ladder structure and cancel duplicates/extras
                    orders_to_cancel = []
                    valid_orders = []
                    level_used = {}  # Track which levels already have an order
                    
                    # Use placement price for validation unless price has moved significantly
                    price_moved_significantly = False
                    validation_price = self.current_price
                    if self.price_when_orders_placed > 0:
                        price_change_pct = abs((self.current_price - self.price_when_orders_placed) / self.price_when_orders_placed) * 100
                        if price_change_pct >= self.price_update_threshold:
                            price_moved_significantly = True
                            validation_price = self.current_price
                        else:
                            # Price hasn't moved significantly - validate against placement price
                            validation_price = self.price_when_orders_placed
                    
                    for order in existing_buy_orders:
                        # Calculate expected price for each ladder level (use effective_levels)
                        matches_ladder = False
                        matched_level = None
                        
                        for level_idx, level_pct in enumerate(effective_levels):
                            expected_price = validation_price * (1 - level_pct / 100)
                            # Allow 0.5% tolerance for price matching
                            price_diff_pct = abs((order.rate - expected_price) / expected_price) * 100
                            if price_diff_pct < 0.5:
                                matches_ladder = True
                                matched_level = level_idx
                                break
                        
                        if matches_ladder:
                            # Check if we already have an order at this level
                            if matched_level in level_used:
                                # Duplicate at same level - cancel this one (keep the first one we found)
                                orders_to_cancel.append(order)
                                logging.debug(f"{self.symbol}: Cancelling duplicate buy order {order.order_id} at {order.rate:.4f} "
                                            f"(level {matched_level} already has an order)")
                            else:
                                # First order at this level - keep it
                                valid_orders.append(order)
                                level_used[matched_level] = order
                        else:
                            # Doesn't match any ladder level - cancel it
                            orders_to_cancel.append(order)
                    
                    # If we have more valid orders than needed, cancel the extras
                    if len(valid_orders) > needed_orders:
                        # Sort by price (closest to current price first) and keep only needed_orders
                        valid_orders.sort(key=lambda o: abs(o.rate - self.current_price))
                        extra_orders = valid_orders[needed_orders:]
                        valid_orders = valid_orders[:needed_orders]
                        orders_to_cancel.extend(extra_orders)
                        self._info(f"{self.symbol}: Found {len(extra_orders)} extra buy orders (have {len(valid_orders) + len(extra_orders)}, need {needed_orders}) - cancelling extras")
                    
                    # Cancel extra orders
                    if orders_to_cancel:
                        for order in orders_to_cancel:
                            try:
                                self._info(f"{self.symbol}: Cancelling extra buy order {order.order_id} at {order.rate:.4f} "
                                           f"(have {len(existing_buy_orders)} orders, need {needed_orders})")
                                self.api.cancel_order(order.order_id, order_type="buy")
                            except Exception as e:
                                logging.warning(f"{self.symbol}: Failed to cancel order {order.order_id}: {e}")
                        # Wait a moment for cancellations to process
                        if orders_to_cancel:
                            time.sleep(0.5)
                    
                    # Use valid orders (should be <= needed_orders now)
                    existing_buy_orders = valid_orders
                    existing_buy_committed = sum(order.amount * order.rate for order in existing_buy_orders)
                    
                    # After cancelling extras, check if we still have enough orders
                    # If we have fewer than needed, fall through to place new orders
                    if len(existing_buy_orders) < needed_orders:
                        self._info(f"{self.symbol}: After cancelling extras, have {len(existing_buy_orders)} orders, need {needed_orders} - will place missing orders")
                        # Fall through to order placement logic below (don't return)
                    else:
                        # We have enough orders (exactly needed_orders) - ladder is complete
                        # Note: Recalc info already shown in Buy Ladder Check above, no need to duplicate
                        self._info(f"{self.symbol}: Buy ladder stable ({len(existing_buy_orders)} orders)")
                        # Update tracking variable
                        self._last_buy_committed = existing_buy_committed
                        # Log buy orders list before returning
                        self.log_buy_orders_list()
                        return
                else:
                    # We have exactly needed_orders (not more, not fewer), no resize/price movement needed
                    # But check if any orders are undersized before declaring stable
                    # Only check for undersized orders if we actually have the full count
                    # (if we're missing orders, we'll place them below without checking sizes)
                    # IMPORTANT: Skip undersized check if price hasn't moved significantly - this allows
                    # multiple orders to fill before rebalancing, preventing unnecessary rebalances when
                    # gap-filling orders are placed after a single fill
                    undersized_orders = []
                    if len(existing_buy_orders) == needed_orders:
                        # Only check for undersized orders if price has moved significantly
                        # This allows multiple orders to fill before rebalancing (as intended)
                        price_moved_significantly = False
                        if self.price_when_orders_placed > 0:
                            price_change_pct = abs((self.current_price - self.price_when_orders_placed) / self.price_when_orders_placed) * 100
                            if price_change_pct >= self.price_update_threshold:
                                price_moved_significantly = True
                        
                        # Only check for undersized orders if price has moved significantly
                        # This prevents immediate rebalancing after placing gap-filling orders
                        if price_moved_significantly:
                            # Use same validation_price logic as order matching for consistency
                            validation_price_for_size_check = self.current_price
                            
                            total_committed = sum(o.amount * o.rate for o in existing_buy_orders)
                            if total_committed > 0:
                                for order in existing_buy_orders:
                                    for level_idx, level_pct in enumerate(effective_levels):
                                        expected_price = validation_price_for_size_check * (1 - level_pct / 100)
                                        price_diff_pct = abs((order.rate - expected_price) / expected_price) * 100
                                        if price_diff_pct < 0.5:
                                            # Use level_idx relative to effective_levels for proper weighted distribution
                                            expected_amount = self.calculate_order_size(level_idx, len(effective_levels), total_committed, is_sell_order=False, buy_level_pct=level_pct)
                                            actual_amount = order.amount * order.rate
                                            if actual_amount < expected_amount * 0.5:
                                                undersized_orders.append((order, level_idx, level_pct, actual_amount, expected_amount))
                                            break
                        else:
                            # Price hasn't moved significantly - skip undersized check to allow multiple fills
                            logging.debug(f"{self.symbol}: Skipping undersized check - price hasn't moved significantly "
                                        f"(allows multiple orders to fill before rebalancing)")
                    
                    if undersized_orders:
                        # Cancel ALL orders and rebuild - need full balance to redistribute properly
                        self._info(f"{self.symbol}: Found {len(undersized_orders)} undersized buy orders - cancelling ALL {len(existing_buy_orders)} orders for full rebalance")
                        for order, level_idx, level_pct, actual, expected in undersized_orders:
                            self._info(f"{self.symbol}: Undersized: level {level_idx} ({level_pct}%) has ${actual:.2f} vs expected ${expected:.2f} ({actual/expected*100:.0f}%)")
                        # Cancel ALL orders to free up full balance for proper redistribution
                        for order in existing_buy_orders:
                            try:
                                self.api.cancel_order(order.order_id, order_type="buy")
                                logging.debug(f"{self.symbol}: Cancelled buy order {order.order_id} for rebalance")
                            except Exception as e:
                                logging.warning(f"{self.symbol}: Failed to cancel order {order.order_id}: {e}")
                        if existing_buy_orders:
                            time.sleep(0.5)
                        existing_buy_orders = []
                        # Fall through to place all orders with proper distribution
                    elif len(existing_buy_orders) == needed_orders:
                        # Ladder is complete and stable - log status and return
                        # Note: Recalc info already shown in Buy Ladder Check above, no need to duplicate
                        self._info(f"{self.symbol}: Buy ladder stable ({len(existing_buy_orders)} orders)")
                        # Update tracking variable
                        self._last_buy_committed = existing_buy_committed
                        # Log buy orders list before returning
                        self.log_buy_orders_list()
                        return
                    # If we have fewer than needed_orders, fall through to place missing orders
        
        # Check if existing orders match the ladder structure (for orders we haven't updated yet)
        # Cancel orders that don't match expected price levels
        # Also cancel orders at shallow levels when price elevation is active
        # Also handle duplicates - if we have more orders than needed, keep only one per level
        orders_to_cancel = []
        valid_orders = []
        level_used = {}  # Track which levels already have an order
        
        # Use placement price for validation unless price has moved significantly
        # This allows multiple orders to execute before recalculation
        price_moved_significantly = False
        validation_price = self.current_price
        if self.price_when_orders_placed > 0:
            price_change_pct = abs((self.current_price - self.price_when_orders_placed) / self.price_when_orders_placed) * 100
            if price_change_pct >= self.price_update_threshold:
                price_moved_significantly = True
                validation_price = self.current_price
            else:
                # Price hasn't moved significantly - validate against placement price
                validation_price = self.price_when_orders_placed
        
        for order in existing_buy_orders:
            # Calculate expected price for each ladder level
            matches_ladder = False
            matched_level = None
            is_at_skipped_level = False
            
            # First check if order is at a skipped level (shallow level being avoided)
            # Always check against current price for skipped levels (price elevation is current-state logic)
            if self.price_elevation_enabled and skip_levels > 0:
                for level_pct in self.buy_levels[:skip_levels]:
                    expected_price = self.current_price * (1 - level_pct / 100)
                    price_diff_pct = abs((order.rate - expected_price) / expected_price) * 100
                    if price_diff_pct < 0.5:
                        is_at_skipped_level = True
                        self._info(f"{self.symbol}: Order at {order.rate:.4f} is at skipped level ({level_pct}%) - will cancel due to price elevation")
                        break
            
            if is_at_skipped_level:
                orders_to_cancel.append(order)
                continue
            
            # Check if order matches an effective ladder level (using validation_price and effective_levels)
            for level_idx, level_pct in enumerate(effective_levels):
                expected_price = validation_price * (1 - level_pct / 100)
                # Allow 0.5% tolerance for price matching
                price_diff_pct = abs((order.rate - expected_price) / expected_price) * 100
                if price_diff_pct < 0.5:
                    matches_ladder = True
                    matched_level = level_idx
                    break
            
            if matches_ladder:
                # Check if we already have an order at this level
                if matched_level in level_used:
                    # Duplicate at same level - cancel this one (keep the first one we found)
                    orders_to_cancel.append(order)
                    logging.debug(f"{self.symbol}: Cancelling duplicate buy order {order.order_id} at {order.rate:.4f} "
                                f"(level {matched_level} already has an order)")
                else:
                    # First order at this level - keep it
                    valid_orders.append(order)
                    level_used[matched_level] = order
            else:
                # Doesn't match any ladder level - cancel it
                orders_to_cancel.append(order)
        
        # If we have more valid orders than needed, cancel the extras
        # This can happen if bot was restarted after adding funds, creating duplicate orders
        if len(valid_orders) > needed_orders:
            # Sort by price (closest to current price first) and keep only needed_orders
            valid_orders.sort(key=lambda o: abs(o.rate - self.current_price))
            extra_orders = valid_orders[needed_orders:]
            valid_orders = valid_orders[:needed_orders]
            orders_to_cancel.extend(extra_orders)
            self._info(f"{self.symbol}: Found {len(extra_orders)} extra buy orders (have {len(valid_orders) + len(extra_orders)}, need {needed_orders}) - cancelling extras")
        
        # Cancel orders that don't match the ladder or are duplicates/extras
        for order in orders_to_cancel:
            try:
                self._info(f"{self.symbol}: Cancelling buy order {order.order_id} at {order.rate:.4f} "
                           f"(doesn't match ladder structure or is duplicate/extra)")
                self.api.cancel_order(order.order_id, order_type="buy")
            except Exception as e:
                logging.warning(f"{self.symbol}: Failed to cancel order {order.order_id}: {e}")
        
        # Update existing_buy_orders to only include valid ones
        existing_buy_orders = valid_orders
        
        # Need to place new orders
        # Calculate committed funds in existing buy orders
        existing_buy_committed = sum(order.amount * order.rate for order in existing_buy_orders)
        
        # Calculate available funds AFTER accounting for committed orders
        uncommitted_balance = max(0, self.available_balance - existing_buy_committed)
        
        # Place missing buy orders
        orders_to_place = needed_orders - len(existing_buy_orders)
        
        # EFFICIENCY: Only place gap-filling orders if:
        # 1. Price has moved significantly (>= threshold) - ladder needs repositioning anyway
        # 2. Multiple orders are missing (>1) - allows natural accumulation before rebalancing
        # This prevents unnecessary order placement after single fills and reduces API calls
        price_moved_significantly = False
        should_skip_single_gap = False
        if orders_to_place > 0:
            if self.price_when_orders_placed > 0:
                price_change_pct = abs((self.current_price - self.price_when_orders_placed) / self.price_when_orders_placed) * 100
                if price_change_pct >= self.price_update_threshold:
                    price_moved_significantly = True
            
            # Skip placing single gap-filling orders unless price moved or multiple orders missing
            # We'll check if it's a shallow level later (after missing_level_indices is calculated)
            should_skip_single_gap = orders_to_place == 1 and not price_moved_significantly
        
        # Full rebuild only on a 4% price move or a 3%+ rung fill. Two shallow fills
        # (22% missing on a 9-rung ladder) used to cancel -7/-10/-15% insurance and
        # re-arm the 0.5% bid — that is the grind-down vacuum.
        if orders_to_place > 0 and needed_orders > 0 and existing_buy_orders:
            should_rebuild, rebuild_reason = self._should_full_rebuild_buy_ladder(
                existing_buy_orders, effective_levels, price_moved_significantly
            )
            if should_rebuild:
                self._info(
                    f"{self.symbol}: Missing {orders_to_place}/{needed_orders} buy orders — "
                    f"full rebuild ({rebuild_reason})"
                )
                for order in existing_buy_orders:
                    try:
                        self.api.cancel_order(order.order_id, order_type="buy")
                        logging.debug(f"{self.symbol}: Cancelled buy order {order.order_id} for full rebuild")
                    except Exception as e:
                        logging.warning(f"{self.symbol}: Failed to cancel order {order.order_id}: {e}")
                time.sleep(0.5)
                existing_buy_orders = []
                existing_buy_committed = 0
                uncommitted_balance = self.available_balance
                orders_to_place = needed_orders
                should_skip_single_gap = False
            else:
                self._info(
                    f"{self.symbol}: Missing {orders_to_place}/{needed_orders} buy orders — "
                    f"gap-filling only (keeping {len(existing_buy_orders)} deeper rungs parked; "
                    f"rebuild needs {self.price_update_threshold:.1f}% price move or "
                    f"{self.buy_ladder_rebuild_on_fill_pct:.1f}%+ fill)"
                )
        
        self._info(f"{self.symbol}: Placing buy ladder - available: {self.available_balance:.2f} {self.market}, "
                    f"committed: {existing_buy_committed:.2f} {self.market}, uncommitted: {uncommitted_balance:.2f} {self.market}, "
                    f"existing: {len(existing_buy_orders)}, needed: {needed_orders}")
        
        # Check if we have enough uncommitted balance
        if uncommitted_balance < self.min_order_size:
            self._info(f"{self.symbol}: Insufficient uncommitted balance for new buy orders "
                        f"(uncommitted: {uncommitted_balance:.2f} {self.market}, minimum: {self.min_order_size:.2f} {self.market})")
            self.log_buy_orders_list()
            return
        
        # If we cancelled orders, wait a moment before placing new ones
        if orders_to_cancel:
            time.sleep(0.5)
        
        # Calculate total amount to allocate from UNCOMMITTED balance only
        # Account for buy fee: if we want to spend $X, we need $X * (1 + fee) available
        # So available_for_orders = uncommitted_balance / (1 + fee)
        available_for_orders = uncommitted_balance / (1 + self.buy_fee)
        
        # When placing all orders from scratch, use 99% to maximize capital usage
        # When adding orders incrementally, use 95% buffer for fees, rounding, and safety margin
        if len(existing_buy_orders) == 0:
            # Placing all orders from scratch - use 99% to maximize capital
            base_buy_amount = available_for_orders * 0.99
        else:
            # Adding orders incrementally - use 95% buffer for safety
            base_buy_amount = available_for_orders * 0.95
        
        # Apply allocation % to uncommitted slice being deployed now
        if allocation_pct < 100:
            total_buy_amount = base_buy_amount * (allocation_pct / 100)
        else:
            total_buy_amount = base_buy_amount
        
        # Never exceed intended ladder budget (allocation % + optional USDC cap)
        intended_ladder = self._intended_buy_committed(allocation_pct)
        remaining_ladder_budget = max(0.0, intended_ladder - existing_buy_committed)
        if total_buy_amount > remaining_ladder_budget:
            if remaining_ladder_budget < self.min_order_size:
                self._info(
                    f"{self.symbol}: Buy ladder at target — ${existing_buy_committed:.0f} committed "
                    f"(target ${intended_ladder:.0f}), no additional orders"
                )
                self.log_buy_orders_list()
                return
            self._info(
                f"{self.symbol}: Capping this placement to ${remaining_ladder_budget:.0f} "
                f"(target ladder ${intended_ladder:.0f}, already ${existing_buy_committed:.0f} committed)"
            )
            total_buy_amount = remaining_ladder_budget
        
        # Identify which ladder levels are missing (gap-filling logic)
        # When skip_levels > 0, we only consider levels from skip_levels onwards
        # The level indices are relative to the effective_levels (after skipping)
        # Note: effective_levels was already defined earlier in the function, use it consistently
        
        # Check if we can place at least one order above minimum
        # If we can't place even one order above minimum, skip trying to place multiple
        if orders_to_place > 0:
            # Calculate if a single order would be above minimum
            single_order_amount = total_buy_amount / orders_to_place if orders_to_place > 0 else 0
            if single_order_amount < self.min_order_size:
                self._info(f"{self.symbol}: Cannot place buy orders - uncommitted balance ({uncommitted_balance:.2f} {self.market}) "
                           f"too small to place even one order above minimum ({self.min_order_size:.2f} {self.market}). "
                           f"Would need {self.min_order_size * orders_to_place:.2f} {self.market} for {orders_to_place} orders.")
                self.log_buy_orders_list()
                return
            
            # CRITICAL: Check if orders will be properly sized using weighted distribution
            # This check only applies when placing ALL orders from scratch
            # When placing missing orders incrementally, each order uses available uncommitted balance
            if len(existing_buy_orders) == 0 and len(effective_levels) > 0:
                # Shallowest level (index 0) gets smallest order in weighted distribution
                # Get the actual level percentage for the first level to apply small position multiplier if needed
                first_level_pct = effective_levels[0] if effective_levels else None
                smallest_expected_order = self.calculate_order_size(0, len(effective_levels), total_buy_amount, is_sell_order=False, buy_level_pct=first_level_pct)
                
                # Use config min_order_size as threshold (no hardcoded multiplier)
                min_acceptable_order_size = self.min_order_size
                
                if smallest_expected_order < min_acceptable_order_size:
                    self._info(f"{self.symbol}: Insufficient balance for properly-sized orders - "
                               f"smallest order would be ${smallest_expected_order:.2f} (need ${min_acceptable_order_size:.2f} minimum). "
                               f"Total available: ${total_buy_amount:.2f}, needed orders: {needed_orders}. "
                               f"Skipping order placement to avoid undersized orders that would trigger rebalance.")
                    self.log_buy_orders_list()
                    return
        
        # Check if price moved up (used for deep level logic)
        price_moved_up = self.price_when_orders_placed > 0 and self.current_price > self.price_when_orders_placed
        
        # If placing all orders from scratch (no existing orders), place all needed_orders in order
        if len(existing_buy_orders) == 0:
            # Simple case: place all orders from scratch
            level_indices_to_fill = list(range(needed_orders))
            self._info(f"{self.symbol}: Placing all {needed_orders} orders from scratch at levels: {[effective_levels[i] for i in level_indices_to_fill]}%")
        else:
            # Map parked orders to the levels they were placed at. Use placement
            # price unless the 4% threshold fired — otherwise a 1-2% drift makes
            # -7/-10/-15% insurance look "missing" and we would duplicate them.
            reference_price = validation_price if validation_price > 0 else self.current_price
            existing_level_indices = self._occupied_buy_level_indices(
                existing_buy_orders, effective_levels, reference_price
            )
            if price_moved_up:
                for order in existing_buy_orders:
                    for relative_idx, level_pct in enumerate(effective_levels):
                        if relative_idx in existing_level_indices or level_pct < 15.0:
                            continue
                        discount_from_current = ((self.current_price - order.rate) / self.current_price) * 100
                        if discount_from_current >= level_pct * 0.8:
                            existing_level_indices.add(relative_idx)
                            logging.debug(
                                f"{self.symbol}: Deep level {level_pct}% order exists at old price "
                                f"{order.rate:.4f} (discount: {discount_from_current:.1f}%), keeping it"
                            )
                            break
            
            # Find missing level indices (gaps to fill) - relative to effective_levels
            all_level_indices = set(range(len(effective_levels)))
            missing_level_indices = sorted(all_level_indices - existing_level_indices)
            
            # Check if we should skip single gap-filling (unless it's a shallow level)
            if should_skip_single_gap and missing_level_indices:
                first_missing_index = missing_level_indices[0]
                if first_missing_index < len(effective_levels):
                    missing_level_pct = effective_levels[first_missing_index]
                    # EXCEPTION: Always allow gap-filling for shallow levels (0.5%, 1.0%) since they're frequent and recover quickly
                    if missing_level_pct <= 1.0:
                        self._info(f"{self.symbol}: Allowing gap-filling for shallow level {missing_level_pct}% "
                                   f"(frequent drops, immediate replacement needed)")
                        should_skip_single_gap = False  # Don't skip shallow levels
                    else:
                        self._info(f"{self.symbol}: Skip gap-filling - only 1 order missing at {missing_level_pct}% level "
                                   f"and price hasn't moved significantly (allows multiple orders to fill before rebalancing)")
                        self.log_buy_orders_list()
                        return
                else:
                    self._info(f"{self.symbol}: Skip gap-filling - only 1 order missing and price hasn't moved significantly "
                               f"(allows multiple orders to fill before rebalancing)")
                    self.log_buy_orders_list()
                    return
            elif should_skip_single_gap:
                self._info(f"{self.symbol}: Skip gap-filling - only 1 order missing and price hasn't moved significantly "
                           f"(allows multiple orders to fill before rebalancing)")
                self.log_buy_orders_list()
                return
            
            # If we have gaps, fill those first; otherwise fill from the end
            if missing_level_indices:
                level_indices_to_fill = missing_level_indices[:orders_to_place]
                self._info(f"{self.symbol}: Filling gaps at levels: {[effective_levels[i] for i in level_indices_to_fill]}%")
            else:
                # No gaps detected, fill from the end (fallback)
                level_indices_to_fill = list(range(len(existing_buy_orders), len(existing_buy_orders) + orders_to_place))
                self._info(f"{self.symbol}: No gaps detected, filling from end at levels: {[effective_levels[i] if i < len(effective_levels) else 'N/A' for i in level_indices_to_fill]}%")
        
        if needed_orders <= 0:
            logging.warning(f"{self.symbol}: Cannot place buy orders - needed_orders is {needed_orders}")
            return
        amount_per_order = total_buy_amount / needed_orders
        
        self._info(f"{self.symbol}: Attempting to place {orders_to_place} buy orders, "
                    f"total_buy_amount: {total_buy_amount:.2f} {self.market}, "
                    f"amount_per_order: {amount_per_order:.2f} {self.market}")
        
        # Collect orders for consolidated notification
        placed_orders = []
        api_error_occurred = False  # Track if server errors occurred
        
        for i, level_index in enumerate(level_indices_to_fill):
            if level_index >= len(effective_levels):
                logging.debug(f"{self.symbol}: Reached end of effective_levels at index {level_index}")
                break
            
            buy_level_pct = effective_levels[level_index]
            
            # Skip placing deep levels (15%+) if price moved up (they should stay at old prices)
            if buy_level_pct >= 15.0 and price_moved_up:
                logging.debug(f"{self.symbol}: Skipping placement of deep level {buy_level_pct}% (price moved up, existing order should stay at old price)")
                continue
            
            buy_price = self.current_price * (1 - buy_level_pct / 100)
            
            # CRITICAL: Don't place orders at levels where ask price is at or below the buy price
            # This prevents infinite fill loops when price stays at a level
            # Use ask_price if available (what we'd pay to buy now), otherwise fall back to current_price
            price_to_check = self.ask_price if self.ask_price > 0 else self.current_price
            
            # Calculate how close the buy price is to current ask
            # When filling gaps after price drops, ensure we have the FULL expected discount
            # to avoid placing orders that would fill immediately
            if price_to_check > 0 and len(existing_buy_orders) > 0:
                discount_from_ask = ((price_to_check - buy_price) / price_to_check) * 100
                # Require full discount percentage when filling gaps (not just 80%)
                # This ensures replacement orders have proper spacing from current ask price
                if discount_from_ask < buy_level_pct:
                    self._info(f"{self.symbol}: Skipping placement at {buy_level_pct}% level - buy price ${buy_price:.4f} is only {discount_from_ask:.2f}% below ask ${price_to_check:.4f} "
                                f"(need full {buy_level_pct:.1f}% discount when filling gaps to avoid immediate fill)")
                    continue
            
            if price_to_check <= buy_price:
                logging.debug(f"{self.symbol}: Skipping placement at {buy_level_pct}% level - ask price ${price_to_check:.4f} is at or below buy price ${buy_price:.4f} "
                            f"(would fill immediately, wait for price to recover or ladder to reposition)")
                continue
            # Use level_index within effective_levels for weight calculation
            # Pass buy_level_pct to apply small position multiplier if applicable
            # When filling gaps with existing orders, calculate order size based on total committed (existing + new)
            # to ensure gap-filling orders match the size of existing orders in the ladder
            if len(existing_buy_orders) > 0:
                # We have existing orders - calculate size based on total ladder (existing + new)
                # This ensures gap-filling orders are properly sized to match the rest of the ladder
                # Use existing_buy_committed + total_buy_amount as the total that will be committed after placing new orders
                total_committed_for_calculation = existing_buy_committed + total_buy_amount
                order_size = self.calculate_order_size(level_index, len(effective_levels), total_committed_for_calculation, is_sell_order=False, buy_level_pct=buy_level_pct)
                # Cap to what we can actually afford per order (total_buy_amount / orders_to_place)
                # This ensures we don't exceed the available uncommitted balance
                max_affordable_per_order = total_buy_amount / orders_to_place if orders_to_place > 0 else total_buy_amount
                if order_size > max_affordable_per_order:
                    # Calculated size exceeds what we can afford from uncommitted balance
                    # This means we don't have enough balance to properly fill the gap at the correct size
                    # The order will be placed at the capped size, but may trigger undersized detection
                    # In that case, the system will cancel all and rebuild (which is correct behavior)
                    logging.debug(f"{self.symbol}: Gap-filling order size ${order_size:.2f} exceeds affordable ${max_affordable_per_order:.2f}, "
                               f"capping to ${max_affordable_per_order:.2f} (may trigger rebalance if too small)")
                    order_size = max_affordable_per_order
            else:
                # No existing orders - calculate based on total_buy_amount (placing all orders from scratch)
                order_size = self.calculate_order_size(level_index, len(effective_levels), total_buy_amount, is_sell_order=False, buy_level_pct=buy_level_pct)
            
            logging.debug(f"{self.symbol}: Buy order {i+1}/{orders_to_place}: level {buy_level_pct}%, "
                         f"price {buy_price:.4f}, size {order_size:.2f} {self.market}, min: {self.min_order_size:.2f}")
            
            # Ensure minimum order size (after accounting for fees)
            # order_size is quote-currency notional; fees may apply on top at execution
            if order_size < self.min_order_size:
                logging.warning(f"{self.symbol}: Skipping buy order {i+1} - order size ({order_size:.2f}) "
                              f"below minimum ({self.min_order_size:.2f})")
                continue
            
            # Log the actual cost including fees
            actual_cost = order_size * (1 + self.buy_fee)
            logging.debug(f"{self.symbol}: Buy order: {order_size:.2f} {self.market} at {buy_price:.4f} "
                         f"(actual cost with {self.buy_fee*100}% fee: {actual_cost:.2f} {self.market})")
            
            try:
                # Calculate coin amount from quote notional and rate
                # API expects coin amount, not quote notional (per API docs)
                coin_amount = order_size / buy_price
                
                self._info(f"{self.symbol}: Placing buy order {i+1}/{orders_to_place}: "
                           f"{coin_amount:.8f} {self.cointype} at {buy_price:.4f} {self.market} "
                           f"(spending {order_size:.2f} {self.market})")
                
                response = self.api.place_buy_order(
                    cointype=self.cointype,
                    amount=coin_amount,  # Coin amount (API expects coins, not quote notional)
                    rate=buy_price,
                    market=self.market
                )
                
                if response.get('status') == 'ok':
                    order_id = response.get('id', 'unknown')
                    self._info(f"{self.symbol}: Placed buy order {i+1}/{orders_to_place} at {buy_price:.2f} "
                               f"({buy_level_pct}% below price), size: {order_size:.2f} {self.market}, order ID: {order_id}")
                    # Track order ID for fill detection
                    if not hasattr(self, 'previous_order_ids'):
                        self.previous_order_ids = set()
                    self.previous_order_ids.add(order_id)
                    # Collect order for consolidated notification
                    placed_orders.append({
                        'amount': coin_amount,
                        'rate': buy_price,
                        'order_id': order_id,
                        'level_pct': buy_level_pct
                    })
                else:
                    error_msg = response.get('message', 'Unknown error')
                    logging.error(f"{self.symbol}: Failed to place buy order {i+1}: {error_msg}")
                    logging.error(f"{self.symbol}: Response: {response}")
                    self.notifier.notify_error(self.symbol, f"Buy order failed: {error_msg}")
            except Exception as e:
                error_str = str(e)
                logging.error(f"{self.symbol}: Exception placing buy order {i+1}: {error_str}")
                logging.error(f"{self.symbol}: Order details - coin_amount: {coin_amount:.8f}, "
                            f"rate: {buy_price:.4f}, order_size: {order_size:.2f} {self.market}")
                
                # Check if this is a server error (502, 500, 503, 504)
                # The API client already retried, so if we get here, it failed after retries
                is_server_error = any(status_code in error_str for status_code in ['502', '500', '503', '504', 'Bad Gateway'])
                
                if is_server_error:
                    api_error_occurred = True
                    logging.warning(f"{self.symbol}: Server error occurred during order placement (already retried by API client). "
                                  f"Stopping order placement to avoid incomplete ladder. Will retry next cycle.")
                    break
                
                # Handle "Insufficient funds" - likely a buy filled mid-cycle causing stale balance
                # Stop placing more orders this cycle; next cycle will have fresh balance
                if 'Insufficient funds' in error_str:
                    logging.warning(f"{self.symbol}: Insufficient funds detected - likely a buy filled mid-cycle. "
                                  f"Stopping order placement, will retry next cycle with fresh balance.")
                    break
                
                # Only notify on first error to avoid spam
                if i == 0:
                    self.notifier.notify_error(self.symbol, f"Buy order exception: {error_str[:100]}")
        
        # If API errors occurred during order placement, reset excessive new funds detection
        # This prevents false positives where incomplete ladder due to API errors
        # triggers the excessive new funds detection
        if api_error_occurred:
            if self._increased_capital_detected_at is not None:
                self._info(f"{self.symbol}: API errors occurred during order placement - resetting excessive new funds detection "
                           f"to avoid false positive trigger from incomplete ladder.")
                self._increased_capital_detected_at = None
        
        # Send notification if any orders were placed (immediate notification)
        if placed_orders:
            self.notifier.notify_buy_ladder_recalculated(
                symbol=self.symbol,
                orders=placed_orders,
                current_price=self.current_price
            )
        
        # Track price when orders were placed (for slow price drop detection)
        if self.current_price > 0:
            self.price_when_orders_placed = self.current_price
        
        # Re-fetch open orders so tracking matches exchange (dry-run store / API)
        try:
            orders_data = self.api.get_orders(self.cointype, self.market)
            if orders_data:
                self.update_open_orders(orders_data)
                logging.debug(
                    f"{self.symbol}: Re-fetched orders after buy ladder, "
                    f"found {len(self.get_buy_orders())} buy orders"
                )
        except Exception as e:
            logging.warning(f"{self.symbol}: Failed to re-fetch orders after buy placement: {e}")

        # Update tracking variable for buy committed amount (after placing orders)
        updated_buy_orders = self.get_buy_orders()
        self._last_buy_committed = sum(order.amount * order.rate for order in updated_buy_orders)

        # Log buy orders list at end of function
        self.log_buy_orders_list()
    def place_sell_ladder(self):
        """Place sell orders at ladder levels above average entry price (Core) and/or current price (Working)"""
        avg_entry, coin_amount = self.calculate_average_entry()
        
        if avg_entry == 0 or coin_amount == 0:
            logging.warning(f"{self.symbol}: Cannot place sell orders - avg_entry: {avg_entry}, coin_amount: {coin_amount}")
            if self.coin_balance > 0:
                logging.warning(f"{self.symbol}: Have {self.coin_balance:.8f} coins but average entry calculation failed. This may be due to transaction history API issues.")
            return
        
        if coin_amount < 0.0001:  # Minimum coin amount check
            logging.debug(f"{self.symbol}: Coin amount too small to sell ({coin_amount:.8f})")
            return
        
        # Only place sell orders if we have a meaningful position
        # Skip sell orders if balance is too small - focus on building position with buy orders first
        # Require at least enough for meaningful sell orders (e.g., 10+ coins or 5% of a reasonable position)
        min_meaningful_balance = 10.0  # Skip sell orders if balance is less than this
        if coin_amount < min_meaningful_balance:
            self._info(f"{self.symbol}: Coin balance too small ({coin_amount:.8f}) for sell orders "
                        f"(minimum: {min_meaningful_balance}). "
                        f"Skipping sell orders - focus on building position with buy orders first.")
            return
        
        # Mean-reversion mode: Split position into Core and Working
        if self.mean_reversion_enabled:
            # Decide whether the Working ladder will actually place BEFORE splitting the
            # position. The Working slice is only carved out when it has somewhere to go;
            # otherwise it reverts to Core rather than sitting idle on neither ladder.
            # Working only runs while underwater by more than cancel_working_threshold_pct
            # (mirrors the guard in _place_working_sell_ladder) - near or above avg entry
            # Core handles selling at a profit.
            candidate_working = coin_amount * self.working_position_pct
            min_working = self._min_working_coins_threshold()
            
            if avg_entry > 0:
                entry_threshold = avg_entry * (1.0 - self.cancel_working_threshold_pct / 100.0)
                below_working_threshold = self.current_price < entry_threshold
            else:
                below_working_threshold = True
            
            working_active = (
                self.working_ladder_enabled
                and candidate_working >= min_working
                and self.current_price > 0
                and below_working_threshold
            )
            
            if working_active:
                self.working_coins = candidate_working
                self.core_coins = coin_amount - self.working_coins
                self._info(f"{self.symbol}: Mean-reversion mode: Core={self.core_coins:.8f} ({100*(1-self.working_position_pct):.1f}%), "
                            f"Working={self.working_coins:.8f} ({100*self.working_position_pct:.1f}%)")
            else:
                # Working stood down - cancel any leftover Working orders first so their
                # coins are free before Core sizes against the full position.
                if not hasattr(self, '_working_order_ids'):
                    self._working_order_ids = set()
                stale_working = [
                    order for order in self.get_sell_orders()
                    if order.order_id in self._working_order_ids
                ]
                if stale_working:
                    self._info(f"{self.symbol}: Working ladder standing down - cancelling {len(stale_working)} "
                               f"Working order(s) and returning the slice to Core")
                    self._cancel_working_orders(stale_working)
                    time.sleep(0.5)
                
                self.working_coins = 0.0
                self.core_coins = coin_amount
                
                if not self.working_ladder_enabled:
                    logging.debug(f"{self.symbol}: Working ladder disabled in config - full position allocated to Core")
                elif self.current_price <= 0:
                    logging.warning(f"{self.symbol}: Cannot place Working ladder - no current price available")
                elif not below_working_threshold:
                    pct_below_entry = ((avg_entry - self.current_price) / avg_entry * 100) if avg_entry > 0 else 0.0
                    self._info(f"{self.symbol}: Price (${self.current_price:.4f}) within {self.cancel_working_threshold_pct:.1f}% of "
                               f"avg entry (${avg_entry:.4f}, {pct_below_entry:.2f}% below) - "
                               f"Working ladder off, full position allocated to Core")
                else:
                    self._info(
                        f"{self.symbol}: Working slice too small ({candidate_working:.8f} "
                        f"< {min_working:.8f} min for ${self.min_order_size:.2f} orders) - "
                        f"full position allocated to Core"
                    )
            
            # Place Core ladder (recovery strategy - relative to avg_entry)
            if self.core_coins >= min_meaningful_balance:
                self._place_core_sell_ladder(avg_entry, self.core_coins)
            else:
                self._info(f"{self.symbol}: Core position too small ({self.core_coins:.8f}), skipping Core ladder")
            
            # Place Working ladder (mean-reversion strategy - relative to current_price)
            if working_active:
                self._place_working_sell_ladder(self.working_coins, avg_entry)
        else:
            # Recovery mode: All position is Core (existing behavior)
            self.core_coins = coin_amount
            self.working_coins = 0.0
            self._place_core_sell_ladder(avg_entry, coin_amount)
        
        # Re-fetch open orders so log reflects newly placed orders (critical for Working ladder:
        # after cancel/replace, open_orders would otherwise still have cancelled order IDs that
        # no longer match _working_order_ids, causing all to appear as [Core])
        try:
            orders_data = self.api.get_orders(self.cointype, self.market)
            if orders_data:
                self.update_open_orders(orders_data)
                logging.debug(f"{self.symbol}: Re-fetched orders after sell ladder, found {len(self.get_sell_orders())} sell orders")
        except Exception as e:
            logging.warning(f"{self.symbol}: Failed to re-fetch orders before logging: {e}")
        
        # Log combined sell orders list after both Core and Working ladders have been processed
        self.log_sell_orders_list()
    
    def _place_core_sell_ladder(self, avg_entry: float, core_coins: float):
        """Place Core sell ladder (recovery strategy - relative to average entry price)"""
        
        # Ensure tracking set is initialized
        if not hasattr(self, '_working_order_ids'):
            self._working_order_ids = set()
        
        # Ensure open_orders is initialized
        if not hasattr(self, 'open_orders') or self.open_orders is None:
            self.open_orders = []
        
        existing_sell_orders = self.get_sell_orders()
        
        # CRITICAL: Exclude Working orders from Core processing
        # Working orders are tracked by order_id to avoid misidentification after price moves
        existing_sell_orders = [order for order in existing_sell_orders 
                               if order.order_id not in self._working_order_ids]
        
        # Determine Core order limit (independent or split from max_sell_orders)
        if self.mean_reversion_enabled:
            if self.max_core_sell_orders is not None:
                # Use explicit Core limit if configured
                max_core_orders = min(self.max_core_sell_orders, len(self.sell_levels))
            else:
                # Fallback: Split max_sell_orders (legacy behavior)
                max_working_orders = min(len(self.working_sell_levels), max(3, self.max_sell_orders // 3))
                max_core_orders = self.max_sell_orders - max_working_orders
        else:
            # Recovery mode: Use max_sell_orders for Core (all orders are Core)
            if self.max_core_sell_orders is not None:
                max_core_orders = min(self.max_core_sell_orders, len(self.sell_levels))
            else:
                max_core_orders = self.max_sell_orders
        
        needed_orders = min(max_core_orders, len(self.sell_levels))
        
        # Check for and cancel dust orders FIRST (before any other validation)
        # These are tiny orders left after partial fills that should be removed
        # This check must run before early returns to ensure dust orders are always caught
        dust_threshold = min(0.10, self.min_order_size * 0.01)  # $0.10 or 1% of min_order_size, whichever is smaller
        dust_orders = []
        for order in existing_sell_orders:
            order_value = order.amount * order.rate
            if order_value < dust_threshold:
                dust_orders.append(order)
                self._info(f"{self.symbol}: Detected dust sell order: {order.amount:.8f} @ ${order.rate:.4f} = ${order_value:.2f} "
                           f"(below ${dust_threshold:.2f} threshold) - will cancel")
        
        # Cancel dust orders
        if dust_orders:
            for order in dust_orders:
                try:
                    self._info(f"{self.symbol}: Cancelling dust sell order {order.order_id} at {order.rate:.4f} "
                               f"(value: ${order.amount * order.rate:.2f})")
                    self.api.cancel_order(order.order_id, order_type="sell")
                    existing_sell_orders.remove(order)  # Remove from list to avoid processing it further
                except Exception as e:
                    logging.warning(f"{self.symbol}: Failed to cancel dust sell order {order.order_id}: {e}")
            
            self._info(f"{self.symbol}: Cancelled {len(dust_orders)} dust sell order(s), waiting for cancellations to process...")
            time.sleep(0.5)
            # Re-fetch orders to get accurate state after cancellation
            try:
                orders_data = self.api.get_orders(self.cointype, self.market)
                if orders_data:
                    self.update_open_orders(orders_data)
                    existing_sell_orders = self.get_sell_orders()
                    logging.debug(f"{self.symbol}: Re-fetched orders after dust cancellation, found {len(existing_sell_orders)} remaining sell orders")
            except Exception as e:
                logging.warning(f"{self.symbol}: Could not re-fetch orders after dust cancellation: {e}")
        
        # Check if existing sell orders match the ladder structure
        # Cancel orders that don't match expected price levels
        orders_to_cancel = []
        valid_orders = []
        
        for order in existing_sell_orders:
            # Calculate expected price for each ladder level based on average entry
            matches_ladder = False
            best_match_diff = float('inf')
            for level_pct in self.sell_levels[:needed_orders]:
                expected_price = avg_entry * (1 + level_pct / 100)
                # Allow 0.5% tolerance for price matching
                price_diff_pct = abs((order.rate - expected_price) / expected_price) * 100
                if price_diff_pct < best_match_diff:
                    best_match_diff = price_diff_pct
                if price_diff_pct < 0.5:
                    matches_ladder = True
                    break
            
            if matches_ladder:
                valid_orders.append(order)
                logging.debug(f"{self.symbol}: Sell order {order.order_id} at {order.rate:.4f} matches ladder (diff: {best_match_diff:.2f}%)")
            else:
                orders_to_cancel.append(order)
                logging.debug(f"{self.symbol}: Sell order {order.order_id} at {order.rate:.4f} doesn't match ladder (closest diff: {best_match_diff:.2f}%)")
        
        # If we have more valid orders than needed, cancel the extras
        # This can happen if bot was restarted or orders were placed incorrectly
        if len(valid_orders) > needed_orders:
            # Sort by price (keep lowest profit levels first = more likely to fill)
            valid_orders.sort(key=lambda o: o.rate)  # Sort by price (lower prices = lower profit levels)
            extra_orders = valid_orders[needed_orders:]
            valid_orders = valid_orders[:needed_orders]
            orders_to_cancel.extend(extra_orders)
            self._info(f"{self.symbol}: Found {len(extra_orders)} extra sell orders (have {len(valid_orders) + len(extra_orders)}, need {needed_orders}) - cancelling extras")
        
        # Cancel sell orders that don't match the ladder or are extra
        cancelled_count = 0
        for order in orders_to_cancel:
            try:
                self._info(f"{self.symbol}: Cancelling sell order {order.order_id} at {order.rate:.4f} "
                           f"(doesn't match ladder structure or is extra)")
                result = self.api.cancel_order(order.order_id, order_type="sell")
                if result.get('status') == 'ok':
                    cancelled_count += 1
            except Exception as e:
                logging.warning(f"{self.symbol}: Failed to cancel sell order {order.order_id}: {e}")
        
        # Update existing_sell_orders to only include valid ones
        existing_sell_orders = valid_orders
        
        # Identify which levels have existing orders by matching orders to their levels
        # This is critical for correctly placing missing orders when a sell order fills
        occupied_levels = set()
        for order in existing_sell_orders:
            # Find which level this order matches
            for level_idx, level_pct in enumerate(self.sell_levels[:needed_orders]):
                expected_price = avg_entry * (1 + level_pct / 100)
                price_diff_pct = abs((order.rate - expected_price) / expected_price) * 100
                if price_diff_pct < 0.5:  # Same tolerance as validation
                    occupied_levels.add(level_idx)
                    break
        
        # Calculate committed coins after validation (only valid orders)
        # NOTE: Working orders were already filtered out at the start of this method using order_id tracking
        existing_sell_amount = sum(order.amount for order in existing_sell_orders)
        available_coins = core_coins - existing_sell_amount
        
        # Log comprehensive sell ladder check (standardized with buy ladder format)
        self._info(f"{self.symbol}: Core Sell Ladder Check\n"
                    f"  existing: {len(existing_sell_orders)}\n"
                    f"  max_orders: {needed_orders}\n"
                    f"  avg_entry: {avg_entry:.4f}\n"
                    f"  core_coins: {core_coins:.8f}\n"
                    f"  committed: {existing_sell_amount:.8f}\n"
                    f"  available: {available_coins:.8f}")
        
        if len(existing_sell_orders) >= needed_orders:
            # Update sell orders if:
            # 1. Average entry changed significantly (new buys filled)
            # 2. Balance increased significantly (new coins available for selling)
            # 3. Available coins significantly exceed committed coins (orders need to be resized)
            old_avg_entry = getattr(self, '_last_avg_entry', 0)
            old_coin_amount = getattr(self, '_last_coin_amount', 0)
            
            avg_entry_changed = False
            balance_increased = False
            needs_resize = False
            
            # IMPORTANT: Average Entry Price Effect with LIFO
            # When sell orders fill, they consume buy orders via LIFO (newest first), which changes
            # the average entry price. This creates a different effect than FIFO:
            #
            # Scenario A (most common during recovery): Newest orders are at LOWER prices
            #   - Sell fills → consumes low-priced buys → avg entry INCREASES
            #   - Sell ladder recalculated with HIGHER prices (same profit % but higher absolute prices)
            #   - Higher prices = less likely to fill → self-correcting, prevents premature selling
            #   - Each sell is profitable relative to the specific buy order (not just on average)
            #
            # Scenario B: Newest orders are at HIGHER prices
            #   - Sell fills → consumes high-priced buys → avg entry DECREASES
            #   - Sell ladder recalculated with LOWER prices (same profit % but lower absolute prices)
            #   - Lower prices = more likely to fill → more sells → further avg entry decreases
            #
            # This behavior is EXPECTED and by design - it ensures sell orders always reflect the true
            # cost basis of remaining holdings. The threshold prevents excessive recalculation on
            # minor changes, but significant position changes (like multiple sells filling) will trigger
            # recalculation to maintain accurate profit targets relative to actual cost basis.
            #
            # LIFO is optimized for mean-reversion trading because it:
            # 1. Sells lower-cost-basis coins first, ensuring each order is profitable
            # 2. Frees up capital earlier by selling profitable positions first
            # 3. Maintains higher average entry on remaining holdings (higher absolute sell prices)
            # 4. Maximizes profit potential by keeping higher-cost-basis coins longer
            #
            # THRESHOLD ANALYSIS (1%):
            # - Minimum sell level: 3.0% profit above entry
            # - 1% change in avg entry = 33% of minimum profit target (significant but not excessive)
            # - Typical sell order: 6-10% of position (with equal distribution: 95% / 15 orders ≈ 6.3%)
            # - Impact per sell: If oldest buy is 5-10% above avg entry and represents 10% of position,
            #   removing it could change avg entry by 0.5-1.0% (depends on price spread)
            #
            # Is 1% optimal?
            # - Too low (0.5%): Would trigger on every small sell, causing excessive recalculation,
            #   more API calls, more order cancellations, higher fees, potential race conditions
            # - Too high (2-3%): Would allow sell orders to drift significantly from true cost basis.
            #   Example: If avg entry drops 2% but threshold is 2%, orders stay at old prices,
            #   potentially selling at less profitable prices than intended (profit % would be lower
            #   than target relative to NEW cost basis)
            # - 1%: Good balance - triggers on meaningful changes (single large sell or multiple small
            #   sells) while avoiding excessive recalculation. Represents 33% of minimum profit target,
            #   which is significant enough to warrant recalculation but not so small as to be noisy.
            #
            # SCENARIO-SPECIFIC CONSIDERATIONS:
            # - Scenario A (cascading): 1% is appropriate - allows progressive recalculation as sells
            #   fill, maintaining accuracy without over-trading. Multiple sells filling quickly will
            #   cumulatively exceed 1% and trigger recalculation.
            # - Scenario B (self-correcting): 1% is appropriate - prevents unnecessary recalculation
            #   when avg entry increases (which makes sells less likely to fill anyway)
            # - Large position changes: 1% threshold ensures recalculation happens when position
            #   changes meaningfully, maintaining profit target accuracy
            #
            # CONCLUSION: 1% is a well-balanced threshold that works well in all scenarios. It's
            # significant enough to represent meaningful cost basis changes (33% of minimum profit
            # target) while avoiding excessive recalculation. Consider adjusting only if:
            # - You experience excessive recalculation (lower to 1.5-2%) OR
            # - You notice sell orders drifting from true cost basis (lower to 0.5-0.75%)
            
            if old_avg_entry > 0 and abs(avg_entry - old_avg_entry) / old_avg_entry > 0.01:  # 1% change
                avg_entry_changed = True
                self._info(f"{self.symbol}: Average entry changed ({old_avg_entry:.4f} -> {avg_entry:.4f}), updating sell ladder")
            
            # Check if balance increased significantly
            old_core_coins = getattr(self, '_last_core_coins', core_coins)
            if old_core_coins > 0 and core_coins > old_core_coins * 1.05:  # Core balance increased by >5%
                balance_increased = True
                increase_pct = ((core_coins - old_core_coins) / old_core_coins) * 100
                self._info(f"{self.symbol}: Core balance increased by {increase_pct:.1f}% ({old_core_coins:.8f} -> {core_coins:.8f}), updating Core ladder")
            
            # Check for over-commitment: Core orders using more coins than Core allocation allows
            # This can happen if working_position_pct was changed in config between restarts
            # If available_coins is negative, Core is definitively over-committed
            # Must check BEFORE the normal rebalance logic, as available_coins will be negative
            if available_coins < 0 and existing_sell_amount > 0 and old_core_coins > 0:
                over_commit_pct = ((existing_sell_amount - core_coins) / core_coins) * 100
                self._info(f"{self.symbol}: Core sell orders OVER-COMMITTED - "
                           f"committed {existing_sell_amount:.8f} exceeds Core allocation {core_coins:.8f} "
                           f"by {over_commit_pct:.1f}%. Resizing to release coins for Working orders.")
                needs_resize = True
            
            # Check if available coins significantly exceed committed coins (indicates orders need resizing)
            # Only resize if we have previous tracking AND balance actually increased (new buys filled)
            # DON'T resize on first run (old_core_coins == 0) - that's handled separately below
            if existing_sell_amount > 0 and available_coins > existing_sell_amount * self.sell_ladder_rebalance_threshold:
                # Only resize if we have previous tracking AND balance increased (new buys), not on first run
                if old_core_coins > 0 and core_coins > old_core_coins:
                    needs_resize = True
                    unused_pct = (available_coins / core_coins) * 100 if core_coins > 0 else 0
                    self._info(f"{self.symbol}: Available Core coins ({available_coins:.8f}) significantly exceed committed ({existing_sell_amount:.8f}), "
                               f"{unused_pct:.1f}% unused. Updating Core ladder to use full balance.")
                elif old_core_coins > 0:
                    # Balance decreased or unchanged (sell filled) - don't resize, keep existing orders
                    logging.debug(f"{self.symbol}: Available coins exceed committed, but balance decreased/unchanged (sell filled). "
                                f"Keeping existing orders, not resizing.")
            
            # First run after startup (old_core_coins is 0) - just initialize tracking, DON'T resize
            # Existing orders were placed before restart, keep them as-is with their original sizes
            # Only resize if balance actually increases DURING operation (new buys fill)
            # Resizing on restart would dilute higher-priced orders and reduce maximum profit potential
            #
            # EXCEPTION: If Core orders are over-committed (committed > core_coins by >2%), this means
            # the Core/Working split changed (e.g., working_position_pct was adjusted in config).
            # In this case, we MUST resize to release coins for Working orders.
            if old_core_coins == 0 and core_coins > 0:
                # Check for over-commitment: Core orders using more coins than Core allocation allows
                # This happens when working_position_pct is increased or Core/Working split is changed
                # If available_coins is negative, Core is definitively over-committed
                if available_coins < 0 and existing_sell_amount > 0:
                    over_commit_pct = ((existing_sell_amount - core_coins) / core_coins) * 100
                    self._info(f"{self.symbol}: Core sell orders OVER-COMMITTED on startup - "
                               f"committed {existing_sell_amount:.8f} exceeds Core allocation {core_coins:.8f} "
                               f"by {over_commit_pct:.1f}%. Resizing to release coins for Working orders.")
                    self._resize_sell_orders(existing_sell_orders, core_coins, avg_entry)
                    self._last_avg_entry = avg_entry
                    self._last_core_coins = core_coins
                    return
                elif available_coins > existing_sell_amount * self.sell_ladder_rebalance_threshold:
                    unused_pct = (available_coins / core_coins) * 100 if core_coins > 0 else 0
                    self._info(f"{self.symbol}: First Core sell ladder check - {core_coins:.8f} coins, "
                               f"{available_coins:.8f} uncommitted ({unused_pct:.1f}%). "
                               f"Keeping existing {len(existing_sell_orders)} orders (restart detected, not resizing).")
                else:
                    self._info(f"{self.symbol}: First Core sell ladder check - {core_coins:.8f} coins, "
                               f"all committed to {len(existing_sell_orders)} orders. Keeping existing orders.")
                # Only update prices if avg_entry changed, don't resize
                if avg_entry_changed:
                    self._update_sell_orders(existing_sell_orders, avg_entry)
                self._last_avg_entry = avg_entry
                self._last_core_coins = core_coins
                return
            
            if avg_entry_changed or balance_increased or needs_resize:
                if needs_resize and avg_entry_changed:
                    # Both conditions: need to resize amounts AND update prices
                    # Cancel and recreate with NEW prices (based on new avg_entry) and new amounts
                    self._info(f"{self.symbol}: Resizing and updating Core sell orders - avg_entry changed ({old_avg_entry:.4f} -> {avg_entry:.4f}), "
                               f"cancelling {len(existing_sell_orders)} orders to use full Core balance with updated prices")
                    self._resize_and_update_sell_orders(existing_sell_orders, core_coins, avg_entry)
                    # Return immediately after resizing - tracking variables will be set below
                elif needs_resize:
                    # Only resizing needed: preserve prices, update amounts
                    self._info(f"{self.symbol}: Resizing Core sell orders - cancelling {len(existing_sell_orders)} orders to use full Core balance (preserving prices)")
                    self._resize_sell_orders(existing_sell_orders, core_coins, avg_entry)
                    # Return immediately after resizing - tracking variables will be set below
                else:
                    # Only price update needed: update prices, keep amounts
                    self._update_sell_orders(existing_sell_orders, avg_entry)
            
            self._last_avg_entry = avg_entry
            self._last_core_coins = core_coins
            logging.debug(f"{self.symbol}: Sell ladder complete ({len(existing_sell_orders)} orders), no new orders needed")
            return
        
        # If we cancelled orders, wait and re-fetch to get accurate state
        if orders_to_cancel:
            self._info(f"{self.symbol}: Cancelled {len(orders_to_cancel)} sell orders, waiting for cancellations to process...")
            time.sleep(1.5)  # Give time for cancellations to complete
            
            # Re-fetch orders to get accurate current state
            try:
                orders_data = self.api.get_orders(self.cointype, self.market)
                if orders_data:
                    self.update_open_orders(orders_data)
                    existing_sell_orders = self.get_sell_orders()
                    self._info(f"{self.symbol}: After cancellation, found {len(existing_sell_orders)} remaining sell orders")
            except Exception as e:
                logging.warning(f"{self.symbol}: Could not re-fetch orders after cancellation: {e}")
        
        # Re-check if we have enough orders after cancellation
        if len(existing_sell_orders) >= needed_orders:
            self._info(f"{self.symbol}: Sell ladder complete after cancellation ({len(existing_sell_orders)} orders)")
            return
        
        # Check for CRITICAL gap in ladder coverage
        # If we have significantly fewer orders than PLACEABLE levels, we're missing profit opportunities
        # Note: Some levels can't be placed if current price is above the sell price
        existing_sell_amount = sum(order.amount for order in existing_sell_orders)
        available_coins = max(0, core_coins - existing_sell_amount)
        old_core_coins = getattr(self, '_last_core_coins', core_coins)
        
        # Count how many levels are actually placeable (sell price > current price)
        placeable_levels = 0
        for level_pct in self.sell_levels[:needed_orders]:
            sell_price = avg_entry * (1 + level_pct / 100)
            if sell_price > self.current_price:
                placeable_levels += 1
        
        # Only consider it a gap if we have fewer orders than 80% of PLACEABLE levels
        # (not total needed_orders, which includes levels below current price)
        ladder_has_gaps = placeable_levels > 0 and len(existing_sell_orders) < placeable_levels * 0.8
        if ladder_has_gaps:
            missing_count = placeable_levels - len(existing_sell_orders)
            coverage_pct = (len(existing_sell_orders) / placeable_levels) * 100 if placeable_levels > 0 else 100
            self._info(f"{self.symbol}: ⚠️ LADDER GAP DETECTED - Only {len(existing_sell_orders)}/{placeable_levels} placeable orders ({coverage_pct:.0f}% coverage). "
                       f"Missing {missing_count} orders means missing profit opportunities on rebounds! "
                       f"(Note: {needed_orders - placeable_levels} levels skipped - price is above those sell prices)")
            self._info(f"{self.symbol}: Rebuilding sell ladder to fill gaps and restore full coverage.")
            
            # Cancel existing orders and rebuild with proper levels
            for order in existing_sell_orders:
                try:
                    self.api.cancel_order(order.order_id, order_type="sell")
                    logging.debug(f"{self.symbol}: Cancelled sell order {order.order_id} for ladder rebuild")
                except Exception as e:
                    logging.warning(f"{self.symbol}: Failed to cancel order {order.order_id}: {e}")
            
            # Wait for cancellations
            if existing_sell_orders:
                time.sleep(1.5)
            
            # Reset for full rebuild - go directly to order placement
            existing_sell_orders = []
            existing_sell_amount = 0
            available_coins = core_coins
            # Mark as balance_increased to skip checks below and proceed with placement
            balance_increased = True
        else:
            # Normal flow: Only add orders if balance increased or on first run
            balance_increased = False
            
            if old_core_coins > 0:
                # Check if balance increased significantly (new buys filled)
                if core_coins > old_core_coins * 1.05:  # Core balance increased by >5%
                    balance_increased = True
                    increase_pct = ((core_coins - old_core_coins) / old_core_coins) * 100
                    self._info(f"{self.symbol}: Core balance increased by {increase_pct:.1f}% ({old_core_coins:.8f} -> {core_coins:.8f}), "
                               f"will add new Core sell orders to use additional coins")
                elif core_coins <= old_core_coins:
                    # Balance decreased or stayed same (sell order filled, no new buys)
                    # BUT check if any levels have become newly placeable due to price drop
                    # (sell price was above current price before, now below)
                    
                    # Find occupied levels
                    occupied_levels = set()
                    for order in existing_sell_orders:
                        for level_idx, level_pct in enumerate(self.sell_levels[:needed_orders]):
                            expected_price = avg_entry * (1 + level_pct / 100)
                            price_diff_pct = abs((order.rate - expected_price) / expected_price) * 100
                            if price_diff_pct < 0.5:
                                occupied_levels.add(level_idx)
                                break
                    
                    # Find placeable levels that are missing orders
                    newly_placeable_levels = []
                    for level_idx, level_pct in enumerate(self.sell_levels[:needed_orders]):
                        sell_price = avg_entry * (1 + level_pct / 100)
                        if sell_price > self.current_price and level_idx not in occupied_levels:
                            newly_placeable_levels.append((level_idx, level_pct, sell_price))
                    
                    if newly_placeable_levels:
                        # There are placeable levels without orders - need to redistribute!
                        # Option B: Don't redistribute if a sell just filled this cycle
                        if getattr(self, '_sell_filled_this_cycle', False):
                            self._info(f"{self.symbol}: Price dropped - {len(newly_placeable_levels)} new level(s) now placeable, "
                                       f"but sell filled this cycle - deferring redistribution to next cycle.")
                            self._last_avg_entry = avg_entry
                            self._last_core_coins = core_coins
                            return
                        
                        # Since all coins are committed to existing orders, we need to RESIZE
                        # to include the new levels in the distribution
                        self._info(f"{self.symbol}: Price dropped - {len(newly_placeable_levels)} new level(s) now placeable. "
                                   f"Resizing all orders to redistribute across {len(existing_sell_orders) + len(newly_placeable_levels)} levels.")
                        
                        # Cancel existing orders and rebuild with all placeable levels
                        for order in existing_sell_orders:
                            try:
                                self.api.cancel_order(order.order_id, order_type="sell")
                                logging.debug(f"{self.symbol}: Cancelled sell order {order.order_id} for redistribution")
                            except Exception as e:
                                logging.warning(f"{self.symbol}: Failed to cancel order {order.order_id}: {e}")
                        
                        if existing_sell_orders:
                            time.sleep(1.0)
                        
                        # Place orders at ALL placeable levels with proper distribution
                        total_for_all_orders = core_coins * 0.98
                        all_placeable_levels = []
                        for level_idx, level_pct in enumerate(self.sell_levels[:needed_orders]):
                            sell_price = avg_entry * (1 + level_pct / 100)
                            if sell_price > self.current_price:
                                all_placeable_levels.append((level_pct, sell_price))
                        
                        # Use actual placeable count for proper weight normalization
                        num_placeable = len(all_placeable_levels)
                        placed_amount = 0
                        for order_idx, (level_pct, sell_price) in enumerate(all_placeable_levels):
                            if self.sell_order_distribution == "weighted":
                                # Use order_idx (0-based within placeable levels) for proper weight distribution
                                order_size = self.calculate_order_size(order_idx, num_placeable, total_for_all_orders, is_sell_order=True)
                            else:
                                order_size = total_for_all_orders / num_placeable
                            
                            # Ensure we don't exceed remaining coins
                            remaining = total_for_all_orders - placed_amount
                            order_size = min(order_size, remaining)
                            
                            if order_size < 0.0001:
                                continue
                            
                            try:
                                rounded_amount = round(order_size, 8)
                                rounded_rate = round(sell_price, 8)
                                
                                response = self.api.place_sell_order(
                                    cointype=self.cointype,
                                    amount=rounded_amount,
                                    rate=rounded_rate,
                                    market=self.market
                                )
                                
                                if response.get('status') == 'ok':
                                    order_id = response.get('id', 'unknown')
                                    self._info(f"{self.symbol}: Placed sell order at {rounded_rate:.4f} "
                                               f"({level_pct}% above entry), size: {rounded_amount:.8f}")
                                    placed_amount += rounded_amount
                                    # Track order ID for fill detection
                                    if not hasattr(self, 'previous_order_ids'):
                                        self.previous_order_ids = set()
                                    self.previous_order_ids.add(order_id)
                                else:
                                    error_msg = response.get('message', 'Unknown error')
                                    logging.error(f"{self.symbol}: Failed to place sell order at {level_pct}%: {error_msg}")
                            except Exception as e:
                                logging.error(f"{self.symbol}: Exception placing sell order at {level_pct}%: {e}")
                        
                        self._last_avg_entry = avg_entry
                        self._last_core_coins = core_coins
                        return
                    else:
                        # No newly placeable levels - but check for significant uncommitted balance
                        # This can happen after sells fill: orders are gone but remaining orders are undersized
                        uncommitted_pct = (available_coins / core_coins * 100) if core_coins > 0 else 0
                        if available_coins > core_coins * self.sell_ladder_rebalance_threshold:
                            # Option B: Don't resize if a sell just filled this cycle
                            # This preserves higher-level order sizes instead of diluting them
                            if getattr(self, '_sell_filled_this_cycle', False):
                                self._info(f"{self.symbol}: Significant uncommitted Core balance detected ({available_coins:.4f} coins, "
                                           f"{uncommitted_pct:.1f}%) but sell filled this cycle - NOT resizing to preserve order sizes.")
                                self._last_avg_entry = avg_entry
                                self._last_core_coins = core_coins
                                return
                            
                            self._info(f"{self.symbol}: Significant uncommitted Core balance detected ({available_coins:.4f} coins, "
                                       f"{uncommitted_pct:.1f}%). Resizing all Core orders to use full balance.")
                            
                            # Cancel existing orders and rebuild with proper distribution
                            for order in existing_sell_orders:
                                try:
                                    self.api.cancel_order(order.order_id, order_type="sell")
                                    logging.debug(f"{self.symbol}: Cancelled sell order {order.order_id} for rebalance")
                                except Exception as e:
                                    logging.warning(f"{self.symbol}: Failed to cancel order {order.order_id}: {e}")
                            
                            if existing_sell_orders:
                                time.sleep(1.0)
                            
                            # Place ONLY after cancel — never on top of existing Core sells.
                            total_for_all_orders = core_coins * 0.98
                            all_placeable_levels = []
                            for level_pct in self.sell_levels[:needed_orders]:
                                sell_price = avg_entry * (1 + level_pct / 100)
                                if sell_price > self.current_price:
                                    all_placeable_levels.append((level_pct, sell_price))
                            
                            num_placeable = len(all_placeable_levels)
                            # Prefer sell_order_distribution; fall back to legacy order_size_distribution.
                            use_weighted = self.sell_order_distribution == "weighted"
                            placed_amount = 0.0
                            for order_idx, (level_pct, sell_price) in enumerate(all_placeable_levels):
                                if use_weighted:
                                    order_size = self.calculate_order_size(
                                        order_idx, num_placeable, total_for_all_orders, is_sell_order=True
                                    )
                                else:
                                    order_size = total_for_all_orders / num_placeable if num_placeable else 0
                                
                                remaining = total_for_all_orders - placed_amount
                                order_size = min(order_size, remaining)
                                
                                if order_size < 0.0001:
                                    continue
                                
                                try:
                                    rounded_amount = round(order_size, 8)
                                    rounded_rate = round(sell_price, 8)
                                    
                                    response = self.api.place_sell_order(
                                        cointype=self.cointype,
                                        amount=rounded_amount,
                                        rate=rounded_rate,
                                        market=self.market
                                    )
                                    
                                    if response.get('status') == 'ok':
                                        order_id = response.get('id', 'unknown')
                                        self._info(f"{self.symbol}: Placed sell order at {rounded_rate:.4f} "
                                                   f"({level_pct}% above entry), size: {rounded_amount:.8f}")
                                        placed_amount += rounded_amount
                                        if not hasattr(self, 'previous_order_ids'):
                                            self.previous_order_ids = set()
                                        self.previous_order_ids.add(order_id)
                                    else:
                                        error_msg = response.get('message', 'Unknown error')
                                        logging.error(f"{self.symbol}: Failed to place sell order at {level_pct}%: {error_msg}")
                                except Exception as e:
                                    logging.error(f"{self.symbol}: Exception placing sell order at {level_pct}%: {e}")
                            
                            self._last_avg_entry = avg_entry
                            self._last_core_coins = core_coins
                            return
                        else:
                            # Truly stable - no newly placeable levels, no significant uncommitted balance
                            logging.debug(f"{self.symbol}: Core sell ladder stable - {len(existing_sell_orders)} orders, balance unchanged")
                            self._last_avg_entry = avg_entry
                            self._last_core_coins = core_coins
                            return
            else:
                # First run or no previous tracking (restart)
                # Check for over-commitment first: Core orders using more coins than Core allocation allows
                # This happens when working_position_pct is adjusted in config between restarts
                # If available_coins is negative, Core is definitively over-committed
                if available_coins < 0 and existing_sell_amount > 0:
                    over_commit_pct = ((existing_sell_amount - core_coins) / core_coins) * 100
                    self._info(f"{self.symbol}: Core sell orders OVER-COMMITTED (restart path) - "
                               f"committed {existing_sell_amount:.8f} exceeds Core allocation {core_coins:.8f} "
                               f"by {over_commit_pct:.1f}%. Resizing to release coins for Working orders.")
                    self._resize_sell_orders(existing_sell_orders, core_coins, avg_entry)
                    self._last_avg_entry = avg_entry
                    self._last_core_coins = core_coins
                    return
                # Only treat as initialization if we have uncommitted balance to use
                # If all coins are already committed, keep existing orders (they were placed before restart)
                elif available_coins > existing_sell_amount * self.sell_ladder_rebalance_threshold:
                    balance_increased = True
                    self._info(f"{self.symbol}: First Core sell ladder check or restart - {core_coins:.8f} coins, "
                               f"{available_coins:.8f} uncommitted. Will add orders to use available coins.")
                else:
                    # All coins already committed to existing orders
                    # BUT check if any levels have become newly placeable
                    
                    # Find occupied levels
                    occupied_levels = set()
                    for order in existing_sell_orders:
                        for level_idx, level_pct in enumerate(self.sell_levels[:needed_orders]):
                            expected_price = avg_entry * (1 + level_pct / 100)
                            price_diff_pct = abs((order.rate - expected_price) / expected_price) * 100
                            if price_diff_pct < 0.5:
                                occupied_levels.add(level_idx)
                                break
                    
                    # Find placeable levels that are missing orders
                    newly_placeable_levels = []
                    for level_idx, level_pct in enumerate(self.sell_levels[:needed_orders]):
                        sell_price = avg_entry * (1 + level_pct / 100)
                        if sell_price > self.current_price and level_idx not in occupied_levels:
                            newly_placeable_levels.append((level_idx, level_pct, sell_price))
                    
                    if newly_placeable_levels:
                        # There are placeable levels without orders - need to redistribute!
                        # Since all coins are committed to existing orders, we need to RESIZE
                        self._info(f"{self.symbol}: First run - {len(newly_placeable_levels)} placeable level(s) missing orders. "
                                   f"Resizing all orders to redistribute across {len(existing_sell_orders) + len(newly_placeable_levels)} levels.")
                        
                        # Cancel existing orders and rebuild with all placeable levels
                        for order in existing_sell_orders:
                            try:
                                self.api.cancel_order(order.order_id, order_type="sell")
                                logging.debug(f"{self.symbol}: Cancelled sell order {order.order_id} for redistribution")
                            except Exception as e:
                                logging.warning(f"{self.symbol}: Failed to cancel order {order.order_id}: {e}")
                        
                        if existing_sell_orders:
                            time.sleep(1.0)
                        
                        # Place orders at ALL placeable levels with proper distribution
                        total_for_all_orders = core_coins * 0.98
                        all_placeable_levels = []
                        for level_idx, level_pct in enumerate(self.sell_levels[:needed_orders]):
                            sell_price = avg_entry * (1 + level_pct / 100)
                            if sell_price > self.current_price:
                                all_placeable_levels.append((level_pct, sell_price))
                        
                        # Use actual placeable count for proper weight normalization
                        num_placeable = len(all_placeable_levels)
                        placed_amount = 0
                        for order_idx, (level_pct, sell_price) in enumerate(all_placeable_levels):
                            if self.sell_order_distribution == "weighted":
                                # Use order_idx (0-based within placeable levels) for proper weight distribution
                                order_size = self.calculate_order_size(order_idx, num_placeable, total_for_all_orders, is_sell_order=True)
                            else:
                                order_size = total_for_all_orders / num_placeable
                            
                            # Ensure we don't exceed remaining coins
                            remaining = total_for_all_orders - placed_amount
                            order_size = min(order_size, remaining)
                            
                            if order_size < 0.0001:
                                continue
                            
                            try:
                                rounded_amount = round(order_size, 8)
                                rounded_rate = round(sell_price, 8)
                                
                                response = self.api.place_sell_order(
                                    cointype=self.cointype,
                                    amount=rounded_amount,
                                    rate=rounded_rate,
                                    market=self.market
                                )
                                
                                if response.get('status') == 'ok':
                                    order_id = response.get('id', 'unknown')
                                    self._info(f"{self.symbol}: Placed sell order at {rounded_rate:.4f} "
                                               f"({level_pct}% above entry), size: {rounded_amount:.8f}")
                                    placed_amount += rounded_amount
                                    # Track order ID for fill detection
                                    if not hasattr(self, 'previous_order_ids'):
                                        self.previous_order_ids = set()
                                    self.previous_order_ids.add(order_id)
                                else:
                                    error_msg = response.get('message', 'Unknown error')
                                    logging.error(f"{self.symbol}: Failed to place sell order at {level_pct}%: {error_msg}")
                            except Exception as e:
                                logging.error(f"{self.symbol}: Exception placing sell order at {level_pct}%: {e}")
                        
                        self._last_avg_entry = avg_entry
                        self._last_core_coins = core_coins
                        return
                    else:
                        # No newly placeable levels - keep existing orders
                        self._info(f"{self.symbol}: First Core sell ladder check or restart - {core_coins:.8f} coins, "
                                   f"all committed to {len(existing_sell_orders)} orders. Keeping existing orders.")
                        # Only update prices if avg_entry changed, don't add orders
                        old_avg_entry = getattr(self, '_last_avg_entry', 0)
                        if old_avg_entry > 0 and abs(avg_entry - old_avg_entry) / old_avg_entry > 0.01:
                            self._update_sell_orders(existing_sell_orders, avg_entry)
                        self._last_avg_entry = avg_entry
                        self._last_core_coins = core_coins
                        return
        
        # Only proceed to place new orders if balance increased
        if not balance_increased:
            logging.debug(f"{self.symbol}: Core balance unchanged - keeping existing {len(existing_sell_orders)} orders, not adding new ones")
            self._last_avg_entry = avg_entry
            self._last_core_coins = core_coins
            return
        
        # Recalculate occupied levels after potential re-fetch (orders may have changed)
        occupied_levels = set()
        for order in existing_sell_orders:
            # Find which level this order matches
            for level_idx, level_pct in enumerate(self.sell_levels[:needed_orders]):
                expected_price = avg_entry * (1 + level_pct / 100)
                price_diff_pct = abs((order.rate - expected_price) / expected_price) * 100
                if price_diff_pct < 0.5:  # Same tolerance as validation
                    occupied_levels.add(level_idx)
                    break
        
        # Calculate sell amounts
        # Account for existing sell orders that are already using some of the balance
        # Recalculate existing_sell_amount after potential re-fetch
        existing_sell_amount = sum(order.amount for order in existing_sell_orders)
        available_coins = max(0, core_coins - existing_sell_amount)
        
        # Place missing sell orders (only because balance increased)
        orders_to_place = needed_orders - len(existing_sell_orders)
        
        if orders_to_place <= 0:
            logging.debug(f"{self.symbol}: Core sell ladder complete ({len(existing_sell_orders)} orders), no new orders needed")
            self._last_avg_entry = avg_entry
            self._last_core_coins = core_coins
            return
        
        # Identify missing levels (levels that should have orders but don't)
        # Start from level 0 (lowest profit) and find which levels are missing
        missing_level_indices = []
        for level_idx in range(needed_orders):
            if level_idx not in occupied_levels:
                missing_level_indices.append(level_idx)
        
        # Sort missing levels to place orders starting from lowest profit level first
        missing_level_indices.sort()
        
        if len(missing_level_indices) != orders_to_place:
            logging.warning(f"{self.symbol}: Level mismatch - found {len(missing_level_indices)} missing levels but need to place {orders_to_place} orders. "
                          f"Occupied levels: {sorted(occupied_levels)}, Missing: {missing_level_indices}")
        
        # CRITICAL: Calculate what each missing order SHOULD be based on full balance distribution
        # This ensures we maintain proper weighted distribution and don't let upper levels diminish
        # Use 98% of total Core balance for all orders (2% buffer)
        total_for_all_orders = core_coins * 0.98
        
        # Check if we can place missing orders at their proper sizes
        # If not, trigger a resize to rebalance everything properly
        should_resize = False
        total_required_for_missing = 0.0
        
        for level_index in missing_level_indices:
            if level_index >= len(self.sell_levels):
                continue
            
            # Calculate what this order SHOULD be based on full balance and weighted distribution
            if self.sell_order_distribution == "weighted":
                required_size = self.calculate_order_size(level_index, needed_orders, total_for_all_orders, is_sell_order=True)
            else:
                required_size = total_for_all_orders / needed_orders
            
            total_required_for_missing += required_size
        
        # If we have significant uncommitted balance (>30% of committed) AND balance increased, resize to rebalance
        # This ensures proper weighted distribution when balance increased significantly
        # Don't resize if balance decreased (sell filled) - keep existing orders
        if existing_sell_amount > 0 and available_coins > existing_sell_amount * self.sell_ladder_rebalance_threshold:
            # Only resize if balance actually increased (new buys), not if it decreased (sell filled)
            if old_core_coins == 0 or core_coins > old_core_coins:
                unused_pct = (available_coins / core_coins) * 100 if core_coins > 0 else 0
                self._info(f"{self.symbol}: Core balance increased - significant uncommitted balance ({unused_pct:.1f}%). "
                           f"Rebalancing all Core orders to maintain proper weighted distribution.")
                # Resize to redistribute tokens properly across all levels
                self._resize_sell_orders(existing_sell_orders, core_coins, avg_entry)
                # Update tracking variables
                self._last_avg_entry = avg_entry
                self._last_core_coins = core_coins
                return
            else:
                # Balance decreased (sell filled) - don't resize, keep existing orders
                logging.debug(f"{self.symbol}: Significant uncommitted balance, but balance decreased (sell filled). "
                            f"Keeping existing orders, not resizing.")
        
        # We have enough available coins to place missing orders at proper sizes
        # Use available coins (98% of available to leave buffer)
        available_for_orders = available_coins * 0.98
        
        if available_for_orders < 0.0001:
            logging.warning(f"{self.symbol}: Insufficient Core coins available for new sell orders "
                          f"(total: {core_coins:.8f}, committed: {existing_sell_amount:.8f}, "
                          f"available: {available_coins:.8f}). All Core coins are already committed to existing orders.")
            self._info(f"{self.symbol}: Skipping Core sell order placement - waiting for existing orders to fill or cancel")
            return
        
        # Calculate per-order amount for missing orders (equal distribution fallback)
        amount_per_sell_order = available_for_orders / orders_to_place if orders_to_place > 0 else 0
        
        # Safety check: ensure we're not trying to sell more than available
        if available_for_orders > available_coins:
            logging.warning(f"{self.symbol}: Calculation error - available ({available_for_orders:.8f}) exceeds available coins ({available_coins:.8f})")
            available_for_orders = available_coins * 0.98
        
        # Calculate order sizes upfront, but track remaining balance as we place orders
        remaining_coins = available_for_orders
        
        # Collect orders for consolidated notification
        placed_orders = []
        
        # FIRST PASS: Identify which levels are actually placeable (sell_price > current_price)
        # This is critical for correct weight distribution - we should normalize across
        # only the levels that will actually get orders, not all levels
        placeable_levels = []
        skipped_levels = []
        for level_index in missing_level_indices:
            if level_index >= len(self.sell_levels):
                continue
            sell_level_pct = self.sell_levels[level_index]
            sell_price = avg_entry * (1 + sell_level_pct / 100)
            
            if self.current_price > 0 and self.current_price >= sell_price:
                skipped_levels.append((level_index, sell_level_pct, sell_price))
            else:
                placeable_levels.append((level_index, sell_level_pct, sell_price))
        
        # Log skipped levels (current price is at or above sell price)
        for level_index, sell_level_pct, sell_price in skipped_levels:
            self._info(f"{self.symbol}: Skipping order at level {level_index} ({sell_level_pct}% = ${sell_price:.4f}) - "
                       f"current price (${self.current_price:.4f}) is at or above this level. "
                       f"Will place when price drops below ${sell_price:.4f}")
        
        # Calculate number of placeable levels for correct weight normalization
        num_placeable = len(placeable_levels)
        if num_placeable == 0:
            self._info(f"{self.symbol}: No placeable Core sell levels - current price (${self.current_price:.4f}) is above all sell levels")
            self._last_avg_entry = avg_entry
            self._last_core_coins = core_coins
            return
        
        # SECOND PASS: Place orders at placeable levels with correct weight distribution
        for order_idx, (level_index, sell_level_pct, sell_price) in enumerate(placeable_levels):
            # Check if we have enough coins left for this order
            if remaining_coins < 0.0001:
                logging.warning(f"{self.symbol}: Insufficient coins remaining for more sell orders "
                              f"(remaining: {remaining_coins:.8f})")
                break
            
            # Calculate order size using distribution strategy (inverse-weighted for sells)
            # CRITICAL: Use order_idx (0-based within placeable levels) and num_placeable
            # This ensures proper weight distribution across only the levels being placed
            if self.order_size_distribution == "weighted":
                # Use order_idx (0, 1, 2...) for weight calculation, normalized to num_placeable
                order_size = self.calculate_order_size(order_idx, num_placeable, total_for_all_orders, is_sell_order=True)
            else:
                # Equal distribution across placeable orders
                order_size = total_for_all_orders / num_placeable
            
            # But don't exceed remaining coins available for this order
            if order_size > remaining_coins:
                order_size = remaining_coins
            
            # Ensure we have enough coins
            if order_size < 0.0001:
                continue
            
            # Calculate expected proceeds after sell fee
            gross_proceeds = order_size * sell_price
            net_proceeds = gross_proceeds * (1 - self.sell_fee)
            profit_pct = ((sell_price - avg_entry) / avg_entry) * 100
            net_profit_pct = profit_pct - (self.total_fee * 100)
            
            # Log profitability check
            if net_profit_pct < 0:
                logging.warning(
                    f"{self.symbol}: Sell order at {sell_price:.4f} ({profit_pct:.2f}% profit) "
                    f"will result in net loss after fees ({net_profit_pct:.2f}%). "
                    f"Consider increasing sell levels above {self.total_fee * 100:.1f}%."
                )
            else:
                logging.debug(
                    f"{self.symbol}: Sell order: {order_size:.8f} {self.cointype} at {sell_price:.4f} "
                    f"(gross: {gross_proceeds:.2f} {self.market}, net after {self.sell_fee*100}% fee: {net_proceeds:.2f} {self.market}, "
                    f"net profit: {net_profit_pct:.2f}%)"
                )
            
            try:
                # Note: For sell orders, 'amount' is the coin quantity to sell
                # Exchange calculates quote-currency proceeds from rate and amount
                # Round to 8 decimal places (API max precision)
                rounded_amount = round(order_size, 8)
                rounded_rate = round(sell_price, 8)
                
                # Validate amounts before placing order
                if rounded_amount <= 0:
                    logging.warning(f"{self.symbol}: Skipping sell order - invalid amount: {rounded_amount}")
                    continue
                if rounded_rate <= 0:
                    logging.warning(f"{self.symbol}: Skipping sell order - invalid rate: {rounded_rate}")
                    continue
                # Check against remaining coins, not total
                if rounded_amount > remaining_coins:
                    logging.warning(f"{self.symbol}: Skipping sell order - amount ({rounded_amount}) exceeds remaining ({remaining_coins:.8f})")
                    break  # Stop trying to place more orders
                
                response = self.api.place_sell_order(
                    cointype=self.cointype,
                    amount=rounded_amount,  # Coin quantity to sell (rounded to 8 decimals)
                    rate=rounded_rate,  # Rate rounded to 8 decimals
                    market=self.market
                )
                
                if response.get('status') == 'ok':
                    order_id = response.get('id', 'unknown')
                    self._info(f"{self.symbol}: Placed Core sell order at {sell_price:.2f} "
                               f"({sell_level_pct}% above entry), size: {rounded_amount:.8f}")
                    # Update remaining coins after successful placement (use rounded_amount that was actually placed)
                    remaining_coins -= rounded_amount
                    # Track order ID for fill detection
                    if not hasattr(self, 'previous_order_ids'):
                        self.previous_order_ids = set()
                    self.previous_order_ids.add(order_id)
                    # Collect order for consolidated notification
                    placed_orders.append({
                        'amount': rounded_amount,
                        'rate': rounded_rate,
                        'order_id': order_id,
                        'level_pct': sell_level_pct
                    })
                else:
                    error_msg = response.get('message', 'Unknown error')
                    logging.error(f"{self.symbol}: Failed to place Core sell order: {error_msg}")
                    logging.error(f"{self.symbol}: Order details - amount: {rounded_amount}, rate: {rounded_rate}, core_coins: {core_coins}")
                    self.notifier.notify_error(self.symbol, f"Sell order failed: {error_msg}")
            except Exception as e:
                error_str = str(e)
                logging.error(f"{self.symbol}: Exception placing Core sell order: {e}")
                logging.error(f"{self.symbol}: Order details - amount: {order_size:.8f}, rate: {sell_price:.8f}, core_coins: {core_coins:.8f}")
                
                # Handle "Insufficient funds" - likely a fill occurred mid-cycle causing stale balance
                # Stop placing more orders this cycle; next cycle will have fresh balance
                if 'Insufficient funds' in error_str:
                    logging.warning(f"{self.symbol}: Insufficient funds detected - likely a sell filled mid-cycle. "
                                  f"Stopping order placement, will retry next cycle with fresh balance.")
                    break
                
                # Only notify on first error to avoid spam
                if order_idx == 0:
                    self.notifier.notify_error(self.symbol, f"Sell order exception: {error_str[:100]}")
        
        # Send notification if any orders were placed (immediate notification)
        if placed_orders:
            self.notifier.notify_sell_ladder_recalculated(
                symbol=self.symbol,
                orders=placed_orders,
                avg_entry=avg_entry,
                current_price=self.current_price,
                order_type="Core"
            )
        
        # Update tracking variables at the end of the method
        # This ensures we track the current state for future comparisons
        self._last_avg_entry = avg_entry
        self._last_core_coins = core_coins
        
        # Track price when orders were placed (for slow price drop detection)
        if self.current_price > 0:
            self.price_when_orders_placed = self.current_price
    
    def _cancel_working_orders(self, working_orders: List) -> None:
        """Cancel open Working sell orders and remove them from tracking."""
        for order in working_orders:
            try:
                self.api.cancel_order(order.order_id, order_type="sell")
                self._working_order_ids.discard(order.order_id)
            except Exception as e:
                logging.warning(f"{self.symbol}: Failed to cancel Working order {order.order_id}: {e}")
        if working_orders:
            self._save_working_order_ids()

    def _working_ladder_needs_level_recalc(self, working_orders: List) -> bool:
        """True when existing Working orders don't match the configured levels
        relative to the price at which they were PLACED.

        IMPORTANT: This anchors to the placement price (_last_working_price), NOT
        the current price. Price-driven repositioning is governed separately by
        working_price_update_threshold. Anchoring this check to current_price would
        force a full cancel/rebuild on every small tick (e.g. on each buy) and
        defeat that threshold — which is exactly the over-recreation bug this fixes.
        It still catches a genuinely malformed ladder (orders at the wrong levels).
        """
        if not working_orders or not self.working_sell_levels:
            return False

        anchor_price = getattr(self, '_last_working_price', 0.0)
        if anchor_price <= 0:
            # No recorded placement price (e.g. after restart): infer it from the
            # lowest rung so we still validate relative spacing without
            # false-firing on normal price drift.
            lowest_rate = min(order.rate for order in working_orders)
            anchor_price = lowest_rate / (1 + self.working_sell_levels[0] / 100.0)
        if anchor_price <= 0:
            return False

        tolerance_pct = 0.15
        levels_to_check = self.working_sell_levels[:len(working_orders)]
        expected_rates = [
            anchor_price * (1 + level_pct / 100.0)
            for level_pct in levels_to_check
        ]
        matched = set()
        for order in sorted(working_orders, key=lambda o: o.rate):
            best_idx = None
            best_diff = float('inf')
            for idx, expected_rate in enumerate(expected_rates):
                if idx in matched:
                    continue
                diff_pct = abs(order.rate - expected_rate) / expected_rate * 100
                if diff_pct < best_diff:
                    best_diff = diff_pct
                    best_idx = idx
            if best_idx is None or best_diff > tolerance_pct:
                return True
            matched.add(best_idx)
        return False

    def _place_working_sell_ladder(self, working_coins: float, avg_entry: float):
        """Place Working sell ladder (mean-reversion strategy - relative to current price)
        
        Working orders are tracked by order_id (not price matching) because:
        - Working orders are placed relative to current_price
        - After price moves, price-matching fails
        - Order IDs provide reliable identification
        
        IMPORTANT: Working orders should only be active when price is BELOW average entry.
        When price is above entry, Core orders handle selling (recovery strategy).
        This prevents Working orders from depleting the position during uptrends.
        """
        if self.current_price <= 0:
            logging.warning(f"{self.symbol}: Cannot place Working ladder - no current price available")
            return
        
        if working_coins < 0.0001:
            logging.debug(f"{self.symbol}: Working coins too small ({working_coins:.8f})")
            return
        
        min_working = self._min_working_coins_threshold()
        if working_coins < min_working:
            self._info(
                f"{self.symbol}: Working slice too small ({working_coins:.8f} "
                f"< {min_working:.8f}), skipping Working ladder"
            )
            return
        
        # Check if price is within threshold of average entry - if so, cancel existing Working orders
        # This preserves coins for Core orders which have better profit margins during recovery
        avg_entry = self.average_entry_price
        if avg_entry > 0:
            # Calculate threshold below entry (e.g., 5% means cancel when price >= 95% of entry)
            threshold_multiplier = 1.0 - (self.cancel_working_threshold_pct / 100.0)
            entry_threshold = avg_entry * threshold_multiplier
            if self.current_price >= entry_threshold:
                pct_below_entry = ((avg_entry - self.current_price) / avg_entry * 100) if avg_entry > 0 else 0
                if self.current_price >= avg_entry:
                    self._info(f"{self.symbol}: Price (${self.current_price:.4f}) >= avg entry (${avg_entry:.4f}) - "
                                f"cancelling Working orders (Core handles selling when profitable)")
                else:
                    self._info(f"{self.symbol}: Price (${self.current_price:.4f}) within {self.cancel_working_threshold_pct:.1f}% of avg entry (${avg_entry:.4f}, "
                                f"{pct_below_entry:.2f}% below) - cancelling Working orders to preserve coins for Core orders")
                
                # Cancel all existing Working orders
                existing_sell_orders = self.get_sell_orders()
                if not hasattr(self, '_working_order_ids'):
                    self._working_order_ids = set()
                working_orders = [order for order in existing_sell_orders if order.order_id in self._working_order_ids]
                
                for order in working_orders:
                    try:
                        self.api.cancel_order(order.order_id, order_type="sell")
                        self._working_order_ids.discard(order.order_id)
                        self._info(f"{self.symbol}: Cancelled Working order {order.order_id} (price within {self.cancel_working_threshold_pct:.1f}% of entry)")
                    except Exception as e:
                        logging.warning(f"{self.symbol}: Failed to cancel Working order {order.order_id}: {e}")
                
                if working_orders:
                    self._save_working_order_ids()
                return  # Don't place new Working orders when price is within threshold of entry
        
        # Ensure tracking set is initialized
        if not hasattr(self, '_working_order_ids'):
            self._working_order_ids = set()
        
        # Ensure open_orders is initialized
        if not hasattr(self, 'open_orders') or self.open_orders is None:
            self.open_orders = []
        
        existing_sell_orders = self.get_sell_orders()
        
        # Filter to only Working orders using tracked order IDs (reliable after price moves)
        # Clean up tracked IDs that no longer exist (orders filled or cancelled)
        current_order_ids = {order.order_id for order in existing_sell_orders}
        old_working_count = len(self._working_order_ids)
        self._working_order_ids = self._working_order_ids.intersection(current_order_ids)
        
        # Save if any Working orders were removed (filled or cancelled externally)
        if len(self._working_order_ids) < old_working_count:
            removed_count = old_working_count - len(self._working_order_ids)
            self._info(f"{self.symbol}: {removed_count} Working order(s) filled/cancelled, updating tracking")
            self._save_working_order_ids()
        
        working_orders = [order for order in existing_sell_orders if order.order_id in self._working_order_ids]
        
        # Determine Working order limit (independent or split from max_sell_orders)
        if self.max_working_sell_orders is not None:
            # Use explicit Working limit if configured
            max_working_orders = min(self.max_working_sell_orders, len(self.working_sell_levels))
        else:
            # Fallback: Split max_sell_orders (legacy behavior)
            max_working_orders = min(len(self.working_sell_levels), max(3, self.max_sell_orders // 3))
        needed_orders = max_working_orders
        
        existing_sell_amount = sum(order.amount for order in working_orders)
        available_coins = max(0, working_coins - existing_sell_amount)
        
        # Calculate price change from when orders were placed (for logging)
        old_price = getattr(self, '_last_working_price', 0.0)
        
        # If _last_working_price is not set (0.0), infer it from existing orders
        # The lowest working order should be at the first working_sell_level above the price when placed
        if old_price == 0.0:
            if working_orders and self.working_sell_levels:
                # Find the lowest priced working order
                lowest_order = min(working_orders, key=lambda o: o.rate)
                # Infer the price when orders were placed: lowest_order.rate / (1 + first_level/100)
                first_level = self.working_sell_levels[0]
                inferred_price = lowest_order.rate / (1 + first_level / 100.0)
                old_price = inferred_price
                # Save inferred price so it persists for future checks
                self._last_working_price = inferred_price
                self._info(f"{self.symbol}: Inferred last working price from orders: ${inferred_price:.4f} "
                           f"(lowest order at ${lowest_order.rate:.4f}, first level {first_level}%)")
            else:
                # No orders to infer from, use current price (first run)
                old_price = self.current_price
        
        # Calculate price status for logging (similar to buy ladder)
        working_price_threshold = self.working_price_update_threshold
        if old_price > 0 and self.current_price > 0:
            change_pct = abs((self.current_price - old_price) / old_price) * 100
            remaining_pct = max(0, working_price_threshold - change_pct)
            direction = "↑" if self.current_price > old_price else "↓"
            price_status = f"  price: {direction}{change_pct:.2f}% from placement (${old_price:.4f} → ${self.current_price:.4f}), need {remaining_pct:.2f}% more to recalc (threshold: {working_price_threshold}%)"
        else:
            price_status = f"  price: tracking from ${self.current_price:.4f}"
        
        self._info(f"{self.symbol}: Working Sell Ladder Check\n"
                    f"  existing: {len(working_orders)}\n"
                    f"  max_orders: {needed_orders}\n"
                    f"  current_price: {self.current_price:.4f}\n"
                    f"  working_coins: {working_coins:.8f}\n"
                    f"  committed: {existing_sell_amount:.8f}\n"
                    f"  available: {available_coins:.8f}\n"
                    f"{price_status}")
        
        fills_occurred = len(self._working_order_ids) < old_working_count
        level_drift = self._working_ladder_needs_level_recalc(working_orders) if working_orders else False
        price_change_pct = abs(self.current_price - old_price) / old_price * 100 if old_price > 0 else 0
        price_moved = price_change_pct > working_price_threshold

        needs_recalc = bool(working_orders) and (
            price_moved or level_drift or fills_occurred or len(working_orders) != needed_orders
        )

        if needs_recalc:
            if price_moved:
                reason = f"price moved {price_change_pct:.1f}% ({old_price:.4f} -> {self.current_price:.4f})"
            elif level_drift:
                lowest = min(o.rate for o in working_orders)
                expected = self.current_price * (1 + self.working_sell_levels[0] / 100.0)
                reason = (
                    f"orders drifted from configured levels "
                    f"(lowest ${lowest:.4f} vs expected ${expected:.4f} at +{self.working_sell_levels[0]}%)"
                )
            elif fills_occurred:
                reason = "Working order(s) filled"
            else:
                reason = f"order count mismatch ({len(working_orders)} vs {needed_orders} needed)"
            self._info(f"{self.symbol}: Recalculating Working ladder - {reason}")
            self._cancel_working_orders(working_orders)
            time.sleep(0.5)
            working_orders = []
            existing_sell_amount = 0
            available_coins = working_coins
        elif len(working_orders) >= needed_orders:
            old_working_coins = getattr(self, '_last_working_coins', 0)
            needs_resize = (
                old_working_coins > 0
                and working_coins > old_working_coins
                and existing_sell_amount > 0
                and available_coins > existing_sell_amount * self.sell_ladder_rebalance_threshold
            )
            if needs_resize:
                unused_pct = (available_coins / working_coins) * 100 if working_coins > 0 else 0
                self._info(f"{self.symbol}: Available Working coins ({available_coins:.8f}) significantly exceed committed ({existing_sell_amount:.8f}), "
                               f"{unused_pct:.1f}% unused. Resizing Working ladder to use full balance.")
                self._cancel_working_orders(working_orders)
                time.sleep(0.5)
                working_orders = []
                existing_sell_amount = 0
                available_coins = working_coins
            else:
                self._last_working_coins = working_coins
                return

        if len(working_orders) > 0:
            self._last_working_coins = working_coins
            return

        levels_to_use = self.working_sell_levels[:needed_orders]
        if not levels_to_use:
            return

        available_for_orders = working_coins * 0.98
        if available_for_orders < 0.0001:
            logging.debug(f"{self.symbol}: No available Working coins for new orders")
            return

        order_size_each = available_for_orders / len(levels_to_use)
        remaining_coins = available_for_orders
        placed_orders = []

        for level_pct in levels_to_use:
            sell_price = self.current_price * (1 + level_pct / 100)

            if sell_price <= self.current_price:
                continue

            order_size = min(order_size_each, remaining_coins)
            if order_size < 0.0001:
                continue

            try:
                rounded_amount = round(order_size, 8)
                rounded_rate = round(sell_price, 8)

                response = self.api.place_sell_order(
                    cointype=self.cointype,
                    amount=rounded_amount,
                    rate=rounded_rate,
                    market=self.market
                )

                if response.get('status') == 'ok':
                    order_id = response.get('id', 'unknown')
                    self._working_order_ids.add(order_id)
                    self._info(f"{self.symbol}: Placed Working sell order at {rounded_rate:.4f} "
                               f"({level_pct}% above current price), size: {rounded_amount:.8f}")
                    remaining_coins -= rounded_amount
                    placed_orders.append({
                        'amount': rounded_amount,
                        'rate': rounded_rate,
                        'order_id': order_id,
                        'level_pct': level_pct
                    })
                else:
                    error_msg = response.get('message', 'Unknown error')
                    logging.error(f"{self.symbol}: Failed to place Working sell order: {error_msg}")
            except Exception as e:
                logging.error(f"{self.symbol}: Exception placing Working sell order: {e}")

        self._last_working_price = self.current_price
        self._last_working_coins = working_coins

        if placed_orders:
            self._save_working_order_ids()
            self._info(f"{self.symbol}: Placed {len(placed_orders)} Working sell order(s)")
            self.notifier.notify_sell_ladder_recalculated(
                symbol=self.symbol,
                orders=placed_orders,
                avg_entry=None,
                current_price=self.current_price,
                order_type="Working"
            )
    
    def _update_buy_orders(self, existing_orders: List[Order]):
        """Update existing buy orders to match new price levels
        
        Deep levels (15%+) only reposition on DOWN moves to allow them to fill on retracements.
        Shallow levels (below 15%) reposition on both up and down moves to catch quick dips.
        """
        if len(existing_orders) == 0 or self.current_price == 0:
            return
        
        # Determine if price moved up or down from when orders were placed
        price_moved_up = False
        if self.price_when_orders_placed > 0:
            price_moved_up = self.current_price > self.price_when_orders_placed
        
        # Try to edit orders first, fall back to cancel/recreate if editing fails
        orders_to_cancel = []
        
        for order in existing_orders:
            buy_level_pct = self._nearest_ladder_level_pct(
                order.rate, self.buy_levels, self.current_price, is_buy=True
            )
            if buy_level_pct is None:
                logging.debug(
                    f"{self.symbol}: Skipping buy order {order.order_id} @ {order.rate:.4f} — "
                    f"no matching ladder level"
                )
                continue
            
            # Deep levels (15%+) only reposition on DOWN moves
            # This allows them to stay at lower prices and fill on retracements
            if buy_level_pct >= 15.0 and price_moved_up:
                logging.debug(f"{self.symbol}: Skipping reposition of deep level {buy_level_pct}% (price moved up, keeping at {order.rate:.4f})")
                continue
            
            new_price = self.current_price * (1 - buy_level_pct / 100)
            
            # Only update if price has changed significantly
            price_diff_pct = abs((new_price - order.rate) / order.rate) * 100
            if price_diff_pct < 0.1:  # Less than 0.1% change, skip
                continue
            
            # Use the exact rate from the order object (API's stored rate)
            # Round to 8 decimal places to match API precision
            current_rate = round(order.rate, 8)
            new_rate = round(new_price, 8)
            
            # Try to edit the order
            try:
                response = self.api.edit_order(
                    order_id=order.order_id,
                    cointype=self.cointype,
                    current_rate=current_rate,
                    new_rate=new_rate,
                    order_type="buy"
                )
                if response.get('status') == 'ok':
                    new_id = str(response.get('id') or order.order_id)
                    self._replace_tracked_order_id(order.order_id, new_id, new_rate=new_rate)
                    order.order_id = new_id
                    order.rate = new_rate
                    self._info(f"{self.symbol}: Updated buy order {new_id} to {new_price:.2f}")
                    # Don't notify on order updates - too frequent, only log
                else:
                    # Editing failed, mark for cancellation
                    error_msg = response.get('message', 'Unknown error')
                    logging.debug(f"{self.symbol}: Buy order edit failed: {error_msg}, will cancel/recreate")
                    orders_to_cancel.append(order)
            except Exception as e:
                # Editing not supported or failed, cancel and recreate
                logging.debug(f"{self.symbol}: Order edit failed, will cancel/recreate: {e}")
                orders_to_cancel.append(order)
        
        # Cancel orders that couldn't be edited
        for order in orders_to_cancel:
            try:
                self.api.cancel_order(order.order_id, order_type="buy")
                logging.debug(f"{self.symbol}: Cancelled buy order {order.order_id} for update")
            except Exception as e:
                logging.warning(f"{self.symbol}: Failed to cancel order {order.order_id}: {e}")
        
        # Place new orders to fill the ladder
        if orders_to_cancel:
            time.sleep(0.5)  # Brief pause before placing new orders
            self.place_buy_ladder()
        
        # Track price when orders were updated (for slow price drop detection)
        if self.current_price > 0:
            self.price_when_orders_placed = self.current_price
    
    def _resize_sell_orders(self, existing_orders: List[Order], total_coins: float, avg_entry: float):
        """Resize sell orders to use full balance while preserving existing prices
        
        This cancels existing orders and recreates them at the SAME prices,
        but with updated amounts to use the full balance. This prevents orders
        from moving further away from current price.
        """
        if len(existing_orders) == 0:
            return
        
        # Store existing prices to preserve them, sorted from lowest to highest
        # This ensures correct mapping for inverse-weighted distribution:
        # - Lowest price (index 0) gets largest amount
        # - Highest price (index n-1) gets smallest amount
        existing_prices = sorted([order.rate for order in existing_orders])
        
        # Cancel all existing orders
        for order in existing_orders:
            try:
                self.api.cancel_order(order.order_id, order_type="sell")
                logging.debug(f"{self.symbol}: Cancelled sell order {order.order_id} for resize")
            except Exception as e:
                logging.warning(f"{self.symbol}: Failed to cancel order {order.order_id}: {e}")
        
        # Wait for cancellations to process
        time.sleep(1.5)
        
        # Calculate new amounts: distribute total coins across all orders
        # Use 98% of total coins to leave buffer
        total_for_orders = total_coins * 0.98
        
        # Place orders at same prices but with new amounts
        remaining_coins = total_for_orders
        
        # Collect orders for consolidated notification
        placed_orders = []
        
        for i, price in enumerate(existing_prices):
            if remaining_coins < 0.0001:
                break
            
            # Calculate order size using distribution strategy (inverse-weighted for sells)
            # Index i maps to level: i=0 is lowest price (should get largest amount for inverse-weighted)
            if self.sell_order_distribution == "weighted":
                # Calculate size based on weighted distribution (inverse for sells)
                order_amount = self.calculate_order_size(i, len(existing_prices), total_for_orders, is_sell_order=True)
            else:
                # Equal distribution
                order_amount = total_for_orders / len(existing_prices)
            
            # Don't exceed remaining coins
            order_amount = min(order_amount, remaining_coins)
            
            if order_amount < 0.0001:
                continue
            
            try:
                rounded_amount = round(order_amount, 8)
                rounded_rate = round(price, 8)
                
                response = self.api.place_sell_order(
                    cointype=self.cointype,
                    amount=rounded_amount,
                    rate=rounded_rate,
                    market=self.market
                )
                
                if response.get('status') == 'ok':
                    order_id = response.get('id', 'unknown')
                    self._info(f"{self.symbol}: Resized sell order {i+1}/{len(existing_prices)}: "
                               f"{rounded_amount:.8f} @ {rounded_rate:.4f} {self.market} (preserved price)")
                    remaining_coins -= rounded_amount
                    
                    # Track order ID
                    if not hasattr(self, 'previous_order_ids'):
                        self.previous_order_ids = set()
                    self.previous_order_ids.add(order_id)
                    
                    # Collect order for consolidated notification
                    placed_orders.append({
                        'amount': rounded_amount,
                        'rate': rounded_rate,
                        'order_id': order_id
                    })
                else:
                    error_msg = response.get('message', 'Unknown error')
                    logging.error(f"{self.symbol}: Failed to place resized sell order: {error_msg}")
            except Exception as e:
                logging.error(f"{self.symbol}: Exception placing resized sell order: {e}")
        
        # Send notification if any orders were placed (immediate notification)
        if placed_orders:
            self.notifier.notify_sell_ladder_recalculated(
                symbol=self.symbol,
                orders=placed_orders,
                avg_entry=avg_entry,
                current_price=self.current_price,
                order_type="Core"
            )
        
        self._info(f"{self.symbol}: Resized {len(existing_prices)} sell orders - preserved prices, updated amounts to use full balance")
        
        # Track price when orders were resized (for slow price drop detection)
        if self.current_price > 0:
            self.price_when_orders_placed = self.current_price
    
    def _resize_and_update_sell_orders(self, existing_orders: List[Order], total_coins: float, avg_entry: float):
        """Resize sell orders AND update prices based on new average entry
        
        This cancels existing orders and recreates them with:
        - NEW prices based on new avg_entry (maintains profit percentages)
        - NEW amounts to use the full balance
        """
        if len(existing_orders) == 0 or avg_entry == 0:
            return
        
        # Cancel all existing orders
        for order in existing_orders:
            try:
                self.api.cancel_order(order.order_id, order_type="sell")
                logging.debug(f"{self.symbol}: Cancelled sell order {order.order_id} for resize and update")
            except Exception as e:
                logging.warning(f"{self.symbol}: Failed to cancel order {order.order_id}: {e}")
        
        # Wait for cancellations to process
        time.sleep(1.5)
        
        # Calculate new prices based on new avg_entry and sell_levels
        # Calculate new amounts: distribute total coins across all orders
        total_for_orders = total_coins * 0.98
        
        remaining_coins = total_for_orders
        
        # Collect orders for consolidated notification
        placed_orders = []
        
        for i, sell_level_pct in enumerate(self.sell_levels[:len(existing_orders)]):
            if remaining_coins < 0.0001:
                break
            
            # Calculate NEW price based on new avg_entry
            new_price = avg_entry * (1 + sell_level_pct / 100)
            
            # Calculate order size using distribution strategy (inverse-weighted for sells)
            if self.sell_order_distribution == "weighted":
                # Calculate size based on weighted distribution (inverse for sells)
                order_amount = self.calculate_order_size(i, len(existing_orders), total_for_orders, is_sell_order=True)
            else:
                # Equal distribution
                order_amount = total_for_orders / len(existing_orders)
            
            # Don't exceed remaining coins
            order_amount = min(order_amount, remaining_coins)
            
            if order_amount < 0.0001:
                continue
            
            try:
                rounded_amount = round(order_amount, 8)
                rounded_rate = round(new_price, 8)
                
                response = self.api.place_sell_order(
                    cointype=self.cointype,
                    amount=rounded_amount,
                    rate=rounded_rate,
                    market=self.market
                )
                
                if response.get('status') == 'ok':
                    order_id = response.get('id', 'unknown')
                    old_price = existing_orders[i].rate if i < len(existing_orders) else 0
                    price_change = ((new_price - old_price) / old_price * 100) if old_price > 0 else 0
                    self._info(f"{self.symbol}: Resized and updated sell order {i+1}/{len(existing_orders)}: "
                               f"{rounded_amount:.8f} @ {rounded_rate:.4f} {self.market} "
                               f"(was {old_price:.4f}, {price_change:+.2f}%)")
                    remaining_coins -= rounded_amount
                    
                    # Track order ID
                    if not hasattr(self, 'previous_order_ids'):
                        self.previous_order_ids = set()
                    self.previous_order_ids.add(order_id)
                    
                    # Collect order for consolidated notification
                    placed_orders.append({
                        'amount': rounded_amount,
                        'rate': rounded_rate,
                        'order_id': order_id,
                        'level_pct': sell_level_pct
                    })
                else:
                    error_msg = response.get('message', 'Unknown error')
                    logging.error(f"{self.symbol}: Failed to place resized/updated sell order: {error_msg}")
            except Exception as e:
                logging.error(f"{self.symbol}: Exception placing resized/updated sell order: {e}")
        
        # Send notification if any orders were placed (immediate notification)
        if placed_orders:
            self.notifier.notify_sell_ladder_recalculated(
                symbol=self.symbol,
                orders=placed_orders,
                avg_entry=avg_entry,
                current_price=self.current_price,
                order_type="Core"
            )
        
        self._info(f"{self.symbol}: Resized and updated {len(existing_orders)} sell orders - new prices based on avg_entry {avg_entry:.4f}, updated amounts to use full balance")
        
        # Track price when orders were resized and updated (for slow price drop detection)
        if self.current_price > 0:
            self.price_when_orders_placed = self.current_price
    
    def _update_sell_orders(self, existing_orders: List[Order], avg_entry: float):
        """Update existing sell orders to match new average entry
        
        Strategy:
        - Use conservative threshold (1.0%) to avoid unnecessary updates
        - Preserve higher-level orders (15%+) to capture long-term moves (2X, 3X)
        - Update lower levels more aggressively since they're more likely to fill
        """
        if len(existing_orders) == 0 or avg_entry == 0:
            return
        
        # Try to edit orders first, fall back to cancel/recreate if editing fails
        orders_to_cancel = []
        orders_updated = 0
        orders_preserved = 0
        preserved_details = []
        updated_details = []
        
        self._info(f"{self.symbol}: Evaluating {len(existing_orders)} sell orders for updates "
                    f"(avg_entry: {avg_entry:.4f})")
        
        for order in existing_orders:
            sell_level_pct = self._nearest_ladder_level_pct(
                order.rate, self.sell_levels, avg_entry, is_buy=False
            )
            if sell_level_pct is None:
                logging.debug(
                    f"{self.symbol}: Skipping sell order {order.order_id} @ {order.rate:.4f} — "
                    f"no matching ladder level vs avg_entry {avg_entry:.4f}"
                )
                continue
            new_price = avg_entry * (1 + sell_level_pct / 100)
            
            # Calculate price change percentage
            price_diff_pct = abs((new_price - order.rate) / order.rate) * 100
            
            # Determine threshold based on level
            is_high_level = sell_level_pct >= 15.0
            threshold = 2.0 if is_high_level else 1.0
            
            # Preserve higher-level orders (15%+) to capture long-term moves (2X, 3X)
            # These are for capturing big moves and shouldn't be disrupted by average entry changes
            if is_high_level:
                # Only update high-level orders if price change is very significant (>2%)
                # This preserves orders during normal average entry adjustments
                if price_diff_pct < threshold:
                    orders_preserved += 1
                    preserved_details.append({
                        'level': sell_level_pct,
                        'current_price': order.rate,
                        'new_price': new_price,
                        'diff_pct': price_diff_pct,
                        'reason': f'High-level preserved (diff {price_diff_pct:.2f}% < {threshold}% threshold)'
                    })
                    logging.debug(f"{self.symbol}: Preserving HIGH-LEVEL sell order at {sell_level_pct}% "
                                f"(current: ${order.rate:.4f}, new: ${new_price:.4f}, diff: {price_diff_pct:.2f}%, "
                                f"threshold: {threshold}%)")
                    continue
            
            # For lower levels (2.5-15%), use conservative threshold (1.0%)
            # This avoids unnecessary updates while still maintaining accuracy
            if price_diff_pct < threshold:  # Less than 1.0% change, skip (was 0.1%)
                orders_preserved += 1
                preserved_details.append({
                    'level': sell_level_pct,
                    'current_price': order.rate,
                    'new_price': new_price,
                    'diff_pct': price_diff_pct,
                    'reason': f'Low-level preserved (diff {price_diff_pct:.2f}% < {threshold}% threshold)'
                })
                logging.debug(f"{self.symbol}: Preserving sell order at {sell_level_pct}% "
                            f"(current: ${order.rate:.4f}, new: ${new_price:.4f}, diff: {price_diff_pct:.2f}%, "
                            f"threshold: {threshold}%)")
                continue
            
            # Price change is significant - attempt to update
            self._info(f"{self.symbol}: Updating sell order at {sell_level_pct}% "
                        f"(current: ${order.rate:.4f} -> new: ${new_price:.4f}, "
                        f"diff: {price_diff_pct:.2f}% > {threshold}% threshold)")
            
            # Try to edit the order
            try:
                response = self.api.edit_order(
                    order_id=order.order_id,
                    cointype=self.cointype,
                    current_rate=order.rate,
                    new_rate=new_price,
                    order_type="sell"
                )
                if response.get('status') == 'ok':
                    new_id = str(response.get('id') or order.order_id)
                    old_rate = order.rate
                    self._replace_tracked_order_id(order.order_id, new_id, new_rate=new_price)
                    order.order_id = new_id
                    order.rate = new_price
                    orders_updated += 1
                    updated_details.append({
                        'level': sell_level_pct,
                        'order_id': new_id,
                        'old_price': old_rate,
                        'new_price': new_price,
                        'diff_pct': price_diff_pct
                    })
                    self._info(f"{self.symbol}: Successfully updated sell order {new_id} at {sell_level_pct}% "
                               f"(${old_rate:.4f} -> ${new_price:.4f}, change: {price_diff_pct:+.2f}%)")
                    # Don't notify on order updates - too frequent, only log
                else:
                    # Editing failed, mark for cancellation
                    error_msg = response.get('message', 'Unknown error')
                    logging.warning(f"{self.symbol}: Sell order edit failed at {sell_level_pct}% "
                                  f"(order_id: {order.order_id}, error: {error_msg}), will cancel/recreate")
                    orders_to_cancel.append(order)
            except Exception as e:
                # Editing not supported or failed, cancel and recreate
                logging.warning(f"{self.symbol}: Order edit exception at {sell_level_pct}% "
                              f"(order_id: {order.order_id}): {e}, will cancel/recreate")
                orders_to_cancel.append(order)
        
        # Detailed summary logging
        if orders_updated > 0 or orders_preserved > 0 or orders_to_cancel:
            self._info(f"{self.symbol}: SELL ORDER UPDATE SUMMARY")
            self._info(f"{self.symbol}:   Total orders evaluated: {len(existing_orders)}")
            self._info(f"{self.symbol}:   Updated: {orders_updated}")
            self._info(f"{self.symbol}:   Preserved: {orders_preserved}")
            self._info(f"{self.symbol}:   To cancel/recreate: {len(orders_to_cancel)}")
            
            if updated_details:
                self._info(f"{self.symbol}:   Updated orders details:")
                for detail in updated_details:
                    self._info(f"{self.symbol}:      - Level {detail['level']:.1f}%: "
                               f"Order {detail['order_id']} "
                               f"${detail['old_price']:.4f} -> ${detail['new_price']:.4f} "
                               f"({detail['diff_pct']:+.2f}%)")
            
            if preserved_details:
                high_level_preserved = [d for d in preserved_details if d['level'] >= 15.0]
                low_level_preserved = [d for d in preserved_details if d['level'] < 15.0]
                
                if high_level_preserved:
                    self._info(f"{self.symbol}:   High-level orders preserved (15%+): {len(high_level_preserved)}")
                    for detail in high_level_preserved[:3]:  # Show first 3
                        self._info(f"{self.symbol}:      - Level {detail['level']:.1f}%: "
                                   f"${detail['current_price']:.4f} (diff: {detail['diff_pct']:.2f}%)")
                    if len(high_level_preserved) > 3:
                        self._info(f"{self.symbol}:      ... and {len(high_level_preserved) - 3} more")
                
                if low_level_preserved:
                    logging.debug(f"{self.symbol}:   Low-level orders preserved (<15%): {len(low_level_preserved)}")
                    # Only log details in debug mode for low levels (too verbose otherwise)
                    for detail in low_level_preserved[:2]:  # Show first 2 in debug
                        logging.debug(f"{self.symbol}:      - Level {detail['level']:.1f}%: "
                                    f"${detail['current_price']:.4f} (diff: {detail['diff_pct']:.2f}%)")
            
            if orders_to_cancel:
                self._info(f"{self.symbol}:   Orders to cancel/recreate: {len(orders_to_cancel)}")
                for order in orders_to_cancel[:3]:  # Show first 3
                    self._info(f"{self.symbol}:      - Order {order.order_id} at ${order.rate:.4f}")
        
        # Cancel orders that couldn't be edited
        for order in orders_to_cancel:
            try:
                self.api.cancel_order(order.order_id, order_type="sell")
                logging.debug(f"{self.symbol}: Cancelled sell order {order.order_id} for update")
            except Exception as e:
                logging.warning(f"{self.symbol}: Failed to cancel order {order.order_id}: {e}")
        
        # Place new orders to fill the ladder
        if orders_to_cancel:
            time.sleep(0.5)  # Brief pause before placing new orders
            self.place_sell_ladder()
        
        # Track price when orders were updated (for slow price drop detection)
        if self.current_price > 0:
            self.price_when_orders_placed = self.current_price
    
    def process(self):
        """Main processing loop for this symbol"""
        try:
            current_time = time.time()
            
            # IMPROVED: Always run balance reconciliation check
            if self.coin_balance > 0:
                self._validate_orders_vs_balance()
            
            # Detect and sync missing fills on first run (startup check)
            if not self._startup_sync_done and self.coin_balance > 0:
                self._detect_and_sync_missing_fills()
                # Check for duplicate orders after syncing
                self._detect_duplicate_orders()
                self._startup_sync_done = True
                self._last_sync_time = current_time
            # Also sync periodically during normal operation (every 10 minutes)
            elif self.coin_balance > 0 and (current_time - self._last_sync_time) >= self._sync_interval:
                self._info(f"{self.symbol}: 🔍 Periodic sync check (last sync was {int(current_time - self._last_sync_time)}s ago)")
                self._detect_and_sync_missing_fills()
                self._last_sync_time = current_time
            
            # Get current price
            price_data = self.api.get_latest_price(self.cointype, self.market)
            # exchange API returns: {"status":"ok", "prices":{"bid":..., "ask":..., "last":...}}
            
            # Extract all price components for analysis
            bid_price = None
            ask_price = None
            last_price = None
            
            if 'prices' in price_data:
                prices = price_data['prices']
                bid_price = float(prices.get('bid', 0)) if prices.get('bid') else None
                ask_price = float(prices.get('ask', 0)) if prices.get('ask') else None
                last_price = float(prices.get('last', 0)) if prices.get('last') else None
            elif 'latest' in price_data:
                last_price = float(price_data['latest'])
            elif 'price' in price_data:
                last_price = float(price_data['price'])
            elif 'last' in price_data:
                last_price = float(price_data['last'])
            elif isinstance(price_data, (int, float)):
                last_price = float(price_data)
            
            # Use last price as the primary price for trading logic
            price = last_price
            
            # Calculate spread if we have both bid and ask
            spread_abs = None
            spread_pct = None
            mid_price = None
            if bid_price and ask_price and bid_price > 0 and ask_price > 0:
                spread_abs = ask_price - bid_price
                mid_price = (bid_price + ask_price) / 2
                spread_pct = (spread_abs / mid_price) * 100 if mid_price > 0 else None
                
                # Store for later use
                self.bid_price = bid_price
                self.ask_price = ask_price
                self.mid_price = mid_price
                self.spread_abs = spread_abs
                self.spread_pct = spread_pct
            else:
                # Reset if not available
                self.bid_price = 0.0
                self.ask_price = 0.0
                self.mid_price = 0.0
                self.spread_abs = 0.0
                self.spread_pct = 0.0
            
            # ========== START TRADING CYCLE ==========
            self._begin_cycle_log()
            self._info(f"{self.symbol}: ========== START TRADING CYCLE ==========")
            
            # Reset sell-fill flag for this cycle (Option B: don't resize on sell fills)
            self._sell_filled_this_cycle = False
            
            # --- Section: Price Information ---
            self._info(f"{self.symbol}: --- Price Information ---")
            if bid_price and ask_price and last_price and spread_abs and spread_pct:
                self._info(f"{self.symbol}: Price|bid={bid_price:.4f}|ask={ask_price:.4f}|last={last_price:.4f}|"
                           f"mid={mid_price:.4f}|spread_abs={spread_abs:.4f}|spread_pct={spread_pct:.3f}%")
            elif last_price:
                self._info(f"{self.symbol}: Price|last={last_price:.4f}|bid=N/A|ask=N/A|spread=N/A")
            
            if price and price > 0:
                self.update_price(price)
                # Update rolling price high for price elevation tracking
                self._update_rolling_price_high()
            else:
                logging.warning(f"{self.symbol}: Could not get valid price data: {price_data}")
                self._flush_cycle_log(suffix=" (aborted)")
                return
            
            # --- Section: Position Health ---
            self._info(f"{self.symbol}: --- Position Health ---")
            self.log_position_health()
            
            # Get open orders (handle gracefully if API endpoint doesn't work)
            # Track previous orders BEFORE updating to detect fills
            previous_orders_dict = {}
            if hasattr(self, 'open_orders') and self.open_orders:
                # Store previous order details for fill detection
                for order in self.open_orders:
                    previous_orders_dict[order.order_id] = order
            
            orders_fetch_ok = False
            try:
                orders_data = self.api.get_orders(self.cointype, self.market)
                if not orders_data or orders_data.get('status') != 'ok':
                    err = (orders_data or {}).get('message', 'unknown error')
                    logging.error(
                        f"{self.symbol}: Open orders fetch failed ({err}) — "
                        f"skipping ladder placement this cycle (will not treat book as empty)."
                    )
                else:
                    # Only clear/replace open_orders after a successful fetch.
                    self.open_orders = []
                    self.update_open_orders(orders_data)
                    orders_fetch_ok = True
                    buy_count = len([o for o in self.open_orders if o.side == 'buy'])
                    sell_count = len([o for o in self.open_orders if o.side == 'sell'])
                    logging.debug(f"{self.symbol}: Fetched orders - {buy_count} buy, {sell_count} sell")
                    
                    # Detect filled orders by comparing previous vs current
                    # Use previous balance (from before this iteration) to verify fills
                    current_order_ids = {order.order_id for order in self.open_orders}
                    previous_order_ids = set(previous_orders_dict.keys())
                    missing_order_ids = previous_order_ids - current_order_ids
                    
                    # Verify fills by checking balance changes
                    # An order that disappears could be filled OR cancelled
                    # We verify by checking if balance changed as expected
                    for order_id in missing_order_ids:
                        missing_order = previous_orders_dict.get(order_id)
                        if not missing_order:
                            continue
                        
                        # Verify if this was actually a fill or just a cancellation
                        # CRITICAL: Only save orders to JSON when we can definitively verify they were filled
                        # For buy orders: coin balance should increase by approximately the order amount
                        # For sell orders: coin balance should decrease by approximately the order amount
                        # We use strict verification to ensure we only track actual purchases, not pending orders
                        is_verified_fill = False
                        verification_note = ""
                        
                        if missing_order.side == 'buy':
                            # IMPROVED: Use API verification as PRIMARY method (more reliable than balance)
                            # Balance can be affected by concurrent activity, but API is definitive
                            is_verified_fill = self._verify_order_via_api(missing_order)
                            
                            if is_verified_fill:
                                # API confirmed fill - this is definitive
                                coin_balance_change = self.coin_balance - self.previous_coin_balance
                                expected_increase = missing_order.amount
                                verification_note = f" (verified via API - balance changed by {coin_balance_change:.8f}, expected ~{expected_increase:.8f})"
                            else:
                                # API didn't confirm - check balance as secondary verification
                                coin_balance_change = self.coin_balance - self.previous_coin_balance
                                expected_increase = missing_order.amount
                                
                                # Balance verification: must increase by at least 90% of expected
                                if coin_balance_change > 0 and coin_balance_change >= expected_increase * 0.90:
                                    # Balance check passed - treat as filled
                                    if coin_balance_change <= expected_increase * 1.10:
                                        is_verified_fill = True
                                        verification_note = f" (verified: balance increased by {coin_balance_change:.8f}, expected ~{expected_increase:.8f})"
                                    else:
                                        # Balance increased more than expected - likely multiple fills, but API didn't confirm this specific one
                                        verification_note = f" (NOT verified: balance increased by {coin_balance_change:.8f}, but API didn't confirm this order - may be different order)"
                                        logging.warning(f"{self.symbol}: ⚠️ Order {order_id} (buy) - balance increased but API didn't confirm. May be different order.")
                                else:
                                    verification_note = f" (NOT verified: balance changed by {coin_balance_change:.8f}, expected ~{expected_increase:.8f} - likely CANCELLED)"
                                    logging.warning(f"{self.symbol}: ⚠️ Order {order_id} (buy) disappeared but not verified via API or balance - likely CANCELLED")
                        
                        elif missing_order.side == 'sell':
                            # IMPROVED: Use API verification as PRIMARY method (more reliable than balance)
                            is_verified_fill = self._verify_order_via_api(missing_order)
                            
                            if is_verified_fill:
                                # API confirmed fill - this is definitive
                                coin_balance_change = self.previous_coin_balance - self.coin_balance
                                expected_decrease = missing_order.amount
                                verification_note = f" (verified via API - balance changed by {coin_balance_change:.8f}, expected ~{expected_decrease:.8f})"
                            else:
                                # API didn't confirm - check balance as secondary verification
                                coin_balance_change = self.previous_coin_balance - self.coin_balance
                                expected_decrease = missing_order.amount
                                
                                # Balance verification: must decrease by at least 90% of expected
                                if coin_balance_change > 0 and coin_balance_change >= expected_decrease * 0.90:
                                    if coin_balance_change <= expected_decrease * 1.10:
                                        is_verified_fill = True
                                        verification_note = f" (verified: balance decreased by {coin_balance_change:.8f}, expected ~{expected_decrease:.8f})"
                                    else:
                                        verification_note = f" (NOT verified: balance decreased by {coin_balance_change:.8f}, but API didn't confirm this order - may be different order)"
                                        logging.warning(f"{self.symbol}: ⚠️ Order {order_id} (sell) - balance decreased but API didn't confirm. May be different order.")
                                else:
                                    verification_note = f" (NOT verified: balance changed by {coin_balance_change:.8f}, expected ~{expected_decrease:.8f} - likely CANCELLED)"
                                    logging.warning(f"{self.symbol}: ⚠️ Order {order_id} (sell) disappeared but not verified via API or balance - likely CANCELLED")
                        
                        if is_verified_fill:
                            # Treat as filled and notify
                            # Note: Fill price may differ from current price if price moved after fill
                            price_diff = ((self.current_price - missing_order.rate) / missing_order.rate * 100) if self.current_price > 0 and missing_order.rate > 0 else 0
                            price_note = f" (current price: {self.current_price:.4f}, {price_diff:+.1f}% from fill price)" if self.current_price > 0 else ""
                            
                            if missing_order.side == 'buy':
                                completed = self._find_completed_order(missing_order)
                                fill_amount = float(completed.get('amount', missing_order.amount)) if completed else missing_order.amount
                                fill_rate = float(completed.get('rate', missing_order.rate)) if completed else missing_order.rate
                                fill_timestamp = completed.get('solddate') if completed else None
                                if completed:
                                    missing_order.amount = fill_amount
                                    missing_order.rate = fill_rate
                                save_timestamp = fill_timestamp or datetime.utcnow().isoformat()
                                saved = self._save_filled_order(missing_order, save_timestamp)
                                self._notify_buy_fill_if_needed(
                                    missing_order, fill_amount, fill_rate
                                )
                                coin_balance_change = self.coin_balance - self.previous_coin_balance
                                expected_increase = missing_order.amount
                                if saved:
                                    if "via API" in verification_note:
                                        self._info(
                                            f"{self.symbol}: Verified token receipt via API. "
                                            f"Balance: {self.previous_coin_balance:.8f} -> {self.coin_balance:.8f} "
                                            f"(+{coin_balance_change:.8f}). Saved order {order_id} to JSON."
                                        )
                                    else:
                                        self._info(
                                            f"{self.symbol}: Verified token receipt via balance - increased from "
                                            f"{self.previous_coin_balance:.8f} to {self.coin_balance:.8f} "
                                            f"(+{coin_balance_change:.8f}, expected ~{expected_increase:.8f}). "
                                            f"Saved order {order_id} to JSON."
                                        )
                                self.total_coins += missing_order.amount
                                self.total_invested += missing_order.amount * missing_order.rate
                                self._info(
                                    f"{self.symbol}: Tracked buy fill - {missing_order.amount:.8f} @ {missing_order.rate:.4f}, "
                                    f"total: {self.total_coins:.8f} coins, ${self.total_invested:.2f} invested"
                                )

                            self._info(f"{self.symbol}: Order {order_id} ({missing_order.side}) FILLED - "
                                       f"{missing_order.amount:.8f} @ {missing_order.rate:.4f}{price_note}{verification_note}")

                            # Track filled sell orders (reduce tracked position via LIFO)
                            if missing_order.side == 'sell':
                                completed = self._find_completed_order(missing_order)
                                fill_amount = float(completed.get('amount', missing_order.amount)) if completed else missing_order.amount
                                fill_rate = float(completed.get('rate', missing_order.rate)) if completed else missing_order.rate
                                fill_timestamp = (
                                    completed.get('solddate')
                                    if completed and completed.get('solddate')
                                    else datetime.now(timezone.utc).isoformat()
                                )
                                # Placement time on dry-run orders is not the fill time.
                                if completed and completed.get('solddate'):
                                    try:
                                        fill_dt = datetime.fromisoformat(
                                            fill_timestamp.replace('Z', '+00:00')
                                        )
                                        if fill_dt.tzinfo is None:
                                            fill_dt = fill_dt.replace(tzinfo=timezone.utc)
                                        if (datetime.now(timezone.utc) - fill_dt).total_seconds() > 86400:
                                            fill_timestamp = datetime.now(timezone.utc).isoformat()
                                    except (ValueError, TypeError, AttributeError):
                                        fill_timestamp = datetime.now(timezone.utc).isoformat()
                                if completed:
                                    missing_order.amount = fill_amount
                                    missing_order.rate = fill_rate

                                # Check if order is already in JSON before processing
                                # This prevents duplicate notifications when sync method also finds the order
                                # Don't pass fill_timestamp — same reasoning as the buy path above.
                                order_already_in_json = self._is_order_already_in_json(missing_order)
                                
                                # Determine if this is a Working sell BEFORE save (save discards from _working_order_ids)
                                is_working_sell = order_id in getattr(self, '_working_order_ids', set())
                                
                                # Calculate profit for this sell before saving (for skim functionality)
                                sell_profit, sell_cost_basis = self._calculate_sell_profit(missing_order)
                                
                                # Reset daily profit if new day
                                self._reset_daily_profit_if_new_day()
                                
                                # Update daily realized profit
                                self.daily_realized_profit += sell_profit
                                self._info(f"{self.symbol}: Sell profit: ${sell_profit:.2f}, "
                                           f"Daily total: ${self.daily_realized_profit:.2f}")
                                
                                # Execute skim if conditions met (uses sell profit, not daily total)
                                if self.skim_enabled and sell_profit > 0:
                                    self._execute_skim(sell_profit)
                                
                                # When we sell, we reduce our position and consume buy orders via LIFO
                                # This tracks the sell and marks corresponding buy orders as consumed
                                # Only save if not already in JSON
                                if not order_already_in_json:
                                    self._save_filled_sell_order(missing_order, fill_timestamp)
                                    self._info(f"{self.symbol}: Sell order filled - position reduced by {missing_order.amount:.8f} "
                                               f"@ {missing_order.rate:.4f} = ${missing_order.amount * missing_order.rate:.2f}")
                                else:
                                    logging.debug(f"{self.symbol}: Sell order {order_id} already in JSON, skipping save (already processed by sync)")
                                
                                # Notify only if order is not already in JSON (wasn't already notified by sync)
                                if not order_already_in_json:
                                    # Use LIFO cost basis for notification whenever we have it (correct P&L for this sell)
                                    if sell_cost_basis > 0 and missing_order.amount > 0:
                                        avg_entry_for_notify = sell_cost_basis / missing_order.amount
                                    else:
                                        avg_entry_for_notify = self.average_entry_price
                                    self.notifier.notify_order_filled(
                                        symbol=self.symbol,
                                        side=missing_order.side,
                                        amount=missing_order.amount,
                                        rate=missing_order.rate,
                                        avg_entry=avg_entry_for_notify,
                                        profit_usd=sell_profit,
                                        daily_skim_purchases=self.daily_skim_purchases if self.daily_skim_purchases else None,
                                        api=self.api
                                    )
                                else:
                                    logging.debug(f"{self.symbol}: Order {order_id} already in JSON, skipping notification (already notified by sync)")
                        else:
                            # Order disappeared but couldn't verify as fill - likely cancelled
                            logging.warning(f"{self.symbol}: ⚠️ Order {order_id} ({missing_order.side}) disappeared but couldn't verify as fill - "
                                          f"may have been cancelled. Amount: {missing_order.amount:.8f} @ {missing_order.rate:.4f}")
                    
                    # Update previous order IDs for next iteration
                    self.previous_order_ids = current_order_ids
            except Exception as e:
                orders_fetch_ok = False
                logging.error(
                    f"{self.symbol}: Could not fetch open orders: {e}. "
                    f"Skipping ladder placement this cycle."
                )
            
            if not orders_fetch_ok:
                self._flush_cycle_log(suffix=" (orders fetch failed)")
                return

            # --- Section: Buy Ladder ---
            self._info(f"{self.symbol}: --- Buy Ladder ---")
            self.place_buy_ladder()
            
            
            # --- Section: Sell Ladder ---
            self._info(f"{self.symbol}: --- Sell Ladder ---")
            self.place_sell_ladder()
            
            # ========== END TRADING CYCLE ==========
            self._flush_cycle_log()
            
        except Exception as e:
            if self._cycle_logging or self._cycle_log_buffer:
                self._flush_cycle_log(suffix=" (error)")
            logging.error(f"{self.symbol}: Error in process loop: {e}")
            self.notifier.notify_error(self.symbol, str(e))

