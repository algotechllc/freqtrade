"""No-op notifier placeholder (Slack notifications planned later)."""

import logging
from typing import Any, Optional


class TelegramNotifier:
    """Stub: preserves OrderManager call sites without sending messages."""

    def __init__(self, bot_token: Optional[str] = None, chat_id: Optional[str] = None):
        self.enabled = False

    def notify_buy_order_placed(self, symbol: str, amount: float, rate: float, order_id: str = None):
        logging.debug("notify_buy_order_placed(%s) skipped (notifications disabled)", symbol)

    def notify_sell_order_placed(self, symbol: str, amount: float, rate: float, order_id: str = None):
        logging.debug("notify_sell_order_placed(%s) skipped", symbol)

    def notify_buy_ladder_recalculated(self, symbol: str, orders: list, current_price: float):
        logging.debug("notify_buy_ladder_recalculated(%s, %s orders)", symbol, len(orders))

    def notify_sell_ladder_recalculated(
        self,
        symbol: str,
        orders: list,
        avg_entry: float = None,
        current_price: float = 0,
        order_type: str = "Core",
    ):
        logging.debug("notify_sell_ladder_recalculated(%s, %s, %s orders)", symbol, order_type, len(orders))

    def notify_order_filled(self, symbol: str, side: str, amount: float, rate: float, **kwargs: Any):
        logging.info("%s: fill notification stub — %s %s @ %s", symbol, side, amount, rate)

    def notify_skim_purchase(self, symbol: str, sell_profit: float, skim_amount: float, **kwargs: Any):
        logging.debug("notify_skim_purchase(%s) skipped", symbol)

    def notify_order_cancelled(self, symbol: str, order_id: str, reason: str = ""):
        logging.debug("notify_order_cancelled(%s, %s)", symbol, order_id)

    def notify_order_updated(self, symbol: str, order_id: str, new_rate: float):
        logging.debug("notify_order_updated(%s, %s)", symbol, order_id)

    def notify_error(self, symbol: str, error: str):
        logging.error("%s: %s", symbol, error)

    def notify_status(self, message: str):
        logging.info("status: %s", message)

    def notify_daily_summary(self, summary: dict, symbol: str = "XRP/USDC", **kwargs: Any):
        logging.debug("notify_daily_summary(%s) skipped", symbol)
