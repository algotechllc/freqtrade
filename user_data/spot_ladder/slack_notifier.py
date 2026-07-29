"""Slack notifications for spot ladder (same hooks as legacy Telegram notifier)."""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request
from typing import Any, Optional

logger = logging.getLogger(__name__)


class SlackNotifier:
    """Posts ladder events to a Slack Incoming Webhook."""

    def __init__(
        self,
        webhook_url: Optional[str] = None,
        *,
        dry_run: bool = False,
        dry_run_label: bool = True,
        username: str = "Spot Ladder",
        icon_emoji: str = ":chart_with_upwards_trend:",
    ):
        self.webhook_url = (webhook_url or os.environ.get("SPOT_LADDER_SLACK_WEBHOOK_URL") or "").strip()
        self.dry_run = dry_run
        self.dry_run_label = dry_run_label
        self.username = username
        self.icon_emoji = icon_emoji
        self.enabled = bool(self.webhook_url)
        if not self.enabled:
            logger.warning(
                "Slack notifier disabled: set slack.webhook_url in spot_ladder/config.yaml "
                "or SPOT_LADDER_SLACK_WEBHOOK_URL"
            )

    def _prefix(self, text: str) -> str:
        if self.dry_run and self.dry_run_label:
            return f"*[DRY-RUN]* {text}"
        return text

    def _post(self, text: str) -> None:
        if not self.enabled:
            logger.debug("Slack (disabled): %s", text[:200])
            return
        body = {
            "text": self._prefix(text),
            "username": self.username,
            "icon_emoji": self.icon_emoji,
        }
        data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(
            self.webhook_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                if resp.status >= 400:
                    logger.warning("Slack webhook HTTP %s", resp.status)
        except urllib.error.URLError as e:
            logger.error("Slack post failed: %s", e)

    @staticmethod
    def _format_ladder_orders(orders: list, side: str) -> str:
        lines = []
        for row in orders:
            amount = float(row.get("amount") or 0)
            rate = float(row.get("rate") or 0)
            level = row.get("level_pct")
            oid = row.get("order_id") or ""
            extra = f" ({level}%)" if level is not None else ""
            lines.append(f"• {amount:.4f} @ {rate:.4f}{extra} `{oid}`")
        header = f"*{side} ladder* ({len(orders)} orders)"
        return header + "\n" + "\n".join(lines[:25])

    def notify_buy_order_placed(
        self, symbol: str, amount: float, rate: float, order_id: str = None
    ):
        self._post(f"{symbol}: buy placed {amount:.4f} @ {rate:.4f} `{order_id or ''}`")

    def notify_sell_order_placed(
        self, symbol: str, amount: float, rate: float, order_id: str = None
    ):
        self._post(f"{symbol}: sell placed {amount:.4f} @ {rate:.4f} `{order_id or ''}`")

    def notify_buy_ladder_recalculated(self, symbol: str, orders: list, current_price: float):
        if not orders:
            return
        self._post(
            f"{symbol}: buy ladder updated (price {current_price:.4f})\n"
            + self._format_ladder_orders(orders, "Buy")
        )

    def notify_sell_ladder_recalculated(
        self,
        symbol: str,
        orders: list,
        avg_entry: float = None,
        current_price: float = 0,
        order_type: str = "Core",
    ):
        if not orders:
            return
        ctx = f"avg {avg_entry:.4f}, mkt {current_price:.4f}" if avg_entry else f"mkt {current_price:.4f}"
        self._post(
            f"{symbol}: {order_type} sell ladder updated ({ctx})\n"
            + self._format_ladder_orders(orders, f"{order_type} sell")
        )

    def notify_order_filled(self, symbol: str, side: str, amount: float, rate: float, **kwargs: Any):
        side_u = side.upper()
        avg_entry = kwargs.get("avg_entry")
        msg = f"{symbol}: *{side_u} FILLED* {amount:.4f} @ {rate:.4f}"
        if avg_entry and side.lower() == "sell":
            msg += f" (cost basis ~{float(avg_entry):.4f})"
        daily_skim = kwargs.get("daily_skim_purchases")
        if daily_skim:
            msg += f"\n_skim activity recorded_"
        self._post(msg)

    def notify_skim_purchase(
        self,
        symbol: str,
        sell_profit: float,
        skim_amount: float,
        *args: Any,
        **kwargs: Any,
    ):
        self._post(
            f"{symbol}: skim — profit ${sell_profit:.2f}, skim ${skim_amount:.2f}"
        )

    def notify_order_cancelled(self, symbol: str, order_id: str, reason: str = ""):
        self._post(f"{symbol}: order cancelled `{order_id}` {reason}".strip())

    def notify_order_updated(self, symbol: str, order_id: str, new_rate: float):
        self._post(f"{symbol}: order `{order_id}` updated → {new_rate:.4f}")

    def notify_error(self, symbol: str, error: str):
        self._post(f":warning: {symbol}: {error}")

    def notify_status(self, message: str):
        self._post(message)

    def notify_daily_summary(self, summary: dict, symbol: str = "XRP/USDC", **kwargs: Any):
        lines = [f"*Daily summary — {symbol}*"]
        for key, value in summary.items():
            if value is None:
                continue
            lines.append(f"• *{key}*: {value}")
        self._post("\n".join(lines))
