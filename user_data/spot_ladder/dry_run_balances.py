"""
Dry-run balance simulation for the spot ladder (Hyperliquid / no wallet).

Freqtrade's Wallets only track Trade DB rows; ladder orders use exchange.create_order
directly. Balances are derived from:
  1. Closed/open dry-run CCXT orders on the exchange instance, when present
  2. Otherwise filled_orders_{COIN}.json (for restarts and manual test seeding)
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from freqtrade.exchange import Exchange

logger = logging.getLogger(__name__)


def _order_side(order: dict[str, Any]) -> str:
    return str(order.get("side") or "").lower()


def _order_fee_cost(order: dict[str, Any]) -> float:
    fee = order.get("fee")
    if isinstance(fee, dict):
        return float(fee.get("cost") or 0)
    return 0.0


def refresh_dry_order_fills(exchange: Exchange, pair: str) -> None:
    """Re-run dry-run limit fill checks for all orders on this pair."""
    if not exchange._config.get("dry_run"):
        return
    store = exchange._dry_run_open_orders
    for order_id, order in list(store.items()):
        if order.get("symbol") != pair:
            continue
        try:
            store[order_id] = exchange.check_dry_limit_order_filled(order)
        except Exception as e:
            logger.debug("dry_run fill refresh failed for %s: %s", order_id, e)


def _apply_closed_dry_orders(
    orders: list[dict[str, Any]], usdc: float, base: float
) -> tuple[float, float]:
    """Apply closed dry-run order fills on top of existing quote/base balances."""
    closed = [o for o in orders if (o.get("status") or "") == "closed"]
    closed.sort(key=lambda o: int(o.get("timestamp") or 0))

    for order in closed:
        side = _order_side(order)
        filled = float(order.get("filled") or order.get("amount") or 0)
        if filled <= 0:
            continue
        cost = float(order.get("cost") or 0)
        if cost <= 0:
            px = float(order.get("average") or order.get("price") or 0)
            cost = filled * px
        fee = _order_fee_cost(order)
        if side == "buy":
            usdc -= cost + fee
            base += filled
        elif side == "sell":
            usdc += cost - fee
            base -= filled

    return max(usdc, 0.0), max(base, 0.0)


def _ledger_from_dry_orders(orders: list[dict[str, Any]], start_wallet: float) -> tuple[float, float]:
    """Return (quote_balance, base_balance) after applying closed dry orders from start_wallet."""
    return _apply_closed_dry_orders(orders, float(start_wallet), 0.0)


def _ledger_from_filled_orders_json(path: str, start_wallet: float) -> tuple[float, float]:
    """Reconstruct balances from ladder filled_orders JSON (manual seed / after restart)."""
    if not os.path.isfile(path):
        return float(start_wallet), 0.0

    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("Could not read filled orders JSON %s: %s", path, e)
        return float(start_wallet), 0.0

    buys = data.get("buy_orders") or []
    sells = data.get("sell_orders") or []

    base = 0.0
    for row in buys:
        amount = float(row.get("amount") or 0)
        consumed = float(row.get("consumed_amount") or 0)
        base += amount - consumed

    usdc = float(start_wallet)
    for row in buys:
        usdc -= float(row.get("amount") or 0) * float(row.get("rate") or 0)
    for row in sells:
        usdc += float(row.get("amount") or 0) * float(row.get("rate") or 0)

    return max(usdc, 0.0), max(base, 0.0)


def compute_dry_run_balances_flat(
    exchange: Exchange,
    pair: str,
    stake_currency: str,
    base_currency: str,
    start_wallet: float,
    filled_orders_path: str,
) -> dict[str, dict[str, float]]:
    """
    Flat balance dict for OrderManager.update_balances().

    stake_currency / base_currency keys are upper-case (e.g. USDC, XRP).
    Quote balance is the full dry_run_wallet after simulated fills (OrderManager
    applies balance_percentage_per_symbol separately).
    """
    refresh_dry_order_fills(exchange, pair)
    dry_for_pair = [
        o for o in exchange._dry_run_open_orders.values() if o.get("symbol") == pair
    ]

    has_json = os.path.isfile(filled_orders_path)
    if has_json:
        usdc, base = _ledger_from_filled_orders_json(filled_orders_path, start_wallet)
        source = "filled_orders JSON"
    else:
        usdc, base = float(start_wallet), 0.0
        source = "dry_run_wallet"

    if dry_for_pair:
        closed = [o for o in dry_for_pair if (o.get("status") or "") == "closed"]
        if closed:
            usdc, base = _apply_closed_dry_orders(dry_for_pair, usdc, base)
            source = f"{source} + closed dry-run orders"
        # Open-only dry orders do not change holdings; JSON seed (or start_wallet) stays as-is.

    stake = stake_currency.upper()
    coin = base_currency.upper()
    logger.debug(
        "Dry-run balances (%s): %s=%.2f %s=%.8f (start_wallet=%.2f)",
        source,
        stake,
        usdc,
        coin,
        base,
        start_wallet,
    )
    return {
        stake: {"balance": usdc},
        coin: {"balance": base},
    }


def dry_run_completed_order_rows(
    exchange: Exchange, pair: str, cointype: str
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Closed dry-run orders as ladder-shaped buy/sell rows (for fill verification)."""
    refresh_dry_order_fills(exchange, pair)
    buyorders: list[dict[str, Any]] = []
    sellorders: list[dict[str, Any]] = []
    for order in exchange._dry_run_open_orders.values():
        if order.get("symbol") != pair or (order.get("status") or "") != "closed":
            continue
        side = _order_side(order)
        ts = order.get("timestamp")
        solddate = ""
        if ts:
            from datetime import datetime, timezone

            solddate = datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc).isoformat()
        row = {
            "id": str(order.get("id", "")),
            "amount": float(order.get("filled") or order.get("amount") or 0),
            "rate": float(order.get("average") or order.get("price") or 0),
            "solddate": solddate,
            "coin": cointype.upper(),
        }
        if side == "buy":
            buyorders.append(row)
        elif side == "sell":
            sellorders.append(row)
    return buyorders, sellorders
