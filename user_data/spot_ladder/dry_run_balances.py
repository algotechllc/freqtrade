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


def refresh_dry_order_fills(exchange: Exchange, pair: str) -> bool:
    """
    Re-run dry-run limit fill checks for open orders on this pair.

    Fetches the L2 book once and reuses it — calling check_dry_limit_order_filled
    without an orderbook would hit fetch_l2_order_book per order (very slow on HL).

    Returns True if any order status/fill fields changed.
    """
    if not exchange._config.get("dry_run"):
        return False
    store = exchange._dry_run_open_orders
    open_orders = [
        (oid, o)
        for oid, o in list(store.items())
        if o.get("symbol") == pair and (o.get("status") or "open") == "open"
    ]
    if not open_orders:
        return False

    orderbook = None
    if exchange.exchange_has("fetchL2OrderBook"):
        try:
            orderbook = exchange.fetch_l2_order_book(pair, 1)
        except Exception as e:
            logger.debug("dry_run L2 fetch failed for %s: %s", pair, e)

    changed = False
    for order_id, order in open_orders:
        before_status = order.get("status")
        before_filled = order.get("filled")
        try:
            updated = exchange.check_dry_limit_order_filled(order, orderbook=orderbook)
            store[order_id] = updated
            if updated.get("status") != before_status or updated.get("filled") != before_filled:
                changed = True
        except Exception as e:
            logger.debug("dry_run fill refresh failed for %s: %s", order_id, e)
    return changed


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


def _load_filled_orders_json(path: str) -> dict[str, Any] | None:
    if not os.path.isfile(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("Could not read filled orders JSON %s: %s", path, e)
        return None


def _json_recorded_order_ids(data: dict[str, Any]) -> set[str]:
    """IDs already in the ladder ledger — do not re-apply matching dry-run closes."""
    ids: set[str] = set()
    for key in ("buy_orders", "sell_orders"):
        for row in data.get(key) or []:
            for field in ("order_id", "api_order_id", "id"):
                oid = row.get(field)
                if oid:
                    ids.add(str(oid))
    return ids


def _ledger_from_filled_orders_json(path: str, start_wallet: float) -> tuple[float, float]:
    """Reconstruct balances from ladder filled_orders JSON (manual seed / after restart)."""
    data = _load_filled_orders_json(path)
    if data is None:
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

    When filled_orders JSON exists, start from that ledger. Closed dry-run orders
    are applied only if their IDs are not already recorded in JSON (avoids
    double-counting seed lots + fills that OrderManager already saved).
    """
    fills_changed = refresh_dry_order_fills(exchange, pair)
    # Persist only when a fill closed an order (not on every cycle).
    if fills_changed:
        try:
            from spot_ladder.dry_run_order_store import save_dry_run_orders

            state_dir = os.path.dirname(filled_orders_path) if filled_orders_path else ""
            if state_dir:
                save_dry_run_orders(exchange, pair, state_dir, base_currency)
        except Exception as e:
            logger.debug("dry-run order persist after balance refresh failed: %s", e)

    dry_for_pair = [
        o for o in exchange._dry_run_open_orders.values() if o.get("symbol") == pair
    ]

    json_data = _load_filled_orders_json(filled_orders_path)
    if json_data is not None:
        usdc, base = _ledger_from_filled_orders_json(filled_orders_path, start_wallet)
        source = "filled_orders JSON"
        known_ids = _json_recorded_order_ids(json_data)
    else:
        usdc, base = float(start_wallet), 0.0
        source = "dry_run_wallet"
        known_ids = set()

    if dry_for_pair:
        closed = [o for o in dry_for_pair if (o.get("status") or "") == "closed"]
        if closed:
            unseen = [o for o in closed if str(o.get("id", "")) not in known_ids]
            if unseen:
                usdc, base = _apply_closed_dry_orders(unseen, usdc, base)
                source = f"{source} + {len(unseen)} unseen closed dry-run order(s)"
        # Open-only dry orders do not change holdings.

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
