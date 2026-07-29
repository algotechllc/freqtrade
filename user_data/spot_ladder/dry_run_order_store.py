"""
Persist Freqtrade dry-run open/closed orders so restarts match LIVE exchange books.

LIVE Hyperliquid orders survive process restart via fetch_open_orders.
Dry-run orders only live in exchange._dry_run_open_orders (memory) unless saved here.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

from freqtrade.exchange import Exchange

logger = logging.getLogger(__name__)


def dry_run_orders_path(state_dir: str, cointype: str) -> str:
    return os.path.join(state_dir, f"dry_run_orders_{cointype.upper()}.json")


def _json_safe_order(order: dict[str, Any]) -> dict[str, Any]:
    """Copy order fields that round-trip through JSON (skip non-serializable noise)."""
    out: dict[str, Any] = {}
    for key, value in order.items():
        if key.startswith("_"):
            continue
        if isinstance(value, (str, int, float, bool)) or value is None:
            out[key] = value
        elif isinstance(value, dict):
            try:
                json.dumps(value)
                out[key] = value
            except (TypeError, ValueError):
                continue
        elif isinstance(value, list):
            try:
                json.dumps(value)
                out[key] = value
            except (TypeError, ValueError):
                continue
    return out


def load_dry_run_orders(
    exchange: Exchange,
    pair: str,
    state_dir: str,
    cointype: str,
) -> int:
    """
    Load persisted dry-run orders for pair into exchange._dry_run_open_orders.

    Only runs when dry_run is enabled. Does not overwrite IDs already in memory.
    Returns number of orders loaded from disk.
    """
    if not exchange._config.get("dry_run"):
        return 0

    path = dry_run_orders_path(state_dir, cointype)
    if not os.path.isfile(path):
        return 0

    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("Could not read dry-run order store %s: %s", path, e)
        return 0

    orders = data.get("orders") or []
    store = exchange._dry_run_open_orders
    loaded = 0
    for order in orders:
        if not isinstance(order, dict):
            continue
        if order.get("symbol") and order.get("symbol") != pair:
            continue
        oid = str(order.get("id") or "")
        if not oid:
            continue
        # Force pair in case file was moved between configs.
        order = dict(order)
        order["symbol"] = pair
        order["id"] = oid
        if oid in store:
            continue
        store[oid] = order
        loaded += 1

    if loaded:
        open_n = sum(
            1
            for o in store.values()
            if o.get("symbol") == pair and (o.get("status") or "open") == "open"
        )
        logger.info(
            "Restored %s dry-run order(s) for %s from %s (%s still open)",
            loaded,
            pair,
            path,
            open_n,
        )
    return loaded


def save_dry_run_orders(
    exchange: Exchange,
    pair: str,
    state_dir: str,
    cointype: str,
) -> None:
    """Write dry-run orders for this pair to disk (open + closed; drop canceled)."""
    if not exchange._config.get("dry_run"):
        return
    if not state_dir:
        return

    os.makedirs(state_dir, exist_ok=True)
    path = dry_run_orders_path(state_dir, cointype)
    store = exchange._dry_run_open_orders
    orders: list[dict[str, Any]] = []
    for order in store.values():
        if order.get("symbol") != pair:
            continue
        status = (order.get("status") or "open").lower()
        # Canceled rows are not on the LIVE book; omit to keep the file lean.
        if status == "canceled":
            continue
        orders.append(_json_safe_order(order))

    payload = {
        "pair": pair,
        "cointype": cointype.upper(),
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "orders": orders,
    }
    tmp = f"{path}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        os.replace(tmp, path)
        logger.debug(
            "Saved %s dry-run order(s) for %s to %s",
            len(orders),
            pair,
            path,
        )
    except OSError as e:
        logger.warning("Failed to save dry-run order store %s: %s", path, e)
        try:
            if os.path.isfile(tmp):
                os.remove(tmp)
        except OSError:
            pass
