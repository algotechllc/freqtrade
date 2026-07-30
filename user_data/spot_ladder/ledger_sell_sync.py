"""
Backfill filled_orders JSON sell_orders from the dry-run order store.

Dry-run CCXT orders keep placement time in `timestamp`; fills are tracked with
`closed_at` once we detect a close. Sells that filled without going through
OrderManager._save_filled_sell_order are picked up here (daily summary + repair).
"""

from __future__ import annotations

import copy
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from spot_ladder.dry_run_balances import dry_run_order_fill_timestamp
from spot_ladder.dry_run_order_store import dry_run_orders_path

logger = logging.getLogger(__name__)


def _parse_ts(value: str) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError, AttributeError):
        return None


def _apply_lifo_consumption(buy_orders: list[dict], sell: dict) -> None:
    """Mark buy lots consumed by this sell (newest buys first)."""
    amount_to_consume = float(sell.get("amount", 0))
    sell_ts = sell.get("fill_timestamp") or ""

    def sort_key(row: dict) -> str:
        ts = row.get("fill_timestamp") or ""
        return ts if _parse_ts(ts) else datetime.min.replace(tzinfo=timezone.utc).isoformat()

    for buy in sorted(buy_orders, key=sort_key, reverse=True):
        if amount_to_consume <= 0:
            break
        buy_ts = buy.get("fill_timestamp") or ""
        if buy_ts and sell_ts:
            buy_dt = _parse_ts(buy_ts)
            sell_dt = _parse_ts(sell_ts)
            if buy_dt and sell_dt and buy_dt >= sell_dt:
                continue
        buy_amount = float(buy.get("amount", 0))
        already = float(buy.get("consumed_amount", 0))
        remaining = buy_amount - already
        if remaining <= 0:
            continue
        if remaining <= amount_to_consume:
            buy["consumed_amount"] = buy_amount
            buy["fully_consumed"] = True
            buy["consumed_by_sell"] = sell.get("order_id")
            amount_to_consume -= remaining
        else:
            buy["consumed_amount"] = already + amount_to_consume
            buy["consumed_by_sell"] = sell.get("order_id")
            amount_to_consume = 0


def _load_working_order_ids(state_dir: Path, cointype: str) -> set[str]:
    path = state_dir / f"working_orders_{cointype.upper()}.json"
    if not path.is_file():
        return set()
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return {str(i) for i in (data.get("working_order_ids") or [])}
    except (OSError, json.JSONDecodeError, TypeError):
        return set()


def _closed_sells_from_dry_store(
    state_dir: Path, cointype: str, pair: str
) -> list[dict[str, Any]]:
    path = dry_run_orders_path(str(state_dir), cointype)
    if not os.path.isfile(path):
        return []
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("Could not read dry-run orders %s: %s", path, e)
        return []

    sells: list[dict[str, Any]] = []
    for order in data.get("orders") or []:
        if not isinstance(order, dict):
            continue
        if (order.get("status") or "").lower() != "closed":
            continue
        if order.get("symbol") and order.get("symbol") != pair:
            continue
        if str(order.get("side", "")).lower() != "sell":
            continue
        amount = float(order.get("filled") or order.get("amount") or 0)
        rate = float(order.get("average") or order.get("price") or 0)
        if amount <= 0 or rate <= 0:
            continue
        oid = str(order.get("id") or "")
        sells.append(
            {
                "order_id": oid,
                "api_order_id": oid,
                "amount": amount,
                "rate": rate,
                "fill_timestamp": dry_run_order_fill_timestamp(order),
                "closed_at": order.get("closed_at"),
            }
        )
    return sells


def sync_missing_sells_from_dry_run_store(
    filled_path: Path,
    state_dir: Path,
    cointype: str,
    pair: str,
    *,
    symbol: str = "",
    market: str = "USDC",
) -> int:
    """
    Append closed dry-run sells missing from filled_orders JSON and apply LIFO.

    Returns number of sells added.
    """
    if not filled_path.is_file():
        return 0

    closed_sells = _closed_sells_from_dry_store(state_dir, cointype, pair)
    if not closed_sells:
        return 0

    with open(filled_path, encoding="utf-8") as f:
        data = json.load(f)

    buy_orders = data.get("buy_orders") or []
    sell_orders = data.get("sell_orders") or []
    known_ids = {str(o.get("order_id", "")) for o in sell_orders}
    known_ids |= {str(o.get("api_order_id", "")) for o in sell_orders if o.get("api_order_id")}
    working_ids = _load_working_order_ids(state_dir, cointype)

    added = 0
    for row in sorted(closed_sells, key=lambda s: s.get("fill_timestamp") or ""):
        oid = row["order_id"]
        if not oid or oid in known_ids:
            continue
        sell_record = {
            "order_id": oid,
            "api_order_id": oid,
            "symbol": symbol or data.get("symbol", ""),
            "amount": row["amount"],
            "rate": row["rate"],
            "market": market,
            "fill_timestamp": row["fill_timestamp"],
            "total_usd": row["amount"] * row["rate"],
            "is_working": oid in working_ids,
            "synced_from_dry_run_store": True,
        }
        _apply_lifo_consumption(buy_orders, sell_record)
        sell_orders.append(sell_record)
        known_ids.add(oid)
        added += 1
        logger.info(
            "Ledger sync: added sell %s %.4f @ %.4f (%s)",
            oid[:12],
            row["amount"],
            row["rate"],
            row["fill_timestamp"][:19],
        )

    if added:
        data["buy_orders"] = buy_orders
        data["sell_orders"] = sell_orders
        data["last_updated"] = datetime.now(timezone.utc).isoformat()
        tmp = f"{filled_path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, str(filled_path))

    return added


def _load_filled_working_order_ids(state_dir: Path, cointype: str) -> set[str]:
    path = state_dir / f"filled_working_orders_{cointype.upper()}.json"
    if not path.is_file():
        return set()
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return {str(i) for i in (data.get("working_order_ids") or [])}
    except (OSError, json.JSONDecodeError, TypeError):
        return set()


def _reset_buy_consumption(buy_orders: list[dict]) -> None:
    for buy in buy_orders:
        buy["consumed_amount"] = 0.0
        buy.pop("fully_consumed", None)
        buy.pop("consumed_by_sell", None)


def _merge_sell_sources(
    existing_sells: list[dict],
    dry_sells: list[dict],
    *,
    symbol: str,
    market: str,
    working_ids: set[str],
    filled_working_ids: set[str],
) -> list[dict[str, Any]]:
    """Union ledger + dry-run closed sells (dry-run wins on amount/rate/time for same id)."""
    merged: dict[str, dict[str, Any]] = {}

    for row in existing_sells:
        oid = str(row.get("order_id") or row.get("api_order_id") or "")
        if not oid:
            continue
        merged[oid] = dict(row)

    for row in dry_sells:
        oid = row["order_id"]
        if not oid:
            continue
        prev = merged.get(oid, {})
        amount = float(row.get("amount") or prev.get("amount") or 0)
        rate = float(row.get("rate") or prev.get("rate") or 0)
        fill_ts = row.get("fill_timestamp") or prev.get("fill_timestamp") or ""
        is_working = oid in working_ids or oid in filled_working_ids or bool(prev.get("is_working"))
        merged[oid] = {
            "order_id": oid,
            "api_order_id": oid,
            "symbol": prev.get("symbol") or symbol,
            "amount": amount,
            "rate": rate,
            "market": prev.get("market") or market,
            "fill_timestamp": fill_ts,
            "total_usd": amount * rate,
            "is_working": is_working,
            "backfilled": True,
        }

    return list(merged.values())


def rebuild_full_sell_history(
    filled_path: Path,
    state_dir: Path,
    cointype: str,
    pair: str,
    *,
    symbol: str = "",
    market: str = "USDC",
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Rebuild sell_orders from scratch: merge ledger + dry-run store, replay LIFO on buys.

    Buy lots are unchanged except consumed_amount / fully_consumed (reset then replayed).
    Returns summary dict with counts.
    """
    if not filled_path.is_file():
        raise FileNotFoundError(f"Ledger not found: {filled_path}")

    with open(filled_path, encoding="utf-8") as f:
        data = json.load(f)

    original_data = copy.deepcopy(data)
    buy_orders = copy.deepcopy(data.get("buy_orders") or [])
    existing_sells = data.get("sell_orders") or []
    dry_sells = _closed_sells_from_dry_store(state_dir, cointype, pair)
    working_ids = _load_working_order_ids(state_dir, cointype)
    filled_working_ids = _load_filled_working_order_ids(state_dir, cointype)

    merged = _merge_sell_sources(
        existing_sells,
        dry_sells,
        symbol=symbol or str(data.get("symbol") or ""),
        market=market,
        working_ids=working_ids,
        filled_working_ids=filled_working_ids,
    )
    merged.sort(key=lambda s: s.get("fill_timestamp") or "")

    _reset_buy_consumption(buy_orders)
    new_sell_orders: list[dict] = []
    for sell in merged:
        record = dict(sell)
        _apply_lifo_consumption(buy_orders, record)
        new_sell_orders.append(record)

    summary = {
        "existing_sells": len(existing_sells),
        "dry_store_sells": len(dry_sells),
        "merged_sells": len(merged),
        "added_from_dry_store": len(
            {s["order_id"] for s in dry_sells}
            - {str(o.get("order_id") or o.get("api_order_id") or "") for o in existing_sells}
        ),
        "filled_path": str(filled_path),
    }

    if dry_run:
        summary["dry_run"] = True
        return summary

    meta = data.get("metadata") or {}
    if not isinstance(meta, dict):
        meta = {}
    meta["sell_history_backfill"] = datetime.now(timezone.utc).isoformat()

    data["buy_orders"] = buy_orders
    data["sell_orders"] = new_sell_orders
    data["metadata"] = meta
    data["last_updated"] = datetime.now(timezone.utc).isoformat()

    backup = filled_path.with_suffix(
        f".pre_sell_backfill_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    )
    with open(backup, "w", encoding="utf-8") as f:
        json.dump(original_data, f, indent=2)
    summary["backup"] = str(backup)

    tmp = f"{filled_path}.tmp"
    out = {
        "symbol": data.get("symbol"),
        "cointype": data.get("cointype"),
        "market": data.get("market"),
        "last_updated": data["last_updated"],
        "buy_orders": buy_orders,
        "sell_orders": new_sell_orders,
        "metadata": meta,
    }
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
    os.replace(tmp, str(filled_path))

    return summary


def refresh_sell_fill_timestamps_from_dry_store(
    filled_path: Path,
    state_dir: Path,
    cointype: str,
    pair: str,
) -> int:
    """
    Fix sell_orders rows that used placement time instead of closed_at.

    Only updates when dry-run store has a matching closed sell with closed_at.
    """
    if not filled_path.is_file():
        return 0

    by_id = {s["order_id"]: s for s in _closed_sells_from_dry_store(state_dir, cointype, pair)}
    if not by_id:
        return 0

    with open(filled_path, encoding="utf-8") as f:
        data = json.load(f)

    updated = 0
    for sell in data.get("sell_orders") or []:
        oid = str(sell.get("order_id") or sell.get("api_order_id") or "")
        dry = by_id.get(oid)
        if not dry or not dry.get("closed_at"):
            continue
        new_ts = dry["fill_timestamp"]
        old_ts = sell.get("fill_timestamp") or ""
        if old_ts == new_ts:
            continue
        sell["fill_timestamp"] = new_ts
        updated += 1

    if updated:
        data["last_updated"] = datetime.now(timezone.utc).isoformat()
        with open(filled_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    return updated
