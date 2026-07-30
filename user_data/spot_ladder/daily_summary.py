"""
Daily ladder summary — same sections/metrics as the legacy Telegram daily_summary.

Built from filled_orders JSON, optional dry-run order book, and market price fetch.
"""

from __future__ import annotations

import copy
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


def _parse_ts(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError, AttributeError):
        return None


def _ts_date(value: str) -> Optional[date]:
    dt = _parse_ts(value)
    return dt.date() if dt else None


def _report_timezone(reporting: dict[str, Any]) -> ZoneInfo:
    """Timezone for calendar-day boundaries (trading activity / realized P&L)."""
    name = (reporting.get("timezone") or os.environ.get("TZ") or "UTC").strip()
    if name.upper() in ("UTC", "GMT", "ETC/UTC"):
        return timezone.utc
    try:
        return ZoneInfo(name)
    except Exception:
        logger.warning("Invalid reporting.timezone %r — using UTC", name)
        return timezone.utc


def _fill_date_in_tz(value: str, tz: ZoneInfo) -> Optional[date]:
    dt = _parse_ts(value)
    if not dt:
        return None
    return dt.astimezone(tz).date()


def _resolve_report_date(reporting: dict[str, Any], tz: ZoneInfo) -> date:
    """
    Default: previous calendar day in reporting.timezone (run today → report yesterday).

    Override with reporting.summary_date_offset_days (0 = today, -1 = yesterday).
    """
    offset_days = int(reporting.get("summary_date_offset_days", -1))
    local_today = datetime.now(tz).date()
    return local_today + timedelta(days=offset_days)


def _fmt_money(amount: float, *, signed: bool = True) -> str:
    if signed:
        sign = "+" if amount >= 0 else "-"
        return f"${sign}{abs(amount):,.2f}"
    return f"${amount:,.2f}"


def _fmt_coins(amount: float) -> str:
    return f"{amount:,.2f}"


def _fmt_rate(rate: float) -> str:
    return f"${rate:.4f}"


def _fmt_report_date(d: date) -> str:
    return d.strftime("%d-%m-%Y")


def calculate_average_entry_from_stored_orders(
    cointype: str,
    current_balance: float,
    *,
    state_dir: str | Path,
) -> tuple[float, float, float]:
    """
    Average entry from unconsumed buy lots (same scaling as legacy daily summary).

    Returns (avg_entry, fifo_coins, total_cost).
    """
    path = Path(state_dir) / f"filled_orders_{cointype.upper()}.json"
    if not path.is_file():
        return (0.0, 0.0, 0.0)

    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    total_cost = 0.0
    total_amount = 0.0
    for row in data.get("buy_orders") or []:
        if row.get("fully_consumed"):
            continue
        amount = float(row.get("amount", 0))
        consumed = float(row.get("consumed_amount", 0))
        remaining = amount - consumed
        rate = float(row.get("rate", 0))
        if remaining > 0 and rate > 0:
            total_cost += remaining * rate
            total_amount += remaining

    if total_amount <= 0:
        return (0.0, 0.0, 0.0)

    avg = total_cost / total_amount
    if current_balance > 0 and total_amount > 0:
        scaled_cost = total_cost * (current_balance / total_amount)
    else:
        scaled_cost = total_cost
    return (avg, total_amount, scaled_cost)


def _active_lifo_queue(buy_orders: list[dict]) -> list[dict]:
    queue: list[dict] = []
    for row in buy_orders:
        if row.get("fully_consumed"):
            continue
        amount = float(row.get("amount", 0))
        consumed = float(row.get("consumed_amount", 0))
        remaining = amount - consumed
        if remaining > 0.0001:
            queue.append(
                {
                    "rate": float(row.get("rate", 0)),
                    "remaining": remaining,
                    "fill_timestamp": row.get("fill_timestamp", ""),
                }
            )
    queue.sort(key=lambda x: x.get("fill_timestamp", ""), reverse=True)
    return queue


def _lifo_match_profit(
    buy_queue: list[dict],
    sell_amount: float,
    sell_rate: float,
    buy_fee: float,
    sell_fee: float,
) -> tuple[float, float]:
    """Return (net_profit, cost_basis) mutating buy_queue remaining."""
    amount_to_match = sell_amount
    total_cost_basis = 0.0
    for lot in buy_queue:
        if amount_to_match <= 0:
            break
        remaining = float(lot.get("remaining", 0))
        if remaining <= 0:
            continue
        consume = min(remaining, amount_to_match)
        total_cost_basis += consume * float(lot.get("rate", 0))
        lot["remaining"] = remaining - consume
        amount_to_match -= consume
    if amount_to_match > 0.0001:
        total_cost_basis += amount_to_match * sell_rate

    proceeds = sell_amount * sell_rate
    net = proceeds - total_cost_basis - total_cost_basis * buy_fee - proceeds * sell_fee
    return (net, total_cost_basis)


def _apply_sell_consumption(buy_orders: list[dict], sell: dict) -> None:
    """Apply LIFO consumption to buy_orders copy (mirrors OrderManager save path)."""
    amount_to_consume = float(sell.get("amount", 0))
    sell_ts = sell.get("fill_timestamp") or ""

    def sort_key(row: dict) -> str:
        ts = row.get("fill_timestamp") or ""
        return ts if _parse_ts(ts) else "1970-01-01T00:00:00"

    sorted_buys = sorted(buy_orders, key=sort_key, reverse=True)
    for buy in sorted_buys:
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
            amount_to_consume -= remaining
        else:
            buy["consumed_amount"] = already + amount_to_consume
            amount_to_consume = 0


def _replay_sell_profits(
    buy_orders: list[dict],
    sell_orders: list[dict],
    buy_fee: float,
    sell_fee: float,
) -> list[tuple[dict, float]]:
    """Chronological realized profit per sell (rebuild consumption from scratch)."""
    buys = copy.deepcopy(buy_orders)
    for row in buys:
        row["consumed_amount"] = 0.0
        row.pop("fully_consumed", None)

    results: list[tuple[dict, float]] = []
    for sell in sorted(sell_orders, key=lambda s: s.get("fill_timestamp") or ""):
        queue = _active_lifo_queue(buys)
        profit, _ = _lifo_match_profit(
            queue,
            float(sell.get("amount", 0)),
            float(sell.get("rate", 0)),
            buy_fee,
            sell_fee,
        )
        results.append((sell, profit))
        _apply_sell_consumption(buys, sell)
    return results


def _activity_stats(
    orders: list[dict], report_date: date, tz: ZoneInfo
) -> tuple[int, float, float]:
    """Count, total coins, volume-weighted avg rate for fills on report_date (local tz)."""
    day_orders = [
        o for o in orders if _fill_date_in_tz(o.get("fill_timestamp", ""), tz) == report_date
    ]
    count = len(day_orders)
    if not count:
        return (0, 0.0, 0.0)
    coins = sum(float(o.get("amount", 0)) for o in day_orders)
    notional = sum(float(o.get("amount", 0)) * float(o.get("rate", 0)) for o in day_orders)
    avg_rate = notional / coins if coins > 0 else 0.0
    return (count, coins, avg_rate)


def _coin_balance_from_buys(buy_orders: list[dict]) -> float:
    total = 0.0
    for row in buy_orders:
        if row.get("fully_consumed"):
            continue
        amount = float(row.get("amount", 0))
        consumed = float(row.get("consumed_amount", 0))
        total += amount - consumed
    return total


def _quote_balance_from_ledger(
    buy_orders: list[dict],
    sell_orders: list[dict],
    start_wallet: float,
) -> float:
    usdc = float(start_wallet)
    for row in buy_orders:
        usdc -= float(row.get("amount", 0)) * float(row.get("rate", 0))
    for row in sell_orders:
        usdc += float(row.get("amount", 0)) * float(row.get("rate", 0))
    return max(usdc, 0.0)


def _is_seed_ledger_row(row: dict) -> bool:
    """Dry-run / test seed lots are not live trading history."""
    oid = str(row.get("order_id") or "")
    if oid.startswith("seed-"):
        return True
    return bool(row.get("is_seed"))


def _trading_days(first: Optional[date], last: date) -> int:
    if not first:
        return 1
    return max(1, (last - first).days + 1)


def _first_activity_date(
    buys: list[dict], sells: list[dict], tz: ZoneInfo
) -> Optional[date]:
    """First live fill date (excludes seed lots), in reporting timezone."""
    dates: list[date] = []
    for row in buys + sells:
        if _is_seed_ledger_row(row):
            continue
        d = _fill_date_in_tz(row.get("fill_timestamp", ""), tz)
        if d:
            dates.append(d)
    return min(dates) if dates else None


def _fetch_closing_price(pair: str, exchange_name: str = "hyperliquid") -> Optional[float]:
    try:
        import ccxt

        ex_cls = getattr(ccxt, exchange_name, None)
        if ex_cls is None:
            return None
        ex = ex_cls({"enableRateLimit": True})
        ticker = ex.fetch_ticker(pair)
        for key in ("last", "close", "bid", "ask"):
            val = ticker.get(key)
            if val:
                return float(val)
    except Exception as e:
        logger.debug("Market price fetch failed for %s: %s", pair, e)
    return None


def _price_from_state(state_dir: Path, cointype: str) -> Optional[float]:
    path = state_dir / f"price_high_{cointype.upper()}.json"
    if not path.is_file():
        return None
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        history = data.get("price_history") or []
        if history:
            return float(history[-1].get("price", 0)) or None
        for key in ("rolling_high", "rolling_mean", "rolling_low"):
            if data.get(key):
                return float(data[key])
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        pass
    return None


def _open_sell_orders(state_dir: Path, cointype: str, pair: str) -> list[dict]:
    path = state_dir / f"dry_run_orders_{cointype.upper()}.json"
    if not path.is_file():
        return []
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        out = []
        for order in data.get("orders") or []:
            if (order.get("status") or "open") != "open":
                continue
            if order.get("symbol") and order.get("symbol") != pair:
                continue
            if str(order.get("side", "")).lower() != "sell":
                continue
            out.append(order)
        return out
    except (OSError, json.JSONDecodeError):
        return []


def _load_working_order_ids(state_dir: Path, cointype: str) -> set[str]:
    path = state_dir / f"working_orders_{cointype.upper()}.json"
    if not path.is_file():
        return set()
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        ids = data.get("working_order_ids") or data.get("order_ids") or []
        return {str(i) for i in ids}
    except (OSError, json.JSONDecodeError, TypeError):
        return set()


def _projected_realized_profit(
    buy_orders: list[dict],
    open_sells: list[dict],
    avg_entry: float,
    mean_reversion: bool,
    working_order_ids: set[str],
    buy_fee: float,
    sell_fee: float,
) -> float:
    if not open_sells or avg_entry <= 0:
        if not open_sells:
            return 0.0

    queue = _active_lifo_queue(copy.deepcopy(buy_orders))
    total = 0.0
    for order in sorted(open_sells, key=lambda o: float(o.get("price") or o.get("rate") or 0)):
        amount = float(order.get("amount") or order.get("remaining") or 0)
        rate = float(order.get("price") or order.get("rate") or 0)
        if amount <= 0 or rate <= 0:
            continue
        order_id = str(order.get("id") or order.get("order_id") or "")
        is_working = order_id in working_order_ids
        if mean_reversion and is_working:
            profit, _ = _lifo_match_profit(queue, amount, rate, buy_fee, sell_fee)
        else:
            cost = amount * avg_entry
            proceeds = amount * rate
            profit = proceeds - cost - cost * buy_fee - proceeds * sell_fee
        total += profit
    return total


def _load_skim_data(state_dir: Path, cointype: str) -> dict[str, Any]:
    path = state_dir / f"skim_purchases_{cointype.upper()}.json"
    if not path.is_file():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def _skim_totals(skim_data: dict, report_date: date) -> tuple[dict[str, float], dict[str, float], float]:
    """Return (today_amounts_by_coin, alltime_amounts_by_coin, alltime_cost)."""
    daily = skim_data.get("daily_purchases") or {}
    today_key = report_date.isoformat()
    today_amounts: dict[str, float] = {}
    alltime_amounts: dict[str, float] = {}
    alltime_cost = 0.0

    for day, tokens in daily.items():
        if not isinstance(tokens, dict):
            continue
        for coin, row in tokens.items():
            if not isinstance(row, dict):
                continue
            amt = float(row.get("amount", 0))
            cost = float(row.get("total_cost", 0))
            alltime_amounts[coin] = alltime_amounts.get(coin, 0.0) + amt
            alltime_cost += cost
            if day == today_key:
                today_amounts[coin] = today_amounts.get(coin, 0.0) + amt
    return today_amounts, alltime_amounts, alltime_cost


@dataclass
class DailySummaryReport:
    symbol: str
    market: str
    cointype: str
    report_date: date
    buy_count: int = 0
    buy_coins: float = 0.0
    buy_avg_rate: float = 0.0
    sell_count: int = 0
    sell_coins: float = 0.0
    sell_avg_rate: float = 0.0
    realized_today: float = 0.0
    cumulative_profit: float = 0.0
    trading_days: int = 1
    coin_balance: float = 0.0
    coin_value: float = 0.0
    unrealized_pnl: float = 0.0
    projected_profit: float = 0.0
    quote_balance: float = 0.0
    total_portfolio: float = 0.0
    closing_price: float = 0.0
    average_entry: float = 0.0
    skim_enabled: bool = False
    skim_today: dict[str, float] = field(default_factory=dict)
    skim_alltime: dict[str, float] = field(default_factory=dict)
    skim_alltime_cost: float = 0.0


def build_daily_summary(
    ladder_cfg: dict[str, Any],
    ft_cfg: dict[str, Any],
    *,
    report_date: Optional[date] = None,
    user_data_dir: Optional[Path] = None,
) -> DailySummaryReport:
    """Assemble daily summary metrics from ladder state + config."""
    user_data = user_data_dir or Path(__file__).resolve().parents[1]
    state_rel = ladder_cfg.get("paths", {}).get("state_dir", "spot_ladder/state")
    state_dir = (user_data / state_rel).resolve()

    trading = ladder_cfg.get("trading") or {}
    symbol = trading.get("symbols", ["XRP/USDC"])[0]
    market = trading.get("base_currency", "USDC").upper()
    cointype = symbol.split("/")[0].upper()
    ccxt_pairs = ladder_cfg.get("ccxt_pairs") or {}
    ccxt_pair = ccxt_pairs.get(symbol)
    if not ccxt_pair:
        whitelist = (ft_cfg.get("exchange") or {}).get("pair_whitelist") or []
        ccxt_pair = whitelist[0] if whitelist else f"{cointype}/{market}:USDC"

    buy_fee = float(trading.get("buy_fee_percentage", 0.0004))
    sell_fee = float(trading.get("sell_fee_percentage", 0.0004))
    mean_reversion = bool((trading.get("mean_reversion") or {}).get("enabled", False))
    skim_enabled = bool((ladder_cfg.get("skim") or {}).get("enabled", False))
    reporting = ladder_cfg.get("reporting") or {}
    tz = _report_timezone(reporting)

    if report_date is None:
        report_date = _resolve_report_date(reporting, tz)

    filled_path = state_dir / f"filled_orders_{cointype}.json"

    from spot_ladder.ledger_sell_sync import (  # noqa: WPS433
        refresh_sell_fill_timestamps_from_dry_store,
        sync_missing_sells_from_dry_run_store,
    )

    if filled_path.is_file():
        refresh_sell_fill_timestamps_from_dry_store(
            filled_path, state_dir, cointype, ccxt_pair
        )
        sync_missing_sells_from_dry_run_store(
            filled_path,
            state_dir,
            cointype,
            ccxt_pair,
            symbol=symbol,
            market=market,
        )

    buys: list[dict] = []
    sells: list[dict] = []
    if filled_path.is_file():
        with open(filled_path, encoding="utf-8") as f:
            data = json.load(f)
        buys = data.get("buy_orders") or []
        sells = data.get("sell_orders") or []

    start_wallet = float(ft_cfg.get("dry_run_wallet", 1000))
    coin_balance = _coin_balance_from_buys(buys)
    quote_balance = _quote_balance_from_ledger(buys, sells, start_wallet)

    avg_entry, _, cost_basis = calculate_average_entry_from_stored_orders(
        cointype, coin_balance, state_dir=state_dir
    )

    closing = _fetch_closing_price(ccxt_pair, ft_cfg.get("exchange", {}).get("name", "hyperliquid"))
    if not closing:
        closing = _price_from_state(state_dir, cointype) or 0.0

    coin_value = coin_balance * closing if closing > 0 else 0.0
    unrealized = coin_value - cost_basis if cost_basis > 0 else 0.0
    total_portfolio = quote_balance + coin_value

    sell_profits = _replay_sell_profits(buys, sells, buy_fee, sell_fee)
    cumulative = sum(p for _, p in sell_profits)
    realized_today = sum(
        p
        for sell, p in sell_profits
        if _fill_date_in_tz(sell.get("fill_timestamp", ""), tz) == report_date
    )

    buy_count, buy_coins, buy_avg = _activity_stats(buys, report_date, tz)
    sell_count, sell_coins, sell_avg = _activity_stats(sells, report_date, tz)

    first_day = _first_activity_date(buys, sells, tz)
    trading_days = _trading_days(first_day, report_date)

    open_sells = _open_sell_orders(state_dir, cointype, ccxt_pair)
    working_ids = _load_working_order_ids(state_dir, cointype)
    projected = _projected_realized_profit(
        buys, open_sells, avg_entry, mean_reversion, working_ids, buy_fee, sell_fee
    )

    skim_today: dict[str, float] = {}
    skim_alltime: dict[str, float] = {}
    skim_cost = 0.0
    if skim_enabled:
        skim_data = _load_skim_data(state_dir, cointype)
        skim_today, skim_alltime, skim_cost = _skim_totals(skim_data, report_date)

    return DailySummaryReport(
        symbol=symbol,
        market=market,
        cointype=cointype,
        report_date=report_date,
        buy_count=buy_count,
        buy_coins=buy_coins,
        buy_avg_rate=buy_avg,
        sell_count=sell_count,
        sell_coins=sell_coins,
        sell_avg_rate=sell_avg,
        realized_today=realized_today,
        cumulative_profit=cumulative,
        trading_days=trading_days,
        coin_balance=coin_balance,
        coin_value=coin_value,
        unrealized_pnl=unrealized,
        projected_profit=projected,
        quote_balance=quote_balance,
        total_portfolio=total_portfolio,
        closing_price=closing,
        average_entry=avg_entry,
        skim_enabled=skim_enabled,
        skim_today=skim_today,
        skim_alltime=skim_alltime,
        skim_alltime_cost=skim_cost,
    )


def format_daily_summary(report: DailySummaryReport) -> str:
    """Format like legacy Telegram daily summary."""
    lines = [
        f"*DAILY SUMMARY - {report.symbol}*",
        f"Date: {_fmt_report_date(report.report_date)}",
        "",
        "*Trading Activity:*",
    ]

    if report.buy_count:
        lines.append(
            f"• Buys: {report.buy_count} ({_fmt_coins(report.buy_coins)} coins @ {_fmt_rate(report.buy_avg_rate)})"
        )
    else:
        lines.append("• Buys: 0")

    if report.sell_count:
        lines.append(
            f"• Sells: {report.sell_count} ({_fmt_coins(report.sell_coins)} coins @ {_fmt_rate(report.sell_avg_rate)})"
        )
    else:
        lines.append("• Sells: 0")

    daily_avg = report.cumulative_profit / report.trading_days if report.trading_days else 0.0

    lines.extend(
        [
            "",
            "*Today's Profit:*",
            f"• Realized: {_fmt_money(report.realized_today)}",
            f"• Cumulative: {_fmt_money(report.cumulative_profit)}",
            f"• Daily Average: {_fmt_money(daily_avg)} ({report.trading_days} days)",
            "",
            "*Coin Holdings:*",
            f"• Balance: {_fmt_coins(report.coin_balance)} coins",
            f"• Value: {_fmt_money(report.coin_value, signed=False)}",
            f"• Unrealized P&L: {_fmt_money(report.unrealized_pnl)}",
            f"• Projected Realized Profit: {_fmt_money(report.projected_profit)}",
            "",
            "*Account Funds:*",
            f"• {report.market} Balance: {_fmt_money(report.quote_balance, signed=False)}",
            f"• Total Portfolio: {_fmt_money(report.total_portfolio, signed=False)}",
            "",
            f"*Closing Price:* {_fmt_rate(report.closing_price)}",
            f"*Average Entry:* {_fmt_rate(report.average_entry)}",
        ]
    )

    if report.skim_enabled:
        lines.extend(["", "*SKIM TOKENS*"])
        if report.skim_alltime:
            parts = [f"{amt:,.4f} {coin}" for coin, amt in sorted(report.skim_alltime.items()) if amt > 0]
            skim_line = ", ".join(parts) if parts else "0"
            lines.append(f"• Total Skim (all-time): {skim_line}")
        else:
            lines.append("• Total Skim (all-time): 0")
        lines.append(f"• Cost: {_fmt_money(report.skim_alltime_cost, signed=False)}")

    return "\n".join(lines)
