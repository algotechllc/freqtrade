#!/usr/bin/env python3
"""
Seed price_high_{COIN}.json with 180 days of history for price_elevation.

Without this, elevation stays neutral (100% deploy) until live samples accumulate,
so a high-price flatten would still fill the whole buy ladder.

Uses Binance XRPUSDT 4h closes (USD-pegged; close enough to Hyperliquid XRP/USDC
for percentile ranking). Optional --source hyperliquid uses CCXT if available.

Run on the server (inside the freqtrade container):

  docker compose exec -T freqtrade python /freqtrade/user_data/spot_ladder/fetch_historical_prices.py --days 180

Preview only:

  docker compose exec -T freqtrade python /freqtrade/user_data/spot_ladder/fetch_historical_prices.py --days 180 --dry-run
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple

import yaml

USER_DATA = Path(__file__).resolve().parents[1]
BINANCE_KLINES = "https://api.binance.com/api/v3/klines"


def _load_ladder_config() -> dict:
    with open(USER_DATA / "spot_ladder" / "config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _fetch_binance_4h(symbol: str, days: int) -> List[Tuple[float, float]]:
    """Return (unix_seconds, close) for `days` of 4h candles."""
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - days * 24 * 3600 * 1000
    out: List[Tuple[float, float]] = []
    cursor = start_ms
    while cursor < end_ms:
        params = urllib.parse.urlencode({
            "symbol": symbol,
            "interval": "4h",
            "startTime": cursor,
            "endTime": end_ms,
            "limit": 1000,
        })
        url = f"{BINANCE_KLINES}?{params}"
        with urllib.request.urlopen(url, timeout=30) as resp:
            rows = json.loads(resp.read().decode())
        if not rows:
            break
        for row in rows:
            ts_ms = int(row[0])
            close = float(row[4])
            out.append((ts_ms / 1000.0, close))
        last_open = int(rows[-1][0])
        nxt = last_open + 4 * 3600 * 1000
        if nxt <= cursor:
            break
        cursor = nxt
        if len(rows) < 1000:
            break
        time.sleep(0.2)
    # Deduplicate by timestamp, keep last
    by_ts = {t: p for t, p in out}
    return sorted(by_ts.items())


def _fetch_hyperliquid(ccxt_pair: str, days: int) -> List[Tuple[float, float]]:
    import ccxt  # type: ignore

    ex = ccxt.hyperliquid({"enableRateLimit": True})
    since = int((time.time() - days * 24 * 3600) * 1000)
    candles = []
    cursor = since
    while True:
        batch = ex.fetch_ohlcv(ccxt_pair, timeframe="4h", since=cursor, limit=500)
        if not batch:
            break
        candles.extend(batch)
        nxt = int(batch[-1][0]) + 4 * 3600 * 1000
        if nxt <= cursor or len(batch) < 2:
            break
        cursor = nxt
        if cursor > int(time.time() * 1000):
            break
    by_ts = {int(c[0]) / 1000.0: float(c[4]) for c in candles}
    return sorted(by_ts.items())


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed price_elevation history JSON")
    parser.add_argument("--days", type=int, default=180)
    parser.add_argument("--coin", default="")
    parser.add_argument("--source", choices=("binance", "hyperliquid"), default="binance")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    cfg = _load_ladder_config()
    trading = cfg.get("trading") or {}
    symbol = trading.get("symbols", ["XRP/USDC"])[0]
    cointype = (args.coin or symbol.split("/")[0]).upper()
    pe = cfg.get("price_elevation") or {}
    window_hours = int(pe.get("window_hours") or args.days * 24)
    days = max(args.days, int(window_hours / 24))

    state_rel = (cfg.get("paths") or {}).get("state_dir", "spot_ladder/state")
    state_dir = (USER_DATA / state_rel).resolve()
    state_dir.mkdir(parents=True, exist_ok=True)
    out_path = state_dir / f"price_high_{cointype}.json"

    ccxt_pair = (cfg.get("ccxt_pairs") or {}).get(symbol, f"{cointype}/USDC:USDC")
    binance_symbol = f"{cointype}USDT"

    print(f"Fetching {days}d of {cointype} closes from {args.source}…")
    if args.source == "hyperliquid":
        prices = _fetch_hyperliquid(ccxt_pair, days)
        source_label = f"Hyperliquid CCXT {ccxt_pair} 4h"
    else:
        prices = _fetch_binance_4h(binance_symbol, days)
        source_label = f"Binance {binance_symbol} 4h (USD-pegged proxy for USDC)"

    if len(prices) < 10:
        print(f"Too few samples ({len(prices)}). Refusing to write.", file=sys.stderr)
        return 1

    cutoff = time.time() - window_hours * 3600
    entries = [{"timestamp": t, "price": p} for t, p in prices if t >= cutoff]
    if len(entries) < 10:
        entries = [{"timestamp": t, "price": p} for t, p in prices]

    highs = [e["price"] for e in entries]
    rolling_high = max(highs)
    rolling_low = min(highs)
    rolling_mean = sum(highs) / len(highs)
    span_days = (entries[-1]["timestamp"] - entries[0]["timestamp"]) / 86400.0
    now = datetime.now(timezone.utc).isoformat()

    data = {
        "symbol": f"{cointype}/USDC",
        "last_updated": now,
        "rolling_high": rolling_high,
        "rolling_low": rolling_low,
        "rolling_mean": rolling_mean,
        "window_hours": window_hours,
        "price_history": entries,
        "seeded": True,
        "seeded_at": now,
        "data_source": source_label,
        "real_historical_data": True,
    }

    print(f"  samples: {len(entries)} over {span_days:.0f}d")
    print(f"  range: ${rolling_low:.4f} – ${rolling_high:.4f} (mean ${rolling_mean:.4f})")
    print(f"  file: {out_path}")

    if args.dry_run:
        print("[dry-run] not writing")
        return 0

    if out_path.exists():
        backup = out_path.with_name(f"{out_path.stem}_backup_{int(time.time())}.json")
        backup.write_text(out_path.read_text(encoding="utf-8"), encoding="utf-8")
        print(f"  backed up existing → {backup.name}")

    out_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print("wrote seed file")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
