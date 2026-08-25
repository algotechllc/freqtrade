#!/usr/bin/env python3
"""
Seed price_high_{COIN}.json with 180 days of history for price_elevation.

Without this, elevation stays neutral (100% deploy) until live samples accumulate,
so a high-price flatten would still fill the whole buy ladder.

Default source is Hyperliquid (same venue as the bot). Other public APIs are tried
if that fails — Binance often returns HTTP 451 from restricted regions.

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
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, List, Optional, Tuple

import yaml

USER_DATA = Path(__file__).resolve().parents[1]
UA = {"User-Agent": "spot-ladder-seed/1.0"}

PriceSeries = List[Tuple[float, float]]


def _load_ladder_config() -> dict:
    with open(USER_DATA / "spot_ladder" / "config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _http_json(url: str, data: bytes | None = None, headers: dict | None = None, timeout: int = 30):
    hdrs = {**UA, **(headers or {})}
    req = urllib.request.Request(url, data=data, headers=hdrs)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def _dedupe(rows: PriceSeries) -> PriceSeries:
    return sorted({t: p for t, p in rows}.items())


def _fetch_hyperliquid(coin: str, days: int) -> PriceSeries:
    """Native Hyperliquid candleSnapshot (perp coin id, e.g. XRP)."""
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - days * 24 * 3600 * 1000
    payload = json.dumps({
        "type": "candleSnapshot",
        "req": {
            "coin": coin,
            "interval": "4h",
            "startTime": start_ms,
            "endTime": end_ms,
        },
    }).encode()
    rows = _http_json(
        "https://api.hyperliquid.xyz/info",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    if not isinstance(rows, list):
        raise RuntimeError(f"unexpected Hyperliquid response: {type(rows).__name__}")
    out: PriceSeries = []
    for row in rows:
        ts_ms = int(row.get("t") or 0)
        close = float(row.get("c") or 0)
        if ts_ms > 0 and close > 0:
            out.append((ts_ms / 1000.0, close))
    return _dedupe(out)


def _fetch_kraken(coin: str, days: int) -> PriceSeries:
    pair = f"{coin}USD"
    url = "https://api.kraken.com/0/public/OHLC?" + urllib.parse.urlencode({
        "pair": pair,
        "interval": 240,
    })
    data = _http_json(url)
    errors = data.get("error") or []
    if errors:
        raise RuntimeError(f"Kraken error: {errors}")
    result = data.get("result") or {}
    key = next((k for k in result if k != "last"), None)
    if not key:
        raise RuntimeError("Kraken: no OHLC series")
    cutoff = time.time() - days * 24 * 3600
    out: PriceSeries = []
    for row in result[key]:
        ts = float(row[0])
        close = float(row[4])
        if ts >= cutoff and close > 0:
            out.append((ts, close))
    return _dedupe(out)


def _fetch_bybit(coin: str, days: int) -> PriceSeries:
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - days * 24 * 3600 * 1000
    out: PriceSeries = []
    page_end = end_ms
    for _ in range(4):
        params = urllib.parse.urlencode({
            "category": "linear",
            "symbol": f"{coin}USDT",
            "interval": "240",
            "start": start_ms,
            "end": page_end,
            "limit": 1000,
        })
        data = _http_json(f"https://api.bybit.com/v5/market/kline?{params}")
        if int(data.get("retCode") or 0) != 0:
            raise RuntimeError(f"Bybit error: {data.get('retMsg')}")
        rows = ((data.get("result") or {}).get("list") or [])
        if not rows:
            break
        for row in rows:
            ts_ms = int(row[0])
            close = float(row[4])
            if close > 0:
                out.append((ts_ms / 1000.0, close))
        oldest_ms = min(int(r[0]) for r in rows)
        if oldest_ms <= start_ms or len(rows) < 1000:
            break
        page_end = oldest_ms - 1
        time.sleep(0.15)
    return _dedupe(out)


def _fetch_binance(coin: str, days: int) -> PriceSeries:
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - days * 24 * 3600 * 1000
    out: PriceSeries = []
    cursor = start_ms
    while cursor < end_ms:
        params = urllib.parse.urlencode({
            "symbol": f"{coin}USDT",
            "interval": "4h",
            "startTime": cursor,
            "endTime": end_ms,
            "limit": 1000,
        })
        rows = _http_json(f"https://api.binance.com/api/v3/klines?{params}")
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
    return _dedupe(out)


SOURCES: dict[str, Callable[[str, int], PriceSeries]] = {
    "hyperliquid": _fetch_hyperliquid,
    "kraken": _fetch_kraken,
    "bybit": _fetch_bybit,
    "binance": _fetch_binance,
}
AUTO_ORDER = ("hyperliquid", "kraken", "bybit", "binance")


def _try_source(name: str, coin: str, days: int) -> Optional[PriceSeries]:
    fetch = SOURCES[name]
    print(f"Fetching {days}d of {coin} 4h closes from {name}…")
    try:
        prices = fetch(coin, days)
    except urllib.error.HTTPError as e:
        hint = " (geo-blocked — trying next source)" if e.code == 451 else ""
        print(f"  {name} failed: HTTP {e.code}{hint}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"  {name} failed: {e}", file=sys.stderr)
        return None
    if len(prices) < 10:
        print(f"  {name} returned only {len(prices)} samples — trying next source", file=sys.stderr)
        return None
    return prices


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed price_elevation history JSON")
    parser.add_argument("--days", type=int, default=180)
    parser.add_argument("--coin", default="")
    parser.add_argument(
        "--source",
        choices=("auto", *SOURCES.keys()),
        default="auto",
        help="auto tries Hyperliquid first, then Kraken/Bybit/Binance",
    )
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

    names = AUTO_ORDER if args.source == "auto" else (args.source,)
    prices: PriceSeries = []
    source_used = ""
    for name in names:
        got = _try_source(name, cointype, days)
        if got:
            prices = got
            source_used = name
            break

    if len(prices) < 10:
        print("All sources failed. Refusing to write.", file=sys.stderr)
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
    labels = {
        "hyperliquid": f"Hyperliquid {cointype} perp 4h",
        "kraken": f"Kraken {cointype}USD 4h",
        "bybit": f"Bybit {cointype}USDT perp 4h",
        "binance": f"Binance {cointype}USDT 4h",
    }

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
        "data_source": labels.get(source_used, source_used),
        "real_historical_data": True,
    }

    print(f"  source: {source_used}")
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
