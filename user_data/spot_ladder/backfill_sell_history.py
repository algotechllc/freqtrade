#!/usr/bin/env python3
"""
Rebuild full sell history in filled_orders_{COIN}.json from dry-run order store + ledger.

Merges all closed sells from dry_run_orders_{COIN}.json with existing sell_orders,
resets LIFO consumption on buy lots, replays sells chronologically, and writes the ledger.
Creates a timestamped backup before modifying the file.

Run on the server (inside the freqtrade container):

  docker compose exec -T freqtrade python /freqtrade/user_data/spot_ladder/backfill_sell_history.py

Preview only (no writes):

  docker compose exec -T freqtrade python /freqtrade/user_data/spot_ladder/backfill_sell_history.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

USER_DATA = Path(__file__).resolve().parents[1]
if str(USER_DATA) not in sys.path:
    sys.path.insert(0, str(USER_DATA))

from spot_ladder.ledger_sell_sync import rebuild_full_sell_history  # noqa: E402


def _load_ladder_config() -> dict:
    with open(USER_DATA / "spot_ladder" / "config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _load_ft_config() -> dict:
    with open(USER_DATA / "config.json", encoding="utf-8") as f:
        cfg = json.load(f)
    private = USER_DATA / "config-private.json"
    if private.is_file():
        with open(private, encoding="utf-8") as f:
            extra = json.load(f)
        if isinstance(extra, dict):
            for key, value in extra.items():
                if isinstance(value, dict) and isinstance(cfg.get(key), dict):
                    cfg[key] = {**cfg[key], **value}
                else:
                    cfg[key] = value
    return cfg


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill sell history into filled_orders JSON")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print summary only; do not modify filled_orders JSON",
    )
    args = parser.parse_args()

    ladder_cfg = _load_ladder_config()
    ft_cfg = _load_ft_config()
    trading = ladder_cfg.get("trading") or {}
    symbol = trading.get("symbols", ["XRP/USDC"])[0]
    market = trading.get("base_currency", "USDC").upper()
    cointype = symbol.split("/")[0].upper()

    state_rel = ladder_cfg.get("paths", {}).get("state_dir", "spot_ladder/state")
    state_dir = (USER_DATA / state_rel).resolve()
    ccxt_pair = (ladder_cfg.get("ccxt_pairs") or {}).get(symbol)
    if not ccxt_pair:
        whitelist = (ft_cfg.get("exchange") or {}).get("pair_whitelist") or []
        ccxt_pair = whitelist[0] if whitelist else f"{cointype}/{market}:USDC"

    filled_path = state_dir / f"filled_orders_{cointype}.json"

    try:
        summary = rebuild_full_sell_history(
            filled_path,
            state_dir,
            cointype,
            ccxt_pair,
            symbol=symbol,
            market=market,
            dry_run=args.dry_run,
        )
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    print("Sell history backfill summary:")
    for key, value in summary.items():
        print(f"  {key}: {value}")

    if args.dry_run:
        print("\nDry-run — no files modified.")
    else:
        print(f"\nUpdated {filled_path}")
        print(f"Backup: {summary.get('backup')}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
