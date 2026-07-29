#!/usr/bin/env python3
"""
Post daily ladder summary to Slack (run from cron inside the freqtrade container).

Example (UTC midnight):
  docker compose exec -T freqtrade python /freqtrade/user_data/spot_ladder/daily_summary_slack.py
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

USER_DATA = Path(__file__).resolve().parents[1]
if str(USER_DATA) not in sys.path:
    sys.path.insert(0, str(USER_DATA))

from spot_ladder.notifier_factory import create_ladder_notifier  # noqa: E402


def _load_ft_config() -> dict:
    cfg_path = USER_DATA / "config.json"
    with open(cfg_path, encoding="utf-8") as f:
        return json.load(f)


def _load_ladder_config() -> dict:
    path = USER_DATA / "spot_ladder" / "config.yaml"
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _summary_from_state(ladder_cfg: dict, symbol: str) -> dict:
    state_rel = ladder_cfg.get("paths", {}).get("state_dir", "spot_ladder/state")
    state_dir = (USER_DATA / state_rel).resolve()
    coin = symbol.split("/")[0].upper()
    filled_path = state_dir / f"filled_orders_{coin}.json"
    summary: dict = {"date_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}

    if not filled_path.is_file():
        summary["note"] = f"No state file {filled_path.name}"
        return summary

    with open(filled_path, encoding="utf-8") as f:
        data = json.load(f)

    buys = data.get("buy_orders") or []
    sells = data.get("sell_orders") or []
    total_coins = sum(float(b.get("amount", 0)) - float(b.get("consumed_amount", 0)) for b in buys)
    invested = sum(float(b.get("amount", 0)) * float(b.get("rate", 0)) for b in buys)
    summary["open_lot_coins"] = f"{total_coins:.4f}"
    summary["cost_basis_usdc"] = f"{invested:.2f}"
    summary["buy_lots"] = len(buys)
    summary["sell_fills_recorded"] = len(sells)
    if data.get("last_updated"):
        summary["state_last_updated"] = data["last_updated"]
    return summary


def main() -> int:
    reporting = (_load_ladder_config().get("reporting") or {})
    if not reporting.get("enabled", False):
        print("reporting.enabled is false in spot_ladder/config.yaml — exit 0")
        return 0

    ft_cfg = _load_ft_config()
    ladder_cfg = _load_ladder_config()
    notifier = create_ladder_notifier(ladder_cfg, ft_cfg)
    symbol = ladder_cfg["trading"]["symbols"][0]
    summary = _summary_from_state(ladder_cfg, symbol)
    notifier.notify_daily_summary(summary, symbol=symbol)
    print("Daily summary sent (if Slack webhook configured).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
