#!/usr/bin/env python3
"""
Post daily ladder summary to Slack (run from cron inside the freqtrade container).

Example (UTC midnight):
  docker compose exec -T freqtrade python /freqtrade/user_data/spot_ladder/daily_summary_slack.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import yaml

USER_DATA = Path(__file__).resolve().parents[1]
if str(USER_DATA) not in sys.path:
    sys.path.insert(0, str(USER_DATA))

from spot_ladder.daily_summary import build_daily_summary, format_daily_summary  # noqa: E402
from spot_ladder.notifier_factory import create_ladder_notifier  # noqa: E402


def _load_ft_config() -> dict:
    cfg_path = USER_DATA / "config.json"
    with open(cfg_path, encoding="utf-8") as f:
        cfg = json.load(f)
    private = USER_DATA / "config-private.json"
    if private.is_file():
        with open(private, encoding="utf-8") as f:
            private_cfg = json.load(f)
        if isinstance(private_cfg, dict):
            for key, value in private_cfg.items():
                if isinstance(value, dict) and isinstance(cfg.get(key), dict):
                    cfg[key] = {**cfg[key], **value}
                else:
                    cfg[key] = value
    return cfg


def _load_ladder_config() -> dict:
    path = USER_DATA / "spot_ladder" / "config.yaml"
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def main() -> int:
    reporting = (_load_ladder_config().get("reporting") or {})
    if not reporting.get("enabled", False):
        print("reporting.enabled is false in spot_ladder/config.yaml — exit 0")
        return 0

    ft_cfg = _load_ft_config()
    ladder_cfg = _load_ladder_config()
    notifier = create_ladder_notifier(ladder_cfg, ft_cfg)
    symbol = ladder_cfg["trading"]["symbols"][0]
    report = build_daily_summary(ladder_cfg, ft_cfg, user_data_dir=USER_DATA)
    text = format_daily_summary(report)
    notifier.notify_daily_summary({"text": text}, symbol=symbol)
    print("Daily summary sent (if Slack webhook configured).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
