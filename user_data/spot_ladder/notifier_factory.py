"""Build ladder notifier from spot_ladder/config.yaml (+ env secrets)."""

from __future__ import annotations

import logging
import os
from typing import Any

from spot_ladder.slack_notifier import SlackNotifier
from spot_ladder.telegram_notifier import TelegramNotifier

logger = logging.getLogger(__name__)


def create_ladder_notifier(ladder_config: dict[str, Any], freqtrade_config: dict[str, Any]) -> Any:
    """
    notifications.provider: slack | none (default slack if webhook configured, else none)
    """
    notif = ladder_config.get("notifications") or {}
    provider = (notif.get("provider") or "slack").lower().strip()
    dry_run = bool(freqtrade_config.get("dry_run"))
    dry_run_label = notif.get("dry_run_label", True)
    notify_rebalancing = notif.get("notify_rebalancing")
    if notify_rebalancing is None:
        notify_rebalancing = (ladder_config.get("telegram") or {}).get("notify_rebalancing", False)
    notify_rebalancing = bool(notify_rebalancing)

    slack_cfg = ladder_config.get("slack") or {}
    webhook = (slack_cfg.get("webhook_url") or os.environ.get("SPOT_LADDER_SLACK_WEBHOOK_URL") or "").strip()

    if provider == "none":
        logger.info("Ladder notifications disabled (notifications.provider=none)")
        return TelegramNotifier()

    if provider == "slack":
        if not webhook:
            logger.warning(
                "Slack webhook missing: set SPOT_LADDER_SLACK_WEBHOOK_URL in container env "
                "(docker compose .env) or slack.webhook_url in spot_ladder/config.yaml"
            )
        notifier = SlackNotifier(
            webhook_url=webhook,
            dry_run=dry_run,
            dry_run_label=bool(dry_run_label),
            notify_rebalancing=notify_rebalancing,
            username=str(slack_cfg.get("username") or "Spot Ladder"),
            icon_emoji=str(slack_cfg.get("icon_emoji") or ":chart_with_upwards_trend:"),
        )
        if notifier.enabled:
            logger.info(
                "Slack ladder notifications enabled (dry_run=%s, notify_rebalancing=%s)",
                dry_run,
                notify_rebalancing,
            )
        return notifier

    if provider == "telegram":
        tg = ladder_config.get("telegram") or {}
        logger.warning(
            "notifications.provider=telegram but only Slack is implemented; "
            "port telegram_notifier from your legacy bot or use slack."
        )
        return TelegramNotifier(
            bot_token=tg.get("bot_token"),
            chat_id=tg.get("chat_id"),
            notify_rebalancing=notify_rebalancing,
        )

    logger.warning("Unknown notifications.provider=%s — notifications off", provider)
    return TelegramNotifier()
