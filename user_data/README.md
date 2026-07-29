# user_data (this fork)

Deployable ladder config and code are tracked in git. Runtime files stay on the server only.

## Tracked in git

- `config.json` — bot settings (no secrets; keys live in `config-private.json`)
- `config-private.json.example` — template for the server
- `strategies/SpotLadderStrategy.py`
- `spot_ladder/` — ladder code, `config.yaml`, `state/*.example`, `state/.gitkeep`

## Not tracked (created on the server)

- `config-private.json` — Hyperliquid wallet / key
- `spot_ladder/state/*.json` — live ledger (`filled_orders_*`, `price_high_*`, `working_orders_*`, `dry_run_orders_*`, …)
- `tradesv3.sqlite`, `logs/*`, `data/*`

## Server deploy

```bash
git clone git@github.com:YOUR_ORG/freqtrade.git
cd freqtrade
cp user_data/config-private.json.example user_data/config-private.json
# Edit config-private.json with wallet credentials

# Optional dry-run seed (not for production ledger):
# cp user_data/spot_ladder/state/filled_orders_XRP.json.example \
#    user_data/spot_ladder/state/filled_orders_XRP.json

docker compose up -d
docker compose logs -f
```

Updates: `git pull` then `docker compose restart freqtrade`. Do not overwrite `spot_ladder/state/*.json` or `config-private.json` when pulling.

Dry-run open/closed ladder orders are persisted to `spot_ladder/state/dry_run_orders_XRP.json` so a restart adopts the book like LIVE `fetch_open_orders` (instead of wiping memory and recreating the full ladder).

## Slack notifications (ladder bot)

Uses the same `OrderManager` hooks as the legacy Telegram bot (`notify_order_filled`, ladder recalc messages, errors). **Not** Freqtrade’s built-in Telegram.

1. In Slack: **Apps → Incoming Webhooks** → add to your channel → copy webhook URL.
2. On the server, set the secret (preferred — keep out of git):

   ```bash
   # docker-compose.yml environment: section, or /etc/environment:
   export SPOT_LADDER_SLACK_WEBHOOK_URL='https://hooks.slack.com/services/...'
   ```

   Or put `slack.webhook_url` in `spot_ladder/config.yaml` on the server only (do not commit).

3. In `spot_ladder/config.yaml`: `notifications.provider: slack`, `notifications.dry_run_label: true` (prefixes `*[DRY-RUN]*` while `dry_run: true` in `config.json`).
4. Restart: `docker compose up -d --force-recreate`
5. Confirm startup log: `Slack ladder notifications enabled`

**Daily summary:** set `reporting.enabled: true`, then cron (UTC example):

```bash
0 0 * * * cd ~/freqtrade && docker compose exec -T freqtrade python /freqtrade/user_data/spot_ladder/daily_summary_slack.py
```

To match your old Telegram formatting exactly, copy message text from your legacy `telegram_notifier.py` into `spot_ladder/slack_notifier.py` (method bodies only — call sites stay the same).

## Stay current with upstream freqtrade

```bash
git fetch upstream
git merge upstream/develop   # or your tracking branch
# Resolve conflicts only in non-user_data paths when possible
```

Keep custom work under `user_data/` as above so merges stay isolated from `freqtrade/` core.
