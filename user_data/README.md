# user_data (this fork)

Deployable ladder config and code are tracked in git. Runtime files stay on the server only.

## Strategy overview

Freqtrade is the process host only: `SpotLadderStrategy` does not emit trade signals. On each live/dry-run loop it runs `OrderManager` (see `spot_ladder/config.yaml` → `trading.loop_interval`, default 120s).

**Market:** Hyperliquid **XRP/USDC perp** (`XRP/USDC:USDC` in CCXT). Collateral is USDC; long position size is treated as XRP inventory for ladder sizing.

**Buy ladder:** Limit buys placed below last price at configured `%` rungs (`buy_levels`, default 0.5–15%). Size uses weighted distribution (more on near rungs). The ladder reprices when price moves beyond `price_update_threshold` (and related time windows). Up to `balance_percentage_per_symbol` of free USDC is committed (default 90%). Optional `price_elevation` (disabled by default) can reduce allocation and skip shallow rungs when price is high in a rolling window.

**Sell ladders (mean-reversion enabled):** Position splits into **Core** (~90%) and **Working** (~10%, `working_position_pct`):

- **Core** — recovery sells at `sell_levels` % above **average entry** (from `filled_orders_{COIN}.json`, LIFO consumption). Active whenever there is a meaningful position.
- **Working** — sells at `working_sell_levels` % above **current price** while underwater (price more than `cancel_working_threshold_pct` below avg entry). Cancelled near/above entry so Core keeps higher-margin exits. Tracked by order id in `working_orders_{COIN}.json`.

With `mean_reversion.enabled: false`, the full position uses the Core ladder only.

**Ledger:** Buys and sells persist to `spot_ladder/state/filled_orders_{COIN}.json`. Sell profit and consumption use **LIFO** (newest buys consumed first). Dry-run open/closed orders also persist to `dry_run_orders_{COIN}.json` so restarts adopt the book like live `fetch_open_orders`.

**Notifications:** Ladder fills/errors via Slack (`notifications.provider: slack`), not Freqtrade’s built-in Telegram. Daily summary: `reporting.enabled` + cron on `daily_summary_slack.py`.

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

`OrderManager` calls the notifier interface (`notify_order_filled`, ladder recalc, errors). This fork implements that with Slack Incoming Webhooks. **Not** Freqtrade’s built-in Telegram.

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

To customize message text, edit method bodies in `spot_ladder/slack_notifier.py` (call sites in `order_manager.py` stay unchanged).

## Stay current with upstream freqtrade

```bash
git fetch upstream
git merge upstream/develop   # or your tracking branch
# Resolve conflicts only in non-user_data paths when possible
```

Keep custom work under `user_data/` as above so merges stay isolated from `freqtrade/` core.
