# user_data (this fork)

Deployable ladder config and code are tracked in git. Runtime files stay on the server only.

## Tracked in git

- `config.json` — bot settings (no secrets; keys live in `config-private.json`)
- `config-private.json.example` — template for the server
- `strategies/SpotLadderStrategy.py`
- `spot_ladder/` — ladder code, `config.yaml`, `state/*.example`, `state/.gitkeep`

## Not tracked (created on the server)

- `config-private.json` — Hyperliquid wallet / key
- `spot_ladder/state/*.json` — live ledger (`filled_orders_*`, `price_high_*`, `working_orders_*`, …)
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

## Stay current with upstream freqtrade

```bash
git fetch upstream
git merge upstream/develop   # or your tracking branch
# Resolve conflicts only in non-user_data paths when possible
```

Keep custom work under `user_data/` as above so merges stay isolated from `freqtrade/` core.
