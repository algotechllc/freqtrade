"""
Hyperliquid exchange adapter using the response shape expected by OrderManager.
Uses Freqtrade's CCXT exchange instance (sync wrappers).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from freqtrade.exchange import Exchange


logger = logging.getLogger(__name__)


class HyperliquidExchangeAdapter:
    """Bridge OrderManager to Hyperliquid via Freqtrade's Exchange."""

    def __init__(self, exchange: Exchange, pair: str):
        self.exchange = exchange
        self.pair = pair
        parts = pair.split("/")
        self.cointype = parts[0].upper()
        quote = parts[1] if len(parts) > 1 else "USDC"
        # XRP/USDC:USDC → USDC (settle suffix after colon)
        self.market = quote.split(":")[0].upper()

    def _amount_to_precision(self, amount: float) -> float:
        return float(self.exchange.amount_to_precision(self.pair, amount))

    def _price_to_precision(self, price: float) -> float:
        return float(self.exchange.price_to_precision(self.pair, price))

    def get_latest_price(self, cointype: str, market: str = "USDC") -> dict[str, Any]:
        try:
            ticker = self.exchange.fetch_ticker(self.pair)
            bid = float(ticker.get("bid") or 0)
            ask = float(ticker.get("ask") or 0)
            last = float(ticker.get("last") or ticker.get("close") or 0)
            if not last and bid and ask:
                last = (bid + ask) / 2
            return {
                "status": "ok",
                "prices": {"bid": bid, "ask": ask, "last": last},
            }
        except Exception as e:
            logger.warning("get_latest_price failed for %s: %s", self.pair, e)
            return {"status": "error", "message": str(e)}

    def get_balances(self) -> dict[str, Any]:
        """Balance payload for OrderManager; strategy wrapper converts to a flat dict."""
        balances = self.exchange.get_balances()
        items: list[dict[str, Any]] = []
        for currency, row in balances.items():
            if currency in ("info", "free", "used", "total", "datetime", "timestamp"):
                continue
            if not isinstance(row, dict):
                continue
            total = float(row.get("total") or row.get("free") or 0)
            items.append({currency.upper(): {"balance": total}})
        return {"status": "ok", "balances": items}

    @staticmethod
    def balances_to_flat(balances_response: dict[str, Any]) -> dict[str, dict[str, float]]:
        flat: dict[str, dict[str, float]] = {}
        for item in balances_response.get("balances", []):
            if isinstance(item, dict):
                flat.update(item)
        return flat

    @staticmethod
    def _row_since_ms(row: dict[str, Any]) -> int:
        solddate = row.get("solddate") or ""
        if not solddate:
            return 0
        try:
            dt = datetime.fromisoformat(solddate.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except (ValueError, TypeError):
            return 0

    def _ccxt_order_to_ladder_row(self, order: dict[str, Any], side: str) -> dict[str, Any]:
        amount = float(order.get("amount") or order.get("remaining") or 0)
        rate = float(order.get("price") or 0)
        return {
            "id": str(order.get("id", "")),
            "coin": self.cointype,
            "amount": amount,
            "rate": rate,
            "market": self.market,
        }

    def _get_open_orders(self) -> list[dict[str, Any]]:
        """Open orders for this pair (dry-run store or exchange API)."""
        if self.exchange._config.get("dry_run"):
            return [
                o
                for o in self.exchange._dry_run_open_orders.values()
                if o.get("symbol") == self.pair and (o.get("status") or "open") == "open"
            ]
        if self.exchange.exchange_has("fetchOpenOrders"):
            return self.exchange._api.fetch_open_orders(self.pair)
        return []

    def get_orders(self, cointype: str, market: str = "USDC") -> dict[str, Any]:
        try:
            open_orders = self._get_open_orders()
            buyorders: list[dict[str, Any]] = []
            sellorders: list[dict[str, Any]] = []
            for order in open_orders:
                side = str(order.get("side") or "").lower()
                row = self._ccxt_order_to_ladder_row(order, side)
                if side == "buy":
                    buyorders.append(row)
                elif side == "sell":
                    sellorders.append(row)
            return {"status": "ok", "buyorders": buyorders, "sellorders": sellorders}
        except Exception as e:
            logger.warning("get_orders failed: %s", e)
            return {"status": "ok", "buyorders": [], "sellorders": []}

    def get_completed_orders(self, cointype: str, market: str = "USDC", since_ms: Optional[int] = None) -> dict[str, Any]:
        """Recent fills for sync / verification (OrderManager response shape)."""
        try:
            if self.exchange._config.get("dry_run"):
                from spot_ladder.dry_run_balances import dry_run_completed_order_rows

                buyorders, sellorders = dry_run_completed_order_rows(
                    self.exchange, self.pair, self.cointype
                )
                if since_ms is not None:
                    buyorders = [o for o in buyorders if self._row_since_ms(o) >= since_ms]
                    sellorders = [o for o in sellorders if self._row_since_ms(o) >= since_ms]
                return {"status": "ok", "buyorders": buyorders, "sellorders": sellorders}

            if since_ms is None:
                since_ms = int(datetime.now(timezone.utc).timestamp() * 1000) - 90 * 86400 * 1000
            trades = self.exchange._api.fetch_my_trades(self.pair, since=since_ms)
            buyorders: list[dict[str, Any]] = []
            sellorders: list[dict[str, Any]] = []
            for trade in trades:
                side = (trade.get("side") or "").lower()
                ts = trade.get("timestamp")
                solddate = (
                    datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()
                    if ts
                    else datetime.now(timezone.utc).isoformat()
                )
                row = {
                    "id": str(trade.get("order") or trade.get("id", "")),
                    "amount": float(trade.get("amount") or 0),
                    "rate": float(trade.get("price") or 0),
                    "solddate": solddate,
                    "coin": self.cointype,
                }
                if side == "buy":
                    buyorders.append(row)
                elif side == "sell":
                    sellorders.append(row)
            return {"status": "ok", "buyorders": buyorders, "sellorders": sellorders}
        except Exception as e:
            logger.warning("get_completed_orders failed: %s", e)
            return {"status": "error", "message": str(e), "buyorders": [], "sellorders": []}

    def place_buy_order(
        self, cointype: str, amount: float, rate: float, market: str = "USDC"
    ) -> dict[str, Any]:
        try:
            amount = self._amount_to_precision(amount)
            rate = self._price_to_precision(rate)
            order = self.exchange.create_order(
                pair=self.pair,
                ordertype="limit",
                side="buy",
                amount=amount,
                rate=rate,
                leverage=1.0,
            )
            return {"status": "ok", "id": str(order.get("id", ""))}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def place_sell_order(
        self, cointype: str, amount: float, rate: float, market: str = "USDC"
    ) -> dict[str, Any]:
        try:
            amount = self._amount_to_precision(amount)
            rate = self._price_to_precision(rate)
            order = self.exchange.create_order(
                pair=self.pair,
                ordertype="limit",
                side="sell",
                amount=amount,
                rate=rate,
                leverage=1.0,
            )
            return {"status": "ok", "id": str(order.get("id", ""))}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def cancel_order(self, order_id: str, order_type: str = "buy") -> dict[str, Any]:
        try:
            self.exchange.cancel_order(order_id, self.pair)
            return {"status": "ok"}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def edit_order(
        self,
        order_id: str,
        cointype: str,
        current_rate: float,
        new_rate: float,
        order_type: str = "buy",
    ) -> dict[str, Any]:
        """Hyperliquid: cancel and replace (edit_order API compatibility)."""
        try:
            open_orders = self._get_open_orders()
            target = next((o for o in open_orders if str(o.get("id")) == str(order_id)), None)
            if not target:
                return {"status": "error", "message": "Order not found for edit"}
            amount = float(target.get("amount") or target.get("remaining") or 0)
            side = (target.get("side") or order_type).lower()
            self.exchange.cancel_order(order_id, self.pair)
            new_rate = self._price_to_precision(new_rate)
            amount = self._amount_to_precision(amount)
            order = self.exchange.create_order(
                pair=self.pair,
                ordertype="limit",
                side=side,
                amount=amount,
                rate=new_rate,
                leverage=1.0,
            )
            return {"status": "ok", "id": str(order.get("id", order_id))}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def buy_now(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """Skim / market buy — not implemented for Hyperliquid adapter yet."""
        return {"status": "error", "message": "buy_now not supported on Hyperliquid adapter"}
