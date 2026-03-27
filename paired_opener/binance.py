from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode

import httpx
import websockets

from paired_opener.config import AccountConfig, Settings
from paired_opener.domain import (
    ExchangeOrder,
    ExchangeOrderStatus,
    ExchangeStateError,
    OrderSide,
    PositionSide,
    Quote,
    SymbolRules,
)
from paired_opener.exchange import ExchangeGateway


class BinanceFuturesGateway(ExchangeGateway):
    def __init__(self, settings: Settings, account: AccountConfig | None = None) -> None:
        self._settings = settings
        if account is not None:
            self._account = account
        elif settings.accounts:
            self._account = settings.active_account
        else:
            self._account = AccountConfig(
                account_id="default",
                name="默认账户",
                api_key=settings.binance_api_key,
                api_secret=settings.binance_api_secret,
                use_testnet=settings.binance_use_testnet,
                rest_base_url=settings.binance_rest_base_url,
                ws_base_url=settings.binance_ws_base_url,
            )
        headers = {"X-MBX-APIKEY": self._account.api_key}
        self._client = httpx.AsyncClient(
            base_url=self._account.effective_rest_base_url,
            headers=headers,
            timeout=10.0,
        )
        self._papi_client = httpx.AsyncClient(
            base_url="https://papi.binance.com",
            headers=headers,
            timeout=10.0,
        )
        self._rules_cache: dict[str, SymbolRules] = {}
        self._quote_cache: dict[str, Quote] = {}
        self._order_cache: dict[str, ExchangeOrder] = {}
        self._symbol_leverage_cache: dict[str, int] = {}
        self._quote_tasks: dict[str, asyncio.Task[None]] = {}
        self._user_stream_task: asyncio.Task[None] | None = None
        self._listen_key: str | None = None

    async def close(self) -> None:
        for task in self._quote_tasks.values():
            task.cancel()
        if self._user_stream_task is not None:
            self._user_stream_task.cancel()
        await self._client.aclose()
        await self._papi_client.aclose()

    async def _public_request(self, method: str, path: str, params: dict[str, Any] | None = None) -> Any:
        response = await self._client.request(method, path, params=params)
        response.raise_for_status()
        return response.json()

    async def _signed_request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        use_papi: bool = False,
    ) -> Any:
        params = dict(params or {})
        params["timestamp"] = int(datetime.now(UTC).timestamp() * 1000)
        params["recvWindow"] = self._settings.binance_recv_window_ms
        query = urlencode(params, doseq=True)
        signature = hmac.new(
            self._account.api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        client = self._papi_client if use_papi else self._client
        response = await client.request(method, f"{path}?{query}&signature={signature}")
        response.raise_for_status()
        return response.json()

    async def _start_listen_key(self) -> str:
        response = await self._client.post("/fapi/v1/listenKey")
        response.raise_for_status()
        payload = response.json()
        return payload["listenKey"]

    async def _keepalive_listen_key(self) -> None:
        if self._listen_key is None:
            return
        response = await self._client.put("/fapi/v1/listenKey", params={"listenKey": self._listen_key})
        response.raise_for_status()

    async def _ensure_user_stream(self) -> None:
        if self._user_stream_task is not None or not self._account.api_key:
            return
        self._listen_key = await self._start_listen_key()
        self._user_stream_task = asyncio.create_task(self._run_user_stream())

    def _order_from_stream(self, payload: dict[str, Any]) -> ExchangeOrder:
        order = payload["o"]
        return ExchangeOrder(
            symbol=order["s"],
            order_id=str(order["i"]),
            client_order_id=order.get("c", ""),
            side=OrderSide(order["S"]),
            position_side=PositionSide(order["ps"]),
            type=order["o"],
            price=Decimal(order.get("p") or order.get("ap") or "0"),
            orig_qty=Decimal(order.get("q") or "0"),
            executed_qty=Decimal(order.get("z") or "0"),
            status=ExchangeOrderStatus(order["X"]),
            raw=payload,
        )

    async def _run_user_stream(self) -> None:
        keepalive_at = asyncio.get_running_loop().time() + 30 * 60
        while True:
            listen_key = self._listen_key or await self._start_listen_key()
            self._listen_key = listen_key
            stream = f"{self._account.effective_websocket_base_url}/{listen_key}"
            try:
                async with websockets.connect(stream, ping_interval=20, ping_timeout=20) as websocket:
                    async for message in websocket:
                        now = asyncio.get_running_loop().time()
                        if now >= keepalive_at:
                            await self._keepalive_listen_key()
                            keepalive_at = now + 30 * 60
                        payload = json.loads(message)
                        if payload.get("e") == "ORDER_TRADE_UPDATE":
                            order = self._order_from_stream(payload)
                            self._order_cache[order.order_id] = order
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(1)

    async def ensure_hedge_mode(self) -> None:
        await self._ensure_user_stream()
        try:
            payload = await self._signed_request("POST", "/fapi/v1/positionSide/dual", {"dualSidePosition": "true"})
        except httpx.HTTPStatusError as exc:
            body = exc.response.text
            if "No need to change position side" in body or '"code":-4059' in body:
                return
            raise ExchangeStateError(f"Failed to enable hedge mode: {body}") from exc
        if payload.get("code") not in (200, None):
            raise ExchangeStateError(f"Failed to enable hedge mode: {payload}")

    async def is_hedge_mode_enabled(self) -> bool:
        payload = await self._signed_request('GET', '/fapi/v1/positionSide/dual')
        return str(payload.get('dualSidePosition', '')).lower() == 'true'

    async def ensure_cross_margin(self, symbol: str) -> None:
        try:
            payload = await self._signed_request(
                "POST",
                "/fapi/v1/marginType",
                {"symbol": symbol, "marginType": "CROSSED"},
            )
        except httpx.HTTPStatusError as exc:
            body = exc.response.text
            if "No need to change margin type" in body:
                return
            raise ExchangeStateError(f"Failed to set cross margin for {symbol}: {body}") from exc
        if payload.get("code") not in (200, None):
            raise ExchangeStateError(f"Failed to set cross margin for {symbol}: {payload}")

    async def ensure_leverage(self, symbol: str, leverage: int) -> None:
        payload = await self._signed_request("POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": leverage})
        if int(payload.get("leverage", 0)) != leverage:
            raise ExchangeStateError(f"Leverage mismatch for {symbol}: {payload}")
        self._symbol_leverage_cache[symbol.upper()] = leverage

    async def get_symbol_rules(self, symbol: str) -> SymbolRules:
        symbol = symbol.upper()
        if symbol in self._rules_cache:
            return self._rules_cache[symbol]
        exchange_info = await self._public_request("GET", "/fapi/v1/exchangeInfo")
        for item in exchange_info["symbols"]:
            if item["symbol"] != symbol:
                continue
            filters = {entry["filterType"]: entry for entry in item["filters"]}
            rules = SymbolRules(
                symbol=symbol,
                tick_size=Decimal(filters["PRICE_FILTER"]["tickSize"]),
                step_size=Decimal(filters["LOT_SIZE"]["stepSize"]),
                min_qty=Decimal(filters["LOT_SIZE"]["minQty"]),
                min_notional=Decimal(filters.get("MIN_NOTIONAL", {}).get("notional", "0")),
                max_leverage=int(item.get("maxLeverage") or 125),
            )
            self._rules_cache[symbol] = rules
            return rules
        raise ExchangeStateError(f"Symbol {symbol} not found in exchange info")

    async def _ensure_quote_stream(self, symbol: str) -> None:
        if symbol in self._quote_tasks:
            return
        self._quote_tasks[symbol] = asyncio.create_task(self._run_quote_stream(symbol))

    async def _run_quote_stream(self, symbol: str) -> None:
        stream = f"{self._account.effective_websocket_base_url}/{symbol.lower()}@bookTicker"
        while True:
            try:
                async with websockets.connect(stream, ping_interval=20, ping_timeout=20) as websocket:
                    async for message in websocket:
                        payload = json.loads(message)
                        self._quote_cache[symbol] = Quote(
                            symbol=symbol,
                            bid_price=Decimal(payload["b"]),
                            ask_price=Decimal(payload["a"]),
                        )
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(1)

    async def get_quote(self, symbol: str) -> Quote:
        symbol = symbol.upper()
        await self._ensure_quote_stream(symbol)
        quote = self._quote_cache.get(symbol)
        if quote is not None:
            return quote
        payload = await self._public_request("GET", "/fapi/v1/ticker/bookTicker", {"symbol": symbol})
        return Quote(symbol=symbol, bid_price=Decimal(payload["bidPrice"]), ask_price=Decimal(payload["askPrice"]))

    async def get_order_book(self, symbol: str, limit: int = 10) -> dict[str, Any]:
        payload = await self._public_request("GET", "/fapi/v1/depth", {"symbol": symbol.upper(), "limit": limit})
        return {
            "symbol": symbol.upper(),
            "lastUpdateId": payload.get("lastUpdateId"),
            "bids": [
                {"price": Decimal(price), "qty": Decimal(qty)}
                for price, qty in payload.get("bids", [])
            ],
            "asks": [
                {"price": Decimal(price), "qty": Decimal(qty)}
                for price, qty in payload.get("asks", [])
            ],
            "event_time": datetime.now(UTC),
        }

    async def get_symbol_leverage(self, symbol: str) -> int:
        target = symbol.upper()
        cached = self._symbol_leverage_cache.get(target)
        if cached is not None:
            return cached
        if not self._account.api_key or not self._account.api_secret:
            return 1
        try:
            payload = await self._signed_request("GET", "/papi/v1/um/positionRisk", use_papi=True)
        except Exception:
            return 1
        for item in payload:
            if str(item.get("symbol", "")).upper() != target:
                continue
            leverage = int(item.get("leverage") or 0)
            if leverage > 0:
                self._symbol_leverage_cache[target] = leverage
                return leverage
        return 1

    async def get_open_orders(self, symbol: str) -> list[dict[str, Any]]:
        payload = await self._signed_request("GET", "/fapi/v1/openOrders", {"symbol": symbol.upper()})
        if not isinstance(payload, list):
            return []
        return [item for item in payload if isinstance(item, dict)]

    async def get_account_overview(self) -> dict[str, Any]:
        if not self._account.api_key or not self._account.api_secret:
            raise ExchangeStateError("Binance API credentials are not configured")
        try:
            return await self._get_account_overview_from_papi()
        except Exception as exc:
            raise ExchangeStateError(f"统一账户数据获取失败: {exc}") from exc

    async def get_unified_account_snapshot(
        self,
        *,
        history_window_days: int = 7,
        income_limit: int = 100,
        interest_limit: int = 100,
    ) -> dict[str, Any]:
        if not self._account.api_key or not self._account.api_secret:
            raise ExchangeStateError("Binance API credentials are not configured")
        bounded_window_days = max(1, min(history_window_days, 30))
        now = datetime.now(UTC)
        end_time = int(now.timestamp() * 1000)
        start_time = int((now - timedelta(days=bounded_window_days)).timestamp() * 1000)

        account_payload, um_account_payload = await asyncio.gather(
            self._signed_request("GET", "/papi/v1/account", use_papi=True),
            self._signed_request("GET", "/papi/v1/um/account", use_papi=True),
        )
        optional_results = await asyncio.gather(
            self._optional_papi_request(
                "/papi/v1/um/income",
                {"startTime": start_time, "endTime": end_time, "limit": income_limit},
            ),
            self._optional_papi_request(
                "/papi/v1/margin/marginInterestHistory",
                {"startTime": start_time, "endTime": end_time, "size": interest_limit},
            ),
            self._optional_papi_request(
                "/papi/v1/portfolio/interest-history",
                {"startTime": start_time, "endTime": end_time, "size": interest_limit},
            ),
        )
        (income_payload, income_error), (borrow_interest_payload, borrow_interest_error), (
            negative_interest_payload,
            negative_interest_error,
        ) = optional_results

        positions = self._parse_unified_positions(um_account_payload.get("positions", []))
        assets = self._parse_unified_assets(um_account_payload.get("assets", []))
        unrealized_pnl = sum((entry["unrealized_pnl"] for entry in positions), Decimal("0"))
        if unrealized_pnl == Decimal("0") and assets:
            unrealized_pnl = sum((entry["cross_unrealized_pnl"] for entry in assets), Decimal("0"))
        income_summary = self._summarize_income_history(income_payload, bounded_window_days)
        interest_summary = self._summarize_interest_history(
            borrow_interest_payload,
            negative_interest_payload,
            bounded_window_days,
        )
        section_errors = {
            key: value
            for key, value in {
                "income_history": income_error,
                "margin_interest_history": borrow_interest_error,
                "negative_balance_interest_history": negative_interest_error,
            }.items()
            if value is not None
        }

        return {
            "status": "ok",
            "source": "papi",
            "account_id": self._account.account_id,
            "account_name": self._account.name,
            "account_status": account_payload.get("accountStatus", ""),
            "updated_at": now,
            "totals": {
                "equity": Decimal(account_payload.get("accountEquity") or "0"),
                "margin": Decimal(account_payload.get("accountInitialMargin") or "0"),
                "available_balance": Decimal(
                    account_payload.get("totalAvailableBalance")
                    or account_payload.get("virtualMaxWithdrawAmount")
                    or "0"
                ),
                "unrealized_pnl": unrealized_pnl,
                "total_income": income_summary["total_income"],
                "total_interest": interest_summary["total_interest"],
            },
            "positions": positions,
            "assets": assets,
            "income_summary": income_summary,
            "interest_summary": interest_summary,
            "section_errors": section_errors,
        }

    async def _optional_papi_request(
        self,
        path: str,
        params: dict[str, Any],
    ) -> tuple[Any, str | None]:
        try:
            payload = await self._signed_request("GET", path, params, use_papi=True)
            return payload, None
        except Exception as exc:
            return None, str(exc)

    def _parse_unified_positions(self, payload: list[dict[str, Any]]) -> list[dict[str, Any]]:
        positions: list[dict[str, Any]] = []
        for item in payload:
            position_amt = Decimal(item.get("positionAmt") or "0")
            pnl = Decimal(item.get("unrealizedProfit") or item.get("unRealizedProfit") or "0")
            if position_amt == Decimal("0") and pnl == Decimal("0"):
                continue
            position_side = item.get("positionSide") or ("LONG" if position_amt > 0 else "SHORT")
            positions.append(
                {
                    "symbol": item.get("symbol", ""),
                    "position_side": position_side,
                    "qty": abs(position_amt),
                    "entry_price": Decimal(item.get("entryPrice") or "0"),
                    "mark_price": Decimal(item.get("markPrice") or "0"),
                    "unrealized_pnl": pnl,
                    "notional": abs(Decimal(item.get("notional") or "0")),
                    "leverage": int(item.get("leverage") or 0),
                    "liquidation_price": Decimal(item.get("liquidationPrice") or "0"),
                }
            )
        positions.sort(key=lambda entry: (entry["symbol"], entry["position_side"]))
        return positions

    def _parse_unified_assets(self, payload: list[dict[str, Any]]) -> list[dict[str, Any]]:
        assets: list[dict[str, Any]] = []
        for item in payload:
            wallet_balance = Decimal(item.get("crossWalletBalance") or "0")
            cross_unrealized_pnl = Decimal(item.get("crossUnPnl") or "0")
            if wallet_balance == Decimal("0") and cross_unrealized_pnl == Decimal("0"):
                continue
            assets.append(
                {
                    "asset": item.get("asset", ""),
                    "wallet_balance": wallet_balance,
                    "cross_wallet_balance": wallet_balance,
                    "cross_unrealized_pnl": cross_unrealized_pnl,
                    "available_balance": Decimal(item.get("availableBalance") or "0"),
                    "initial_margin": Decimal(item.get("initialMargin") or "0"),
                    "maintenance_margin": Decimal(item.get("maintMargin") or "0"),
                    "margin_balance": Decimal(item.get("marginBalance") or "0"),
                    "max_withdraw_amount": Decimal(item.get("maxWithdrawAmount") or "0"),
                }
            )
        assets.sort(key=lambda entry: entry["asset"])
        return assets

    def _summarize_income_history(self, payload: Any, history_window_days: int) -> dict[str, Any]:
        rows = payload if isinstance(payload, list) else []
        by_type: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
        by_asset: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
        total_income = Decimal("0")
        for item in rows:
            if not isinstance(item, dict):
                continue
            income = Decimal(item.get("income") or "0")
            income_type = str(item.get("incomeType") or "UNKNOWN")
            asset = str(item.get("asset") or "UNKNOWN")
            total_income += income
            by_type[income_type] += income
            by_asset[asset] += income
        return {
            "window_days": history_window_days,
            "records": len(rows),
            "total_income": total_income,
            "by_type": dict(sorted(by_type.items())),
            "by_asset": dict(sorted(by_asset.items())),
        }

    def _summarize_interest_history(
        self,
        borrow_payload: Any,
        negative_payload: Any,
        history_window_days: int,
    ) -> dict[str, Any]:
        borrow_rows = self._extract_rows(borrow_payload)
        negative_rows = self._extract_rows(negative_payload)
        borrow_interest_total = sum(
            (Decimal(item.get("interest") or item.get("amount") or "0") for item in borrow_rows if isinstance(item, dict)),
            Decimal("0"),
        )
        negative_interest_total = sum(
            (Decimal(item.get("interest") or item.get("amount") or "0") for item in negative_rows if isinstance(item, dict)),
            Decimal("0"),
        )
        return {
            "window_days": history_window_days,
            "records": len(borrow_rows) + len(negative_rows),
            "margin_interest_total": borrow_interest_total,
            "negative_balance_interest_total": negative_interest_total,
            "total_interest": borrow_interest_total + negative_interest_total,
        }

    def _extract_rows(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            rows = payload.get("rows")
            if isinstance(rows, list):
                return [item for item in rows if isinstance(item, dict)]
            data = payload.get("data")
            if isinstance(data, list):
                return [item for item in data if isinstance(item, dict)]
        return []

    async def _get_account_overview_from_papi(self) -> dict[str, Any]:
        account_payload = await self._signed_request("GET", "/papi/v1/account", use_papi=True)
        positions_payload = await self._signed_request("GET", "/papi/v1/um/positionRisk", use_papi=True)
        positions: list[dict[str, Any]] = []
        unrealized_pnl = Decimal("0")
        for item in positions_payload:
            position_amt = Decimal(item.get("positionAmt") or "0")
            pnl = Decimal(item.get("unRealizedProfit") or item.get("unrealizedProfit") or "0")
            if position_amt == Decimal("0") and pnl == Decimal("0"):
                continue
            position_side = item.get("positionSide") or ("LONG" if position_amt > 0 else "SHORT")
            positions.append(
                {
                    "symbol": item.get("symbol", ""),
                    "position_side": position_side,
                    "qty": abs(position_amt),
                    "entry_price": Decimal(item.get("entryPrice") or "0"),
                    "unrealized_pnl": pnl,
                    "notional": abs(Decimal(item.get("notional") or "0")),
                    "leverage": int(item.get("leverage") or 0),
                }
            )
            unrealized_pnl += pnl
        positions.sort(key=lambda entry: (entry["symbol"], entry["position_side"]))
        return {
            "status": "ok",
            "source": "papi",
            "updated_at": datetime.now(UTC),
            "totals": {
                "equity": Decimal(account_payload.get("accountEquity") or "0"),
                "margin": Decimal(account_payload.get("accountInitialMargin") or "0"),
                "available_balance": Decimal(account_payload.get("totalAvailableBalance") or "0"),
                "unrealized_pnl": unrealized_pnl,
            },
            "positions": positions,
        }

    async def _get_account_overview_from_fapi(self) -> dict[str, Any]:
        payload = await self._signed_request("GET", "/fapi/v3/account")
        positions: list[dict[str, Any]] = []
        for item in payload.get("positions", []):
            position_amt = Decimal(item.get("positionAmt") or "0")
            if position_amt == Decimal("0"):
                continue
            position_side = item.get("positionSide") or ("LONG" if position_amt > 0 else "SHORT")
            positions.append(
                {
                    "symbol": item.get("symbol", ""),
                    "position_side": position_side,
                    "qty": abs(position_amt),
                    "entry_price": Decimal(item.get("entryPrice") or "0"),
                    "unrealized_pnl": Decimal(item.get("unrealizedProfit") or "0"),
                    "notional": abs(Decimal(item.get("notional") or "0")),
                    "leverage": int(item.get("leverage") or 0),
                }
            )
        positions.sort(key=lambda entry: (entry["symbol"], entry["position_side"]))
        return {
            "status": "ok",
            "source": "fapi",
            "updated_at": datetime.now(UTC),
            "totals": {
                "equity": Decimal(payload.get("totalMarginBalance") or "0"),
                "margin": Decimal(payload.get("totalInitialMargin") or "0"),
                "available_balance": Decimal(payload.get("availableBalance") or "0"),
                "unrealized_pnl": Decimal(payload.get("totalUnrealizedProfit") or "0"),
            },
            "positions": positions,
        }

    def _to_order(self, payload: dict[str, Any]) -> ExchangeOrder:
        return ExchangeOrder(
            symbol=payload["symbol"],
            order_id=str(payload["orderId"]),
            client_order_id=payload.get("clientOrderId", ""),
            side=OrderSide(payload["side"]),
            position_side=PositionSide(payload["positionSide"]),
            type=payload["type"],
            price=Decimal(payload.get("price") or payload.get("avgPrice") or "0"),
            orig_qty=Decimal(payload.get("origQty") or "0"),
            executed_qty=Decimal(payload.get("executedQty") or "0"),
            status=ExchangeOrderStatus(payload["status"]),
            raw=payload,
        )

    async def place_limit_order(
        self,
        *,
        symbol: str,
        side: OrderSide,
        position_side: PositionSide,
        qty: Decimal,
        price: Decimal,
        client_order_id: str,
    ) -> ExchangeOrder:
        await self._ensure_user_stream()
        payload = await self._signed_request(
            "POST",
            "/fapi/v1/order",
            {
                "symbol": symbol,
                "side": side.value,
                "positionSide": position_side.value,
                "type": "LIMIT",
                "timeInForce": "GTC",
                "quantity": str(qty),
                "price": str(price),
                "newClientOrderId": client_order_id,
            },
        )
        order = self._to_order(payload)
        self._order_cache[order.order_id] = order
        return order

    async def place_market_order(
        self,
        *,
        symbol: str,
        side: OrderSide,
        position_side: PositionSide,
        qty: Decimal,
        client_order_id: str,
    ) -> ExchangeOrder:
        await self._ensure_user_stream()
        payload = await self._signed_request(
            "POST",
            "/fapi/v1/order",
            {
                "symbol": symbol,
                "side": side.value,
                "positionSide": position_side.value,
                "type": "MARKET",
                "quantity": str(qty),
                "newClientOrderId": client_order_id,
            },
        )
        order = self._to_order(payload)
        self._order_cache[order.order_id] = order
        return order

    async def get_order(self, *, symbol: str, order_id: str) -> ExchangeOrder:
        cached = self._order_cache.get(order_id)
        if cached is not None and cached.status in (
            ExchangeOrderStatus.FILLED,
            ExchangeOrderStatus.CANCELED,
            ExchangeOrderStatus.EXPIRED,
            ExchangeOrderStatus.REJECTED,
        ):
            return cached
        payload = await self._signed_request("GET", "/fapi/v1/order", {"symbol": symbol, "orderId": order_id})
        order = self._to_order(payload)
        self._order_cache[order.order_id] = order
        return order

    async def cancel_order(self, *, symbol: str, order_id: str) -> ExchangeOrder:
        payload = await self._signed_request("DELETE", "/fapi/v1/order", {"symbol": symbol, "orderId": order_id})
        order = self._to_order(payload)
        self._order_cache[order.order_id] = order
        return order







