from __future__ import annotations

import asyncio
import hashlib
import hmac
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode

import httpx

from monitor_app.config import MonitorAccountConfig, Settings


class MonitorGatewayError(RuntimeError):
    pass


class BinanceMonitorGateway:
    def __init__(self, settings: Settings, account: MonitorAccountConfig) -> None:
        self._settings = settings
        self._account = account
        headers = {"X-MBX-APIKEY": self._account.api_key}
        self._client = httpx.AsyncClient(
            base_url="https://papi.binance.com",
            headers=headers,
            timeout=10.0,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def _signed_request(self, path: str, params: dict[str, Any] | None = None) -> Any:
        params = dict(params or {})
        params["timestamp"] = int(datetime.now(UTC).timestamp() * 1000)
        params["recvWindow"] = self._settings.binance_recv_window_ms
        query = urlencode(params, doseq=True)
        signature = hmac.new(
            self._account.api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        response = await self._client.get(f"{path}?{query}&signature={signature}")
        response.raise_for_status()
        return response.json()

    async def get_unified_account_snapshot(
        self,
        *,
        history_window_days: int = 7,
        income_limit: int = 100,
        interest_limit: int = 100,
    ) -> dict[str, Any]:
        if not self._account.api_key or not self._account.api_secret:
            raise MonitorGatewayError("Binance API credentials are not configured")

        bounded_window_days = max(1, min(history_window_days, 30))
        now = datetime.now(UTC)
        end_time = int(now.timestamp() * 1000)
        start_time = int((now - timedelta(days=bounded_window_days)).timestamp() * 1000)

        try:
            account_payload, um_account_payload = await asyncio.gather(
                self._signed_request("/papi/v1/account"),
                self._signed_request("/papi/v1/um/account"),
            )
        except Exception as exc:
            raise MonitorGatewayError(f"Failed to fetch unified account snapshot: {exc}") from exc

        optional_results = await asyncio.gather(
            self._optional_request(
                "/papi/v1/um/income",
                {"startTime": start_time, "endTime": end_time, "limit": income_limit},
            ),
            self._optional_request(
                "/papi/v1/margin/marginInterestHistory",
                {"startTime": start_time, "endTime": end_time, "size": interest_limit},
            ),
            self._optional_request(
                "/papi/v1/portfolio/interest-history",
                {"startTime": start_time, "endTime": end_time, "size": interest_limit},
            ),
        )
        (income_payload, income_error), (borrow_interest_payload, borrow_interest_error), (
            negative_interest_payload,
            negative_interest_error,
        ) = optional_results

        positions = self._parse_positions(um_account_payload.get("positions", []))
        assets = self._parse_assets(um_account_payload.get("assets", []))
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
            "main_account_id": self._account.main_account_id,
            "main_account_name": self._account.main_account_name,
            "child_account_id": self._account.child_account_id,
            "child_account_name": self._account.child_account_name,
            "account_name": self._account.display_name,
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

    async def _optional_request(self, path: str, params: dict[str, Any]) -> tuple[Any, str | None]:
        try:
            payload = await self._signed_request(path, params)
            return payload, None
        except Exception as exc:
            return None, str(exc)

    def _parse_positions(self, payload: list[dict[str, Any]]) -> list[dict[str, Any]]:
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

    def _parse_assets(self, payload: list[dict[str, Any]]) -> list[dict[str, Any]]:
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

    def _summarize_interest_history(self, borrow_payload: Any, negative_payload: Any, history_window_days: int) -> dict[str, Any]:
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