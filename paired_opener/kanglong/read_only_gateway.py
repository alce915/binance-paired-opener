from __future__ import annotations

from decimal import Decimal
from typing import Any


class KanglongReadOnlyGateway:
    """亢龙第一阶段使用的最小只读交易所能力边界。"""

    __slots__ = ("__gateway",)

    def __init__(self, gateway: Any) -> None:
        self.__gateway = gateway

    async def get_symbol_rules(self, symbol: str):
        return await self.__gateway.get_symbol_rules(symbol)

    async def get_quote(self, symbol: str):
        return await self.__gateway.get_quote(symbol)

    async def refresh_quote(self, symbol: str):
        return await self.__gateway.refresh_quote(symbol)

    async def get_order_book(self, symbol: str, limit: int = 10):
        return await self.__gateway.get_order_book(symbol, limit=limit)

    async def refresh_order_book(self, symbol: str, limit: int = 10):
        return await self.__gateway.refresh_order_book(symbol, limit=limit)

    async def get_portfolio_margin_precheck(
        self,
        symbol: str,
        requested_leverage: int,
        additional_gross_notional: Decimal,
    ):
        return await self.__gateway.get_portfolio_margin_precheck(
            symbol,
            requested_leverage,
            additional_gross_notional,
        )

    @property
    def _rate_limit_observer(self):
        delegate = getattr(self.__gateway, "_delegate", self.__gateway)
        return getattr(delegate, "_rate_limit_observer", None)

    @property
    def _rate_limit_observer_owner(self):
        return getattr(self.__gateway, "_delegate", self.__gateway)

    @_rate_limit_observer.setter
    def _rate_limit_observer(self, observer) -> None:
        delegate = getattr(self.__gateway, "_delegate", self.__gateway)
        if not hasattr(delegate, "_rate_limit_observer"):
            raise AttributeError("rate-limit observer is unavailable")
        delegate._rate_limit_observer = observer
