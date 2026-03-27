from __future__ import annotations

from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Any

from paired_opener.domain import ExchangeOrder, OrderSide, PositionSide, Quote, SymbolRules


class ExchangeGateway(ABC):
    @abstractmethod
    async def ensure_hedge_mode(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def is_hedge_mode_enabled(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def ensure_cross_margin(self, symbol: str) -> None:
        raise NotImplementedError

    @abstractmethod
    async def ensure_leverage(self, symbol: str, leverage: int) -> None:
        raise NotImplementedError

    @abstractmethod
    async def get_symbol_rules(self, symbol: str) -> SymbolRules:
        raise NotImplementedError

    @abstractmethod
    async def get_quote(self, symbol: str) -> Quote:
        raise NotImplementedError

    @abstractmethod
    async def get_order_book(self, symbol: str, limit: int = 10) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def get_account_overview(self) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def get_symbol_leverage(self, symbol: str) -> int:
        raise NotImplementedError

    @abstractmethod
    async def get_open_orders(self, symbol: str) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    async def place_market_order(
        self,
        *,
        symbol: str,
        side: OrderSide,
        position_side: PositionSide,
        qty: Decimal,
        client_order_id: str,
    ) -> ExchangeOrder:
        raise NotImplementedError

    @abstractmethod
    async def get_order(self, *, symbol: str, order_id: str) -> ExchangeOrder:
        raise NotImplementedError

    @abstractmethod
    async def cancel_order(self, *, symbol: str, order_id: str) -> ExchangeOrder:
        raise NotImplementedError
