from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any, Awaitable, Callable, TypeVar

import httpx

from paired_opener.domain import ExchangeOrder, ExchangeStateError, OrderSide, PositionSide, Quote, SymbolRules
from paired_opener.errors import ErrorCategory, ErrorStrategy, TradingError
from paired_opener.exchange import ExchangeGateway

_T = TypeVar("_T")


class ClassifiedExchangeGateway(ExchangeGateway):
    def __init__(self, delegate: ExchangeGateway) -> None:
        self._delegate = delegate

    async def close(self) -> None:
        close = getattr(self._delegate, "close", None)
        if close is not None:
            await close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)

    def _error(
        self,
        message: str,
        *,
        category: ErrorCategory,
        strategy: ErrorStrategy,
        code: str,
        raw_code: int | str | None = None,
        raw_message: str | None = None,
        operator_action: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> TradingError:
        return TradingError(
            message,
            category=category,
            strategy=strategy,
            source="gateway",
            code=code,
            raw_code=raw_code,
            raw_message=raw_message,
            operator_action=operator_action,
            context=context or {},
        )

    def _extract_http_error(self, exc: httpx.HTTPStatusError) -> tuple[int | str | None, str]:
        response = exc.response
        text = response.text if response is not None else str(exc)
        if response is None:
            return None, text
        try:
            payload = response.json()
        except ValueError:
            return None, text
        if isinstance(payload, dict):
            return payload.get("code"), str(payload.get("msg") or payload.get("message") or text)
        return None, text

    def _classify_exception(self, exc: Exception, *, operation: str, context: dict[str, Any]) -> TradingError:
        if isinstance(exc, ExchangeStateError):
            message = str(exc)
            lower = message.lower()
            if "credentials are not configured" in lower:
                return self._error(
                    "Binance API 凭证未配置。",
                    category=ErrorCategory.AUTH_ERROR,
                    strategy=ErrorStrategy.MANUAL_INTERVENTION,
                    code="binance_credentials_missing",
                    raw_message=message,
                    operator_action="配置有效的 Binance API Key 和 Secret 后重试。",
                    context={"operation": operation, **context},
                )
            if "not found in exchange info" in lower or "unknown symbol" in lower:
                return self._error(
                    message,
                    category=ErrorCategory.INVALID_PARAMETER,
                    strategy=ErrorStrategy.TERMINATE,
                    code="symbol_not_found",
                    raw_message=message,
                    operator_action="检查交易对代码后重试。",
                    context={"operation": operation, **context},
                )
            return self._error(
                message,
                category=ErrorCategory.EXCHANGE_STATE,
                strategy=ErrorStrategy.MANUAL_INTERVENTION,
                code="exchange_state_error",
                raw_message=message,
                operator_action="检查账户模式、杠杆、持仓状态或交易所返回信息后重试。",
                context={"operation": operation, **context},
            )

        if isinstance(exc, TradingError):
            exc.with_context(operation=operation, **context)
            return exc
        if isinstance(exc, httpx.HTTPStatusError):
            raw_code, raw_message = self._extract_http_error(exc)
            message_lower = (raw_message or "").lower()
            if exc.response is not None and exc.response.status_code == 429 or str(raw_code) == "-1003" or "too many requests" in message_lower:
                return self._error(
                    "Binance 接口限流，请稍后重试。",
                    category=ErrorCategory.RATE_LIMIT,
                    strategy=ErrorStrategy.RETRY,
                    code="binance_rate_limit",
                    raw_code=raw_code,
                    raw_message=raw_message,
                    operator_action="降低请求频率，稍后重试。",
                    context={"operation": operation, **context},
                )
            if exc.response is not None and exc.response.status_code == 401 or str(raw_code) in {"-2014", "-1022"}:
                return self._error(
                    "Binance API 鉴权失败。",
                    category=ErrorCategory.AUTH_ERROR,
                    strategy=ErrorStrategy.MANUAL_INTERVENTION,
                    code="binance_auth_error",
                    raw_code=raw_code,
                    raw_message=raw_message,
                    operator_action="检查 API Key、Secret 和签名配置后重试。",
                    context={"operation": operation, **context},
                )
            if (exc.response is not None and exc.response.status_code in {403, 451}) or "forbidden" in message_lower or "permission" in message_lower or "restricted" in message_lower:
                return self._error(
                    "Binance 接口权限不足或当前出口受限。",
                    category=ErrorCategory.PERMISSION_ERROR,
                    strategy=ErrorStrategy.MANUAL_INTERVENTION,
                    code="binance_permission_error",
                    raw_code=raw_code,
                    raw_message=raw_message,
                    operator_action="检查 API 权限、账户权限或网络出口地区限制。",
                    context={"operation": operation, **context},
                )
            if str(raw_code) in {"-2019", "-2027"} or "insufficient" in message_lower or "balance" in message_lower or "margin" in message_lower:
                return self._error(
                    "账户可用余额或保证金不足。",
                    category=ErrorCategory.INSUFFICIENT_BALANCE,
                    strategy=ErrorStrategy.MANUAL_INTERVENTION,
                    code="binance_insufficient_balance",
                    raw_code=raw_code,
                    raw_message=raw_message,
                    operator_action="补充保证金或降低下单规模后重试。",
                    context={"operation": operation, **context},
                )
            if (exc.response is not None and exc.response.status_code == 400) or str(raw_code) in {"-1100", "-1101", "-1102", "-1111", "-1116", "-1121"}:
                return self._error(
                    "提交给 Binance 的参数不合法。",
                    category=ErrorCategory.INVALID_PARAMETER,
                    strategy=ErrorStrategy.TERMINATE,
                    code="binance_invalid_parameter",
                    raw_code=raw_code,
                    raw_message=raw_message,
                    operator_action="检查交易对、数量、精度和模式参数。",
                    context={"operation": operation, **context},
                )
            return self._error(
                "Binance 请求失败。",
                category=ErrorCategory.UNKNOWN,
                strategy=ErrorStrategy.MANUAL_INTERVENTION,
                code="binance_http_error",
                raw_code=raw_code,
                raw_message=raw_message,
                operator_action="检查交易所响应和服务日志后重试。",
                context={"operation": operation, **context},
            )
        if isinstance(exc, httpx.TimeoutException):
            return self._error(
                "Binance 请求超时。",
                category=ErrorCategory.NETWORK_TIMEOUT,
                strategy=ErrorStrategy.RETRY,
                code="binance_timeout",
                raw_message=str(exc),
                operator_action="等待网络恢复后重试。",
                context={"operation": operation, **context},
            )
        if isinstance(exc, httpx.RequestError):
            return self._error(
                "Binance 网络请求失败。",
                category=ErrorCategory.NETWORK_TIMEOUT,
                strategy=ErrorStrategy.RETRY,
                code="binance_network_error",
                raw_message=str(exc),
                operator_action="检查网络连通性后重试。",
                context={"operation": operation, **context},
            )

        if isinstance(exc, ValueError):
            return self._error(
                str(exc),
                category=ErrorCategory.INVALID_PARAMETER,
                strategy=ErrorStrategy.TERMINATE,
                code="invalid_parameter",
                raw_message=str(exc),
                operator_action="检查请求参数后重试。",
                context={"operation": operation, **context},
            )
        return self._error(
            str(exc) or "未知交易错误",
            category=ErrorCategory.UNKNOWN,
            strategy=ErrorStrategy.MANUAL_INTERVENTION,
            code="unknown_error",
            raw_message=str(exc),
            operator_action="检查日志和交易所返回信息后再继续。",
            context={"operation": operation, **context},
        )

    async def _call(self, operation: str, func: Callable[[], Awaitable[_T]], **context: Any) -> _T:
        last_error: TradingError | None = None
        for attempt in range(3):
            try:
                return await func()
            except Exception as exc:
                last_error = self._classify_exception(exc, operation=operation, context=context)
            if last_error.retryable and attempt < 2:
                delay = float(attempt + 1) if last_error.category == ErrorCategory.RATE_LIMIT else 0.5 * (attempt + 1)
                await asyncio.sleep(delay)
                continue
            raise last_error
        raise last_error or self._error(
            "交易所调用失败。",
            category=ErrorCategory.UNKNOWN,
            strategy=ErrorStrategy.MANUAL_INTERVENTION,
            code="gateway_call_failed",
            operator_action="检查日志后重试。",
            context={"operation": operation, **context},
        )

    async def ensure_hedge_mode(self) -> None:
        await self._call("ensure_hedge_mode", lambda: self._delegate.ensure_hedge_mode())

    async def is_hedge_mode_enabled(self) -> bool:
        return await self._call("is_hedge_mode_enabled", lambda: self._delegate.is_hedge_mode_enabled())

    async def ensure_cross_margin(self, symbol: str) -> None:
        await self._call("ensure_cross_margin", lambda: self._delegate.ensure_cross_margin(symbol), symbol=symbol)

    async def ensure_leverage(self, symbol: str, leverage: int) -> None:
        await self._call("ensure_leverage", lambda: self._delegate.ensure_leverage(symbol, leverage), symbol=symbol, leverage=leverage)

    async def get_symbol_rules(self, symbol: str) -> SymbolRules:
        return await self._call("get_symbol_rules", lambda: self._delegate.get_symbol_rules(symbol), symbol=symbol)

    async def get_quote(self, symbol: str) -> Quote:
        return await self._call("get_quote", lambda: self._delegate.get_quote(symbol), symbol=symbol)

    async def refresh_quote(self, symbol: str) -> Quote:
        return await self._call("refresh_quote", lambda: self._delegate.refresh_quote(symbol), symbol=symbol)

    async def get_order_book(self, symbol: str, limit: int = 10) -> dict[str, Any]:
        return await self._call("get_order_book", lambda: self._delegate.get_order_book(symbol, limit), symbol=symbol, limit=limit)

    async def get_account_overview(self) -> dict[str, Any]:
        return await self._call("get_account_overview", lambda: self._delegate.get_account_overview())

    async def get_symbol_leverage(self, symbol: str) -> int:
        return await self._call("get_symbol_leverage", lambda: self._delegate.get_symbol_leverage(symbol), symbol=symbol)

    async def get_open_orders(self, symbol: str) -> list[dict[str, Any]]:
        return await self._call("get_open_orders", lambda: self._delegate.get_open_orders(symbol), symbol=symbol)

    async def get_open_orders_strict(self, symbol: str) -> list[dict[str, Any]]:
        return await self._call("get_open_orders_strict", lambda: self._delegate.get_open_orders_strict(symbol), symbol=symbol)

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
        return await self._call(
            "place_limit_order",
            lambda: self._delegate.place_limit_order(
                symbol=symbol,
                side=side,
                position_side=position_side,
                qty=qty,
                price=price,
                client_order_id=client_order_id,
            ),
            symbol=symbol,
            side=side.value,
            position_side=position_side.value,
            qty=str(qty),
            price=str(price),
            client_order_id=client_order_id,
        )

    async def place_market_order(
        self,
        *,
        symbol: str,
        side: OrderSide,
        position_side: PositionSide,
        qty: Decimal,
        client_order_id: str,
    ) -> ExchangeOrder:
        return await self._call(
            "place_market_order",
            lambda: self._delegate.place_market_order(
                symbol=symbol,
                side=side,
                position_side=position_side,
                qty=qty,
                client_order_id=client_order_id,
            ),
            symbol=symbol,
            side=side.value,
            position_side=position_side.value,
            qty=str(qty),
            client_order_id=client_order_id,
        )

    async def get_order(self, *, symbol: str, order_id: str) -> ExchangeOrder:
        return await self._call("get_order", lambda: self._delegate.get_order(symbol=symbol, order_id=order_id), symbol=symbol, order_id=order_id)

    async def cancel_order(self, *, symbol: str, order_id: str) -> ExchangeOrder:
        return await self._call("cancel_order", lambda: self._delegate.cancel_order(symbol=symbol, order_id=order_id), symbol=symbol, order_id=order_id)

    def get_cached_order(self, symbol: str, order_id: str) -> ExchangeOrder | None:
        return self._delegate.get_cached_order(symbol, order_id)

    def is_order_stream_healthy(self) -> bool:
        return self._delegate.is_order_stream_healthy()


