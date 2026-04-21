from __future__ import annotations

from enum import StrEnum
from typing import Any

from app_i18n.runtime import format_reason, make_api_detail

class ErrorCategory(StrEnum):
    RATE_LIMIT = "rate_limit"
    AUTH_ERROR = "auth_error"
    PERMISSION_ERROR = "permission_error"
    NETWORK_TIMEOUT = "network_timeout"
    MATCHING_FAILURE = "matching_failure"
    INSUFFICIENT_BALANCE = "insufficient_balance"
    INVALID_PARAMETER = "invalid_parameter"
    EXCHANGE_STATE = "exchange_state"
    UNKNOWN = "unknown"


class ErrorStrategy(StrEnum):
    RETRY = "retry"
    TERMINATE = "terminate"
    MANUAL_INTERVENTION = "manual_intervention"


_DEFAULT_HTTP_STATUS: dict[ErrorCategory, int] = {
    ErrorCategory.INVALID_PARAMETER: 422,
    ErrorCategory.AUTH_ERROR: 401,
    ErrorCategory.PERMISSION_ERROR: 403,
    ErrorCategory.RATE_LIMIT: 429,
    ErrorCategory.INSUFFICIENT_BALANCE: 409,
    ErrorCategory.MATCHING_FAILURE: 409,
    ErrorCategory.EXCHANGE_STATE: 409,
    ErrorCategory.NETWORK_TIMEOUT: 503,
    ErrorCategory.UNKNOWN: 500,
}


class TradingError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        category: ErrorCategory,
        strategy: ErrorStrategy,
        source: str,
        code: str,
        params: dict[str, Any] | None = None,
        raw_code: int | str | None = None,
        raw_message: str | None = None,
        operator_action: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.category = category
        self.strategy = strategy
        self.source = source
        self.code = code
        self.params = dict(params or {})
        self.raw_code = raw_code
        self.raw_message = raw_message or message
        self.operator_action = operator_action
        self.context = dict(context or {})

    @property
    def retryable(self) -> bool:
        return self.strategy == ErrorStrategy.RETRY

    def with_context(self, **context: Any) -> "TradingError":
        clean = {key: value for key, value in context.items() if value is not None}
        if clean:
            self.context.update(clean)
        return self

    def with_source(self, source: str) -> "TradingError":
        if source:
            self.source = source
        return self

    def to_detail(self, *, precheck: dict[str, Any] | None = None) -> dict[str, Any]:
        return make_api_detail(
            code=self.code,
            params=self.params,
            category=self.category.value,
            strategy=self.strategy.value,
            source=self.source,
            raw_code=self.raw_code,
            raw_message=self.raw_message,
            operator_action=self.operator_action,
            context=self.context,
            message=format_reason(self.code, self.params) if self.params else str(self),
            precheck=precheck,
        )

    def to_event_payload(self) -> dict[str, Any]:
        return {
            "error": str(self),
            "error_category": self.category.value,
            "error_strategy": self.strategy.value,
            "error_code": self.code,
            "error_params": self.params,
            "retryable": self.retryable,
            "operator_action": self.operator_action,
            "source": self.source,
            "context": self.context,
        }


class ExchangeStateError(TradingError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "exchange_state",
        source: str = "service",
        raw_code: int | str | None = None,
        raw_message: str | None = None,
        operator_action: str | None = "检查交易所账户状态、持仓模式或杠杆设置后重试。",
        context: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            message,
            category=ErrorCategory.EXCHANGE_STATE,
            strategy=ErrorStrategy.MANUAL_INTERVENTION,
            source=source,
            code=code,
            raw_code=raw_code,
            raw_message=raw_message,
            operator_action=operator_action,
            context=context,
            params=None,
        )


class MatchingFailureError(TradingError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "matching_failure",
        source: str = "engine",
        raw_code: int | str | None = None,
        raw_message: str | None = None,
        operator_action: str | None = "检查订单状态和市场流动性后重新发起会话。",
        context: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            message,
            category=ErrorCategory.MATCHING_FAILURE,
            strategy=ErrorStrategy.TERMINATE,
            source=source,
            code=code,
            raw_code=raw_code,
            raw_message=raw_message,
            operator_action=operator_action,
            context=context,
            params=None,
        )


def invalid_parameter_error(
    message: str,
    *,
    source: str,
    code: str = "invalid_parameter",
    raw_code: int | str | None = None,
    raw_message: str | None = None,
    operator_action: str | None = None,
    context: dict[str, Any] | None = None,
) -> TradingError:
    return TradingError(
        message,
        category=ErrorCategory.INVALID_PARAMETER,
        strategy=ErrorStrategy.TERMINATE,
        source=source,
        code=code,
        raw_code=raw_code,
        raw_message=raw_message,
        operator_action=operator_action,
        context=context,
        params=None,
    )


def unknown_trading_error(
    message: str,
    *,
    source: str,
    code: str = "unknown_error",
    raw_code: int | str | None = None,
    raw_message: str | None = None,
    operator_action: str | None = "检查日志和交易所返回信息，确认原因后再继续操作。",
    context: dict[str, Any] | None = None,
) -> TradingError:
    return TradingError(
        message,
        category=ErrorCategory.UNKNOWN,
        strategy=ErrorStrategy.MANUAL_INTERVENTION,
        source=source,
        code=code,
        raw_code=raw_code,
        raw_message=raw_message,
        operator_action=operator_action,
        context=context,
        params=None,
    )


def ensure_trading_error(
    exc: Exception,
    *,
    source: str,
    code: str | None = None,
    context: dict[str, Any] | None = None,
    default_message: str | None = None,
    operator_action: str | None = None,
) -> TradingError:
    if isinstance(exc, TradingError):
        if context:
            exc.with_context(**context)
        if source:
            exc.with_source(source)
        return exc
    if isinstance(exc, ValueError):
        return invalid_parameter_error(
            default_message or str(exc),
            source=source,
            code=code or "invalid_parameter",
            raw_message=str(exc),
            operator_action=operator_action,
            context=context,
        )
    return unknown_trading_error(
        default_message or str(exc) or "未知交易错误",
        source=source,
        code=code or "unknown_error",
        raw_message=str(exc),
        operator_action=operator_action,
        context=context,
    )


def http_status_for_error(error: TradingError) -> int:
    return _DEFAULT_HTTP_STATUS.get(error.category, 500)
