from __future__ import annotations

import asyncio
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Awaitable, Callable

from paired_opener.exchange import RateLimitObservation


CALCULATION_VERSION = "kanglong-capacity-v1"
_FAST_COMPONENTS = {"account", "positions", "open_orders", "quote", "order_book"}
_SLOW_COMPONENTS = {"symbol_config", "leverage_bracket", "commission_rate"}


@dataclass(frozen=True, slots=True)
class CapacityPolicy:
    margin_safety_ratio: Decimal = Decimal("0.10")
    price_buffer_bps: int = 20
    max_notional_ratio: Decimal = Decimal("0.90")
    min_liquidation_buffer_ratio: Decimal = Decimal("0.20")


@dataclass(frozen=True, slots=True)
class EffectiveLeverageBracket:
    bracket: int
    effective_floor: Decimal
    effective_cap: Decimal
    max_allowed_leverage: int
    notional_coef: Decimal = Decimal("1")
    notional_floor: Decimal = Decimal("0")
    notional_cap: Decimal = Decimal("0")

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "EffectiveLeverageBracket":
        return cls(
            bracket=int(payload.get("bracket") or 0),
            effective_floor=Decimal(str(payload.get("effective_floor") or "0")),
            effective_cap=Decimal(str(payload.get("effective_cap") or "0")),
            max_allowed_leverage=int(payload.get("max_allowed_leverage") or 0),
            notional_coef=Decimal(str(payload.get("notional_coef") or "1")),
            notional_floor=Decimal(str(payload.get("notional_floor") or "0")),
            notional_cap=Decimal(str(payload.get("notional_cap") or "0")),
        )


@dataclass(frozen=True, slots=True)
class AccountCapacityEstimate:
    requested_gross_notional: Decimal
    capacity_requested_gross_notional: Decimal
    existing_symbol_exposure: Decimal
    projected_symbol_exposure: Decimal
    conservative_openable_notional: Decimal
    estimated_capacity_usage_percent: Decimal | None
    limiting_factor: str
    requested_leverage: int
    current_symbol_leverage: int
    bracket_max_allowed_leverage: int
    bracket_notional_coef: Decimal
    selected_bracket_effective_cap: Decimal
    current_symbol_max_notional_value: Decimal
    effective_capacity_leverage: int
    margin_capacity_notional: Decimal
    equity_capacity_notional: Decimal
    liquidation_capacity_notional: Decimal
    bracket_remaining_notional: Decimal
    symbol_config_remaining_notional: Decimal
    blocked: bool
    blocked_reason: str | None
    warnings: tuple[str, ...]

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["warnings"] = list(self.warnings)
        return payload


def select_effective_bracket(
    brackets: list[EffectiveLeverageBracket],
    projected_exposure: Decimal,
) -> tuple[EffectiveLeverageBracket, bool]:
    if not brackets:
        raise ValueError("at least one effective leverage bracket is required")
    ordered = sorted(brackets, key=lambda item: (item.effective_floor, item.effective_cap))
    for index, bracket in enumerate(ordered):
        is_final = index == len(ordered) - 1
        if projected_exposure >= bracket.effective_floor and (
            projected_exposure < bracket.effective_cap
            or is_final and projected_exposure <= bracket.effective_cap
        ):
            return bracket, False
    return ordered[-1], projected_exposure > ordered[-1].effective_cap


def estimate_account_capacity(
    *,
    per_leg_notional: Decimal,
    requested_leverage: int,
    current_symbol_leverage: int,
    current_symbol_max_notional_value: Decimal,
    brackets: list[EffectiveLeverageBracket],
    available_balance: Decimal,
    equity: Decimal,
    maker_fee_rate: Decimal,
    taker_fee_rate: Decimal,
    existing_symbol_exposure: Decimal,
    policy: CapacityPolicy,
    capacity_requested_gross_notional: Decimal | None = None,
) -> AccountCapacityEstimate:
    requested_gross = Decimal(per_leg_notional) * Decimal("2")
    capacity_requested_gross = (
        Decimal(capacity_requested_gross_notional)
        if capacity_requested_gross_notional is not None
        else requested_gross
    )
    existing_exposure = max(Decimal(existing_symbol_exposure), Decimal("0"))
    projected_exposure = existing_exposure + capacity_requested_gross
    selected_bracket, projected_above_final_cap = select_effective_bracket(brackets, projected_exposure)
    effective_leverage = max(
        min(
            int(requested_leverage),
            max(int(current_symbol_leverage), 1),
            max(int(selected_bracket.max_allowed_leverage), 1),
        ),
        1,
    )
    available_after_safety = max(
        Decimal(available_balance) * (Decimal("1") - Decimal(policy.margin_safety_ratio)),
        Decimal("0"),
    )
    fee_reserve_rate = max(Decimal(maker_fee_rate), Decimal(taker_fee_rate), Decimal("0"))
    cost_rate = (
        Decimal("1") / Decimal(effective_leverage)
        + fee_reserve_rate
        + Decimal(policy.price_buffer_bps) / Decimal("10000")
    )
    margin_capacity = available_after_safety / cost_rate if cost_rate > 0 else Decimal("0")
    equity_capacity = max(
        Decimal(equity) * Decimal(effective_leverage) * Decimal(policy.max_notional_ratio),
        Decimal("0"),
    )
    liquidation_capacity = max(
        Decimal(equity)
        * Decimal(effective_leverage)
        * (Decimal("1") - Decimal(policy.min_liquidation_buffer_ratio)),
        Decimal("0"),
    )
    bracket_remaining = max(selected_bracket.effective_cap - existing_exposure, Decimal("0"))
    symbol_config_remaining = max(
        Decimal(current_symbol_max_notional_value) - existing_exposure,
        Decimal("0"),
    )
    constraints = {
        "margin_capacity": margin_capacity,
        "equity_capacity": equity_capacity,
        "liquidation_buffer": liquidation_capacity,
        "leverage_bracket": bracket_remaining,
        "symbol_max_notional": symbol_config_remaining,
    }
    limiting_factor, conservative_openable = min(constraints.items(), key=lambda item: item[1])
    estimated_usage = (
        capacity_requested_gross / conservative_openable * Decimal("100")
        if conservative_openable > 0
        else None
    )
    blocked_reason: str | None = None
    if projected_above_final_cap:
        blocked_reason = "projected_exposure_exceeds_final_bracket"
    elif int(requested_leverage) > int(selected_bracket.max_allowed_leverage):
        blocked_reason = "requested_leverage_exceeds_projected_bracket"
    elif estimated_usage is None or estimated_usage > Decimal("100"):
        blocked_reason = "estimated_capacity_exceeded"
    warnings: list[str] = []
    if int(current_symbol_leverage) < int(requested_leverage):
        warnings.append("current_leverage_below_requested")
    if estimated_usage is not None and Decimal("80") <= estimated_usage <= Decimal("100"):
        warnings.append("estimated_capacity_high")
    return AccountCapacityEstimate(
        requested_gross_notional=requested_gross,
        capacity_requested_gross_notional=capacity_requested_gross,
        existing_symbol_exposure=existing_exposure,
        projected_symbol_exposure=projected_exposure,
        conservative_openable_notional=conservative_openable,
        estimated_capacity_usage_percent=estimated_usage,
        limiting_factor=limiting_factor,
        requested_leverage=int(requested_leverage),
        current_symbol_leverage=int(current_symbol_leverage),
        bracket_max_allowed_leverage=int(selected_bracket.max_allowed_leverage),
        bracket_notional_coef=selected_bracket.notional_coef,
        selected_bracket_effective_cap=selected_bracket.effective_cap,
        current_symbol_max_notional_value=Decimal(current_symbol_max_notional_value),
        effective_capacity_leverage=effective_leverage,
        margin_capacity_notional=margin_capacity,
        equity_capacity_notional=equity_capacity,
        liquidation_capacity_notional=liquidation_capacity,
        bracket_remaining_notional=bracket_remaining,
        symbol_config_remaining_notional=symbol_config_remaining,
        blocked=blocked_reason is not None,
        blocked_reason=blocked_reason,
        warnings=tuple(warnings),
    )


def estimate_batch_capacity(estimates: list[tuple[str, AccountCapacityEstimate]]) -> dict[str, Any]:
    requested = sum((item.capacity_requested_gross_notional for _, item in estimates), Decimal("0"))
    capacity = sum((item.conservative_openable_notional for _, item in estimates), Decimal("0"))
    usage = requested / capacity * Decimal("100") if capacity > 0 else None
    comparable = [
        (account_id, estimate.estimated_capacity_usage_percent)
        for account_id, estimate in estimates
        if estimate.estimated_capacity_usage_percent is not None
    ]
    bottleneck = max(comparable, key=lambda item: item[1])[0] if comparable else None
    return {
        "batch_requested_gross_notional": requested,
        "batch_conservative_openable_notional": capacity,
        "batch_estimated_usage_percent": usage,
        "bottleneck_account_id": bottleneck,
        "batch_blocked": any(estimate.blocked for _, estimate in estimates),
    }


@dataclass(frozen=True, slots=True)
class _CachedComponent:
    value: Any
    observed_at: datetime
    stored_at: float


@dataclass(frozen=True, slots=True)
class CapacitySnapshot:
    account_id: str
    symbol: str
    account_equity: Decimal
    available_balance: Decimal
    current_symbol_leverage: int
    current_symbol_max_notional_value: Decimal
    notional_coef: Decimal
    brackets: tuple[EffectiveLeverageBracket, ...]
    existing_symbol_exposure: Decimal
    maker_fee_rate: Decimal
    taker_fee_rate: Decimal
    account_status: str
    hedge_mode: bool
    blocked_reasons: tuple[str, ...]
    assembled_at: datetime
    oldest_component_at: datetime
    snapshot_components: dict[str, dict[str, Any]]
    all_components_fresh: bool


class _SharedRateLimitBudget:
    def __init__(self) -> None:
        self._blocked_until = 0.0

    def observe(self, observation: RateLimitObservation) -> None:
        if observation.http_status not in {418, 429} or observation.retry_after_seconds is None:
            return
        self._blocked_until = max(
            self._blocked_until,
            time.monotonic() + float(max(observation.retry_after_seconds, Decimal("0"))),
        )

    async def wait(self) -> None:
        remaining = self._blocked_until - time.monotonic()
        if remaining > 0:
            await asyncio.sleep(remaining)


class CapacitySnapshotCoordinator:
    def __init__(
        self,
        runtime_manager,
        *,
        fast_ttl_ms: int = 3_000,
        slow_ttl_ms: int = 60_000,
        private_concurrency: int = 4,
    ) -> None:
        self._runtime_manager = runtime_manager
        self._fast_ttl_ms = max(int(fast_ttl_ms), 1)
        self._slow_ttl_ms = max(int(slow_ttl_ms), 1)
        self._private_semaphore = asyncio.Semaphore(max(int(private_concurrency), 1))
        self._cache: dict[tuple[Any, ...], _CachedComponent] = {}
        self._inflight: dict[tuple[Any, ...], asyncio.Task[_CachedComponent]] = {}
        self._lock = asyncio.Lock()
        self._rate_budget = _SharedRateLimitBudget()
        self._observed_gateway_ids: set[int] = set()

    async def get_snapshot(
        self,
        credential_revision: str,
        account_id: str,
        symbol: str,
        *,
        force_refresh: bool = False,
    ) -> CapacitySnapshot:
        target = symbol.upper()
        gateway = self._runtime_manager.current(account_id).gateway
        self._attach_rate_limit_observer(gateway)

        async def load_private() -> Any:
            async with self._private_semaphore:
                await self._rate_budget.wait()
                # 这里只采集可复用的原始账户/档位快照；实际请求杠杆与投影敞口
                # 在 estimate_account_capacity() 中按每次表单输入计算，不能让内部
                # 的占位杠杆提前制造 requested_leverage blocker。
                return await gateway.get_portfolio_margin_precheck(target, 1, Decimal("0"))

        async def load_quote() -> Any:
            method = gateway.refresh_quote if force_refresh else gateway.get_quote
            return await method(target)

        async def load_order_book() -> Any:
            if force_refresh and hasattr(gateway, "refresh_order_book"):
                return await gateway.refresh_order_book(target, limit=20)
            return await gateway.get_order_book(target, limit=20)

        private, quote, order_book = await asyncio.gather(
            self._get_component(
                ("private", credential_revision, account_id, target),
                load_private,
                ttl_ms=self._fast_ttl_ms,
                force_refresh=force_refresh,
            ),
            self._get_component(
                ("public", target, "quote"),
                load_quote,
                ttl_ms=self._fast_ttl_ms,
                force_refresh=force_refresh,
            ),
            self._get_component(
                ("public", target, "order_book", 20),
                load_order_book,
                ttl_ms=self._fast_ttl_ms,
                force_refresh=force_refresh,
            ),
        )
        return self._assemble_snapshot(account_id, target, private, quote, order_book)

    async def refresh_capacity(
        self,
        credential_revision: str,
        account_ids: list[str],
        symbol: str,
        *,
        force_refresh: bool = False,
    ) -> dict[str, CapacitySnapshot]:
        snapshots = await asyncio.gather(
            *[
                self.get_snapshot(
                    credential_revision,
                    account_id,
                    symbol,
                    force_refresh=force_refresh,
                )
                for account_id in account_ids
            ]
        )
        return {snapshot.account_id: snapshot for snapshot in snapshots}

    async def _get_component(
        self,
        key: tuple[Any, ...],
        loader: Callable[[], Awaitable[Any]],
        *,
        ttl_ms: int,
        force_refresh: bool,
    ) -> tuple[_CachedComponent, str]:
        owner = False
        async with self._lock:
            cached = self._cache.get(key)
            if not force_refresh and cached is not None and (time.monotonic() - cached.stored_at) * 1000 <= ttl_ms:
                return cached, "cache"
            task = self._inflight.get(key)
            if task is None:
                task = asyncio.create_task(self._load_component(loader))
                self._inflight[key] = task
                owner = True
        try:
            component = await task
        finally:
            if owner:
                async with self._lock:
                    self._inflight.pop(key, None)
        if owner:
            async with self._lock:
                self._cache[key] = component
                self._prune_cache_locked()
        return component, "upstream"

    @staticmethod
    async def _load_component(loader: Callable[[], Awaitable[Any]]) -> _CachedComponent:
        value = await loader()
        return _CachedComponent(value=value, observed_at=datetime.now(UTC), stored_at=time.monotonic())

    def _prune_cache_locked(self) -> None:
        if len(self._cache) <= 2048:
            return
        oldest = sorted(self._cache, key=lambda key: self._cache[key].stored_at)[: len(self._cache) - 2048]
        for key in oldest:
            self._cache.pop(key, None)

    def _attach_rate_limit_observer(self, gateway: Any) -> None:
        delegate = getattr(gateway, "_delegate", gateway)
        # 只读 facade 会为能力隔离隐藏完整 gateway；observer 属性本身已代理到
        # 稳定的底层 Binance gateway，因此以属性所有者而非临时 facade 的 id 去重。
        observer_owner = getattr(gateway, "_rate_limit_observer_owner", delegate)
        identity = id(observer_owner)
        if identity in self._observed_gateway_ids or not hasattr(gateway, "_rate_limit_observer"):
            return
        previous = gateway._rate_limit_observer

        def combined(observation: RateLimitObservation) -> None:
            self._rate_budget.observe(observation)
            if previous is not None:
                previous(observation)

        gateway._rate_limit_observer = combined
        self._observed_gateway_ids.add(identity)

    def _assemble_snapshot(
        self,
        account_id: str,
        symbol: str,
        private_result: tuple[_CachedComponent, str],
        quote_result: tuple[_CachedComponent, str],
        order_book_result: tuple[_CachedComponent, str],
    ) -> CapacitySnapshot:
        private, private_source = private_result
        quote, quote_source = quote_result
        order_book, order_book_source = order_book_result
        payload = private.value
        now = datetime.now(UTC)
        private_observed_at = payload.get("component_observed_at") or {}

        def private_component(name: str) -> _CachedComponent:
            observed_at = private_observed_at.get(name)
            if not isinstance(observed_at, datetime):
                observed_at = private.observed_at
            elif observed_at.tzinfo is None:
                observed_at = observed_at.replace(tzinfo=UTC)
            return _CachedComponent(
                value=private.value,
                observed_at=observed_at,
                stored_at=private.stored_at,
            )

        component_specs = {
            "account": (private_component("account"), private_source, self._fast_ttl_ms, {
                "account_status": payload.get("account_status"),
                "account_equity": payload.get("account_equity"),
                "available_balance": payload.get("available_balance"),
            }),
            "positions": (private_component("positions"), private_source, self._fast_ttl_ms, {
                "count": len(payload.get("positions") or []),
                "existing_symbol_exposure": payload.get("existing_symbol_exposure"),
            }),
            "open_orders": (private_component("open_orders"), private_source, self._fast_ttl_ms, {
                "count": len(payload.get("open_orders") or []),
            }),
            "symbol_config": (private_component("symbol_config"), private_source, self._slow_ttl_ms, {
                "current_leverage": payload.get("current_leverage"),
                "max_notional_value": payload.get("current_symbol_max_notional_value"),
            }),
            "leverage_bracket": (private_component("leverage_bracket"), private_source, self._slow_ttl_ms, {
                "notional_coef": payload.get("notional_coef"),
                "bracket_count": len(payload.get("brackets") or []),
            }),
            "commission_rate": (private_component("commission_rate"), private_source, self._slow_ttl_ms, payload.get("commission_rates") or {}),
            "quote": (quote, quote_source, self._fast_ttl_ms, {
                "bid_price": getattr(quote.value, "bid_price", None),
                "ask_price": getattr(quote.value, "ask_price", None),
            }),
            "order_book": (order_book, order_book_source, self._fast_ttl_ms, {
                "bid_levels": len(order_book.value.get("bids") or []),
                "ask_levels": len(order_book.value.get("asks") or []),
            }),
        }
        components: dict[str, dict[str, Any]] = {}
        for name, (component, source, ttl_ms, details) in component_specs.items():
            age_ms = max(int((now - component.observed_at).total_seconds() * 1000), 0)
            components[name] = {
                "observed_at": component.observed_at,
                "source": source,
                "age_ms": age_ms,
                "ttl_ms": ttl_ms,
                "valid": age_ms <= ttl_ms,
                "details": details,
            }
        observed_times = [component[0].observed_at for component in component_specs.values()]
        rates = payload.get("commission_rates") or {}
        brackets = tuple(EffectiveLeverageBracket.from_payload(item) for item in payload.get("brackets") or [])
        blocked_reasons = list(payload.get("blocked_reasons") or [])
        if str(payload.get("account_status") or "") != "NORMAL":
            blocked_reasons.append("portfolio_margin_account_not_normal")
        if not bool(payload.get("hedge_mode")):
            blocked_reasons.append("hedge_mode_required")
        return CapacitySnapshot(
            account_id=account_id,
            symbol=symbol,
            account_equity=Decimal(str(payload.get("account_equity") or "0")),
            available_balance=Decimal(str(payload.get("available_balance") or "0")),
            current_symbol_leverage=int(payload.get("current_leverage") or 1),
            current_symbol_max_notional_value=Decimal(str(payload.get("current_symbol_max_notional_value") or "0")),
            notional_coef=Decimal(str(payload.get("notional_coef") or "1")),
            brackets=brackets,
            existing_symbol_exposure=Decimal(str(payload.get("existing_symbol_exposure") or "0")),
            maker_fee_rate=Decimal(str(rates.get("maker") or "0")),
            taker_fee_rate=Decimal(str(rates.get("taker") or "0")),
            account_status=str(payload.get("account_status") or ""),
            hedge_mode=bool(payload.get("hedge_mode")),
            blocked_reasons=tuple(dict.fromkeys(blocked_reasons)),
            assembled_at=now,
            oldest_component_at=min(observed_times),
            snapshot_components=components,
            all_components_fresh=all(component["valid"] for component in components.values()),
        )
