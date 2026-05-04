from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from paired_opener.config import Settings
from paired_opener.domain import ExecutionProfile, OrderSide, Quote, SessionKind, SessionSpec, SymbolRules
from paired_opener.rounding import normalize_qty


OPEN_SESSION_KINDS = {SessionKind.PAIRED_OPEN, SessionKind.SINGLE_OPEN}


@dataclass(slots=True, frozen=True)
class ResolvedExecutionPolicy:
    execution_profile: ExecutionProfile
    market_fallback_max_ratio: Decimal
    market_fallback_min_residual_qty: Decimal
    max_reprice_ticks: int | None
    max_spread_bps: int | None
    max_reference_deviation_bps: int | None

    def to_payload(self, *, prefix: str = "") -> dict[str, Any]:
        return {
            f"{prefix}execution_profile": self.execution_profile.value,
            f"{prefix}market_fallback_max_ratio": str(self.market_fallback_max_ratio),
            f"{prefix}market_fallback_min_residual_qty": str(self.market_fallback_min_residual_qty),
            f"{prefix}max_reprice_ticks": self.max_reprice_ticks,
            f"{prefix}max_spread_bps": self.max_spread_bps,
            f"{prefix}max_reference_deviation_bps": self.max_reference_deviation_bps,
        }


@dataclass(slots=True, frozen=True)
class PriceGuardBlock:
    code: str
    message: str
    details: dict[str, Any]


def default_execution_profile(settings: Settings, session_kind: SessionKind) -> ExecutionProfile:
    raw = settings.default_open_execution_profile if session_kind in OPEN_SESSION_KINDS else settings.default_close_execution_profile
    return ExecutionProfile(str(raw))


def resolve_execution_policy(
    settings: Settings,
    *,
    session_kind: SessionKind,
    execution_profile: ExecutionProfile | str | None = None,
    market_fallback_max_ratio: Decimal | str | None = None,
    market_fallback_min_residual_qty: Decimal | str | None = None,
    max_reprice_ticks: int | None = None,
    max_spread_bps: int | None = None,
    max_reference_deviation_bps: int | None = None,
) -> ResolvedExecutionPolicy:
    profile = ExecutionProfile(str(execution_profile)) if execution_profile is not None else default_execution_profile(settings, session_kind)
    if profile == ExecutionProfile.MAKER_FIRST:
        fallback_ratio = Decimal(str(market_fallback_max_ratio)) if market_fallback_max_ratio is not None else settings.maker_first_market_fallback_max_ratio
        fallback_residual = (
            Decimal(str(market_fallback_min_residual_qty))
            if market_fallback_min_residual_qty is not None
            else settings.maker_first_market_fallback_min_residual_qty
        )
        resolved_max_reprice_ticks = int(max_reprice_ticks if max_reprice_ticks is not None else settings.maker_first_max_reprice_ticks)
        resolved_max_spread_bps = int(max_spread_bps if max_spread_bps is not None else settings.maker_first_max_spread_bps)
        resolved_max_reference_deviation_bps = int(
            max_reference_deviation_bps
            if max_reference_deviation_bps is not None
            else settings.maker_first_max_reference_deviation_bps
        )
    else:
        fallback_ratio = Decimal(str(market_fallback_max_ratio)) if market_fallback_max_ratio is not None else settings.balanced_market_fallback_max_ratio
        fallback_residual = (
            Decimal(str(market_fallback_min_residual_qty))
            if market_fallback_min_residual_qty is not None
            else settings.balanced_market_fallback_min_residual_qty
        )
        resolved_max_reprice_ticks = int(max_reprice_ticks if max_reprice_ticks is not None else settings.balanced_max_reprice_ticks)
        resolved_max_spread_bps = int(max_spread_bps if max_spread_bps is not None else settings.balanced_max_spread_bps)
        resolved_max_reference_deviation_bps = int(
            max_reference_deviation_bps
            if max_reference_deviation_bps is not None
            else settings.balanced_max_reference_deviation_bps
        )
    return ResolvedExecutionPolicy(
        execution_profile=profile,
        market_fallback_max_ratio=max(fallback_ratio, Decimal("0")),
        market_fallback_min_residual_qty=max(fallback_residual, Decimal("0")),
        max_reprice_ticks=max(resolved_max_reprice_ticks, 0),
        max_spread_bps=max(resolved_max_spread_bps, 0) if resolved_max_spread_bps is not None else None,
        max_reference_deviation_bps=max(resolved_max_reference_deviation_bps, 0)
        if resolved_max_reference_deviation_bps is not None
        else None,
    )


def policy_from_spec(spec: SessionSpec) -> ResolvedExecutionPolicy:
    return ResolvedExecutionPolicy(
        execution_profile=spec.execution_profile,
        market_fallback_max_ratio=spec.market_fallback_max_ratio,
        market_fallback_min_residual_qty=spec.market_fallback_min_residual_qty,
        max_reprice_ticks=spec.max_reprice_ticks,
        max_spread_bps=spec.max_spread_bps,
        max_reference_deviation_bps=spec.max_reference_deviation_bps,
    )


def compute_market_fallback_qty(policy: ResolvedExecutionPolicy, remaining_qty: Decimal, rules: SymbolRules) -> Decimal:
    normalized_remaining = normalize_qty(remaining_qty, rules)
    if normalized_remaining <= Decimal("0") or policy.market_fallback_max_ratio <= Decimal("0"):
        return Decimal("0")
    by_ratio = normalized_remaining * policy.market_fallback_max_ratio
    by_residual = normalized_remaining - policy.market_fallback_min_residual_qty
    fallback_qty = min(by_ratio, by_residual)
    if fallback_qty <= Decimal("0"):
        return Decimal("0")
    return normalize_qty(fallback_qty, rules)


def evaluate_price_guard(
    *,
    policy: ResolvedExecutionPolicy,
    side: OrderSide,
    quote: Quote,
    candidate_price: Decimal,
    reference_price: Decimal,
    rules: SymbolRules,
) -> PriceGuardBlock | None:
    bid_price = Decimal(quote.bid_price)
    ask_price = Decimal(quote.ask_price)
    if policy.max_spread_bps is not None:
        midpoint = (bid_price + ask_price) / Decimal("2") if (bid_price + ask_price) > Decimal("0") else Decimal("0")
        if midpoint > Decimal("0"):
            spread_bps = ((ask_price - bid_price) / midpoint) * Decimal("10000")
            if spread_bps > Decimal(policy.max_spread_bps):
                return PriceGuardBlock(
                    code="max_spread_bps",
                    message="Spread exceeds the allowed threshold; stop executing this leg.",
                    details={
                        "spread_bps": str(spread_bps.quantize(Decimal("0.01"))),
                        "max_spread_bps": policy.max_spread_bps,
                    },
                )

    adverse_move = candidate_price - reference_price if side == OrderSide.BUY else reference_price - candidate_price
    if adverse_move < Decimal("0"):
        adverse_move = Decimal("0")
    if policy.max_reprice_ticks is not None and rules.tick_size > Decimal("0"):
        ticks = adverse_move / rules.tick_size
        if ticks > Decimal(policy.max_reprice_ticks):
            return PriceGuardBlock(
                code="max_reprice_ticks",
                message="Passive price moved too far from the reference price in ticks; stop this leg.",
                details={
                    "reference_price": str(reference_price),
                    "candidate_price": str(candidate_price),
                    "ticks": str(ticks.quantize(Decimal("0.01"))),
                    "max_reprice_ticks": policy.max_reprice_ticks,
                },
            )
    if policy.max_reference_deviation_bps is not None and reference_price > Decimal("0"):
        deviation_bps = (adverse_move / reference_price) * Decimal("10000")
        if deviation_bps > Decimal(policy.max_reference_deviation_bps):
            return PriceGuardBlock(
                code="max_reference_deviation_bps",
                message="Passive price deviated too far from the reference price; stop this leg.",
                details={
                    "reference_price": str(reference_price),
                    "candidate_price": str(candidate_price),
                    "deviation_bps": str(deviation_bps.quantize(Decimal("0.01"))),
                    "max_reference_deviation_bps": policy.max_reference_deviation_bps,
                },
            )
    return None
