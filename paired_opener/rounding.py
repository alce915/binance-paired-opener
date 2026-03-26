from __future__ import annotations

from decimal import Decimal, ROUND_DOWN, ROUND_UP

from paired_opener.domain import SymbolRules


def quantize_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    units = (value / step).quantize(Decimal("1"), rounding=ROUND_DOWN)
    return units * step


def quantize_step_up(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    units = (value / step).quantize(Decimal("1"), rounding=ROUND_UP)
    return units * step


def normalize_price(price: Decimal, rules: SymbolRules) -> Decimal:
    return quantize_step(price, rules.tick_size)


def normalize_qty(qty: Decimal, rules: SymbolRules) -> Decimal:
    return quantize_step(qty, rules.step_size)


def min_qty_for_notional(price: Decimal, rules: SymbolRules) -> Decimal:
    if price <= 0:
        raise ValueError(f"Price {price} must be positive for {rules.symbol}")
    qty_from_notional = quantize_step_up(rules.min_notional / price, rules.step_size)
    return max(rules.min_qty, qty_from_notional)


def validate_qty_and_notional(qty: Decimal, price: Decimal, rules: SymbolRules) -> None:
    if qty < rules.min_qty:
        raise ValueError(f"Quantity {qty} is below minimum {rules.min_qty} for {rules.symbol}")
    notional = qty * price
    if notional < rules.min_notional:
        raise ValueError(
            f"Notional {notional} is below minimum {rules.min_notional} for {rules.symbol}"
        )
