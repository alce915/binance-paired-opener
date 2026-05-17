from __future__ import annotations

from decimal import Decimal

import pytest

from paired_opener.domain import PositionSide
from paired_opener.kanglong.models import KanglongAccountSnapshot
from paired_opener.kanglong.snapshots import build_snapshot_bundle, monitor_account_to_kanglong_snapshot


def test_monitor_payload_converts_to_kanglong_snapshot_for_symbol() -> None:
    account = {
        "account_id": "sub1",
        "account_name": "Sub 1",
        "updated_at": "2026-05-17T00:00:00+00:00",
        "totals": {
            "available_balance": "1000",
            "equity": "1200",
            "margin": "100",
        },
        "positions": [
            {
                "symbol": "ETHUSDC",
                "position_side": "LONG",
                "position_amt": "1.5",
                "entry_price": "3000",
                "mark_price": "3100",
                "unrealized_pnl": "150",
            },
            {
                "symbol": "ETHUSDC",
                "position_side": "SHORT",
                "position_amt": "1.5",
                "entry_price": "3200",
                "mark_price": "3100",
                "unrealized_pnl": "150",
            },
        ],
    }

    snapshot = monitor_account_to_kanglong_snapshot(account, symbol="ETHUSDC", leverage=75)

    assert snapshot.account_id == "sub1"
    assert snapshot.available_balance == Decimal("1000")
    assert snapshot.qty(PositionSide.LONG) == Decimal("1.5")
    assert snapshot.pnl(PositionSide.SHORT) == Decimal("150")
    assert snapshot.snapshot_version == "sub1:2026-05-17T00:00:00+00:00"


def test_monitor_payload_derives_short_side_from_negative_position_amount() -> None:
    account = {
        "account_id": "sub-short",
        "account_name": "Sub Short",
        "updated_at": "2026-05-17T00:00:00+00:00",
        "totals": {"available_balance": "1", "equity": "1", "margin": "0"},
        "positions": [
            {
                "symbol": "ETHUSDC",
                "position_amt": "-2",
                "entry_price": "3200",
                "mark_price": "3100",
                "unrealized_pnl": "200",
            },
        ],
    }

    snapshot = monitor_account_to_kanglong_snapshot(account, symbol="ETHUSDC", leverage=75)

    assert snapshot.qty(PositionSide.SHORT) == Decimal("2")
    assert snapshot.qty(PositionSide.LONG) == Decimal("0")


def test_monitor_payload_derives_short_side_from_unknown_explicit_side() -> None:
    account = {
        "account_id": "sub-unknown",
        "account_name": "Sub Unknown",
        "updated_at": "2026-05-17T00:00:00+00:00",
        "totals": {"available_balance": "1", "equity": "1", "margin": "0"},
        "positions": [
            {
                "symbol": "ETHUSDC",
                "position_side": "UNKNOWN",
                "position_amt": "-2",
                "entry_price": "3200",
                "mark_price": "3100",
                "unrealized_pnl": "200",
            },
        ],
    }

    snapshot = monitor_account_to_kanglong_snapshot(account, symbol="ETHUSDC", leverage=75)

    assert snapshot.qty(PositionSide.SHORT) == Decimal("2")
    assert snapshot.qty(PositionSide.LONG) == Decimal("0")


@pytest.mark.parametrize("explicit_side", ["UNKNOWN", "BOTH"])
def test_monitor_payload_rejects_zero_quantity_with_unknown_explicit_side(
    explicit_side: str,
) -> None:
    account_id = f"sub-{explicit_side.lower()}-zero"
    account = {
        "account_id": account_id,
        "account_name": "Sub Unknown Zero",
        "updated_at": "2026-05-17T00:00:00+00:00",
        "totals": {"available_balance": "1", "equity": "1", "margin": "0"},
        "positions": [
            {
                "symbol": "ETHUSDC",
                "position_side": explicit_side,
                "position_amt": "0",
                "entry_price": "3200",
                "mark_price": "3100",
                "unrealized_pnl": "0",
            },
        ],
    }

    with pytest.raises(ValueError, match=f"{account_id}.*ETHUSDC"):
        monitor_account_to_kanglong_snapshot(account, symbol="ETHUSDC", leverage=75)


def test_monitor_payload_rejects_zero_quantity_without_explicit_side() -> None:
    account = {
        "account_id": "sub-zero",
        "account_name": "Sub Zero",
        "updated_at": "2026-05-17T00:00:00+00:00",
        "totals": {"available_balance": "1", "equity": "1", "margin": "0"},
        "positions": [
            {
                "symbol": "ETHUSDC",
                "position_amt": "0",
                "entry_price": "3200",
                "mark_price": "3100",
                "unrealized_pnl": "0",
            },
        ],
    }

    with pytest.raises(ValueError, match="sub-zero.*ETHUSDC"):
        monitor_account_to_kanglong_snapshot(account, symbol="ETHUSDC", leverage=75)


def test_snapshot_bundle_id_is_stable_for_same_inputs() -> None:
    bundle_a = build_snapshot_bundle(
        symbol="ETHUSDC",
        accounts=[
            {
                "account_id": "sub1",
                "account_name": "Sub 1",
                "updated_at": "2026-05-17T00:00:00+00:00",
                "totals": {"available_balance": "1", "equity": "1", "margin": "0"},
                "positions": [],
            }
        ],
        config_version="cfg-1",
        symbol_rule_version="rules-1",
        price_version="price-1",
        leverage=75,
    )
    bundle_b = build_snapshot_bundle(
        symbol="ETHUSDC",
        accounts=[
            {
                "account_id": "sub1",
                "account_name": "Sub 1",
                "updated_at": "2026-05-17T00:00:00+00:00",
                "totals": {"available_balance": "1", "equity": "1", "margin": "0"},
                "positions": [],
            }
        ],
        config_version="cfg-1",
        symbol_rule_version="rules-1",
        price_version="price-1",
        leverage=75,
    )

    assert bundle_a["snapshot_bundle_id"] == bundle_b["snapshot_bundle_id"]
    assert isinstance(bundle_a["accounts"][0], KanglongAccountSnapshot)
    assert bundle_a["accounts"][0].account_id == "sub1"


def test_snapshot_bundle_id_is_stable_across_account_order() -> None:
    sub1 = {
        "account_id": "sub1",
        "account_name": "Sub 1",
        "updated_at": "2026-05-17T00:00:00+00:00",
        "totals": {"available_balance": "1", "equity": "1", "margin": "0"},
        "positions": [],
    }
    sub2 = {
        "account_id": "sub2",
        "account_name": "Sub 2",
        "updated_at": "2026-05-17T00:01:00+00:00",
        "totals": {"available_balance": "2", "equity": "2", "margin": "0"},
        "positions": [],
    }

    bundle_a = build_snapshot_bundle(
        symbol="ETHUSDC",
        accounts=[sub1, sub2],
        config_version="cfg-1",
        symbol_rule_version="rules-1",
        price_version="price-1",
        leverage=75,
    )
    bundle_b = build_snapshot_bundle(
        symbol="ETHUSDC",
        accounts=[sub2, sub1],
        config_version="cfg-1",
        symbol_rule_version="rules-1",
        price_version="price-1",
        leverage=75,
    )

    assert bundle_a["snapshot_bundle_id"] == bundle_b["snapshot_bundle_id"]
    assert [account.account_id for account in bundle_a["accounts"]] == ["sub1", "sub2"]
    assert [account.account_id for account in bundle_b["accounts"]] == ["sub2", "sub1"]
