from __future__ import annotations

from decimal import Decimal

import pytest

from paired_opener.kanglong.ledger import KanglongCheckpoint, KanglongLedgerBaseline, KanglongLedgerEntry
from paired_opener.kanglong.reporter import build_ledger_report, summarize_ledger_costs


def _baseline(**overrides: object) -> KanglongLedgerBaseline:
    payload = {
        "run_id": "run-1",
        "account_id": "main",
        "wallet_balance": Decimal("1000"),
        "available_balance": Decimal("900"),
        "equity": Decimal("1000"),
        "margin": Decimal("100"),
        "margin_deficit": Decimal("0"),
        "total_unrealized_pnl": Decimal("0"),
        "long_qty": Decimal("0"),
        "long_entry_price": Decimal("0"),
        "long_mark_price": Decimal("0"),
        "long_leverage": 75,
        "short_qty": Decimal("0"),
        "short_entry_price": Decimal("0"),
        "short_mark_price": Decimal("0"),
        "short_leverage": 75,
    }
    payload.update(overrides)
    return KanglongLedgerBaseline(**payload)


def _entry(**overrides: object) -> KanglongLedgerEntry:
    payload = {
        "run_id": "run-1",
        "checkpoint_id": 1,
        "sequence": 1,
        "operation_id": "run-1:group-0001:round-0001:1",
        "account_id": "main",
        "entry_type": "fee",
        "asset": "USDC",
        "amount": Decimal("-1"),
        "qty_delta": Decimal("0"),
        "margin_delta": Decimal("0"),
        "available_delta": Decimal("-1"),
        "equity_delta": Decimal("-1"),
        "realized_pnl_delta": Decimal("0"),
        "price_wear": Decimal("0"),
        "fee_amount": Decimal("1"),
        "fee_asset": "USDC",
        "operation_payload_hash": "sha256:payload",
        "payload": {"leg": "close"},
    }
    payload.update(overrides)
    return KanglongLedgerEntry(**payload)


def test_report_summary_records_fee_total_by_asset() -> None:
    costs = summarize_ledger_costs(
        [
            {
                "entry_type": "fee",
                "fee_amount": "0.25",
                "fee_asset": "USDC",
                "amount": "-0.25",
            },
            {
                "entry_type": "fee",
                "fee_amount": "0.75",
                "fee_asset": "USDC",
                "amount": "-0.75",
            },
        ]
    )

    assert costs["transfer_fee_cost"] == "1.00"
    assert costs["total_fee_cost"] == "1.00"
    assert costs["fee_by_asset"] == {"USDC": "1.00"}


def test_report_summary_records_price_wear_by_group_and_total() -> None:
    costs = summarize_ledger_costs(
        [
            {
                "entry_type": "price_wear",
                "operation_id": "run-1:group-0001:round-0001:1",
                "price_wear": "1.50",
                "amount": "-1.50",
            },
            {
                "entry_type": "price_wear",
                "operation_id": "run-1:group-0002:round-0001:1",
                "price_wear": "2.25",
                "amount": "-2.25",
            },
        ]
    )

    assert costs["transfer_price_diff_loss"] == "3.75"
    assert costs["total_price_diff_loss"] == "3.75"
    assert costs["transfer_price_diff_pnl"] == "-3.75"
    assert costs["price_wear_by_group"] == {"group-0001": "1.50", "group-0002": "2.25"}


def test_report_summary_records_source_checkpoint_hashes() -> None:
    costs = summarize_ledger_costs(
        [],
        latest_checkpoint={
            "checkpoint_id": 3,
            "ledger_hash": "sha256:ledger",
            "ledger_state_hash": "sha256:state",
        },
    )

    assert costs["source_checkpoint_id"] == 3
    assert costs["source_ledger_hash"] == "sha256:ledger"
    assert costs["source_ledger_state_hash"] == "sha256:state"


def test_report_summary_records_conversion_unavailable_for_non_quote_fee_asset() -> None:
    costs = summarize_ledger_costs(
        [
            {
                "entry_type": "fee",
                "fee_amount": "0.01",
                "fee_asset": "BNB",
                "amount": "-0.01",
            }
        ],
        symbol="ETHUSDC",
    )

    assert costs["fee_by_asset"] == {"BNB": "0.01"}
    assert costs["total_fee_cost"] is None
    assert costs["warning_code"] == "kanglong_conversion_unavailable"
    assert costs["conversion"]["conversion_status"] == "unavailable"


def test_report_summary_records_report_metadata_from_checkpoint() -> None:
    baseline = _baseline()
    entry = _entry()
    checkpoint = KanglongCheckpoint.from_entries(
        run_id="run-1",
        checkpoint_id=1,
        previous_ledger_hash="sha256:genesis",
        entries=[entry],
        baselines=[baseline],
    )

    report = build_ledger_report(
        [entry.to_storage_payload()],
        baselines=[baseline.to_storage_payload()],
        latest_checkpoint={
            "checkpoint_id": checkpoint.checkpoint_id,
            "previous_ledger_hash": checkpoint.previous_ledger_hash,
            "ledger_hash": checkpoint.ledger_hash,
            "ledger_state_hash": checkpoint.ledger_state_hash,
        },
        summary_status="running",
        symbol="ETHUSDC",
        generated_at="2026-06-09T00:00:00+00:00",
    )

    summary = report["report_summary"]
    assert summary["report_version"] == "kanglong_transfer_report_v1"
    assert summary["generated_from_checkpoint_id"] == 1
    assert summary["source_ledger_hash"] == checkpoint.ledger_hash
    assert summary["source_ledger_state_hash"] == checkpoint.ledger_state_hash
    assert summary["generated_at"] == "2026-06-09T00:00:00+00:00"
    assert summary["summary_status"] == "running"
    assert report["ledger_report"]["conversion_status"] == "not_required"


def test_report_generation_rejects_ledger_hash_mismatch() -> None:
    baseline = _baseline()
    entry = _entry()
    checkpoint = KanglongCheckpoint.from_entries(
        run_id="run-1",
        checkpoint_id=1,
        previous_ledger_hash="sha256:genesis",
        entries=[entry],
        baselines=[baseline],
    )

    with pytest.raises(ValueError, match="kanglong_ledger_hash_mismatch"):
        build_ledger_report(
            [entry.to_storage_payload()],
            baselines=[baseline.to_storage_payload()],
            latest_checkpoint={
                "checkpoint_id": checkpoint.checkpoint_id,
                "previous_ledger_hash": checkpoint.previous_ledger_hash,
                "ledger_hash": "sha256:wrong",
                "ledger_state_hash": checkpoint.ledger_state_hash,
            },
            summary_status="running",
            symbol="ETHUSDC",
        )
