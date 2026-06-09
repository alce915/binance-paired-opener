from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from paired_opener.kanglong.ledger import (
    KanglongCheckpoint,
    KanglongLedgerBaseline,
    KanglongLedgerEntry,
    canonical_decimal,
    canonical_json,
    hash_baseline,
    hash_checkpoint,
    hash_ledger_state,
    hash_operation_payload,
    ledger_entry_from_storage_payload,
)
from paired_opener.storage import SqliteRepository


def _baseline(**overrides: object) -> KanglongLedgerBaseline:
    payload = {
        "run_id": "run-1",
        "account_id": "acct-1",
        "wallet_balance": Decimal("1000.0000"),
        "available_balance": Decimal("900.0"),
        "equity": Decimal("1001.500000"),
        "margin": Decimal("100.000"),
        "margin_deficit": Decimal("0"),
        "total_unrealized_pnl": Decimal("1.5"),
        "long_qty": Decimal("1.2300"),
        "long_entry_price": Decimal("2400.10"),
        "long_mark_price": Decimal("2401.20"),
        "long_leverage": 75,
        "short_qty": Decimal("0"),
        "short_entry_price": Decimal("0"),
        "short_mark_price": Decimal("0"),
        "short_leverage": 75,
    }
    payload.update(overrides)
    return KanglongLedgerBaseline(**payload)


def _entry(sequence: int, entry_type: str, **overrides: object) -> KanglongLedgerEntry:
    payload = {
        "run_id": "run-1",
        "checkpoint_id": 1,
        "sequence": sequence,
        "operation_id": f"op-{sequence}",
        "account_id": "acct-1",
        "entry_type": entry_type,
        "asset": "USDC",
        "amount": Decimal("0"),
        "qty_delta": Decimal("0"),
        "margin_delta": Decimal("0"),
        "available_delta": Decimal("0"),
        "equity_delta": Decimal("0"),
        "realized_pnl_delta": Decimal("0"),
        "price_wear": Decimal("0"),
        "fee_amount": Decimal("0"),
        "fee_asset": "USDC",
        "operation_payload_hash": "sha256:payload",
        "payload": {"round_id": "round-1", "leg": entry_type},
    }
    payload.update(overrides)
    return KanglongLedgerEntry(**payload)


def test_baseline_hash_is_stable_for_canonical_decimal_values() -> None:
    baseline = _baseline(wallet_balance=Decimal("1000"), long_qty=Decimal("1.23"))
    equivalent = _baseline(wallet_balance=Decimal("1000.00000000"), long_qty=Decimal("1.230000000"))

    assert canonical_decimal("1.230000000", 8) == "1.23000000"
    assert canonical_decimal(Decimal("0"), 8) == "0.00000000"
    assert hash_baseline(baseline).startswith("sha256:")
    assert hash_baseline(baseline) == hash_baseline(equivalent)


def test_ledger_hash_chains_previous_hash_and_checkpoint_entries() -> None:
    entries = [
        _entry(1, "close_position", amount=Decimal("10"), qty_delta=Decimal("-1")),
        _entry(2, "open_position", amount=Decimal("-10"), qty_delta=Decimal("1")),
    ]

    checkpoint = KanglongCheckpoint.from_entries(
        run_id="run-1",
        checkpoint_id=1,
        previous_ledger_hash="sha256:genesis",
        entries=entries,
        baselines=[_baseline()],
    )

    assert checkpoint.ledger_hash == hash_checkpoint("sha256:genesis", entries)
    assert checkpoint.ledger_entry_count == 2
    assert checkpoint.ledger_hash != hash_checkpoint("sha256:other", entries)
    assert checkpoint.ledger_hash != hash_checkpoint("sha256:genesis", [entries[1], entries[0]])


def test_ledger_state_hash_changes_when_account_margin_changes() -> None:
    baseline = _baseline(margin=Decimal("100.0"))
    equivalent = _baseline(margin=Decimal("100.00000000"))
    changed = _baseline(margin=Decimal("101.0"))
    entries = [_entry(1, "close_position", qty_delta=Decimal("-1"))]

    assert hash_ledger_state([baseline], entries) == hash_ledger_state([equivalent], entries)
    assert hash_ledger_state([baseline], entries) != hash_ledger_state([changed], entries)


def test_operation_payload_hash_prevents_replay_with_changed_payload() -> None:
    payload = {
        "round_id": "round-1",
        "qty": Decimal("1.2300"),
        "legs": [{"side": "close", "account_id": "acct-1"}],
    }
    equivalent = {
        "legs": [{"account_id": "acct-1", "side": "close"}],
        "qty": "1.23",
        "round_id": "round-1",
    }
    changed = {
        "round_id": "round-1",
        "qty": Decimal("1.2400"),
        "legs": [{"side": "close", "account_id": "acct-1"}],
    }

    assert canonical_json(payload) == canonical_json(equivalent)
    assert hash_operation_payload(payload) == hash_operation_payload(equivalent)
    assert hash_operation_payload(payload) != hash_operation_payload(changed)


def test_fee_and_price_wear_entries_are_separate_amounts() -> None:
    fee = _entry(
        1,
        "fee",
        amount=Decimal("-0.25"),
        available_delta=Decimal("-0.25"),
        equity_delta=Decimal("-0.25"),
        fee_amount=Decimal("0.25"),
        price_wear=Decimal("0"),
    )
    price_wear = _entry(
        2,
        "price_wear",
        amount=Decimal("-1.50"),
        available_delta=Decimal("-1.50"),
        equity_delta=Decimal("-1.50"),
        fee_amount=Decimal("0"),
        price_wear=Decimal("1.50"),
    )

    assert fee.entry_type == "fee"
    assert fee.fee_amount == Decimal("0.250000000")
    assert fee.price_wear == Decimal("0E-9")
    assert price_wear.entry_type == "price_wear"
    assert price_wear.fee_amount == Decimal("0E-9")
    assert price_wear.price_wear == Decimal("1.500000000")


def test_storage_recomputes_checkpoint_hashes_from_ledger_objects(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    baseline = _baseline()
    entry = _entry(1, "fee", amount=Decimal("-0.1"), fee_amount=Decimal("0.1"))
    try:
        repository.create_kanglong_run(
            {
                "run_id": "run-1",
                "symbol": "ETHUSDC",
                "main_account_id": "main",
                "subaccount_ids": ["sub1"],
                "status": "running",
            }
        )
        repository.save_kanglong_ledger_baselines("run-1", [baseline])
        committed = repository.commit_kanglong_checkpoint(
            run_id="run-1",
            checkpoint_id=1,
            expected_previous_checkpoint_id=0,
            expected_previous_ledger_hash="sha256:genesis",
            previous_ledger_hash="sha256:genesis",
            ledger_hash="wrong",
            ledger_state_hash="wrong",
            ledger_entries=[entry],
            events=[],
        )
        stored_entry = repository.list_kanglong_ledger_entries("run-1")[0]
        stored_checkpoint = repository.latest_kanglong_checkpoint("run-1")

        with pytest.raises(ValueError, match="kanglong_ledger_hash_mismatch"):
            repository.commit_kanglong_checkpoint(
                run_id="run-1",
                checkpoint_id=2,
                expected_previous_checkpoint_id=1,
                expected_previous_ledger_hash="wrong",
                previous_ledger_hash="wrong",
                ledger_hash=hash_checkpoint("wrong", []),
                ledger_state_hash=hash_ledger_state([baseline], [entry]),
                ledger_entries=[],
                events=[],
            )
    finally:
        repository.close()

    assert committed["ledger_hash"] == hash_checkpoint("sha256:genesis", [entry])
    assert committed["ledger_state_hash"] == hash_ledger_state([baseline], [entry])
    assert stored_checkpoint["ledger_hash"] == committed["ledger_hash"]
    assert ledger_entry_from_storage_payload(stored_entry) == entry


def test_storage_rejects_ledger_object_bound_to_another_checkpoint(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    entry = _entry(
        1,
        "fee",
        run_id="other-run",
        checkpoint_id=99,
        amount=Decimal("-0.1"),
        fee_amount=Decimal("0.1"),
    )
    try:
        repository.create_kanglong_run(
            {
                "run_id": "run-1",
                "symbol": "ETHUSDC",
                "main_account_id": "main",
                "subaccount_ids": ["sub1"],
                "status": "running",
            }
        )

        with pytest.raises(ValueError, match="kanglong_operation_payload_mismatch"):
            repository.commit_kanglong_checkpoint(
                run_id="run-1",
                checkpoint_id=1,
                expected_previous_checkpoint_id=0,
                expected_previous_ledger_hash="sha256:genesis",
                previous_ledger_hash="sha256:genesis",
                ledger_hash="ignored",
                ledger_state_hash="ignored",
                ledger_entries=[entry],
                events=[],
            )
    finally:
        repository.close()
