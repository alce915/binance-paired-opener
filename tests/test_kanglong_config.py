from __future__ import annotations

from decimal import Decimal

from paired_opener.config import Settings
from paired_opener.domain import PositionSide
from paired_opener.kanglong.config import load_kanglong_symbol_config
from paired_opener.kanglong.models import KanglongEvent, KanglongEventStatus, KanglongRunStatus, ResidualLedgerEntry


def test_kanglong_event_serializes_decimal_values_as_strings() -> None:
    event = KanglongEvent(
        run_id="run-1",
        group_id="group-1",
        round_id="round-1",
        mode="simulation",
        account_id="sub-1",
        symbol="ETHUSDC",
        position_side=PositionSide.LONG,
        action_type="single_close",
        leg_id="leg-close",
        paired_leg_id="leg-open",
        round_match_id="match-1",
        planned_qty=Decimal("0.010"),
        submitted_qty=Decimal("0.010"),
        filled_qty=Decimal("0.009"),
        matched_qty=Decimal("0.009"),
        close_residual_qty=Decimal("0.001"),
        open_residual_qty=Decimal("0"),
        avg_price=Decimal("3000.1"),
        status=KanglongEventStatus.PARTIAL_FILLED,
        reason="limit_order_unfilled",
    )

    payload = event.to_payload()

    assert KanglongRunStatus.DRAFT_PLAN.value == "draft_plan"
    assert payload["filled_qty"] == "0.009"
    assert payload["status"] == "partial_filled"
    assert payload["position_side"] == "LONG"


def test_residual_ledger_keeps_account_side_and_leg_type() -> None:
    entry = ResidualLedgerEntry(
        account_id="sub-1",
        side=PositionSide.LONG,
        leg_type="close",
        signed_qty=Decimal("0.001"),
        reason="step_size_rounding",
        event_id="event-1",
    )

    payload = entry.to_payload()

    assert payload["account_id"] == "sub-1"
    assert payload["side"] == "LONG"
    assert payload["leg_type"] == "close"
    assert payload["signed_qty"] == "0.001"


def test_ethusdc_kanglong_config_defaults() -> None:
    config = load_kanglong_symbol_config(Settings(_env_file=None), "ETHUSDC")

    assert config.per_round_qty_limit == Decimal("5")
    assert config.qty_tolerance == Decimal("0.0001")
    assert config.max_rounds_per_group == 30
    assert config.max_chain_groups == 100
    assert config.max_main_temp_qty == Decimal("150")


def test_non_ethusdc_kanglong_config_uses_conservative_defaults() -> None:
    config = load_kanglong_symbol_config(Settings(_env_file=None), "BTCUSDC")

    assert config.per_round_qty_limit == Decimal("0.05")
    assert config.max_main_temp_qty == Decimal("1.50")


def test_symbol_config_file_overrides_defaults(tmp_path) -> None:
    config_file = tmp_path / "kanglong_symbol_configs.json"
    config_file.write_text(
        '{"ETHUSDC":{"per_round_qty_limit":"0.02","qty_tolerance":"0.0002","max_rounds_per_group":10}}',
        encoding="utf-8",
    )
    settings = Settings(_env_file=None, kanglong_symbol_configs_file=config_file)

    config = load_kanglong_symbol_config(settings, "ETHUSDC")

    assert config.per_round_qty_limit == Decimal("0.02")
    assert config.qty_tolerance == Decimal("0.0002")
    assert config.max_rounds_per_group == 10
