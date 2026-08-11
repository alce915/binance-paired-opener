from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from paired_opener.schemas import (
    AccountCredentialCreateRequest,
    AccountCredentialImportChanges,
    AccountCredentialImportPreviewResponse,
    AccountCredentialSummary,
    AccountCredentialUpdateRequest,
    KanglongActionRequest,
    KanglongBatchPlanRequest,
    KanglongBatchRecoverRequest,
    KanglongControlRequest,
)


def _preview_payload() -> dict[str, object]:
    return {
        "preview_token": "p" * 32,
        "credential_revision": "revision-1",
        "expires_at": datetime.now(UTC) + timedelta(minutes=5),
        "final_accounts": [
            {
                "account_id": "a1",
                "name": "账号 1",
                "api_key_masked": "ABCD…WXYZ",
                "has_api_secret": True,
                "account_mode": "portfolio_margin",
                "enabled": True,
                "order": 0,
            }
        ],
        "changes": AccountCredentialImportChanges(
            added_account_ids=["a1"],
            updated_account_ids=[],
            unchanged_account_ids=[],
            removed_account_ids=[],
        ),
    }


def test_credential_summary_never_has_secret_field() -> None:
    summary = AccountCredentialSummary(
        account_id="a1",
        name="账号 1",
        api_key_masked="ABCD…WXYZ",
        has_api_secret=True,
        account_mode="portfolio_margin",
        enabled=True,
        order=0,
    )
    assert "api_secret" not in summary.model_dump()


def test_close_plan_requires_source_open_run_id() -> None:
    with pytest.raises(ValidationError):
        KanglongBatchPlanRequest(
            operation="close",
            symbol="ETHUSDC",
            preferred_side="LONG",
            leverage=100,
            per_leg_notional="250000",
            account_ids=["a1"],
        )


def test_create_requires_secret_but_update_may_retain_existing_secret() -> None:
    with pytest.raises(ValidationError):
        AccountCredentialCreateRequest(account_id="a1", name="账号 1", api_key="KEY-123456")
    update = AccountCredentialUpdateRequest(name="新名称")
    assert update.api_secret is None


def test_batch_plan_round_count_is_part_of_contract() -> None:
    request = KanglongBatchPlanRequest(
        operation="open",
        symbol="ETHUSDC",
        preferred_side="LONG",
        account_ids=["a1"],
        round_count=30,
    )
    assert request.round_count == 30
    assert request.leverage == 100
    assert request.per_leg_notional == Decimal("250000")


def test_import_preview_exposes_revision_and_expiry_but_no_secret() -> None:
    response = AccountCredentialImportPreviewResponse.model_validate(_preview_payload())
    assert response.credential_revision
    assert response.expires_at
    assert '\"api_secret\":' not in response.model_dump_json()


def test_batch_actions_reuse_plan_and_action_version_contracts() -> None:
    action = KanglongActionRequest(plan_version="plan-v1", idempotency_key="confirm-0001")
    control = KanglongControlRequest(
        plan_version="plan-v1",
        expected_action_version=3,
        idempotency_key="pause-0001",
    )
    recover = KanglongBatchRecoverRequest(
        plan_version="plan-v1",
        expected_action_version=4,
        idempotency_key="recover-0001",
        release_reason="operator reviewed checkpoint",
    )
    assert action.plan_version == control.plan_version == recover.plan_version


def test_hmac_only_and_account_limit_are_enforced() -> None:
    with pytest.raises(ValidationError):
        AccountCredentialCreateRequest(
            account_id="a1",
            name="账号 1",
            api_key="KEY-123456",
            api_secret="SECRET-123456",
            credential_type="rsa",
        )
    with pytest.raises(ValidationError):
        KanglongBatchPlanRequest(
            operation="open",
            symbol="ETHUSDC",
            preferred_side="LONG",
            account_ids=[f"a{i}" for i in range(101)],
        )


def test_openapi_declares_every_batch_route_and_conflict() -> None:
    text = Path("docs/openapi/kanglong-batch-simulation.yaml").read_text(encoding="utf-8")
    for route in (
        "/config/account-credentials:",
        "/config/account-credentials/import/preview:",
        "/config/account-credentials/import/commit:",
        "/config/account-credentials/{account_id}:",
        "/config/account-credentials/order:",
        "/config/account-credentials/{account_id}/verify:",
        "/kanglong/batch-simulation/plan:",
        "/kanglong/batch-simulation/plan/{run_id}/confirm:",
        "/kanglong/batch-simulation/plan/{run_id}/execute:",
        "/kanglong/batch-simulation/run/{run_id}/{action}:",
        "/kanglong/batch-simulation/run/{run_id}:",
        "/kanglong/batch-simulation/run/{run_id}/events:",
        "/kanglong/batch-simulation/open-runs:",
    ):
        assert route in text
    for conflict in ("idempotency_key_conflict", "plan_version_conflict", "action_version_conflict"):
        assert conflict in text
    assert "confirmed_warning_codes:" in text
    assert "operator:" in text
    assert "confirmed_plan_hash:" in text
    assert "paused_plan_stale" not in text
