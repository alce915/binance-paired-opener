from __future__ import annotations

import json
from pathlib import Path

from app_i18n.runtime import event_registry, log_registry, messages, precheck_registry, reason_registry


def load_messages() -> dict[str, str]:
    return json.loads(Path("i18n/messages/zh-CN.json").read_text(encoding="utf-8"))


def test_kanglong_i18n_messages_and_registries_exist() -> None:
    catalog = messages()
    required_message_keys = {
        "console.kanglong.title",
        "console.kanglong.main_account",
        "console.kanglong.subaccounts",
        "console.kanglong.selected_side",
        "console.kanglong.run_simulation",
        "console.kanglong.report.result_grade",
        "console.kanglong.report.status",
        "runtime.kanglong.account_selection_required",
        "runtime.kanglong.request_failed",
        "runtime.kanglong.status.blocked_main_not_flat",
        "reasons.kanglong.blocked_main_not_flat",
        "events.kanglong.round_completed",
        "log.kanglong.abort_recovered",
        "precheck.labels.kanglong.main_flat",
        "precheck.kanglong.main_flat.fail",
    }

    assert required_message_keys.issubset(catalog)
    assert reason_registry()["kanglong.blocked_main_not_flat"]["key"] == "reasons.kanglong.blocked_main_not_flat"
    assert event_registry()["kanglong.round_completed"]["key"] == "events.kanglong.round_completed"
    assert log_registry()["kanglong.abort_recovered"]["key"] == "log.kanglong.abort_recovered"
    assert precheck_registry()["kanglong.main_flat"]["fail_key"] == "precheck.kanglong.main_flat.fail"


def test_kanglong_workspace_i18n_keys_exist() -> None:
    catalog = load_messages()
    required_message_keys = {
        "console.kanglong.nav",
        "console.kanglong.account_pool.title",
        "console.kanglong.selected_accounts.title",
        "console.kanglong.plan.summary_title",
        "console.kanglong.execution.log_title",
        "console.kanglong.actions.detect",
        "console.kanglong.actions.confirm",
        "console.kanglong.actions.execute",
        "console.kanglong.logs.filter.all",
        "console.kanglong.logs.filter.warning",
        "console.kanglong.logs.filter.error",
        "console.kanglong.logs.filter.current_group",
        "console.kanglong.logs.filter.cost",
        "console.kanglong.logs.filter.ledger",
        "runtime.kanglong.idempotency_conflict",
        "runtime.kanglong.plan_stale",
    }

    assert required_message_keys.issubset(catalog)
    assert catalog["console.kanglong.logs.filter.all"] == "全部"
    assert catalog["console.kanglong.logs.filter.warning"] == "警告"
    assert catalog["console.kanglong.logs.filter.error"] == "错误"
    assert catalog["console.kanglong.logs.filter.current_group"] == "当前组"
    assert catalog["console.kanglong.logs.filter.cost"] == "成本事件"
    assert catalog["console.kanglong.logs.filter.ledger"] == "账本事件"
    assert catalog["runtime.kanglong.idempotency_conflict"] == "重复请求的幂等键与原请求不一致，请刷新后重试。"
    assert catalog["runtime.kanglong.plan_stale"] == "检测链路已过期，请重新检测账号状态。"
    assert catalog["events.kanglong.group_simulated"] == "亢龙第 {group_id} 组模拟完成"
    assert catalog["reasons.kanglong.blocked_plan_stale"] == "计划版本已变化，需要重新检测并确认。"
    assert catalog["reasons.kanglong.idempotency_conflict"] == "同一个幂等键被用于不同请求。"

    group_simulated = event_registry()["kanglong.group_simulated"]
    assert group_simulated["key"] == "events.kanglong.group_simulated"
    assert group_simulated["level"] == "info"
    assert reason_registry()["kanglong.blocked_plan_stale"]["key"] == "reasons.kanglong.blocked_plan_stale"
    assert reason_registry()["kanglong.idempotency_conflict"]["key"] == "reasons.kanglong.idempotency_conflict"
