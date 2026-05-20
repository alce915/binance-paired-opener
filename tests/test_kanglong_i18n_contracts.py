from __future__ import annotations

import json
import re
from pathlib import Path

from app_i18n.runtime import event_registry, log_registry, messages, precheck_registry, reason_registry
from paired_opener.kanglong.models import KanglongRunStatus


KANGLONG_TEMPLATE_MESSAGE_COPY = {
    "console.kanglong.test_template.button": "测试模板",
    "console.kanglong.test_template.modal_title": "亢龙测试账号模板",
    "console.kanglong.test_template.close": "关闭",
    "console.kanglong.test_template.library_title": "模板库",
    "console.kanglong.test_template.library_empty": "暂无测试模板",
    "console.kanglong.test_template.market_data_account": "行情源账号",
    "console.kanglong.test_template.save": "保存模板",
    "console.kanglong.test_template.save_and_apply": "保存并应用",
    "console.kanglong.test_template.preview": "预览快照",
    "console.kanglong.test_template.editor.basic": "基础信息",
    "console.kanglong.test_template.editor.main_account": "主账号",
    "console.kanglong.test_template.editor.subaccounts": "子账号",
    "console.kanglong.test_template.editor.batch_generator": "批量生成",
    "console.kanglong.test_template.editor.advanced_json": "高级 JSON",
    "console.kanglong.test_template.field.name": "模板名称",
    "console.kanglong.test_template.field.symbol": "交易对",
    "console.kanglong.test_template.field.main_name": "主账号名称",
    "console.kanglong.test_template.field.sub_name": "子账号名称",
    "console.kanglong.test_template.field.collateral": "保证金",
    "console.kanglong.test_template.field.leverage": "杠杆",
    "console.kanglong.test_template.field.long_entry_price": "LONG 开仓价",
    "console.kanglong.test_template.field.short_entry_price": "SHORT 开仓价",
    "console.kanglong.test_template.field.qty": "持仓数量",
    "console.kanglong.test_template.preview.title": "预览快照",
    "console.kanglong.test_template.preview.empty": "尚未生成预览",
    "console.kanglong.test_template.preview.refresh": "刷新预览",
    "console.kanglong.test_template.preview.available_balance": "可用 {value}",
    "console.kanglong.test_template.preview.unrealized_pnl": "未实现 {value}",
    "console.kanglong.test_template.preview.position_side": "方向",
    "console.kanglong.test_template.preview.qty": "数量",
    "console.kanglong.test_template.preview.entry_price": "开仓均价",
    "console.kanglong.test_template.preview.mark_price": "标记价格",
    "console.kanglong.test_template.preview.unrealized_pnl_label": "未实现盈亏",
    "console.kanglong.test_template.actions.new": "新建模板",
    "console.kanglong.test_template.actions.apply": "应用",
    "console.kanglong.test_template.actions.clone": "复制",
    "console.kanglong.test_template.actions.delete": "删除",
    "console.kanglong.test_template.actions.validate_import": "校验并导入表单",
    "console.kanglong.test_template.status.unsaved": "未保存",
    "console.kanglong.test_template.status.saved": "已保存",
    "console.kanglong.test_template.status.preview_stale": "预览已过期",
    "console.kanglong.test_template.status.preview_ready": "预览可应用",
    "console.kanglong.test_template.status.blocked": "存在阻断",
    "console.kanglong.test_template.status.dirty": "存在未保存改动",
    "console.kanglong.test_template.status.warning_pending": "警告待确认",
    "console.kanglong.test_template.status.active_run_locked": "当前模板被运行占用",
    "console.kanglong.test_template.validation.empty_numeric": "请输入数值",
    "console.kanglong.test_template.validation.positive_number": "请输入大于 0 的数值",
    "console.kanglong.test_template.validation.non_negative_number": "请输入不小于 0 的数值",
    "console.kanglong.test_template.validation.name_required": "请输入名称",
    "console.kanglong.test_template.validation.symbol_required": "请输入交易对",
    "console.kanglong.test_template.validation.market_data_required": "请选择行情源账号",
    "console.kanglong.test_template.validation.market_data_unavailable": "行情源账号不可用",
    "console.kanglong.test_template.validation.subaccount_required": "至少需要 1 个子账号",
    "console.kanglong.test_template.validation.warning_confirm_required": "确认警告后才能应用",
    "console.kanglong.test_template.exit_mode": "退出测试模板",
    "console.kanglong.test_template.applied": "已应用测试模板",
    "console.kanglong.test_template.snapshot_stale": "模板已更新，当前快照已过期",
}

KANGLONG_TEMPLATE_REASON_KEYS = {
    "kanglong_test_template_not_found": "reasons.kanglong.test_template.not_found",
    "kanglong_test_template_symbol_mismatch": "reasons.kanglong.test_template.symbol_mismatch",
    "kanglong_test_template_accounts_required": "reasons.kanglong.test_template.accounts_required",
    "kanglong_test_template_account_mismatch": "reasons.kanglong.test_template.account_mismatch",
    "kanglong_test_template_market_data_account_required": "reasons.kanglong.test_template.market_data_account_required",
    "kanglong_test_template_market_data_account_unavailable": "reasons.kanglong.test_template.market_data_account_unavailable",
    "kanglong_test_template_invalid_id": "reasons.kanglong.test_template.invalid_id",
    "kanglong_test_template_invalid_decimal": "reasons.kanglong.test_template.invalid_decimal",
    "kanglong_test_template_negative_collateral": "reasons.kanglong.test_template.negative_collateral",
    "kanglong_test_template_invalid_leverage": "reasons.kanglong.test_template.invalid_leverage",
    "kanglong_test_template_non_positive_qty": "reasons.kanglong.test_template.non_positive_qty",
    "kanglong_test_template_min_qty_not_met": "reasons.kanglong.test_template.min_qty_not_met",
    "kanglong_test_template_min_notional_not_met": "reasons.kanglong.test_template.min_notional_not_met",
    "kanglong_test_template_invalid_price": "reasons.kanglong.test_template.invalid_price",
    "kanglong_test_template_leverage_exceeded": "reasons.kanglong.test_template.leverage_exceeded",
    "kanglong_test_template_quote_unavailable": "reasons.kanglong.test_template.quote_unavailable",
    "kanglong_test_template_orderbook_unavailable": "reasons.kanglong.test_template.orderbook_unavailable",
    "kanglong_test_template_store_corrupted": "reasons.kanglong.test_template.store_corrupted",
    "kanglong_test_template_store_unreadable": "reasons.kanglong.test_template.store_unreadable",
    "kanglong_test_template_store_write_conflict": "reasons.kanglong.test_template.store_write_conflict",
    "kanglong_test_template_unsupported_version": "reasons.kanglong.test_template.unsupported_version",
    "kanglong_test_template_migration_failed": "reasons.kanglong.test_template.migration_failed",
    "kanglong_test_template_active_run_exists": "reasons.kanglong.test_template.active_run_exists",
    "kanglong_test_template_invalid_template": "reasons.kanglong.test_template.invalid_template",
    "blocked_plan_stale": "reasons.kanglong.blocked_plan_stale",
    "blocked_plan_recheck_failed": "reasons.kanglong.blocked_plan_recheck_failed",
}

KANGLONG_TEMPLATE_REASON_CODE_RE = re.compile(r'"(kanglong_test_template_[a-z0-9_]+)"')


def load_messages() -> dict[str, str]:
    return json.loads(Path("i18n/messages/zh-CN.json").read_text(encoding="utf-8"))


def reachable_template_reason_codes() -> set[str]:
    source = Path("paired_opener/kanglong/test_templates.py").read_text(encoding="utf-8")
    return set(KANGLONG_TEMPLATE_REASON_CODE_RE.findall(source))


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
        "events.kanglong.synthetic_ledger_failed",
        "events.kanglong.round_completed",
        "log.kanglong.abort_recovered",
        "precheck.labels.kanglong.main_flat",
        "precheck.kanglong.main_flat.fail",
    }

    assert required_message_keys.issubset(catalog)
    assert reason_registry()["kanglong.blocked_main_not_flat"]["key"] == "reasons.kanglong.blocked_main_not_flat"
    assert event_registry()["kanglong.synthetic_ledger_failed"]["key"] == "events.kanglong.synthetic_ledger_failed"
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
    assert catalog["events.kanglong.synthetic_ledger_failed"] == "亢龙第 {group_id} 组账本校验失败，已暂停等待人工恢复：{reason}"
    assert catalog["reasons.kanglong.blocked_plan_stale"] == "计划版本已变化，需要重新检测并确认。"
    assert catalog["reasons.kanglong.idempotency_conflict"] == "同一个幂等键被用于不同请求。"

    group_simulated = event_registry()["kanglong.group_simulated"]
    assert group_simulated["key"] == "events.kanglong.group_simulated"
    assert group_simulated["level"] == "info"
    synthetic_failed = event_registry()["kanglong.synthetic_ledger_failed"]
    assert synthetic_failed["key"] == "events.kanglong.synthetic_ledger_failed"
    assert synthetic_failed["level"] == "error"
    assert reason_registry()["kanglong.blocked_plan_stale"]["key"] == "reasons.kanglong.blocked_plan_stale"
    assert reason_registry()["kanglong.idempotency_conflict"]["key"] == "reasons.kanglong.idempotency_conflict"


def test_kanglong_test_template_ui_messages_exist() -> None:
    catalog = load_messages()

    for key, expected_copy in KANGLONG_TEMPLATE_MESSAGE_COPY.items():
        assert catalog[key] == expected_copy


def test_kanglong_test_template_reason_registry_entries_have_messages() -> None:
    catalog = load_messages()
    registry = reason_registry()

    for code, message_key in KANGLONG_TEMPLATE_REASON_KEYS.items():
        assert registry[code]["key"] == message_key
        assert message_key in catalog


def test_reachable_kanglong_test_template_reason_codes_are_registered_and_localized() -> None:
    catalog = load_messages()
    registry = reason_registry()
    reachable_codes = reachable_template_reason_codes()

    assert reachable_codes
    assert sorted(reachable_codes - set(registry)) == []
    assert sorted(reachable_codes - set(KANGLONG_TEMPLATE_REASON_KEYS)) == []

    missing_messages = sorted(
        registry[code]["key"]
        for code in reachable_codes
        if registry[code]["key"] not in catalog
    )
    assert missing_messages == []


def test_all_kanglong_run_statuses_have_display_copy() -> None:
    catalog = load_messages()
    missing = [
        f"runtime.kanglong.status.{status.value}"
        for status in KanglongRunStatus
        if f"runtime.kanglong.status.{status.value}" not in catalog
    ]

    assert missing == []
