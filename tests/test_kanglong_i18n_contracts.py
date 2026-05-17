from __future__ import annotations

from app_i18n.runtime import event_registry, log_registry, messages, precheck_registry, reason_registry


def test_kanglong_i18n_messages_and_registries_exist() -> None:
    catalog = messages()
    required_message_keys = {
        "console.kanglong.title",
        "console.kanglong.run_simulation",
        "console.kanglong.report.result_grade",
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
