# Kanglong Transfer Simulation Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` for implementation in this repository. If subagents are not available, use `superpowers:executing-plans` and execute one checkbox at a time. Update this checklist as each item is completed.

**Goal:** Rebuild Kanglong transfer simulation so it uses the same market-data-driven matching process as the simulation desk, while preserving transfer-specific chain planning, cross-account open/close pairing, resumable checkpoints, and fee/slippage wear reporting.

**Architecture:** Extract a shared deterministic matching core from `paired_opener/simulation.py`, keep Kanglong transfer planning in `paired_opener/kanglong/planner.py`, execute transfer rounds through a resumable `KanglongTransferExecutor`, and persist every mutation through append-only ledger entries plus checkpoint hashes in `paired_opener/storage.py`.

**Tech Stack:** Python, SQLite, pytest, existing local app API, existing vanilla JS frontend in `paired_opener/static/app.js`, Node-based frontend contract tests.

---

## Scope

This plan covers the implementation of the 2026-06-03 redesign spec:

- UI transfer settings between account pool and link detection.
- Locked transfer fields: symbol, mode, order side, leverage, per-round quantity.
- Editable transfer fields: percent, round count, round interval.
- Immediate button/action refresh after account status and link detection.
- `plan_input_hash` and `confirmed_plan_hash` stale-plan protection.
- Shared market-data-driven matching with simulation desk behavior.
- Transfer execution as synchronized single-direction close/open between two accounts.
- Supplemental order mechanism reused from simulation desk.
- Resumable checkpoints, lock lease/fencing, idempotent round attempts.
- Append-only ledger with chain hash and state hash.
- Fee and price-wear reporting.
- Legacy static Kanglong runs treated as read-only.

## Non-Goals

- No live exchange order submission.
- No change to account credential storage.
- No replacement of the simulation desk UI.
- No bulk migration of old run reports beyond read-only compatibility.

## File Structure

Create:

- `paired_opener/simulation_matching.py`
- `paired_opener/kanglong/ledger.py`
- `paired_opener/kanglong/executor.py`
- `tests/test_simulation_matching.py`
- `tests/test_kanglong_ledger.py`
- `tests/test_kanglong_executor.py`
- `tests/fixtures/kanglong_market/ethusdc_rounds.json`

Modify:

- `paired_opener/simulation.py`
- `paired_opener/storage.py`
- `paired_opener/kanglong/models.py`
- `paired_opener/kanglong/planner.py`
- `paired_opener/kanglong/service.py`
- `paired_opener/kanglong/simulator.py`
- `paired_opener/kanglong/reporter.py`
- `paired_opener/app.py`
- `paired_opener/static/app.js`
- `i18n/messages/zh-CN.json`
- `tests/test_simulation_service.py`
- `tests/test_simulation_api.py`
- `tests/test_kanglong_workflow_contracts.py`
- `tests/test_kanglong_storage.py`
- `tests/test_app_kanglong_display.mjs`

## Data Model Contract

Persist `kanglong_runs.engine_version=2` for redesigned runs.

Use these run phases:

- `draft_plan`
- `chain_ready`
- `plan_confirmed`
- `execution_starting`
- `running`
- `pause_pending`
- `stop_pending`
- `paused_by_user`
- `paused_market_unstable`
- `paused_plan_stale`
- `stopped_by_user`
- `completed`
- `completed_with_dust_residual`
- `needs_abort_recover`
- `aborted_recovered`
- `blocked_accounts`
- `blocked_chain`
- `blocked_margin`
- `legacy_readonly`

Use this action matrix:

| Phase | Actions |
| --- | --- |
| `draft_plan` | `refresh_plan` |
| `blocked_accounts` | `refresh_plan` |
| `blocked_chain` | `refresh_plan` |
| `blocked_margin` | `refresh_plan` |
| `chain_ready` | `confirm`, `refresh_plan` |
| `plan_confirmed` | `execute`, `refresh_plan` |
| `execution_starting` | `view_report` |
| `running` | `pause`, `stop`, `view_report` |
| `pause_pending` | `stop`, `view_report` |
| `stop_pending` | `view_report` |
| `paused_by_user` | `resume`, `stop`, `view_report` |
| `paused_market_unstable` | `resume`, `stop`, `recover`, `view_report` |
| `paused_plan_stale` | `refresh_plan`, `recover`, `view_report` |
| `stopped_by_user` | `view_report`, `refresh_plan` |
| `completed` | `view_report` |
| `completed_with_dust_residual` | `view_report` |
| `needs_abort_recover` | `recover`, `view_report` |
| `aborted_recovered` | `refresh_plan`, `view_report` |
| `legacy_readonly` | `refresh_plan`, `view_report` |

Use these error codes exactly:

- `kanglong_invalid_transfer_setting`
- `kanglong_stale_plan_input_hash`
- `kanglong_stale_confirmed_plan_hash`
- `kanglong_stale_checkpoint`
- `kanglong_stale_action_version`
- `kanglong_lease_conflict`
- `kanglong_operation_payload_mismatch`
- `kanglong_ledger_hash_mismatch`
- `kanglong_ledger_state_hash_mismatch`
- `kanglong_market_data_stale`
- `kanglong_event_limit_reached`
- `kanglong_conversion_unavailable`
- `kanglong_legacy_run_readonly`

---

## Phase 1: Shared Matching Core

### Task 1.1: Characterize current simulation matcher behavior

- [x] Read `paired_opener/simulation.py` methods `_match_orderbook`, `_match_limit_orderbook`, `_match_orderbook_snapshot`, `_poll_passive_limit_fill`, `_passive_limit_price`, and `_limit_order_crosses`.
- [x] Record the current inputs and outputs for `MatchResult`: `filled_qty`, `avg_price`, `fee`, `residual_qty`, `depth_levels_consumed`, `slippage_bps`, `liquidity`, `wait_seconds_consumed`.
- [x] Confirm existing tests covering matching in `tests/test_simulation_service.py` and `tests/test_simulation_api.py`.

### Task 1.2: Add failing tests for extracted matcher

- [x] Create `tests/test_simulation_matching.py`.
- [x] Add `test_taker_match_consumes_orderbook_depth_with_weighted_average`.
- [x] Add `test_passive_limit_waits_across_snapshots_and_reports_residual`.
- [x] Add `test_passive_limit_returns_zero_fill_when_price_never_crosses`.
- [x] Add `test_stale_market_snapshot_raises_market_data_stale`.
- [x] Add `test_fee_is_calculated_from_frozen_fee_rate_and_notional`.
- [x] Run `python -m pytest tests/test_simulation_matching.py -q` and verify these tests fail because `paired_opener.simulation_matching` does not exist yet.

### Task 1.3: Create `paired_opener/simulation_matching.py`

- [x] Add `OrderbookLevel` with `price: Decimal` and `qty: Decimal`.
- [x] Add `OrderbookSnapshot` with `symbol`, `bids`, `asks`, `event_time`, and optional `source`.
- [x] Add `MarketDataProvider` protocol with `get_orderbook(symbol: str) -> OrderbookSnapshot`.
- [x] Add `DeterministicMarketDataProvider` that reads an in-memory ordered snapshot list and advances on each call.
- [x] Add `OrderbookMatcher.match_orderbook_snapshot(...)` for taker fills.
- [x] Add `OrderbookMatcher.poll_passive_limit_fill(...)` for maker-style polling.
- [x] Return the existing `MatchResult` shape so `SimulationService` can delegate without response contract drift.
- [x] Quantize prices, quantities, notional, fees, and residuals through one helper in this module.

### Task 1.4: Delegate simulation desk to the shared matcher

- [x] Update `paired_opener/simulation.py` to import the shared matcher.
- [x] Replace private matching math in `_match_orderbook_snapshot`, `_passive_limit_price`, `_limit_order_crosses`, and `_passive_fill_qty_from_snapshot` with calls into `OrderbookMatcher`.
- [x] Keep the existing public `SimulationService` API unchanged.
- [x] Run `python -m pytest tests/test_simulation_matching.py tests/test_simulation_service.py tests/test_simulation_api.py -q`.

### Task 1.5: Guard against matcher drift

- [x] Add regression assertions that old simulation desk payloads still include the same `avg_price`, `fee`, `residual_qty`, and `wait_seconds_consumed` field names.
- [x] Run `python -m compileall paired_opener`.

---

## Phase 2: Transfer Settings and Plan Hashes

### Task 2.1: Extend Kanglong models

- [x] Update `paired_opener/kanglong/models.py` with `TransferExecutionSettings`.
- [x] Fields: `symbol`, `direction`, `mode`, `order_side`, `leverage`, `transfer_percent`, `round_count`, `round_interval_seconds`, `per_round_qty`.
- [x] Validate `mode == "transfer"` and `leverage == 75`.
- [x] Validate `transfer_percent > 0` and `transfer_percent <= 100`.
- [x] Validate `round_count >= 1`.
- [x] Validate `round_interval_seconds >= 0`.
- [x] Map `direction=long` to `order_side=LONG | close long/open long`.
- [x] Map `direction=short` to `order_side=SHORT | close short/open short`.

### Task 2.2: Add request contract tests

- [x] In `tests/test_kanglong_workflow_contracts.py`, add `test_detect_link_accepts_transfer_settings_and_returns_plan_input_hash`.
- [x] Add `test_detect_link_rejects_editing_locked_transfer_fields`.
- [x] Add `test_confirm_rejects_stale_plan_input_hash`.
- [x] Add `test_execute_rejects_stale_confirmed_plan_hash`.
- [x] Add `test_per_round_qty_uses_percent_and_round_count`.
- [x] Run `python -m pytest tests/test_kanglong_workflow_contracts.py -q` and verify the new tests fail against current contracts.

### Task 2.3: Implement server-side plan input hash

- [x] In `paired_opener/kanglong/service.py`, canonicalize account pool ids, symbol, direction, transfer percent, round count, interval, and frozen account labels into `plan_input_hash`.
- [x] Store `plan_input_hash` on detect-link output inside `kanglong_runs.request_json`.
- [x] In confirm, reject a submitted `plan_input_hash` that differs from the latest run value.
- [x] Generate `confirmed_plan_hash` from the confirmed chain plan, round settings, and frozen execution labels.
- [x] Store `confirmed_plan_hash` in `plan_json`.
- [x] In execute, reject a submitted `confirmed_plan_hash` that differs from the stored value.

### Task 2.4: Update planner quantity semantics

- [x] In `paired_opener/kanglong/service.py`, calculate total planned transfer quantity from current position quantity multiplied by `transfer_percent / 100` before calling `build_kanglong_plan`.
- [x] Calculate `per_round_qty = total_planned_qty / round_count`, quantized to ETH precision used by existing simulation code.
- [x] Keep per-round quantity read-only in the API response.
- [x] Generate supplemental rounds only during execution, not during link planning.
- [x] Preserve residual quantity so the final round can close dust without exceeding the configured per-round cap.
- [x] Run `python -m pytest tests/test_kanglong_workflow_contracts.py -q`.

---

## Phase 3: Storage, Checkpoints, and Locks

### Task 3.1: Add storage schema tests

- [x] In `tests/test_kanglong_storage_workflow.py`, add `test_schema_creates_engine_version_and_ledger_tables`.
- [x] Add `test_legacy_kanglong_run_reads_as_readonly_and_does_not_block_active_run`.
- [x] Add `test_commit_checkpoint_inserts_events_entries_and_checkpoint_atomically`.
- [x] Add `test_checkpoint_hash_chain_rejects_previous_hash_mismatch`.
- [x] Add `test_lock_lease_uses_fencing_token_and_expires`.
- [x] Add `test_account_lock_release_does_not_drop_run_lease`.
- [x] Add `test_control_request_uses_action_version_compare_and_swap`.
- [x] Run `pytest tests/test_kanglong_storage_workflow.py -q` and verify the new tests fail.

### Task 3.2: Extend `kanglong_runs`

- [x] In `paired_opener/storage.py`, add `engine_version INTEGER NOT NULL DEFAULT 1` to `kanglong_runs`.
- [x] Add compatibility migration for existing rows so missing values read as `1`.
- [x] Ensure new redesign runs insert `engine_version=2`.
- [x] Ensure old rows without version map to `legacy_readonly` unless refreshed.

### Task 3.3: Add append-only ledger tables

- [x] Add `kanglong_ledger_baselines` with `run_id`, `account_id`, wallet, available, equity, margin, margin deficit, unrealized PnL, position qty, entry price, mark price, leverage, and `baseline_hash`.
- [x] Add `kanglong_run_checkpoints` with `checkpoint_id`, `previous_ledger_hash`, `ledger_hash`, `ledger_state_hash`, `events_high_watermark`, and `is_safe`.
- [x] Add `kanglong_ledger_entries` with unique indexes on `run_id + checkpoint_id + sequence` and `run_id + operation_id + sequence`.
- [x] Include fee, margin, available, equity, realized PnL, price-wear, and account position deltas in ledger entry columns.
- [x] Add `checkpoint_id` to `kanglong_events` and keep it nullable for legacy events.

### Task 3.4: Add lock and scheduler fields

- [x] Extend `progress_json` contract with `next_wake_at`, `scheduled_reason`, `worker_epoch`, `lease_token`, `lock_expires_at`, `action_version`, and `control_request`.
- [x] Update `kanglong_locks` to include `lease_token`, `fencing_token`, `worker_epoch`, and `expires_at`.
- [x] Add `acquire_kanglong_run_lease(run_id, worker_id, ttl_seconds)`.
- [x] Add `renew_kanglong_run_lease(run_id, lease_token, fencing_token, ttl_seconds)`.
- [x] Add `release_kanglong_run_lease(run_id, lease_token, fencing_token)`.

### Task 3.5: Implement atomic checkpoint commit

- [x] Add `commit_kanglong_checkpoint(...)` in `paired_opener/storage.py`.
- [x] Start one SQLite transaction.
- [x] Verify the current latest checkpoint id and previous hash before inserts.
- [x] Insert ledger entries.
- [x] Insert events with the new checkpoint id.
- [x] Insert `kanglong_run_checkpoints` row.
- [x] Update `kanglong_runs.progress_json`, `status`, `available_actions_json`, and `report_summary_json` when provided.
- [x] Commit transaction once all inserts succeed.
- [x] Roll back the transaction on hash mismatch, stale checkpoint, stale action version, or lease conflict.
- [x] Run `pytest tests/test_kanglong_storage_workflow.py -q`.

---

## Phase 4: Ledger and Hashing

### Task 4.1: Add ledger tests

- [ ] Create `tests/test_kanglong_ledger.py`.
- [ ] Add `test_baseline_hash_is_stable_for_canonical_decimal_values`.
- [ ] Add `test_ledger_hash_chains_previous_hash_and_checkpoint_entries`.
- [ ] Add `test_ledger_state_hash_changes_when_account_margin_changes`.
- [ ] Add `test_operation_payload_hash_prevents_replay_with_changed_payload`.
- [ ] Add `test_fee_and_price_wear_entries_are_separate_amounts`.
- [ ] Run `pytest tests/test_kanglong_ledger.py -q` and verify these tests fail.

### Task 4.2: Create `paired_opener/kanglong/ledger.py`

- [ ] Add `KanglongLedgerBaseline`.
- [ ] Add `KanglongLedgerEntry`.
- [ ] Add `KanglongCheckpoint`.
- [ ] Add `canonical_decimal(value, places)`.
- [ ] Add `canonical_json(value)`.
- [ ] Add `hash_baseline(baseline)`.
- [ ] Add `hash_operation_payload(payload)`.
- [ ] Add `hash_checkpoint(previous_ledger_hash, entries)`.
- [ ] Add `hash_ledger_state(baselines, entries)`.

### Task 4.3: Define transfer ledger entries

- [ ] Use entry type `close_position` for the source account leg.
- [ ] Use entry type `open_position` for the target account leg.
- [ ] Use entry type `fee` for fee deductions.
- [ ] Use entry type `price_wear` for difference between planned reference price and execution average price.
- [ ] Use entry type `residual` for dust left after the final allowed round.
- [ ] Use entry type `control` for pause, stop, resume, recover transitions.
- [ ] Use entry type `report` for final report summary provenance.

### Task 4.4: Wire ledger helpers to storage

- [ ] Convert `KanglongLedgerEntry` objects to `kanglong_ledger_entries` insert parameters.
- [ ] Convert stored entries back to dataclasses for hash recalculation.
- [ ] Recompute checkpoint hash before each `commit_kanglong_checkpoint`.
- [ ] Recompute state hash after baseline plus all committed entries.
- [ ] Run `pytest tests/test_kanglong_ledger.py tests/test_kanglong_storage.py -q`.

---

## Phase 5: Market-Driven Transfer Executor

### Task 5.1: Add deterministic market fixture

- [ ] Create `tests/fixtures/kanglong_market/ethusdc_rounds.json`.
- [ ] Include at least six orderbook snapshots for `ETHUSDC`.
- [ ] Include one fully filled round.
- [ ] Include one passive-limit zero-fill round.
- [ ] Include one partial-fill round that needs supplemental execution.
- [ ] Include one stale timestamp snapshot.

### Task 5.2: Add executor tests

- [ ] Create `tests/test_kanglong_executor.py`.
- [ ] Add `test_transfer_round_closes_source_and_opens_target_with_same_filled_qty`.
- [ ] Add `test_partial_second_leg_records_residual_and_supplemental_round`.
- [ ] Add `test_zero_fill_round_waits_for_interval_and_keeps_state_running`.
- [ ] Add `test_max_consecutive_unfilled_pauses_market_unstable`.
- [ ] Add `test_max_events_per_run_pauses_with_final_warning_report`.
- [ ] Add `test_operation_retry_is_idempotent_for_same_payload_hash`.
- [ ] Add `test_operation_retry_with_changed_payload_enters_needs_abort_recover`.
- [ ] Add `test_pause_request_stops_after_current_round_checkpoint`.
- [ ] Add `test_stop_request_records_stopped_by_user_without_new_market_orders`.
- [ ] Run `pytest tests/test_kanglong_executor.py -q` and verify these tests fail.

### Task 5.3: Create executor class

- [ ] Create `paired_opener/kanglong/executor.py`.
- [ ] Add `KanglongTransferExecutor`.
- [ ] Constructor dependencies: storage, market data provider, orderbook matcher, clock, fee policy snapshot, execution config.
- [ ] Use `operation_id = run_id + group_id + round_id + round_attempt`.
- [ ] Store `operation_payload_hash` before mutating ledger state.
- [ ] Read the newest orderbook snapshot for the source close leg.
- [ ] Read a fresh orderbook snapshot for the target open leg.
- [ ] Match source close and target open through the shared matcher.
- [ ] Use the smaller filled quantity as the aligned transfer quantity when one leg partially fills.
- [ ] Persist residual quantities for supplemental rounds.
- [ ] Persist fee and price-wear entries per leg.

### Task 5.4: Reuse simulation supplemental mechanism

- [ ] Map configured `round_count` to the per-round transfer cap only.
- [ ] Allow supplemental attempts after a configured round when residual remains.
- [ ] Enforce defaults: per group `max_supplemental_rounds_per_group=50`, per run `max_supplemental_rounds_per_run=300`, consecutive unfilled `5`, max duration `21600`, max events `20000`.
- [ ] On supplemental limit, transition to `paused_market_unstable` with `recover` action available.
- [ ] On event limit, write a warning event, checkpoint it, generate report summary, and transition to `paused_market_unstable`.

### Task 5.5: Replace static group completion

- [ ] In `paired_opener/kanglong/simulator.py`, remove static instant group completion for engine version 2.
- [ ] Keep a compatibility function that reads old engine version 1 data and returns a read-only report.
- [ ] Route engine version 2 execute/resume calls to `KanglongTransferExecutor`.
- [ ] Ensure execution logs show round-level entries over time instead of all groups completing in one timestamp.
- [ ] Run `pytest tests/test_kanglong_executor.py tests/test_kanglong_workflow_contracts.py -q`.

---

## Phase 6: Service API and State Transitions

### Task 6.1: Add API state transition tests

- [ ] In `tests/test_kanglong_workflow_contracts.py`, add `test_actions_refresh_immediately_after_account_status_passes`.
- [ ] Add `test_actions_refresh_immediately_after_confirm_link`.
- [ ] Add `test_pause_stop_priority_uses_stop_over_pause`.
- [ ] Add `test_stop_pending_cannot_be_downgraded_to_pause_pending`.
- [ ] Add `test_legacy_run_execute_returns_legacy_readonly_error`.
- [ ] Run `pytest tests/test_kanglong_workflow_contracts.py -q`.

### Task 6.2: Implement action matrix helper

- [ ] Add a single `available_actions_for_status(status)` helper in `paired_opener/kanglong/models.py` or `service.py`.
- [ ] Replace duplicated action generation in service responses.
- [ ] Persist `available_actions_json` after detect, confirm, execute start, pause, resume, stop, recover, completion, and blocked states.
- [ ] Ensure old status values map cleanly to either engine version 1 read-only display or engine version 2 phases.

### Task 6.3: Implement control request CAS

- [ ] Add `request_control_action(run_id, action, expected_action_version)` in `paired_opener/storage.py`.
- [ ] Increment `action_version` only when the current version equals `expected_action_version`.
- [ ] Reject stale versions with `kanglong_stale_action_version`.
- [ ] Apply priority `stop > pause`.
- [ ] Keep `stop_pending` once stored until the executor consumes it.

### Task 6.4: Wire API endpoints

- [ ] In `paired_opener/app.py`, validate transfer settings on detect-link.
- [ ] Include `plan_input_hash` in detect-link response.
- [ ] Require `plan_input_hash` on confirm.
- [ ] Include `confirmed_plan_hash` in confirm response.
- [ ] Require `confirmed_plan_hash` on execute.
- [ ] Expose pause, resume, stop, recover using the CAS control request path.
- [ ] Return the fixed error codes from the spec.
- [ ] Run `pytest tests/test_kanglong_workflow_contracts.py tests/test_kanglong_executor.py -q`.

---

## Phase 7: Reporting Fee and Price Wear

### Task 7.1: Add report tests

- [ ] In `tests/test_kanglong_workflow_contracts.py` or a new `tests/test_kanglong_reporter.py`, add `test_report_summary_records_fee_total_by_asset`.
- [ ] Add `test_report_summary_records_price_wear_by_group_and_total`.
- [ ] Add `test_report_summary_records_source_checkpoint_hashes`.
- [ ] Add `test_report_generation_rejects_ledger_hash_mismatch`.
- [ ] Run the report tests and verify they fail.

### Task 7.2: Update reporter contract

- [ ] In `paired_opener/kanglong/reporter.py`, compute total fee from committed `fee` ledger entries.
- [ ] Compute price wear from committed `price_wear` ledger entries.
- [ ] Keep fee values in the actual fee asset deducted by the unified account.
- [ ] Convert reporting display values only when an exchange-rate snapshot is present.
- [ ] If conversion is unavailable, return raw asset totals and warning code `kanglong_conversion_unavailable`.
- [ ] Add `report_version`, `generated_from_checkpoint_id`, `source_ledger_hash`, `source_ledger_state_hash`, `generated_at`, and `summary_status`.

### Task 7.3: Freeze report source at checkpoint

- [ ] Generate report summary from the latest safe checkpoint.
- [ ] Store `report_summary_json` on `kanglong_runs`.
- [ ] Add a ledger `report` entry linking the summary to checkpoint id and hashes.
- [ ] Reject report regeneration if stored checkpoint hash differs from recalculated hash.
- [ ] Run `pytest tests/test_kanglong_ledger.py tests/test_kanglong_workflow_contracts.py -q`.

---

## Phase 8: Frontend Transfer Settings and Logs

### Task 8.1: Add frontend contract tests

- [ ] In `tests/test_app_kanglong_display.mjs`, add `test_transfer_settings_panel_renders_between_account_pool_and_link_detection`.
- [ ] Add `test_locked_fields_follow_symbol_direction_and_leverage`.
- [ ] Add `test_editable_percent_rounds_interval_are_sent_to_detect_link`.
- [ ] Add `test_confirm_button_enables_without_page_refresh_after_status_passes`.
- [ ] Add `test_execute_button_enables_without_page_refresh_after_confirm`.
- [ ] Add `test_execution_logs_do_not_show_template_or_sub_internal_ids`.
- [ ] Add `test_report_panel_shows_fee_and_price_wear_totals`.
- [ ] Run `node tests/test_app_kanglong_display.mjs` and verify these tests fail.

### Task 8.2: Build transfer settings UI

- [ ] In `paired_opener/static/app.js`, render the settings block between account pool and detect-link.
- [ ] Add stable `data-testid` values:
  - `kanglong-transfer-symbol`
  - `kanglong-transfer-mode`
  - `kanglong-transfer-order-side`
  - `kanglong-transfer-leverage`
  - `kanglong-transfer-percent`
  - `kanglong-transfer-round-count`
  - `kanglong-transfer-round-interval`
  - `kanglong-transfer-per-round-qty`
  - `kanglong-detect-link-button`
  - `kanglong-confirm-link-button`
  - `kanglong-execute-button`
- [ ] Lock symbol, mode, order side, leverage, and per-round quantity.
- [ ] Update symbol from the main symbol selector.
- [ ] Update order side from migration direction.
- [ ] Set mode display to `移仓`.
- [ ] Set leverage display to `75X`.

### Task 8.3: Wire immediate action refresh

- [ ] Store latest `plan_input_hash`, `confirmed_plan_hash`, `available_actions`, and `action_version` in Kanglong UI state.
- [ ] After account status succeeds, update button disabled states from returned actions without reload.
- [ ] After detect-link succeeds, enable confirm when `confirm` is present.
- [ ] After confirm succeeds, enable execute when `execute` is present.
- [ ] When editable settings change, clear stale `confirmed_plan_hash` and disable execute until confirm runs again.
- [ ] Display stale-hash API errors beside the related action button.

### Task 8.4: Clean display labels and logs

- [ ] Use frozen display labels from `plan_labels_snapshot` and `account_labels_snapshot`.
- [ ] Remove raw strings matching `tpl:*`, `sub:*`, and `main:*` from visible account cards and execution logs.
- [ ] Keep internal ids only in hidden state or API payloads.
- [ ] Add log rows for source close, target open, residual, supplemental round, fee, price wear, pause, resume, stop, recover, and report generation.
- [ ] Run `node tests/test_app_kanglong_display.mjs`.

---

## Phase 9: Integrated Verification

### Task 9.1: Run Python verification

- [ ] Run `python -m pytest tests/test_simulation_matching.py tests/test_simulation_service.py tests/test_simulation_api.py -q`.
- [ ] Run `pytest tests/test_kanglong_ledger.py tests/test_kanglong_storage.py tests/test_kanglong_executor.py tests/test_kanglong_workflow_contracts.py -q`.
- [ ] Run `python -m compileall paired_opener`.

### Task 9.2: Run frontend verification

- [ ] Run `node tests/test_app_kanglong_display.mjs`.
- [ ] Run `node tests/test_app_simulation_payloads.mjs`.
- [ ] Run `node tests/test_app_kanglong_test_templates.mjs`.

### Task 9.3: Browser smoke

- [ ] Start the local server using the repository's existing launch command.
- [ ] Open `http://127.0.0.1:8000/`.
- [ ] Select `ETHUSDC`.
- [ ] Select migration direction `空`.
- [ ] Confirm transfer settings show symbol `ETHUSDC`, mode `移仓`, side matching `空`, leverage `75X`, and read-only per-round quantity.
- [ ] Enter transfer percent, round count, and interval.
- [ ] Run account status check.
- [ ] Confirm the link button enables without reload.
- [ ] Confirm link.
- [ ] Confirm execute button enables without reload.
- [ ] Start transfer simulation.
- [ ] Observe round logs arriving over multiple ticks, not all groups completing in one timestamp.
- [ ] Pause and resume a running transfer.
- [ ] Stop a running transfer and confirm no new market round is created after stop.
- [ ] Open report and confirm fee totals plus price-wear totals are visible.

### Task 9.4: Regression checks

- [ ] Run `rg -n "tpl:|sub:|main:" paired_opener/static i18n tests` and inspect visible UI strings only.
- [ ] Run `rg -n "simulate_group|GROUP_COMPLETED|ROUND_SIMULATED" paired_opener tests` and ensure remaining hits are legacy read-only compatibility or migration comments.
- [ ] Run `rg -n "TO[D]O|TB[D]|implement [l]ater|fill in [d]etails|appropriate error [h]andling" docs/superpowers/plans/2026-06-09-kanglong-transfer-simulation-redesign-implementation.md paired_opener tests` and remove any vague implementation markers introduced by this work.
- [ ] Run `git diff --check`.

---

## Rollout Notes

- New runs must use `engine_version=2`.
- Existing engine version 1 runs stay visible as read-only reports.
- A refreshed plan creates a new engine version 2 run instead of mutating an old version 1 run into a partially migrated state.
- Unified-account fee deduction is modeled through ledger entries and account available/equity deltas. There is no separate synthetic fee escrow.
- Fee rates are frozen per run from the execution request snapshot. The frozen fee rate is only a calculation input; the fee itself is still deducted in the simulated unified account ledger entries.

## Self-Review Checklist

- [ ] The plan includes tests before implementation work in every risky area.
- [ ] Matching logic is shared by simulation desk and Kanglong transfer execution.
- [ ] Transfer execution uses fresh market snapshots per leg and per round.
- [ ] Checkpoints are append-only and hash-verifiable.
- [ ] Resume/retry behavior is idempotent for the same operation payload.
- [ ] Payload mismatch enters `needs_abort_recover`.
- [ ] Pause and stop are represented as control requests consumed at checkpoint boundaries.
- [ ] UI actions update from API state without requiring page refresh.
- [ ] Visible account labels do not expose template/sub/main internal ids.
- [ ] Report totals distinguish fee cost from price-wear cost.
- [ ] Browser smoke proves transfer is not completed instantly from static cached data.
