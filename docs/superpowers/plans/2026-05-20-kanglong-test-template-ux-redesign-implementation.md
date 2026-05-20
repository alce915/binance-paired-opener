# Kanglong Test Template UX Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Kanglong test template JSON-first modal with a visual template editor that preserves the existing simulation safety rules.

**Architecture:** Keep the existing `/kanglong/simulation/test-templates` backend endpoints and modal entrypoint, but add a frontend form-state layer that converts between visual fields and the existing template JSON. Persist `market_data_account_id` as the template default while using the current form value for preview/apply. The backend remains the final validator for template persistence and preview.

**Tech Stack:** FastAPI/Pydantic backend, Python `Decimal`, plain HTML/CSS/JavaScript frontend in `paired_opener/static/index.html` and `paired_opener/static/app.js`, Node VM frontend tests, pytest backend/i18n tests.

---

## File Structure

- Modify: `paired_opener/kanglong/test_templates.py`
  - Explicitly preserve and normalize optional `market_data_account_id` at the template root.
  - Keep it out of `template_content_hash` unless the existing backend tests prove current behavior already treats it as hash-affecting.

- Modify: `tests/test_kanglong_test_templates.py`
  - Add contract tests for persisting `market_data_account_id`.
  - Add tests that blank numeric strings are rejected rather than silently treated as zero.

- Modify: `tests/test_kanglong_test_template_api.py`
  - Add or update API coverage proving saved templates return `market_data_account_id`.
  - Keep preview using request-body `market_data_account_id`, not synthetic account ids.

- Modify: `paired_opener/static/index.html`
  - Keep the same modal ids for compatibility.
  - Replace the current JSON-heavy modal body with visual editor-compatible containers only where needed.
  - Update inline styles for the three-column editor, compact form rows, preview cards, dirty/warning/locked states.

- Modify: `paired_opener/static/app.js`
  - Add `kanglongState.testTemplateDraft`, `testTemplateDirty`, `templatePreviewSeq`, `templatePreviewStatus`, and `templateWarningConfirmation`.
  - Add form conversion, validation, dirty-state, preview sequencing, warning confirmation, and active-run lock helpers.
  - Replace `renderKanglongTemplateEditor()` and preview rendering with visual form rendering.
  - Keep `applyKanglongTemplatePreview()`, `exitKanglongTemplateMode()`, and plan request safety checks.

- Modify: `i18n/messages/zh-CN.json`
  - Add all new `console.kanglong.test_template.*` labels, actions, statuses, validation messages, and confirmations.

- Modify: `tests/test_app_kanglong_test_templates.mjs`
  - Extend the existing harness around `selectedKanglongTemplate()` through `buildSimulationRunPayload`.
  - Add tests for form defaults, JSON import, dirty protection, preview sequencing, warning confirmation, and active-run locks.

- Modify: `tests/test_kanglong_i18n_contracts.py`
  - Add the new required frontend message keys.

## Execution Preflight

The current main checkout may already contain unrelated dirty changes from previous Kanglong recovery work. Before executing implementation tasks:

```powershell
git status --short
git diff -- i18n/messages/zh-CN.json tests/test_kanglong_i18n_contracts.py
```

If `i18n/messages/zh-CN.json` or `tests/test_kanglong_i18n_contracts.py` already contain unrelated edits, do not stage the whole file blindly. Either finish and commit the previous work first, or use a reviewed selective stage flow so the test-template UX changes are not mixed with unrelated recovery changes.

---

## Task 1: Backend Template Contract

**Files:**
- Modify: `paired_opener/kanglong/test_templates.py`
- Modify: `tests/test_kanglong_test_templates.py`
- Modify: `tests/test_kanglong_test_template_api.py`

- [ ] **Step 1: Write backend persistence tests**

Add these tests to `tests/test_kanglong_test_templates.py` near the existing store/hash tests:

```python
def test_template_store_persists_market_data_account_id_without_hashing_it(tmp_path) -> None:
    template = template_payload()
    template["market_data_account_id"] = "market-main"
    store = KanglongTemplateStore(tmp_path / "kanglong_test_templates.json")

    saved = store.upsert_template(template)
    reloaded = store.get_template(saved["id"])

    assert saved["market_data_account_id"] == "market-main"
    assert reloaded["market_data_account_id"] == "market-main"

    changed_market = dict(reloaded)
    changed_market["market_data_account_id"] = "market-other"
    assert template_content_hash(reloaded) == template_content_hash(changed_market)
```

Add this parametrized case to the existing invalid numeric coverage:

```python
@pytest.mark.parametrize(
    ("section", "field", "value", "code"),
    [
        ("main", "collateral", "", "kanglong_test_template_invalid_decimal"),
        ("sub", "collateral", "", "kanglong_test_template_invalid_decimal"),
        ("sub", "qty", "", "kanglong_test_template_invalid_decimal"),
        ("sub", "long_entry_price", "", "kanglong_test_template_invalid_decimal"),
        ("sub", "short_entry_price", "", "kanglong_test_template_invalid_decimal"),
    ],
)
def test_upsert_rejects_blank_numeric_template_values(tmp_path, section, field, value, code) -> None:
    template = template_payload()
    target = template["main_account"] if section == "main" else template["subaccounts"][0]
    target[field] = value
    store = KanglongTemplateStore(tmp_path / "kanglong_test_templates.json")

    with pytest.raises(TemplateValidationError) as excinfo:
        store.upsert_template(template)

    assert excinfo.value.code == code
```

- [ ] **Step 2: Run the backend tests and confirm the current failure**

Run:

```powershell
python -m pytest tests/test_kanglong_test_templates.py::test_template_store_persists_market_data_account_id_without_hashing_it tests/test_kanglong_test_templates.py::test_upsert_rejects_blank_numeric_template_values -q
```

Expected before implementation: at least one failure if the field is not explicitly persisted or blank values are silently normalized.

- [ ] **Step 3: Normalize `market_data_account_id` explicitly**

In `paired_opener/kanglong/test_templates.py`, add the field to `_TEMPLATE_KNOWN_FIELDS`:

```python
_TEMPLATE_KNOWN_FIELDS = {
    "id",
    "name",
    "symbol",
    "market_data_account_id",
    "main_account",
    "subaccounts",
    "created_at",
    "updated_at",
    "template_content_hash",
}
```

Add a helper below `validate_template_identifier()`:

```python
def normalize_optional_text(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text
```

Update both `_normalize_template()` and `_normalize_loaded_template()` so the normalized root includes:

```python
"market_data_account_id": normalize_optional_text(
    template.get("market_data_account_id"),
    "market_data_account_id",
),
```

For `_normalize_loaded_template()`, use `source.get("market_data_account_id")` instead of `template.get(...)`.

- [ ] **Step 4: Preserve hash behavior**

Do not add `market_data_account_id` to `_normalize_hash_payload()`. This keeps the saved default行情源 from changing `template_content_hash`; the actual applied行情源 is still passed through preview and plan requests.

- [ ] **Step 5: Run backend template tests**

Run:

```powershell
python -m pytest tests/test_kanglong_test_templates.py tests/test_kanglong_test_template_api.py -q
```

Expected: all tests pass.

- [ ] **Step 6: Commit backend contract changes**

```powershell
git add paired_opener/kanglong/test_templates.py tests/test_kanglong_test_templates.py tests/test_kanglong_test_template_api.py
git commit -m "fix: persist kanglong template market data source"
```

---

## Task 2: Frontend Form State And Conversion Helpers

**Files:**
- Modify: `paired_opener/static/app.js`
- Modify: `tests/test_app_kanglong_test_templates.mjs`

- [ ] **Step 1: Write frontend helper tests**

Extend `tests/test_app_kanglong_test_templates.mjs` so `globalThis.api` exposes the new helper functions:

```javascript
globalThis.api = {
  buildDefaultKanglongTemplateDraft,
  kanglongTemplateToFormState,
  kanglongTemplateFormToPayload,
  validateKanglongTemplateForm,
  isKanglongTemplateFormDirty,
  kanglongPreviewConfirmationKey,
  applyKanglongTemplatePreview,
  createKanglongPlan,
  exitKanglongTemplateMode,
  renderKanglongTemplateLibrary,
  saveCurrentKanglongTemplate,
  state: kanglongState,
  get availableAccounts() { return availableAccounts; },
  setEditorPayload(payload) {
    elementById.set("kanglongTemplateEditorText", { value: JSON.stringify(payload) });
  },
  library: kanglongTemplateLibrary,
  renderCalls,
  requestCalls,
};
```

Add these tests after the harness definition:

```javascript
{
  const api = makeTemplateHarness();
  const draft = api.buildDefaultKanglongTemplateDraft();

  assert.equal(draft.symbol, "ETHUSDC");
  assert.equal(draft.mainAccount.leverage, 75);
  assert.equal(draft.mainAccount.collateral, "");
  assert.equal(draft.subaccounts.length, 1);
  assert.equal(draft.subaccounts[0].qty, "");
}

{
  const api = makeTemplateHarness();
  const form = api.kanglongTemplateToFormState({
    id: "tpl_a",
    name: "Template A",
    symbol: "ethusdc",
    market_data_account_id: "market-main",
    main_account: { account_id: "test-main", name: "Main", collateral: "1000", leverage: 75, positions: [] },
    subaccounts: [
      {
        row_id: "sub-1",
        account_id: "test-sub-1",
        name: "Sub 1",
        collateral: "500",
        leverage: 75,
        long_entry_price: "2400",
        short_entry_price: "2600",
        qty: "1.2",
      },
    ],
  });
  const payload = api.kanglongTemplateFormToPayload(form);

  assert.equal(payload.symbol, "ETHUSDC");
  assert.equal(payload.market_data_account_id, "market-main");
  assert.equal(payload.main_account.collateral, "1000");
  assert.equal(payload.subaccounts[0].row_id, "sub-1");
}

{
  const api = makeTemplateHarness();
  const form = api.buildDefaultKanglongTemplateDraft();
  const result = api.validateKanglongTemplateForm(form);

  assert.equal(result.valid, false);
  assert.ok(result.errors.some((item) => item.field === "main_account.collateral"));
}

{
  const api = makeTemplateHarness();
  const first = api.kanglongPreviewConfirmationKey({
    template_content_hash: "sha256:a",
    snapshot_bundle_id: "snap-a",
    request_seq: 1,
    warnings: [{ code: "w1" }],
  });
  const second = api.kanglongPreviewConfirmationKey({
    template_content_hash: "sha256:a",
    snapshot_bundle_id: "snap-b",
    request_seq: 1,
    warnings: [{ code: "w1" }],
  });

  assert.notEqual(first, second);
}
```

- [ ] **Step 2: Run Node tests and confirm failure**

Run:

```powershell
node tests\test_app_kanglong_test_templates.mjs
```

Expected before implementation: failure because helper functions do not exist.

- [ ] **Step 3: Extend `kanglongState`**

In `paired_opener/static/app.js`, extend the state object near the existing test template fields:

```javascript
  testTemplateDraft: null,
  testTemplateOriginalPayload: null,
  testTemplateDirty: false,
  templatePreviewSeq: 0,
  templatePreviewStatus: "empty",
  templatePreviewWarningsConfirmedKey: "",
  templatePreviewError: null,
  activeTemplateRunLock: null,
```

- [ ] **Step 4: Add form conversion helpers before `selectedKanglongTemplate()`**

Add these helpers before `function selectedKanglongTemplate()`:

```javascript
function nextKanglongTemplateRowId(index = 1) {
  return `sub-${index}`;
}

function buildDefaultKanglongTemplateDraft() {
  return {
    id: "",
    name: "",
    symbol: DEFAULT_SYMBOL,
    marketDataAccountId: kanglongState.marketDataAccountId || currentAccount?.id || "",
    mainAccount: {
      accountId: "test-main",
      name: copyOrDefault("console.kanglong.test_template.default_main_name", "测试主账号"),
      collateral: "",
      leverage: 75,
    },
    subaccounts: [
      {
        rowId: "sub-1",
        accountId: "test-sub-1",
        name: copyOrDefault("console.kanglong.test_template.default_sub_name", "测试子账号 1"),
        collateral: "",
        leverage: 75,
        longEntryPrice: "",
        shortEntryPrice: "",
        qty: "",
      },
    ],
  };
}

function kanglongTemplateToFormState(template = {}) {
  const subaccounts = Array.isArray(template.subaccounts) ? template.subaccounts : [];
  const draft = buildDefaultKanglongTemplateDraft();
  return {
    ...draft,
    id: String(template.id || ""),
    name: String(template.name || ""),
    symbol: String(template.symbol || DEFAULT_SYMBOL).trim().toUpperCase(),
    marketDataAccountId: String(template.market_data_account_id || kanglongState.marketDataAccountId || currentAccount?.id || ""),
    mainAccount: {
      accountId: String(template.main_account?.account_id || "test-main"),
      name: String(template.main_account?.name || draft.mainAccount.name),
      collateral: String(template.main_account?.collateral ?? ""),
      leverage: Number(template.main_account?.leverage || 75),
    },
    subaccounts: subaccounts.length
      ? subaccounts.map((item, index) => ({
          rowId: String(item.row_id || nextKanglongTemplateRowId(index + 1)),
          accountId: String(item.account_id || `test-sub-${index + 1}`),
          name: String(item.name || copyOrDefault("console.kanglong.test_template.default_sub_name_index", "测试子账号 {index}", { index: index + 1 })),
          collateral: String(item.collateral ?? ""),
          leverage: Number(item.leverage || 75),
          longEntryPrice: String(item.long_entry_price ?? ""),
          shortEntryPrice: String(item.short_entry_price ?? ""),
          qty: String(item.qty ?? ""),
        }))
      : draft.subaccounts,
  };
}

function kanglongTemplateFormToPayload(form = kanglongState.testTemplateDraft || buildDefaultKanglongTemplateDraft()) {
  return {
    ...(form.id ? { id: form.id } : {}),
    name: String(form.name || "").trim(),
    symbol: String(form.symbol || DEFAULT_SYMBOL).trim().toUpperCase(),
    market_data_account_id: String(form.marketDataAccountId || "").trim(),
    main_account: {
      account_id: String(form.mainAccount?.accountId || "test-main").trim(),
      name: String(form.mainAccount?.name || "").trim(),
      collateral: String(form.mainAccount?.collateral ?? "").trim(),
      leverage: Number(form.mainAccount?.leverage || 0),
      positions: [],
    },
    subaccounts: (Array.isArray(form.subaccounts) ? form.subaccounts : []).map((item, index) => ({
      row_id: String(item.rowId || nextKanglongTemplateRowId(index + 1)).trim(),
      account_id: String(item.accountId || `test-sub-${index + 1}`).trim(),
      name: String(item.name || "").trim(),
      collateral: String(item.collateral ?? "").trim(),
      leverage: Number(item.leverage || 0),
      long_entry_price: String(item.longEntryPrice ?? "").trim(),
      short_entry_price: String(item.shortEntryPrice ?? "").trim(),
      qty: String(item.qty ?? "").trim(),
    })),
  };
}
```

- [ ] **Step 5: Add validation and dirty helpers**

Add the following below the conversion helpers:

```javascript
function isBlankTemplateValue(value) {
  return String(value ?? "").trim() === "";
}

function isPositiveTemplateNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) && number > 0;
}

function isNonNegativeTemplateNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) && number >= 0;
}

function validateKanglongTemplateForm(form = kanglongState.testTemplateDraft) {
  const errors = [];
  const add = (field, messageCode) => errors.push({ field, messageCode });
  if (!String(form?.name || "").trim()) add("name", "console.kanglong.test_template.validation.name_required");
  if (!String(form?.symbol || "").trim()) add("symbol", "console.kanglong.test_template.validation.symbol_required");
  if (!String(form?.marketDataAccountId || "").trim()) add("market_data_account_id", "console.kanglong.test_template.validation.market_data_required");
  if (isBlankTemplateValue(form?.mainAccount?.collateral)) add("main_account.collateral", "console.kanglong.test_template.validation.empty_numeric");
  if (!isBlankTemplateValue(form?.mainAccount?.collateral) && !isNonNegativeTemplateNumber(form.mainAccount.collateral)) add("main_account.collateral", "console.kanglong.test_template.validation.non_negative_number");
  if (!isPositiveTemplateNumber(form?.mainAccount?.leverage)) add("main_account.leverage", "console.kanglong.test_template.validation.positive_number");
  const rows = Array.isArray(form?.subaccounts) ? form.subaccounts : [];
  if (!rows.length) add("subaccounts", "console.kanglong.test_template.validation.subaccount_required");
  rows.forEach((row, index) => {
    const prefix = `subaccounts.${index}`;
    if (!String(row.name || "").trim()) add(`${prefix}.name`, "console.kanglong.test_template.validation.name_required");
    if (isBlankTemplateValue(row.collateral)) add(`${prefix}.collateral`, "console.kanglong.test_template.validation.empty_numeric");
    if (!isBlankTemplateValue(row.collateral) && !isNonNegativeTemplateNumber(row.collateral)) add(`${prefix}.collateral`, "console.kanglong.test_template.validation.non_negative_number");
    if (!isPositiveTemplateNumber(row.leverage)) add(`${prefix}.leverage`, "console.kanglong.test_template.validation.positive_number");
    if (!isPositiveTemplateNumber(row.longEntryPrice)) add(`${prefix}.long_entry_price`, "console.kanglong.test_template.validation.positive_number");
    if (!isPositiveTemplateNumber(row.shortEntryPrice)) add(`${prefix}.short_entry_price`, "console.kanglong.test_template.validation.positive_number");
    if (!isPositiveTemplateNumber(row.qty)) add(`${prefix}.qty`, "console.kanglong.test_template.validation.positive_number");
  });
  return { valid: errors.length === 0, errors };
}

function normalizedTemplatePayloadText(payload) {
  return JSON.stringify(payload || {}, Object.keys(payload || {}).sort());
}

function isKanglongTemplateFormDirty() {
  const original = kanglongState.testTemplateOriginalPayload || {};
  const current = kanglongTemplateFormToPayload(kanglongState.testTemplateDraft);
  return normalizedTemplatePayloadText(original) !== normalizedTemplatePayloadText(current);
}

function markKanglongTemplateDraftChanged() {
  kanglongState.testTemplateDirty = isKanglongTemplateFormDirty();
  kanglongState.templatePreviewStatus = "stale";
  kanglongState.templatePreviewWarningsConfirmedKey = "";
}

function kanglongPreviewConfirmationKey(preview = kanglongState.templatePreview) {
  const warnings = Array.isArray(preview?.warnings) ? preview.warnings : [];
  const warningCodes = warnings.map((warning) => String(warning.code || warning.messageCode || warning)).sort().join(",");
  return [
    preview?.template_content_hash || preview?.templateContentHash || "",
    preview?.snapshot_bundle_id || preview?.snapshotBundleId || "",
    preview?.request_seq || preview?.requestSeq || kanglongState.templatePreviewSeq || "",
    warningCodes,
  ].join("|");
}
```

- [ ] **Step 6: Run helper tests**

Run:

```powershell
node tests\test_app_kanglong_test_templates.mjs
```

Expected: helper tests pass or fail only on renderer behavior not yet implemented.

- [ ] **Step 7: Commit helper layer**

```powershell
git add paired_opener/static/app.js tests/test_app_kanglong_test_templates.mjs
git commit -m "feat: add kanglong template form state"
```

---

## Task 3: Visual Modal Rendering

**Files:**
- Modify: `paired_opener/static/index.html`
- Modify: `paired_opener/static/app.js`
- Modify: `i18n/messages/zh-CN.json`
- Modify: `tests/test_app_kanglong_test_templates.mjs`
- Modify: `tests/test_kanglong_i18n_contracts.py`

- [ ] **Step 1: Add render tests for visual sections**

In `tests/test_app_kanglong_test_templates.mjs`, add assertions after `api.renderKanglongTestTemplateModal()`:

```javascript
{
  const api = makeTemplateHarness();
  api.state.testTemplateDraft = api.buildDefaultKanglongTemplateDraft();
  api.renderKanglongTestTemplateModal();

  const editorText = JSON.stringify(api.editor.children || []);
  assert.match(editorText, /基础信息|console\.kanglong\.test_template\.editor\.basic/);
  assert.match(editorText, /主账号|console\.kanglong\.test_template\.editor\.main_account/);
  assert.match(editorText, /子账号|console\.kanglong\.test_template\.editor\.subaccounts/);
}
```

Extend the harness `globalThis.api` with:

```javascript
editor: kanglongTemplateEditor,
preview: kanglongTemplatePreview,
renderKanglongTestTemplateModal,
```

- [ ] **Step 2: Add language keys**

Add these keys to `i18n/messages/zh-CN.json`:

```json
"console.kanglong.test_template.editor.basic": "基础信息",
"console.kanglong.test_template.editor.main_account": "主账号",
"console.kanglong.test_template.editor.subaccounts": "子账号",
"console.kanglong.test_template.editor.batch_generator": "批量生成",
"console.kanglong.test_template.editor.advanced_json": "高级 JSON",
"console.kanglong.test_template.preview.title": "预览快照",
"console.kanglong.test_template.preview.empty": "尚未生成预览",
"console.kanglong.test_template.preview.refresh": "刷新预览",
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
"console.kanglong.test_template.validation.warning_confirm_required": "确认警告后才能应用"
```

- [ ] **Step 3: Update i18n contract tests**

In `tests/test_kanglong_i18n_contracts.py`, add the keys above to the required UI message set used by `test_kanglong_test_template_ui_messages_exist()`.

- [ ] **Step 4: Replace modal renderer with section helpers**

In `paired_opener/static/app.js`, split rendering into these functions:

```javascript
function renderKanglongTemplateStatusBar(container) { /* creates badges from state */ }
function renderKanglongTemplateBasicFields(container, form, validation) { /* name, symbol, market source */ }
function renderKanglongTemplateMainAccountFields(container, form, validation) { /* main account fields */ }
function renderKanglongTemplateSubaccountRows(container, form, validation) { /* compact rows */ }
function renderKanglongTemplateBatchGenerator(container, form) { /* batch controls */ }
function renderKanglongTemplateAdvancedJson(container, form) { /* collapsed JSON editor */ }
function renderKanglongTemplatePreviewPanel() { /* preview cards */ }
```

Use small local builders to avoid repeated DOM code:

```javascript
function kanglongTemplateField({ id, labelKey, fallback, value, onInput, type = "text" }) {
  const field = document.createElement("label");
  field.className = "kanglong-template-field";
  const title = document.createElement("span");
  title.textContent = copyOrDefault(labelKey, fallback);
  const input = document.createElement("input");
  input.id = id;
  input.type = type;
  input.value = value ?? "";
  input.addEventListener("input", () => onInput(input.value));
  field.append(title, input);
  return { field, input };
}
```

- [ ] **Step 5: Initialize draft on modal open**

Update `openKanglongTestTemplateModal()` so after templates load:

```javascript
const activeTemplate = selectedKanglongTemplate();
kanglongState.testTemplateDraft = activeTemplate
  ? kanglongTemplateToFormState(activeTemplate)
  : buildDefaultKanglongTemplateDraft();
kanglongState.testTemplateOriginalPayload = activeTemplate
  ? kanglongTemplateFormToPayload(kanglongState.testTemplateDraft)
  : {};
kanglongState.testTemplateDirty = false;
```

- [ ] **Step 6: Update inline styles**

Modify the inline `<style>` in `paired_opener/static/index.html`:

```css
.kanglong-template-modal {
  width: min(1220px, calc(100vw - 32px));
}
.kanglong-template-grid {
  grid-template-columns: 220px minmax(420px, 1fr) minmax(300px, 0.85fr);
  align-items: start;
}
.kanglong-template-section {
  display: grid;
  gap: 10px;
  padding: 12px;
  border: 1px solid rgba(216, 197, 170, 0.72);
  border-radius: 8px;
  background: rgba(255, 253, 248, 0.74);
}
.kanglong-template-section-title {
  margin: 0;
  font-size: 15px;
  font-weight: 800;
}
.kanglong-template-field-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
}
.kanglong-template-sub-row {
  display: grid;
  grid-template-columns: minmax(120px, 1.1fr) repeat(5, minmax(82px, 0.8fr)) auto;
  gap: 8px;
  align-items: end;
}
.kanglong-template-status-bar,
.kanglong-template-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}
.kanglong-template-preview-card {
  display: grid;
  gap: 8px;
  padding: 10px;
  border: 1px solid rgba(216, 197, 170, 0.72);
  border-radius: 8px;
  background: #fffaf3;
}
@media (max-width: 1040px) {
  .kanglong-template-grid,
  .kanglong-template-field-grid,
  .kanglong-template-sub-row {
    grid-template-columns: 1fr;
  }
}
```

- [ ] **Step 7: Run frontend render and i18n tests**

Run:

```powershell
node tests\test_app_kanglong_test_templates.mjs
python -m pytest tests/test_kanglong_i18n_contracts.py -q
```

Expected: both pass.

- [ ] **Step 8: Commit visual rendering**

```powershell
git add paired_opener/static/index.html paired_opener/static/app.js i18n/messages/zh-CN.json tests/test_app_kanglong_test_templates.mjs tests/test_kanglong_i18n_contracts.py
git commit -m "feat: redesign kanglong template modal"
```

---

## Task 4: Preview, Save, Apply, Dirty, Warning, And Active-Run Rules

**Files:**
- Modify: `paired_opener/static/app.js`
- Modify: `tests/test_app_kanglong_test_templates.mjs`

- [ ] **Step 1: Add behavior tests**

Add tests to `tests/test_app_kanglong_test_templates.mjs`:

```javascript
{
  const api = makeTemplateHarness();
  api.state.testTemplateDraft = api.kanglongTemplateToFormState({
    id: "tpl_a",
    name: "Template A",
    symbol: "ETHUSDC",
    market_data_account_id: "market-main",
    main_account: { account_id: "test-main", name: "Main", collateral: "1000", leverage: 75, positions: [] },
    subaccounts: [{
      row_id: "sub-1",
      account_id: "test-sub-1",
      name: "Sub 1",
      collateral: "500",
      leverage: 75,
      long_entry_price: "2400",
      short_entry_price: "2600",
      qty: "1",
    }],
  });

  api.state.templatePreviewSeq = 1;
  api.acceptKanglongTemplatePreviewResponse(1, { template_id: "tpl_a", accounts: [] });
  api.acceptKanglongTemplatePreviewResponse(0, { template_id: "stale", accounts: [] });

  assert.equal(api.state.templatePreview.template_id, "tpl_a");
}

{
  const api = makeTemplateHarness();
  const preview = {
    template_content_hash: "sha256:a",
    snapshot_bundle_id: "snap-a",
    request_seq: 3,
    warnings: [{ code: "warn_a" }],
  };
  const key = api.kanglongPreviewConfirmationKey(preview);
  api.state.templatePreviewWarningsConfirmedKey = key;

  assert.equal(api.isKanglongTemplateWarningConfirmed(preview), true);
  assert.equal(api.isKanglongTemplateWarningConfirmed({ ...preview, snapshot_bundle_id: "snap-b" }), false);
}
```

Expose these helpers in the harness:

```javascript
acceptKanglongTemplatePreviewResponse,
isKanglongTemplateWarningConfirmed,
```

- [ ] **Step 2: Implement preview sequencing**

Add:

```javascript
function acceptKanglongTemplatePreviewResponse(requestSeq, preview) {
  if (requestSeq !== kanglongState.templatePreviewSeq) {
    return false;
  }
  kanglongState.templatePreview = { ...preview, request_seq: requestSeq };
  kanglongState.templatePreviewStatus = Array.isArray(preview?.blocks) && preview.blocks.length ? "blocked" : "ready";
  kanglongState.templatePreviewError = null;
  kanglongState.templatePreviewWarningsConfirmedKey = "";
  return true;
}

async function refreshKanglongTemplatePreview() {
  const validation = validateKanglongTemplateForm(kanglongState.testTemplateDraft);
  if (!validation.valid) {
    kanglongState.templatePreviewStatus = "blocked";
    kanglongState.templatePreviewError = validation.errors[0] || null;
    renderKanglongTestTemplateModal();
    return null;
  }
  const requestSeq = kanglongState.templatePreviewSeq + 1;
  kanglongState.templatePreviewSeq = requestSeq;
  kanglongState.templatePreviewStatus = "loading";
  const saved = await saveKanglongTestTemplate(kanglongTemplateFormToPayload(kanglongState.testTemplateDraft));
  const template = saved?.template || saved;
  const preview = await previewKanglongTestTemplate(template.id, kanglongState.testTemplateDraft.marketDataAccountId);
  if (acceptKanglongTemplatePreviewResponse(requestSeq, {
    ...preview,
    template_id: template.id,
    template_content_hash: template.template_content_hash || preview.template_content_hash,
    market_data_account_id: kanglongState.testTemplateDraft.marketDataAccountId,
  })) {
    kanglongState.activeTestTemplateId = template.id;
    kanglongState.activeTemplateContentHash = template.template_content_hash || preview.template_content_hash || null;
  }
  renderKanglongTestTemplateModal();
  return kanglongState.templatePreview;
}
```

- [ ] **Step 3: Implement warning confirmation helpers**

Add:

```javascript
function templatePreviewWarnings(preview = kanglongState.templatePreview) {
  return Array.isArray(preview?.warnings) ? preview.warnings : [];
}

function templatePreviewBlocks(preview = kanglongState.templatePreview) {
  return Array.isArray(preview?.blocks) ? preview.blocks : [];
}

function isKanglongTemplateWarningConfirmed(preview = kanglongState.templatePreview) {
  const warnings = templatePreviewWarnings(preview);
  if (!warnings.length) return true;
  return kanglongState.templatePreviewWarningsConfirmedKey === kanglongPreviewConfirmationKey(preview);
}

function confirmKanglongTemplateWarnings(preview = kanglongState.templatePreview) {
  kanglongState.templatePreviewWarningsConfirmedKey = kanglongPreviewConfirmationKey(preview);
}
```

Use `window.confirm()` for the first implementation. The prompt text must come from `console.kanglong.test_template.validation.warning_confirm_required`.

- [ ] **Step 4: Implement save and apply with one safety path**

Replace `saveCurrentKanglongTemplate()` with a path that:

1. Builds payload from `kanglongState.testTemplateDraft`.
2. Validates form.
3. Saves template.
4. Updates `kanglongState.testTemplates`, `testTemplateOriginalPayload`, `testTemplateDirty`, `activeTestTemplateId`, and `activeTemplateContentHash`.
5. If `applyPreview` is true, refreshes preview, blocks if `blocks.length > 0`, confirms warnings if needed, and calls `applyKanglongTemplatePreview()`.

The core apply branch should be:

```javascript
if (applyPreview) {
  const preview = await refreshKanglongTemplatePreview();
  if (!preview || templatePreviewBlocks(preview).length) {
    throw new Error(copyOrDefault("console.kanglong.test_template.status.blocked", "存在阻断"));
  }
  if (!isKanglongTemplateWarningConfirmed(preview)) {
    const confirmed = window.confirm(copyOrDefault(
      "console.kanglong.test_template.validation.warning_confirm_required",
      "当前预览存在警告，确认后才能应用。"
    ));
    if (!confirmed) return savedTemplate;
    confirmKanglongTemplateWarnings(preview);
  }
  applyKanglongTemplatePreview(preview);
}
```

- [ ] **Step 5: Implement dirty guards**

Add:

```javascript
function confirmDiscardKanglongTemplateDraft() {
  if (!kanglongState.testTemplateDirty) return true;
  return window.confirm(copyOrDefault(
    "console.kanglong.test_template.confirm_discard_dirty",
    "当前模板有未保存改动，确认放弃这些改动吗？"
  ));
}
```

Call it before:

- Selecting another template.
- Closing the modal.
- Template library direct apply.
- Template deletion.
- Exiting test template mode from inside the modal.

- [ ] **Step 6: Implement active-run lock UI logic**

Use existing active run state if available from `kanglongState.plan` or `kanglongRunId(kanglongState.plan)`. Add:

```javascript
function isCurrentTemplateLockedByActiveRun() {
  return Boolean(
    kanglongState.activeTestTemplateId
    && kanglongState.plan
    && kanglongState.accountSource === KANGLONG_ACCOUNT_SOURCE_TEST_TEMPLATE
    && kanglongRunId(kanglongState.plan)
  );
}
```

Disable save/delete/apply buttons when this returns true, but keep clone and close enabled.

- [ ] **Step 7: Run behavior tests**

Run:

```powershell
node tests\test_app_kanglong_test_templates.mjs
node --check paired_opener\static\app.js
```

Expected: both pass.

- [ ] **Step 8: Commit behavior rules**

```powershell
git add paired_opener/static/app.js tests/test_app_kanglong_test_templates.mjs
git commit -m "feat: harden kanglong template apply flow"
```

---

## Task 5: End-To-End Verification

**Files:**
- Verify only unless a failure points to a concrete fix.

- [ ] **Step 1: Run backend verification**

```powershell
python -m pytest tests/test_kanglong_test_templates.py tests/test_kanglong_test_template_api.py tests/test_kanglong_i18n_contracts.py -q
```

Expected: all tests pass.

- [ ] **Step 2: Run Kanglong workflow regression tests**

```powershell
python -m pytest tests/test_kanglong_workflow_contracts.py tests/test_kanglong_api.py tests/test_kanglong_planner.py tests/test_kanglong_simulator.py -q
```

Expected: all tests pass.

- [ ] **Step 3: Run frontend verification**

```powershell
node tests\test_app_kanglong_display.mjs
node tests\test_app_kanglong_test_templates.mjs
node --check paired_opener\static\app.js
```

Expected: all commands pass.

- [ ] **Step 4: Run syntax and whitespace checks**

```powershell
python -m compileall -q paired_opener
git diff --check
```

Expected: compileall succeeds and `git diff --check` reports no whitespace errors.

- [ ] **Step 5: Check local page manually or with browser tooling**

With the local service on [http://127.0.0.1:8000/](http://127.0.0.1:8000/):

1. Open the Kanglong tab.
2. Open “测试模板”.
3. Confirm the modal shows template library, visual editor, and preview panel.
4. Create a valid template with `ETHUSDC`, `75x`, main collateral, one subaccount, long/short entry prices, and qty.
5. Save template.
6. Save and apply.
7. Confirm the account pool is replaced by test template accounts.
8. Exit test template mode and confirm real accounts return.

If browser automation tools are available, capture screenshots at desktop width and a narrow viewport to verify no text/button overlap.

- [ ] **Step 6: Final commit**

```powershell
git status --short
git add paired_opener/kanglong/test_templates.py paired_opener/static/index.html paired_opener/static/app.js i18n/messages/zh-CN.json tests/test_kanglong_test_templates.py tests/test_kanglong_test_template_api.py tests/test_app_kanglong_test_templates.mjs tests/test_kanglong_i18n_contracts.py
git commit -m "feat: improve kanglong test template editor"
```

Only run the final commit if the task commits above were not used. If the task commits already exist, skip this final commit and keep the branch history as the task commits.

---

## Self-Review

- Spec coverage: The plan covers visual editor layout, default draft values, JSON import rules, `market_data_account_id` persistence, preview sequencing, dirty guards, warning confirmation, active-run locks, language pack extraction, and regression tests.
- Scope check: The plan stays inside the existing test-template module and frontend modal. It does not change Kanglong planner or execution strategy.
- Verification coverage: Backend tests, frontend VM tests, i18n tests, syntax checks, workflow regressions, and local UI verification are included.
- Implementation order: Backend contract first, then frontend state helpers, then visual rendering, then behavior rules, then full verification.
