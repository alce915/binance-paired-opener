import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";

const appSource = fs.readFileSync(path.join(process.cwd(), "paired_opener", "static", "app.js"), "utf8");
const indexSource = fs.readFileSync(path.join(process.cwd(), "paired_opener", "static", "index.html"), "utf8");
const zhSource = fs.readFileSync(path.join(process.cwd(), "i18n", "messages", "zh-CN.json"), "utf8");

for (const id of [
  "kanglongTestTemplateButton",
  "kanglongTestTemplateModal",
  "kanglongTemplateLibrary",
  "kanglongTemplateEditor",
  "kanglongTemplatePreview",
  "kanglongTemplateSaveButton",
  "kanglongTemplateSaveApplyButton",
]) {
  assert.ok(indexSource.includes(`id="${id}"`), `${id} should exist in index.html`);
}

for (const fragment of [
  "accountSource",
  "test_template",
  "/kanglong/simulation/test-templates",
  "console.kanglong.test_template.snapshot_stale",
  "console.kanglong.test_template.library_empty",
]) {
  assert.ok(appSource.includes(fragment), `${fragment} should be wired in app.js`);
}

assert.equal(
  appSource.includes("模板已更新，当前快照已过期"),
  false,
  "stale template copy should render through i18n instead of hard-coded JS text",
);

assert.match(
  indexSource,
  /id="kanglongTestTemplateCloseButton"[^>]*data-i18n-aria-label="console\.kanglong\.test_template\.close"[^>]*aria-label="关闭"/,
  "template modal close button should localize its aria-label with UTF-8 fallback text",
);
assert.ok(appSource.includes("data-i18n-aria-label"), "app.js should apply localized aria-label attributes");
const zhMessages = JSON.parse(zhSource);
assert.equal(zhMessages["console.kanglong.test_template.close"], "关闭", "template close label should be localized in zh-CN");
assert.equal(zhMessages["console.kanglong.test_template.library_empty"], "暂无测试模板", "empty template library text should be localized in zh-CN");

function appSlice(start, end) {
  const startIndex = appSource.indexOf(start);
  assert.notEqual(startIndex, -1, `${start} should exist in app.js`);
  const endIndex = appSource.indexOf(end, startIndex);
  assert.notEqual(endIndex, -1, `${end} should exist after ${start}`);
  return appSource.slice(startIndex, endIndex);
}

{
  const applyStaticI18nSource = appSlice("function applyStaticI18n", "function statusLabel");
  function makeElement(key, textContent = "", ariaLabel = "") {
    return {
      dataset: { i18n: key, i18nAriaLabel: key },
      textContent,
      attributes: { "aria-label": ariaLabel },
      getAttribute(name) {
        return this.attributes[name] || "";
      },
      setAttribute(name, value) {
        this.attributes[name] = value;
      },
    };
  }
  const missingTextElement = makeElement("console.kanglong.test_template.button", "测试模板");
  const localizedTextElement = makeElement("console.kanglong.test_template.save", "保存模板");
  const missingAriaElement = makeElement("console.kanglong.test_template.close", "", "关闭");
  const sandbox = {
    document: {},
    copyOrDefault(key, fallback) {
      const messages = {
        "console.kanglong.test_template.save": "保存测试模板",
      };
      return messages[key] || fallback || key;
    },
    root: {
      querySelectorAll(selector) {
        if (selector === "[data-i18n]") return [missingTextElement, localizedTextElement];
        if (selector === "[data-i18n-aria-label]") return [missingAriaElement];
        return [];
      },
    },
  };
  vm.runInNewContext(`${applyStaticI18nSource}; applyStaticI18n(root);`, sandbox);

  assert.equal(missingTextElement.textContent, "测试模板", "missing static i18n should preserve existing UTF-8 fallback text");
  assert.equal(localizedTextElement.textContent, "保存测试模板", "existing static i18n message should replace fallback text");
  assert.equal(missingAriaElement.attributes["aria-label"], "关闭", "missing aria i18n should preserve existing aria fallback text");
}

function makeTemplateHarness() {
  const apiHelpers = appSlice("function fetchKanglongTestTemplates()", "function formatNumber");
  const helpers = appSlice("function renderKanglongWorkspace()", "function renderKanglongSelectedSubaccounts");
  const modalHelpers = appSlice("function nextKanglongTemplateRowId", "function buildSimulationRunPayload");
  const requestCalls = [];
  const confirmCalls = [];
  const sandbox = {
    console,
    Array,
    Error,
    JSON,
    Object,
    Promise,
    Set,
    String,
    encodeURIComponent,
  };
  const script = `
    const KANGLONG_ACCOUNT_SOURCE_RUNTIME = "runtime";
    const KANGLONG_ACCOUNT_SOURCE_TEST_TEMPLATE = "test_template";
    const KANGLONG_TEMPLATE_MAX_BATCH_SUBACCOUNTS = 50;
    const KANGLONG_LOG_FILTERS = ["all", "warning", "error", "current_group", "cost", "ledger"];
    const DEFAULT_SYMBOL = "ETHUSDC";
    const KANGLONG_PLAN_ENDPOINT = "/kanglong/simulation/plan";
    const requestCalls = globalThis.__requestCalls;
    let availableAccounts = [
      { id: "runtime-main", account_id: "runtime-main", name: "Runtime Main", role: "main" },
      { id: "runtime-sub", account_id: "runtime-sub", name: "Runtime Sub", role: "subaccount" },
    ];
    const currentAccount = { id: "runtime-main", name: "Runtime Main" };
    const kanglongState = {
      mainAccountId: "runtime-main",
      selectedSubaccountIds: new Set(["runtime-sub"]),
      checkedPoolAccountIds: new Set(["runtime-sub"]),
      plan: { run_id: "old-run" },
      confirmedPlanVersion: "old-plan",
      latestEventId: 8,
      seenEventIds: new Set([8]),
      events: [{ event_id: 8 }],
      logFilter: "all",
      accountSource: KANGLONG_ACCOUNT_SOURCE_RUNTIME,
      testTemplates: [],
      activeTestTemplateId: null,
      activeTemplateContentHash: null,
      marketDataAccountId: null,
      templatePreview: null,
      templatePreviewSeq: 0,
      templatePreviewStatus: "empty",
      templatePreviewWarningsConfirmedKey: "",
      templatePreviewError: null,
      realAccountPoolSnapshot: null,
    };
    const renderCalls = [];
    const kanglongConfirmPlanBtn = { disabled: false };
    const kanglongExecutePlanBtn = { disabled: false };
    const kanglongPlanSummary = { replaceChildren() { this.cleared = true; } };
    const kanglongExecutionLog = { replaceChildren() { this.cleared = true; } };
    const kanglongSymbol = { value: "ETHUSDC" };
    const kanglongSide = { value: "" };
    function makeContainer() {
      return {
        children: [],
        replaceChildren(...nodes) {
          this.children = nodes;
        },
        appendChild(node) {
          this.children.push(node);
        },
        append(...nodes) {
          this.children.push(...nodes);
        },
      };
    }
    const kanglongLogFilters = makeContainer();
    const kanglongTemplateLibrary = makeContainer();
    const kanglongTemplateEditor = makeContainer();
    const kanglongTemplatePreview = makeContainer();
    const elementById = new Map([
      ["kanglongTemplateEditorText", null],
      ["kanglongTemplateMarketDataAccount", { value: "market-main" }],
    ]);
    const document = {
      getElementById(id) {
        return elementById.get(id) || null;
      },
      createElement(tagName) {
        return {
          tagName,
          className: "",
          dataset: {},
          hidden: false,
          textContent: "",
          value: "",
          appendChild(child) {
            this.children = this.children || [];
            this.children.push(child);
          },
          append(...nodes) {
            this.children = this.children || [];
            this.children.push(...nodes);
          },
          addEventListener(eventName, handler) {
            this["on" + eventName] = handler;
          },
        };
      },
    };
    function copyOrDefault(key, fallback) { return key || fallback; }
    function setEmptyState(container, className, text) { container.emptyState = { className, text }; }
    function userVisibleErrorMessage(error, fallback = "") { return error?.message || fallback || "error"; }
    function appendLog() {}
    const window = {
      confirm(message) {
        globalThis.__confirmCalls.push(message);
        return globalThis.__confirmResult;
      },
    };
    async function request(path, options = {}) {
      requestCalls.push({ path, options });
      const failure = globalThis.__requestFailures.find((item) => String(path).includes(item.includes));
      if (failure) {
        const error = new Error(failure.message || "request failed");
        error.detail = failure.detail || null;
        error.code = failure.detail?.code || null;
        throw error;
      }
      if (String(path).includes("/kanglong/simulation/test-templates/") && options.method === "PUT") {
        return {
          template: {
            id: "tpl_a",
            name: "Template A Edited",
            template_content_hash: "sha256:saved-template",
          },
        };
      }
      if (String(path).endsWith("/kanglong/simulation/test-templates") && options.method === "POST") {
        return {
          template: {
            id: "tpl_generated",
            name: "Generated Template",
            template_content_hash: "sha256:generated-template",
          },
        };
      }
      if (String(path).includes("/preview")) {
        return {
          template_id: "tpl_a",
          template_content_hash: "sha256:saved-template",
          snapshot_bundle_id: "snap-preview",
          market_data_account_id: "market-main",
          warnings: [],
          blocks: [],
          accounts: [],
        };
      }
      return { run_id: "captured-plan", available_actions: [] };
    }
    ${apiHelpers}
    function kanglongAccountId(account) {
      if (typeof account === "string") return account.trim().toLowerCase();
      return String(account?.account_id || account?.id || "").trim().toLowerCase();
    }
    function renderKanglongAccountPool(accounts = availableAccounts) {
      renderCalls.push({
        accounts,
        mainAccountId: kanglongState.mainAccountId,
        selectedSubaccountIds: Array.from(kanglongState.selectedSubaccountIds),
      });
    }
    function renderKanglongLogFilters() {}
    function syncKanglongWorkflowButtons() {}
    ${helpers}
    ${modalHelpers}
    globalThis.api = {
      buildDefaultKanglongTemplateDraft,
      kanglongTemplateToFormState,
      kanglongTemplateFormToPayload,
      validateKanglongTemplateForm,
      isKanglongTemplateFormDirty,
      kanglongPreviewConfirmationKey,
      buildKanglongTemplateBatchSubaccounts,
      importKanglongTemplateJsonText,
      acceptKanglongTemplatePreviewResponse,
      isKanglongTemplateWarningConfirmed,
      confirmDiscardKanglongTemplateDraft,
      isCurrentTemplateLockedByActiveRun,
      applyKanglongTemplatePreview,
      createKanglongPlan,
      exitKanglongTemplateMode,
      renderKanglongTemplateLibrary,
      renderKanglongTestTemplateModal,
      saveCurrentKanglongTemplate,
      state: kanglongState,
      get availableAccounts() { return availableAccounts; },
      setEditorPayload(payload) {
        elementById.set("kanglongTemplateEditorText", { value: JSON.stringify(payload) });
      },
      failRequestsMatching(includes, detail = {}) {
        globalThis.__requestFailures.push({
          includes,
          detail,
          message: detail.message || detail.code || "request failed",
        });
      },
      library: kanglongTemplateLibrary,
      editor: kanglongTemplateEditor,
      preview: kanglongTemplatePreview,
      renderCalls,
      requestCalls,
      confirmCalls: globalThis.__confirmCalls,
      setConfirmResult(value) { globalThis.__confirmResult = value; },
    };
  `;
  sandbox.__requestCalls = requestCalls;
  sandbox.__confirmCalls = confirmCalls;
  sandbox.__requestFailures = [];
  sandbox.__confirmResult = true;
  vm.runInNewContext(script, sandbox);
  return sandbox.api;
}

function collectNodeText(node) {
  if (!node) return "";
  const ownText = typeof node.textContent === "string" ? node.textContent : "";
  const childText = Array.isArray(node.children) ? node.children.map(collectNodeText).join(" ") : "";
  return `${ownText} ${childText}`.trim();
}

function findNodeByText(node, text) {
  if (!node) return null;
  if (node.textContent === text) return node;
  if (!Array.isArray(node.children)) return null;
  for (const child of node.children) {
    const match = findNodeByText(child, text);
    if (match) return match;
  }
  return null;
}

function findNode(node, predicate) {
  if (!node) return null;
  if (predicate(node)) return node;
  if (!Array.isArray(node.children)) return null;
  for (const child of node.children) {
    const match = findNode(child, predicate);
    if (match) return match;
  }
  return null;
}

function findTemplateSection(node, titleText) {
  return findNode(node, (candidate) => (
    candidate.className === "kanglong-template-section"
    && collectNodeText(candidate).includes(titleText)
  ));
}

function findFieldInput(node, labelText, occurrence = 0) {
  let seen = 0;
  const match = findNode(node, (candidate) => {
    if (candidate.className !== "kanglong-template-field" || !Array.isArray(candidate.children)) return false;
    if (candidate.children[0]?.textContent !== labelText) return false;
    if (seen !== occurrence) {
      seen += 1;
      return false;
    }
    return true;
  });
  return match?.children?.[1] || null;
}

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
  assert.ok(result.errors.some((item) => item.field === "name"));
  assert.ok(result.errors.some((item) => item.field === "main_account.collateral"));
}

{
  const api = makeTemplateHarness();
  const form = api.kanglongTemplateToFormState({
    id: "tpl_a",
    name: "Template A",
    symbol: "ETHUSDC",
    market_data_account_id: "market-main",
    main_account: { account_id: "test-main", name: "Main", collateral: "1000", leverage: 75, positions: [] },
    subaccounts: Array.from({ length: 51 }, (_, index) => ({
      row_id: `sub-${index + 1}`,
      account_id: `test-sub-${index + 1}`,
      name: `Sub ${index + 1}`,
      collateral: "500",
      leverage: 75,
      long_entry_price: "2400",
      short_entry_price: "2600",
      qty: "1",
    })),
  });
  const result = api.validateKanglongTemplateForm(form);

  assert.equal(result.valid, false);
  assert.ok(result.errors.some((item) => (
    item.field === "subaccounts"
    && item.messageCode === "console.kanglong.test_template.validation.too_many_subaccounts"
  )));
}

{
  const api = makeTemplateHarness();
  const form = api.kanglongTemplateToFormState({
    id: "tpl_a",
    name: "Template A",
    symbol: "ETHUSDC",
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
        qty: "1",
      },
    ],
  });
  api.state.testTemplateDraft = structuredClone(form);
  api.state.testTemplateOriginalPayload = api.kanglongTemplateFormToPayload(form);

  assert.equal(api.isKanglongTemplateFormDirty(), false);
  api.state.testTemplateDraft.mainAccount.collateral = "1001";
  assert.equal(api.isKanglongTemplateFormDirty(), true);
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

{
  const api = makeTemplateHarness();
  const rows = api.buildKanglongTemplateBatchSubaccounts({
    count: 2,
    collateral: "500",
    leverage: 75,
    longEntryPrice: "2400",
    shortEntryPrice: "2600",
    qty: "1.5",
  });

  assert.equal(rows.length, 2);
  assert.equal(rows[0].rowId, "sub-1");
  assert.equal(rows[1].accountId, "test-sub-2");
  assert.equal(rows[0].longEntryPrice, "2400");
  assert.equal(rows[0].shortEntryPrice, "2600");
  assert.equal(rows[0].qty, "1.5");
}

{
  const api = makeTemplateHarness();
  const rows = api.buildKanglongTemplateBatchSubaccounts({
    count: 5000,
    collateral: "500",
    leverage: 75,
    longEntryPrice: "2400",
    shortEntryPrice: "2600",
    qty: "1.5",
  });

  assert.equal(rows.length, 50, "batch generation should cap generated subaccounts");
}

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
  api.renderKanglongTestTemplateModal();

  const subaccountSection = findTemplateSection(api.editor, "console.kanglong.test_template.editor.subaccounts");
  const countInput = findFieldInput(subaccountSection, "console.kanglong.test_template.field.subaccount_count");
  assert.ok(countInput, "subaccount generator should expose a count input inside the subaccount section");
  countInput.value = "3";
  findNodeByText(subaccountSection, "console.kanglong.test_template.actions.generate").onclick();

  assert.equal(api.state.testTemplateDraft.subaccounts.length, 3, "batch generator should create the requested subaccount count");
  assert.equal(api.state.testTemplateDraft.subaccounts[2].accountId, "test-sub-3");
}

{
  const api = makeTemplateHarness();
  api.state.testTemplateDraft = api.kanglongTemplateToFormState({
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
  api.renderKanglongTestTemplateModal();

  const subaccountSection = findTemplateSection(api.editor, "console.kanglong.test_template.editor.subaccounts");
  findFieldInput(subaccountSection, "console.kanglong.test_template.field.subaccount_count").value = "3";
  findNodeByText(subaccountSection, "console.kanglong.test_template.actions.generate").onclick();
  await api.saveCurrentKanglongTemplate();

  api.renderKanglongTestTemplateModal();
  const savedSubaccountSection = findTemplateSection(api.editor, "console.kanglong.test_template.editor.subaccounts");
  assert.equal(api.state.activeTestTemplateId, "tpl_generated", "saving should keep the saved template selected");
  assert.equal(api.state.testTemplateDraft.subaccounts.length, 3, "saving should preserve the generated subaccounts even when the API returns metadata only");
  assert.equal(findFieldInput(savedSubaccountSection, "console.kanglong.test_template.field.subaccount_count").value, "3");

  const newTemplateButton = findNodeByText(api.library, "console.kanglong.test_template.actions.new_template");
  assert.ok(newTemplateButton, "template library should always expose a new-template tab");
  newTemplateButton.onclick();

  assert.equal(api.state.activeTestTemplateId, null);
  assert.equal(api.state.testTemplateDraft.name, "");
  assert.equal(api.state.testTemplateDraft.mainAccount.collateral, "");
  assert.equal(api.state.testTemplateDraft.subaccounts.length, 1);
  assert.equal(api.state.testTemplateDraft.subaccounts[0].qty, "");
}

{
  const api = makeTemplateHarness();
  api.state.testTemplateDraft = api.buildDefaultKanglongTemplateDraft();
  api.state.testTemplateDraft.mainAccount.name = "";
  api.state.testTemplateDraft.mainAccount.collateral = "20000";
  api.renderKanglongTestTemplateModal();

  const mainSection = findTemplateSection(api.editor, "console.kanglong.test_template.editor.main_account");
  const generateMainButton = findNodeByText(mainSection, "console.kanglong.test_template.actions.generate_main");
  assert.ok(generateMainButton, "main account section should expose a generate button");
  generateMainButton.onclick();

  assert.equal(api.state.testTemplateDraft.mainAccount.accountId, "test-main");
  assert.equal(api.state.testTemplateDraft.mainAccount.name, "console.kanglong.test_template.default_main_name");
  assert.equal(api.state.testTemplateDraft.mainAccount.collateral, "20000");
  assert.equal(api.state.testTemplateDraft.mainAccount.leverage, 75);
}

{
  const api = makeTemplateHarness();
  api.state.testTemplateDraft = api.buildDefaultKanglongTemplateDraft();
  const imported = api.importKanglongTemplateJsonText(JSON.stringify({
    id: "tpl_imported",
    name: "Imported",
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
  }));

  assert.equal(imported, true);
  assert.equal(api.state.testTemplateDraft.name, "Imported");
  assert.equal(api.state.testTemplateDirty, true);
}

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
  api.state.testTemplateOriginalPayload = api.kanglongTemplateFormToPayload(api.state.testTemplateDraft);
  api.failRequestsMatching("/preview", {
    code: "kanglong_test_template_market_data_account_unavailable",
    message: "行情源账号不可用",
  });

  let previewError = null;
  try {
    await api.saveCurrentKanglongTemplate({ applyPreview: true });
  } catch (error) {
    previewError = error;
  }

  assert.ok(previewError, "preview failure should still reject save-and-apply");
  assert.equal(api.state.templatePreviewStatus, "blocked");
  assert.equal(api.state.templatePreviewError.code, "kanglong_test_template_market_data_account_unavailable");
  assert.match(collectNodeText(api.preview), /行情源账号不可用|kanglong_test_template_market_data_account_unavailable/);
}

{
  const api = makeTemplateHarness();
  api.state.templatePreviewSeq = 1;

  api.acceptKanglongTemplatePreviewResponse(1, { template_id: "tpl_a", accounts: [] });
  api.acceptKanglongTemplatePreviewResponse(0, { template_id: "stale", accounts: [] });

  assert.equal(api.state.templatePreview.template_id, "tpl_a");
}

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
  api.state.testTemplateOriginalPayload = api.kanglongTemplateFormToPayload(api.state.testTemplateDraft);
  api.state.activeTestTemplateId = "tpl_a";
  api.renderKanglongTestTemplateModal();

  const generatePreviewButton = findNodeByText(api.preview, "console.kanglong.test_template.preview.generate");
  assert.ok(generatePreviewButton, "empty preview panel should expose an explicit generate preview action");
  await generatePreviewButton.onclick();

  assert.equal(api.state.templatePreviewStatus, "ready", "generate preview should fetch a ready template snapshot");
  assert.equal(api.state.templatePreview?.snapshot_bundle_id, "snap-preview", "generated preview should be stored for display");
  assert.equal(api.state.accountSource, "runtime", "generating preview should not apply the template account pool");
  assert.equal(api.availableAccounts[0].account_id, "runtime-main", "runtime account pool should stay active after preview-only generation");
  assert.match(
    collectNodeText(api.preview),
    /console\.kanglong\.test_template\.preview\.generated/,
    "preview-only generation should not claim that the template was applied",
  );
  assert.equal(
    api.requestCalls.some((call) => String(call.path).includes("/preview")),
    true,
    "generate preview should call the backend preview endpoint",
  );
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

{
  const api = makeTemplateHarness();
  api.state.testTemplateDirty = true;
  api.setConfirmResult(false);

  assert.equal(api.confirmDiscardKanglongTemplateDraft(), false);
  assert.equal(api.confirmCalls.length, 1);
}

{
  const api = makeTemplateHarness();
  api.state.accountSource = "test_template";
  api.state.activeTestTemplateId = "tpl_a";
  api.state.plan = { run_id: "run-a" };

  assert.equal(api.isCurrentTemplateLockedByActiveRun(), true);
}

{
  const api = makeTemplateHarness();
  api.state.testTemplateDraft = api.buildDefaultKanglongTemplateDraft();
  api.renderKanglongTestTemplateModal();

  const editorText = collectNodeText(api.editor);
  assert.match(editorText, /基础信息|console\.kanglong\.test_template\.editor\.basic/);
  assert.match(editorText, /主账号|console\.kanglong\.test_template\.editor\.main_account/);
  assert.match(editorText, /子账号|console\.kanglong\.test_template\.editor\.subaccounts/);
}

{
  const api = makeTemplateHarness();
  const preview = {
    template_id: "tpl_eth_drop_001",
    template_content_hash: "sha256:template-v1",
    market_data_account_id: "market-main",
    accounts: [
      { account_id: "tpl:tpl_eth_drop_001:main", name: "Template Main", role: "main" },
      { account_id: "tpl:tpl_eth_drop_001:sub:sub-1", name: "Template Sub", role: "subaccount" },
    ],
  };

  api.applyKanglongTemplatePreview(preview);

  assert.equal(api.state.accountSource, "test_template", "preview apply should enter template account mode");
  assert.equal(api.availableAccounts[0].account_id, "tpl:tpl_eth_drop_001:main", "preview accounts should replace the runtime pool");
  assert.equal(api.state.mainAccountId, "tpl:tpl_eth_drop_001:main", "main account should come from the preview main role");
  assert.deepEqual(Array.from(api.state.selectedSubaccountIds), [], "preview apply should clear selected subaccounts");
  assert.equal(api.state.activeTestTemplateId, "tpl_eth_drop_001", "preview apply should store template id");
  assert.equal(api.state.activeTemplateContentHash, "sha256:template-v1", "preview apply should store template hash");
  assert.equal(api.state.marketDataAccountId, "market-main", "preview apply should store market data account id");
  assert.equal(api.state.plan, null, "preview apply should clear stale plan");
  assert.equal(api.state.events.length, 0, "preview apply should clear stale events");
  assert.equal(api.renderCalls.length > 0, true, "preview apply should rerender the workspace");
}

{
  const api = makeTemplateHarness();
  api.state.activeTemplateContentHash = "sha256:template-v1";
  api.state.templatePreview = {
    template_id: "tpl_eth_drop_001",
    template_content_hash: "sha256:template-v1",
    market_data_account_id: "market-main",
    accounts: [
      {
        account_id: "tpl:tpl_eth_drop_001:sub:sub-1",
        name: "Template Sub",
        role: "subaccount",
        available_balance: "630.2804",
        total_unrealized_pnl: "0",
        positions: [],
      },
    ],
  };

  api.renderKanglongTestTemplateModal();

  const previewText = collectNodeText(api.preview);
  assert.match(previewText, /Template Sub/, "preview should still show the readable account name");
  assert.doesNotMatch(previewText, /tpl:tpl_eth_drop_001:sub:sub-1/, "preview should hide synthetic template account ids");
  assert.match(previewText, /console\.kanglong\.test_template\.preview\.available_balance/, "preview should keep account balance information visible");
}

{
  const api = makeTemplateHarness();
  api.state.testTemplates = [
    { id: "tpl_a", name: "Template A", template_content_hash: "sha256:a" },
    { id: "tpl_b", name: "Template B", template_content_hash: "sha256:b" },
  ];
  api.applyKanglongTemplatePreview({
    template_id: "tpl_a",
    template_content_hash: "sha256:a",
    market_data_account_id: "market-main",
    accounts: [
      { account_id: "tpl:tpl_a:main", name: "Template A Main", role: "main" },
      { account_id: "tpl:tpl_a:sub:sub-1", name: "Template A Sub", role: "subaccount" },
    ],
  });
  api.state.selectedSubaccountIds.add("tpl:tpl_a:sub:sub-1");

  api.renderKanglongTemplateLibrary();
  const templateBButton = api.library.children.find((node) => node.textContent === "Template B");
  assert.ok(templateBButton, "template B should render in the library");
  templateBButton.onclick();

  let createError = null;
  try {
    await api.createKanglongPlan();
  } catch (error) {
    createError = error;
  }

  assert.equal(api.state.accountSource, "runtime", "selecting another library template should exit the applied template account mode");
  assert.equal(api.state.activeTestTemplateId, "tpl_b", "selecting another library template should keep the new template selected for editing");
  assert.equal(api.availableAccounts[0].account_id, "runtime-main", "selecting another library template should restore the real account pool");
  assert.equal(api.requestCalls.length, 0, "plan request should not be sent with old tpl accounts and new template metadata");
  assert.ok(createError, "plan creation should require a fresh runtime/template selection after switching templates");
}

{
  const api = makeTemplateHarness();
  api.state.testTemplates = [
    { id: "tpl_a", name: "Template A", template_content_hash: "sha256:a" },
  ];
  api.applyKanglongTemplatePreview({
    template_id: "tpl_a",
    template_content_hash: "sha256:a",
    market_data_account_id: "market-main",
    accounts: [
      { account_id: "tpl:tpl_a:main", name: "Template A Main", role: "main" },
      { account_id: "tpl:tpl_a:sub:sub-1", name: "Template A Sub", role: "subaccount" },
    ],
  });
  api.state.selectedSubaccountIds.add("tpl:tpl_a:sub:sub-1");
  api.setEditorPayload({
    id: "tpl_a",
    name: "Template A Edited",
    symbol: "ETHUSDC",
    main_account: { collateral: "1000", leverage: 75, positions: [] },
    subaccounts: [],
  });

  await api.saveCurrentKanglongTemplate();

  let createError = null;
  try {
    await api.createKanglongPlan();
  } catch (error) {
    createError = error;
  }
  const planRequests = api.requestCalls.filter((call) => call.path === "/kanglong/simulation/plan");

  assert.equal(api.state.activeTemplateContentHash, "sha256:saved-template", "plain save should record the latest saved template hash");
  assert.equal(api.state.templatePreview.template_content_hash, "sha256:a", "plain save should not silently refresh the applied snapshot");
  assert.equal(api.availableAccounts[0].account_id, "tpl:tpl_a:main", "plain save should leave the old preview visible until the user applies it");
  assert.ok(createError, "stale applied template snapshots should block plan creation");
  assert.equal(planRequests.length, 0, "plan request should not be sent with a stale applied snapshot");
}
