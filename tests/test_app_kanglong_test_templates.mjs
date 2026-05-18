import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";

const appSource = fs.readFileSync(path.join(process.cwd(), "paired_opener", "static", "app.js"), "utf8");
const indexSource = fs.readFileSync(path.join(process.cwd(), "paired_opener", "static", "index.html"), "utf8");

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
]) {
  assert.ok(appSource.includes(fragment), `${fragment} should be wired in app.js`);
}

assert.equal(
  appSource.includes("模板已更新，当前快照已过期"),
  false,
  "stale template copy should render through i18n instead of hard-coded JS text",
);

function appSlice(start, end) {
  const startIndex = appSource.indexOf(start);
  assert.notEqual(startIndex, -1, `${start} should exist in app.js`);
  const endIndex = appSource.indexOf(end, startIndex);
  assert.notEqual(endIndex, -1, `${end} should exist after ${start}`);
  return appSource.slice(startIndex, endIndex);
}

function makeTemplateHarness() {
  const helpers = appSlice("function renderKanglongWorkspace()", "function renderKanglongSelectedSubaccounts");
  const sandbox = {
    console,
    Array,
    Object,
    Set,
    String,
  };
  const script = `
    const KANGLONG_ACCOUNT_SOURCE_RUNTIME = "runtime";
    const KANGLONG_ACCOUNT_SOURCE_TEST_TEMPLATE = "test_template";
    const KANGLONG_LOG_FILTERS = ["all", "warning", "error", "current_group", "cost", "ledger"];
    let availableAccounts = [
      { id: "runtime-main", account_id: "runtime-main", name: "Runtime Main", role: "main" },
      { id: "runtime-sub", account_id: "runtime-sub", name: "Runtime Sub", role: "subaccount" },
    ];
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
      realAccountPoolSnapshot: null,
    };
    const renderCalls = [];
    const kanglongConfirmPlanBtn = { disabled: false };
    const kanglongExecutePlanBtn = { disabled: false };
    const kanglongPlanSummary = { replaceChildren() { this.cleared = true; } };
    const kanglongExecutionLog = { replaceChildren() { this.cleared = true; } };
    const kanglongTemplatePreview = { textContent: "" };
    function makeContainer() {
      return {
        children: [],
        replaceChildren(...nodes) {
          this.children = nodes;
        },
      };
    }
    const kanglongLogFilters = makeContainer();
    const document = {
      createElement(tagName) {
        return {
          tagName,
          className: "",
          dataset: {},
          hidden: false,
          textContent: "",
          addEventListener(eventName, handler) {
            this["on" + eventName] = handler;
          },
        };
      },
    };
    function copyOrDefault(key, fallback) { return key || fallback; }
    function setEmptyState(container, className, text) { container.emptyState = { className, text }; }
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
    globalThis.api = {
      applyKanglongTemplatePreview,
      exitKanglongTemplateMode,
      state: kanglongState,
      get availableAccounts() { return availableAccounts; },
      renderCalls,
    };
  `;
  vm.runInNewContext(script, sandbox);
  return sandbox.api;
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
