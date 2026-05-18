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
  const helpers = appSlice("function renderKanglongWorkspace()", "function renderKanglongSelectedSubaccounts");
  const modalHelpers = appSlice("function selectedKanglongTemplate()", "function buildSimulationRunPayload");
  const requestCalls = [];
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
      realAccountPoolSnapshot: null,
    };
    const renderCalls = [];
    const kanglongConfirmPlanBtn = { disabled: false };
    const kanglongExecutePlanBtn = { disabled: false };
    const kanglongPlanSummary = { replaceChildren() { this.cleared = true; } };
    const kanglongExecutionLog = { replaceChildren() { this.cleared = true; } };
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
    async function request(path, options = {}) {
      requestCalls.push({ path, options });
      return { run_id: "captured-plan", available_actions: [] };
    }
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
      applyKanglongTemplatePreview,
      createKanglongPlan,
      exitKanglongTemplateMode,
      renderKanglongTemplateLibrary,
      state: kanglongState,
      get availableAccounts() { return availableAccounts; },
      library: kanglongTemplateLibrary,
      renderCalls,
      requestCalls,
    };
  `;
  sandbox.__requestCalls = requestCalls;
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
