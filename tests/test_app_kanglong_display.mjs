import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";

const appSource = fs.readFileSync(path.join(process.cwd(), "paired_opener", "static", "app.js"), "utf8");
const indexSource = fs.readFileSync(path.join(process.cwd(), "paired_opener", "static", "index.html"), "utf8");
const zhSource = fs.readFileSync(path.join(process.cwd(), "i18n", "messages", "zh-CN.json"), "utf8");

for (const id of [
  "navKanglongBtn",
  "kanglongWorkspace",
  "kanglongMainAccountCard",
  "kanglongAccountPool",
  "kanglongAddSelectedBtn",
  "kanglongSelectedSubaccounts",
  "kanglongPlanSummary",
  "kanglongLogFilters",
  "kanglongExecutionLog",
  "kanglongTestTemplateButton",
  "kanglongTestTemplateModal",
  "kanglongTemplateLibrary",
  "kanglongTemplateEditor",
  "kanglongTemplatePreview",
]) {
  assert.ok(indexSource.includes(`id="${id}"`), `${id} should exist in index.html`);
}

assert.equal(indexSource.includes(`id="kanglongPanel"`), false, "old simulation Kanglong panel should be removed");
assert.ok(appSource.includes(`"kanglong"`), "app.js should recognize kanglong as an app page");
assert.ok(appSource.includes(`KANGLONG_PLAN_ENDPOINT = "/kanglong/simulation/plan"`), "app.js should declare the split plan endpoint reference");
assert.ok(appSource.includes(`"/kanglong/simulation/run/active"`), "app.js should request the active Kanglong run endpoint");
assert.equal(/request\(\s*["']\/kanglong\/simulation\/run["']/.test(appSource), false, "frontend should not POST to the deprecated Kanglong run endpoint");

for (const symbol of [
  "kanglongState",
  "renderKanglongAccountPool",
  "addSelectedKanglongAccounts",
  "removeSelectedKanglongAccount",
  "renderKanglongAccountRow",
  "setKanglongMainAccount",
  "invalidateKanglongPlan",
  "createKanglongPlan",
  "confirmKanglongPlan",
  "executeKanglongPlan",
  "pollKanglongEvents",
  "restoreActiveKanglongRun",
  "renderKanglongPlanSummary",
  "renderKanglongLogFilters",
  "setKanglongLogFilter",
  "formatKanglongStatus",
  "appendKanglongExecutionEvent",
  "newKanglongIdempotencyKey",
  "fetchKanglongTestTemplates",
  "saveKanglongTestTemplate",
  "previewKanglongTestTemplate",
  "applyKanglongTemplatePreview",
  "exitKanglongTemplateMode",
]) {
  assert.ok(appSource.includes(symbol), `${symbol} should be implemented in app.js`);
}

for (const fragment of [
  "/confirm",
  "/execute",
  "/events?after_event_id=",
]) {
  assert.ok(appSource.includes(fragment), `${fragment} endpoint fragment should be wired in app.js`);
}

for (const key of [
  "console.kanglong.nav",
  "console.kanglong.account_pool.title",
  "console.kanglong.plan.summary_title",
  "console.kanglong.execution.log_title",
]) {
  assert.ok(indexSource.includes(key) || appSource.includes(key), `${key} should be wired in the Task 7 shell`);
}
assert.equal(
  indexSource.includes('data-i18n="console.kanglong.stage.account_selection"'),
  false,
  "account selection stage badge should not render in the Kanglong workspace shell",
);

const zhMessages = JSON.parse(zhSource);
const kanglongStatusLabels = {
  blocked_initial_subaccount_unbalanced: "子账号初始仓位未配平",
  blocked_no_profitable_account: "未找到可释放盈利的账号",
  blocked_manual_side_not_profitable: "手动方向不可释放盈利",
  blocked_main_insufficient_capacity: "主账号承接能力不足",
  blocked_plan_recheck_failed: "执行前复检未通过",
};
for (const [key, expected] of Object.entries({
  "console.kanglong.plan.status": "状态：{status}",
  "console.kanglong.plan.groups": "组数：{count}",
  "console.kanglong.plan.rounds": "轮次：{count}",
  "console.kanglong.plan.release_qty": "计划释放：{qty}",
  "events.kanglong.group_simulated": "亢龙第 {group_id} 组模拟完成",
  "runtime.kanglong.status.plan_confirmed": "链路已确认",
  "runtime.kanglong.status.blocked_plan_stale": "检测链路已过期",
  "runtime.kanglong.status.idempotency_conflict": "重复请求冲突",
  "runtime.kanglong.status.kanglong_run_not_found": "运行不存在",
  ...Object.fromEntries(Object.entries(kanglongStatusLabels).map(([status, label]) => [`runtime.kanglong.status.${status}`, label])),
})) {
  assert.equal(zhMessages[key], expected, `${key} should use proper UTF-8 Chinese text`);
}

for (const [key, expected] of Object.entries({
  "console.kanglong.actions.add_selected": "加入子账号",
  "console.kanglong.actions.remove": "移除",
  "console.kanglong.actions.set_main": "设为主账号",
  "console.kanglong.card.no_position": "无本方向持仓",
  "console.kanglong.card.no_profit": "无盈利仓位",
  "console.kanglong.card.stale": "数据过期",
  "console.kanglong.card.joined": "已加入",
  "console.kanglong.card.main": "主账号",
  "console.kanglong.card.risk_unknown": "风险未知",
  "console.kanglong.card.pool_empty": "账号池为空",
  "console.kanglong.card.selected_empty": "尚未选择子账号",
})) {
  assert.equal(zhMessages[key], expected, `${key} should be defined with proper zh-CN text`);
}

const task7I18nKeys = [
  ...indexSource.matchAll(/data-i18n="([^"]+)"/g),
]
  .map((match) => match[1])
  .filter((key) => key === "runtime.symbol" || key.startsWith("console.kanglong."));

for (const key of task7I18nKeys) {
  assert.ok(Object.hasOwn(zhMessages, key), `${key} should be defined in zh-CN.json`);
  assert.notEqual(zhMessages[key], key, `${key} should not render as a raw key`);
}

function appSlice(start, end) {
  const startIndex = appSource.indexOf(start);
  assert.notEqual(startIndex, -1, `${start} should exist in app.js`);
  const endIndex = appSource.indexOf(end, startIndex);
  assert.notEqual(endIndex, -1, `${end} should exist after ${start}`);
  return appSource.slice(startIndex, endIndex);
}

function makeKanglongHarness(requestImpl) {
  const kanglongHelpers = appSlice("function invalidateKanglongPlan()", "function renderKanglongSelectedSubaccounts");
  const actionHelper = appSlice("async function runKanglongWorkflowAction", "kanglongAddSelectedBtn?.addEventListener");
  const sandbox = {
    __request: requestImpl,
    console,
    Date,
    Error,
    Math,
    Number,
    Object,
    Set,
    String,
    encodeURIComponent,
  };
  const script = `
    const DEFAULT_SYMBOL = "ETHUSDC";
    const KANGLONG_PLAN_ENDPOINT = "/kanglong/simulation/plan";
    const KANGLONG_ACCOUNT_SOURCE_RUNTIME = "runtime";
    const KANGLONG_ACCOUNT_SOURCE_TEST_TEMPLATE = "test_template";
    const KANGLONG_LOG_FILTERS = ["all", "warning", "error", "current_group", "cost", "ledger"];
    const APP_LOCALE = "zh-CN";
    const APP_TIMEZONE = "Asia/Shanghai";
    const I18N_MESSAGES = {
      "console.kanglong.plan.status": "状态：{status}",
      "console.kanglong.plan.groups": "组数：{count}",
      "console.kanglong.plan.rounds": "轮次：{count}",
      "console.kanglong.plan.release_qty": "计划释放：{qty}",
      "console.kanglong.log.empty": "暂无执行日志",
      "console.kanglong.logs.filter.all": "全部",
      "console.kanglong.logs.filter.warning": "警告",
      "console.kanglong.logs.filter.error": "错误",
      "console.kanglong.logs.filter.current_group": "当前组",
      "console.kanglong.logs.filter.cost": "成本事件",
      "console.kanglong.logs.filter.ledger": "账本事件",
      "events.kanglong.group_simulated": "亢龙第 {group_id} 组模拟完成",
      "runtime.kanglong.status.plan_confirmed": "链路已确认",
      "runtime.kanglong.status.chain_ready": "链路可确认",
      "runtime.kanglong.status.blocked_plan_stale": "检测链路已过期",
      "runtime.kanglong.status.idempotency_conflict": "重复请求冲突",
      "runtime.kanglong.status.kanglong_run_not_found": "运行不存在",
      "runtime.kanglong.status.blocked_initial_subaccount_unbalanced": "子账号初始仓位未配平",
      "runtime.kanglong.status.blocked_no_profitable_account": "未找到可释放盈利的账号",
      "runtime.kanglong.status.blocked_manual_side_not_profitable": "手动方向不可释放盈利",
      "runtime.kanglong.status.blocked_main_insufficient_capacity": "主账号承接能力不足",
      "runtime.kanglong.status.blocked_plan_recheck_failed": "执行前复检未通过",
      "runtime.execution_message_unavailable": "日志信息暂不可用",
      "runtime.kanglong.account_selection_required": "请选择主账号和至少一个子账号。",
    };
    const currentAccount = { id: "main" };
    let availableAccounts = [];
    const kanglongState = {
      mainAccountId: "main",
      selectedSubaccountIds: new Set(["sub1"]),
      checkedPoolAccountIds: new Set(),
      plan: {
        run_id: "old-run",
        plan_version: "old-plan",
        available_actions: ["confirm", "execute"],
        report: { summary: { status: "chain_ready" } },
      },
      confirmedPlanVersion: "old-plan",
      latestEventId: 0,
      seenEventIds: new Set(),
      events: [],
      logFilter: "all",
      accountSource: KANGLONG_ACCOUNT_SOURCE_RUNTIME,
      testTemplates: [],
      activeTestTemplateId: null,
      activeTemplateContentHash: null,
      marketDataAccountId: null,
      templatePreview: null,
      realAccountPoolSnapshot: null,
    };
    let kanglongActiveRunRestored = false;
    const kanglongConfirmPlanBtn = { disabled: false };
    const kanglongExecutePlanBtn = { disabled: false };
    const kanglongDetectPlanBtn = { disabled: false };
    const kanglongSymbol = { value: "ETHUSDC" };
    const kanglongSide = { value: "LONG" };
    const appendLogEntries = [];
    const renderAccountPoolCalls = [];
    function makeContainer() {
      return {
        children: [],
        hidden: false,
        textContent: "",
        replaceChildren(...nodes) {
          this.children = nodes;
          this.textContent = nodes.map((node) => node.textContent || "").join("");
        },
        appendChild(node) {
          this.children.push(node);
          this.textContent = this.children.map((child) => child.textContent || "").join("\\n");
        },
        querySelector(selector) {
          return this.children.find((child) => selector === ".empty-state" && child.className === "empty-state") || null;
        },
      };
    }
    const kanglongPlanSummary = makeContainer();
    const kanglongLogFilters = makeContainer();
    const kanglongExecutionLog = makeContainer();
    const document = {
      createElement(tagName) {
        return {
          tagName,
          className: "",
          dataset: {},
          hidden: false,
          textContent: "",
          children: [],
          addEventListener(eventName, handler) {
            this["on" + eventName] = handler;
          },
          appendChild(child) {
            this.children.push(child);
            this.textContent = this.children.map((node) => node.textContent || "").join("");
          },
        };
      },
    };
    function copyOrDefault(key, fallback, params = {}) {
      const template = I18N_MESSAGES[key] || fallback || key;
      return template.replace(/\\{(\\w+)\\}/g, (_, name) => {
        const value = params[name];
        return value === undefined || value === null ? "{" + name + "}" : String(value);
      });
    }
    function resolveLogMessage(source = {}, fallback = "") {
      const safeFallback = fallback || source.fallbackMessage || copyOrDefault("runtime.execution_message_unavailable", "日志信息暂不可用");
      const messageCode = source.messageCode || source.message_code;
      const messageParams = source.messageParams || source.message_params || {};
      if (messageCode) {
        const rendered = copyOrDefault(messageCode, messageCode, messageParams);
        if (rendered !== messageCode) return rendered;
      }
      return source.fallbackMessage || safeFallback;
    }
    function setEmptyState(container, className, text) {
      const empty = document.createElement("div");
      empty.className = className;
      empty.textContent = text;
      container.replaceChildren(empty);
    }
    function kanglongAccountId(account) {
      if (typeof account === "string") return account.trim().toLowerCase();
      return String(account?.id || account?.account_id || "").trim().toLowerCase();
    }
    function nowTime() {
      return "12:00:00";
    }
    function appendLog(level, message, createdAt, options = {}) {
      appendLogEntries.push({ level, message, createdAt, options });
    }
    function userVisibleErrorMessage(error, fallback = "") {
      return error?.message || fallback || "error";
    }
    function renderKanglongAccountPool(accounts = []) {
      renderAccountPoolCalls.push({
        accounts,
        mainAccountId: kanglongState.mainAccountId,
        selectedSubaccountIds: Array.from(kanglongState.selectedSubaccountIds),
        symbol: kanglongSymbol.value,
        side: kanglongSide.value,
      });
    }
    async function request(path, options = {}) {
      return globalThis.__request(path, options);
    }
    ${kanglongHelpers}
    ${actionHelper}
    globalThis.api = {
      appendKanglongExecutionEvent,
      confirmButton: kanglongConfirmPlanBtn,
      createKanglongPlan,
      detectButton: kanglongDetectPlanBtn,
      executeButton: kanglongExecutePlanBtn,
      executionLog: kanglongExecutionLog,
      logFilters: kanglongLogFilters,
      planSummary: kanglongPlanSummary,
      pollKanglongEvents,
      renderKanglongPlanSummary,
      renderKanglongLogFilters,
      restoreActiveKanglongRun,
      runKanglongWorkflowAction,
      setKanglongLogFilter,
      state: kanglongState,
      logs: appendLogEntries,
      renderAccountPoolCalls,
    };
  `;
  vm.runInNewContext(script, sandbox);
  return sandbox.api;
}

{
  const api = makeKanglongHarness(async () => {
    throw new Error("network down");
  });
  await api.runKanglongWorkflowAction(api.detectButton, api.createKanglongPlan);
  assert.equal(api.state.plan, null, "failed detect should clear the stale plan");
  assert.equal(api.state.confirmedPlanVersion, "", "failed detect should clear stale confirmation");
  assert.equal(api.confirmButton.disabled, true, "failed detect should disable confirm");
  assert.equal(api.executeButton.disabled, true, "failed detect should disable execute");
}

{
  const api = makeKanglongHarness(async () => ({}));
  api.renderKanglongPlanSummary({
    status: "plan_confirmed",
    report: { summary: { status: "chain_ready", group_count: 2, round_count: 4, planned_release_qty: "1.5" } },
  });
  assert.ok(api.planSummary.textContent.includes("状态：链路已确认"), "summary status should localize the top-level response status");
  assert.equal(api.planSummary.textContent.includes("plan_confirmed"), false, "summary should not expose raw status codes");
  assert.equal(api.planSummary.textContent.includes("状态：链路可确认"), false, "summary should not show stale report summary status");
}

{
  const api = makeKanglongHarness(async () => ({}));
  api.renderKanglongPlanSummary({ status: "blocked_plan_stale" });
  assert.ok(api.planSummary.textContent.includes("状态：检测链路已过期"), "stale plan status should render through i18n");
  assert.equal(api.planSummary.textContent.includes("blocked_plan_stale"), false, "blocked status codes should stay out of user-visible summary text");
}

for (const [status, label] of Object.entries(kanglongStatusLabels)) {
  const api = makeKanglongHarness(async () => ({}));
  api.renderKanglongPlanSummary({ status });
  assert.ok(api.planSummary.textContent.includes(`状态：${label}`), `${status} should render localized summary status`);
  assert.equal(api.planSummary.textContent.includes(status), false, `${status} should not leak into summary text`);
}

{
  const api = makeKanglongHarness(async () => ({}));
  api.renderKanglongLogFilters();
  assert.equal(api.logFilters.children.length >= 6, true, "log filter controls should render in the scaffolded container");

  api.appendKanglongExecutionEvent({
    event_id: 21,
    event_type: "kanglong_group_simulated",
    payload: { message_key: "events.kanglong.group_simulated", message_params: { group_id: "G1" } },
  });
  api.appendKanglongExecutionEvent({
    event_id: 22,
    event_type: "kanglong_cost_recorded",
    payload: { status: "fee_recorded", fee_cost: "0.12" },
  });
  api.appendKanglongExecutionEvent({
    event_id: 23,
    event_type: "kanglong_execute_failed",
    payload: { status: "error", error_code: "kanglong_run_not_found" },
  });

  api.setKanglongLogFilter("current_group");
  assert.equal(api.executionLog.children[0].hidden, false, "current-group filter should keep group rows visible");
  assert.equal(api.executionLog.children[1].hidden, true, "current-group filter should hide non-group rows");

  api.setKanglongLogFilter("cost");
  assert.equal(api.executionLog.children[1].hidden, false, "cost filter should keep cost rows visible");
  assert.equal(api.executionLog.children[2].hidden, true, "cost filter should hide error-only rows");

  api.setKanglongLogFilter("error");
  assert.equal(api.executionLog.children[2].hidden, false, "error filter should keep error rows visible");
  assert.equal(api.executionLog.children[0].hidden, true, "error filter should hide informational rows");

  api.setKanglongLogFilter("all");
  assert.equal(api.executionLog.children.every((row) => row.hidden === false), true, "all filter should restore all rows");
}

{
  const api = makeKanglongHarness(async () => ({}));
  const event = {
    event_id: 7,
    event_type: "kanglong_group_simulated",
    payload: {
      message_key: "events.kanglong.group_simulated",
      message_params: { group_id: "A" },
      status: "filled",
    },
  };
  api.appendKanglongExecutionEvent(event);
  api.appendKanglongExecutionEvent(event);
  assert.equal(api.executionLog.children.length, 1, "duplicate Kanglong event IDs should append once");
  assert.ok(api.executionLog.textContent.includes("亢龙第 A 组模拟完成"), "message_key should render through i18n");
}

{
  const calls = [];
  const api = makeKanglongHarness(async (requestPath) => {
    calls.push(requestPath);
    if (requestPath.endsWith("after_event_id=0")) {
      return {
        run_id: "old-run",
        events: [{ event_id: 1, payload: { message_key: "events.kanglong.group_simulated", message_params: { group_id: "1" } } }],
        next_after_event_id: 1,
        latest_event_id: 2,
        has_more: true,
      };
    }
    return {
      run_id: "old-run",
      events: [{ event_id: 2, payload: { message_key: "events.kanglong.group_simulated", message_params: { group_id: "2" } } }],
      next_after_event_id: 2,
      latest_event_id: 2,
      has_more: false,
    };
  });
  await api.pollKanglongEvents();
  assert.equal(calls.length, 2, "Kanglong event polling should continue while has_more is true");
  assert.equal(api.state.latestEventId, 2, "Kanglong event cursor should advance to the next page cursor");
  assert.equal(api.executionLog.children.length, 2, "both Kanglong event pages should append");
}

{
  const calls = [];
  const api = makeKanglongHarness(async (requestPath) => {
    calls.push(requestPath);
    return { status: "idle", available_actions: ["create_plan"] };
  });
  api.state.plan = null;
  api.state.confirmedPlanVersion = "";
  api.state.latestEventId = 0;
  api.confirmButton.disabled = true;
  api.executeButton.disabled = true;

  await api.restoreActiveKanglongRun();

  assert.deepEqual(calls, ["/kanglong/simulation/run/active"], "idle restore should only query the active endpoint");
  assert.equal(api.state.plan, null, "idle active response should leave empty Kanglong state empty");
  assert.equal(api.state.confirmedPlanVersion, "", "idle active response should not set a confirmed plan");
  assert.equal(api.confirmButton.disabled, true, "idle active response should keep confirm disabled");
  assert.equal(api.executeButton.disabled, true, "idle active response should keep execute disabled");
}

{
  const calls = [];
  const api = makeKanglongHarness(async (requestPath) => {
    calls.push(requestPath);
    if (requestPath === "/kanglong/simulation/run/active") {
      return {
        run_id: "active-run",
        status: "plan_confirmed",
        plan_version: "plan-active",
        latest_event_id: 5,
        available_actions: ["execute", "refresh_plan"],
        request: {
          symbol: "BTCUSDC",
          main_account_id: "main-active",
          subaccount_ids: ["sub-active"],
          selected_side: "SHORT",
        },
        report: {
          summary: {
            group_count: 2,
            round_count: 4,
            planned_release_qty: "1.25",
          },
        },
      };
    }
    assert.equal(requestPath, "/kanglong/simulation/run/active-run/events?after_event_id=0");
    return {
      run_id: "active-run",
      events: [
        { event_id: 1, payload: { message_key: "events.kanglong.group_simulated", message_params: { group_id: "A" } } },
        { event_id: 2, payload: { message_key: "events.kanglong.group_simulated", message_params: { group_id: "B" } } },
        { event_id: 2, payload: { message_key: "events.kanglong.group_simulated", message_params: { group_id: "B" } } },
        { event_id: 5, payload: { message_key: "events.kanglong.group_simulated", message_params: { group_id: "E" } } },
      ],
      next_after_event_id: 5,
      latest_event_id: 5,
      has_more: false,
    };
  });

  await api.restoreActiveKanglongRun();

  assert.deepEqual(calls, [
    "/kanglong/simulation/run/active",
    "/kanglong/simulation/run/active-run/events?after_event_id=0",
  ], "active restore should poll existing events from the beginning");
  assert.equal(api.state.plan.run_id, "active-run", "active payload should become the current Kanglong plan");
  assert.equal(api.state.confirmedPlanVersion, "plan-active", "confirmed active plan should restore execute version");
  assert.equal(api.state.latestEventId, 5, "event polling should advance to the restored latest event cursor");
  assert.equal(api.executeButton.disabled, false, "execute should be enabled for a restored executable plan");
  assert.equal(api.confirmButton.disabled, true, "confirm should be disabled when only execute is available");
  assert.ok(api.planSummary.textContent.includes("链路已确认"), "restored plan summary should render localized active status");
  assert.equal(api.planSummary.textContent.includes("plan_confirmed"), false, "restored plan summary should not expose raw status");
  assert.equal(api.executionLog.children.length, 3, "restored active run should render existing events without duplicates");
  assert.equal(api.renderAccountPoolCalls.length, 1, "restored selection should rerender the account pool");
  assert.equal(JSON.stringify(api.renderAccountPoolCalls[0].selectedSubaccountIds), JSON.stringify(["sub-active"]), "account pool rerender should see restored subaccounts");
  assert.equal(api.renderAccountPoolCalls[0].symbol, "BTCUSDC", "account pool rerender should see restored symbol");
  assert.equal(api.renderAccountPoolCalls[0].side, "SHORT", "account pool rerender should see restored side");
}

{
  const api = makeKanglongHarness(async (requestPath) => {
    if (requestPath === "/kanglong/simulation/run/active") {
      return {
        run_id: "active-template-run",
        status: "plan_confirmed",
        plan_version: "plan-template",
        latest_event_id: 0,
        available_actions: ["execute"],
        request: {
          account_source: "test_template",
          test_template_id: "tpl_eth_drop_001",
          template_content_hash: "sha256:template-v1",
          market_data_account_id: "market-main",
          main_account_id: "tpl:tpl_eth_drop_001:main",
          subaccount_ids: ["tpl:tpl_eth_drop_001:sub:sub-1"],
        },
        report: {
          synthetic_account_state: {
            accounts: [
              { account_id: "tpl:tpl_eth_drop_001:main", name: "Synthetic Main", role: "main" },
              { account_id: "tpl:tpl_eth_drop_001:sub:sub-1", name: "Synthetic Sub", role: "subaccount" },
            ],
          },
          account_snapshot: {
            accounts: [
              { account_id: "tpl:tpl_eth_drop_001:main", name: "Snapshot Main", role: "main" },
            ],
          },
        },
      };
    }
    return {
      run_id: "active-template-run",
      events: [],
      next_after_event_id: 0,
      latest_event_id: 0,
      has_more: false,
    };
  });

  await api.restoreActiveKanglongRun();

  assert.equal(api.state.accountSource, "test_template", "active template run should restore template account source");
  assert.equal(api.state.activeTestTemplateId, "tpl_eth_drop_001", "active template run should restore template id");
  assert.equal(api.state.activeTemplateContentHash, "sha256:template-v1", "active template run should restore template hash");
  assert.equal(api.state.marketDataAccountId, "market-main", "active template run should restore market data account id");
  assert.equal(api.state.mainAccountId, "tpl:tpl_eth_drop_001:main", "template main account should be resolved after replacing the account pool");
  assert.deepEqual(Array.from(api.state.selectedSubaccountIds), ["tpl:tpl_eth_drop_001:sub:sub-1"], "template subaccount should be restored after replacing the account pool");
  assert.equal(api.renderAccountPoolCalls[0].accounts[0].name, "Synthetic Main", "active restore should prefer synthetic account state over the original snapshot");
}

{
  const calls = [];
  const api = makeKanglongHarness(async (requestPath) => {
    calls.push(requestPath);
    if (requestPath === "/kanglong/simulation/run/active") {
      return {
        run_id: "active-run",
        status: "plan_confirmed",
        plan_version: "plan-active",
        latest_event_id: 5,
        available_actions: ["execute"],
        report: { summary: { group_count: 1 } },
      };
    }
    throw new Error("event polling failed");
  });

  await api.restoreActiveKanglongRun();
  await api.restoreActiveKanglongRun();

  assert.equal(api.state.plan.run_id, "active-run", "restore should keep the active plan when event polling fails");
  assert.equal(api.state.latestEventId, 0, "failed restore polling should not seed the cursor from active latest_event_id");
  assert.deepEqual(calls, [
    "/kanglong/simulation/run/active",
    "/kanglong/simulation/run/active-run/events?after_event_id=0",
    "/kanglong/simulation/run/active",
    "/kanglong/simulation/run/active-run/events?after_event_id=0",
  ], "failed restore polling should leave active restore retryable from the beginning");
  assert.equal(api.logs.length, 2, "each failed restore polling attempt should be logged");
  assert.equal(api.logs[0].options.messageCode, "runtime.kanglong.request_failed", "restore polling failure should use Kanglong request error copy");
}
