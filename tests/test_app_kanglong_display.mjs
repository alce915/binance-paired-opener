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
  "kanglongExecutionLog",
]) {
  assert.ok(indexSource.includes(`id="${id}"`), `${id} should exist in index.html`);
}

assert.equal(indexSource.includes(`id="kanglongPanel"`), false, "old simulation Kanglong panel should be removed");
assert.ok(appSource.includes(`"kanglong"`), "app.js should recognize kanglong as an app page");
assert.ok(appSource.includes(`KANGLONG_PLAN_ENDPOINT = "/kanglong/simulation/plan"`), "app.js should declare the split plan endpoint reference");
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
  "renderKanglongPlanSummary",
  "appendKanglongExecutionEvent",
  "newKanglongIdempotencyKey",
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
  "console.kanglong.stage.account_selection",
  "console.kanglong.account_pool.title",
  "console.kanglong.plan.summary_title",
  "console.kanglong.execution.log_title",
]) {
  assert.ok(indexSource.includes(key) || appSource.includes(key), `${key} should be wired in the Task 7 shell`);
}

const zhMessages = JSON.parse(zhSource);
for (const [key, expected] of Object.entries({
  "console.kanglong.plan.status": "状态：{status}",
  "console.kanglong.plan.groups": "组数：{count}",
  "console.kanglong.plan.rounds": "轮次：{count}",
  "console.kanglong.plan.release_qty": "计划释放：{qty}",
  "events.kanglong.group_simulated": "亢龙第 {group_id} 组模拟完成",
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
    const APP_LOCALE = "zh-CN";
    const APP_TIMEZONE = "Asia/Shanghai";
    const I18N_MESSAGES = {
      "console.kanglong.plan.status": "状态：{status}",
      "console.kanglong.plan.groups": "组数：{count}",
      "console.kanglong.plan.rounds": "轮次：{count}",
      "console.kanglong.plan.release_qty": "计划释放：{qty}",
      "console.kanglong.log.empty": "暂无执行日志",
      "events.kanglong.group_simulated": "亢龙第 {group_id} 组模拟完成",
      "runtime.execution_message_unavailable": "日志信息暂不可用",
      "runtime.kanglong.account_selection_required": "请选择主账号和至少一个子账号。",
    };
    const currentAccount = { id: "main" };
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
      logFilter: "all",
    };
    const kanglongConfirmPlanBtn = { disabled: false };
    const kanglongExecutePlanBtn = { disabled: false };
    const kanglongDetectPlanBtn = { disabled: false };
    const kanglongSymbol = { value: "ETHUSDC" };
    const kanglongSide = { value: "LONG" };
    const appendLogEntries = [];
    function makeContainer() {
      return {
        children: [],
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
    const kanglongExecutionLog = makeContainer();
    const document = {
      createElement(tagName) {
        return {
          tagName,
          className: "",
          textContent: "",
          children: [],
          appendChild(child) {
            this.children.push(child);
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
    function nowTime() {
      return "12:00:00";
    }
    function appendLog(level, message, createdAt, options = {}) {
      appendLogEntries.push({ level, message, createdAt, options });
    }
    function userVisibleErrorMessage(error, fallback = "") {
      return error?.message || fallback || "error";
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
      planSummary: kanglongPlanSummary,
      pollKanglongEvents,
      renderKanglongPlanSummary,
      runKanglongWorkflowAction,
      state: kanglongState,
      logs: appendLogEntries,
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
  assert.ok(api.planSummary.textContent.includes("状态：plan_confirmed"), "summary status should prefer top-level response status");
  assert.equal(api.planSummary.textContent.includes("状态：chain_ready"), false, "summary should not show stale report summary status");
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
