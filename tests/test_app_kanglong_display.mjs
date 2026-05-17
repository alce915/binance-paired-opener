import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

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
assert.equal(appSource.includes("/kanglong/simulation/run\""), false, "frontend should not call deprecated Kanglong run endpoint");

for (const symbol of [
  "kanglongState",
  "renderKanglongAccountPool",
  "addSelectedKanglongAccounts",
  "removeSelectedKanglongAccount",
  "renderKanglongAccountRow",
  "setKanglongMainAccount",
  "invalidateKanglongPlan",
]) {
  assert.ok(appSource.includes(symbol), `${symbol} should be implemented in app.js`);
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
