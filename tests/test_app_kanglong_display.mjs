import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const appSource = fs.readFileSync(path.join(process.cwd(), "paired_opener", "static", "app.js"), "utf8");
const indexSource = fs.readFileSync(path.join(process.cwd(), "paired_opener", "static", "index.html"), "utf8");
const zhSource = fs.readFileSync(path.join(process.cwd(), "i18n", "messages", "zh-CN.json"), "utf8");

for (const id of [
  "navKanglongBtn",
  "kanglongWorkspace",
  "kanglongAccountPool",
  "kanglongSelectedSubaccounts",
  "kanglongPlanSummary",
  "kanglongExecutionLog",
]) {
  assert.ok(indexSource.includes(`id="${id}"`), `${id} should exist in index.html`);
}

assert.equal(indexSource.includes(`id="kanglongPanel"`), false, "old simulation Kanglong panel should be removed");
assert.ok(appSource.includes(`"kanglong"`), "app.js should recognize kanglong as an app page");
assert.ok(appSource.includes("/kanglong/simulation/plan"), "frontend should call split plan endpoint");
assert.equal(appSource.includes("/kanglong/simulation/run\""), false, "frontend should not call deprecated Kanglong run endpoint");

for (const key of [
  "console.kanglong.nav",
  "console.kanglong.stage.account_selection",
  "console.kanglong.account_pool.title",
  "console.kanglong.plan.summary_title",
  "console.kanglong.execution.log_title",
]) {
  assert.ok(indexSource.includes(key) || appSource.includes(key) || zhSource.includes(key), `${key} should be wired`);
}
