import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const appSource = fs.readFileSync(path.join(process.cwd(), "paired_opener", "static", "app.js"), "utf8");
const indexSource = fs.readFileSync(path.join(process.cwd(), "paired_opener", "static", "index.html"), "utf8");

for (const id of ["kanglongPanel", "kanglongMainAccount", "kanglongSubaccounts", "kanglongSelectedSide", "kanglongRunSimulation", "kanglongReport"]) {
  assert.ok(indexSource.includes(`id="${id}"`), `${id} should exist in index.html`);
}

for (const key of [
  "console.kanglong.title",
  "console.kanglong.main_account",
  "console.kanglong.subaccounts",
  "console.kanglong.selected_side",
  "console.kanglong.run_simulation",
  "console.kanglong.report.result_grade",
  "runtime.kanglong.request_failed",
]) {
  assert.ok(indexSource.includes(key) || appSource.includes(key), `${key} should be used by the Kanglong UI`);
}

assert.ok(appSource.includes("/kanglong/simulation/run"), "Kanglong simulation endpoint should be wired");
assert.ok(appSource.includes("runKanglongSimulation"), "Kanglong run handler should exist");

for (const text of ["亢龙有悔移仓模拟", "开始模拟", "主账号", "子账号", "结果等级"]) {
  assert.equal(appSource.includes(text), false, `Kanglong display text should stay in i18n, not app.js: ${text}`);
}
