import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const html = fs.readFileSync(path.join(process.cwd(), "paired_opener", "static", "index.html"), "utf8");
const appSource = fs.readFileSync(path.join(process.cwd(), "paired_opener", "static", "app.js"), "utf8");

for (const testId of [
  "kanglong-batch-form",
  "kanglong-batch-defaults",
  "kanglong-batch-capacity-preview",
  "kanglong-batch-capacity-account-row",
  "kanglong-batch-queue",
  "kanglong-batch-cost-report",
]) {
  assert.ok(html.includes(`data-testid="${testId}"`) || appSource.includes(`"${testId}"`), `${testId} should exist`);
}

assert.ok(appSource.includes('KANGLONG_BATCH_PLAN_ENDPOINT = "/kanglong/batch-simulation/plan"'));
assert.ok(appSource.includes('KANGLONG_BATCH_CAPACITY_ENDPOINT = "/kanglong/batch-simulation/capacity-preview"'));
assert.ok(appSource.includes('perLegNotional: "250000"'));
assert.ok(appSource.includes("batch_requested_gross_notional"), "batch total must come from the backend preview");
assert.equal(appSource.includes("500000 USD"), false, "batch total must not be hard-coded");
assert.ok(appSource.includes("AbortController"), "stale capacity requests must be cancelled");
assert.ok(appSource.includes("request_seq"));
assert.ok(appSource.includes("input_hash"));
assert.ok(appSource.includes('operation === "close"'), "close mode must bypass open-capacity preview");
assert.ok(appSource.includes("effective_capacity_leverage"));
assert.ok(appSource.includes("projected_symbol_exposure"));
assert.ok(appSource.includes("current_symbol_max_notional_value"));
assert.ok(appSource.includes("snapshot_components"));
assert.ok(appSource.includes("available_actions"), "control buttons must use backend-provided actions");
assert.ok(appSource.includes("expected_action_version"));
assert.ok(appSource.includes("pendingBatchActions"), "timed-out retries must retain their idempotency key and payload");
assert.ok(appSource.includes('"/sessions/open"') || appSource.includes("'/sessions/open'"), "existing trading UI must remain intact");
