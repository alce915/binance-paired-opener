import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const html = fs.readFileSync(path.join(process.cwd(), "paired_opener", "static", "index.html"), "utf8");
const appSource = fs.readFileSync(path.join(process.cwd(), "paired_opener", "static", "app.js"), "utf8");

for (const testId of [
  "kanglong-account-manager",
  "kanglong-account-import",
  "kanglong-account-import-preview",
  "kanglong-account-import-commit",
]) {
  assert.ok(html.includes(`data-testid="${testId}"`), `${testId} should exist`);
}

assert.ok(html.includes('type="password"'), "manual credential entry must mask Secret");
assert.ok(appSource.includes("/config/account-credentials/import/preview"));
assert.ok(appSource.includes("/config/account-credentials/import/commit"));
assert.ok(appSource.includes('credential_type !== "hmac"'), "client should reject non-HMAC imports");
assert.ok(appSource.includes("256 * 1024"), "client should enforce the 256 KiB limit");
assert.ok(appSource.includes("accounts.length > 100"), "client should enforce the 100-account limit");
assert.ok(appSource.includes('"X-Local-Management-Token"'), "protected writes must use the bootstrap token header");
assert.ok(appSource.includes("credential_revision_conflict"), "revision conflicts must invalidate previews");
assert.ok(appSource.includes("migration_required"), "legacy migration guidance must be supported");
assert.equal(appSource.includes('localStorage.setItem("api_secret"'), false);
assert.equal(appSource.includes("document.cookie"), false, "credentials and token must never be written to cookies");
