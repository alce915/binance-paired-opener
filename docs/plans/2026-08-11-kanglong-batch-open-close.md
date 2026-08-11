# 亢龙多账号双腿开平仓 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在本机网页中安全管理有序的 Binance 统一账户凭据，并按账号顺序使用真实行情完成可恢复、可配平、可核算成本的双腿开平仓模拟。

**Architecture:** 复用 `codex/kanglong-transfer-executor-v2` 已验证的共享盘口撮合、运行记录、账本、事件、checkpoint、租约/fencing 和报表能力；只新增轻量批次编排器及 `kanglong_batch_accounts` 账号队列表，不另建一套平行账本、事件、锁或报表基础设施。凭据使用 Windows DPAPI CurrentUser 加密文件保存，通过受保护的本地管理接口更新，前端永不获得 Secret。

**Tech Stack:** Python 3.12、FastAPI、Pydantic v2、httpx、SQLite、原生 HTML/CSS/JavaScript、pytest、Node.js test runner、Binance Portfolio Margin `/papi` 只读 API。

## Global Constraints

- 第一阶段只能使用真实行情和真实账户只读数据模拟成交，不得提交、修改或撤销真实订单。
- 默认杠杆 `100X`；默认每腿名义价值 `250000 USD`；双腿合计约 `500000 USD`。
- 第一阶段不修改真实杠杆；保守容量使用 `min(requested_leverage, current_symbol_leverage, bracket_max_allowed_leverage)`，不得把尚未生效的请求杠杆用于占用百分比。
- 杠杆档位必须按已有仓位、挂单和双腿总名义价值形成的投影敞口选择，并同时受 `symbolConfig.maxNotionalValue` 限制；账号返回的 `notionalCoef` 必须进入档位归一化和计划 hash。
- 默认交易对、优先方向、杠杆、每腿名义价值、初始轮次和轮次间隔必须可在网页配置并持久化。
- 每次参数变化必须重新计算每账号和批次的“保守可开仓估算占用百分比”；该估算基于带时间戳的当前快照，不得表述为 Binance 承诺的可成交额度。
- 账号串行执行；当前账号未配平时不得推进下一个账号。
- API Secret 不得出现在响应、日志、SQLite、前端状态或明文配置文件中。
- 第一阶段只支持现有 HMAC 签名网关；RSA/Ed25519 凭据结构化拒绝，不扩展第二套签名器。
- 凭据管理接口必须同时校验 loopback 客户端、loopback `Host`、允许的 `Origin` 以及 CSRF/本地管理 token；DPAPI 不可用、密文损坏或文件权限无法收紧时必须 fail closed。
- 所有配置和批次状态变更 endpoint 必须校验同源 `Origin` 与 CSRF/本地管理 token；GET 查询保持无副作用。
- 同一配置/数据目录只允许一个服务进程并固定 `uvicorn workers=1`；启动时必须获取操作系统管理的独占实例锁，第二个实例 fail closed，以保证进程内凭据 CAS、容量缓存和共享限频预算成立。
- 平仓目标与阻断规则来自来源 run ledger，不复用开仓容量阻断；低可开容量不能阻止合法平仓。
- 所有批次后台任务必须由进程内 registry 按 `run_id` 跟踪，shutdown 在关闭 runtime/SQLite 前完成停止调度、等待或取消。
- 统一账户使用 `/papi/v1/...` 读取；公开行情继续使用 USDⓈ-M 公共接口。
- 所有中文与 JSON 文件使用 UTF-8。
- 实现基线必须包含 `codex/kanglong-transfer-executor-v2` 已验证的执行内核；实施和合并时按实际 `git status` 保留用户未提交改动，不在计划中固化临时文件数量。
- 接口契约先于实现更新。

---

### Task 1: 冻结 API 契约和批次请求模型

**Files:**
- Create: `docs/API_CONTRACT.md`
- Create: `docs/openapi/kanglong-batch-simulation.yaml`
- Modify: `paired_opener/schemas.py`
- Test: `tests/test_kanglong_batch_contracts.py`

**Interfaces:**
- Produces: `AccountCredentialCreateRequest`、`AccountCredentialUpdateRequest`、`AccountCredentialImportPreviewRequest`、`AccountCredentialImportPreviewResponse`、`AccountCredentialImportCommitRequest`、`AccountCredentialOrderRequest`、`KanglongBatchPlanRequest`、`KanglongBatchRunResponse`、`KanglongBatchRecoverRequest`；批次确认/执行复用现有 `KanglongActionRequest`，暂停/继续/停止复用现有 `KanglongControlRequest`，不修改现有移仓使用的 `KanglongRecoverRequest` 必填字段。
- Produces endpoints: `/config/account-credentials/*` 与 `/kanglong/batch-simulation/*`。

- [x] **Step 1: 先在契约文档中定义请求与响应**

`docs/API_CONTRACT.md` 必须写明：

```text
GET    /config/account-credentials
POST   /config/account-credentials/import/preview
POST   /config/account-credentials/import/commit
POST   /config/account-credentials
PUT    /config/account-credentials/{account_id}
DELETE /config/account-credentials/{account_id}
PUT    /config/account-credentials/order
POST   /config/account-credentials/{account_id}/verify

POST   /kanglong/batch-simulation/plan
POST   /kanglong/batch-simulation/plan/{run_id}/confirm
POST   /kanglong/batch-simulation/plan/{run_id}/execute
POST   /kanglong/batch-simulation/run/{run_id}/{action}
GET    /kanglong/batch-simulation/run/{run_id}
GET    /kanglong/batch-simulation/run/{run_id}/events
GET    /kanglong/batch-simulation/open-runs
```

凭据列表响应只能包含 `api_key_masked` 和 `has_api_secret`，禁止出现 `api_secret`。导入限制为 UTF-8 JSON、最大 256 KiB、最多 100 个账号；前者必须在服务端 JSON 解析前执行 body 限制，后者由 Pydantic 再次校验。默认 `merge`，显式选择 `replace` 才可删除未出现在文件中的账号。预览必须返回候选顺序、变更摘要、基准 `credential_revision`、默认 5 分钟的 `expires_at` 和一次性 `preview_token`。token 同时绑定基准 revision 与候选内容哈希；提交时任一记录无效或当前 revision 已变化都使整批失败，后者返回 `409 credential_revision_conflict`。凭据 schema 增加固定 `credential_type="hmac"`；其他类型返回 `credential_type_not_supported`。

批次控制接口直接复用现有 `kanglong_idempotency` 和保存在 `progress_json.action_version` 的动作版本：确认/执行请求必须携带 `plan_version` 与 `idempotency_key`；暂停/继续/停止必须再携带 `expected_action_version`；批次恢复使用新的 `KanglongBatchRecoverRequest` 携带 `plan_version`、`expected_action_version` 和 `release_reason`。相同 idempotency key 与相同请求 hash 返回首次结果，不重复执行副作用；同 key 不同请求返回 `409 idempotency_key_conflict`；过期 `plan_version` 返回 `409 plan_version_conflict`，过期动作版本返回 `409 action_version_conflict`。

契约把 `running`、`pause_pending`、`paused_by_user`、`paused_market_unstable`、`stop_pending`、`stopped_by_user` 和 `completed_with_dust_residual` 列为正式 run status；`blocked_plan_stale` 的动作固定为 `refresh_plan/view_report`，`paused_plan_recheck_changed` 固定为 `refresh_plan/stop/view_report`，二者不提供 recover。`paused_plan_stale` 不进入契约。

- [x] **Step 2: 写契约红测**

```python
def test_credential_summary_never_has_secret_field() -> None:
    summary = AccountCredentialSummary(
        account_id="a1",
        name="账号 1",
        api_key_masked="ABCD…WXYZ",
        has_api_secret=True,
        account_mode="portfolio_margin",
        enabled=True,
        order=0,
    )
    assert "api_secret" not in summary.model_dump()


def test_close_plan_requires_source_open_run_id() -> None:
    with pytest.raises(ValidationError):
        KanglongBatchPlanRequest(
            operation="close",
            symbol="ETHUSDC",
            preferred_side="LONG",
            leverage=100,
            per_leg_notional="250000",
            account_ids=["a1"],
        )


def test_create_requires_secret_but_update_may_retain_existing_secret() -> None:
    with pytest.raises(ValidationError):
        AccountCredentialCreateRequest(account_id="a1", name="账号 1", api_key="KEY-123456")
    update = AccountCredentialUpdateRequest(name="新名称")
    assert update.api_secret is None


def test_batch_plan_round_count_is_part_of_contract() -> None:
    request = KanglongBatchPlanRequest(
        operation="open",
        symbol="ETHUSDC",
        preferred_side="LONG",
        account_ids=["a1"],
        round_count=30,
    )
    assert request.round_count == 30


def test_import_preview_exposes_revision_and_expiry_but_no_secret() -> None:
    response = AccountCredentialImportPreviewResponse.model_validate(preview_payload())
    assert response.credential_revision
    assert response.expires_at
    assert "api_secret" not in response.model_dump_json()


def test_batch_actions_reuse_plan_and_action_version_contracts() -> None:
    action = KanglongActionRequest(plan_version="plan-v1", idempotency_key="confirm-0001")
    control = KanglongControlRequest(
        plan_version="plan-v1",
        expected_action_version=3,
        idempotency_key="pause-0001",
    )
    recover = KanglongBatchRecoverRequest(
        plan_version="plan-v1",
        expected_action_version=4,
        idempotency_key="recover-0001",
        release_reason="operator reviewed checkpoint",
    )
    assert action.plan_version == control.plan_version == recover.plan_version
```

- [x] **Step 3: 运行红测**

Run: `python -m pytest tests/test_kanglong_batch_contracts.py -q`

Expected: FAIL，缺少新的 schema 类型。

- [x] **Step 4: 实现最小 Pydantic 模型**

```python
class AccountCredentialCreateRequest(BaseModel):
    account_id: str = Field(..., min_length=1, max_length=64, pattern=r"^[a-zA-Z0-9_-]+$")
    name: str = Field(..., min_length=1, max_length=100)
    api_key: str = Field(..., min_length=8, max_length=256)
    api_secret: str = Field(..., min_length=8, max_length=256)
    credential_type: Literal["hmac"] = "hmac"
    account_mode: Literal["portfolio_margin"] = "portfolio_margin"
    enabled: bool = True


class AccountCredentialUpdateRequest(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=100)
    api_key: str | None = Field(default=None, min_length=8, max_length=256)
    api_secret: str | None = Field(default=None, min_length=8, max_length=256)
    enabled: bool | None = None

    @model_validator(mode="after")
    def require_at_least_one_change(self):
        if not self.model_fields_set:
            raise ValueError("at least one field must be supplied")
        return self


class AccountCredentialImportPreviewRequest(BaseModel):
    accounts: list[AccountCredentialCreateRequest] = Field(..., min_length=1, max_length=100)
    mode: Literal["merge", "replace"] = "merge"


class AccountCredentialImportCommitRequest(BaseModel):
    preview_token: str = Field(..., min_length=32, max_length=128)


class AccountCredentialImportChanges(BaseModel):
    added_account_ids: list[str]
    updated_account_ids: list[str]
    unchanged_account_ids: list[str]
    removed_account_ids: list[str]


class AccountCredentialImportPreviewResponse(BaseModel):
    preview_token: str
    credential_revision: str
    expires_at: datetime
    final_accounts: list[AccountCredentialSummary]
    changes: AccountCredentialImportChanges


class AccountCredentialOrderRequest(BaseModel):
    account_ids: list[str] = Field(..., min_length=1, max_length=100)


class KanglongBatchPlanRequest(BaseModel):
    operation: Literal["open", "close"]
    symbol: str
    preferred_side: PositionSide
    leverage: int = Field(default=100, ge=1, le=125)
    per_leg_notional: Decimal = Field(default=Decimal("250000"), gt=0)
    account_ids: list[str] = Field(..., min_length=1, max_length=100)
    source_open_run_id: str | None = None
    round_count: int = Field(default=30, ge=1, le=500)
    round_interval_seconds: int = Field(default=3, ge=0, le=3600)

    @model_validator(mode="after")
    def validate_source_run(self):
        if self.operation == "close" and not self.source_open_run_id:
            raise ValueError("source_open_run_id is required for close")
        return self


class KanglongBatchRecoverRequest(BaseModel):
    plan_version: str
    expected_action_version: int = Field(..., ge=0)
    idempotency_key: str = Field(..., min_length=8, max_length=128)
    operator: str = Field(default="manual")
    release_reason: str = Field(..., min_length=3, max_length=500)
```

- [x] **Step 5: 完成 OpenAPI 文件并回跑**

Run: `python -m pytest tests/test_kanglong_batch_contracts.py -q`

Expected: PASS；OpenAPI 与 Pydantic 的字段、约束、导入模式和 `round_count` 一致，并明确动作幂等、计划版本和动作版本的 409 冲突响应。

### Task 2: 实现 DPAPI 加密凭据仓库

**Files:**
- Create: `paired_opener/account_credentials.py`
- Modify: `paired_opener/config.py`
- Modify: `.gitignore`
- Test: `tests/test_account_credentials.py`

**Interfaces:**
- Produces: `SecretProtector` protocol、`WindowsDpapiProtector`、`AccountCredentialStore`、`CredentialStoreState`。
- Produces: `AccountCredentialStore.load() -> list[AccountConfig]`、`state() -> CredentialStoreState`、`current_revision()`、`prepare()`、`commit(expected_revision=...)`、`upsert()`、`delete()`、`reorder()`。

- [x] **Step 1: 写加密、原子写入和掩码红测**

```python
class FakeProtector:
    def protect(self, value: bytes) -> bytes:
        return b"cipher:" + value[::-1]

    def unprotect(self, value: bytes) -> bytes:
        assert value.startswith(b"cipher:")
        return value[7:][::-1]


def test_store_persists_only_ciphertext(tmp_path: Path) -> None:
    path = tmp_path / "accounts.secure.json"
    store = AccountCredentialStore(path, FakeProtector())
    prepared = store.prepare([credential("a1", "KEY-123456", "SECRET-123456")])
    store.commit(prepared, expected_revision=store.current_revision())
    raw = path.read_text(encoding="utf-8")
    assert "SECRET-123456" not in raw
    assert store.load()[0].account_id == "a1"


def test_mask_key_does_not_reveal_full_value() -> None:
    masked = mask_api_key("ABCDEFGH12345678")
    assert masked == "ABCD…5678"


def test_corrupt_ciphertext_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "accounts.secure.json"
    path.write_text('{"version":1,"ciphertext":"broken"}', encoding="utf-8")
    with pytest.raises(CredentialStoreUnavailable):
        AccountCredentialStore(path, FakeProtector()).load()


def test_missing_secure_file_is_unconfigured_not_corrupt(tmp_path: Path) -> None:
    store = AccountCredentialStore(tmp_path / "accounts.secure.json", FakeProtector())
    assert store.load() == []
    assert store.state() is CredentialStoreState.UNCONFIGURED


def test_commit_rejects_stale_revision_without_changing_file(tmp_path: Path) -> None:
    store = seeded_store(tmp_path)
    stale_revision = store.current_revision()
    mutate_store_from_another_request(store)
    before = store.path.read_bytes()
    prepared = store.prepare([credential("a1", "NEW-KEY-123", "NEW-SECRET-123")])
    with pytest.raises(CredentialRevisionConflict):
        store.commit(prepared, expected_revision=stale_revision)
    assert store.path.read_bytes() == before
```

- [x] **Step 2: 运行红测**

Run: `python -m pytest tests/test_account_credentials.py -q`

Expected: FAIL，`paired_opener.account_credentials` 不存在。

- [x] **Step 3: 实现 protector 边界与 DPAPI**

```python
class SecretProtector(Protocol):
    def protect(self, value: bytes) -> bytes: ...
    def unprotect(self, value: bytes) -> bytes: ...


class WindowsDpapiProtector:
    def protect(self, value: bytes) -> bytes:
        return _crypt_protect_data(value, description="Binance Paired Opener accounts")

    def unprotect(self, value: bytes) -> bytes:
        return _crypt_unprotect_data(value)
```

DPAPI 调用使用 `ctypes.windll.crypt32.CryptProtectData`/`CryptUnprotectData`，作用域为 CurrentUser，不使用 `CRYPTPROTECT_LOCAL_MACHINE`。安全文件不存在是可区分的 `UNCONFIGURED` 状态并返回空列表；文件一旦存在，DPAPI 不可用、解密失败或密文版本未知时抛出 `CredentialStoreUnavailable`，不得回退为明文、空账号列表或环境变量 Secret。

- [x] **Step 4: 实现密文文件和原子替换**

```python
def _write_records(self, records: list[dict[str, object]]) -> None:
    plaintext = json.dumps({"version": 1, "accounts": records}, ensure_ascii=False).encode("utf-8")
    encrypted = self._protector.protect(plaintext)
    payload = json.dumps(
        {"version": 1, "encoding": "dpapi-current-user", "ciphertext": base64.b64encode(encrypted).decode("ascii")},
        ensure_ascii=True,
        indent=2,
    )
    temp_path = self._path.with_suffix(self._path.suffix + ".tmp")
    temp_path.write_text(payload, encoding="utf-8")
    os.replace(temp_path, self._path)
```

`prepare(records)` 只完成校验、生成新的随机 `credential_revision`、序列化、加密并写入受限权限的临时文件，返回 `PreparedCredentialWrite`；`commit(prepared, expected_revision=...)` 在同一进程锁内比较当前 revision 后才执行原子替换。外层可保存不敏感的 revision，但密文载荷也必须包含同值并在读取时核对，防止文件被拼接。创建文件后将 Windows ACL 收紧为当前用户和 `SYSTEM` 可读写；ACL 无法确认时删除临时文件并失败。这样 Task 3 可以先构建候选 runtime，再一次性切换密文文件和内存状态。

- [x] **Step 5: 加入配置与忽略规则**

```python
binance_accounts_secure_file: Path = CONFIG_DIR / "binance_accounts.secure.json"
```

`.gitignore` 添加：

```text
config/binance_accounts.secure.json
config/binance_accounts.secure.json.tmp
```

- [x] **Step 6: 回跑凭据测试**

Run: `python -m pytest tests/test_account_credentials.py tests/test_accounts.py -q`

Expected: PASS，且测试目录中没有明文 Secret；DPAPI/密文/ACL 异常均 fail closed，失败的 `prepare()` 或 revision 冲突不改变旧文件。

### Task 3: 接入账号管理 API 与运行时热更新

**Files:**
- Create: `paired_opener/single_instance.py`
- Modify: `paired_opener/account_runtime.py`
- Modify: `paired_opener/api.py`
- Modify: `paired_opener/main.py`
- Modify: `scripts/run_service.py`
- Modify: `paired_opener/schemas.py`
- Modify: `app_i18n/runtime.py`
- Modify: `i18n/messages/zh-CN.json`
- Test: `tests/test_account_credentials_api.py`
- Test: `tests/test_accounts.py`
- Test: `tests/test_single_instance.py`

**Interfaces:**
- Consumes: `AccountCredentialStore`。
- Produces: `SingleInstanceGuard.acquire(data_dir)`、`AccountRuntimeManager.prepare_accounts()`、同步 `commit_accounts()`、`close_replaced()`、`aclose()`、`AccountCredentialCommitCoordinator.commit()`，以及 `AccountMutationGuard`。
- Produces: Task 1 定义的凭据管理 endpoints。

- [x] **Step 1: 写敏感字段和活动批次保护红测**

```python
def test_account_list_masks_key_and_never_returns_secret(client) -> None:
    response = client.get("/config/account-credentials")
    assert response.status_code == 200
    text = response.text
    assert "SECRET-123456" not in text
    assert response.json()["accounts"][0]["api_key_masked"] == "KEY-…3456"


def test_reorder_rejected_when_mutation_guard_is_locked(client, locked_mutation_guard) -> None:
    response = client.put("/config/account-credentials/order", json={"account_ids": ["a2", "a1"]})
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "account_credentials_locked_by_active_batch"


def test_second_instance_for_same_data_directory_fails_closed(tmp_path: Path) -> None:
    first = SingleInstanceGuard.acquire(tmp_path)
    try:
        with pytest.raises(ServiceInstanceAlreadyRunning):
            SingleInstanceGuard.acquire(tmp_path)
    finally:
        first.close()


@pytest.mark.asyncio
async def test_runtime_swap_closes_replaced_runtime_before_return(runtime_manager) -> None:
    old = runtime_manager.current("a1")
    prepared = await runtime_manager.prepare_accounts([replacement_account("a1")])
    replaced = runtime_manager.commit_accounts(prepared)
    await runtime_manager.close_replaced(replaced)
    assert old.is_closed is True


@pytest.mark.asyncio
async def test_runtime_manager_shutdown_closes_current_runtime(runtime_manager) -> None:
    current = runtime_manager.current("a1")
    await runtime_manager.aclose()
    assert current.is_closed is True


def test_service_starts_in_setup_mode_without_any_account(app_factory) -> None:
    app = app_factory(secure_accounts=None, legacy_accounts=[])
    with TestClient(app) as client:
        assert client.get("/").status_code == 200
        response = client.post("/kanglong/batch-simulation/capacity-preview", json=minimal_preview())
        assert response.status_code == 503
        assert response.json()["detail"]["code"] == "account_credentials_not_configured"


def test_import_body_limit_is_enforced_before_json_decode(client, local_management_headers) -> None:
    response = client.post(
        "/config/account-credentials/import/preview",
        content=b"{" + b"x" * (256 * 1024) + b"}",
        headers={**local_management_headers, "Content-Type": "application/json"},
    )
    assert response.status_code == 413


def test_bootstrap_token_rotates_and_is_never_persisted(app_factory) -> None:
    first = bootstrap_token_from_html(app_factory())
    second = bootstrap_token_from_html(app_factory())
    assert first != second
    assert "Set-Cookie" not in bootstrap_response(app_factory()).headers
```

- [x] **Step 2: 运行红测**

Run: `python -m pytest tests/test_account_credentials_api.py tests/test_single_instance.py -q`

Expected: FAIL，路由或单实例保护不存在。

- [x] **Step 3: 实现 runtime 原子替换**

```python
class AccountMutationGuard(Protocol):
    def ensure_mutation_allowed(self) -> None: ...


def current(self, account_id: str) -> AccountRuntime:
    runtime = self._runtimes.get(account_id)
    if runtime is None:
        raise AccountCredentialsNotConfigured(account_id)
    return runtime


async def prepare_accounts(self, accounts: list[AccountConfig]) -> PreparedRuntimeSet:
    candidates: dict[str, AccountRuntime] = {}
    try:
        for account in accounts:
            runtime = self._runtime_factory.create(account)
            await runtime.verify_read_only_access()
            candidates[account.account_id] = runtime
    except Exception:
        await asyncio.gather(*(item.aclose() for item in candidates.values()), return_exceptions=True)
        raise
    return PreparedRuntimeSet(accounts=tuple(accounts), runtimes=MappingProxyType(candidates))


def commit_accounts(self, prepared: PreparedRuntimeSet) -> tuple[AccountRuntime, ...]:
    previous = self._runtimes
    self._runtimes = dict(prepared.runtimes)
    return tuple(
        runtime
        for account_id, runtime in previous.items()
        if self._runtimes.get(account_id) is not runtime
    )


async def close_replaced(self, replaced: tuple[AccountRuntime, ...]) -> None:
    results = await asyncio.gather(*(runtime.aclose() for runtime in replaced), return_exceptions=True)
    for result in results:
        if isinstance(result, Exception):
            self._logger.warning("account_runtime_close_failed", extra={"error_type": type(result).__name__})


async def aclose(self) -> None:
    async with self._mutation_lock:
        current = tuple(self._runtimes.values())
        self._runtimes = {}
    await self.close_replaced(current)


class AccountCredentialCommitCoordinator:
    async def commit(
        self,
        *,
        candidate: VerifiedCredentialCandidate,
        prepared_write: PreparedCredentialWrite,
        prepared_runtime: PreparedRuntimeSet,
    ) -> None:
        try:
            async with self._mutation_lock:
                self._mutation_guard.ensure_mutation_allowed()
                self._store.commit(
                    prepared_write,
                    expected_revision=candidate.credential_revision,
                )
                replaced = self._runtime_manager.commit_accounts(prepared_runtime)
        except BaseException:
            prepared_write.discard()
            await self._await_cleanup(tuple(prepared_runtime.runtimes.values()))
            raise
        await self._await_cleanup(replaced)

    async def _await_cleanup(self, runtimes: tuple[AccountRuntime, ...]) -> None:
        cleanup = asyncio.create_task(self._runtime_manager.close_replaced(runtimes))
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await cleanup
            raise
```

`commit_accounts()` 是只做内存指针替换的同步提交原语，调用方必须持有 `AccountCredentialCommitCoordinator` 的变更锁；它不能执行网络请求或 `await`。`AccountMutationGuard` 在本任务先接入现有 session/移仓活动状态；Task 6 注册批次活动查询。候选 runtime 在锁外验证，真正提交时在锁内重新执行 guard 与 revision 校验，再连续调用安全文件 `commit()` 和 runtime `commit_accounts()`；二者之间没有取消点。`close_replaced()` 使用 `return_exceptions=True` 且只记录脱敏错误；提交完成后由 coordinator 使用 `asyncio.shield()` 等待关闭，取消请求不能跳过清理。`lifespan()` 的 shutdown 必须 `await runtime_manager.aclose()`，不得留下裸 `create_task()`。

- [x] **Step 4: 强制同一数据目录单实例运行**

`SingleInstanceGuard.acquire(data_dir)` 在 `data_dir/.paired-opener.instance.lock` 上取得由操作系统持有的独占文件锁并保留打开句柄；锁内容不作为所有权依据，进程崩溃或句柄关闭后由操作系统释放，获取失败抛出不含敏感信息的 `ServiceInstanceAlreadyRunning`。`lifespan()` 必须在解密凭据、打开 SQLite 和构建 runtime 前获取锁，并在完整 shutdown 后释放。`scripts/run_service.py` 显式使用 `workers=1`，检测到大于 1 的 worker 配置时拒绝启动；不得通过删除锁文件绕过仍由其他进程持有的锁，也不引入 Redis 或常驻协调服务。

- [x] **Step 5: 实现路由且统一清理错误**

```python
@app.post("/config/account-credentials/import/commit")
async def commit_account_credentials(request: AccountCredentialImportCommitRequest, raw: Request):
    require_local_management_request(raw)
    candidate = app.state.credential_imports.consume_verified_preview(request.preview_token)
    prepared_runtime = await app.state.runtime_manager.prepare_accounts(candidate.accounts)
    prepared_write = app.state.account_credential_store.prepare(candidate.accounts)
    await app.state.account_credential_commit_coordinator.commit(
        candidate=candidate,
        prepared_write=prepared_write,
        prepared_runtime=prepared_runtime,
    )
    return masked_credential_list()
```

导入预览在内存中保存默认 5 分钟、一次性的 token、基准 `credential_revision` 与候选内容哈希，不写密文文件、不替换 runtime；commit 采用 `merge` 或显式 `replace` 的候选全集。`AccountCredentialCommitCoordinator.commit()` 取得唯一账号变更锁后重新调用 `AccountMutationGuard.ensure_mutation_allowed()`、比较 revision，然后在无 `await` 的临界区内先原子替换安全文件、再同步替换 runtime 指针；进程在文件替换后异常退出时，重启从新安全文件重建 runtime，当前请求取消则由 shield 保证临界区和旧 runtime 清理完成。revision 或活动状态冲突返回 409 并销毁候选 runtime/临时文件。手工新增、编辑、删除和改序复用同一个 coordinator，不能各自形成不同事务边界。异常处理必须将 `api_key`、`api_secret`、签名参数和请求正文替换为 `[REDACTED]`。

`require_local_management_request()` 必须同时验证 `request.client.host` 为 loopback、`Host` 为 loopback 地址、`Origin` 属于服务自身 origin，并校验 `X-Local-Management-Token`；缺失或不匹配均返回 403。token 在每次服务启动时由 `secrets.token_urlsafe(32)` 生成，通过带 `Cache-Control: no-store` 的首页 bootstrap 注入，前端仅保存在内存。凭据 preview 路由使用专用 body-limit middleware 在解析前按实际读取字节数拒绝超过 `256 KiB` 的请求，不能只信任 `Content-Length`。安全文件不存在时允许以空 runtime 集合启动；现有业务需要 current runtime 时统一返回 `503 account_credentials_not_configured`。若检测到旧账号配置则保持现有功能可用并显示迁移提示，但安全文件一旦存在，密文损坏必须停止启动且不得回退旧配置。

```python
class CredentialImportBodyLimitMiddleware:
    LIMIT = 256 * 1024
    PATH = "/config/account-credentials/import/preview"

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope.get("path") != self.PATH:
            await self.app(scope, receive, send)
            return
        total = 0

        async def limited_receive():
            nonlocal total
            message = await receive()
            if message["type"] == "http.request":
                total += len(message.get("body", b""))
                if total > self.LIMIT:
                    raise RequestBodyTooLarge
            return message

        try:
            await self.app(scope, limited_receive, send)
        except RequestBodyTooLarge:
            response = JSONResponse(
                status_code=413,
                content={"detail": {"code": "credential_import_too_large"}},
            )
            await response(scope, receive, send)


def render_index_with_local_token(document: str, token: str) -> HTMLResponse:
    bootstrap = json.dumps({"localManagementToken": token}, ensure_ascii=False)
    rendered = document.replace("__LOCAL_MANAGEMENT_BOOTSTRAP__", html.escape(bootstrap, quote=True))
    return HTMLResponse(rendered, headers={"Cache-Control": "no-store, max-age=0"})
```

- [x] **Step 6: 回跑账号测试**

Run: `python -m pytest tests/test_account_credentials_api.py tests/test_accounts.py tests/test_single_instance.py tests/test_i18n_contracts.py -q`

Expected: PASS；另需覆盖预览不落盘、解析前 body 限制、重复 ID 整批失败、HMAC-only、`merge`/`replace`、无账号设置模式、旧配置迁移提示、候选 runtime 构建失败保持旧文件和旧 runtime、准备后出现活动任务会在提交锁内被拒绝、请求取消不产生文件/runtime 分裂、两个页面基于同一 revision 时只有先提交者成功、非法 `Host`/`Origin`/token 返回 403、bootstrap token 不进 Cookie/URL/持久化存储且重启轮换、同目录第二实例拒绝启动且首实例异常退出后锁可重新取得、runtime 切换返回前旧连接已关闭且 shutdown 会关闭当前 runtime。

### Task 4: 补齐 Portfolio Margin 只读预检和费率读取

**Files:**
- Modify: `paired_opener/exchange.py`
- Modify: `paired_opener/binance.py`
- Modify: `paired_opener/classified_gateway.py`
- Test: `tests/test_binance_gateway.py`
- Test: `tests/test_classified_gateway.py`

**Interfaces:**
- Produces: `RateLimitObservation` 与可选 `rate_limit_observer` hook；不改变业务响应结构。
- Produces: `get_portfolio_margin_precheck(symbol: str, requested_leverage: int, additional_gross_notional: Decimal) -> dict`，返回完整档位、归一化后的有效 floor/cap、`notionalCoef`、当前杠杆和 `symbolConfig.maxNotionalValue`。
- Produces: `get_commission_rates(symbol: str) -> dict[str, Decimal]`。
- Produces: `get_portfolio_margin_open_orders(symbol: str) -> list[dict]`。
- Produces: `get_order_book(symbol, limit=20)` 保证返回请求深度或显式失败。

- [x] **Step 1: 写 PAPI 路径和只读保证红测**

```python
@pytest.mark.asyncio
async def test_portfolio_margin_precheck_uses_only_read_endpoints(gateway, recorder) -> None:
    result = await gateway.get_portfolio_margin_precheck("ETHUSDC", 100, Decimal("500000"))
    assert result["hedge_mode"] is True
    assert result["projected_symbol_exposure"] == Decimal("500000")
    assert result["selected_bracket"]["max_allowed_leverage"] >= 100
    assert result["current_symbol_max_notional_value"] > Decimal("0")
    assert all(call.method == "GET" for call in recorder.calls)
    assert not any("/order" in call.path or call.path.endswith("/leverage") for call in recorder.calls)


@pytest.mark.asyncio
async def test_commission_rates_are_account_specific(gateway) -> None:
    rates = await gateway.get_commission_rates("ETHUSDC")
    assert rates == {"maker": Decimal("0.0002"), "taker": Decimal("0")}


@pytest.mark.asyncio
async def test_gateway_reports_weight_and_retry_after(gateway, recorder, observations) -> None:
    recorder.respond_with_headers(
        status=429,
        headers={"X-MBX-USED-WEIGHT-1M": "5900", "Retry-After": "2"},
    )
    with pytest.raises(TradingError):
        await gateway.get_portfolio_margin_precheck("ETHUSDC", 100, Decimal("500000"))
    assert observations[-1].used_weight_by_window["1m"] == 5900
    assert observations[-1].retry_after_seconds == Decimal("2")
```

- [x] **Step 2: 运行红测**

Run: `python -m pytest tests/test_binance_gateway.py -q -k "portfolio_margin or commission or depth"`

Expected: FAIL，方法不存在或仍使用 `/fapi` 私有端点。

- [x] **Step 3: 实现只读 PAPI 聚合**

```python
async def get_commission_rates(self, symbol: str) -> dict[str, Decimal]:
    payload = await self._signed_request(
        "GET", "/papi/v1/um/commissionRate", {"symbol": symbol.upper()}, use_papi=True
    )
    return {
        "maker": Decimal(str(payload["makerCommissionRate"])),
        "taker": Decimal(str(payload["takerCommissionRate"])),
    }
```

预检同时读取 `/papi/v1/account`、`/papi/v1/um/positionSide/dual`、`/papi/v1/um/symbolConfig`、`/papi/v1/um/leverageBracket`、当前仓位、当前挂单和 commissionRate；`accountStatus != NORMAL` 或 `dualSidePosition != true` 时返回结构化阻断原因。网关保留完整 bracket 列表与原始 `notionalCoef`，缺失时使用 `Decimal("1")`，并在适配层一次性计算 `effective_floor = notionalFloor * notionalCoef`、`effective_cap = notionalCap * notionalCoef`；随后用 `existing_symbol_exposure + additional_gross_notional` 在有效区间中选择投影档位。原始值、coefficient 和有效值同时进入快照 hash。所有方法只能发送 GET，并在模型层以 `Decimal` 保留原始精度。

`BinanceGateway` 在每个响应后将 `X-MBX-USED-WEIGHT-*`、HTTP 状态和可选 `Retry-After` 解析为 `RateLimitObservation`，发送给构造时注入的 observer；未注入时保持现有行为。`ClassifiedExchangeGateway` 将 `retry_after_seconds` 保留在结构化错误 context 中。observer 只接收限流元数据，不接收请求参数、签名或凭据。

- [x] **Step 4: 合入参考分支的 20 档深度保护**

```python
ORDER_BOOK_STREAM_DEPTH = 20
required_levels = min(max(int(limit), 1), ORDER_BOOK_STREAM_DEPTH)
if cached is None or min(len(cached.get("bids", [])), len(cached.get("asks", []))) < required_levels:
    return await self.refresh_order_book(normalized_symbol, limit=limit)
```

- [x] **Step 5: 回跑网关测试**

Run: `python -m pytest tests/test_binance_gateway.py tests/test_classified_gateway.py -q`

Expected: PASS。

### Task 5: 实现默认参数和容量预览

**Files:**
- Create: `paired_opener/kanglong/batch_settings.py`
- Create: `paired_opener/kanglong/batch_capacity.py`
- Modify: `paired_opener/config.py`
- Modify: `paired_opener/account_runtime.py`
- Modify: `paired_opener/schemas.py`
- Modify: `paired_opener/api.py`
- Modify: `docs/API_CONTRACT.md`
- Modify: `docs/openapi/kanglong-batch-simulation.yaml`
- Test: `tests/test_kanglong_batch_settings.py`
- Test: `tests/test_kanglong_batch_capacity.py`
- Test: `tests/test_kanglong_batch_api.py`

**Interfaces:**
- Consumes: Task 4 的 PAPI 账户、费率、杠杆档位、仓位和挂单数据。
- Produces: `KanglongBatchDefaultsStore`、`CapacitySnapshotCoordinator`、`estimate_batch_capacity()`。
- Produces endpoints: `GET/PUT /config/kanglong-batch-defaults`、`POST /kanglong/batch-simulation/capacity-preview`。

- [x] **Step 1: 先补接口契约**

```text
GET  /config/kanglong-batch-defaults
PUT  /config/kanglong-batch-defaults
POST /kanglong/batch-simulation/capacity-preview
```

`capacity-preview` 仅接受 `operation=open`。响应必须包含 `requested_gross_notional`、`capacity_requested_gross_notional`、`existing_symbol_exposure`、`projected_symbol_exposure`、`conservative_openable_notional`、`estimated_capacity_usage_percent`、`limiting_factor`、`requested_leverage`、`current_symbol_leverage`、`bracket_max_allowed_leverage`、`bracket_notional_coef`、`selected_bracket_effective_cap`、`current_symbol_max_notional_value`、`effective_capacity_leverage`、`batch_requested_gross_notional`、`batch_conservative_openable_notional`、`batch_estimated_usage_percent`、`bottleneck_account_id`、`assembled_at`、`oldest_component_at`、`snapshot_components`、`calculation_version` 和各限制项分量。`snapshot_components` 至少包含 `account`、`positions`、`open_orders`、`symbol_config`、`leverage_bracket`、`commission_rate`、`quote` 和 `order_book`，每项都有 `observed_at`、`source=cache|upstream`、`age_ms`、`ttl_ms`；不能用一个顶层时间或来源掩盖混合 TTL 数据。任一必需分量缺失或 `age_ms > ttl_ms` 时返回容量未知并阻止确认。字段命名与页面文案都必须明确它是当前快照下的保守估算，不是交易所保证额度。平仓来源剩余量由 Task 6 的 close plan 响应返回，不经过此 endpoint。

- [x] **Step 2: 写默认值持久化和容量公式红测**

```python
def test_defaults_round_trip_utf8(tmp_path: Path) -> None:
    store = KanglongBatchDefaultsStore(tmp_path / "defaults.json")
    store.save(KanglongBatchDefaults(symbol="ETHUSDC", leverage=100, per_leg_notional=Decimal("250000")))
    assert store.load().leverage == 100


def test_capacity_uses_gross_two_leg_notional() -> None:
    result = estimate_account_capacity(
        per_leg_notional=Decimal("250000"),
        requested_leverage=100,
        current_symbol_leverage=100,
        current_symbol_max_notional_value=Decimal("1000000"),
        brackets=effective_brackets(("0", "1000000", 125)),
        available_balance=Decimal("10000"),
        equity=Decimal("10000"),
        maker_fee_rate=Decimal("0"),
        taker_fee_rate=Decimal("0"),
        existing_symbol_exposure=Decimal("0"),
        policy=capacity_policy(),
    )
    assert result.requested_gross_notional == Decimal("500000")
    assert result.estimated_capacity_usage_percent > Decimal("0")


def test_existing_exposure_reduces_bracket_capacity() -> None:
    result = estimate_account_capacity(
        per_leg_notional=Decimal("250000"),
        requested_leverage=100,
        current_symbol_leverage=100,
        current_symbol_max_notional_value=Decimal("600000"),
        brackets=effective_brackets(("0", "600000", 125)),
        available_balance=Decimal("100000"),
        equity=Decimal("100000"),
        maker_fee_rate=Decimal("0"),
        taker_fee_rate=Decimal("0"),
        existing_symbol_exposure=Decimal("200000"),
        policy=capacity_policy(),
    )
    assert result.bracket_remaining_notional == Decimal("400000")
    assert result.estimated_capacity_usage_percent == Decimal("125")
    assert result.blocked is True


def test_projected_two_leg_exposure_selects_the_final_bracket() -> None:
    result = estimate_account_capacity(
        per_leg_notional=Decimal("250000"),
        requested_leverage=100,
        current_symbol_leverage=100,
        current_symbol_max_notional_value=Decimal("800000"),
        brackets=effective_brackets(("0", "400000", 125), ("400000", "800000", 75)),
        existing_symbol_exposure=Decimal("0"),
        **capacity_inputs(),
    )
    assert result.projected_symbol_exposure == Decimal("500000")
    assert result.bracket_max_allowed_leverage == 75
    assert result.blocked_reason == "requested_leverage_exceeds_projected_bracket"


def test_symbol_config_max_notional_is_a_capacity_limit() -> None:
    result = estimate_account_capacity(
        per_leg_notional=Decimal("250000"),
        current_symbol_max_notional_value=Decimal("450000"),
        existing_symbol_exposure=Decimal("0"),
        **capacity_inputs(),
    )
    assert result.symbol_config_remaining_notional == Decimal("450000")
    assert result.blocked is True


def test_capacity_uses_current_leverage_when_requested_leverage_is_not_active() -> None:
    result = estimate_account_capacity(
        per_leg_notional=Decimal("250000"),
        requested_leverage=100,
        current_symbol_leverage=20,
        current_symbol_max_notional_value=Decimal("1000000"),
        brackets=effective_brackets(("0", "1000000", 125)),
        available_balance=Decimal("10000"),
        equity=Decimal("10000"),
        maker_fee_rate=Decimal("0"),
        taker_fee_rate=Decimal("0"),
        existing_symbol_exposure=Decimal("0"),
        policy=capacity_policy(),
    )
    assert result.requested_leverage == 100
    assert result.current_symbol_leverage == 20
    assert result.effective_capacity_leverage == 20
    assert result.margin_capacity_notional < Decimal("1000000")


@pytest.mark.asyncio
async def test_concurrent_preview_requests_share_one_upstream_snapshot(coordinator, gateway) -> None:
    await asyncio.gather(*[
        coordinator.get_snapshot("revision-1", "a1", "ETHUSDC")
        for _ in range(20)
    ])
    assert gateway.snapshot_call_count("a1", "ETHUSDC") == 1


@pytest.mark.asyncio
async def test_force_refresh_bypasses_unexpired_cache(coordinator, gateway) -> None:
    await coordinator.get_snapshot("revision-1", "a1", "ETHUSDC")
    await coordinator.get_snapshot("revision-1", "a1", "ETHUSDC", force_refresh=True)
    assert gateway.snapshot_call_count("a1", "ETHUSDC") == 2


@pytest.mark.asyncio
async def test_new_credential_revision_never_reuses_old_account_snapshot(coordinator, gateway) -> None:
    first = await coordinator.get_snapshot("revision-1", "a1", "ETHUSDC")
    gateway.switch_account_payload("a1", account_equity="20000")
    second = await coordinator.get_snapshot("revision-2", "a1", "ETHUSDC")
    assert first.account_equity != second.account_equity
    assert gateway.snapshot_call_count("a1", "ETHUSDC") == 2


@pytest.mark.asyncio
async def test_public_market_snapshot_is_shared_across_accounts(coordinator, gateway) -> None:
    await asyncio.gather(
        coordinator.get_snapshot("revision-1", "a1", "ETHUSDC", force_refresh=True),
        coordinator.get_snapshot("revision-1", "a2", "ETHUSDC", force_refresh=True),
        coordinator.get_snapshot("revision-1", "a3", "ETHUSDC", force_refresh=True),
    )
    assert gateway.public_quote_call_count("ETHUSDC") == 1
    assert gateway.public_order_book_call_count("ETHUSDC", depth=20) == 1
    assert gateway.private_snapshot_call_count() == 3
```

- [x] **Step 3: 运行红测**

Run: `python -m pytest tests/test_kanglong_batch_settings.py tests/test_kanglong_batch_capacity.py tests/test_kanglong_batch_api.py -q`

Expected: FAIL，设置和容量模块不存在。

- [x] **Step 4: 实现默认设置模型和原子 UTF-8 文件**

```python
class KanglongBatchDefaults(BaseModel):
    symbol: str = "ETHUSDC"
    preferred_side: PositionSide = PositionSide.LONG
    leverage: int = Field(default=100, ge=1, le=125)
    per_leg_notional: Decimal = Field(default=Decimal("250000"), gt=0)
    round_count: int = Field(default=30, ge=1, le=500)
    round_interval_seconds: int = Field(default=3, ge=0, le=3600)
```

保存路径为 `config/kanglong_batch_defaults.json`；写入临时文件后使用 `os.replace()` 原子替换。

- [x] **Step 5: 实现容量公式**

```python
requested_gross = per_leg_notional * Decimal("2")
capacity_requested_gross = capacity_requested_gross_notional or requested_gross
projected_symbol_exposure = existing_symbol_exposure + capacity_requested_gross
selected_bracket = select_effective_bracket(brackets, projected_symbol_exposure)
effective_capacity_leverage = min(
    int(requested_leverage),
    int(current_symbol_leverage),
    int(selected_bracket.max_allowed_leverage),
)
available_after_safety = max(available_balance * (Decimal("1") - policy.margin_safety_ratio), Decimal("0"))
cost_rate = (
    Decimal("1") / Decimal(effective_capacity_leverage)
    + max(maker_fee_rate, taker_fee_rate, Decimal("0"))
    + Decimal(policy.price_buffer_bps) / Decimal("10000")
)
margin_capacity = available_after_safety / cost_rate if cost_rate > 0 else Decimal("0")
equity_capacity = equity * Decimal(effective_capacity_leverage) * policy.max_notional_ratio
liquidation_capacity = equity * Decimal(effective_capacity_leverage) * (Decimal("1") - policy.min_liquidation_buffer_ratio)
bracket_remaining = max(selected_bracket.effective_notional_cap - existing_symbol_exposure, Decimal("0"))
symbol_config_remaining = max(current_symbol_max_notional_value - existing_symbol_exposure, Decimal("0"))
conservative_openable = min(
    margin_capacity,
    equity_capacity,
    liquidation_capacity,
    bracket_remaining,
    symbol_config_remaining,
)
estimated_usage = (
    capacity_requested_gross / conservative_openable * Decimal("100")
    if conservative_openable > 0
    else None
)
```

`select_effective_bracket()` 只消费 Task 4 已归一化的有效 floor/cap，不在容量层再次乘 `notionalCoef`；普通档位使用 `effective_floor <= projected < effective_cap`，最后一档允许等于 cap。若投影敞口超过最终有效 cap，容量按最终 cap 计算并直接 block。请求杠杆高于投影档位最大杠杆时返回 `requested_leverage_exceeds_projected_bracket`，不能仅通过把有效杠杆下调后继续。批次百分比必须使用 `sum(capacity_requested_gross) / sum(conservative_openable) * 100`，并单独返回最大账号百分比；不得对百分比做算术平均。计算和阻断判断全程使用未舍入的 `Decimal`，只在响应展示字段中格式化。当前 symbol 杠杆低于请求杠杆时返回结构化 warning，但占用百分比始终使用 `effective_capacity_leverage`；可选 hypothetical 展示值不得参与阻断或批次汇总。`assembled_at` 是本次响应组装时间，`oldest_component_at` 是必需分量中最早的 `observed_at`；每个分量独立保存采集时间、来源、TTL 和限制值。

- [x] **Step 6: 实现短时快照协调与旧响应隔离**

请求包含 `request_seq` 和 `input_hash`，响应原样返回；前端只接受与当前输入一致的响应。任一账号 `estimated_capacity_usage_percent > 100` 返回 block，`80 <= estimated_capacity_usage_percent <= 100` 返回 warning。后端按凭据仓库中的 canonical order 规范化所选 `account_ids`，不得依赖客户端提交顺序。

`CapacitySnapshotCoordinator` 保持为 `batch_capacity.py` 内的进程级组件，不建表、不新增服务。账号私有组件使用 `(credential_revision, account_id, symbol, component)` key，公开 exchange info/quote/order book 使用 `(symbol, component, depth)` key；两类 key 各自合并 in-flight request。账户/仓位/挂单/quote/order book 快照默认 TTL 为 3 秒，symbol config/杠杆档位/佣金率默认 TTL 为 60 秒，TTL 从服务端配置读取。账号 ID 不变但 API Key/API Secret 更新后必须传入新的 revision，旧 revision 私有缓存不再可达并按 TTL/容量上限淘汰。账号上游请求使用容量为 4 的共享 semaphore；它作为 Task 4 的共享 `rate_limit_observer` 注入所有账号 runtime，根据 observation 维护同一出口 IP 的调度预算，`429/418` 按 `Retry-After` 暂停共享调度器。普通 preview 允许缓存；plan 确认和账号开腿前的 `force_refresh=True` 绕过已完成缓存，但同一次批量刷新仍合并公共行情和相同 key 的并发读取。缓存只保存只读响应和采集时间，不保存凭据。

容量预览只是交互反馈。本任务提供可复用的 `refresh_capacity(credential_revision, account_ids, force_refresh=False)`；Task 6 确认 plan 时必须以计划冻结的 revision 和 `force_refresh=True` 刷新所有所选账号的只读快照，Task 7 串行执行轮到每个账号且尚未模拟第一腿前必须再次核对当前 revision、强制刷新并重新做 block/warning 判断，避免凭据更新或后续账号沿用陈旧容量。

- [x] **Step 7: 回跑设置和容量测试**

Run: `python -m pytest tests/test_kanglong_batch_settings.py tests/test_kanglong_batch_capacity.py tests/test_kanglong_batch_api.py -q`

Expected: PASS；测试必须断言 `assembled_at`、`oldest_component_at` 及每个必需分量的来源/年龄/TTL/限制值存在，任一分量过期都会阻断；还需覆盖后端账号顺序规范化、双腿总额与已有敞口选择投影档位、`notionalCoef` 只归一化一次、`symbolConfig.maxNotionalValue` 成为限制项、当前 20X/请求 100X 时有效杠杆为 20X、计算不因展示舍入改变阻断结果、同 revision 普通 preview 合并私有读取、新 revision 不命中旧账号快照、强制刷新不复用旧响应、100 账号同 symbol 时公共 quote/order book 各只读取一次、私有上游并发不超过 4，并遵循 `Retry-After`。

### Task 6: 建立批次计划、持久化和开平仓所有权

**Files:**
- Create: `paired_opener/kanglong/batch_models.py`
- Create: `paired_opener/kanglong/batch_planner.py`
- Modify: `paired_opener/storage.py`
- Modify: `docs/API_CONTRACT.md`
- Modify: `docs/openapi/kanglong-batch-simulation.yaml`
- Test: `tests/test_kanglong_batch_planner.py`
- Test: `tests/test_kanglong_batch_storage.py`

**Interfaces:**
- Consumes: Task 4 的预检、规则、报价和费率快照，以及 Task 5 的 `refresh_capacity(credential_revision, account_ids, force_refresh=False)`。
- Consumes: 现有 run、ledger、event、checkpoint、lease/fencing 和 reporter 存储接口。
- Produces: `KanglongBatchPlan`、`KanglongBatchAccountPlan`、`KanglongBatchPlanner.refresh_pending_suffix(stored_plan, account_statuses, refreshed_accounts, credential_revision)`、`refresh_close_availability(source_open_run_id, account_ids, force_refresh=False)`，以及现有 repository 上的轻量批次账号队列方法。
- Produces schema: 现有 `kanglong_runs` 增加 `run_kind` 判别列（已有记录默认为 `transfer`），并新增 `kanglong_batch_accounts`；不新增平行 ledger/event/lock/report 表。

close plan 响应按账号返回 `source_long_remaining_qty`、`source_short_remaining_qty`、`target_long_qty`、`target_short_qty`、`source_ledger_hash` 和 `source_checkpoint_id`；目标分别以各腿来源剩余量为上限，不能先取两腿最小值后丢失较大腿，也不能用客户端的 `per_leg_notional` 扩大。页面上的平仓预览直接消费这些字段。

- [x] **Step 1: 写数量、顺序和平仓所有权红测**

```python
def test_open_plan_preserves_account_order_and_targets_each_leg() -> None:
    plan = planner.plan_open(
        account_ids=["a3", "a1", "a2"],
        credential_revision="revision-1",
        symbol="ETHUSDC",
        preferred_side=PositionSide.LONG,
        leverage=100,
        per_leg_notional=Decimal("250000"),
        reference_price=Decimal("2000"),
        rules=rules(step_size="0.001"),
    )
    assert [item.account_id for item in plan.accounts] == ["a3", "a1", "a2"]
    assert all(item.target_long_qty == Decimal("125") for item in plan.accounts)
    assert all(item.target_short_qty == Decimal("125") for item in plan.accounts)


def test_close_plan_cannot_exceed_source_batch_remaining_qty() -> None:
    plan = planner.plan_close(source_open_run=source_run_with_remaining("12.345"))
    assert plan.accounts[0].target_long_qty == Decimal("12.345")
    assert plan.accounts[0].target_short_qty == Decimal("12.345")


def test_close_plan_preserves_unequal_source_dust_without_expanding_either_leg() -> None:
    plan = planner.plan_close(
        source_open_run=source_run_with_leg_remaining(long_qty="12.345", short_qty="12.3445")
    )
    assert plan.accounts[0].target_long_qty == Decimal("12.345")
    assert plan.accounts[0].target_short_qty == Decimal("12.3445")


def test_close_plan_is_not_blocked_by_open_capacity(short_of_open_capacity_snapshot) -> None:
    result = planner.plan_close(
        source_open_run=source_run_with_remaining("12.345"),
        account_snapshot=short_of_open_capacity_snapshot,
    )
    assert result.blocked is False
    assert result.open_capacity_check_applied is False


def test_only_one_close_run_can_lock_the_same_source(repository) -> None:
    first_conflict = repository.acquire_kanglong_locks(
        run_id="close-1",
        lock_scopes=["kanglong:account:a1", "kanglong:source-open-run:open-1"],
        ttl_ms=60_000,
    )
    second_conflict = repository.acquire_kanglong_locks(
        run_id="close-2",
        lock_scopes=["kanglong:account:a2", "kanglong:source-open-run:open-1"],
        ttl_ms=60_000,
    )
    assert first_conflict is None
    assert second_conflict is not None


@pytest.mark.asyncio
async def test_batch_recovery_reacquires_all_frozen_lock_scopes(repository, batch_service_factory) -> None:
    plan = batch_plan(
        run_id="close-1",
        operation="close",
        lock_scopes=("kanglong:account:a1", "kanglong:source-open-run:open-1"),
    )
    repository.save_batch_plan(plan, status="execution_starting")
    restarted = batch_service_factory(repository)
    await restarted.initialize_startup_recovery()
    assert repository.get_kanglong_lock("kanglong:account:a1")["run_id"] == "close-1"
    assert repository.get_kanglong_lock("kanglong:source-open-run:open-1")["run_id"] == "close-1"


def test_batch_run_populates_legacy_required_fields_without_using_them_as_source(repository) -> None:
    plan = batch_plan(account_ids=("a3", "a1", "a2"))
    repository.save_batch_plan(plan)
    stored = repository.get_kanglong_run(plan.run_id)
    assert stored["main_account_id"] == "a3"
    assert json.loads(stored["subaccount_ids_json"]) == ["a1", "a2"]
    assert [row["account_id"] for row in repository.list_batch_accounts(plan.run_id)] == ["a3", "a1", "a2"]


def test_refresh_plan_keeps_completed_prefix_immutable(planner) -> None:
    original = batch_plan(account_ids=("a1", "a2", "a3"), plan_version="plan-v1")
    refreshed = planner.refresh_pending_suffix(
        stored_plan=original,
        account_statuses={"a1": "completed", "a2": "blocked_precheck", "a3": "pending"},
        refreshed_accounts={"a2": refreshed_account_plan("a2"), "a3": refreshed_account_plan("a3")},
        credential_revision="revision-1",
    )
    assert refreshed.plan_version != original.plan_version
    assert refreshed.accounts[0] == original.accounts[0]
    assert refreshed.accounts[1:] != original.accounts[1:]
    assert refreshed.completed_prefix_length == 1


def test_refresh_plan_rejects_unbalanced_active_account(planner) -> None:
    original = batch_plan(account_ids=("a1", "a2"), plan_version="plan-v1")
    with pytest.raises(UnsafeBatchRefresh):
        planner.refresh_pending_suffix(
            stored_plan=original,
            account_statuses={"a1": "completed", "a2": "second_leg"},
            refreshed_accounts={"a2": refreshed_account_plan("a2")},
            credential_revision="revision-1",
        )
```

- [x] **Step 2: 运行红测**

Run: `python -m pytest tests/test_kanglong_batch_planner.py tests/test_kanglong_batch_storage.py -q`

Expected: FAIL，批次模块不存在。

- [x] **Step 3: 实现不可变计划模型**

```python
@dataclass(frozen=True, slots=True)
class KanglongBatchAccountPlan:
    account_id: str
    sequence: int
    target_long_qty: Decimal
    target_short_qty: Decimal
    maker_fee_rate: Decimal
    taker_fee_rate: Decimal
    bracket_max_allowed_leverage: int
    bracket_notional_coef: Decimal
    selected_bracket_effective_cap: Decimal
    current_symbol_leverage: int
    current_symbol_max_notional_value: Decimal
    effective_capacity_leverage: int
    reference_mid_price: Decimal
    capacity_snapshot_id: str
    market_snapshot_id: str


@dataclass(frozen=True, slots=True)
class KanglongBatchPlan:
    run_id: str
    operation: Literal["open", "close"]
    symbol: str
    preferred_side: PositionSide
    requested_leverage: int
    per_leg_notional: Decimal
    accounts: tuple[KanglongBatchAccountPlan, ...]
    source_open_run_id: str | None
    credential_revision: str
    lock_scopes: tuple[str, ...]
    completed_prefix_length: int
    input_hash: str
    plan_version: str
```

- [x] **Step 4: 在现有存储中增加最小批次队列表**

```sql
CREATE TABLE kanglong_batch_accounts (
    run_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    last_precheck_snapshot_at TEXT,
    PRIMARY KEY (run_id, account_id),
    UNIQUE (run_id, sequence)
);
```

迁移使用现有 `_ensure_column("kanglong_runs", "run_kind", "TEXT NOT NULL DEFAULT 'transfer'")`，批次写入 `kanglong_batch`。为满足现有 `kanglong_runs.main_account_id` 和 `subaccount_ids_json` 的非空约束，批次兼容写入 `main_account_id=canonical_account_ids[0]`、`subaccount_ids_json=canonical_account_ids[1:]`；这两个字段不得用于恢复批次账号游标或锁范围，批次事实源仍是不可变计划和 `kanglong_batch_accounts`。run 元数据、不可变计划（包括 `credential_revision` 和排序后的完整 `lock_scopes`）、目标数量、模拟成交分录、成本分录、当前轮次、事件、checkpoint hash、lease 和 fencing 全部写入现有对应字段、ledger/checkpoint 表及接口。账号游标只允许在事务内从 `sequence=N` 推进到 `N+1`，且前置账号状态必须是 `completed` 或 `completed_with_dust`。所有新 repository 方法都必须接收并验证现有 fencing token。

批次计划可以并存。plan 创建时将全部 `kanglong:account:{account_id}` 锁按字典序排序，close 再加入 `kanglong:source-open-run:{source_open_run_id}` 后重新排序，并把精确列表冻结到 `KanglongBatchPlan.lock_scopes`。开始执行前只能按冻结列表 all-or-nothing 获取锁；持有来源锁后，在同一 fencing 事务中重读来源 ledger 并校验所有账号可平余额，之后才能将 run 切到 `execution_starting`。任一锁冲突时不得留下部分锁，余额变化时释放本次取得的全部锁，整个 run 进入现有 `blocked_plan_stale`，不允许部分账号启动。执行开始后锁由现有 heartbeat 续租并在安全终态释放；启动恢复与 `needs_abort_recover` 流程必须从计划 JSON 调用 `_stored_lock_scopes()` 重建完整列表并重新获取，包括来源锁，不得从 main/sub 兼容字段重新推导。

plan 创建时按凭据仓库 canonical order 固化账号顺序与当前 `credential_revision`；确认和执行前都先比较凭据仓库当前 revision，不一致则返回 `409 credential_revision_conflict` 并将未执行计划置为 `blocked_plan_stale`。开仓确认通过 Task 5 的 `refresh_capacity(plan.credential_revision, account_ids, force_refresh=True)` 为全部账号刷新容量、投影档位与价格快照并原子保存计划；平仓确认调用独立 `refresh_close_availability()`，只校验来源 ledger 剩余量、账号状态、Hedge Mode、规则、行情与费率，不运行开仓容量阻断。Task 3 的 `AccountMutationGuard` 在此注册“存在活动 `kanglong_batch` run”查询，避免执行中删除、替换或重排账号。

`refresh_pending_suffix()` 只允许在账号边界调用。它从 `sequence=0` 起验证连续的 `completed/completed_with_dust` 前缀，将这些 `KanglongBatchAccountPlan` 对象逐项原样复制；仅用最新快照重建后续 `pending/blocked_precheck` 账号，并更新 `completed_prefix_length`、`credential_revision`、`input_hash` 和 `plan_version`。任一非前缀账号处于 `first_leg/second_leg/aligning/retry_wait/needs_recovery`，或 checkpoint 显示未配平时抛出 `UnsafeBatchRefresh` 并由服务转入 `needs_abort_recover`。新 plan hash 覆盖完整不可变前缀和新后缀；executor 从 `completed_prefix_length` 后的第一个账号继续，禁止重新执行前缀账号。

- [x] **Step 5: 回跑计划与存储测试**

Run: `python -m pytest tests/test_kanglong_batch_planner.py tests/test_kanglong_batch_storage.py tests/test_kanglong_storage_workflow.py -q`

Expected: PASS；测试应证明旧 run 迁移后 `run_kind=transfer`，批次与现有执行共享 ledger/event/lease/checkpoint/locks，数据库只新增一个批次账号表和一个判别列；批次兼容字段可写但不成为事实源，开仓确认会强制刷新容量/投影档位，平仓确认只刷新来源可用量且不受开仓容量阻断，凭据 revision 变化会使计划失效，部分刷新保持 completed 前缀的目标/费率/参考价格/快照 ID 不变且 executor 不会重跑，非安全账号阶段拒绝刷新，旧 fencing token 不能推进账号游标。来源锁测试必须使用不同账号但相同 `source_open_run_id`，并证明重启恢复会从冻结计划重新取得来源锁；平仓 LONG/SHORT 目标分别不超过持锁后重读的各腿来源未平数量，包括不等 dust。

### Task 7: 实现串行双腿执行器、重试和最终配平

**Files:**
- Create: `paired_opener/kanglong/batch_executor.py`
- Create: `paired_opener/kanglong/task_registry.py`
- Modify: `paired_opener/simulation_matching.py`
- Modify: `paired_opener/kanglong/reporter.py`
- Modify: `paired_opener/kanglong/models.py`
- Modify: `paired_opener/api.py`
- Modify: `paired_opener/storage.py`
- Test: `tests/test_kanglong_batch_api.py`
- Test: `tests/test_kanglong_batch_executor.py`
- Test: `tests/test_kanglong_task_registry.py`
- Test: `tests/test_kanglong_batch_reporter.py`
- Test: `tests/fixtures/kanglong_market/batch_ethusdc_rounds.json`

**Interfaces:**
- Consumes: `OrderbookMatcher`、Task 6 的 plan 与现有 repository。
- Produces: `KanglongBatchExecutor.run_next(run_id, lease_token, fencing_token) -> dict`。
- Produces: `task_registry.py` 中的共享 `KanglongExecutionTaskRegistry.start(run_id)`、`wake(run_id)`、`aclose(grace_seconds=15)`；它接管参考分支现有移仓与新批次的后台任务，按 `run_kind` 选择 worker，不新增进程或服务。
- Produces: `KanglongRepository.commit_kanglong_action(run_id, mutation, idempotency_key, request_hash, response, lease_expectation=None) -> dict`，在一个 SQLite 事务中提交 run、`progress_json.action_version`、events 和 idempotency，并在 worker-owned transition 时校验 lease/fencing。
- Produces: 批次 API 的计划、确认、执行、暂停、继续、停止和恢复行为。

- [x] **Step 1: 写串行、部分成交和中断恢复红测**

```python
@pytest.mark.asyncio
async def test_second_account_does_not_start_until_first_is_aligned(executor, repository, lease) -> None:
    await executor.run_next("run-1", lease.lease_token, lease.fencing_token)
    active = repository.get_active_account("run-1")
    assert active["account_id"] == "a1"
    assert repository.get_account("run-1", "a2")["status"] == "pending"


@pytest.mark.asyncio
async def test_restart_resumes_missing_second_leg_without_recounting_first_leg(factory, repository, lease) -> None:
    first = factory()
    await first.run_next("run-1", lease.lease_token, lease.fencing_token)
    before = repository.sum_leg_qty("run-1", "a1", "LONG")
    resumed = factory()
    await resumed.run_next("run-1", lease.lease_token, lease.fencing_token)
    after = repository.sum_leg_qty("run-1", "a1", "LONG")
    assert after == before
    assert repository.account_gap("run-1", "a1") <= Decimal("0.001")


@pytest.mark.asyncio
async def test_duplicate_or_out_of_order_book_snapshot_is_ignored(executor, repository) -> None:
    await executor.on_book_snapshot(snapshot(update_id=102, event_time=2000))
    before = repository.sum_fill_qty("run-1", "a1")
    await executor.on_book_snapshot(snapshot(update_id=102, event_time=2000))
    await executor.on_book_snapshot(snapshot(update_id=101, event_time=1999))
    assert repository.sum_fill_qty("run-1", "a1") == before


@pytest.mark.asyncio
async def test_account_is_rechecked_immediately_before_first_leg(executor, gateway, repository, lease) -> None:
    gateway.block_account_on_next_precheck("a2")
    await executor.run_until_account_boundary("run-1", lease.lease_token, lease.fencing_token)
    assert repository.get_account("run-1", "a2")["status"] == "blocked_precheck"
    assert repository.sum_fill_qty("run-1", "a2") == Decimal("0")


@pytest.mark.asyncio
async def test_open_recheck_uses_frozen_qty_at_fresh_executable_price(executor, gateway, repository, lease) -> None:
    gateway.move_price_beyond_reference_deviation("a2")
    await executor.run_until_account_boundary("run-1", lease.lease_token, lease.fencing_token)
    stored = repository.get_kanglong_run("run-1")
    assert stored["status"] == "paused_plan_recheck_changed"
    assert repository.sum_fill_qty("run-1", "a2") == Decimal("0")


@pytest.mark.asyncio
async def test_close_recheck_does_not_apply_open_capacity_block(executor, low_capacity_close_run, lease) -> None:
    await executor.run_next(low_capacity_close_run.run_id, lease.lease_token, lease.fencing_token)
    assert low_capacity_close_run.repository.get_account(low_capacity_close_run.run_id, "a1")["status"] != "blocked_precheck"


@pytest.mark.parametrize(
    ("raw_gap", "expected_action"),
    [("0.0005", "dust"), ("0.001", "align"), ("0.0015", "align")],
)
def test_alignment_uses_tradeability_not_less_than_or_equal_step_size(
    executor, raw_gap, expected_action
) -> None:
    assert executor.classify_gap(Decimal(raw_gap), quote()) == expected_action


@pytest.mark.asyncio
async def test_transport_retry_reuses_operation_id_after_response_loss(executor, repository, lease) -> None:
    executor.fail_after_checkpoint_once()
    with pytest.raises(ResponseLost):
        await executor.run_next("run-1", lease.lease_token, lease.fencing_token)
    committed_id = repository.last_operation_id("run-1")
    await executor.run_next("run-1", lease.lease_token, lease.fencing_token)
    assert repository.count_operation_id("run-1", committed_id) == 1


def test_duplicate_execute_returns_first_response_without_second_side_effect(client, confirmed_run) -> None:
    payload = {"plan_version": confirmed_run.plan_version, "idempotency_key": "execute-0001"}
    first = client.post(f"/kanglong/batch-simulation/plan/{confirmed_run.run_id}/execute", json=payload)
    second = client.post(f"/kanglong/batch-simulation/plan/{confirmed_run.run_id}/execute", json=payload)
    assert second.json() == first.json()
    assert confirmed_run.repository.count_execution_start_events(confirmed_run.run_id) == 1


def test_stale_action_version_is_rejected_without_state_change(client, paused_run) -> None:
    before = paused_run.repository.get_kanglong_run(paused_run.run_id)
    response = client.post(
        f"/kanglong/batch-simulation/run/{paused_run.run_id}/resume",
        json={
            "plan_version": paused_run.plan_version,
            "expected_action_version": before["progress"]["action_version"] - 1,
            "idempotency_key": "resume-0001",
        },
    )
    assert response.status_code == 409
    assert paused_run.repository.get_kanglong_run(paused_run.run_id)["status"] == before["status"]


def test_action_state_and_idempotency_roll_back_together_on_crash(client, confirmed_run) -> None:
    payload = {"plan_version": confirmed_run.plan_version, "idempotency_key": "execute-crash-0001"}
    confirmed_run.repository.enable_failpoint("after_run_update_before_idempotency_insert")
    with pytest.raises(InjectedCrash):
        client.post(f"/kanglong/batch-simulation/plan/{confirmed_run.run_id}/execute", json=payload)
    assert confirmed_run.repository.get_kanglong_run(confirmed_run.run_id)["status"] == "plan_confirmed"
    assert confirmed_run.repository.count_execution_start_events(confirmed_run.run_id) == 0
    assert confirmed_run.repository.get_kanglong_idempotency("execute-crash-0001", request_hash(payload)) is None

    confirmed_run.repository.disable_failpoint()
    response = client.post(f"/kanglong/batch-simulation/plan/{confirmed_run.run_id}/execute", json=payload)
    assert response.status_code == 200
    assert confirmed_run.repository.count_execution_start_events(confirmed_run.run_id) == 1


def test_expired_idempotency_key_can_be_reused(repository, confirmed_run, clock) -> None:
    repository.commit_kanglong_action(**first_action(confirmed_run, key="expired-key-0001"))
    clock.advance(hours=25)
    response = repository.commit_kanglong_action(**new_valid_action(confirmed_run, key="expired-key-0001"))
    assert response["status"] == "running"
    assert repository.count_kanglong_idempotency("expired-key-0001") == 1


def test_worker_owned_action_rejects_stale_fencing(repository, running_run) -> None:
    with pytest.raises(ValueError, match="kanglong_stale_fencing_token"):
        repository.commit_kanglong_action(
            **worker_action(running_run),
            lease_expectation=KanglongLeaseExpectation("old-lease", "old-fence"),
        )


@pytest.mark.asyncio
async def test_execution_registry_shutdown_waits_for_tasks_and_prevents_duplicate_recovery(registry, executor) -> None:
    registry.start("run-1")
    registry.start("run-1")
    assert registry.active_run_ids() == {"run-1"}
    await registry.aclose(grace_seconds=15)
    assert registry.active_run_ids() == set()
    assert executor.max_concurrent_workers_for("run-1") == 1


def test_transport_retry_policy_is_bounded_and_persists_long_waits(retry_policy) -> None:
    assert retry_policy.max_attempts == 5
    assert retry_policy.base_delay_ms == 500
    assert retry_policy.max_delay_ms == 30_000
    decision = retry_policy.on_rate_limit(retry_after_seconds=120)
    assert decision.persist_retry_wait is True
    assert decision.next_wakeup_at is not None
```

- [x] **Step 2: 运行红测**

Run: `python -m pytest tests/test_kanglong_batch_executor.py tests/test_kanglong_task_registry.py tests/test_kanglong_batch_api.py -q`

Expected: FAIL，执行器不存在。

- [x] **Step 3: 实现确定性 operation ID 和单步 checkpoint**

```python
def operation_id(run_id: str, account_id: str, round_index: int, leg: str, economic_attempt: int) -> str:
    return f"{run_id}:{account_id}:round-{round_index:04d}:{leg}:attempt-{economic_attempt:04d}"
```

`run_next()` 每次只推进一个可恢复 checkpoint；第一腿结果写入 pending operation，第二腿完成并可解释后同事务提交账本。若进程在两腿之间中断，恢复读取 pending operation，不重复累计第一腿。`economic_attempt` 只在已有尝试通过 checkpoint 得到确定结果、并创建新的补充尝试时递增；HTTP/网络重试和响应丢失始终复用原 operation ID 与 payload hash，重试次数另记 `transport_retry_count`。

传输策略固定为 `max_attempts=5`、`base_delay_ms=500`、`max_delay_ms=30000` 和 full jitter。网络超时、可判定为临时的 5xx、429 与行情临时过期可重试；参数、权限、签名、账号模式、计划/ledger/checkpoint/hash/fencing 冲突不可自动重试。429 等待 `max(backoff, Retry-After)`，418 只遵循 `Retry-After`；计算等待超过 30 秒时把账号置为 `retry_wait`、持久化 `next_wakeup_at` 并结束本次 `run_next()`，由 registry 到期唤醒，不在 worker 内长时间 sleep。第五次失败后若 checkpoint 已配平则进入 `paused_market_unstable`，否则进入 `needs_abort_recover`。

所有成交继续调用现有 `OrderbookMatcher`。对每个 symbol/account 保存最后接受的盘口标识；相同或更小的 `update_id` 直接忽略。只有行情源明确提供稳定 `event_time` 时，才允许缺少 `update_id` 的快照使用 `(event_time, payload_hash)` 去重并要求时间单调；否则进入 `paused_market_unstable`。checkpoint 必须保存该标识，重启后仍不能重复消费同一快照。

- [x] **Step 4: 实现最终配平和账号游标推进**

账号从 `pending` 进入第一腿之前，先核对当前 `credential_revision == plan.credential_revision`，再以 `force_refresh=True` 刷新该账号的 Portfolio Margin 状态、仓位、挂单、费率、杠杆档位和行情。开仓按计划冻结的 LONG/SHORT 目标数量与最新可执行价格计算 `capacity_requested_gross_notional`，重新选择投影档位并运行 Task 5 容量判断；相对 `reference_mid_price` 的偏离超过现有 symbol 配置 `max_reference_deviation_bps` 时同样判定计划需刷新。平仓只调用 Task 6 的 `refresh_close_availability()`，校验来源各腿剩余量和只读环境，不执行开仓容量阻断，也不得扩大任一腿目标。block 时账号记录 `blocked_precheck`：若整个 run 尚无模拟成交，批次使用 `blocked_plan_stale`，动作是 `refresh_plan/view_report`；若已有前置账号完成且当前处于安全账号边界，批次使用 `paused_plan_recheck_changed`，动作是 `refresh_plan/stop/view_report`。`refresh_plan` 调用 Task 6 的 `refresh_pending_suffix()`，以当前 revision 和新快照为未执行后缀生成新的 `plan_version`，完整继承已完成账号计划与 ledger 后回到 `chain_ready` 再次确认；若检测到未配平或 checkpoint 不安全，系统直接转入 `needs_abort_recover` 而不是提供 refresh。warning 记录事件后按已确认策略继续。此检查只发生在该账号尚无模拟成交时，恢复中的未配平账号优先完成恢复，不把容量预检误用为放弃第二腿的理由。

```python
raw_gap = abs(account_state.long_qty - account_state.short_qty)
tradeable_gap = normalize_qty(raw_gap, rules)
if raw_gap == 0:
    repository.complete_account_and_advance(run_id, account_id, dust_qty=Decimal("0"), fencing_token=fencing_token)
elif tradeable_gap > 0 and is_tradeable(tradeable_gap, quote, rules):
    repository.schedule_alignment_round(run_id, account_id, tradeable_gap, fencing_token=fencing_token)
else:
    repository.complete_account_with_dust_and_advance(run_id, account_id, raw_gap, fencing_token=fencing_token)
```

- [x] **Step 5: 实现费用与磨损分账**

```python
adverse = max(fill_cost_vs_mid, Decimal("0"))
improvement = max(-fill_cost_vs_mid, Decimal("0"))
fee = fill_notional * (maker_rate if role == "maker" else taker_rate)
```

`spread_cost`、`market_impact_cost`、`timing_drift_cost` 和 `alignment_cost` 必须分别落账，汇总时不以价格改善抵消 `total_adverse_wear`。

扩展现有 reporter 按 `run_id/account_id/leg/round` 聚合批次费用和磨损；不创建 `batch_reporter` 或第二套成本表。

- [x] **Step 6: 接入 API 与启动恢复扫描**

`lifespan()` 在 runtime 初始化后调用：

```python
await app.state.kanglong_execution_task_registry.initialize_startup_recovery()
try:
    yield
finally:
    await app.state.kanglong_execution_task_registry.aclose(grace_seconds=15)
    await app.state.runtime_manager.aclose()
    app.state.repository.close()
```

`KanglongExecutionTaskRegistry` 放在职责单一的 `task_registry.py`，替换参考分支 `api.py` 中仅记录 run ID 的 `_active_kanglong_background_runs`/裸 `create_task()` 路径；内部维护 `dict[run_id, asyncio.Task]` 和 accepting flag，并按 run 的 `run_kind` 调用既有移仓 worker 或新批次 worker。`start(run_id)` 在同一事件循环内先检查 registry 和 repository 的 lease，再创建并登记唯一任务；done callback 必须消费异常并移除完全相同的 task。`aclose()` 先关闭 accepting，通知 worker 不再开始下一次可恢复单步，等待当前 checkpoint 最多 15 秒，随后取消并 `gather(return_exceptions=True)` 所有剩余任务。只有安全 checkpoint 在启动时自动重新调度；非安全状态转入 `needs_abort_recover`。不引入数据库队列表、外部 worker 或消息中间件。

```python
class KanglongExecutionTaskRegistry:
    def start(self, run_id: str) -> bool:
        if not self._accepting or run_id in self._tasks:
            return False
        if self._repository.has_live_kanglong_lease(run_id):
            return False
        task = asyncio.create_task(self._worker_loop(run_id), name=f"kanglong-batch:{run_id}")
        self._tasks[run_id] = task
        task.add_done_callback(lambda completed: self._finish(run_id, completed))
        return True

    wake = start

    def _finish(self, run_id: str, completed: asyncio.Task[None]) -> None:
        if self._tasks.get(run_id) is completed:
            self._tasks.pop(run_id, None)
        try:
            completed.result()
        except asyncio.CancelledError:
            return
        except Exception as exc:
            self._logger.error(
                "kanglong_batch_worker_failed",
                extra={"run_id": run_id, "error_type": type(exc).__name__},
            )

    async def aclose(self, grace_seconds: int = 15) -> None:
        self._accepting = False
        self._shutdown_requested.set()
        tasks = tuple(self._tasks.values())
        if not tasks:
            return
        _, pending = await asyncio.wait(tasks, timeout=max(grace_seconds, 0))
        for task in pending:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
```

Task 7 必须在现有 `KanglongRunStatus` 中正式增加以下成员，并删除 `paused_plan_stale` raw 映射：

```python
RUNNING = "running"
PAUSE_PENDING = "pause_pending"
PAUSED_BY_USER = "paused_by_user"
PAUSED_MARKET_UNSTABLE = "paused_market_unstable"
STOP_PENDING = "stop_pending"
STOPPED_BY_USER = "stopped_by_user"
COMPLETED_WITH_DUST_RESIDUAL = "completed_with_dust_residual"
```

唯一动作矩阵至少明确覆盖：

```python
KanglongRunStatus.BLOCKED_PLAN_STALE.value: ("refresh_plan", "view_report")
KanglongRunStatus.PAUSED_PLAN_RECHECK_CHANGED.value: ("refresh_plan", "stop", "view_report")
KanglongRunStatus.PAUSED_MARKET_UNSTABLE.value: ("resume", "stop", "recover", "view_report")
KanglongRunStatus.NEEDS_ABORT_RECOVER.value: ("recover", "view_report")
```

这两个计划过期状态不提供 `recover`；系统检测到非安全 checkpoint 后先转入 `needs_abort_recover`。账号级状态仍保存在 `kanglong_batch_accounts`。

确认、执行、暂停、继续、停止和恢复全部通过现有 `kanglong_idempotency` 保存请求 hash 与首次响应。禁止沿用“先更新 run、事务结束后再调用 `remember_kanglong_idempotency()`”的现有调用顺序；必须使用以下单事务写入口：

```python
@dataclass(frozen=True, slots=True)
class KanglongActionMutation:
    expected_statuses: tuple[str, ...]
    expected_plan_version: str
    expected_action_version: int | None
    next_status: str
    available_actions: tuple[str, ...]
    plan: dict[str, object] | None
    progress: dict[str, object] | None
    report: dict[str, object] | None
    confirmed_at: str | None
    result_grade: str | None
    events: tuple[dict[str, object], ...]
    increment_action_version: bool


@dataclass(frozen=True, slots=True)
class KanglongLeaseExpectation:
    lease_token: str
    fencing_token: str


def commit_kanglong_action(
    self,
    *,
    run_id: str,
    mutation: KanglongActionMutation,
    idempotency_key: str,
    request_hash: str,
    response: dict[str, object],
    lease_expectation: KanglongLeaseExpectation | None = None,
) -> dict[str, object]:
    with self._lock, self._connection:
        now_dt = datetime.now(UTC)
        now = now_dt.isoformat()
        self._connection.execute(
            "DELETE FROM kanglong_idempotency WHERE expires_at <= ?",
            (now,),
        )
        existing = self._connection.execute(
            "SELECT request_hash, response_json FROM kanglong_idempotency WHERE idempotency_key = ?",
            (idempotency_key,),
        ).fetchone()
        if existing is not None:
            if existing["request_hash"] != request_hash:
                raise ValueError("idempotency_key_conflict")
            return _json_load(existing["response_json"], {})

        row = self._connection.execute(
            "SELECT * FROM kanglong_runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if row is None:
            raise ValueError("kanglong_run_not_found")
        if row["plan_version"] != mutation.expected_plan_version:
            raise ValueError("plan_version_conflict")
        if row["status"] not in mutation.expected_statuses:
            raise ValueError("kanglong_action_status_conflict")
        stored_progress = _json_load(row["progress_json"], {})
        current_action_version = int(stored_progress.get("action_version", 0))
        if (
            mutation.expected_action_version is not None
            and current_action_version != mutation.expected_action_version
        ):
            raise ValueError("action_version_conflict")

        if lease_expectation is not None:
            lease = self._connection.execute(
                """
                SELECT lease_token, fencing_token FROM kanglong_locks
                WHERE lock_scope = ? AND run_id = ? AND status = 'active' AND expires_at > ?
                """,
                (f"kanglong:run:{run_id}:lease", run_id, now),
            ).fetchone()
            if (
                lease is None
                or lease["lease_token"] != lease_expectation.lease_token
                or lease["fencing_token"] != lease_expectation.fencing_token
            ):
                raise ValueError("kanglong_stale_fencing_token")

        next_action_version = current_action_version + int(mutation.increment_action_version)
        progress = mutation.progress if mutation.progress is not None else stored_progress
        progress = {**progress, "action_version": next_action_version}
        plan = mutation.plan if mutation.plan is not None else _json_load(row["plan_json"], {})
        report = mutation.report if mutation.report is not None else _json_load(row["report_json"], {})
        confirmed_at = mutation.confirmed_at if mutation.confirmed_at is not None else row["confirmed_at"]
        result_grade = mutation.result_grade if mutation.result_grade is not None else row["result_grade"]
        self._connection.execute(
            """
            UPDATE kanglong_runs
            SET status = ?, available_actions_json = ?,
                plan_json = ?, progress_json = ?, report_json = ?,
                confirmed_at = ?, result_grade = ?, updated_at = ?
            WHERE run_id = ?
            """,
            (
                mutation.next_status,
                _json_dumps(list(mutation.available_actions)),
                _json_dumps(plan),
                _json_dumps(progress),
                _json_dumps(report),
                confirmed_at,
                result_grade,
                now,
                run_id,
            ),
        )
        for event in mutation.events:
            self._connection.execute(
                """
                INSERT INTO kanglong_events (run_id, group_id, round_id, event_type, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    event.get("group_id"),
                    event.get("round_id"),
                    event["event_type"],
                    _json_dumps(event.get("payload", {})),
                    now,
                ),
            )
        latest_event_id = int(
            self._connection.execute(
                "SELECT COALESCE(MAX(event_id), 0) AS value FROM kanglong_events WHERE run_id = ?",
                (run_id,),
            ).fetchone()["value"]
        )
        stored_response = {
            **response,
            "status": mutation.next_status,
            "action_version": next_action_version,
            "latest_event_id": latest_event_id,
        }
        self._connection.execute(
            """
            INSERT INTO kanglong_idempotency
                (idempotency_key, request_hash, response_json, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                idempotency_key,
                request_hash,
                _json_dumps(stored_response),
                now,
                (now_dt + timedelta(hours=24)).isoformat(),
            ),
        )
        return stored_response
```

repository 在同一个 `with self._lock, self._connection` 中先删除已过期 idempotency，再查询当前 key；不存在时校验 status、`plan_version` 和 `progress_json.action_version`，把新版本写回同一个 progress JSON，更新 run、写 events，再插入首次响应。worker-owned transition 还必须传入 `KanglongLeaseExpectation` 并在事务内校验活动 lease/fencing；确认、普通控制请求等 operator transition 传 `None`。任一步抛错整笔回滚；相同 key/same hash 返回已存响应，同 key/different hash 返回冲突，24 小时后 key 可重新使用。启动时另执行一次过期 idempotency 清理，避免仅靠请求触发。不得新增 `kanglong_runs.action_version` 列，继续以现有 `progress_json` 为唯一事实源。测试专用 failpoint 只在测试配置可用，放在 run update 之后、idempotency insert 之前并抛出异常，用于证明没有半提交。

- [x] **Step 7: 回跑执行与报表测试**

Run: `python -m pytest tests/test_kanglong_batch_executor.py tests/test_kanglong_task_registry.py tests/test_kanglong_batch_reporter.py tests/test_kanglong_batch_api.py tests/test_kanglong_api.py -q`

Expected: PASS；除串行、恢复、配平和成本外，必须覆盖盘口重复/乱序不重复成交、第二个开仓账号按冻结数量与最新可执行价格重算投影敞口、价格偏离触发 stale、平仓不执行开仓容量阻断、执行前与部分执行后的 stale 状态及动作分流、所有批次状态均进入 enum、部分刷新不重跑 completed 前缀、`0.5×/1×/1.5× step_size` 配平边界、`5` 次与 `500ms..30s` 传输策略、长 `Retry-After` 持久化唤醒、传输重试复用 operation ID、控制请求重复提交不重复产生副作用、状态写入与 idempotency 之间 failpoint 整笔回滚且重启后安全重试、过期 idempotency 清理与 key 复用、`progress_json.action_version` 冲突、旧 fencing 拒绝、registry 按 run 去重且 shutdown 后无任务、启动恢复沿用原 status/action/lease/fencing、完整冻结锁范围与 reporter。

### Task 8: 新增网页凭据管理和批次开平仓界面

**Files:**
- Modify: `paired_opener/static/index.html`
- Modify: `paired_opener/static/app.js`
- Modify: `i18n/messages/zh-CN.json`
- Modify: `i18n/registry/events.json`
- Modify: `i18n/registry/reasons.json`
- Test: `tests/test_app_kanglong_credentials.mjs`
- Test: `tests/test_app_kanglong_batch.mjs`
- Test: `tests/test_kanglong_i18n_contracts.py`

**Interfaces:**
- Consumes: Task 3 的凭据管理 API、Task 5 的默认值/容量 API、Task 7 的批次执行 API。
- Produces stable hooks: `kanglong-account-manager`、`kanglong-account-import`、`kanglong-batch-form`、`kanglong-batch-queue`、`kanglong-batch-cost-report`。
- Produces stable hooks: `kanglong-batch-defaults`、`kanglong-batch-capacity-preview`、`kanglong-batch-capacity-account-row`。

- [x] **Step 1: 写前端红测**

```javascript
assert.ok(html.includes('data-testid="kanglong-account-manager"'));
assert.ok(html.includes('type="password"'));
assert.ok(appSource.includes('KANGLONG_BATCH_PLAN_ENDPOINT'));
assert.equal(appSource.includes('localStorage.setItem("api_secret"'), false);
assert.equal(appSource.includes('/sessions/open'), true, 'existing trading UI must remain intact');
```

- [x] **Step 2: 运行红测**

Run: `node tests/test_app_kanglong_credentials.mjs && node tests/test_app_kanglong_batch.mjs`

Expected: FAIL，缺少新 UI。

- [x] **Step 3: 实现受保护的账号管理弹窗**

上传文件使用 `File.text()` 后 `JSON.parse()`，前端先检查最大 256 KiB、最多 100 个账号和 `credential_type=hmac`，但服务端仍独立执行解析前 body 限制与 schema 校验；先调用 preview 展示新增/更新/保留/删除和最终顺序，默认 `merge`，只有用户显式选择并再次确认才允许 `replace`。任何错误都不提交部分账号。commit 成功或失败后都立即清空所有 Secret input、文件内容和候选对象引用；列表只渲染 `api_key_masked`。无安全凭据时页面显示设置引导；检测到旧配置时只显示掩码账号数和迁移提示，不自动读取或回显 Secret。若返回 `credential_revision_conflict`，页面重新加载账号列表并要求重新预览，不自动覆盖。所有凭据写接口从 no-store 首页 bootstrap 读取页面内存 token，并通过 `X-Local-Management-Token` 提交；不得把 token、revision 或 Secret 写入 Cookie、`localStorage`、日志或 URL。

- [x] **Step 4: 实现批次表单与队列**

表单默认值：

```javascript
const KANGLONG_BATCH_DEFAULTS = {
  operation: "open",
  symbol: "ETHUSDC",
  preferredSide: "LONG",
  leverage: 100,
  perLegNotional: "250000",
  roundCount: 30,
  roundIntervalSeconds: 3,
};
```

执行按钮前展示容量预览响应中的 `batch_requested_gross_notional`，该值由后端根据账号数与当前 `per_leg_notional` 动态计算；不得在前端硬编码 `500000 USD`。同时明确“真实行情驱动的模拟，不会真实下单”。

`operation=open` 时，参数 change/input 事件经 300ms 防抖调用容量预览；每次修改立即清除旧计划，并通过 `AbortController` 取消前一个仍在等待的 preview 请求。响应的 `request_seq` 或 `input_hash` 与当前输入不一致时直接丢弃；取消只用于减少客户端等待，服务端仍负责请求合并和限流。`operation=close` 时不调用 capacity-preview，选择来源 run 后通过 close plan 响应展示各账号可平余额。

容量区域展示：

```text
账号 01：请求 500,000 USD / 保守估算可开 820,000 USD = 60.98%，限制项：保证金安全容量
批次：请求 1,500,000 USD / 保守估算可开 2,760,000 USD = 54.35%
最高占用：账号 02，78.12%
```

开仓容量区域同时显示 `requested_leverage/current_symbol_leverage/effective_capacity_leverage`、`existing_symbol_exposure/projected_symbol_exposure`、选中档位、`bracket_notional_coef`、`current_symbol_max_notional_value`、`assembled_at`、`oldest_component_at` 和“仅为当前快照估算，不代表 Binance 保证额度”；当前杠杆低于请求值时显示“容量按当前有效杠杆计算”，可选 hypothetical 值必须另区展示且不参与阻断。详情可展开查看各必需分量的采集时间、来源和年龄，任一分量过期时显示容量未知而不是沿用旧百分比。平仓页面改为展示来源批次各账号 LONG/SHORT 剩余量与本次最大可平数量，不显示可开仓百分比。页面展示可以四舍五入，但 plan/execute 使用后端未舍入数值和阻断状态。

队列直接消费后端 canonical run/account status 和结构化 reason。可将状态分组显示为“等待中/执行中/已配平/需处理”，但详情必须区分账号级 `blocked_precheck`、`retry_wait` 与批次级 `blocked_plan_stale`、`paused_plan_recheck_changed`、`paused_market_unstable`、`needs_abort_recover`，按钮只使用后端 `available_actions`，不得由前端自行猜测：前两种 stale 状态不展示 recover，只有 `needs_abort_recover` 展示 recover。确认/执行请求携带当前 `plan_version`；暂停/继续/停止/恢复还携带当前 `expected_action_version`。一次用户操作生成一个 `idempotency_key`，因超时产生的自动重试必须复用相同 key 和 payload；收到确定响应后再次操作才生成新 key。

- [x] **Step 5: 完成 i18n 并回跑**

Run: `node tests/test_app_kanglong_credentials.mjs && node tests/test_app_kanglong_batch.mjs && python -m pytest tests/test_kanglong_i18n_contracts.py tests/test_i18n_contracts.py -q`

Expected: PASS；前端测试还需覆盖无账号设置模式与旧配置迁移提示、HMAC-only、非默认每腿名义价值会更新批次总额、当前 20X/请求 100X 时显示有效 20X 且按其计算百分比、投影敞口/选中档位/当前最大名义值可见、平仓不显示开仓容量阻断、旧容量请求被取消且迟到响应被丢弃、混合 TTL 分量过期时不展示旧百分比、import preview 后才可 commit、revision 冲突要求重新预览、stale 状态不出现 recover 而 `needs_abort_recover` 出现 recover、动作超时重试复用 idempotency key、Secret/token/revision 不进入 Cookie 或持久化浏览器存储。

### Task 9: 完整验证与只读真实行情验收

**Files:**
- Modify: `README.md`
- Create: `docs/dev-logs/v0.2.0.md`
- Test: affected test suites listed below

**Interfaces:**
- Consumes: Tasks 1-8 全部结果。
- Produces: 可复核的测试记录、无敏感字段的手动验收摘要。

- [x] **Step 1: 运行安全与凭据定向测试**

Run: `python -m pytest tests/test_account_credentials.py tests/test_account_credentials_api.py tests/test_accounts.py tests/test_single_instance.py -q`

Expected: PASS。

必须覆盖：DPAPI/密文/ACL fail closed，无账号设置模式与旧配置迁移提示，HMAC-only，非法 loopback/`Host`/`Origin`/bootstrap token 拒绝，token 轮换且不进 Cookie/持久化存储，导入 body 在解析前限制，preview 不落盘，整批校验失败不产生部分更新，候选 runtime 失败后旧文件与旧 runtime 保持可用，准备后出现活动任务会阻止提交，请求取消不会造成文件/runtime 分裂，过期 token 和 credential revision 冲突不覆盖较新配置，同一数据目录第二实例 fail closed 且首实例退出后锁可恢复取得。

- [x] **Step 2: 运行批次与参考移仓回归**

Run: `python -m pytest tests/test_binance_gateway.py tests/test_classified_gateway.py tests/test_kanglong_batch_contracts.py tests/test_kanglong_batch_settings.py tests/test_kanglong_batch_capacity.py tests/test_kanglong_batch_planner.py tests/test_kanglong_batch_storage.py tests/test_kanglong_batch_executor.py tests/test_kanglong_task_registry.py tests/test_kanglong_batch_reporter.py tests/test_kanglong_batch_api.py tests/test_kanglong_executor.py tests/test_kanglong_storage_workflow.py tests/test_simulation_matching.py -q`

Expected: PASS。

必须覆盖：共享 ledger/event/checkpoint/lease/locks/reporter/status action 回归、所有批次 run 状态正式进入 enum、开仓账号开腿前按冻结数量和最新价格重算投影敞口与档位、价格偏离触发 stale、平仓不复用开仓容量阻断且各腿不超过来源剩余量、部分刷新保持 completed 前缀不可变且不重跑、凭据 revision 变化不复用缓存或旧计划、同源 close 在不同账号锁下仍并发互斥且重启后恢复来源锁、`0.5×/1×/1.5× step_size` 配平边界、限定重试次数/延迟与长 `Retry-After` 持久化、传输重试复用 operation ID、动作状态/event/idempotency 单事务及 failpoint 回滚、过期 idempotency 清理与复用、`progress_json.action_version` 冲突、旧 fencing 拒绝、重复与乱序盘口不重复模拟成交、registry shutdown 无残留任务且重启不重复调度。

- [x] **Step 3: 运行前端与 i18n 回归**

Run: `node tests/test_app_kanglong_credentials.mjs && node tests/test_app_kanglong_batch.mjs && node tests/test_app_kanglong_display.mjs && python -m pytest tests/test_kanglong_i18n_contracts.py tests/test_i18n_contracts.py -q`

Expected: PASS。

另运行容量协调器的并发测试，证明私有数据按 `(credential_revision, account_id, symbol, component)` 合并、revision 变化不命中旧私有缓存，公共行情按 `(symbol, component, depth)` 跨账号合并；100 个账号同 symbol 时 quote/order book 各只读取一次，私有账号并发上限为 4。普通预览命中短时缓存，而确认计划和开腿前的 `force_refresh` 必定访问上游但仍合并同批公共读取；混合 TTL 元数据必须准确，双腿投影敞口必须选择正确 bracket 并受 `maxNotionalValue` 限制，`notionalCoef` 只归一化一次；模拟 429/418 时必须遵循 `Retry-After`。

- [x] **Step 4: 证明模拟链路没有订单写调用**

使用 recording gateway 跑完整三账号批次并断言：

```python
assert all(call.method == "GET" for call in recorder.binance_calls)
assert not any("/order" in call.path for call in recorder.binance_calls)
assert not any(call.path.endswith("/leverage") for call in recorder.binance_calls)
```

- [ ] **Step 5: 执行真实行情只读冒烟**

Skipped：按用户明确要求，本阶段不执行需要真实测试账号与外部 Binance 连接的手工冒烟；保留为上线前受控环境验收项。

使用网页保存的一个测试账号读取 `ETHUSDC` 规则、20 档深度、统一账户状态、Hedge Mode、当前 symbol 杠杆、`symbolConfig.maxNotionalValue`、完整杠杆档位、`notionalCoef`、仓位、挂单和佣金率，先生成 `250000 USD/腿、请求 100X` 计划并记录双腿投影敞口与容量结果；若该账号预检允许，再用可通过预检的小额名义价值完整跑一次本地模拟，并在开腿前记录冻结数量按最新可执行价格计算的实际总名义价值。验收网关在发请求前拒绝任何非 GET 方法以及订单、杠杆、持仓模式和撤单路径。输出中只保留账号名称和掩码 Key，并记录请求/当前/有效杠杆、选中档位、容量 `assembled_at`、`oldest_component_at`、各分量年龄、限制项、“保守估算”说明及冒烟实际使用的模拟名义价值。

- [x] **Step 6: 执行最终代码检查**

Run: `python -m compileall paired_opener app_i18n`

Expected: PASS。

Run: `git diff --check`

Expected: 无输出，exit code 0。

Run: `./scripts/code-audit.sh`

Expected: PASS；若仓库不存在该脚本，记录“当前仓库未提供 Pomeva 审计脚本”，并按实际受影响模块完成等价定向审查，不伪造结果。

- [x] **Step 7: 更新版本日志**

`docs/dev-logs/v0.2.0.md` 记录功能范围、只读保证、测试结果、已知限制和未来真实下单阶段必须重新安全评审的边界。

## Self-Review

- Spec coverage: HMAC-only 凭据、无账号设置模式、旧配置迁移提示、带 revision 的原子导入/管理、解析前 body 限制、bootstrap token、本地管理安全、文件/runtime 无取消点切换、runtime 确定性关闭、单实例进程边界、后端账号顺序、Portfolio Margin、双腿投影敞口/完整 bracket/`notionalCoef`/`maxNotionalValue`、请求/当前/有效杠杆、私有与公共两级快照合并、分量新鲜度和限流、可配置默认开仓参数、保守容量占用百分比、开仓价格偏离复检、平仓独立可用量检查、100X、25 万每腿、方向优先、可恢复的来源开仓锁、completed 前缀不可变刷新、串行执行、逐账号强制预检、限定传输重试、API/盘口/operation 三层幂等、动作事务 failpoint、过期幂等清理、`progress_json.action_version`、worker fencing、任务 registry shutdown、可交易差额配平、恢复幂等和成本核算均有对应任务。
- Placeholder scan: 本计划没有留空实现项或模糊的后续补充项。
- Type consistency: `KanglongBatchPlanRequest`（含 `round_count`）、`KanglongBatchRecoverRequest`、`KanglongBatchPlan`（含 `credential_revision`、`lock_scopes`、`completed_prefix_length`）、`KanglongBatchAccountPlan`（含投影档位、当前/有效杠杆和快照 ID）、`KanglongBatchPlanner.refresh_pending_suffix()`、`KanglongBatchExecutor.run_next()`、`KanglongExecutionTaskRegistry`、`KanglongActionMutation`、`KanglongLeaseExpectation`、`commit_kanglong_action()`、`AccountCredentialStore`、`AccountCredentialCommitCoordinator`、`SingleInstanceGuard`、`CapacitySnapshotCoordinator` 和 create/update/import preview/commit schema 在首次出现时定义，后续任务使用相同名称；动作版本始终保存在 `progress_json.action_version`，批次状态全部进入 enum，计划过期状态使用 `blocked_plan_stale` 与 `paused_plan_recheck_changed` 且不提供 recover，非安全时转入 `needs_abort_recover`，不使用 raw `paused_plan_stale`。
- Architecture restraint: schema 只新增 `kanglong_batch_accounts` 和 `kanglong_runs.run_kind` 判别列；不新增 `action_version` 列、任务队列表、Redis 或消息队列。run、ledger、event、checkpoint、lease/fencing 和 reporter 使用现有实现，不维护双份事实源；worker registry 与容量协调器均为进程内组件。
