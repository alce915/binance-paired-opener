# 亢龙多账号双腿开平仓（真实行情模拟）需求

## 目标

在现有交易控制台中新增独立的“亢龙批量开平仓”能力。系统按网页中保存的账号顺序，逐个账号使用 Binance 真实行情模拟 LONG/SHORT 双腿开仓或平仓，确保单账号最终配平，并记录手续费估算和交易磨损。

第一阶段只模拟成交，不向 Binance 提交、修改或撤销任何真实订单，也不修改真实账户杠杆、持仓模式或保证金配置。

## 已确认口径

- 账号类型：Binance 统一账户（Portfolio Margin）的 USDⓈ-M 合约账户。
- 执行方式：账号之间严格串行；当前账号安全完成并配平后才能进入下一个账号。
- 交易对：由用户从允许列表中选择，第一版继续以 `ETHUSDC`、`BTCUSDC` 为默认允许范围。
- 方向：用户选择 LONG 优先或 SHORT 优先；方向只决定第一腿，最终仍同时建立或关闭等量 LONG/SHORT。
- 杠杆：默认请求 `100X`，范围 `1..125`；第一阶段只作为模拟参数，不调用 Binance 的杠杆修改接口。保守容量不得假定请求杠杆已经在账户生效，必须同时展示请求杠杆、交易对当前杠杆和用于容量计算的有效杠杆。
- 名义价值：默认每腿约 `250,000 USD`，LONG/SHORT 合计约 `500,000 USD`。
- 默认参数：交易对、优先方向、杠杆、每腿名义价值、初始轮次和轮次间隔均可在网页中修改并保存为本机默认值。
- 数量：根据执行前真实行情将每腿名义价值换算为合约数量，并按交易对 `step_size` 归一化。
- 平仓范围：只关闭指定开仓批次模拟建立的数量，不把真实账户已有仓位或其他模拟批次仓位纳入本次平仓。
- 失败策略：单账号失败、未配平或状态不可解释时停止整个批次，保留 checkpoint；恢复时先完成该账号，不跳过到后续账号。
- 费用：按每个账号、每个交易对从 Binance 读取的 maker/taker 佣金率快照估算，不能把 USDC 交易对手续费硬编码为零。

## 非目标

- 不开放真实下单 endpoint、按钮或后台开关。
- 不调用 `/papi/v1/um/order`、`/fapi/v1/order` 等真实下单接口。
- 不调用修改杠杆、切换持仓模式、切换保证金模式或撤单接口。
- 不自动跳过失败账号继续执行。
- 不允许从网页读取或导出已保存的完整 API Secret。
- 不让用户在平仓时任意输入数量并误平非本批次仓位。

## 账号与 API 凭据管理

### 网页能力

账号管理入口位于亢龙工作区，支持：

- 上传 UTF-8 JSON 文件批量导入账号。
- 手工新增、编辑账号名称、API Key 和 API Secret。
- 删除账号。
- 上移、下移或拖拽调整账号顺序。
- 启用、停用账号。
- 测试连接并展示统一账户状态、双向持仓模式和读取权限结果。
- 用掩码展示 API Key；API Secret 永远不回显。

导入 JSON 的第一版结构：

```json
{
  "accounts": [
    {
      "account_id": "account-01",
      "name": "账号 01",
      "api_key": "replace-me",
      "api_secret": "replace-me",
      "account_mode": "portfolio_margin",
      "enabled": true
    }
  ]
}
```

数组顺序就是默认执行顺序。`account_id` 在同一安装实例中必须唯一且稳定；修改名称不能改变历史批次归属。

导入规则：

- 第一阶段只支持当前网关已经实现的 HMAC API Key/API Secret，不接受 RSA/Ed25519 私钥；凭据类型不匹配时返回结构化错误，不把签名失败误报为连接失败。
- 单次文件最大 `256 KiB`、最多 `100` 个账号；前端先做体验性检查，服务端必须在 JSON 解析前限制请求体字节数，并在模型层再次限制账号数，任一超限直接拒绝。
- 上传后先显示预览，包括新增、更新、未变化、重复 ID 和无效记录，不立即写入。
- 默认使用 `merge`：同 ID 更新、未出现的旧账号保留；只有用户显式选择并二次确认 `replace` 时才删除文件中未出现的账号。
- 同一文件出现重复 `account_id`、任一记录格式错误或任一 Secret 缺失时，整个导入失败，不允许部分写入。
- 预览生成的单次 token 有效期默认 `5` 分钟，并绑定当前 `credential_revision` 与候选内容哈希；预览后账号配置被其他页面修改时，提交返回 `409 credential_revision_conflict`，不得覆盖较新的配置。
- 保存前先完成全部 schema 校验和候选运行时构建，再原子切换密文文件与运行时；任一步失败都保留旧配置。
- 前端提交的 `account_ids` 只表示选择集合；服务端必须按凭据仓库中的持久化顺序重新排序，拒绝重复、未知或已停用账号，不能信任客户端顺序。
- 没有安全凭据文件且没有旧账号配置时，服务仍以“账号设置模式”启动并提供静态页面与凭据管理接口；交易、容量和批次接口返回结构化 `503 account_credentials_not_configured`，不能因为缺少 active runtime 导致整个服务无法启动。
- 没有安全凭据文件但检测到旧环境变量或旧账号文件时，现有交易功能继续使用旧配置，批次账号管理显示迁移提示；只有用户显式提交安全凭据后才切换为安全文件，系统不自动删除旧文件。安全文件一旦存在，损坏或解密失败必须 fail closed，不得静默回退到旧明文来源。

### 密钥安全

- API Key/API Secret 只允许发送到 `127.0.0.1`/`::1` 上的本机服务。
- 所有凭据写接口同时校验 loopback 客户端、loopback `Host`、同源 `Origin` 和随服务启动生成的 CSRF/本机管理 token；任一校验失败均拒绝请求。token 通过 `Cache-Control: no-store` 的首页 bootstrap 注入，只保存在页面内存并通过 `X-Local-Management-Token` 请求头提交，不写 Cookie、URL 或浏览器持久化存储；服务重启后自动轮换。
- 使用 Windows DPAPI CurrentUser 保护完整凭据载荷后再写入本地文件。
- 加密文件写入 `config/binance_accounts.secure.json`，并加入 `.gitignore`。
- 写文件使用临时文件加原子替换；临时文件中也只能出现密文。
- 密文文件及其目录限制为当前 Windows 用户访问；DPAPI 不可用、密文损坏、解密失败或权限设置失败时必须 fail closed，不得回退到明文或空账号列表覆盖旧文件。
- API 响应、日志、异常、事件、SQLite 和前端状态中不得包含完整 API Secret。
- 更新账号时允许省略 `api_secret`，表示保留现有 Secret。
- 删除账号前检查活动批次；账号参与活动批次时禁止删除或改序。
- 候选 runtime 的联网验证可在账号变更锁外完成；真正提交时必须取得同一个变更锁，重新核对 `credential_revision` 和现有 session/移仓/批次活动状态，然后连续完成密文文件原子替换与 runtime 指针切换，两者之间不得出现 `await`。提交后才等待旧 runtime 关闭；请求取消不得留下“新文件、旧 runtime”状态。
- runtime 原子切换后必须等待旧 runtime 的 HTTP client/连接池关闭；关闭异常只记录脱敏错误，不回滚已经完成的指针切换。服务 shutdown 还必须关闭当前 runtime，不允许遗留无人跟踪的后台清理任务。
- 第一阶段推荐使用只读、允许 `USER_DATA` 的 API Key；不要求 `TRADE` 权限。

## Binance 统一账户读取边界

现有网关已具备部分 `/papi` 账户读取能力，第一阶段统一使用以下只读接口：

- `/papi/v1/account`：统一账户权益、初始保证金和账户状态。
- `/papi/v1/um/positionRisk`：USDⓈ-M 仓位、杠杆和名义价值。
- `/papi/v1/um/positionSide/dual`：确认 `dualSidePosition=true`。
- `/papi/v1/um/symbolConfig`：交易对当前杠杆和配置。
- `/papi/v1/um/leverageBracket`：校验目标名义价值对应档位允许 `100X`。
- `/papi/v1/um/openOrders`：读取目标交易对当前挂单并计入已有风险敞口。
- `/papi/v1/um/commissionRate`：冻结账号级 maker/taker 费率。
- USDⓈ-M 公开行情的 exchange info、quote 和 order book：计算数量和模拟成交。

开仓时，账户状态不是 `NORMAL`、未开启 Hedge Mode、投影总敞口对应档位不支持请求杠杆、可用余额不足、存在风险未知字段、交易规则不可用或行情过期都会使该账号预检失败。平仓不执行“保守可开仓容量”阻断，但仍必须校验来源批次剩余量、账户状态、Hedge Mode、交易规则、行情和费率；低可开容量不得阻止风险收敛性质的平仓模拟。

## 批次输入

```text
operation: open | close
symbol: ETHUSDC
preferred_side: LONG | SHORT
leverage: 100
per_leg_notional: 250000
account_ids: 网页提交选择集合，后端按凭据仓库顺序冻结的账号 ID 列表
source_open_run_id: 平仓时必填
round_count: 默认 30
round_interval_seconds: 默认 3
```

`leverage` 与 `per_leg_notional` 只用于开仓目标和容量计算；平仓目标只能来自 `source_open_run_id` 的剩余 ledger，客户端即使提交这两个字段也不得改变可平数量。

## 默认开仓参数

网页提供独立的“默认开仓设置”，第一版允许配置：

- `default_symbol`：默认 `ETHUSDC`，必须在交易对白名单内。
- `default_preferred_side`：默认 `LONG`。
- `default_leverage`：默认 `100`，范围 `1..125`。
- `default_per_leg_notional`：默认 `250000`，必须大于零。
- `default_round_count`：默认 `30`，范围 `1..500`。
- `default_round_interval_seconds`：默认 `3`，范围 `0..3600`。

设置保存在不含密钥的 `config/kanglong_batch_defaults.json`。修改默认值只影响之后新建的表单和计划；已经确认或正在执行的批次继续使用计划中冻结的参数。

风险安全参数第一阶段只展示、不允许在普通表单中修改：

- `margin_safety_ratio`：默认 `20%`。
- `max_notional_ratio`：默认 `80%`。
- `min_liquidation_buffer_ratio`：默认 `15%`。
- `price_buffer_bps`：默认 `5 bps`。

这些安全参数仍从服务端配置读取，避免用户误把安全缓冲调成零。

## 保守可开仓估算与占用百分比

用户修改默认参数或本次开仓参数时，网页必须在 300ms 防抖后重新请求容量预览，不能只在点击“检测计划”时计算。新的输入出现后前端取消仍在等待的旧请求；服务端仍以 `request_seq` 和 `input_hash` 隔离迟到响应。

容量预览不得在每次输入时无条件重拉所有 Binance 数据。第一阶段使用进程内 `CapacitySnapshotCoordinator`，不新增数据库或独立服务：

- 账号私有数据按 `(credential_revision, account_id, symbol)` 合并并发读取；账号 ID 不变但 API Key/API Secret 更新后必须使用新的 revision，旧 revision 的缓存不得再命中。
- 公开 exchange info、quote 和 order book 按 `(symbol, component, depth)` 跨账号合并；100 个账号预览同一交易对时不得重复拉取 100 份相同公共行情。两类缓存仍由同一个 `CapacitySnapshotCoordinator` 管理，不拆服务。
- 账户、仓位和挂单等动态快照默认缓存 `3` 秒；symbol config、杠杆档位和佣金率默认缓存 `60` 秒，TTL 由服务端配置。
- 普通输入变化优先复用未过期快照，只重新计算 Decimal 公式；计划确认以及账号即将开始第一腿时使用 `force_refresh=true` 绕过缓存。
- 上游读取采用有限并发，默认最多 `4` 个账号同时请求；统一记录 `X-MBX-USED-WEIGHT-*`，遇到 `429/418` 严格遵循 `Retry-After`，不得由多个页面各自盲目重试。
- 缓存只包含只读风险数据，不包含 API Secret；服务重启后自然失效。

每个账号先计算：

```text
requested_per_leg_notional = 用户输入的每腿名义价值
requested_gross_notional = requested_per_leg_notional * 2

existing_symbol_exposure =
  abs(current_long_notional)
  + abs(current_short_notional)
  + current_open_order_notional

capacity_requested_gross_notional =
  普通预览/确认时使用 requested_gross_notional
  账号开腿前使用冻结目标数量按最新可执行价格计算的实际双腿总名义价值

projected_symbol_exposure =
  existing_symbol_exposure
  + capacity_requested_gross_notional

selected_leverage_bracket =
  按 projected_symbol_exposure 在完整 leverageBracket 列表中选择的档位

effective_capacity_leverage = min(
  requested_leverage,
  current_symbol_leverage,
  selected_bracket_max_allowed_leverage
)

available_after_safety = max(
  totalAvailableBalance * (1 - margin_safety_ratio),
  0
)

cost_rate =
  1 / effective_capacity_leverage
  + max(maker_fee_rate, taker_fee_rate, 0)
  + price_buffer_bps / 10000

margin_capacity_notional = available_after_safety / cost_rate
equity_capacity_notional = accountEquity * effective_capacity_leverage * max_notional_ratio
liquidation_buffer_notional = accountEquity * effective_capacity_leverage * (1 - min_liquidation_buffer_ratio)

bracket_remaining_notional = max(
  selected_bracket_effective_notional_cap - existing_symbol_exposure,
  0
)

symbol_config_remaining_notional = max(
  current_symbol_max_notional_value - existing_symbol_exposure,
  0
)

conservative_openable_notional = min(
  margin_capacity_notional,
  equity_capacity_notional,
  liquidation_buffer_notional,
  bracket_remaining_notional,
  symbol_config_remaining_notional
)

estimated_capacity_usage_percent =
  capacity_requested_gross_notional / conservative_openable_notional * 100
```

该公式把 LONG/SHORT 两腿都按完整初始保证金和最不利佣金率计算，不使用统一账户可能产生的风险抵扣来扩大容量，也不假定用户请求的杠杆已经生效，因此只是基于当前快照的保守工程估算，不是 Binance 对可成交额度的承诺。`leverageBracket` 必须读取完整档位列表，保留账号返回的 `notionalCoef`；网关适配层以缺省 coefficient `1` 计算 `effective_floor = notionalFloor * notionalCoef`、`effective_cap = notionalCap * notionalCoef`，容量层只能使用归一化结果，不得忽略或重复应用 coefficient。页面可另列“假设调整到请求杠杆后的模拟容量”，但不得把该 hypothetical 值用于阻断、警告或占用百分比。

批次汇总同时提供：

```text
batch_requested_gross_notional = sum(每个账号 requested_gross_notional)
batch_conservative_openable_notional = sum(每个账号 conservative_openable_notional)
batch_estimated_usage_percent =
  batch_requested_gross_notional / batch_conservative_openable_notional * 100
bottleneck_usage_percent = max(每个账号 estimated_capacity_usage_percent)
```

展示规则：

- 每个账号显示请求双腿名义价值、保守可开仓估算、估算占用百分比、限制容量的最小项，以及 `requested_leverage/current_symbol_leverage/effective_capacity_leverage`。当前杠杆低于请求杠杆时显示结构化警告。
- 表单顶部显示批次加权占用百分比和最高占用账号。
- 任一账号占用超过 `100%` 时阻断计划。
- `80%..100%` 显示高风险警告并要求用户显式确认。
- 容量为零或数据不完整时显示“容量未知”，禁止确认计划。
- 预览返回 `assembled_at`、`oldest_component_at`、`calculation_version` 和各分项容量；账户、仓位、挂单、symbol config、杠杆档位、佣金率、quote 与 order book 各自返回 `observed_at`、`source=cache|upstream`、`age_ms` 和 `ttl_ms`。任一必需分量超过自己的 TTL 或缺失时容量为未知并禁止确认；百分比只在展示层舍入，服务端判断使用未舍入 Decimal。
- 参数修改后旧容量预览和旧计划立即失效；较早返回的异步预览不能覆盖最新输入。
- 平仓页面不展示“可开仓占用百分比”作为阻断项，改为展示来源批次每账号 LONG/SHORT 剩余量、已平数量和本次最大可平数量。

创建计划时冻结以下快照并生成 `plan_version`：

- 账号 ID 与顺序。
- 当前 `credential_revision`；账号凭据提交成功后，基于旧 revision 创建的计划立即失效。
- 账号连接状态和统一账户预检结果。
- symbol rules、请求杠杆、当前 symbol 杠杆、容量有效杠杆、杠杆档位、参考价格和深度摘要。
- 每账号 maker/taker 佣金率。
- 每腿目标名义价值和归一化数量。
- 执行策略、安全上限和输入 hash。

任何输入、账号顺序、凭据版本或行情规则变化都会使旧计划失效，必须重新检测并确认。

开仓计划冻结目标数量和参考价格。账号开腿前使用最新盘口按冻结数量计算当前实际双腿名义价值、投影总敞口和档位；若相对计划参考价格的偏离超过现有 symbol 配置中的 `max_reference_deviation_bps`，或最新实际名义价值使容量/杠杆档位不可执行，则在安全账号边界进入计划刷新，不能继续按旧的 `250000 USD/腿` 容量结论执行。平仓数量仍以来源 ledger 为准，不因价格变化扩大。

计划确认时重新读取所有账号的轻量风险数据。批次执行期间，每个账号从 `pending` 进入 `prechecking` 时还必须再次读取该账号的账户状态、可用余额、仓位、挂单、杠杆档位、费率和最新行情，并重算保守容量；若与计划相比变为不可执行，则停在该账号并要求刷新计划或恢复，不能继续使用旧快照，也不能推进后续账号。

批次已有账号完成后刷新计划时，已完成账号构成不可变前缀：其账号顺序、目标数量、费率/行情快照、成本基准和 ledger 关联全部原样继承，新 `plan_version` 只重算 `pending`/`blocked_precheck` 后缀。当前账号存在未配平成交或非安全 checkpoint 时禁止刷新，必须先进入恢复流程到达安全账号边界。新 plan hash 必须覆盖不可变前缀和新后缀，已完成账号不得再次执行。

## 架构复用边界

- 复用参考分支已有的 `OrderbookMatcher`、Kanglong ledger、checkpoint hash、events、运行租约/fencing 和成本 reporter。
- 批次 run 继续写入现有 `kanglong_runs`、`kanglong_ledger_entries`、`kanglong_events` 和 `kanglong_locks`；仅给 `kanglong_runs` 增加默认值为 `transfer` 的 `run_kind` 判别列，批次使用 `kanglong_batch`，再从已冻结请求的 `operation=open|close` 区分开平仓语义。为兼容旧表的非空字段，批次写入时令 `main_account_id=canonical_account_ids[0]`、`subaccount_ids_json=canonical_account_ids[1:]`；这两个字段仅作旧表兼容，批次账号顺序和状态仍以冻结计划及 `kanglong_batch_accounts` 为事实源。
- 只新增 `kanglong_batch_accounts` 保存账号顺序、账号级状态和预检时间；两腿累计数量与当前轮次继续以现有 ledger/checkpoint 为事实源，不重复落表。
- 新增轻量 `KanglongBatchExecutor` 只负责账号串行编排和同账号双腿语义，不复制 storage、lease、checkpoint、event 或 reporter 实现。
- 移仓 executor 与批次 executor 保持各自的腿语义，但共同依赖上述执行内核。
- 凭据 CAS、容量缓存和限频协调均为进程内机制，因此同一配置/数据目录只允许一个服务进程且固定 `uvicorn workers=1`。服务启动时必须持有由操作系统管理的独占实例锁，第二个实例 fail closed；进程异常退出后锁由操作系统释放，不引入 Redis、分布式锁或新的常驻服务。

## 执行状态机

### 批次状态

```text
draft_plan -> chain_ready -> plan_confirmed -> execution_starting -> running
chain_ready | plan_confirmed | execution_starting -> blocked_plan_stale
blocked_plan_stale -> chain_ready | needs_abort_recover
running -> pause_pending -> paused_by_user
running -> paused_market_unstable | paused_plan_recheck_changed | needs_abort_recover
paused_by_user | paused_market_unstable -> running | stop_pending
paused_plan_recheck_changed -> chain_ready | needs_abort_recover
stop_pending -> stopped_by_user
needs_abort_recover -> abort_recovering -> aborted_recovered
running -> completed | completed_with_dust_residual
```

实现时必须把批次实际使用的 `running`、`pause_pending`、`paused_by_user`、`paused_market_unstable`、`stop_pending`、`stopped_by_user` 和 `completed_with_dust_residual` 正式加入 `KanglongRunStatus`，并更新唯一的 `available_actions_for_status()`；不能继续依赖 raw string。`chain_ready` 在批次页面显示为“计划已就绪”。确认或执行启动前尚无模拟成交时发现计划过期，使用 `blocked_plan_stale`，动作精确为 `refresh_plan/view_report`；已有账号完成后、下一账号开腿前在安全边界复检发生变化，使用 `paused_plan_recheck_changed`，动作精确为 `refresh_plan/stop/view_report`。正常路径都由 `refresh_plan` 生成新的 `plan_version` 并回到 `chain_ready` 后再次确认，不能直接进入 `plan_confirmed`。状态图中的 `-> needs_abort_recover` 仅表示系统检测到未配平或 checkpoint 不安全时的自动迁移；这两个计划过期状态不提供 `recover`，进入 `needs_abort_recover` 后才提供。删除旧的 `paused_plan_stale` raw 映射；账号级 `blocked_precheck` 仍保存在 `kanglong_batch_accounts`，不另造批次状态。

### 账号状态

```text
pending -> prechecking -> first_leg -> second_leg -> aligning -> completed | completed_with_dust
prechecking -> blocked_precheck
blocked_precheck -> prechecking | stopped
first_leg | second_leg | aligning -> retry_wait -> 原阶段
任意活动阶段 -> paused | needs_recovery
```

上述名称是 `kanglong_batch_accounts` 的账号级状态，不进入共享 `KanglongRunStatus`；批次级按钮和恢复动作始终以 run status 为准。

批次只允许一个账号处于活动状态。配平判断必须保留未舍入差额：

```text
raw_gap = abs(batch_long_qty - batch_short_qty)
tradeable_gap = normalize_qty(raw_gap, symbol_rules)
```

`raw_gap == 0` 才是完全配平；只要 `tradeable_gap > 0` 且达到最小名义价值，就必须继续补充轮。只有归一化后数量为零或名义价值低于交易规则下限时，才能把 `raw_gap` 记录为 dust 并进入 `completed_with_dust`；不得因为差额恰好等于一个 `step_size` 而提前完成。

## 开仓语义

1. 使用真实 order book 计算每腿目标数量和拆单轮次。
2. preferred side 对应第一腿；第一腿模拟实际成交量决定第二腿目标量。
3. 第二腿成交不足时，将缺口写入 checkpoint 并进入补充轮。
4. 每轮只在两腿结果和账本变化可解释时原子提交。
5. 当前账号配平后保存完成 checkpoint，再推进账号游标。

同一模拟订单只能从新的 order-book snapshot 获得新增成交。同一 `snapshot_id`/`lastUpdateId` 被轮询多次时只能更新等待时间，不能重复消耗同一档流动性；只有明确提供稳定 `event_time` 的快照源才允许使用 `(event_time, payload_hash)` 作为替代键。snapshot 倒退、无法生成稳定键或在运行内冲突时进入行情异常暂停。

## 平仓语义

1. 用户选择一个已完成或 `completed_with_dust_residual` 的开仓批次。
2. 系统按开仓批次账本逐账号读取可平 LONG/SHORT 数量。
3. preferred side 决定先平哪一腿，另一腿跟随第一腿实际成交量。
4. 不读取真实账户仓位作为本次可平数量来源；真实仓位只用于展示和风险提示。
5. 已平数量通过 operation ID 幂等累计，重复恢复不能重复扣减批次仓位。

创建和确认平仓计划只做预览，不预占来源仓位。创建计划时把按字典序排列的全部 `kanglong:account:{account_id}` 锁和平仓专用的 `kanglong:source-open-run:{source_open_run_id}` 锁冻结为计划 JSON 中的 `lock_scopes`。真正执行前必须按该冻结列表一次性获取现有 `kanglong_locks`；持锁后在同一 fencing 事务中重新计算来源批次剩余量。锁冲突时不得获得部分锁；余额变化时释放本次已取得的锁，当前计划进入 `blocked_plan_stale`，不得部分启动。执行开始后锁由现有 heartbeat 续租；进程重启时恢复逻辑必须从冻结计划重建并重新获取完整 `lock_scopes`，包括来源锁，不能只从旧版 main/sub 账号字段推导账号锁。锁直到批次安全终态后释放，因此不新增 reservation 表。

## 重试、恢复与配平

- 每个批次持有带 fencing token 的运行租约。
- 确认和执行请求必须携带 `plan_version` 与 `idempotency_key`；暂停、继续、停止和恢复还必须携带 `expected_action_version`。run 状态、action version、事件和首次幂等响应必须在同一 SQLite 事务提交；相同 key 与相同请求重试返回首次结果且不重复副作用，同 key 不同请求、旧计划版本或旧动作版本返回结构化 `409`。状态变更后、事务提交前发生进程崩溃时整笔回滚，重启重试仍只产生一次状态变化和事件。
- 幂等记录默认保留 `24` 小时；事务内查询必须忽略并删除已过期记录，启动时执行一次过期清理。过期 key 可作为新请求重新使用，未过期 key 仍按 request hash 判定重复或冲突。
- operation ID 至少包含 `run_id/account_id/round_index/leg/economic_attempt`，跨账号、轮次和补充轮必须唯一。`economic_attempt` 只在上一个经济尝试已经通过 checkpoint 得到确定结果、并明确创建新的补充尝试时递增。
- 同一经济尝试的网络超时、429、临时 5xx、进程重启或响应丢失必须复用相同 operation ID 和 payload hash；传输重试次数单独记录为 `transport_retry_count`，不得进入 operation ID。
- 每次轮次提交同时保存账号游标、两腿累计量、残余、费用、磨损、行情快照 ID 和下一次唤醒时间。
- 网络超时、429、临时 5xx 和行情暂时过期采用有限指数退避；参数错误、权限错误和账本不一致不自动盲重试。
- 默认传输重试策略为最多 `5` 次、基础延迟 `500ms`、上限 `30s`、full jitter；429 使用 `max(指数退避, Retry-After)`，418 必须遵循 `Retry-After`。等待超过 `30s` 时持久化 `retry_wait` 和 `next_wakeup_at` 后释放当前 worker，不允许在内存中长时间 sleep；重试耗尽时，已配平账号进入可人工继续的暂停状态，未配平账号进入 `needs_abort_recover`。
- 进程启动时扫描非终态批次；安全 checkpoint 自动恢复，非安全 checkpoint 进入现有 `needs_abort_recover`。
- 旧 worker 的租约过期或被接管后，不得再写事件、账本或覆盖状态。
- 用户停止时必须先到达安全 checkpoint；未配平账号进入 `needs_abort_recover`，不能直接标记 `stopped_by_user`。
- 批次后台执行任务必须由单个进程内 registry 跟踪并按 `run_id` 去重。服务 shutdown 时先停止接收新批次和新的 `run_next` 调度，等待当前单步 checkpoint 最多 `15` 秒；超时后取消并等待全部任务结束，再关闭 runtime 和 SQLite。重启后只从持久化 checkpoint 恢复，不允许旧任务和启动恢复重复推进同一 run。

## 手续费与磨损

按账号、腿、轮次和批次累计以下字段：

- `maker_notional`、`taker_notional`。
- `estimated_fee`、`fee_rate`、`fee_role`、`fee_asset`。
- `spread_cost`：相对同一行情快照中间价的买卖价差成本。
- `market_impact_cost`：吃穿盘口档位造成的加权价格偏移。
- `timing_drift_cost`：两腿不同时刻参考中间价变化造成的成本。
- `alignment_cost`：补充轮或最终配平额外产生的成本。
- `price_improvement`：有利成交单列，不与不利磨损相互抵消后只展示净值。
- `total_adverse_wear`：只累计不利成本。
- `net_execution_effect`：价格改善减去不利磨损和手续费后的净影响。

USDC 优惠是否为零以 `/papi/v1/um/commissionRate` 的账号级快照为准；查询失败时默认阻断计划，不猜测费率。

## UI 与操作保护

- 创建计划、确认计划、开始模拟三步分离。
- 页面显著显示“真实行情模拟，不会真实下单”。
- 所有改变配置或运行状态的请求都要求同源 `Origin` 和 CSRF/本机管理 token；凭据接口再额外执行 loopback 客户端与 `Host` 校验。
- 开始前二次确认账号数量、每腿名义价值、总名义价值、杠杆和账号顺序。
- 执行期间禁用账号删除、凭据更新和顺序调整。
- 账号队列展示账号级 canonical status、当前轮次和结构化原因；页面可用“等待中/执行中/已配平/需处理”分组，但不得把 `blocked_precheck`、`retry_wait`、`paused`、`needs_recovery` 全部折叠成无法区分的 `failed`。
- 每个账号展示目标数量、LONG/SHORT 累计量、差额、手续费和磨损。
- 参数区实时展示每账号容量占用、批次加权占用和最高占用账号。
- 支持暂停、继续、停止、恢复和导出报告。

## 验收标准

### 凭据管理

- 上传 3 个账号后，刷新和重启服务仍保持原顺序。
- 服务端文件不包含 API Secret 明文。
- 列表、错误响应、运行日志和浏览器状态中无法找到完整 Secret。
- 修改名称且不提交新 Secret 时，原 Secret 仍可用于连接。
- 活动批次期间删除或改序返回结构化冲突。
- merge/replace 导入均先预览；重复 ID 或任一无效记录不会造成部分写入。
- 预览后另一页面修改账号时，旧 `preview_token` 提交返回版本冲突且不覆盖新配置。
- 候选运行时构建失败时，密文文件和当前运行时保持旧版本。
- 无安全凭据、无旧配置时仍能打开账号管理页并完成首次上传；其他交易接口返回结构化未配置错误。
- 旧账号配置存在但安全文件尚未建立时不会被自动删除；首次安全提交后 runtime 与安全文件在同一提交临界区切换。
- 缺少同源/CSRF token、DPAPI 解密失败或密文权限异常时凭据写操作失败关闭。
- 首页 bootstrap token 不进入 Cookie、URL、日志或持久化存储，服务重启后旧 token 失效；超过 `256 KiB` 的请求在 JSON 解析前被服务端拒绝。
- HMAC 凭据可正常验证；RSA/Ed25519 输入返回 `credential_type_not_supported` 且不改变旧配置。
- 同一配置/数据目录启动第二个服务实例时失败关闭；首实例正常或异常退出后，实例锁可由新进程重新取得。

### 统一账户预检

- 使用 `/papi` 返回统一账户权益和 LONG/SHORT 仓位。
- `dualSidePosition=false` 时阻断，不自动切换模式。
- 25 万名义价值对应档位不允许 100X 时阻断，并展示最大允许杠杆。
- 杠杆档位使用“已有仓位 + 挂单 + 双腿总名义价值”的投影敞口选择，并同时受 `symbolConfig.maxNotionalValue` 限制；`notionalCoef` 变化会使旧计划失效。
- 佣金率查询失败时阻断费用评估。

### 多账号执行

- 三账号只允许一个账号同时处于活动状态。
- 客户端乱序提交账号 ID 时，后端仍按凭据仓库顺序冻结计划。
- LONG 优先时先模拟 LONG，SHORT 优先时顺序相反。
- 每个账号只有在两腿原始差额为零，或归一化后确实低于最小可交易数量/名义价值并记录 dust 时，才推进下一个账号；差额恰好为一个 `step_size` 时必须继续配平。
- 任一账号中断后恢复时从原账号 checkpoint 继续，不从第一账号重跑。
- 同一轮恢复多次不会重复计入成交、手续费或磨损。
- 同一经济尝试在响应丢失或网络重试后复用 operation ID，不会因递增传输重试次数而重复计账。
- 失败账号不会被自动跳过。
- 后续账号开始前会重新预检；容量或账户状态变化后停在该账号，不使用计划期旧快照。
- 开仓账号开始前按冻结数量和最新可执行价格重算实际双腿名义价值；价格偏离超过 `max_reference_deviation_bps` 或投影敞口进入更低杠杆档位时刷新计划，不按旧容量继续。
- 低可开容量不会阻断合法的来源批次平仓；带 LONG/SHORT 不等 dust 的来源批次也不会扩大任一腿可平数量。
- 相同 order-book snapshot 连续返回时不会被重复计入成交。
- 两个平仓批次并发消费同一来源开仓批次时，最多一个能取得来源锁；另一个不会扣减或模拟成交。
- 即使两个平仓批次使用不同账号锁，只要 `source_open_run_id` 相同仍只能有一个取得来源锁；服务重启恢复后仍持有或重新取得该来源锁。
- 差额分别为 `0.5×`、`1×`、`1.5× step_size` 时，只有真正不可交易的部分会记录为 dust。
- 相同确认/执行/控制请求因双击或超时重复提交时只产生一次状态变化和一次副作用；旧 `action_version` 不改变运行状态。
- 在状态更新与幂等记录之间注入失败并重启后，请求可安全重试，数据库中仍只有一次状态变化、一次事件和一份首次响应。
- 幂等记录过期前同 key/same hash 返回原响应，超过 24 小时后被清理且 key 可重新用于合法新请求。
- 服务在执行中 shutdown 时不会遗留后台任务、关闭后的 runtime 使用或重复恢复；重启后从最后 checkpoint 继续。
- 传输重试遵循 `5` 次、`500ms..30s` 与 `Retry-After` 规则，权限/参数/账本错误不进入自动重试。
- 批次 API 返回的运行状态全部属于 `KanglongRunStatus`，不再出现 raw `paused_plan_stale`；动作矩阵与文档逐项一致。

### 默认参数与容量

- 修改并保存默认交易对、方向、杠杆、每腿名义价值、轮次和间隔后，刷新页面仍可恢复。
- 新建表单使用最新默认值，已确认批次参数不随默认值变化。
- 任一输入变化都会重新计算容量，并使旧计划失效。
- 多个页面同时请求同一凭据 revision、账号和交易对容量时合并上游读取；同账号更新 API Key/API Secret 后新 revision 不得命中旧账号快照，基于旧 revision 的计划不得确认或执行。
- 100 个账号预览同一交易对时公共 quote/order book 各只读取一次，账号私有读取仍受并发上限约束。
- 未过期快照只重算公式，确认计划和账号开腿前仍强制刷新；响应必须分别展示组装时间、最旧分量时间和每个必需分量的来源/年龄/TTL，任一分量过期时容量未知并阻断。
- 100 个账号的预览遵守有限并发与 Binance weight/`Retry-After`，不会因输入事件形成无界请求风暴。
- 每账号百分比等于请求双腿名义价值除以该账号保守可开仓估算。
- 批次加权百分比使用总请求名义价值除以总保守容量，不能简单平均账号百分比。
- 杠杆档位、已有仓位或挂单成为最小限制项时，界面明确展示该限制项。
- 账号当前杠杆为 `20X`、请求杠杆为 `100X` 时，保守容量使用 `20X` 并分别展示请求、当前和有效杠杆；不得按尚未生效的 `100X` 低估占用百分比。
- 任一账号超过 100% 时无法确认；80% 到 100% 必须确认风险警告。

### 成本与报告

- maker/taker 采用各账号各自费率快照。
- 手续费、spread、impact、timing drift、alignment cost 和 price improvement 分列可复算。
- 每账号汇总之和等于批次汇总。
- 平仓报告能够关联开仓批次，且平仓数量不超过该批次剩余模拟仓位。

## 参考测试

参考任务 `019e6eee-c6f8-7411-9b11-677ef0634dc8` 和分支 `codex/kanglong-transfer-executor-v2` 中的以下能力：

- `tests/test_simulation_matching.py`：真实盘口语义的确定性撮合回放。
- `tests/test_kanglong_executor.py`：双腿、补充轮、唯一 operation ID 和配平。
- `tests/test_kanglong_storage_workflow.py`：checkpoint、租约和 fencing。
- `tests/test_kanglong_reporter.py`：手续费、滑点和磨损报表。
- `tests/test_kanglong_api.py`：后台恢复和清理异常。
- `tests/fixtures/kanglong_market/ethusdc_rounds.json`：不依赖实时行情波动的测试 fixture。

手动验收允许读取 Binance 主网真实行情和统一账户只读数据，但必须通过 fake/simulation gateway 证明没有任何订单写接口被调用。
