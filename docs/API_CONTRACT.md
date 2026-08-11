# API 契约

本文档记录网页客户端与本地服务之间需要稳定保持的接口。亢龙批次模块第一阶段只执行真实行情驱动的模拟交易，不得调用任何下单、改单或撤单接口。

## 通用约定

- 所有 JSON 使用 UTF-8；错误响应使用 `{"detail":{"code":"...","message":"..."}}`。
- 所有配置及运行状态变更请求必须通过同源 `Origin` 和 `X-Local-Management-Token` 校验。
- `/config/account-credentials/*` 还必须校验 loopback 客户端和 loopback `Host`。
- 首页通过 `Cache-Control: no-store` 的 HTML bootstrap 提供本次进程的管理 token。token 只保存在页面内存，不进入 Cookie、URL、日志或持久化浏览器存储。
- 凭据导入请求在 JSON 解析前限制为 `256 KiB`，并由请求模型再次限制最多 `100` 个账号。
- 第一阶段仅接受 `credential_type="hmac"`；其他类型返回 `422 credential_type_not_supported`。
- 安全凭据文件不存在时，账号管理查询可用且返回空账号；其他交易接口返回 `503 account_credentials_not_configured`。

## API Key 管理

### 路由

```text
GET    /config/account-credentials
POST   /config/account-credentials/import/preview
POST   /config/account-credentials/import/commit
POST   /config/account-credentials
PUT    /config/account-credentials/{account_id}
DELETE /config/account-credentials/{account_id}
PUT    /config/account-credentials/order
POST   /config/account-credentials/{account_id}/verify
```

凭据列表和所有写操作响应只能返回 `api_key_masked` 与 `has_api_secret`，禁止返回 `api_secret` 或完整 API Key。

创建账号必须包含 `account_id`、`name`、`api_key`、`api_secret`、固定的 `credential_type=hmac` 和固定的 `account_mode=portfolio_margin`。更新账号时省略 `api_secret` 表示保留现有 Secret。

批量导入只接受 UTF-8 JSON，默认模式为 `merge`；只有显式选择 `replace` 才删除候选文件中不存在的账号。导入分为预览和提交：

1. `POST /import/preview` 校验整个候选列表，重复 ID 或任一记录无效时整批失败且不落盘。
2. 成功响应包含最终账号顺序、变更摘要、当前 `credential_revision`、默认五分钟有效的 `expires_at` 和一次性 `preview_token`，不得包含任何 Secret。
3. `preview_token` 同时绑定基准 revision 与候选内容哈希。
4. `POST /import/commit` 只有在 token 未过期且当前 revision 未变化时才原子提交，否则返回 `409 credential_revision_conflict`。

活动批次期间，删除、更新、重新排序及导入提交返回 `409 account_credentials_locked_by_active_batch`。

## 亢龙多账号批次模拟

### 路由

```text
GET    /config/kanglong-batch-defaults
PUT    /config/kanglong-batch-defaults
POST   /kanglong/batch-simulation/capacity-preview
POST   /kanglong/batch-simulation/plan
POST   /kanglong/batch-simulation/plan/{run_id}/confirm
POST   /kanglong/batch-simulation/plan/{run_id}/execute
POST   /kanglong/batch-simulation/run/{run_id}/{action}  # pause|resume|stop|recover|refresh_plan
GET    /kanglong/batch-simulation/run/{run_id}
GET    /kanglong/batch-simulation/run/{run_id}/events
GET    /kanglong/batch-simulation/open-runs
```

`GET/PUT /config/kanglong-batch-defaults` 读取或原子保存网页新建批次使用的默认交易对、优先方向、杠杆、每腿名义价值、轮次和间隔。修改默认值不影响已经冻结的计划。

`POST /kanglong/batch-simulation/capacity-preview` 只接受 `operation=open`，携带 `request_seq` 和 `input_hash` 并在响应中原样返回。服务端按安全凭据仓库顺序规范化所选账号。响应按账号返回以下保守估算字段：

- 双腿请求：`requested_gross_notional`、`capacity_requested_gross_notional`。
- 投影敞口：`existing_symbol_exposure`、`projected_symbol_exposure`。
- 容量与比例：`conservative_openable_notional`、`estimated_capacity_usage_percent`、`limiting_factor`。
- 杠杆与档位：`requested_leverage`、`current_symbol_leverage`、`bracket_max_allowed_leverage`、`bracket_notional_coef`、`selected_bracket_effective_cap`、`current_symbol_max_notional_value`、`effective_capacity_leverage`。
- 批次汇总：`batch_requested_gross_notional`、`batch_conservative_openable_notional`、`batch_estimated_usage_percent`、`bottleneck_account_id`。
- 快照证据：`assembled_at`、`oldest_component_at`、`snapshot_components`、`calculation_version`。

`snapshot_components` 至少包含 `account`、`positions`、`open_orders`、`symbol_config`、`leverage_bracket`、`commission_rate`、`quote` 和 `order_book`。每项包含独立的 `observed_at`、`source=cache|upstream`、`age_ms`、`ttl_ms` 和限制值。任一必需分量缺失或超过 TTL 时容量为未知并阻止确认。该结果只表示当前快照下的保守估算，不是 Binance 保证额度。平仓来源剩余量不使用此接口。

`KanglongBatchPlanRequest` 字段如下：

| 字段 | 约束 |
| --- | --- |
| `operation` | `open` 或 `close` |
| `symbol` | 必填交易对 |
| `preferred_side` | `LONG` 或 `SHORT`，决定双腿优先顺序 |
| `leverage` | 默认 `100`，范围 `1..125`，仅用于开仓容量估算 |
| `per_leg_notional` | 默认 `250000`，必须大于零 |
| `account_ids` | `1..100` 个账号；服务端按凭据仓库顺序重新排序并冻结 |
| `source_open_run_id` | 平仓必填；开仓省略 |
| `round_count` | 默认 `30`，范围 `1..500` |
| `round_interval_seconds` | 默认 `3`，范围 `0..3600` |

计划创建后，服务端将账号按凭据仓库顺序冻结到不可变 `plan.accounts`，同时冻结
`credential_revision`、排序后的完整 `lock_scopes`、每账号两腿目标数量、费率、杠杆档位、
参考行情与快照 ID，并用完整计划生成 `plan_version`。批次存储以
`kanglong_runs.run_kind=kanglong_batch` 和 `kanglong_batch_accounts` 为事实源；
`main_account_id/subaccount_ids` 仅用于兼容旧表约束。

平仓计划必须从已完成开仓批次的 ledger 计算来源剩余量。每个账号分别返回
`source_long_remaining_qty`、`source_short_remaining_qty`、`target_long_qty`、
`target_short_qty`、`source_ledger_hash` 和 `source_checkpoint_id`。已由先前平仓批次消费的
数量必须扣除；LONG/SHORT 分别计算，不能取两腿最小值，也不能由客户端名义价值扩大。
同一来源开仓批次通过 `kanglong:source-open-run:{run_id}` 锁实现排他关闭。

计划确认及执行启动前必须比较当前凭据 revision。变化时返回
`409 credential_revision_conflict` 并把尚未成交的计划置为 `blocked_plan_stale`。

批次确认和执行复用 `KanglongActionRequest`，必须包含 `plan_version` 和 `idempotency_key`。暂停、继续和停止复用 `KanglongControlRequest`，还必须包含 `expected_action_version`。批次恢复使用 `KanglongBatchRecoverRequest`，包含 `plan_version`、`expected_action_version`、`idempotency_key` 和 `release_reason`，不改变现有移仓 `KanglongRecoverRequest`。

动作版本保存在 `progress_json.action_version`。相同 idempotency key 与相同请求 hash 返回首次响应且不重复副作用；同 key 不同请求返回 `409 idempotency_key_conflict`；旧计划版本返回 `409 plan_version_conflict`；旧动作版本返回 `409 action_version_conflict`。幂等记录保留 24 小时，过期后可清理并重新使用 key。

### 批次状态与动作

批次 API 返回的状态必须属于 `KanglongRunStatus`。批次新增正式状态：

```text
running
pause_pending
paused_by_user
paused_market_unstable
stop_pending
stopped_by_user
completed_with_dust_residual
```

计划过期状态的动作固定如下：

| 状态 | 可用动作 |
| --- | --- |
| `blocked_plan_stale` | `refresh_plan`, `view_report` |
| `paused_plan_recheck_changed` | `refresh_plan`, `stop`, `view_report` |
| `needs_abort_recover` | `recover`, `view_report` |

禁止返回历史 raw 状态 `paused_plan_stale`。账号级 `blocked_precheck`、`retry_wait`、`paused`、`needs_recovery` 保存在 `kanglong_batch_accounts`，不作为批次 run status。

### 冲突响应

所有确认、执行和控制接口显式声明以下 `409`：

- `idempotency_key_conflict`
- `plan_version_conflict`
- `action_version_conflict`
- `account_credentials_locked_by_active_batch`
- `credential_revision_conflict`
- `credential_preview_invalid`
- `account_credential_already_exists`
- `kanglong_action_status_conflict`
- `kanglong_batch_lock_conflict`
- `kanglong_close_source_changed`
- `kanglong_stale_fencing_token`
- `blocked_plan_stale`
- `kanglong_batch_warning_confirmation_required`
- `kanglong_batch_capacity_blocked`
- `kanglong_batch_refresh_unsafe`

`blocked_plan_stale` 表示确认时真实行情、费率、当前杠杆、档位、容量或平仓来源已偏离冻结计划，客户端必须展示原因并重新生成计划，不得静默替换用户已确认的计划。`kanglong_batch_warning_confirmation_required` 同时返回尚未确认的 `warning_codes`；客户端需由操作员明确确认后复用同一冻结计划再次提交。
