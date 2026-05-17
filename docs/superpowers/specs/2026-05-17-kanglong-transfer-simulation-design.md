# 亢龙有悔移仓模拟模块设计

## 背景

现有系统已经具备多账号配置、单账号运行时、双向开仓、双向平仓、单向开仓、单向平仓、模拟盘和实盘两套执行模式。亢龙有悔模块不重新实现交易底层，而是在现有规则之上增加跨账号链路规划、逐组逐轮调度、风险阻断、模拟报告和消耗统计。

第一版只做模拟盘。模拟盘稳定后，实盘不新增另一套策略逻辑，只切换执行模式，保持和当前系统模拟盘/实盘机制一致。

为避免模拟盘和未来实盘出现策略分叉，亢龙模块先定义统一执行事件模型。链路规划、预检、风控、对账和消耗统计只消费统一事件，不直接依赖模拟盘或实盘内部实现。模拟盘和实盘的差异只能存在于执行适配层。

## 目标

- 在 1 个主账号和多个子账号之间模拟完整移仓链路。
- 起始账号默认选择盈利方向可平仓数量最高的子账号。
- 第一组必须由起始子账号向主账号转移盈利方向的全部可平仓数量。
- 每次只允许一组账号参与转移。
- 每组按交易对配置的仓位数量上限分轮执行。
- 每一轮必须完成平仓、开仓、成交对账后才能进入下一轮。
- 中间阶段允许子账号临时不配平，但最终所有子账号必须恢复到运行前基准仓位。
- 主账号最终必须清空本交易对仓位，作为纯中转账户。
- 主账号开始前也必须为空仓，避免亢龙流程误处理主账号既有仓位。
- 最终配平优先账号间转移；只有无法闭合时才生成市场减仓建议，且必须单独确认。
- 全流程统计手续费消耗、移仓/配平价差 PnL 和保守价差损耗。

## 非目标

- 第一版不真实下单。
- 第一版不支持手动设置释放数量。
- 第一版不允许起始账号部分释放。
- 第一版不自动执行市场减仓。
- 第一版不并发执行多组账号转移。

## 核心概念

### 账号角色

- 主账号：纯中转账户。可以临时承接仓位，但最终 LONG/SHORT 必须都清空到交易对数量容忍范围内。
- 子账号：盈利释放和最终恢复对象。中间可以临时不配平，最终必须恢复到运行前基准 LONG/SHORT 数量。

### 盈利方向

亢龙每次运行只处理一个全局方向 `selected_side`。默认自动选择所有参与子账号汇总后未实现盈利金额更多的方向。若 LONG 和 SHORT 汇总后都盈利，优先选择盈利金额更高的方向；如果盈利金额相同，再选择汇总可平仓数量更大的方向。

自动方向报告必须同时生成 `other_side_preview`。`other_side_preview` 展示另一方向的汇总未实现盈利、可平仓数量、首选 donor、主账号承接能力是否满足和不可执行原因。第一版只做提示，不因为另一方向可执行而自动切换 `selected_side`。

`other_side_preview` 不生成执行链路，不占用运行锁，不改变 `plan_version`，也不能作为后续实盘候选。用户若要切换方向，必须重新生成新的模拟计划。

策略允许用户手动选择 LONG 或 SHORT。手动方向仍必须盈利，否则检测阶段阻断。

手动选择只影响盈利方向，不允许绕过盈利检查、主账号承接检查、风险阻断，也不允许手动设置释放数量。

### 起始账号

方向确定后，只扫描 `selected_side` 盈利的子账号，并按该方向可平仓数量从大到小排序。

起始账号为排序第一的子账号。计划释放数量为该账号 `selected_side` 的全部可平仓数量。

## 配置

按交易对配置组内单轮数量上限和容忍误差。

```json
{
  "ETHUSDC": {
    "per_round_qty_limit": "0.05",
    "qty_tolerance": "0.0001",
    "max_rounds_per_group": 30,
    "max_chain_groups": 100,
    "max_main_temp_qty": "1.50",
    "max_main_temp_notional_ratio": "0.80",
    "price_buffer_bps": 5,
    "margin_safety_ratio": "0.20",
    "min_liquidation_buffer_ratio": "0.15",
    "snapshot_ttl_ms": 5000,
    "price_ttl_ms": 2000,
    "run_lock_ttl_ms": 600000,
    "simulation_result_ttl_ms": 60000
  }
}
```

`per_round_qty_limit` 是每一轮最多转移的标的数量。金额、保证金、最小名义金额和盈利覆盖成本作为风险检查，不作为主拆分单位。

`max_chain_groups` 限制完整链路最多生成多少个账号组，避免最优化策略生成过长链路。`price_buffer_bps`、`margin_safety_ratio` 和 `min_liquidation_buffer_ratio` 用于主账号承接能力和组内风险检查。`snapshot_ttl_ms`、`price_ttl_ms` 和 `simulation_result_ttl_ms` 用于判断模拟结果和实盘启动前状态是否过期。`run_lock_ttl_ms` 用于清理异常中断后残留的运行锁；锁过期前不能启动同范围的新运行。

所有仓位数量、价格、费用和 PnL 计算必须使用 Decimal，不允许使用浮点数。下单数量按交易所 step size 向下取整；任何取整差额都必须写入结构化 `residual_ledger`，最终按账号、方向和腿类型聚合校验。价格按交易所 tick size 归一化；价格取整差异只进入成本估算和价差统计，不进入仓位残差账本。

## 统一执行事件模型

所有模拟执行结果必须产出和未来实盘一致的事件结构：

```text
run_id
group_id
round_id
mode
account_id
symbol
position_side
action_type
leg_id
paired_leg_id
round_match_id
client_order_id
exchange_order_id
order_side
reduce_only
planned_qty
submitted_qty
filled_qty
matched_qty
close_residual_qty
open_residual_qty
avg_price
entry_price
realized_pnl
pnl_asset
fee
fee_asset
fee_rate
liquidity_role
fills
order_type
status
reason
market_fallback_used
residual_qty
snapshot_version
submitted_at
filled_at
event_time
```

`action_type` 包括 `single_close`、`single_open` 和 `market_reduce_proposal`。`status` 至少包括 `filled`、`partial_filled`、`rejected`、`timeout` 和 `cancelled`。`leg_id` 和 `paired_leg_id` 用于把一轮内的平仓腿和开仓腿关联起来；`round_match_id` 用于把同一轮的数量对账记录关联到两条腿事件。实盘里记录真实交易所订单号，模拟盘里生成可回放的合成订单号。`liquidity_role` 记录 `maker` 或 `taker`，用于还原真实手续费和价差统计。

`fills` 中每条成交明细至少包含：

```text
trade_id
fill_qty
fill_price
fee
fee_asset
liquidity_role
filled_at
```

`matched_qty`、`close_residual_qty` 和 `open_residual_qty` 是轮次对账字段。单腿事件可以引用同一个 `round_match_id`，报告层按 `round_match_id` 汇总出该轮的 matched 和 residual 结果。

模拟盘必须能模拟部分成交、拒单、超时、手续费、成交均价、Market fallback、残差和交易规则不可用等结果。未来实盘只替换执行适配层，不替换链路规划、预检、风控、对账和消耗统计逻辑。

## 预检与风险阻断

模拟开始前读取所有参与账号快照，包括余额、可用余额、LONG/SHORT 数量、未实现盈亏、杠杆、交易对规则、参考价格和系统挂单状态。

参与账号和交易对在同一时间只能存在一个亢龙运行。预检阶段如果发现本模块之外的未完成挂单、手动调仓痕迹、快照版本异常变化或已有运行锁，必须阻断。组开始和轮开始时重新校验快照版本；如果发现不是当前运行造成的仓位或挂单变化，暂停链路并进入 `paused_external_state_changed`。

运行锁范围按保证金模式决定。逐仓模式按 `account_id + symbol` 加锁；全仓或共享 USDC 保证金模式按 `account_id + margin_asset` 加锁；如果无法可靠识别保证金模式，则退化为整个账号加锁。

运行锁在链路执行期间必须通过 heartbeat 续期，续期间隔小于 `run_lock_ttl_ms / 2`。只有以下情况可以释放运行锁：

```text
completed and final ledger closed
blocked before any side-effect event
paused with ledger closed
unsafe after manual abort/recover confirmed
```

如果链路中途暂停且仍存在 `pending_debt_queue`、`residual_ledger` 或本模块遗留挂单，必须继续持锁。释放这类锁只能通过人工 `abort/recover` 流程：重新读取账号快照、撤销或确认没有本模块遗留挂单、确认未解释仓位变化已处理，并写入审计记录。

如果进程异常退出导致锁过期，下一次运行必须先进入安全恢复检查，确认没有本模块遗留挂单和未解释仓位变化后才能重新加锁。

第一版要求所有参与子账号在运行前已经双向配平：

```text
abs(baseline_long_qty - baseline_short_qty) <= qty_tolerance
```

如果某个子账号运行前已经不配平，状态为 `blocked_initial_subaccount_unbalanced`，不在亢龙流程里顺手修复历史不平衡。

第一版要求主账号在运行前对本交易对为空仓：

```text
abs(main_initial_long_qty) <= qty_tolerance
abs(main_initial_short_qty) <= qty_tolerance
```

如果主账号已有本交易对仓位，状态为 `blocked_main_not_flat`。报告必须展示主账号当前 LONG/SHORT 数量、超出容忍范围的数量，以及需要先手动处理的方向。

第一组有硬阻断规则：

```text
主账号可承接数量 >= 起始账号盈利方向全部可平仓数量
```

如果主账号因为临时仓位上限、名义风险比例或保证金不足只能承接一部分，则模拟不可开始，状态为 `blocked_main_insufficient_capacity`。报告必须提示起始账号、盈利方向、计划释放数量、主账号可承接数量、缺口数量、预计所需保证金、当前可用保证金和建议补充保证金。

主账号可承接数量按最保守口径计算：

```text
main_receivable_qty = min(
  max_main_temp_qty_remaining,
  max_main_temp_notional_ratio_qty,
  margin_capacity_qty,
  liquidation_buffer_qty,
  exchange_rule_limit_qty
)
```

其中 `margin_capacity_qty` 必须扣除预计手续费、价格漂移缓冲和预留安全保证金后再计算初始保证金占用；`liquidation_buffer_qty` 必须保证临时承接后仍满足预设爆仓距离或维护保证金缓冲。只要任一维度只能承接部分计划释放数量，第一组不能开始。

风险提示分两级：

- `warning`：接近风险上限但未超过。
- `blocked`：超过风险上限，不能开始模拟或不能进入当前组。

## 链路规划

链路由多个组组成，每组只包含两个账号：

```text
from_account -> to_account -> symbol -> position_side -> target_qty
```

第一组固定为：

```text
first_donor -> main
```

其中 `first_donor` 是盈利方向可平仓数量最高的子账号。

第一组完成后，系统进入确定性亏欠队列 planner。第一组会让 `first_donor` 的 `selected_side` 数量低于运行前基准，同时让主账号临时持有同方向仓位。账本推进只使用已配对数量，残差必须结构化记录：

```text
matched_qty = min(from_actual_closed_qty, to_actual_opened_qty)
close_residual_qty = from_actual_closed_qty - matched_qty
open_residual_qty = to_actual_opened_qty - matched_qty

main_buffer_qty += matched_qty
pending_debt_queue.push(first_donor, matched_qty)
for each nonzero residual:
  residual_ledger.append(account_id, side, leg_type, signed_qty, reason, event_id)
```

后续每一步从候选子账号里选择新的 donor，用该 donor 平掉盈利方向，并给 `pending_debt_queue` 中的接收账号开同方向仓位。每个实际动作仍然只生成一组账号：

```text
donor -> receiver
```

接收账号按 FIFO 顺序从 `pending_debt_queue` 头部选择。若队首缺口大于 donor 可转移数量，则本组只填队首的一部分。

如果 donor 可转移数量大于队首缺口，planner 可以生成 `donor_batch`。`donor_batch` 由同一个 donor 对多个 receiver 的多个两账号组组成，每个组仍然只包含一个 donor 和一个 receiver。批次内先按 FIFO 消化多个 receiver 缺口；每个成功组立即把 donor 的 `matched_qty` 累计到 `batch_debt_buffer`。只有整个批次完成后，才把 `batch_debt_buffer` 合并为 donor 的新待补回缺口并入队。

如果 `donor_batch` 中途失败，`batch_debt_buffer` 不能丢弃。已成功部分转为待修复缺口，进入 `round_pending_repair` 或 `needs_abort_recover` 路径，并写入审计记录。

`batch_debt_buffer` 至少包含：

```text
batch_id
donor_account_id
side
matched_qty
completed_group_ids
failed_group_id
repair_status
```

本组完成后，被接收账号的缺口按 `matched_qty` 减少。非批次组中，donor 按 `matched_qty` 新增同方向待补回缺口；批次组中，donor 只在批次结束后按累计 `batch_matched_qty` 入队。`pending_debt_queue` 的总量应始终等于主账号临时持有的 `main_buffer_qty` 加上按账号和腿可解释的残差、未合并的 `batch_debt_buffer`，直到闭合阶段由主账号回填最后剩余的缺口。

第一版禁止已经存在未偿还缺口的账号继续作为 donor，避免链路绕圈。只有当该账号在 `pending_debt_queue` 中的缺口被完全填平后，才允许重新进入 donor 候选池。

候选 donor 必须同时满足：

```text
selected_side is profitable
selected_side_closeable_qty > qty_tolerance
account has no pending debt
risk checks pass
estimated_net_release_profit > 0
no external order, lock, or snapshot conflict
```

候选 donor 的计划转移数量：

```text
candidate_transfer_qty = min(
  donor_closeable_profitable_qty,
  total_pending_debt_qty,
  donor_risk_executable_qty,
  receiver_receivable_qty,
  receiver_balance_capacity_qty
)
```

普通组的 `receiver_receivable_qty` 和 `receiver_balance_capacity_qty` 只取 FIFO 队首 receiver。`donor_batch` 必须按 FIFO receiver 逐段计算承接能力，每段数量为：

```text
batch_segment_qty = min(
  donor_remaining_batch_qty,
  receiver_pending_debt_qty,
  receiver_receivable_qty,
  receiver_balance_capacity_qty
)
```

每个 `batch_segment_qty` 都生成一个独立两账号 group。任一 receiver 无法承接时，batch 在该 receiver 前停止，不跳过队列项。

Planner 使用确定性的字典序评分选择下一个 donor：

```text
1. estimated_net_release_profit 更高
2. candidate_transfer_qty 更大
3. 能一次吃掉当前全部 pending_debt_queue 的账号优先
4. 能减少更多待补回账号数量的账号优先
5. estimated_round_count 更少
6. estimated_fee_cost + conservative_price_diff_loss 更低
7. donor_post_transfer_risk_buffer 更高
8. account_id 稳定排序兜底
```

其中：

```text
estimated_net_release_profit =
  candidate_transfer_qty * selected_side_unrealized_profit_per_qty
  - estimated_fee_cost
  - conservative_price_diff_loss
```

评分只用于生成链路，不改变轮次成功条件。

当没有符合条件的 donor、达到 `max_chain_groups`、继续转移预计净释放收益小于等于 0，或继续转移会触发风险阻断时，planner 进入闭合阶段：

```text
main -> remaining_receiver_1
main -> remaining_receiver_2
...
```

闭合阶段由主账号按 `pending_debt_queue` 顺序回填所有剩余缺口，直到主账号清空。

后续组在模拟开始前生成完整链路，但每组开始前必须重新读取这一组账号的最新状态，并按最新价格、余额、仓位、交易规则重新计算当前组是否仍可完整执行。

如果后续组因为状态变化无法完整执行，进入暂停状态，不自动缩量执行。

每组开始前重新计算时，只允许基于“已执行账本 + 剩余缺口”调整尚未执行的后续组顺序。如果当前组无法按原计划完整执行，进入 `paused_group_not_executable`；如果只是后续未执行组顺序发生变化，记录 `plan_adjusted`。

## 组内轮次

每组按交易对配置拆成多轮。每轮实际数量：

```text
round_qty = min(
  per_round_qty_limit,
  group_remaining_qty,
  from_account_current_closeable_qty,
  to_account_receivable_qty,
  to_account_balance_capacity_qty
)
```

计算后按交易所 step size 归一化，并检查最小名义金额、保证金占用、盈利覆盖摩擦成本和价格偏移。

组计划生成和组开始重算时必须检查轮数上限：

```text
required_rounds = ceil(group_target_qty / per_round_qty_limit)
required_rounds <= max_rounds_per_group
```

如果第一组超过 `max_rounds_per_group`，模拟不可开始，状态为 `blocked_group_round_limit_exceeded`。如果后续组超过上限，进入 `paused_group_round_limit_exceeded`，不自动拆成额外组。

每轮流程：

```text
1. 刷新 from/to 账号快照
2. 刷新价格和交易对规则
3. 计算 round_qty
4. from 账号复用现有单向平仓规则平掉盈利方向
5. to 账号复用现有单向开仓规则开启同方向
6. 对账实际平仓数量和实际开仓数量
7. 成功后更新组剩余数量并进入下一轮
```

本轮成功条件：

```text
abs(from_actual_closed_qty - to_actual_opened_qty) <= qty_tolerance
```

若不满足，暂停当前组和整条链路，状态为 `paused_round_unbalanced`。

部分成交按两类处理：

```text
双腿等量部分成交且差额 <= qty_tolerance:
  用 matched_qty 推进账本
  剩余未成交数量回到 group_remaining_qty

单腿部分成交、拒单、超时、残差超过容忍范围，或平仓成功但开仓未完成:
  进入 round_pending_repair
```

进入 `round_pending_repair` 后不得进入下一轮。修复优先使用账号间补偿链路；如果无法通过账号间补偿闭合，只能生成市场减仓建议，并等待单独确认。

## 配平阶段

盈利释放组全部完成后，进入全账号配平阶段。中间阶段允许子账号临时不配平，但最终子账号必须恢复运行前基准仓位。

配平优先账号间转移。系统先建立每个账号的目标账本：

```text
account_id
baseline_long_qty
baseline_short_qty
target_long_qty
target_short_qty
current_long_qty
current_short_qty
need_reduce_long_qty
need_reduce_short_qty
need_increase_long_qty
need_increase_short_qty
```

第一版以运行前账号仓位快照为最终目标账本。子账号目标是恢复到 `baseline_long_qty` 和 `baseline_short_qty`；主账号目标是本交易对 LONG/SHORT 都清空。基于目标账本建立失衡池：

```text
需要减少 LONG 的账号
需要减少 SHORT 的账号
需要增加 LONG 的账号
需要增加 SHORT 的账号
```

优先匹配账号间转移：

```text
A 减少 LONG  <->  B 增加 LONG
A 减少 SHORT <->  B 增加 SHORT
```

每个匹配仍然按组内轮次规则执行。若账号间无法闭合，生成市场减仓建议并进入 `needs_market_reduce_confirmation`，不自动执行。

每次账号间配平都必须满足全局账本不变量：

```text
matched_reduce_qty == matched_increase_qty
sum_account_long_delta and sum_account_short_delta remain explainable by executed events
```

也就是说，配平可以改变仓位所在账号，但不能在没有对应平仓/开仓事件的情况下扩大或缩小全局敞口。

主账号最终必须满足：

```text
abs(main_final_long_qty) <= qty_tolerance
abs(main_final_short_qty) <= qty_tolerance
```

否则不能标记为正常完成。

每个参与子账号最终必须满足：

```text
abs(final_long_qty - baseline_long_qty) <= qty_tolerance
abs(final_short_qty - baseline_short_qty) <= qty_tolerance
```

全局最小摩擦目标不作为第一版默认口径。第一版不改变子账号最终仓位规模，只释放盈利并恢复基准仓位。

如果最终残差小于或等于 `qty_tolerance`，视为闭合；如果残差大于 `qty_tolerance` 但低于交易所最小可下单数量或最小名义金额，标记为 `unsafe_dust_residual`，不能伪装成正常完成。

## 消耗统计

每轮记录：

```text
from_account
to_account
symbol
side
planned_qty
actual_close_qty
actual_open_qty
close_price
open_price
fee_close
fee_open
fee_asset
fee_rate
liquidity_role
entry_price
realized_pnl
pnl_asset
matched_qty
close_residual_qty
open_residual_qty
price_diff_pnl
price_diff_loss
total_round_cost
```

手续费消耗：

```text
fee_cost = close_fee + open_fee
```

手续费必须按 `fee_asset` 归集；如果手续费资产不是结算资产，报告中保留原始资产数量，并按参考价格折算为统一成本展示。`fee_rate` 和 `liquidity_role` 用于解释不同账号、maker/taker 或返佣导致的手续费差异。

移仓/配平价差损耗：

```text
if side == LONG:
  price_diff_pnl = (close_price - open_price) * matched_qty

if side == SHORT:
  price_diff_pnl = (open_price - close_price) * matched_qty

price_diff_loss = max(-price_diff_pnl, 0)
```

`price_diff_pnl` 保留方向和正负号；`price_diff_loss` 只统计不利价差。若价差有利，报告中作为价差收益展示，但不抵消手续费字段。

最终汇总：

```text
released_profit
transfer_fee_cost
rebalance_fee_cost
transfer_price_diff_pnl
rebalance_price_diff_pnl
transfer_price_diff_loss
rebalance_price_diff_loss
total_cost
net_profit_after_cost
```

报告必须区分移仓阶段和配平阶段的消耗。

`released_profit` 必须来自平仓腿事件的 `realized_pnl` 汇总，并保留 `pnl_asset`。如果模拟盘只能基于 `entry_price`、`close_price` 和 `matched_qty` 估算，报告必须标记为 estimated，并保留估算公式和输入价格。

## 状态机

正常状态：

```text
draft_plan
precheck
chain_ready
group_ready
round_simulated
group_completed
plan_adjusted
rebalance_ready
completed
```

阻断和暂停状态：

```text
blocked_main_insufficient_capacity
blocked_main_not_flat
blocked_no_profitable_account
blocked_manual_side_not_profitable
blocked_initial_subaccount_unbalanced
blocked_open_order_conflict
blocked_run_lock_exists
blocked_symbol_rules_unavailable
blocked_group_round_limit_exceeded
blocked_live_simulation_expired
blocked_live_precheck_failed
paused_price_drift
paused_round_unbalanced
paused_external_state_changed
paused_group_not_executable
paused_group_round_limit_exceeded
paused_lock_heartbeat_lost
round_pending_repair
needs_market_reduce_confirmation
needs_abort_recover
abort_recovering
aborted_recovered
unsafe_unclosed
unsafe_dust_residual
```

模拟结果分级：

```text
safe_closed
market_reduce_required
unsafe_unclosed
```

只有 `safe_closed` 可以作为未来实盘执行候选。需要市场减仓的结果标记为 `market_reduce_required`，必须单独确认；在没有人工确认和执行前，不得使用“closed”命名。

`aborted_recovered` 只表示锁、遗留挂单和可解释账本已经通过人工恢复流程处理完毕，不等于 `safe_closed`，不能作为未来实盘候选。进入 `aborted_recovered` 后若仍要继续亢龙流程，必须重新读取快照并生成新的模拟计划。

## 未来实盘准入

未来实盘候选必须同时满足：

```text
simulation_result == safe_closed
now <= simulation_expires_at
plan_version unchanged
config_version unchanged
account_set unchanged
symbol_rule_version unchanged
fresh_live_precheck == passed
operator_live_confirmation == true
```

实盘启动前必须重新读取账号快照、价格、交易对规则、保证金状态和运行锁。模拟结果过期、快照超过 `snapshot_ttl_ms`、价格超过 `price_ttl_ms`、或重新预检失败时，不能进入实盘候选状态。

模拟通过不能自动触发实盘执行。旧模拟结果只能作为参考，不能直接武装实盘。

## 审计与快照

每次模拟必须保留可回放审计记录：

```text
run_id
plan_version
config_version
account_snapshot_version
symbol_rule_version
price_source
simulation_expires_at
live_precheck_run_id
lock_scope
lock_heartbeat_history
residual_ledger
batch_debt_buffer_history
abort_recover_history
other_side_preview
rounding_ledger
event_sequence
operator_choice
```

每个组和每轮都引用同一个 `run_id`，并记录输入快照版本和输出事件。报告必须能从最终结果追溯到每一轮的计划数量、实际成交数量、成交均价、手续费、残差、配对订单、锁续期、暂停原因和实盘准入判断。

`batch_debt_buffer_history` 记录每个 donor batch 的成功 group、失败点和转入待修复缺口的数量。`abort_recover_history` 必须记录操作者、恢复前快照、恢复后快照、处理动作、释放锁原因和确认时间。`rounding_ledger` 是 `residual_ledger` 的子集，用于专门追踪 step size 向下取整产生的数量差额。

## 复用现有规则

亢龙模块只负责跨账号规划、调度、对账和统计。底层交易规则复用现有能力：

- 转出账号释放盈利方向：复用单向平仓规则。
- 转入账号承接同方向仓位：复用单向开仓规则。
- 账号间配平：复用单向平仓和单向开仓规则。
- 必要时减仓配平：生成建议，未来复用单向平仓规则，但必须单独确认。
- 价格、最小下单金额、杠杆、余额、系统挂单、Market fallback 和残差处理沿用现有预检和执行规则。

## 测试重点

- 起始账号选择：全局 `selected_side` 确定后，选择该方向可平仓数量最高的盈利子账号。
- 自动方向先选全局 `selected_side`，后续起始账号和 donor 都只能扫描该方向。
- 自动方向报告展示 `other_side_preview`，但第一版不自动切换到备选方向。
- `other_side_preview` 不生成执行链路、不占用运行锁、不改变 `plan_version`。
- 手动方向选择：只扫描指定方向，方向不盈利时阻断。
- 手动方向选择不能绕过释放数量、主账号承接和风险阻断规则。
- 初始子账号未双向配平时阻断为 `blocked_initial_subaccount_unbalanced`。
- 主账号初始本交易对非空仓时阻断为 `blocked_main_not_flat`。
- 主账号承接不足：检测阶段阻断并给出保证金缺口。
- 主账号承接能力同时覆盖临时数量上限、名义风险比例、保证金、手续费、价格漂移缓冲和爆仓距离。
- 运行锁按逐仓、全仓或共享保证金模式选择正确范围。
- 运行锁 heartbeat 正常续期，heartbeat 丢失进入 `paused_lock_heartbeat_lost`。
- 链路中暂停且账本未闭合时不能释放运行锁，必须通过人工 abort/recover 后释放。
- 确定性 planner 按评分规则选择后续 donor，并用 account_id 稳定排序兜底。
- 亏欠队列按 FIFO 消化，已有未偿还缺口的账号不能作为 donor。
- donor 可一次覆盖多个 receiver 时生成 `donor_batch`，每个成功 group 累计到 `batch_debt_buffer`，批次结束后再把 donor 累计缺口入队。
- `donor_batch` 中途失败时，`batch_debt_buffer` 转为可审计待修复缺口。
- `batch_debt_buffer` 必须记录 donor、side、batch、matched 数量、成功组、失败组和修复状态。
- Planner 计算候选数量时必须同时考虑 donor 可平仓能力和 receiver 承接能力。
- Batch 覆盖多个 receiver 时，每个 receiver 的承接能力单独限制对应 group 数量，且不能跳过 FIFO 队首。
- 亏欠队列链路用 `matched_qty` 推进，结构化残差进入 `residual_ledger`。
- 亏欠队列链路保持 `pending_debt_queue` 总量等于 `main_buffer_qty` 加可解释残差。
- Planner 到达 `max_chain_groups`、无正收益 donor 或风险阻断时进入闭合阶段。
- 每组只能涉及两个账号。
- 组内按交易对数量上限拆轮。
- 第一组超过 `max_rounds_per_group` 时阻断，后续组超过时暂停。
- 每轮不配平时暂停，不进入下一轮。
- 双腿等量部分成交用 `matched_qty` 推进，单腿部分成交、拒单和超时进入 `round_pending_repair`。
- 中间允许子账号临时不配平，但最终必须恢复运行前基准仓位。
- 主账号最终必须清空。
- 子账号恢复基准、主账号最终清空和全局账本不变量必须同时成立。
- 残差超过容忍范围但低于最小下单规则时标记 `unsafe_dust_residual`。
- 所有数量使用 Decimal，下单数量按 step size 向下取整，rounding 差额进入 `residual_ledger` 和 `rounding_ledger`。
- 价格使用 Decimal 并按 tick size 归一化，价格取整差异只进入成本估算和价差统计。
- 外部挂单、手动调仓、运行锁和快照版本异常会阻断或暂停。
- 未闭合暂停进入 `needs_abort_recover`，人工恢复过程中为 `abort_recovering`，确认恢复后记录 `aborted_recovered`。
- `aborted_recovered` 不能作为未来实盘候选，继续流程必须重新生成模拟计划。
- 账号间无法闭合时只生成市场减仓建议。
- 消耗统计正确区分 released profit、手续费、手续费资产、maker/taker、带方向的价差 PnL 和保守价差损耗，并区分移仓阶段和配平阶段。
- 模拟执行事件覆盖部分成交、拒单、超时、Market fallback、realized PnL、手续费、手续费资产、成交明细、matched 数量、残差、交易规则不可用、配对腿 ID 和订单 ID。
- 模拟和未来实盘使用同一套规划与风控接口，仅执行模式不同。
- 实盘候选必须校验模拟结果未过期、重新预检通过，并由操作员单独确认。
