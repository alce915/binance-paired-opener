# 亢龙有悔移仓模拟模块设计

## 背景

现有系统已经具备多账号配置、单账号运行时、双向开仓、双向平仓、单向开仓、单向平仓、模拟盘和实盘两套执行模式。亢龙有悔模块不重新实现交易底层，而是在现有规则之上增加跨账号链路规划、逐组逐轮调度、风险阻断、模拟报告和消耗统计。

第一版只做模拟盘。模拟盘稳定后，实盘不新增另一套策略逻辑，只切换执行模式，保持和当前系统模拟盘/实盘机制一致。

## 目标

- 在 1 个主账号和多个子账号之间模拟完整移仓链路。
- 起始账号默认选择盈利方向可平仓数量最高的子账号。
- 第一组必须由起始子账号向主账号转移盈利方向的全部可平仓数量。
- 每次只允许一组账号参与转移。
- 每组按交易对配置的仓位数量上限分轮执行。
- 每一轮必须完成平仓、开仓、成交对账后才能进入下一轮。
- 中间阶段允许子账号临时不配平，但最终必须完成所有子账号配平。
- 主账号最终必须清空本交易对仓位，作为纯中转账户。
- 最终配平优先账号间转移；只有无法闭合时才生成市场减仓建议，且必须单独确认。
- 全流程统计手续费消耗和移仓/配平价差损耗。

## 非目标

- 第一版不真实下单。
- 第一版不支持手动设置释放数量。
- 第一版不允许起始账号部分释放。
- 第一版不自动执行市场减仓。
- 第一版不并发执行多组账号转移。

## 核心概念

### 账号角色

- 主账号：纯中转账户。可以临时承接仓位，但最终 LONG/SHORT 必须都清空到交易对数量容忍范围内。
- 子账号：盈利释放和最终配平对象。中间可以临时不配平，最终必须自身 LONG/SHORT 配平。

### 盈利方向

默认自动选择未实现盈利金额更多的方向。若 LONG 和 SHORT 都盈利，优先选择盈利金额更高的方向；如果盈利金额相同，再选择可平仓数量更大的方向。

策略允许用户手动选择 LONG 或 SHORT。手动方向仍必须盈利，否则检测阶段阻断。

### 起始账号

自动方向下，先为每个子账号选择盈利更多的方向，再按该方向可平仓数量从大到小排序。手动方向下，只扫描指定方向盈利的子账号，并按该方向可平仓数量排序。

起始账号为排序第一的子账号。计划释放数量为该账号该盈利方向的全部可平仓数量。

## 配置

按交易对配置组内单轮数量上限和容忍误差。

```json
{
  "ETHUSDC": {
    "per_round_qty_limit": "0.05",
    "qty_tolerance": "0.0001",
    "max_rounds_per_group": 30,
    "max_main_temp_qty": "1.50",
    "max_main_temp_notional_ratio": "0.80"
  }
}
```

`per_round_qty_limit` 是每一轮最多转移的标的数量。金额、保证金、最小名义金额和盈利覆盖成本作为风险检查，不作为主拆分单位。

## 预检与风险阻断

模拟开始前读取所有参与账号快照，包括余额、可用余额、LONG/SHORT 数量、未实现盈亏、杠杆、交易对规则、参考价格和系统挂单状态。

第一组有硬阻断规则：

```text
主账号可承接数量 >= 起始账号盈利方向全部可平仓数量
```

如果主账号因为临时仓位上限、名义风险比例或保证金不足只能承接一部分，则模拟不可开始，状态为 `blocked_main_insufficient_capacity`。报告必须提示起始账号、盈利方向、计划释放数量、主账号可承接数量、缺口数量、预计所需保证金、当前可用保证金和建议补充保证金。

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

后续组在模拟开始前生成完整链路，但每组开始前必须重新读取这一组账号的最新状态，并按最新价格、余额、仓位、交易规则重新计算当前组是否仍可完整执行。

如果后续组因为状态变化无法完整执行，进入暂停状态，不自动缩量执行。

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

## 配平阶段

盈利释放组全部完成后，进入全账号配平阶段。中间阶段允许子账号临时不配平，但最终子账号必须自身 LONG/SHORT 配平。

配平优先账号间转移。系统建立失衡池：

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

主账号最终必须满足：

```text
abs(main_final_long_qty) <= qty_tolerance
abs(main_final_short_qty) <= qty_tolerance
```

否则不能标记为正常完成。

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
price_diff_loss
total_round_cost
```

手续费消耗：

```text
fee_cost = close_fee + open_fee
```

移仓/配平价差损耗：

```text
price_diff_loss = abs(open_price - close_price) * matched_qty
```

最终汇总：

```text
released_profit
transfer_fee_cost
rebalance_fee_cost
transfer_price_diff_loss
rebalance_price_diff_loss
total_cost
net_profit_after_cost
```

报告必须区分移仓阶段和配平阶段的消耗。

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
blocked_no_profitable_account
blocked_manual_side_not_profitable
blocked_open_order_conflict
blocked_symbol_rules_unavailable
paused_price_drift
paused_round_unbalanced
paused_group_not_executable
needs_market_reduce_confirmation
unsafe_unclosed
```

模拟结果分级：

```text
safe_closed
closed_with_manual_market_reduce_required
unsafe_unclosed
```

只有 `safe_closed` 可以作为未来实盘执行候选。需要市场减仓的结果必须单独确认。

## 复用现有规则

亢龙模块只负责跨账号规划、调度、对账和统计。底层交易规则复用现有能力：

- 转出账号释放盈利方向：复用单向平仓规则。
- 转入账号承接同方向仓位：复用单向开仓规则。
- 账号间配平：复用单向平仓和单向开仓规则。
- 必要时减仓配平：生成建议，未来复用单向平仓规则，但必须单独确认。
- 价格、最小下单金额、杠杆、余额、系统挂单、Market fallback 和残差处理沿用现有预检和执行规则。

## 测试重点

- 起始账号选择：自动方向下选盈利方向可平仓数量最高的子账号。
- 手动方向选择：只扫描指定方向，方向不盈利时阻断。
- 主账号承接不足：检测阶段阻断并给出保证金缺口。
- 每组只能涉及两个账号。
- 组内按交易对数量上限拆轮。
- 每轮不配平时暂停，不进入下一轮。
- 中间允许子账号临时不配平，但最终账本必须闭合。
- 主账号最终必须清空。
- 账号间无法闭合时只生成市场减仓建议。
- 消耗统计正确区分手续费和价差损耗，并区分移仓阶段和配平阶段。
- 模拟和未来实盘使用同一套规划与风控接口，仅执行模式不同。
