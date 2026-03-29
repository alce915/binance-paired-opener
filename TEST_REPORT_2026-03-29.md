# 测试报告 - 2026-03-29

## 概览
- 项目：币安自动开单系统
- 测试日期：2026-03-29
- 测试目标：
  - 验证四模式开仓/平仓主链路与阻断链路
  - 验证统一预检、模拟执行、连接/断开、账户切换后的规则一致性
  - 验证近期修复的前端节流、交易对切换、账户切换 SSE 时序等问题

## 测试环境
- 工作目录：`D:\codex\币安自动开单系统`
- 本地服务地址：`http://127.0.0.1:8000/`
- Python 虚拟环境：`.venv`
- 当前本地 Git 分支：`main`
- 当前本地最新提交：`b07de46 public backup: 2026-03-29 22:53:53`

## 自动化回归结果
### 1. 单元/集成测试
执行命令：
```powershell
& '.\.venv\Scripts\python.exe' -m pytest -q
```
结果：
- `64 passed in 47.31s`

### 2. 编译检查
执行命令：
```powershell
& 'D:\codex\币安自动开单系统\.venv\Scripts\python.exe' -m compileall paired_opener
```
结果：
- 通过

## 服务健康检查
验证结果：
- `GET /` -> `200`
- `GET /config/accounts` -> `200`
- `POST /market/connect` -> 正常返回 `connecting`
- `POST /market/disconnect` -> 正常返回 `disconnected`

## 四模式模拟测试
统一模拟接口：`POST /simulation/run`

### 主路径结果
- `paired_open`：完成
- `paired_close`：完成
- `single_open`：完成
- `single_close`：完成

### 代表性返回
- `paired_open`
  - `status=completed`
  - `rounds_total=2`
  - `total_notional=5000`
  - `notional_per_round=2500`
- `paired_close`
  - `status=completed`
  - `symbol=ETHUSDC`
  - `rounds_total=2`
- `single_open`
  - `status=completed`
  - `selected_position_side=LONG`
- `single_close`
  - `status=completed`
  - `selected_position_side=LONG`

## 阻断场景测试
### 双向开仓
- 每轮开单金额低于最小下单金额：正确拦截
- 开仓总金额超过可用余额 95%：正确拦截
- 边界复核：95% 规则本身正常，早期异常来自使用了过期的余额上限值

### 双向平仓
- 平仓数量超过 `min(long_qty, short_qty)`：正确拦截
- 每轮平仓金额低于最小下单金额：正确拦截
- 无双边持仓：正确拦截

### 单向开仓
- `regular` 成功路径：正常
- `align` 且双边已对齐：正确拦截
- 已有持仓时杠杆不一致：正确拦截
- 最小下单金额不足：正确拦截

### 单向平仓
- 当前交易对不存在持仓：正确拦截
- `align` 且双边已对齐：正确拦截
- 每轮平仓金额低于最小下单金额：正确拦截
- 平仓数量超过所选持仓数量：正确拦截

## 切账户 / 切交易对 / 预检一致性测试
### 切账户后四模式预检
在 `main / sub1 / sub2` 三个账户下，分别对 `BTCUSDT / ETHUSDC` 做了预检抽样，结果如下：
- 切账户后预检口径保持一致，没有测出“切账户后规则漂移”
- `single_open`、`single_close`、`paired_close` 在不同账户下结果与持仓和交易对规则一致
- `paired_open` 在完整参数下也表现稳定：
  - `BTCUSDT round_qty=0.01` -> 通过
  - `BTCUSDT round_qty=0.001` -> 稳定拦截在最小下单金额

### 切交易对问题修复后验证
已修复：
- 切交易对时“新交易对 + 旧规则”的短暂错配
当前实现：
- 通过原子应用 symbol 上下文，一次性更新 `activeSymbol + currentSymbolInfo`
- 然后再统一重算和预检

### context_stale 调度修复
已修复：
- 上下文变化但价格未超阈值、时间未到 10 秒时，前端不再一直沿用旧结果
当前实现：
- `context_stale` 现在会触发一次静默重验
- 不先闪掉提示，不先锁按钮

### 账户切换 SSE 时序修复
已修复：
- 当连接开关关闭时，切账户不再重建 SSE 连接
当前实现：
- 仅当 `shouldReconnect=true` 时才执行 `closeSse(); openSse();`

## 真实创建前严格拦截
已验证：
- 四个真实创建入口在创建前仍会执行严格校验
- 包括：
  - Hedge Mode 最终确认
  - 最小下单金额
  - 95% 可用余额限制（开仓类）
  - 白名单
  - 持仓存在性
  - 杠杆限制
  - 系统挂单冲突

说明：
- 通过故意构造非法参数请求真实创建接口，确认会被拦截，不会误创建真实会话
- 当前 PowerShell 对 422 响应体抓取不完整，因此这轮主要确认的是“已被业务拦截”，而不是错误 JSON 的完整文本

## 本轮已修复的重要问题
- `context_stale` 不触发预检
- 账户切换在连接开关关闭时仍会重建 SSE
- 切换交易对时存在“新交易对 + 旧规则”的错配
- 四模式模拟执行统一接口已扩展完成
- Hedge Mode 提示时机收口为“只在创建真实会话时严格提示”

## 当前仍建议人工确认的前端体验项
以下项目更适合浏览器内人工点测确认：
- 连接开关关闭时切账户，页面是否完全没有额外抖动
- 切换交易对时，提示栏与金额卡片是否还存在瞬时双跳
- 四模式在价格轻微波动时，`0.3% / 10秒` 节流是否符合预期体感

## Git 同步状态
本地 Git 状态：
- 已完成本地提交
- 本地最新提交：`b07de46 public backup: 2026-03-29 22:53:53`
- 当前相对远程状态：本地领先远程

远程 GitHub 同步状态：
- 使用了 `git-sync` 的共享账号配置：`alce915`
- 推送失败原因不是鉴权，而是网络：
  - `Failed to connect to github.com port 443`
  - `Couldn't connect to server`

结论：
- 代码已安全提交到本地 Git
- GitHub 远程仓库尚未更新
- 网络恢复后可再次执行 `git-sync` 或 `git push` 完成同步

## 总结
本轮测试结果显示：
- 后端四模式统一预检、模拟执行和真实创建前严格拦截总体稳定
- 自动化回归全部通过
- 近期修复的关键前端问题已完成落地
- 当前剩余风险主要集中在前端交互体验层，建议通过一次人工浏览器点测做最终确认
