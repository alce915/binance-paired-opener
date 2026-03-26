# Binance Paired Opener

基于 Binance USD-M Futures 的交易控制台与执行服务。

当前项目包含两套独立入口：
- 开单控制台与交易 API，默认端口 `8000`
- 账户监控控制台与监控 API，默认端口 `8010`

## 安装

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e .[dev]
```

## 主要功能

- 双向开仓
- 双向平仓
- 单向开仓
- 单向平仓
- Binance 账户概览与当前持仓展示
- 订单簿、SSE 推送、模拟执行
- 多账户切换

## 启动开单服务

命令行：

```powershell
paired-opener-api
```

脚本：

```powershell
scripts\restart_service.bat
```

访问地址：
- 控制台：`http://127.0.0.1:8000/`
- OpenAPI：`http://127.0.0.1:8000/docs`

## 启动账户监控服务

命令行：

```powershell
paired-opener-monitor-api
```

脚本：

```powershell
scripts\restart_monitor_service.bat
```

访问地址：
- 监控控制台：`http://127.0.0.1:8010/`
- 健康检查：`http://127.0.0.1:8010/healthz`
- 账户快照：`http://127.0.0.1:8010/api/accounts`
- SSE：`http://127.0.0.1:8010/stream/accounts`

## 配置说明

### 开单系统账户配置

公开仓库中只保留模板文件：

- `config\binance_api.env`

请在本机私有环境中填写真实值，不要把真实 API Key / Secret 提交到 Git。

### 监控系统账户配置

监控子项目支持单独的配置文件：

- 示例：`config\binance_accounts.example.json`
- 本地私有配置：`config\binance_accounts.json`

其中 `config\binance_accounts.json` 已被 `.gitignore` 排除，不会进入公开仓库。

## 常用 CLI 示例

```powershell
paired-opener create BTCUSDT long 50 3 0.001
paired-opener list
```

## 测试

```powershell
python -m pytest -q
```

## 常见排查

- `8000` 控制台无法访问：先执行 `scripts\restart_service.bat`
- `8010` 控制台无法访问：先执行 `scripts\restart_monitor_service.bat`
- 监控接口异常：先访问 `http://127.0.0.1:8010/healthz`
- Binance 主网接口返回 `451`：通常是当前出口 IP 被地区限制拦截

## 注意

- 本系统不保证“不爆仓”，只负责按策略执行、数量对齐与审计记录
- 公开仓库中的配置文件均为模板，真实账户密钥应在本地私有环境中维护
- 如果历史上真实 Binance API Key / Secret 已进入过本地文件，建议立即轮换
