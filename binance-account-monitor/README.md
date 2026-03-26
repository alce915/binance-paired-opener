# binance-account-monitor

Independent Binance unified account monitor web service.

## Features

- Separate deployment from the paired opener project
- Hierarchical monitor configuration: main account -> child accounts
- Unified account snapshot aggregation
- REST API + SSE streaming updates
- Standalone monitor web console

## Setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e .[dev]
Copy-Item .env.example .env
Copy-Item config\binance_monitor_accounts.example.json config\binance_monitor_accounts.json
```

## Run

```powershell
binance-account-monitor
```

Or use:

```powershell
scripts\restart_monitor_service.bat
```

## Endpoints

- `GET /healthz`
- `GET /api/monitor/summary`
- `GET /api/monitor/groups`
- `GET /api/monitor/accounts`
- `GET /stream/monitor`

## Config

The service reads child account API credentials from:

- `config/binance_monitor_accounts.json`

Main accounts are grouping nodes only. Child accounts carry the actual Binance API credentials.