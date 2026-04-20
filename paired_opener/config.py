from __future__ import annotations

import json
import re
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = PROJECT_ROOT / "data"
ENV_FILES = (
    CONFIG_DIR / "binance_api.env",
    CONFIG_DIR / "binance_api.local.env",
    PROJECT_ROOT / ".env",
)


@dataclass(frozen=True, slots=True)
class AccountConfig:
    account_id: str
    name: str
    api_key: str
    api_secret: str
    use_testnet: bool = False
    rest_base_url: str = ""
    ws_base_url: str = ""

    @property
    def effective_rest_base_url(self) -> str:
        if self.rest_base_url:
            return self.rest_base_url
        if self.use_testnet:
            return "https://testnet.binancefuture.com"
        return "https://fapi.binance.com"

    @property
    def effective_websocket_base_url(self) -> str:
        if self.ws_base_url:
            return self.ws_base_url
        if self.use_testnet:
            return "wss://stream.binancefuture.com/ws"
        return "wss://fstream.binance.com/ws"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=ENV_FILES,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "Binance Paired Opener"
    api_host: str = "127.0.0.1"
    api_port: int = 8000
    monitor_app_name: str = "Binance Account Monitor"
    monitor_api_host: str = "127.0.0.1"
    monitor_api_port: int = 8010
    data_dir: Path = DATA_DIR
    database_path: Path = DATA_DIR / "paired_opener.db"

    binance_api_key: str = ""
    binance_api_secret: str = ""
    binance_use_testnet: bool = False
    binance_accounts: str = ""
    binance_accounts_file: Path = CONFIG_DIR / "binance_accounts.json"
    binance_rest_base_url: str = ""
    binance_ws_base_url: str = ""
    binance_recv_window_ms: int = 5_000
    monitor_refresh_interval_ms: int = 5_000
    market_account_refresh_interval_ms: int = 5_000
    monitor_history_window_days: int = 7
    monitor_history_refresh_interval_ms: int = 60_000
    active_account_file: Path = CONFIG_DIR / "active_account.json"
    symbol_whitelist: list[str] = Field(default_factory=lambda: ["BTCUSDT", "ETHUSDT"])
    symbol_whitelist_file: Path = CONFIG_DIR / "symbol_whitelist.json"
    session_event_retention_days: int = 30
    session_event_retention_per_session: int = 2_000
    runtime_log_max_bytes: int = 20 * 1024 * 1024
    runtime_log_backup_count: int = 5
    frontend_execution_log_lines: int = 200
    sse_queue_maxsize: int = 100

    default_poll_interval_ms: int = 50
    default_order_ttl_ms: int = 3_000
    default_max_zero_fill_retries: int = 10
    default_market_fallback_attempts: int = 3
    default_open_execution_profile: str = "maker_first"
    default_close_execution_profile: str = "balanced"
    maker_first_market_fallback_max_ratio: Decimal = Decimal("0.25")
    maker_first_market_fallback_min_residual_qty: Decimal = Decimal("0")
    maker_first_max_reprice_ticks: int = 3
    maker_first_max_spread_bps: int = 8
    maker_first_max_reference_deviation_bps: int = 15
    balanced_market_fallback_max_ratio: Decimal = Decimal("1")
    balanced_market_fallback_min_residual_qty: Decimal = Decimal("0")
    balanced_max_reprice_ticks: int = 8
    balanced_max_spread_bps: int = 20
    balanced_max_reference_deviation_bps: int = 40

    accounts: dict[str, AccountConfig] = Field(default_factory=dict)
    active_account_id: str = "default"

    @property
    def rest_base_url(self) -> str:
        return self.active_account.effective_rest_base_url

    @property
    def websocket_base_url(self) -> str:
        return self.active_account.effective_websocket_base_url

    @property
    def normalized_whitelist(self) -> set[str]:
        return {symbol.upper() for symbol in self.symbol_whitelist}

    def load_persisted_whitelist(self) -> None:
        path = self.symbol_whitelist_file
        if not path.exists():
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError):
            return
        if not isinstance(payload, list):
            return
        normalized = [str(symbol).strip().upper() for symbol in payload if str(symbol).strip()]
        if normalized:
            self.symbol_whitelist = list(dict.fromkeys(normalized))

    def persist_whitelist(self, symbols: list[str]) -> list[str]:
        normalized = list(dict.fromkeys(symbol.strip().upper() for symbol in symbols if symbol.strip()))
        if not normalized:
            raise ValueError("Whitelist cannot be empty")
        self.symbol_whitelist = normalized
        self.symbol_whitelist_file.parent.mkdir(parents=True, exist_ok=True)
        self.symbol_whitelist_file.write_text(
            json.dumps(normalized, ensure_ascii=True, indent=2),
            encoding="utf-8",
        )
        return normalized

    def _load_env_values(self) -> dict[str, str]:
        values: dict[str, str] = {}
        for candidate in ENV_FILES:
            if not candidate.exists():
                continue
            for raw_line in candidate.read_text(encoding="utf-8-sig").splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip()
        return values

    def _as_bool(self, value: str | bool | None, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return default
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    def load_accounts(self, *, include_accounts_file: bool = False) -> None:
        env_values = self._load_env_values()
        accounts = self._load_accounts_from_file() if include_accounts_file else []
        if not accounts:
            configured_accounts = env_values.get("BINANCE_ACCOUNTS", self.binance_accounts)
            account_ids = [item.strip().lower() for item in configured_accounts.split(",") if item.strip()]
            seen_ids: set[str] = set()
            if account_ids:
                for account_id in account_ids:
                    prefix = f"BINANCE_ACCOUNT_{account_id.upper()}_"
                    api_key = env_values.get(f"{prefix}API_KEY", "")
                    api_secret = env_values.get(f"{prefix}API_SECRET", "")
                    if not api_key or not api_secret:
                        continue
                    accounts.append(
                        AccountConfig(
                            account_id=account_id,
                            name=env_values.get(f"{prefix}NAME", account_id),
                            api_key=api_key,
                            api_secret=api_secret,
                            use_testnet=self._as_bool(
                                env_values.get(f"{prefix}USE_TESTNET"),
                                self.binance_use_testnet,
                            ),
                            rest_base_url=env_values.get(f"{prefix}REST_BASE_URL", ""),
                            ws_base_url=env_values.get(f"{prefix}WS_BASE_URL", ""),
                        )
                    )
                    seen_ids.add(account_id)
            if not accounts:
                prefix_ids = sorted(
                    {
                        match.group(1).lower()
                        for key in env_values
                        for match in [re.match(r"BINANCE_ACCOUNT_([A-Z0-9_]+)_API_KEY$", key)]
                        if match is not None
                    }
                )
                for account_id in prefix_ids:
                    if account_id in seen_ids:
                        continue
                    prefix = f"BINANCE_ACCOUNT_{account_id.upper()}_"
                    api_key = env_values.get(f"{prefix}API_KEY", "")
                    api_secret = env_values.get(f"{prefix}API_SECRET", "")
                    if not api_key or not api_secret:
                        continue
                    accounts.append(
                        AccountConfig(
                            account_id=account_id,
                            name=env_values.get(f"{prefix}NAME", account_id),
                            api_key=api_key,
                            api_secret=api_secret,
                            use_testnet=self._as_bool(
                                env_values.get(f"{prefix}USE_TESTNET"),
                                self.binance_use_testnet,
                            ),
                            rest_base_url=env_values.get(f"{prefix}REST_BASE_URL", ""),
                            ws_base_url=env_values.get(f"{prefix}WS_BASE_URL", ""),
                        )
                    )
                    seen_ids.add(account_id)
        if not accounts:
            accounts = [
                AccountConfig(
                    account_id="default",
                    name=env_values.get("BINANCE_ACCOUNT_NAME", "默认账户"),
                    api_key=self.binance_api_key,
                    api_secret=self.binance_api_secret,
                    use_testnet=self.binance_use_testnet,
                    rest_base_url=self.binance_rest_base_url,
                    ws_base_url=self.binance_ws_base_url,
                )
            ]
        self.accounts = {account.account_id: account for account in accounts}
        self.active_account_id = self._load_active_account_id()
        if self.active_account_id not in self.accounts:
            self.active_account_id = accounts[0].account_id
            self.persist_active_account(self.active_account_id)

    def _load_accounts_from_file(self) -> list[AccountConfig]:
        path = self.binance_accounts_file
        if not path.exists():
            return []
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError):
            return []
        raw_accounts = payload.get("accounts") if isinstance(payload, dict) else payload
        if not isinstance(raw_accounts, list):
            return []
        accounts: list[AccountConfig] = []
        seen: set[str] = set()
        for item in raw_accounts:
            if not isinstance(item, dict):
                continue
            account_id = str(item.get("account_id") or item.get("id") or "").strip().lower()
            api_key = str(item.get("api_key") or "").strip()
            api_secret = str(item.get("api_secret") or "").strip()
            if not account_id or not api_key or not api_secret or account_id in seen:
                continue
            accounts.append(
                AccountConfig(
                    account_id=account_id,
                    name=str(item.get("name") or account_id).strip() or account_id,
                    api_key=api_key,
                    api_secret=api_secret,
                    use_testnet=self._as_bool(item.get("use_testnet"), self.binance_use_testnet),
                    rest_base_url=str(item.get("rest_base_url") or "").strip(),
                    ws_base_url=str(item.get("ws_base_url") or "").strip(),
                )
            )
            seen.add(account_id)
        return accounts

    def _load_active_account_id(self) -> str:
        if not self.active_account_file.exists():
            return next(iter(self.accounts), "default")
        try:
            payload = json.loads(self.active_account_file.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError):
            return next(iter(self.accounts), "default")
        account_id = str(payload.get("account_id", "")).strip().lower()
        return account_id or next(iter(self.accounts), "default")

    def persist_active_account(self, account_id: str) -> None:
        normalized = account_id.strip().lower()
        if normalized not in self.accounts:
            raise ValueError(f"Unknown account {account_id}")
        self.active_account_id = normalized
        self.active_account_file.parent.mkdir(parents=True, exist_ok=True)
        self.active_account_file.write_text(
            json.dumps({"account_id": normalized}, ensure_ascii=True, indent=2),
            encoding="utf-8",
        )

    @property
    def active_account(self) -> AccountConfig:
        return self.accounts[self.active_account_id]


settings = Settings()
settings.load_persisted_whitelist()
settings.load_accounts()
if not settings.symbol_whitelist_file.exists():
    settings.persist_whitelist(settings.symbol_whitelist)


