from __future__ import annotations

import json
from decimal import Decimal

import httpx
import typer

from paired_opener.config import settings
from paired_opener.domain import TrendBias

app = typer.Typer(help="CLI for Binance paired opening sessions.")


def _base_url() -> str:
    return f"http://{settings.api_host}:{settings.api_port}"


@app.command("create")
def create_session(symbol: str, trend_bias: TrendBias, leverage: int, round_count: int, round_qty: Decimal) -> None:
    response = httpx.post(
        f"{_base_url()}/sessions/open",
        json={
            "symbol": symbol,
            "trend_bias": trend_bias.value,
            "leverage": leverage,
            "round_count": round_count,
            "round_qty": str(round_qty),
        },
        timeout=10.0,
    )
    response.raise_for_status()
    typer.echo(json.dumps(response.json(), indent=2, ensure_ascii=False))


@app.command("list")
def list_sessions() -> None:
    response = httpx.get(f"{_base_url()}/sessions", timeout=10.0)
    response.raise_for_status()
    typer.echo(json.dumps(response.json(), indent=2, ensure_ascii=False))


@app.command("status")
def session_status(session_id: str) -> None:
    response = httpx.get(f"{_base_url()}/sessions/{session_id}", timeout=10.0)
    response.raise_for_status()
    typer.echo(json.dumps(response.json(), indent=2, ensure_ascii=False))


@app.command("pause")
def pause_session(session_id: str) -> None:
    response = httpx.post(f"{_base_url()}/sessions/{session_id}/pause", timeout=10.0)
    response.raise_for_status()
    typer.echo(response.text)


@app.command("resume")
def resume_session(session_id: str) -> None:
    response = httpx.post(f"{_base_url()}/sessions/{session_id}/resume", timeout=10.0)
    response.raise_for_status()
    typer.echo(response.text)


@app.command("abort")
def abort_session(session_id: str) -> None:
    response = httpx.post(f"{_base_url()}/sessions/{session_id}/abort", timeout=10.0)
    response.raise_for_status()
    typer.echo(response.text)
