from __future__ import annotations

import re
import subprocess
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"


def _read_script(name: str) -> str:
    return (SCRIPTS_DIR / name).read_text(encoding="ascii")


def test_start_service_waits_for_elevated_process() -> None:
    script = _read_script("start_service.ps1")

    assert re.search(r"Start-Process\s+-FilePath\s+'powershell\.exe'.*-Verb\s+RunAs.*-Wait.*-PassThru", script, re.DOTALL)
    assert re.search(r"exit\s+\$\w+\.ExitCode", script)


def test_service_launcher_scripts_are_tracked_by_git() -> None:
    for relative_path in ("scripts/start_service.ps1", "scripts/run_service_console.ps1"):
        result = subprocess.run(
            ["git", "ls-files", "--error-unmatch", relative_path],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, f"{relative_path} must be tracked so launch scripts work on a clean checkout"


def test_start_service_netstat_fallback_matches_listening_rows() -> None:
    script = _read_script("start_service.ps1")

    assert 'Select-String -Pattern ":$port\\s+\\S+\\s+LISTENING"' in script


def test_stop_service_does_not_swallow_health_check_failure() -> None:
    script = _read_script("stop_service.ps1")

    assert '$healthResponds = $false' in script
    assert 'throw "Service stop failed: health endpoint still responds on port $port"' in script
    assert not re.search(
        r"try\s*\{\s*& curl\.exe[\s\S]*throw \"Service stop failed: health endpoint still responds on port \$port\"[\s\S]*\}\s*catch\s*\{\s*\}",
        script,
    )


def test_stop_service_netstat_fallback_matches_listening_rows() -> None:
    script = _read_script("stop_service.ps1")

    assert 'Select-String -Pattern ":$port\\s+\\S+\\s+LISTENING"' in script


def test_restart_service_checks_child_exit_codes() -> None:
    script = _read_script("restart_service.ps1")

    assert script.count("$LASTEXITCODE") >= 2
    assert "stop_service.ps1 failed" in script
    assert "start_service.ps1 failed" in script
