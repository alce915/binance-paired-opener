from __future__ import annotations

import re
import subprocess
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
BAT_FILES = (
    PROJECT_ROOT / "启动自动开单服务.bat",
    PROJECT_ROOT / "关闭自动开单服务.bat",
    PROJECT_ROOT / "重启自动开单服务.bat",
)


def _read_script(name: str) -> str:
    return (SCRIPTS_DIR / name).read_text(encoding="ascii")


def test_service_scripts_do_not_self_elevate() -> None:
    for script_name in ("start_service.ps1", "restart_service.ps1"):
        script = _read_script(script_name)

        assert "Test-IsAdmin" not in script
        assert "-Verb RunAs" not in script


def test_stop_service_only_elevates_on_access_denied_retry() -> None:
    script = _read_script("stop_service.ps1")

    assert "param(" in script
    assert "ElevatedRetry" in script
    assert "-Verb RunAs" in script


def test_service_launcher_scripts_are_tracked_by_git() -> None:
    for relative_path in ("scripts/start_service.ps1", "scripts/run_service_console.ps1", "scripts/run_service.py"):
        assert (PROJECT_ROOT / relative_path).exists(), f"{relative_path} must exist so launch scripts work on a clean checkout"


def test_start_service_netstat_fallback_matches_listening_rows() -> None:
    script = _read_script("start_service.ps1")

    assert 'Select-String -Pattern ":$port\\s+\\S+\\s+LISTENING"' in script
    assert 'Get-ListeningConnections' in script
    assert 'Test-PythonCandidate' in script
    assert 'Service start failed: port $port is already in use' in script
    assert "service-process.json" in script
    assert "run_service.py" in script
    assert "pythonw.exe" in script
    assert "Start-Process -FilePath $servicePythonExe" in script
    assert "-RedirectStandardOutput $runtimeLog" not in script


def test_start_service_only_tails_runtime_log_on_startup_failure() -> None:
    script = _read_script("start_service.ps1")

    assert "Get-Content $runtimeLog -Tail" in script
    assert "Get-Content $runtimeLog -Raw" not in script


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
    assert "service-process.json" in script
    assert "taskkill" in script
    assert "Stop-Process -Id" in script
    assert "launcher_start_time" in script


def test_service_scripts_do_not_shadow_builtin_pid_variable() -> None:
    for script_name in ("start_service.ps1", "stop_service.ps1"):
        script = _read_script(script_name)

        assert "$pid =" not in script


def test_restart_service_checks_child_exit_codes() -> None:
    script = _read_script("restart_service.ps1")

    assert "& $stopScript" in script
    assert "& $startScript" in script
    assert "stop_service.ps1 failed" in script
    assert "start_service.ps1 failed" in script


def test_service_bat_files_only_pause_on_failure() -> None:
    for bat_path in BAT_FILES:
        bat = bat_path.read_text(encoding="utf-8")

        assert "pause" in bat
        assert 'if not "%EXIT_CODE%"=="0" pause' in bat
        assert not re.search(r"(?im)^\s*pause\s*$", bat)
