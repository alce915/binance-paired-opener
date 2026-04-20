@echo off
setlocal
cd /d "%~dp0"

echo.
echo [Codex] Starting regression run...
echo [Codex] Full log: .codex-runtime\regression\latest.log
echo.

powershell -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\run_regression_tests.ps1"
set "EXIT_CODE=%ERRORLEVEL%"

echo.
echo [Codex] Status file: "%~dp0.codex-runtime\regression\latest.status.txt"
echo [Codex] Summary file: "%~dp0.codex-runtime\regression\latest.summary.txt"
echo [Codex] Log file: "%~dp0.codex-runtime\regression\latest.log"
echo [Codex] Exit code: %EXIT_CODE%
echo.
pause
exit /b %EXIT_CODE%
