@echo off
setlocal
cd /d "%~dp0"

echo Restarting paired opener service...
echo.

powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\restart_service.ps1"
set "EXIT_CODE=%ERRORLEVEL%"

echo.
if "%EXIT_CODE%"=="0" (
    echo Service restarted: http://127.0.0.1:8000/
) else (
    echo Service restart failed. Check api.runtime.log
    echo %~dp0api.runtime.log
)
echo.
if not "%EXIT_CODE%"=="0" pause
