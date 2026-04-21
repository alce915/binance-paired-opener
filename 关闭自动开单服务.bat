@echo off
setlocal
cd /d "%~dp0"

echo Stopping paired opener service...
echo.

powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\stop_service.ps1"
set "EXIT_CODE=%ERRORLEVEL%"

echo.
if "%EXIT_CODE%"=="0" (
    echo Service stop command completed.
) else (
    echo Service stop failed.
)
echo.
if not "%EXIT_CODE%"=="0" pause
