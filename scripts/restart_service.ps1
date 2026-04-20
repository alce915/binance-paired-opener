$ErrorActionPreference = 'Stop'

function Test-IsAdmin {
    $current = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($current)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

if (-not (Test-IsAdmin)) {
    try {
        $argsList = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $PSCommandPath)
        $process = Start-Process -FilePath 'powershell.exe' -ArgumentList $argsList -Verb RunAs -Wait -PassThru
        exit $process.ExitCode
    } catch {
        throw "Unable to elevate restart_service.ps1: $($_.Exception.Message)"
    }
}

$projectRoot = Split-Path $PSScriptRoot -Parent
$stopScript = Join-Path $PSScriptRoot 'stop_service.ps1'
$startScript = Join-Path $PSScriptRoot 'start_service.ps1'

Set-Location $projectRoot

& powershell.exe -NoProfile -ExecutionPolicy Bypass -File $stopScript
if ($LASTEXITCODE -ne 0) {
    throw "stop_service.ps1 failed with exit code $LASTEXITCODE"
}

& powershell.exe -NoProfile -ExecutionPolicy Bypass -File $startScript
if ($LASTEXITCODE -ne 0) {
    throw "start_service.ps1 failed with exit code $LASTEXITCODE"
}
